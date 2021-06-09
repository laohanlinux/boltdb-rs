use crate::cursor::Cursor;
use crate::db::{Meta, WeakDB, DB};
use crate::{error::Error, error::Result, Bucket, Page, PgId};
use parking_lot::lock_api::MutexGuard;
use parking_lot::{
    MappedRwLockReadGuard, MappedRwLockWriteGuard, Mutex, RawMutex, RawRwLock, RwLock,
    RwLockReadGuard, RwLockWriteGuard,
};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

/// Represents the internal transaction identifier
pub type TxId = u64;

pub(crate) type CommitHandler = Box<dyn FnOnce() + Send>;

pub(crate) struct TxInner {
    /// is transaction writable
    pub(crate) writeable: bool,
    /// declares that transaction is in use
    pub(crate) managed: AtomicBool,
    /// defines whether transaction will be checked on close
    pub(crate) check: AtomicBool,
    /// ref to DB
    /// if transaction closed then ref points to null
    pub(crate) db: RwLock<WeakDB>,
    /// transaction meta
    pub(crate) meta: RwLock<Meta>,
    /// root bucket.
    /// this bucket holds another buckets
    pub(crate) root: RwLock<Bucket>,
    /// page cache
    pub(crate) pages: RwLock<HashMap<PgId, Page>>,
    /// transactions statistics
    pub(crate) stats: Mutex<TxStats>,
    /// List of callbacks that will be called after commit
    pub(crate) commit_handlers: Mutex<Vec<Box<dyn Fn()>>>,
    /// WriteFlag specifies the flag to write-related methods like `WriteTo()`.
    /// Tx Opens the database file with the specified flag to copy the data.
    ///
    /// By default, the flag is unset, which works well for mostly in-memory
    /// workloads. For databases that are much larger than available RAM,
    /// set the flag to syscall.O_DIRECT to avoid trashing the page cache.
    pub(super) write_flag: usize,
}

impl Debug for TxInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let db = self
            .db
            .try_read()
            .unwrap()
            .upgrade()
            .map(|db| &db as *const DB);
        f.debug_struct("TxInner")
            .field("writable", &self.writeable)
            .field("managed", &self.managed)
            .field("db", &db)
            .field("meta", &*self.meta.try_read().unwrap())
            .field("root", &self.root.try_read().unwrap())
            .field("pages", &*self.pages.try_read().unwrap())
            .field("stats", &self.stats.lock())
            .field(
                "commit handlers len",
                &self.commit_handlers.try_lock().unwrap().len(),
            )
            .field("write_flag", &self.write_flag)
            .finish()
    }
}

/// Represents a `read-only` or `read-write` transaction on the database.
/// `Read-Only` transactions can be user for retrieving values for keys and creating cursors.
/// `Read/Write` transactions can create and remove buckets and create and remove keys.
///
/// *IMPORTANT*: You must commit or rollback transactions when you are done with
/// Them. Pages can not be reclaimed by the writer until no more transactions
/// are using them. A long running read transaction can cause the database to
/// quickly grow.
#[derive(Clone)]
pub(crate) struct TX(pub(crate) Arc<TxInner>);

unsafe impl Sync for TX {}
unsafe impl Send for TX {}

impl TX {
    pub(crate) fn stats(&self) -> MutexGuard<'_, RawMutex, TxStats> {
        self.0.stats.try_lock().unwrap()
    }

    pub(crate) fn meta_mut(&self) -> parking_lot::lock_api::RwLockWriteGuard<'_, RawRwLock, Meta> {
        self.0.meta.try_write().unwrap()
    }

    pub(crate) fn set_pgid(&mut self, id: PgId) -> Result<()> {
        self.0
            .meta
            .try_write()
            .ok_or(Error::Unknown("pgid locked".to_owned()))?
            .pg_id = id;
        Ok(())
    }

    /// Returns a reference to the page with a given id.
    /// If page has been written to then a temporary buffered page is returned.
    /// Use &Page reference to return
    pub(crate) fn page(&self, id: PgId) -> Result<*const Page> {
        // check the dirty pages first.
        {
            let pages = self.0.pages.try_read().unwrap();
            if let Some(p) = pages.get(&id) {
                return Ok(&*p);
            }
        }

        // Otherwise return directly from the mmap.
        Ok(&*self.db()?.page(id))
    }

    pub(crate) fn writable(&self) -> bool {
        self.0.writeable
    }

    pub(crate) fn db(&self) -> Result<DB> {
        self.0
            .db
            .try_read()
            .unwrap()
            .upgrade()
            .ok_or(Error::DatabaseGone)
    }

    pub(crate) fn id(&self) -> TxId {
        self.0.meta.try_read().unwrap().tx_id
    }

    pub(crate) fn pgid(&self) -> PgId {
        self.0.meta.try_read().unwrap().pg_id
    }

    pub(crate) fn on_commit(&mut self, handler: Box<dyn Fn()>) {
        self.0.commit_handlers.lock().push(handler)
    }

    pub(crate) fn size(&self) -> i64 {
        self.pgid() as i64 * self.db().unwrap().page_size() as i64
    }

    /// Bucket retrieves a bucket by name.
    /// Returns None if the bucket does not exist.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn bucket(&self, key: &[u8]) -> Result<MappedRwLockReadGuard<Bucket>> {
        let bucket = self
            .0
            .root
            .try_read()
            .ok_or(Error::Unknown("can't acquire bucket".to_owned()))?;
        RwLockReadGuard::try_map(bucket, |b| b.bucket(&key.to_vec()))
            .map_err(|_| Error::Unknown("can't get bucket".to_owned()))
    }

    pub fn bucket_mut(&self, key: &[u8]) -> Result<MappedRwLockWriteGuard<Bucket>> {
        let bucket = self
            .0
            .root
            .try_write()
            .ok_or(Error::Unknown("can't acquire bucket".to_owned()))?;
        RwLockWriteGuard::try_map(bucket, |b| b.bucket_mut(&key.to_vec()))
            .map_err(|_| Error::Unknown("can't get mut bucket".to_owned()))
    }

    /// returns bucket keys for db
    pub fn buckets(&self) -> Vec<Vec<u8>> {
        self.0.root.read().buckets()
    }

    /// Create a new bucket.
    /// Returns an error if bucket already exists, if the bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn create_bucket(&self, key: &[u8]) -> Result<MappedRwLockWriteGuard<Bucket>> {
        if !self.0.writeable {
            return Err(Error::TxReadOnly);
        }
        let bucket = self
            .0
            .root
            .try_write()
            .ok_or(Error::Unknown("can't create bucket"))?;
        RwLockWriteGuard::try_map(bucket, |b| b.create_bucket(key).ok())
            .map_err(Error::Unknown("can't create bucket"))
    }

    /// Create a new bucket if it does't already exits
    /// Returns an error if the bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn create_bucket_if_not_exists(
        &mut self,
        key: &[u8],
    ) -> Result<MappedRwLockWriteGuard<Bucket>> {
        if !self.0.writeable {
            return Err(Error::TxReadOnly);
        }
        let bucket = self
            .0
            .root
            .try_write()
            .ok_or(Error::Unknown("Can't acquire bucket"))?;
    }
}

/// TxStats represents statistics about the actions performed by the transaction.
#[derive(Default, Debug, Clone)]
pub struct TxStats {
    // Page statistics
    // number of page allocations.
    pub(crate) page_count: usize,
    // total bytes allocated.
    pub(crate) page_alloc: usize,

    // Cursor statistics
    // number of cursors created.
    pub(crate) cursor_cont: usize,

    // Node statistics
    // number of node allocations
    pub(crate) node_count: usize,
    // number of node dereferences
    pub(crate) node_deref: usize,

    // Rebalance statistics
    // number of node rebalances
    pub(crate) rebalance: usize,
    // total time spent rebalancing
    pub(crate) rebalance_time: Duration,

    // Split/Spill statistics
    // number of nodes spilt
    pub(crate) split: usize,
    // number of nodes spill
    pub(crate) spill: usize,
    // total time spent spilling
    pub(crate) spill_time: Duration,

    // Write statistics.
    // number of writes performed
    pub(crate) write: usize,
    // total time spent writing to disk
    pub(crate) write_time: Duration,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_tx_commit_err_tx_closed() {}

    // #[test]
    // fn test_tx_rollback_err_tx_closed() {}
    //
    // #[test]
    // fn test_tx_commit_err_tx_not_writable() {}
    //
    // #[test]
    // fn test_tx_cursor() {}
    //
    // #[test]
    // fn test_tx_create_bucket_err_tx_not_writable() {}
    //
    // #[test]
    // fn test_tx_create_bucket_err_tx_closed() {}
    //
    // #[test]
    // fn test_tx_bucket() {}
    //
    // #[test]
    // fn test_get_not_found() {}
    //
    // #[test]
    // fn test_tx_create_bucket() {}
    //
    // #[test]
    // fn test_create_bucket_if_not_exists() {}
    //
    // #[test]
    // fn test_tx_create_bucket_if_not_exists_err_bucket_name_required() {}
    //
    // #[test]
    // fn test_tx_create_bucket_err_bucket_exists() {}
    //
    // #[test]
    // fn test_tx_create_bucket_err_bucket_name_required() {}
    //
    // #[test]
    // fn test_tx_delete_bucket() {}
    //
    // #[test]
    // fn test_tx_delete_bucket_err_tx_closed() {}
    //
    // #[test]
    // fn test_tx_delete_bucket_read_only() {}
    //
    // #[test]
    // fn test_tx_delete_bucket_not_found() {}
    //
    // #[test]
    // fn test_tx_foreach_no_error() {}
    //
    // #[test]
    // fn test_tx_foreach_with_error() {}
    //
    // #[test]
    // fn test_tx_on_commit() {}
    //
    // #[test]
    // fn test_tx_on_commit_rollback() {}
    //
    // #[test]
    // fn test_tx_copy_file() {}
    //
    // #[test]
    // fn test_tx_copy_file_error_meta() {}
    //
    // #[test]
    // fn test_tx_copy_file_error_normal() {}
    //
    // #[test]
    // fn example_tx_rollback() {}
    //
    // #[test]
    // fn example_tx_copy_file() {}
}
