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
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread::spawn;
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
            .ok_or(Error::Unknown("pgid locked"))?
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
            .ok_or(Error::Unknown("can't acquire bucket"))?;
        RwLockReadGuard::try_map(bucket, |b| b.bucket(&key.to_vec()))
            .map_err(|_| Error::Unknown("can't get bucket"))
    }

    pub fn bucket_mut(&self, key: &[u8]) -> Result<MappedRwLockWriteGuard<Bucket>> {
        let bucket = self
            .0
            .root
            .try_write()
            .ok_or(Error::Unknown("can't acquire bucket"))?;
        RwLockWriteGuard::try_map(bucket, |b| b.bucket_mut(&key.to_vec()))
            .map_err(|_| Error::Unknown("can't get mut bucket"))
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
            .map_err(|_| Error::Unknown("can't create bucket"))
    }

    /// Create a new bucket if it does't already exits
    /// Returns an error if the bucket name is blank, or if the bucket name is too long.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn create_bucket_if_not_exists(
        &mut self,
        key: &[u8],
    ) -> Result<MappedRwLockWriteGuard<Bucket>> {
        if !self.writable() {
            return Err(Error::TxReadOnly);
        }
        let bucket = self
            .0
            .root
            .try_write()
            .ok_or(Error::Unknown("Can't acquire bucket"))?;

        RwLockWriteGuard::try_map(bucket, |b| b.create_bucket_if_not_exists(key).ok())
            .map_err(|_| Error::Unknown("Can't get bucket"))
    }

    /// Deletes a bucket
    /// Returns an error if the bucket cannot be found or if the key represents a non-bucket value.
    pub fn delete_bucket(&mut self, key: &[u8]) -> Result<()> {
        if !self.writable() {
            return Err(Error::TxReadOnly);
        }
        self.0.root.try_write().unwrap().delete_bucket(key)
    }

    /// Executions a function for each bucket in the root.
    /// If the provided function returns an error then the iteration if stopped and
    /// the error is returned to the caller.
    ///
    /// first argument of function is bucket's key, second is bucket itself.
    pub fn for_each<E: Into<Error>>(
        &self,
        mut handler: Box<dyn FnMut(&[u8], Option<&Bucket>)>,
    ) -> Result<()> {
        todo!()
    }

    /// Writes the entries database to a writer.
    /// If err == nil then exactly tx.Size() bytes will be written into the writer.
    pub fn write_to<W: Write>(&self, mut w: W) -> Result<i64> {
        todo!()
    }

    /// Copies the entries database to file at the given path.
    /// A reader transaction is maintained during the copy so it is safe to continue
    /// using the database while a copy is in progress.
    pub fn copy_to(&self, path: &str, mode: OpenOptions) -> Result<()> {
        let file = mode
            .open(path)
            .map_err(|_| Error::Unknown("can't open the file"))?;
        self.write_to(file)?;
        Ok(())
    }

    /// Closes transaction (so subsequent user of it will resolve in error)
    pub(crate) fn close(&self) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.remove_tx()?;
        tx.0.root.try_write().unwrap().clear();
        tx.0.pages.try_write().unwrap().clear();
        Ok(())
    }

    /// Writes all changes to disk and updates the meta page.
    /// Returns an error if a disk write error occurrs; or if Commit is
    /// called on a read-only transaction.
    pub fn commit(&mut self) -> Result<()> {
        todo!()
    }

    /// Closes the transaction and ignores all previous updates. Read-Only
    /// transactions must be rolled back and not committed
    pub fn rollback(&mut self) -> Result<()> {
        if self.0.managed.load(Ordering::Acquire) {
            return Err(Error::TxManaged);
        }
        self.__rollback()?;
        Ok(())
    }

    fn __rollback(&mut self) -> Result<()> {
        todo!()
    }

    /// Sync version of check()
    ///
    /// In case of checking thread panic will also return Error
    pub fn check_sync(&self) -> Result<()> {
        let (sender, ch) = mpsc::channel::<String>();
        let tx = self.clone();
        let handle = spawn(move || tx.__check(sender));

        let mut errs = vec![];
        for err in ch {
            errs.push(err)
        }
        if handle.join().is_err() {
            errs.push("check thread panicked".to_owned());
        }
        if !errs.is_empty() {
            return Err(Error::CheckFailed(errs.join("|")));
        }
        Ok(())
    }

    /// Performs serveral consistency checks on the database for this transaction.
    /// An error is returned if any inconsistency is found.
    ///
    /// It can be safely

    fn __check(&self, ch: mpsc::Sender<String>) {
        todo!()
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
