use crate::cursor::Cursor;
use crate::db::{CheckMode, Meta, WeakDB, DB};
use crate::error::Error::Unexpected;
use crate::page::FREE_LIST_PAGE_FLAG;
use crate::page::{OwnedPage, META_PAGE_FLAG};
use crate::{error::Error, error::Result, Bucket, Page, PageInfo, PgId};
use log::{debug, info, warn};
use parking_lot::lock_api::MutexGuard;
use parking_lot::{
    MappedRwLockReadGuard, MappedRwLockWriteGuard, Mutex, RawMutex, RawRwLock, RwLock,
    RwLockReadGuard, RwLockWriteGuard,
};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Weak};
use std::thread::spawn;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Represents the internal transaction identifier
pub type TxId = u64;

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
    pub(crate) pages: RwLock<HashMap<PgId, OwnedPage>>,
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
/// *IMPORTANT*: You must commit or rollback transactions when you are done withtx.rs
/// Them. Pages can not be reclaimed by the writer until no more transactions
/// are using them. A long running read transaction can cause the database to
/// quickly grow.
#[derive(Clone)]
pub struct TX(pub(crate) Arc<TxInner>);

unsafe impl Sync for TX {}

unsafe impl Send for TX {}

impl TX {
    /// Creates a cursor associated with the root bucket.
    /// All items in the cursor will return a nil value because all items in root bucket are also buckets.
    /// The cursor is only valid as long as the transaction is open.
    /// Do not use a cursor after the transaction is closed.
    pub fn cursor(&self) -> Cursor<RwLockWriteGuard<Bucket>> {
        self.0.stats.lock().cursor_count += 1;
        Cursor::new(self.0.root.write())
    }

    pub(crate) fn stats(&self) -> MutexGuard<'_, RawMutex, TxStats> {
        self.0.stats.try_lock().unwrap()
    }

    pub(crate) fn meta_mut(&self) -> parking_lot::lock_api::RwLockWriteGuard<'_, RawRwLock, Meta> {
        self.0.meta.try_write().unwrap()
    }

    pub(crate) fn set_pgid(&mut self, id: PgId) -> Result<()> {
        self.0.meta.try_write().ok_or("pgid locked")?.pg_id = id;
        Ok(())
    }

    /// Returns a reference to the page with a given id.
    /// If page has been written to then a temporary buffered page is returned.
    /// Why not use &Page reference to return? here can avoid to owner limit
    pub(crate) fn page(&self, id: PgId) -> Result<*const Page> {
        // check the dirty pages first.
        {
            let pages = self.0.pages.try_read().unwrap();
            if let Some(p) = pages.get(&id) {
                return Ok(&**p);
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

    // return current transaction id
    pub(crate) fn id(&self) -> TxId {
        self.0.meta.try_read().unwrap().tx_id
    }

    // return current waker id
    pub(crate) fn pgid(&self) -> PgId {
        self.0.meta.try_read().unwrap().pg_id
    }

    // return the db size by use waker id cal
    pub(crate) fn size(&self) -> i64 {
        self.pgid() as i64 * self.db().unwrap().page_size() as i64
    }

    /// Bucket retrieves a bucket by name.
    /// Returns None if the bucket does not exist.
    /// The bucket instance is only valid for the lifetime of the transaction.
    pub fn bucket(&self, key: &[u8]) -> Result<MappedRwLockReadGuard<Bucket>> {
        let bucket = self.0.root.try_read().ok_or("can't acquire bucket")?;
        RwLockReadGuard::try_map(bucket, |b| b.bucket(key))
            .map_err(|_| Error::Unexpected("can't get bucket"))
    }

    pub fn bucket_mut(&self, key: &[u8]) -> Result<MappedRwLockWriteGuard<Bucket>> {
        let bucket = self
            .0
            .root
            .try_write()
            .ok_or(Error::Unexpected("can't acquire bucket"))?;
        RwLockWriteGuard::try_map(bucket, |b| b.bucket_mut(key))
            .map_err(|_| Error::Unexpected("can't get mut bucket"))
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
        let bucket = self.0.root.try_write().ok_or("can't create bucket")?;
        RwLockWriteGuard::try_map(bucket, |b| b.create_bucket(key).ok())
            .map_err(|_| Error::Unexpected("can't create bucket"))
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
        let bucket = self.0.root.try_write().ok_or("Can't acquire bucket")?;

        RwLockWriteGuard::try_map(bucket, |b| b.create_bucket_if_not_exists(key).ok())
            .map_err(|_| Error::Unexpected("Can't get bucket"))
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
        mut handler: impl FnMut(&[u8], Option<&Bucket>) -> Result<()>,
    ) -> Result<()> {
        let root = self.0.root.try_write().unwrap();
        root.for_each(|key, _v| handler(key, root.bucket(key)))
    }

    /// Writes the entries database to a writer.
    /// If err == nil then exactly tx.Size() bytes will be written into the writer.
    pub fn write_to<W: Write>(&self, mut w: W) -> Result<i64> {
        let db = self.db()?;
        let page_size = db.page_size();
        let mut file =
            db.0.file
                .try_write()
                .ok_or("cannot obtain file write access")?;
        let mut written = 0;
        let mut page = OwnedPage::new(page_size);
        page.flags = META_PAGE_FLAG;

        // first page
        {
            *page.meta_mut() = self.0.meta.try_read().unwrap().clone();
            page.meta_mut().check_sum = page.meta().sum64();
            w.write_all(page.buf())?;
            written += page.size();
        }

        // second page
        {
            page.id = 1;
            page.meta_mut().tx_id -= 1;
            page.meta_mut().check_sum = page.meta().sum64();
            w.write_all(page.buf())?;
            written += page.size();
        }

        file.seek(SeekFrom::Start(page_size as u64 * 2))?;

        let size = self.size() as u64 - (page_size as u64 * 2);
        written +=
            std::io::copy(&mut Read::by_ref(&mut file.get_mut()).take(size), &mut w)? as usize;
        Ok(written as i64)
    }

    /// Copies the entries database to file at the given path.
    /// A reader transaction is maintained during the copy so it is safe to continue
    /// using the database while a copy is in progress.
    pub fn copy_to(&self, path: &str, mode: OpenOptions) -> Result<()> {
        let file = mode
            .open(path)
            .map_err(|_| Error::Unexpected("can't open the file"))?;
        self.write_to(file)?;
        Ok(())
    }

    /// Closes transaction (so subsequent user of it will resolve in error)
    pub(crate) fn close(&self) -> Result<()> {
        defer_lite::defer! {
            kv_log_macro::info!("close transaction, strong-count: {}", Arc::strong_count(&self.0));
        }
        let db = self.db()?;
        let tx = db.remove_tx(self)?;
        *tx.0.db.write() = WeakDB::new();
        tx.0.root.try_write().unwrap().clear();
        tx.0.pages.try_write().unwrap().clear();
        Ok(())
    }

    /// Writes all changes to disk and updates the meta page.
    /// Returns an error if a disk write error occurrs; or if Commit is
    /// called on a read-only transaction.
    pub fn commit(&mut self) -> Result<()> {
        if self.0.managed.load(Ordering::Acquire) {
            return Err(Error::TxManaged);
        } else if !self.writable() {
            return Err(Error::TxReadOnly);
        }
        let mut db = self.db()?;
        {
            let start_time = SystemTime::now();
            // rebalance
            self.0.root.try_write().unwrap().rebalance();
            let mut stats = self.0.stats.lock();
            if stats.rebalance > 0 {
                stats.rebalance_time += SystemTime::now()
                    .duration_since(start_time)
                    .map_err(|_| Unexpected("Cann't get system time"))?;
            }
        }

        {
            // spill
            let start_time = SystemTime::now();
            {
                let mut root = self.0.root.try_write().unwrap();
                root.spill()?;
            }
            self.0.stats.try_lock().unwrap().spill_time = SystemTime::now()
                .duration_since(start_time)
                .map_err(|_| Unexpected("Cann't get system time"))?;
        }

        // Free the old root bucket.
        self.0.meta.try_write().unwrap().root.root =
            self.0.root.try_read().unwrap().local_bucket.root;

        let (txid, tx_pgid, free_list_pgid) = {
            let meta = self.0.meta.try_read().unwrap();
            (meta.tx_id as usize, meta.pg_id, meta.free_list)
        };

        let (free_list_size, page_size) = {
            // Free the freelist and allocate new pages for it. This will overestimate
            // the size of the freelist but not underestimate the size (which would to bad).
            let page = db.page(free_list_pgid);
            let mut free_list = db.0.free_list.try_write().unwrap();
            free_list.free(txid as u64, &page);

            let free_list_size = free_list.size();
            let page_size = db.page_size() as usize;

            (free_list_size, page_size)
        };
        {
            let page = self.allocate((free_list_size / page_size) as u64 + 1);
            if let Err(err) = page {
                self.rollback()?;
                return Err(err);
            }

            let page = page?;
            let page = unsafe { &mut *page };

            // replace free list page
            db.0.free_list.try_write().unwrap().write(page);
            self.0.meta.try_write().unwrap().free_list = page.id;

            // If the high watermark has moved up then attempt to grow the database.
            // TODO: Why?
            if self.pgid() > tx_pgid as u64 {
                if let Err(e) = db.grow((tx_pgid + 1) * page_size as u64) {
                    info!("set a higher watermark");
                    self.rollback()?;
                    return Err(e);
                }
            }

            // Write dirty pages to disk.
            let write_start_time = SystemTime::now();
            if let Err(e) = self.write() {
                self.rollback()?;
                return Err(e);
            }
            // If strict mode is enabled then perform a consistency check.
            // Only the first consistency error is reported in the panic.
            if self.0.check.swap(false, Ordering::AcqRel) {
                let strict = db.0.check_mode.contains(CheckMode::STRICT);
                if let Err(e) = self.check_sync() {
                    kv_log_macro::error!("failed to check sync, {:?}", e);
                    if strict {
                        self.rollback()?;
                        return Err(e);
                    } else {
                        warn!("failed to check sync, {:?}", e);
                    }
                }
            }

            // Write meta to disk
            {
                if let Err(e) = self.write_meta() {
                    self.0.check.store(false, Ordering::Release);
                    self.rollback()?;
                    return Err(e);
                }
            }

            self.0.stats.try_lock().unwrap().write_time += std::time::SystemTime::now()
                .duration_since(write_start_time)
                .map_err(|_| Unexpected("Cann't get system time"))?;
        }

        // Finalize the transaction.
        self.close()?;
        info!("after close: {}", Arc::strong_count(&self.0));
        {
            for h in &*self.0.commit_handlers.try_lock().unwrap() {
                h();
            }
        }
        info!("finished to commit transaction");
        Ok(())
    }

    /// Closes the transaction and ignores all previous updates. Read-Only
    /// transactions must be rolled back and not committed
    pub fn rollback(&self) -> Result<()> {
        if self.0.managed.load(Ordering::Acquire) {
            return Err(Error::TxManaged);
        }
        self.__rollback()?;
        Ok(())
    }

    pub(crate) fn __rollback(&self) -> Result<()> {
        kv_log_macro::debug!("ready to __rollback");
        let db = self.db()?;
        if self.0.check.swap(false, Ordering::AcqRel) {
            let strict = db.0.check_mode.contains(CheckMode::STRICT);
            if let Err(err) = self.check_sync() {
                if strict {
                    return Err(err);
                } else {
                    info!("failed to check sync, {:?}", err);
                }
            }
        }

        if self.writable() {
            let txid = self.id();
            let mut free_list = db.0.free_list.write();
            free_list.rollback(&txid);
            let free_list_id = db.meta().free_list;
            let free_list_page = db.page(free_list_id);
            free_list.reload(&free_list_page);
        }
        self.close()?;
        Ok(())
    }

    /// Sync version of check()
    ///
    /// In case of checking thread panic will also return Error
    pub fn check_sync(&self) -> Result<()> {
        defer_lite::defer! {
            kv_log_macro::info!("succeed to check sync");
        }
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
    /// It can be safely run concurrently on a writable transaction. However, this
    /// incurs a high  cost for large databases and databases with a lot of subbuckets
    /// because of caching. This overhead can be removed if  running on a read-only
    /// transaction, however, it is not saft to execute other writer transactions at
    /// the same time.
    pub fn check(&self) -> mpsc::Receiver<String> {
        let (sender, receiver) = mpsc::channel::<String>();
        let tx = self.clone();
        spawn(move || tx.__check(sender));
        receiver
    }

    fn check_bucket(
        &self,
        b: &Bucket,
        reachable: &mut HashMap<PgId, bool>,
        freed: &HashMap<PgId, bool>,
        ch: &mpsc::Sender<String>,
    ) {
        let b_root = b.local_bucket.root;
        let is_inline_page = b.page.is_some();
        defer_lite::defer! {
            kv_log_macro::debug!("succeed to check bucket: {}, inline: {}", b_root, is_inline_page);
        }
        if b.local_bucket.root == 0 {
            return;
        }
        let meta_pgid = self.pgid();
        let handler = |p: &Page, _pgid: usize| {
            if p.id > meta_pgid {
                ch.send(format!("page {}: out of bounds: {}", p.id, meta_pgid))
                    .unwrap();
            }
            for i in 0..=p.over_flow {
                let id = p.id + u64::from(i);
                if reachable.contains_key(&id) {
                    ch.send(format!("page {}: multiple reference", id)).unwrap();
                }
                reachable.insert(id, true);
            }

            let page_type_is_valid = matches!(
                p.flags,
                crate::page::BRANCH_PAGE_FLAG | crate::page::LEAF_PAGE_FLAG
            );
            if freed.contains_key(&p.id) {
                ch.send(format!("page {}: reachable freed", p.id)).unwrap();
            } else if !page_type_is_valid {
                ch.send(format!("page {}: invalid type: {}", p.id, p.flags))
                    .unwrap();
            }
        };

        b.tx()
            .unwrap()
            .for_each_page(b.local_bucket.root, 0, Box::new(handler));

        // check subbucket
        b.for_each(|k, v| -> Result<()> {
            let child = b.bucket(k);
            if let Some(child) = child {
                self.check_bucket(child, reachable, freed, ch);
            }
            Ok(())
        })
        .unwrap();
    }

    /// Returns a contiguous block of memory starting at a given page.
    /// New pages will stored at commit pharse.
    pub(crate) fn allocate(&mut self, count: u64) -> Result<*mut Page> {
        let mut db = self.db()?;
        let mut page = db.allocate(count, self)?;
        let pgid = page.id;
        let page_ptr = &mut *page as *mut Page;
        self.0.pages.try_write().unwrap().insert(pgid, page);

        // Updates statistics
        {
            let mut stats = self.0.stats.lock();
            stats.page_count += 1;
            stats.page_alloc += count as usize * self.db()?.page_size();
        }

        Ok(page_ptr)
    }

    /// Writes any dirty pages to disk.
    pub(crate) fn write(&mut self) -> Result<()> {
        let mut pages = self
            .0
            .pages
            .write()
            .drain()
            .map(|x| {
                let pgid = x.1.id;
                (pgid, x.1)
            })
            .collect::<Vec<_>>();
        pages.sort_by(|a, b| a.0.cmp(&b.0));
        kv_log_macro::debug!("ready to write dirty pages: {:?}", pages);
        let mut db = self.db()?;
        let page_size = db.page_size();
        for (id, p) in &pages {
            let size = (p.over_flow + 1) as usize * page_size;
            let offset = *id as u64 * page_size as u64;
            let buffer = unsafe { std::slice::from_raw_parts(p.as_ptr(), size) };
            let cursor = std::io::Cursor::new(buffer);
            db.write_at(offset, cursor)?;
        }

        if !db.0.no_sync {
            db.sync()?;
        }

        /// TODO add page pool
        Ok(())
    }

    /// Writes the meta to disk.
    pub(crate) fn write_meta(&mut self) -> Result<()> {
        let meta_info = format!("{:?}", self.meta_mut());
        defer_lite::defer! {
            info!(meta=meta_info; "write meta page");
        }
        let mut db = self.db()?;
        let mut buffer = vec![0u8; db.page_size() as usize];
        let mut page = Page::from_slice_mut(&mut buffer);
        self.0.meta.try_write().unwrap().write(&mut page)?;
        // TODO why the pos is zero
        db.write_at(0, std::io::Cursor::new(buffer))?;
        if !db.0.no_sync {
            db.sync()?;
        }
        self.0.stats.lock().write += 1;
        Ok(())
    }

    /// Returns page information for a given page number.
    /// This is only safe for concurrent use when used by a writable transaction.
    pub fn page_info(&self, id: usize) -> Result<Option<PageInfo>> {
        if !self.opened() {
            return Err(Error::TxClosed);
        }
        if id > self.pgid() as usize {
            return Ok(None);
        }

        let db = self.db()?;

        // Build the page info.
        let p = db.page(id as u64);
        let mut info = PageInfo {
            id: id as u64,
            typ: FREE_LIST_PAGE_FLAG,
            count: p.count as usize,
            over_flow_count: p.over_flow as usize,
        };

        // Determine the type (or if it's free).
        if !db.0.free_list.try_read().unwrap().freed(&(id as u64)) {
            info.typ = p.flags;
        }

        Ok(Some(info))
    }

    /// Defines whether transaction is fresh and not been used before
    /// committing transaction twice must resolve in error
    pub(crate) fn opened(&self) -> bool {
        match self.0.db.try_read().unwrap().upgrade() {
            None => false,
            Some(db) => db.opened(),
        }
    }

    /// Iterates over every page within a given page and page and executes a function.
    pub(crate) fn for_each_page<'a>(
        &self,
        pgid: PgId,
        depth: usize,
        mut func: Box<dyn FnMut(&Page, usize) + 'a>,
    ) {
        let page = unsafe { &*self.page(pgid).unwrap() };
        func(page, depth);
        // Recursively loop over children.
        if page.is_leaf() {
            return;
        }
        let count = page.count as usize;
        for i in 0..count {
            let el = page.branch_page_element(i);
            self.for_each_page(el.pgid, depth + 1, Box::new(|p, d| func(p, d)));
        }
    }

    fn __check(&self, ch: mpsc::Sender<String>) {
        let mut freed = HashMap::<PgId, bool>::new();
        let all_pgids = self
            .db()
            .unwrap()
            .0
            .free_list
            .try_read()
            .unwrap()
            .get_pg_ids();
        for id in &all_pgids {
            if freed.contains_key(id) {
                ch.send(format!("page {}: already freed", id)).unwrap();
            }
            freed.insert(*id, true);
        }

        // check meta
        let mut reachable = HashMap::new();
        reachable.insert(0, true);
        reachable.insert(1, true);
        // check free list
        let freelist_pgid = self.0.meta.try_read().unwrap().free_list;
        let freelist_overflow = unsafe { &*self.page(freelist_pgid).unwrap() }.over_flow;
        for i in 0..=freelist_overflow {
            reachable.insert(freelist_pgid + u64::from(i), true);
        }

        // check bucekt
        self.check_bucket(
            &self.0.root.try_read().unwrap(),
            &mut reachable,
            &freed,
            &ch,
        );

        // check page from 0 to hight watermask
        // every page must be found at freed or reachable
        for i in 0..self.0.meta.try_read().unwrap().pg_id {
            let is_reachable = reachable.contains_key(&i);
            let is_freed = freed.contains_key(&i);
            if !is_reachable && !is_freed {
                ch.send(format!("page {}: unreachable unfreed", i)).unwrap();
            }
        }
    }
}

impl Drop for TX {
    fn drop(&mut self) {
        let count = Arc::strong_count(&self.0);
        // one for the reference, and one that is owned by db
        if count > 2 {
            return;
        }
        let trace_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        // warn!(
        //     "tx count reference has to {}, trace-id: {}",
        //     count, trace_id
        // );
        if let Ok(_db) = self.db() {
            // warn!("has db, trace-id: {}", trace_id);
            if self.0.writeable {
                self.commit().unwrap();
            } else {
                self.rollback().unwrap();
            }
        }
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
    pub(crate) cursor_count: usize,

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

#[derive(Debug, Clone)]
pub(crate) struct WeakTx(Weak<TxInner>);

impl WeakTx {
    pub(crate) fn new() -> Self {
        Self(Weak::new())
    }

    pub(crate) fn upgrade(&self) -> Option<TX> {
        self.0.upgrade().map(TX)
    }

    pub(crate) fn from(tx: &TX) -> Self {
        Self(Arc::downgrade(&tx.0))
    }
}

pub(crate) struct TxBuilder {
    db: WeakDB,
    writable: bool,
    check: bool,
}

impl TxBuilder {
    pub(crate) fn new() -> Self {
        Self {
            db: WeakDB::new(),
            writable: false,
            check: false,
        }
    }
    pub(crate) fn set_db(mut self, db: WeakDB) -> Self {
        self.db = db;
        self
    }

    pub(crate) fn set_writable(mut self, writable: bool) -> Self {
        self.writable = writable;
        self
    }

    pub(crate) fn set_check(mut self, check: bool) -> Self {
        self.check = check;
        self
    }

    pub fn build(self) -> TX {
        let mut meta = match self.db.upgrade() {
            None => Meta::default(),
            Some(db) => db.meta(),
        };
        if self.writable {
            meta.tx_id += 1;
        }
        let tx = TX(Arc::new(TxInner {
            writeable: self.writable,
            managed: Default::default(),
            check: AtomicBool::new(self.check),
            db: RwLock::new(self.db),
            meta: RwLock::new(meta),
            root: RwLock::new(Bucket::new(WeakTx::new())),
            pages: Default::default(),
            stats: Default::default(),
            commit_handlers: Default::default(),
            write_flag: 0,
        }));
        {
            // build circular reference each others.
            let mut bucket = tx.0.root.write();
            bucket.tx = WeakTx::from(&tx);
            // fill root bucket
            bucket.local_bucket = tx.0.meta.try_read().unwrap().root.clone();
        }
        tx
    }
}

/// Guard returned by DB.begin_tx()
///
/// Statically guards against outliving db
/// and prevents from making mutable actions.
///
/// Implements Deref to Tx
pub struct TxGuard<'a> {
    pub(crate) tx: TX,
    pub(crate) db: PhantomData<&'a DB>,
}

impl<'a> Deref for TxGuard<'a> {
    type Target = TX;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

/// Guard returned by DB.begin_rw_tx()
///
/// Statically guards against multiple mutable db borrows.
///
/// Implements Deref and DerefMut to Tx
pub struct RWTxGuard<'a> {
    pub(crate) tx: TX,
    pub(crate) db: PhantomData<&'a mut DB>,
}

impl<'a> Deref for RWTxGuard<'a> {
    type Target = TX;

    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

impl<'a> DerefMut for RWTxGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tx
    }
}

#[cfg(test)]
mod tests {
    use crate::db::DBBuilder;
    use crate::error::{Error, Result};
    use crate::test_util::{mock_db, mock_tx};
    use crate::tx::{TxBuilder, TxInner, TX};
    use log::info;
    use std::io::Read;
    use std::sync::Arc;

    #[test]
    fn commit_empty() {
        let mut db = crate::test_util::mock_db().build().unwrap();
        assert!(db.0.rw_tx.try_read().unwrap().is_none());

        {
            let tx = db.begin_rw_tx().unwrap();
            assert_eq!(Arc::strong_count(&tx.0), 2);
            assert!(tx.db().unwrap().0.rw_tx.try_read().unwrap().is_some());
        }

        assert!(db.0.rw_tx.try_read().unwrap().is_none());
    }

    #[test]
    fn commit_some() {
        let mut db = crate::test_util::mock_db2("c_s".to_owned())
            .build()
            .unwrap();
        let mut tx = db.begin_rw_tx().unwrap();
        {
            let mut bucket = tx.create_bucket(b"bucket").unwrap();
            bucket.put(b"key", b"value".to_vec()).unwrap();
        }
        tx.commit().unwrap();
    }

    #[test]
    fn commit_multiple() {
        let n_commits = 5;
        let n_values = 1000;
        let mut db = crate::test_util::mock_db2("tt.db".to_owned())
            .build()
            .unwrap();
        for i in 0..n_commits {
            let mut tx = db.begin_rw_tx().unwrap();
            let mut bucket = tx.create_bucket_if_not_exists(b"bucket").unwrap();
            for n in 0..n_values {
                bucket
                    .put(format!("key-{}-{}", i, n).as_bytes(), b"value".to_vec())
                    .unwrap();
            }
        }
    }

    #[test]
    fn delete_bucket() {
        let mut db = crate::test_util::mock_db().build().unwrap();
        let mut tx = db.begin_rw_tx().unwrap();
        {
            drop(tx.create_bucket(b"bucket").unwrap());
            drop(tx.bucket(b"bucket").unwrap());

            tx.delete_bucket(b"bucket").unwrap();
            tx.bucket(b"bucket").err().unwrap();
        }
        tx.commit().unwrap();
    }

    #[test]
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    /// ensure that writes produce idempotent file
    fn commit_hash_ensure() {
        use fnv::FnvHasher;
        use std::hash::Hasher;
        use std::io::Write;
        use std::io::{Seek, SeekFrom};
        use std::sync::Arc;

        for _ in 0..20 {
            let mut db = crate::test_util::mock_db()
                .set_auto_remove(true)
                .build()
                .unwrap();
            let mut tx = db.begin_rw_tx().unwrap();
            {
                let mut bucket = tx.create_bucket_if_not_exists(b"bucket").unwrap();
                bucket.put(b"donald", b"trump".to_vec()).unwrap();
                bucket.put(b"barack", b"obama".to_vec()).unwrap();
                bucket.put(b"thomas", b"jefferson".to_vec()).unwrap();
            }
            tx.commit().unwrap();

            let mut file = db.0.file.try_write().unwrap();
            file.flush().unwrap();
            file.seek(SeekFrom::Start(0)).unwrap();

            let mut hasher = FnvHasher::default();
            let mut buf = vec![0u8; db.page_size()];
            while let Ok(()) = file.get_ref().read_exact(&mut buf) {
                hasher.write(&buf);
            }
            let hash = hasher.finish();
            assert_eq!(hash, 9680149046811131486);
        }
    }

    #[test]
    /// ensure that data actually written to disk
    fn commit_ensure() {
        let path = {
            let mut db = crate::test_util::mock_db()
                .set_auto_remove(false)
                .build()
                .unwrap();
            let mut tx = db.begin_rw_tx().unwrap();
            {
                let mut bucket = tx.create_bucket(b"bucket").unwrap();
                bucket.put(b"donald", b"trump".to_vec()).unwrap();
                bucket.put(b"barack", b"obama".to_vec()).unwrap();
                bucket.put(b"thomas", b"jefferson".to_vec()).unwrap();
            }
            tx.commit().unwrap();
            db.path().unwrap()
        };

        let db = DBBuilder::new(path)
            .set_auto_remove(true)
            .set_read_only(false)
            .build()
            .unwrap();
        db.view(|tx| -> crate::error::Result<()> {
            let bucket = tx.bucket(b"bucket").unwrap();
            assert_eq!(bucket.get(b"donald").expect("no donald"), b"trump");
            assert_eq!(bucket.get(b"barack").expect("no barack"), b"obama");
            assert_eq!(bucket.get(b"thomas").expect("no thomas"), b"jefferson");
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn rollback_empty() {
        let db = mock_db().build().unwrap();
        assert!(db.0.rw_tx.try_read().unwrap().is_none());

        let tx = db.begin_tx().unwrap();
        tx.rollback().unwrap();
    }

    #[test]
    fn rollback_some() {
        let mut db = mock_db().build().unwrap();
        let mut tx = db.begin_rw_tx().unwrap();
        {
            let mut bucket = tx.create_bucket(b"bucket").unwrap();
            bucket.put(b"key", b"value".to_vec()).unwrap();
        }
        tx.rollback().unwrap();
    }

    #[test]
    fn for_each() {
        let mut db = mock_db().build().unwrap();
        let mut tx = db.begin_rw_tx().unwrap();
        {
            let mut bucket = tx.create_bucket(b"bucekt").unwrap();
            bucket.put(b"key", b"value".to_vec());
            bucket.put(b"keys", b"value".to_vec());
        }
        {
            let mut bucket = tx.create_bucket(b"another bucket").unwrap();
            bucket.put(b"key", b"value".to_vec()).unwrap();
            bucket.put(b"keys", b"value".to_vec()).unwrap();
        }

        let mut bucket_names = vec![];
        tx.for_each::<Error>(|b, v| -> Result<()> {
            info!(bucket=String::from_utf8(b.to_vec()).unwrap(); "trace tx iterator");
            bucket_names.push(b.to_vec());
            Ok(())
        })
        .unwrap();
        assert_eq!(bucket_names.len(), 2);
        assert!(bucket_names.contains(&b"bucket".to_vec()));
        assert!(bucket_names.contains(&b"another bucket".to_vec()));
    }

    #[test]
    fn check() {
        let mut db = mock_db().build().unwrap();
        {
            let mut tx = db.begin_rw_tx().unwrap();
            {
                let mut bucket = tx.create_bucket(b"bucket").unwrap();
                bucket.put(b"key", b"value".to_vec()).unwrap();
                bucket.put(b"keys", b"value".to_vec()).unwrap();
            }
            {
                let mut bucket = tx.create_bucket(b"another bucket").unwrap();
                bucket.put(b"key", b"value".to_vec()).unwrap();
                bucket.put(b"keys", b"value".to_vec()).unwrap();
            }
            tx.commit().unwrap();
        }
        {
            let mut tx = db.begin_rw_tx().unwrap();
            tx.commit().unwrap();
        }
        {
            let tx = db.begin_tx().unwrap();
            tx.check_sync().unwrap();
            tx.rollback().unwrap();
        }
    }

    #[test]
    fn check_corrupted() {
        let db = DBBuilder::new("./test_data/remark_fail.db")
            .set_read_only(true)
            .build()
            .unwrap();
        db.begin_tx().unwrap().check_sync().unwrap_err();
    }

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
