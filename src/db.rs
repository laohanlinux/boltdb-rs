use crate::bucket::SubBucket;
use crate::error::Error;
use crate::error::Error::{DBOpFailed, Unexpected};
use crate::error::Result;
use crate::free_list::FreeList;
use crate::page::{FREE_LIST_PAGE_FLAG, LEAF_PAGE_FLAG, META_PAGE_FLAG, META_PAGE_SIZE};
use crate::tx::TX;
use crate::{Bucket, Page, PgId, TxId};
use bitflags::bitflags;
use fnv::FnvHasher;
use fs2::FileExt;
use memmap::Mmap;
use parking_lot::{MappedMutexGuard, MappedRwLockReadGuard};
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use std::fs::{File, OpenOptions, Permissions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::AddAssign;
use std::path::{Path, PathBuf};
use std::slice::from_raw_parts;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{sleep, spawn};
use std::time::Duration;
use env_logger::init;
use kv_log_macro::debug;
use parking_lot::lock_api::{RawMutex, RawRwLock};

/// The largest step that can be token when remapping the mman.
const MAX_MMAP_STEP: u64 = 1 << 30; //1GB

/// The data file format version.
const VERSION: usize = 2;

/// Represents a marker value to indicate that a file is a Bolt `DB`.
const MAGIC: u32 = 0xED0CDAED;

/// Default values if not set in a `DB` instance.
const DEFAULT_MAX_BATCH_SIZE: usize = 1000;
const DEFAULT_MAX_BATCH_DELAY: Duration = Duration::from_millis(10);
const DEFAULT_ALLOC_SIZE: isize = 16 * 1024 * 1024;

bitflags! {
    /// Defines when db check will occur
    pub struct CheckMode: u8 {
        /// no check
        const NO = 0b0000;
        /// check on database close
        const CLOSE = 0b0001;
        /// check on end of read transaction
        ///
        /// If there is parallel write transaction going on
        /// check may fail because of old freelist metadata
        const READ = 0b0010;
        /// check on end of write transaction
        const WRITE = 0b0100;
        /// check on close, and end of every transaction
        const ALL = 0b01111;
        /// defines whether result of the check will result
        /// in error or just be spewed in stdout
        const STRICT = 0b1000;
        /// check on close and writes and panic on error
        const STRONG = 0b1101;
        /// check everything and panic on error
        const PARANOID = 0b1111;
    }
}

pub(crate) struct DBInner {
    pub(crate) check_mode: CheckMode,
    pub(crate) no_sync: bool,
    pub(crate) no_grow_sync: bool,
    pub(crate) max_batch_size: usize,
    pub(crate) max_batch_delay: Duration,
    pub(crate) auto_remove: bool,
    pub(crate) alloc_size: u64,
    pub(crate) path: &'static str,
    pub(crate) file: RwLock<BufWriter<File>>,
    pub(crate) mmap_size: Mutex<usize>,
    pub(crate) mmap: RwLock<memmap::Mmap>,
    pub(crate) file_size: RwLock<u64>,
    pub(crate) page_size: usize,
    opened: AtomicBool,
    pub(crate) rw_lock: Mutex<()>,
    // Only one write transaction
    pub(crate) rw_tx: RwLock<Option<TX>>,
    pub(crate) txs: RwLock<Vec<TX>>,
    pub(crate) free_list: RwLock<FreeList>,
    pub(crate) stats: RwLock<Stats>,
    pub(crate) batch: Mutex<Option<Batch>>,
    pub(crate) page_pool: Mutex<Vec<Page>>,
    read_only: bool,
}
//
// /// `DB` represents a collection of buckets persisted to a file on disk.
// /// All data access is performed through transactions which can be obtained through the `DB`
// /// All the functions on `DB` will return a `DatabaseNotOpen` if accessed before Open() is called.
// #[derive(Default)]
// pub struct DB {
//     /// When enabled, the database will perform a Check() after every commit.
//     /// A panic is issued if the database is in a inconsistent state. This
//     /// flag has a large performance impact so it should only be used for
//     /// debugging purpose.
//     pub strict_mode: bool,
//
//     path: &'static str,
//     pub(crate) mmap: Option<Mmap>,
//     pub(crate) mmap_size: usize,
//     pub(crate) page_size: usize,
//     pub(crate) free_list: FreeList,
// }

#[derive(Clone)]
pub struct DB(pub(crate) Arc<DBInner>);

impl Default for DB {
    fn default() -> Self {
        todo!()
    }
}

impl<'a> DB {
    pub fn open<P: AsRef<Path>>(path: P, opt: Options) -> Result<DB> {
        let path = path.as_ref().to_owned();
        let file = {
            let mut open_opts = OpenOptions::new();
            open_opts.read(true);
            if !opt.read_only {
                open_opts.write(true).create(true);
            }
            open_opts.open(path.clone()).map_err(|err| Error::Io(Box::new(err)))?
        };
        DB::open_file(file, Some(path), opt)
    }

    pub fn open_file<P: Into<PathBuf>>(mut file: File, path: Option<P>, opt: Options) -> Result<Self> {
        let path = path.map(|v| v.into());
        let needs_initialization = (!std::path::Path::new(path.as_ref().unwrap()).exists())
            || file.metadata().map_err(|_| "Can't read metadata")?.len() == 0;

        if opt.read_only {
            file.lock_exclusive().map_err(|err| Unexpected("Cannot lock db file"))?;
        } else {
            file.lock_shared().map_err(|err| Unexpected("Cannt lock db file"))?;
        }

        let page_size = if needs_initialization {
            page_size::get()
        } else {
            let mut buf = vec![0u8; 1000];
            file.read_exact(&mut buf).map_err(|err| Error::Io(Box::new(err)))?;
            let page = Page::from_slice(&buf);
            if !page.is_meta() {
                return Err(Unexpected("Database format unknown"));
            }
            page.meta().page_size as usize
        };
        file.allocate(page_size as u64 * 4).map_err(|_| Unexpected("Cannot allocate 4 required pages"))?;
        // todo! why is page_size len?
        let mmap = unsafe {
            memmap::MmapOptions::new().offset(0).len(page_size).map(&file).map_err(|_| Unexpected("Failed to mmap"))?
        };
        let mut db: DB = DB(Arc::new(DBInner {
            check_mode: opt.check_mode,
            no_sync: false,
            no_grow_sync: opt.no_grow_sync,
            max_batch_size: opt.max_batch_size,
            max_batch_delay: opt.max_batch_delay,
            auto_remove: opt.auto_remove,
            alloc_size: 0,
            path: path.as_ref().unwrap(),
            file: RwLock::new(BufWriter::new(file)),
            mmap_size: Default::default(),
            mmap: RwLock::new(mmap),
            file_size: Default::default(),
            page_size,
            opened: AtomicBool::new(tre),
            rw_lock: Default::default(),
            rw_tx: Default::default(),
            txs: Default::default(),
            free_list: Default::default(),
            stats: Default::default(),
            batch: Default::default(),
            page_pool: Default::default(),
            read_only: opt.read_only,
        }));

        if needs_initialization {
            db.init()?;
        }
        db.mmap(opt.initial_mmap_size as u64)?;
        {
            let free_list_id = db.meta().free_list;
            db.mmap((free_list_id + 1) * db.0.page_size as u64)?;
            let freelist_page = db.page(free_list_id);
            db.0.free_list.try_write().unwrap().read(&freelist_page);
        }
        Ok(db)
    }

    /// Return the path to currently open database file.
    pub fn path(&self) -> &'static str {
        self.0.path
    }

    #[inline(always)]
    pub fn opened(&self) -> bool {
        self.0.opened.load(Ordering::Acquire)
    }

    pub(crate) fn remove_tx(&self, tx: &TX) -> Result<TX> {
        if tx.writable() {
            let (free_list_n, free_list_pending_n, free_list_alloc) = {
                let free_list = self.0.free_list.try_read().unwrap();
                (free_list.free_count(), free_list.pending_count(), free_list.size())
            };

            let tx = {
                // Only One write tx
                let mut db_tx = self.0.rw_tx.write();
                if db_tx.is_none() {
                    return Err(Unexpected("No write transaction exists"));
                }
                if !Arc::ptr_eq(&tx.0, &db_tx.as_ref().unwrap().0) {
                    return Err(Unexpected("Trying to remove unowned transaction"));
                }
                db_tx.take().unwrap()
            };

            unsafe {
                self.0.rw_lock.raw().unlock();
            }
            let mut stats = self.0.stats.write();
            stats.free_page_n = free_list_n;
            stats.pending_page_n = free_list_pending_n;
            stats.free_alloc = free_list_alloc;
            Ok(tx)
        } else {
            let mut txs = self.0.txs.try_write_for(Duration::from_secs(10)).unwrap();
            let index = txs.iter().position(|db_tx| Arc::ptr_eq(&tx.0, &db_tx.0));
            debug_assert!(index.is_some(), "trying to remove nonexistent tx");
            let index = index.unwrap();
            let tx = txs.remove(index);
            unsafe {
                // todo!
                self.0.mmap.raw().unlock_shared();
            }

            let mut stats = self.0.stats.write();
            stats.open_tx_n = txs.len();
            Ok(tx)
        }
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        self.0
            .file
            .write()
            .flush()
            .map_err(|_e| DBOpFailed(_e.to_string()))
    }

    pub fn stats(&self) -> Stats {
        self.0.stats.read().clone()
    }

    pub(crate) fn page_size(&self) -> usize {
        self.0.page_size
    }

    pub fn info(&self) -> Info {
        let ptr = self.0.mmap.read().as_ref()[0] as *const u8;
        Info {
            data: ptr,
            page_size: self.0.page_size as i64,
        }
    }

    /// Why use ?
    /// Retrieves page from mmap
    pub fn page(&self, id: PgId) -> MappedRwLockReadGuard<Page> {
        let page_size = self.0.page_size;
        let pos = id as usize * page_size as usize;
        let mmap = self.0.mmap.read_recursive();
        RwLockReadGuard::map(mmap, |mmap| Page::from_slice(&mmap.as_ref()[pos..]))
    }

    fn page_in_buffer<'b>(&'a self, buf: &'b mut [u8], id: PgId) -> &'b mut Page {
        let page_size = self.0.page_size as usize;
        let pos = id as usize * page_size;
        let end_pos = pos + page_size;
        Page::from_slice_mut(&mut buf[pos..end_pos])
    }

    /// Retrieves the current meta page reference
    pub fn meta(&self) -> Meta {
        // We have to return the meta with the highest txid which doesn't fail
        // validation. Otherwise, we can cause errors when in fact the database is
        // in a consistent state. Meta0 is the one with highest txid
        let (page_0, page_1) = (self.page(0), self.page(1));
        let (mut meta_0, mut meta_1) = (page_0.meta(), page_1.meta());
        if meta_1.tx_id > meta_0.tx_id {
            std::mem::swap(&mut meta_0, &mut meta_1);
        }

        // Use higher meta page if valid. Otherwise fallback to previous, if valid
        if meta_0.validate().is_ok() {
            return meta_0.clone();
        }
        // TODO: ?
        // Why meta1 can be returned?
        if meta_1.validate().is_ok() {
            return meta_1.clone();
        }

        // This should never be reached, because both meta1 and meta0 were validated
        // on mmap() and we do fsync() on every write.
        panic!("bolt.DB.meta(): invalid meta pages")
    }

    pub(crate) fn allocate(&mut self, count: u64, tx: &mut TX) -> Result<Page> {
        let mut p = if count == 1 {
            let mut page_pool = self.0.page_pool.lock();
            page_pool
                .pop()
                .or_else(|| {
                    Some(Page::from_slice(&Vec::with_capacity(self.0.page_size)).to_owned())
                })
                .unwrap()
        } else {
            Page::from_slice(&Vec::with_capacity(self.0.page_size * count as usize)).to_owned()
        };

        p.over_flow = count as u32 - 1;

        // Use pages from the freelist if they are available.
        {
            if let Some(free_list_pid) = self.0.free_list.write().allocate(count as usize) {
                p.id = free_list_pid;
                return Ok(p);
            }
        }

        p.id = tx.id();

        // Resize mmap() if we're at the end
        let minsz = (p.id + 1 + count) * self.0.page_size as u64;
        let mmap_size = *self.0.mmap_size.lock() as u64;
        if minsz >= mmap_size {
            if let Err(err) = self.mmap(minsz) {
                return Err(err);
            }
        }

        // Move the page id high water mark
        tx.set_pgid(tx.id() + count as u64)?;
        Ok(p)
    }

    fn mmap(&mut self, mut min_size: u64) -> Result<()> {
        let file = self
            .0
            .file
            .try_read_for(Duration::from_secs(60))
            .ok_or(Error::Unexpected("can't acquire file lock"))?;
        let mut mmap = self
            .0
            .mmap
            .try_write_for(Duration::from_secs(6000))
            .ok_or(Error::Unexpected("can't acquire mmap lock"))?;

        let init_min_size = self.0.page_size as u64 * 4;
        min_size = min_size.max(init_min_size);
        let mut size = self.mmap_size(min_size)?;

        if mmap.len() >= size as usize {
            return Ok(());
        }

        if self.0.read_only {
            let file_len = file
                .get_ref()
                .metadata()
                .map_err(|_| Error::Unexpected("can't get file metadata"))?
                .len();
            size = size.max(file_len);
        }

        let mut mmap_size = self.0.mmap_size.lock();
        file.get_ref()
            .allocate(size)
            .map_err(|_| Error::Unexpected("can't allocate space"))?;

        // TODO: madvise
        let mut mmap_opts = memmap::MmapOptions::new();
        let nmmap = unsafe {
            mmap_opts
                .offset(0)
                .len(size as usize)
                .map(&*file.get_ref())
                .map_err(|e| Error::Unexpected("mmap failed"))?
        };
        *mmap_size = nmmap.len();
        *mmap = nmmap;

        drop(file);
        drop(mmap);
        drop(mmap_size);

        let check_0 = self.page(0).meta().validate();
        let check_1 = self.page(1).meta().validate();

        // Validate the meta pages. We only return an error if both meta pages fail
        // validation, since meta_0 failing validation means that it wasn't saved
        // properly -- but we can recover using meta_1. And vice-versa.
        if check_0.is_err() && check_1.is_err() {
            return Err(Error::InvalidChecksum(format!(
                "mmap fail: {:?}",
                check_0.unwrap_err(),
            )));
        }

        Ok(())
    }

    fn init(&mut self) -> Result<()> {
        // allocate 4 page, meta0, meta1, freelist, root-page
        let mut buf = vec![0u8; self.0.page_size * 4];
        (0..=1).for_each(|i| {
            let mut p = self.page_in_buffer(&mut buf, i);
            p.id = i;
            p.flags = META_PAGE_FLAG;
            let m = p.meta_mut();
            m.magic = MAGIC;
            m.version = VERSION as u32;
            m.page_size = self.0.page_size as u32;
            m.free_list = 2;
            m.root = SubBucket { root: 3, sequence: 0 };
            m.pg_id = 4;
            m.tx_id = i;
            m.check_sum = m.sum64();
        });

        let mut page = self.page_in_buffer(&mut buf, 2);
        page.id = 2;
        page.flags = FREE_LIST_PAGE_FLAG;

        let mut page = self.page_in_buffer(&mut buf, 3);
        page.id = 3;
        page.flags = LEAF_PAGE_FLAG;
        self.write_at(0, std::io::Cursor::new(&mut buf));
        self.sync()?;
        Ok(())
    }

    fn write_at<T: Read>(&mut self, pos: u64, mut buf: T) -> Result<()> {
        let mut file = self.0.file.write();
        file.seek(SeekFrom::Start(pos)).map_err(|_| Unexpected("Can't seek to position"))?;
        std::io::copy(&mut buf, &mut *file).map_err(|_| Unexpected("Can't write buffer"))?;
        Ok(())
    }

    fn mmap_size(&self, mut size: u64) -> Result<u64> {
        // Double the size from 32KB until 1GB
        for i in 15..=30 {
            if size <= 1 << i {
                return Ok(1 << i);
            }
        }

        // Verify the requested size is not above the maximum allowed.
        if size > MAX_MMAP_STEP {
            return Err(Error::Unexpected("mmap too large"));
        }

        // If larger than the 1GB then grow by 1GB at a time.
        let remaining = size % MAX_MMAP_STEP;
        if remaining > 0 {
            size += MAX_MMAP_STEP - remaining;
        }

        // Ensure that the mmap size is a multiple of the page size.
        // This should always be true since we're incrementing in MBs.
        let page_size = self.0.page_size as u64;
        if size % page_size != 0 {
            size = (size / page_size + 1) * page_size;
        }

        // If we've exceeded the max size then only grow up to the max size.
        if size > MAX_MMAP_STEP {
            size = MAX_MMAP_STEP;
        }
        Ok(size)
    }

    pub(crate) fn grow(&mut self, mut size: u64) -> Result<()> {
        if self.0.read_only {
            return Err(Error::DatabaseOnlyRead);
        }
        let file = self.0.file.try_write().unwrap();
        if file.get_ref().metadata().unwrap().len() >= size as u64 {
            return Ok(());
        }

        // If the data is smaller than the alloc size then only allocate what's needed.
        // Once it goes over the allocation size then allocate in chunks.
        {
            let mmapsize = self.0.mmap.try_read().unwrap().as_ref().len() as u64;
            if mmapsize < self.0.alloc_size {
                size = mmapsize;
            } else {
                size += self.0.alloc_size;
            }
        }

        file.get_ref()
            .allocate(size)
            .map_err(|_e| Error::ResizeFail)?;

        if !self.0.no_grow_sync {
            file.get_ref()
                .sync_all()
                .map_err(|_| Error::Unexpected("can't flush file"))?;
        }
        *self.0.file_size.write() = file
            .get_ref()
            .metadata()
            .map_err(|_| Error::Unexpected("can't get metadata file"))?
            .len();
        Ok(())
    }

    fn cleanup(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        let strong_count = Arc::strong_count(&self.0);
        if strong_count > 1 {
            debug!("db strong ref count {}", strong_count);
            return;
        }
        self.cleanup().unwrap();
    }
}

#[derive(Clone)]
pub(crate) struct WeakDB(Weak<DBInner>);

impl WeakDB {
    pub(crate) fn new() -> WeakDB {
        WeakDB(Weak::new())
    }

    pub(crate) fn upgrade(&self) -> Option<DB> {
        self.0.upgrade().map(DB)
    }

    pub(crate) fn from(db: &DB) -> WeakDB {
        WeakDB(Arc::downgrade(&db.0))
    }
}

#[derive(Debug, Default, Clone)]
pub struct Meta {
    magic: u32,
    version: u32,
    page_size: u32,
    flags: u32,
    pub(crate) root: SubBucket,
    free_list: PgId,
    // pg_id high watermark
    pub(crate) pg_id: PgId,
    pub(crate) tx_id: TxId,
    check_sum: u64,
}

impl Meta {
    pub fn validate(&self) -> Result<()> {
        if self.magic != MAGIC {
            return Err(Error::Invalid);
        } else if self.version != VERSION as u32 {
            return Err(Error::VersionMismatch);
        } else if self.check_sum != 0 && self.check_sum != self.sum64() {
            return Err(Error::InvalidChecksum("".to_owned()));
        }
        Ok(())
    }

    // writes the meta onto a page
    pub fn write(&mut self, p: &mut Page) -> Result<()> {
        if self.root.root >= self.pg_id {
            panic!(format!(
                "root bucket pgid({}) above high water mark ({})",
                self.root.root, self.pg_id
            ));
        } else if self.free_list >= self.pg_id {
            panic!(format!(
                "freelist pgid({}) above high water mark ({})",
                self.free_list, self.pg_id
            ));
        }
        // Page id is either going to be 0 or 1 which we can determine by the transaction ID.
        p.id = self.tx_id % 2;
        p.flags |= META_PAGE_FLAG;

        // Calculate the checksum
        self.check_sum = self.sum64();
        p.copy_from_meta(self);
        Ok(())
    }

    pub fn sum64(&self) -> u64 {
        let mut h = FnvHasher::default();
        h.write(self.as_slice());
        h.finish()
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        let ptr = self as *const Meta as *const u8;
        unsafe { from_raw_parts(ptr, self.byte_size()) }
    }

    fn byte_size(&self) -> usize {
        META_PAGE_SIZE
    }
}

// Represents statistics about the database.
#[derive(Clone, Default)]
pub struct Stats {
    // FreeList stats.
    // total number of free pages on the freelist.
    pending_page_n: usize,
    // total number of pending pages on the freelist.
    free_alloc: usize,
    // total bytes allocated in free pages.
    free_list_inuse: u64,
    // total bytes used by the freelist.
    free_page_n: usize,
    // Transaction stats
    // total number of started read transactions.
    tx_n: u64,
    // number of currently open read transactions.
    open_tx_n: usize,
}

/// Transaction statistics
#[derive(Clone, Debug, Default)]
pub struct TxStats {
    // Page statistics
    /// number of page allocations
    pub page_count: usize,
    /// total bytes allocated
    pub page_alloc: usize,

    // Cursor statistics
    /// number of cursor created
    pub cursor_count: usize,

    // Node statistics
    /// number of node allocations
    pub node_count: usize,
    /// number of node dereferences.
    pub node_deref: usize,

    // Rebalance statistics
    /// number of node rebalances
    pub rebalance: usize,
    /// total time spent rebalancing
    pub rebalance_time: Duration,

    // Split/Spill statistics
    /// number of nodes split
    pub split: usize,
    /// number of nodes spilled
    pub spill: usize,
    /// total time spent spilling
    pub spill_time: Duration,

    // Write statistics
    /// number of writes performed
    pub write: usize,
    /// total time spent write to disk
    pub write_time: Duration,
}

impl TxStats {
    /// returns diff stats
    pub fn sub(&self, other: &TxStats) -> TxStats {
        TxStats {
            page_count: self.page_count - other.page_count,
            page_alloc: self.page_alloc - other.page_alloc,
            cursor_count: self.cursor_count - other.cursor_count,
            node_count: self.node_count - other.node_count,
            node_deref: self.node_deref - other.node_deref,
            rebalance: self.rebalance - other.rebalance,
            rebalance_time: self.rebalance_time - other.rebalance_time,
            split: self.split - other.split,
            spill: self.spill - other.spill,
            spill_time: self.spill_time - other.spill_time,
            write: self.write - other.write,
            write_time: self.write_time - other.write_time,
        }
    }
}

impl AddAssign for TxStats {
    fn add_assign(&mut self, rhs: Self) {
        self.page_count += rhs.page_count;
        self.page_alloc += rhs.page_alloc;
        self.cursor_count += rhs.cursor_count;
        self.node_count += rhs.node_count;
        self.node_deref += rhs.node_deref;
        self.rebalance += rhs.rebalance;
        self.rebalance_time += rhs.rebalance_time;
        self.split += rhs.split;
        self.spill += rhs.spill;
        self.spill_time += rhs.spill_time;
        self.write += rhs.write;
        self.write_time += rhs.write_time;
    }
}

struct BatchInner {
    db: WeakDB,
    calls_len: usize,
    ran: AtomicBool,
    pub(super) calls: Mutex<Vec<Call>>,
}

#[derive(Clone)]
pub(crate) struct Batch(Arc<BatchInner>);

unsafe impl Send for Batch {}

unsafe impl Sync for Batch {}

impl Batch {
    pub(super) fn new(db: WeakDB, calls_len: usize, delay: Duration) -> Self {
        let batch = Batch(Arc::new(BatchInner {
            db,
            calls_len,
            ran: AtomicBool::new(false),
            calls: Mutex::new(Vec::new()),
        }));
        let mut bc = batch.clone();
        spawn(move || {
            sleep(delay);
        });
        bc
    }

    /// trigger runs the batch if it has not already been run.
    pub(super) fn trigger(&mut self) {
        if let Ok(false) = self.0.ran.compare_exchange(false, true, SeqCst, SeqCst) {
            self.run();
        }
    }

    /// run performs the transactions in the batch and communicates results.
    /// back to DB.Batch
    fn run(&mut self) {}
}

pub(super) struct Call {}

pub struct Info {
    /// pointer to data
    pub data: *const u8,
    pub page_size: i64,
}

pub struct DBBuilder {
    path: PathBuf,
    no_grow_sync: bool,
    read_only: bool,
    initial_mmap_size: usize,
    auto_remove: bool,
    check_mode: CheckMode,
    max_batch_delay: Duration,
    max_batch_size: usize,
}

impl DBBuilder {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            no_grow_sync: false,
            read_only: false,
            initial_mmap_size: 0,
            auto_remove: false,
            check_mode: CheckMode::NO,
            max_batch_delay: DEFAULT_MAX_BATCH_DELAY,
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
        }
    }

    pub fn set_path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = path.as_ref().to_owned();
        self
    }

    pub fn set_no_grow_sync(mut self, no_grow_sync: bool) -> Self {
        self.no_grow_sync = no_grow_sync;
        Self
    }

    pub fn set_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Initial mmap size of the database
    ///
    /// in bytes. Read transactions won't block write transaction
    ///
    /// If = 0, the initial map size is size of first 4 pages.
    /// If initial_mmap_size is smaller than the previous database size,
    /// it takes no effect.
    ///
    /// Default: 0 (mmap will be equal to 4 page sizes)
    pub fn set_initial_mmap_size(mut self, mmap_size: usize) -> Self {
        self.initial_mmap_size = mmap_size;
        self
    }

    /// Defines wether db file will be removed after db close
    ///
    /// Default: false
    pub fn set_auto_remove(mut self, auto_remove: bool) -> Self {
        self.auto_remove = auto_remove;
        self
    }

    pub fn set_check_mode(mut self, check_mode: CheckMode) -> Self {
        self.check_mode = check_mode;
        self
    }

    pub fn set_batch_delay(mut self, batch_delay: Duration) -> Self {
        self.max_batch_delay = batch_delay;
        self
    }

    pub fn set_batch_size(mut self, batch_size: usize) -> Self {
        self.max_batch_size = batch_size;
        self
    }

    pub fn build(self) -> Result<DB> {
        let opt = Options {
            no_grow_sync: self.no_grow_sync,
            read_only: self.read_only,
            initial_mmap_size: self.initial_mmap_size,
            auto_remove: self.auto_remove,
            check_mode: self.check_mode,
            max_batch_delay: self.max_batch_delay,
            max_batch_size: self.max_batch_size,
        };
        DB::open(self.path, opt)
    }
}

struct Options {
    no_grow_sync: bool,
    read_only: bool,
    initial_mmap_size: usize,
    auto_remove: bool,
    check_mode: CheckMode,
    max_batch_delay: Duration,
    max_batch_size: usize,
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
