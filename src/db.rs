use crate::bucket::{TopBucket, DEFAULT_FILL_PERCENT, MAX_FILL_PERCENT, MIN_FILL_PERCENT};
use crate::error::Error::{DatabaseNotOpen, DatabaseOnlyRead, TrySolo, Unexpected, Unexpected2};
use crate::error::Result;
use crate::error::{is_valid_error, Error};
use crate::free_list::FreeList;
use crate::page::{OwnedPage, Page, PageFlag, MIN_KEYS_PER_PAGE};
use crate::page::{PgId, META_PAGE_SIZE};
use crate::test_util::temp_file;
use crate::tx::{RWTxGuard, TxBuilder, TxGuard, TxStats, TX};
use crate::Error::Io;
use crate::TxId;
use bitflags::bitflags;
use fnv::FnvHasher;
use fs2::FileExt;
use log::{debug, error, info};
use parking_lot::lock_api::{RawMutex, RawRwLock};
use parking_lot::MappedRwLockReadGuard;
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use std::fmt::{Display, Formatter};
use std::fs::{File, OpenOptions};
use std::hash::Hasher;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::slice::from_raw_parts;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::{mpsc, Arc, Weak};
use std::thread::{sleep, spawn};
use std::time::Duration;

/// The largest step that can be token when remapping the mman.
pub const MAX_MMAP_STEP: u64 = 1 << 30; //1GB

/// The data file format version.
pub const VERSION: usize = 2;

/// Represents a marker value to indicate that a file is a Bolt `DB`.
pub const MAGIC: u32 = 0xED0CDAED;

/// Default values if not set in a `DB` instance.
const DEFAULT_MAX_BATCH_SIZE: usize = 1000;
const DEFAULT_MAX_BATCH_DELAY: Duration = Duration::from_millis(10);

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
    pub(crate) path: Option<PathBuf>,
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
    pub(crate) page_pool: Mutex<Vec<OwnedPage>>,
    read_only: bool,
}

impl Display for DBInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("config detail: check_mode:{}, no_sync:{}, no_grow_sync:{}, max_batch_size:{}, max_batch_delay:{:?}, auto_remove: {:?}, allow_size:{}, page_size:{}, read_only:{}, MIN_KEYS_PER_PAGE:{}, MIN_FILL_PERCENT:{}, MAX_FILL_PERCENT:{}, DEFAULT_FILL_PERCENT:{}",
                                 self.check_mode.bits(), self.no_sync, self.no_grow_sync, self.max_batch_size, self.max_batch_delay, self.auto_remove, self.alloc_size, self.page_size, self.read_only, MIN_KEYS_PER_PAGE, MIN_FILL_PERCENT, MAX_FILL_PERCENT, DEFAULT_FILL_PERCENT))
    }
}

#[derive(Clone)]
pub struct DB(pub(crate) Arc<DBInner>);

impl Default for DB {
    fn default() -> Self {
        todo!()
    }
}

impl DB {
    pub fn open<P: AsRef<Path>>(path: P, opt: Options) -> Result<DB> {
        let path = path.as_ref().to_owned();
        let fp = OpenOptions::new()
            .read(true)
            .write(!opt.read_only)
            .create(!opt.read_only)
            .open(path.clone())?;
        DB::open_file(fp, Some(path), opt)
    }

    pub fn open_file<P: Into<PathBuf>>(
        mut file: File,
        path: Option<P>,
        opt: Options,
    ) -> Result<Self> {
        let path = path.map(|v| v.into());
        let needs_initialization = (!std::path::Path::new(path.as_ref().unwrap()).exists())
            || file.metadata().map_err(|_| "Can't read metadata")?.len() == 0;
        if needs_initialization {
            info!("need to initialize a new db");
        }

        // Lock file that other process using Bolt in read-write mode cannot
        // use the database at the same time. This would cause corruption since
        // the two process would write metadata pages and free pages separately.
        // The database file is locked exclusively (only one process can grab the lock)
        // if !opt.read_only.
        // The database file is locked using the shared lock (more than one process may
        // hold a lock at the same time) otherwise (opt.read_only is set).
        if !opt.read_only {
            file.lock_exclusive()
                .map_err(|_| Unexpected("Cannot lock db file"))?;
        } else {
            file.lock_shared()
                .map_err(|_| Unexpected("Can't lock db file"))?;
        }

        let page_size = if needs_initialization {
            page_size::get()
        } else {
            let mut buf = vec![0u8; 1000];
            file.read_exact(&mut buf)?;
            let page = Page::from_slice(&buf);
            if !page.is_meta() {
                return Err(Unexpected("Database format unknown"));
            }
            page.meta().page_size as usize
        };
        // initialize the database if it doesn't exist.
        if let Err(err) = file.allocate(page_size as u64 * 4) {
            if !is_valid_error(&err) {
                return Err(Unexpected2(format!(
                    "Cannot allocate 4 required pages, error: {}",
                    err.to_string()
                )));
            }
        }
        let mmap = unsafe {
            memmap::MmapOptions::new()
                .offset(0)
                .len(page_size)
                .map(&file)
                .map_err(|_| Unexpected("Failed to mmap"))?
        };
        let mut db: DB = DB(Arc::new(DBInner {
            check_mode: opt.check_mode,
            no_sync: opt.no_sync,
            no_grow_sync: opt.no_grow_sync,
            max_batch_size: opt.max_batch_size,
            max_batch_delay: opt.max_batch_delay,
            auto_remove: opt.auto_remove,
            alloc_size: 0,
            path,
            file: RwLock::new(BufWriter::new(file)),
            mmap_size: Default::default(),
            mmap: RwLock::new(mmap),
            file_size: Default::default(),
            page_size,
            opened: AtomicBool::new(true),
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
        info!("database config: {}", db.0);
        Ok(db)
    }

    /// Return the path to currently open database file.
    pub fn path(&self) -> Option<PathBuf> {
        self.0.path.clone()
    }

    #[inline(always)]
    pub fn opened(&self) -> bool {
        self.0.opened.load(Ordering::Acquire)
    }

    #[inline(always)]
    pub fn read_only(&self) -> bool {
        self.0.read_only
    }

    /// Starts a new transaction.
    /// Multiple read-only transactions can be used concurrently but only one
    /// write transaction can be used at a time.
    ///
    /// Transaction should be not dependent on one another.
    ///
    /// If a long running read transaction (for example, a snapshot transaction) is
    /// needed, you might want to set DBBuilder.initial_mmap_size to a large enough value
    /// to avoid potential blocking of write transaction.
    pub fn begin_tx(&self) -> Result<TxGuard> {
        if !self.opened() {
            return Err(Error::DatabaseNotOpen);
        }
        unsafe {
            self.0.mmap.raw().lock_shared();
        }

        let tx = TxBuilder::new()
            .set_db(WeakDB::from(self))
            .set_writable(false)
            .set_check(self.0.check_mode.contains(CheckMode::READ))
            .build();

        let txs_len = {
            let mut txs = self.0.txs.write();
            txs.push(tx.clone());
            txs.len()
            // free txs lock
        };

        {
            let mut stats = self.0.stats.write();
            stats.tx_n += 1;
            stats.open_tx_n = txs_len;
        }
        debug!("start a read only transaction, tid:{}", tx.id());
        Ok(TxGuard {
            tx,
            db: std::marker::PhantomData,
        })
    }

    /// Starts a new writable transaction.
    /// Multiple read-only transactions can be used concurrently but only one
    /// write transaction can be used at a time.
    ///
    /// Transaction should not be dependent on one another.
    ///
    /// If a long-running read transaction (for example, a snapshot transaction) is
    /// needed, you might want to set DBBuilder.init_mmap_size to a large enough value
    /// to avoid potential blocking of write transaction.
    pub fn begin_rw_tx(&self) -> Result<RWTxGuard> {
        if self.read_only() {
            return Err(DatabaseOnlyRead);
        };
        if !self.opened() {
            return Err(DatabaseNotOpen);
        };

        unsafe {
            self.0.rw_lock.raw().lock();
        };
        let mut rw_tx = self.0.rw_tx.write();

        // Free any pages associated with closed read-only transactions.
        let txs = self.0.txs.read();
        let minid = txs
            .iter()
            .map(|tx| tx.id())
            .min()
            .unwrap_or(0xFFFF_FFFF_FFFF_FFFF);
        drop(txs);

        let tx = TxBuilder::new()
            .set_db(WeakDB::from(self))
            .set_writable(true)
            .set_check(self.0.check_mode.contains(CheckMode::WRITE))
            .build();
        *rw_tx = Some(tx.clone());
        drop(rw_tx);

        // Free any pages associated with closed read-only transactions.
        if minid > 0 {
            self.0.free_list.try_write().unwrap().release(minid - 1);
        }

        Ok(RWTxGuard {
            tx,
            db: std::marker::PhantomData,
        })
    }

    /// shorthand for db.begin_rw_tx with addditional guagrantee for panic safery
    pub fn update<'b>(&self, mut handler: impl FnMut(&mut TX) -> Result<()> + 'b) -> Result<()> {
        use std::panic::{catch_unwind, AssertUnwindSafe};
        let mut tx = scopeguard::guard(self.begin_rw_tx()?, |tx| {
            let db_exists = tx.db().is_ok();
            if db_exists {
                tx.__rollback().unwrap();
            }
        });
        let result = catch_unwind(AssertUnwindSafe(|| {
            tx.0.managed.store(true, Ordering::Release);
            let result = handler(&mut tx);
            tx.0.managed.store(false, Ordering::Release);
            result
        }));

        if result.is_err() {
            tx.__rollback()?;
            return Err("Panic while update".into());
        }

        let result = result.unwrap();
        if let Err(err) = result {
            tx.rollback()?;
            return Err(err);
        }

        tx.commit()
    }

    /// shorthand for db.begin_tx with additional guarantee for panic safery
    pub fn view<'b>(&self, handler: impl Fn(&TX) -> Result<()> + 'b) -> Result<()> {
        use std::panic::{catch_unwind, AssertUnwindSafe};

        let tx = scopeguard::guard(self.begin_tx()?, |tx| {
            if tx.db().is_ok() {
                tx.__rollback().unwrap();
            }
        });

        let result = catch_unwind(AssertUnwindSafe(|| {
            tx.0.managed.store(true, Ordering::Release);
            let result = handler(&tx);
            tx.0.managed.store(false, Ordering::Release);
            result
        }));

        if result.is_err() {
            tx.__rollback()?;
            return Err(Error::Unexpected("Panic while update"));
        }

        let result = result.unwrap();
        if let Err(err) = result {
            tx.rollback()?;
            return Err(err);
        }
        Ok(())
    }

    /// Calls fn as part of a batch. It behaves similar to Update,
    /// except:
    ///
    /// 1. concurrent batch calls can be combined into a single
    /// transaction.
    ///
    /// 2. the function passed to batch may be called multiple times,
    /// regardless of whether it returns error or not.
    ///
    /// This means that Batch function side effects must be idempotent and
    /// take permanent effect only after a successful return is seen in
    /// caller.
    ///
    /// The maximum batch size and delay can be adjusted with DBBuilder.batch_size
    /// and DBBuilder.batch_delay, respectively.
    ///
    /// Batch is only useful when there are multiple threads calling it.
    /// While calling it multiple times from single thread just blocks
    /// thread for each single batch call
    pub fn batch(&self, handler: Box<dyn Fn(&mut TX) -> Result<()>>) -> Result<()> {
        let weak_db = WeakDB::from(self);
        let handler = Arc::new(handler);
        let handler_clone = handler.clone();
        let err_recv = {
            let mut batch = self.0.batch.lock();
            if batch.is_none() || batch.as_ref().unwrap().closed() {
                *batch = Some(Batch::new(
                    weak_db,
                    self.0.max_batch_size,
                    self.0.max_batch_delay,
                ));
            }

            let batch = batch.as_mut().unwrap();
            let (err_sender, err_recv) = mpsc::sync_channel(1);
            batch.push(Call::new(handler_clone, err_sender))?;
            err_recv
        };

        if let Err(err) = err_recv.recv().unwrap() {
            error!("failed to execute batch handler, err: {:?}", err);
            return self.update(|tx| handler(tx));
        }

        Ok(())
    }

    /// Returns stats on a bucket.
    pub fn stats(&self) -> Stats {
        self.0.stats.read().clone()
    }

    pub fn page_size(&self) -> usize {
        self.0.page_size
    }

    pub fn info(&self) -> Info {
        let ptr = self.0.mmap.read().as_ref()[0] as *const u8;
        Info {
            data: ptr,
            page_size: self.0.page_size as i64,
        }
    }

    /// Retrieves page from mmap
    pub fn page(&self, id: PgId) -> MappedRwLockReadGuard<Page> {
        let page_size = self.0.page_size;
        let pos = id as usize * page_size as usize;
        let mmap = self.0.mmap.read_recursive();
        RwLockReadGuard::map(mmap, |mmap| Page::from_slice(&mmap.as_ref()[pos..]))
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
        if meta_1.validate().is_ok() {
            return meta_1.clone();
        }

        debug!("meta_0: {:?}, meta_1: {:?}", meta_0, meta_1);
        // This should never be reached, because both meta1 and meta0 were validated
        // on mmap() and we do fsync() on every write.
        panic!("invalid meta pages")
    }
}

impl<'a> DB {
    pub(crate) fn must_check(&self) {
        let err = self.update(|tx| {
            let mut errs = vec![];
            while let Ok(err) = tx.check().recv() {
                errs.push(err);
                if errs.len() > 10 {
                    break;
                }
            }
            if !errs.is_empty() {
                let path = temp_file();
                let mut mode = OpenOptions::new();
                mode.write(true);
                if let Err(err) = tx.copy_to(path.to_str().unwrap(), mode) {
                    panic!(err);
                }

                info!("consistency check failed ({} errors)", errs.len());
                for _ in errs {
                    info!("");
                    info!("db save to:");
                    exit(-1);
                }
            }
            Ok(())
        });

        if err.is_err() {
            panic!("{:?}", err);
        }
    }

    pub(crate) fn remove_tx(&self, tx: &TX) -> Result<TX> {
        if tx.writable() {
            let (free_list_n, free_list_pending_n, free_list_alloc) = {
                let free_list = self.0.free_list.try_read().unwrap();
                (
                    free_list.free_count(),
                    free_list.pending_count(),
                    free_list.size(),
                )
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
            stats.tx_stats += tx.stats().clone();
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
            stats.tx_stats += tx.stats().clone();
            Ok(tx)
        }
    }

    pub(crate) fn sync(&self) -> Result<()> {
        self.0.file.write().flush().map_err(|_e| Io(_e.to_string()))
    }

    fn page_in_buffer<'b>(&'a self, buf: &'b mut [u8], id: PgId) -> &'b mut Page {
        let page_size = self.0.page_size as usize;
        let pos = id as usize * page_size;
        let end_pos = pos + page_size;
        Page::from_slice_mut(&mut buf[pos..end_pos])
    }

    pub(crate) fn allocate(&mut self, count: u64, tx: &mut TX) -> Result<OwnedPage> {
        let mut p = OwnedPage::new(self.0.page_size * count as usize);

        p.over_flow = count as u32 - 1;

        // Use pages from the freelist if they are available.
        {
            if let Some(free_list_pid) = self.0.free_list.write().allocate(count as usize) {
                p.id = free_list_pid;
                debug!(
                    "allocate memory from free cache, count: {}, tx: {}",
                    count,
                    tx.id()
                );
                return Ok(p);
            }
        }
        p.id = tx.pgid();

        // Resize mmap() if we're at the end
        let minsz = (p.id + 1 + count) * self.0.page_size as u64;
        let mmap_size = *self.0.mmap_size.lock() as u64;
        if minsz >= mmap_size {
            if let Err(err) = self.mmap(minsz) {
                return Err(err);
            }
        }

        // Move the page id high watermark
        tx.set_pgid(tx.pgid() + count as u64)?;
        debug!(
            "allocate new memory, pid:{}, count: {}, tx: {}",
            p.id,
            count,
            tx.id()
        );
        Ok(p)
    }

    /// mmap opens that underlying memory-mapped file and initialize tha meta references.
    /// min_size is the minimum size that the new mmap can be.
    fn mmap(&mut self, mut min_size: u64) -> Result<()> {
        let file = self
            .0
            .file
            .try_read_for(Duration::from_secs(60))
            .ok_or(Error::Unexpected("can't acquire file lock"))?;
        let mut mmap = self
            .0
            .mmap
            .try_write_for(Duration::from_secs(60))
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
            size = size.min(file_len);
        }

        let mut mmap_size = self.0.mmap_size.lock();
        if let Err(err) = file.get_ref().allocate(size) {
            if !is_valid_error(&err) {
                return Err(Error::Io(err.to_string()));
            }
        }

        // TODO: madvise
        let mut mmap_opts = memmap::MmapOptions::new();
        let nmmap = unsafe {
            mmap_opts
                .offset(0)
                .len(size as usize)
                .map(file.get_ref())
                .map_err(|err| Error::Io(err.to_string()))?
        };
        *mmap_size = nmmap.len();
        *mmap = nmmap;

        debug!(
            "succeed to reset mmap, size: {}, mmap size: {}",
            size, mmap_size
        );
        drop(file);
        drop(mmap);
        drop(mmap_size);

        let check_0 = self.page(0).meta().validate();
        let check_1 = self.page(1).meta().validate();

        // Validate the meta pages. We only return an error if both meta pages fail
        // validation, since meta_0 failing validation means that it wasn't saved
        // properly -- but we can recover using meta_1. And vice-versa.
        if check_0.is_err() && check_1.is_err() {
            return Err(check_0.unwrap_err());
        }

        Ok(())
    }

    fn init(&mut self) -> Result<()> {
        debug!("ready to init a new db");
        // allocate 4 page, meta0, meta1, freelist, root-page
        let mut buf = vec![0u8; self.0.page_size * 4];
        // init two meta page.
        (0..=1).for_each(|i| {
            let mut p = self.page_in_buffer(&mut buf, i);
            p.id = i;
            p.flags = PageFlag::Meta;
            let m = p.meta_mut();
            m.magic = MAGIC;
            m.version = VERSION as u32;
            m.page_size = self.0.page_size as u32;
            m.free_list = 2;
            m.root = TopBucket {
                root: 3,
                sequence: 0,
            };
            m.pg_id = 4;
            m.tx_id = i;
            m.check_sum = m.sum64();
        });
        debug!("succeed to init meta pages");
        // init free list page.
        let mut page = self.page_in_buffer(&mut buf, 2);
        page.id = 2;
        page.flags = PageFlag::FreeList;
        debug!("succeed to init free list page");
        // init root page
        let mut page = self.page_in_buffer(&mut buf, 3);
        page.id = 3;
        page.flags = PageFlag::Leaf;
        debug!("succeed to init root page");
        self.write_at(0, std::io::Cursor::new(&mut buf))?;
        self.sync()?;
        debug!("succeed to init db, waker pgid: {:?}, top-pgid: {}", 4, 3);
        Ok(())
    }

    pub(crate) fn write_at<T: Read>(&mut self, pos: u64, mut buf: T) -> Result<()> {
        defer_lite::defer! {log::debug!("succeed to write db disk, pos: {}", pos);}
        let mut file = self.0.file.write();
        file.seek(SeekFrom::Start(pos))
            .map_err(|_| Unexpected("Can't seek to position"))?;
        std::io::copy(&mut buf, &mut *file).map_err(|_| Unexpected("Can't write buffer"))?;
        Ok(())
    }

    /// mmap_size determines the appropriate size for the mmap given the current size
    /// of the database. The minimum size is 32KB and doubles until it reaches 1GB.
    /// Returns an error if the new mmap size is greater than then max allowed.
    fn mmap_size(&self, mut size: u64) -> Result<u64> {
        // Double the size from 32KB until 1GB
        for i in 15..=30 {
            if size <= 1 << i {
                return Ok(1 << i);
            }
        }

        // Verify the requested size is not above the maximum allowed.
        if size > crate::os::MAX_MMAP_SIZE {
            return Err("mmap too large".into());
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
        if size > crate::os::MAX_MMAP_SIZE {
            size = crate::os::MAX_MMAP_SIZE;
        }
        Ok(size)
    }

    pub(crate) fn grow(&mut self, mut size: u64) -> Result<()> {
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
        if !self.0.opened.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.0.check_mode.contains(CheckMode::CLOSE) {
            let strict = self.0.check_mode.contains(CheckMode::STRICT);
            let tx = self.begin_tx()?;
            if let Err(e) = tx.check_sync() {
                if strict {
                    return Err(e);
                }
            }
        }
        self.0.opened.store(false, Ordering::Release);
        self.0
            .file
            .try_read()
            .ok_or(Unexpected("Can't acquire file lock"))?
            .get_ref()
            .unlock()
            .map_err(|_| Unexpected("Can't acquire file lock"))?;
        if self.0.auto_remove {
            if let Some(path) = &self.0.path {
                if path.exists() {
                    std::fs::remove_file(path).map_err(|_| Unexpected("Can't remove file"))?;
                }
            }
        }
        Ok(())
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        let strong_count = Arc::strong_count(&self.0);
        if strong_count > 1 {
            // debug!("db strong ref count {}", strong_count);
            return;
        }

        debug!("drop db");
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
#[repr(C)]
pub struct Meta {
    /// database mime header
    pub(crate) magic: u32,
    /// database version
    pub(crate) version: u32,
    /// defined page size.
    /// u32 to be platform independent
    page_size: u32,
    /// haven't seen it's usage
    flags: u32,
    /// bucket that has root property changed
    /// during commits and transactions
    pub(crate) root: TopBucket,
    /// free list page id
    pub(crate) free_list: PgId,
    /// pg_id high watermark
    pub(crate) pg_id: PgId,
    /// transaction id
    pub(crate) tx_id: TxId,
    /// meta check_sum
    pub(crate) check_sum: u64,
}

impl Meta {
    pub fn validate(&self) -> Result<()> {
        if self.magic != MAGIC {
            return Err(Error::Invalid);
        } else if self.version != VERSION as u32 {
            return Err(Error::VersionMismatch);
        } else if self.check_sum != 0 && self.check_sum != self.sum64() {
            return Err(Error::Checksum);
        }
        Ok(())
    }

    // writes the meta onto a page
    pub fn write(&mut self, p: &mut Page) -> Result<()> {
        if self.root.root >= self.pg_id {
            panic!(
                "root bucket pgid({}) above high water mark ({})",
                self.root.root, self.pg_id
            );
        } else if self.free_list >= self.pg_id {
            panic!(
                "freelist pgid({}) above high water mark ({})",
                self.free_list, self.pg_id
            );
        }
        // Page id is either going to be 0 or 1 which we can determine by the transaction ID.
        p.id = self.tx_id % 2;
        // Calculate the checksum
        self.check_sum = self.sum64();
        p.copy_from_meta(self);
        Ok(())
    }

    pub fn sum64(&self) -> u64 {
        let mut h = FnvHasher::default();
        h.write(self.as_slice_no_checksum());
        h.finish()
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        let ptr = self as *const Meta as *const u8;
        unsafe { from_raw_parts(ptr, self.byte_size()) }
    }

    #[inline]
    pub(crate) fn as_slice_no_checksum(&self) -> &[u8] {
        let ptr = self as *const Meta as *const u8;
        unsafe { from_raw_parts(ptr, memoffset::offset_of!(Meta, check_sum)) }
    }

    fn byte_size(&self) -> usize {
        META_PAGE_SIZE
    }
}

// Represents statistics about the database.
#[derive(Clone, Default)]
pub struct Stats {
    // FreeList stats.
    /// total number of free pages on the freelist.
    pub pending_page_n: usize,
    /// total number of pending pages on the freelist.
    pub free_alloc: usize,
    /// total bytes allocated in free pages.
    pub free_list_inuse: u64,
    /// total bytes used by the freelist.
    pub free_page_n: usize,
    // Transaction stats
    /// total number of started read transactions.
    pub tx_n: u64,
    /// number of currently open read transactions.
    pub open_tx_n: usize,
    /// global, ongoing stats.
    pub tx_stats: TxStats,
}

struct BatchInner {
    db: WeakDB,
    max_batch_size: usize,
    ran: AtomicBool,
    pub(super) calls: Mutex<Vec<Call>>,
}

#[derive(Clone)]
pub(crate) struct Batch(Arc<BatchInner>);

unsafe impl Send for Batch {}

unsafe impl Sync for Batch {}

impl Batch {
    pub(super) fn new(db: WeakDB, max_batch_size: usize, delay: Duration) -> Self {
        let batch = Batch(Arc::new(BatchInner {
            db,
            max_batch_size,
            ran: AtomicBool::new(false),
            calls: Mutex::new(Vec::new()),
        }));
        let mut bc = batch.clone();
        spawn(move || {
            sleep(delay);
            bc.trigger();
        });
        batch
    }

    pub(super) fn closed(&self) -> bool {
        self.0.ran.load(Ordering::Acquire)
    }

    pub(crate) fn push(&mut self, call: Call) -> Result<()> {
        if self.0.ran.load(Ordering::Acquire) {
            return Err(Unexpected("batch already run"));
        }

        let calls_len = {
            let mut calls = self.0.calls.try_lock_for(Duration::from_secs(10)).unwrap();
            calls.push(call);
            calls.len()
        };

        // wake up batch, it's ready to run
        if self.0.max_batch_size != 0 && calls_len >= self.0.max_batch_size {
            let mut bc = self.clone();
            spawn(move || bc.trigger());
        }

        Ok(())
    }

    /// trigger runs the batch if it has not already been run.
    pub(super) fn trigger(&mut self) {
        if let Ok(false) = self.0.ran.compare_exchange(false, true, SeqCst, SeqCst) {
            self.run();
        }
    }

    /// run performs the transactions in the batch and communicates results.
    /// back to DB.Batch
    fn run(&mut self) {
        let db = self.0.db.upgrade().unwrap();
        db.0.batch.lock().take();

        let mut calls = self.0.calls.try_lock().unwrap();
        while !calls.is_empty() {
            let mut last_call_id = 0;
            if let Err(err) = db.update(|tx| {
                for (index, call) in calls.iter().enumerate() {
                    last_call_id = index;
                    (call.h)(tx)?;
                }

                Ok(())
            }) {
                let failed_call = calls.remove(last_call_id);
                failed_call
                    .err
                    .send(Err(TrySolo(format!("{:?}", err))))
                    .unwrap();
                continue;
            }

            {
                for call in &*calls {
                    call.err.send(Ok(())).unwrap();
                }
                break;
            }
        }
    }
}

pub(super) struct Call {
    h: Arc<Box<dyn Fn(&mut TX) -> Result<()>>>,
    err: SyncSender<Result<()>>,
}

impl Call {
    pub(super) fn new(
        h: Arc<Box<dyn Fn(&mut TX) -> Result<()>>>,
        err: SyncSender<Result<()>>,
    ) -> Self {
        Self { h, err }
    }
}

pub struct Info {
    /// pointer to data
    pub data: *const u8,
    pub page_size: i64,
}

pub struct DBBuilder {
    path: PathBuf,
    no_sync: bool,
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
            no_sync: false,
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

    pub fn set_no_syn(mut self, no_sync: bool) -> Self {
        self.no_sync = no_sync;
        self
    }

    pub fn set_no_grow_sync(mut self, no_grow_sync: bool) -> Self {
        self.no_grow_sync = no_grow_sync;
        self
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
            no_sync: self.no_sync,
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

#[derive(Debug)]
pub struct Options {
    no_sync: bool,
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
    use crate::test_util::mock_db;
    use std::thread::spawn;
    use std::time::Duration;

    #[test]
    fn db_stats() {
        let db = mock_db().build().unwrap();
        db.update(|tx| {
            tx.create_bucket(b"widgets").unwrap();
            Ok(())
        })
        .unwrap();

        let stats = db.stats();
        // *Note*: no including init pages(0,1,2,3);
        assert_eq!(stats.tx_stats.page_count, 2);
        assert_eq!(stats.free_page_n, 0);
        assert_eq!(stats.pending_page_n, 2);
    }

    #[test]
    fn batch() {
        let db = mock_db()
            .set_batch_delay(Duration::from_secs(2))
            .set_batch_size(10)
            .build()
            .unwrap();

        let mut handles = vec![];
        for i in 0..10 {
            let mut db = db.clone();
            handles.push(spawn(move || {
                db.batch(Box::new(move |tx| {
                    let _ = tx.create_bucket(format!("{}bubu", i).as_bytes()).unwrap();
                    Ok(())
                }))
                .unwrap()
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        db.view(|tx| {
            for i in 0..10 {
                let _ = tx.bucket(format!("{}bubu", i).as_bytes()).unwrap();
            }
            Ok(())
        })
        .unwrap();
    }
}
