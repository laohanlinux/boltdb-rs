use crate::error::Error;
use crate::free_list::FreeList;
use crate::tx::TX;
use crate::{Bucket, Page, PgId, TxId};
use memmap::Mmap;
use std::fs::{File, Permissions};
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::thread::{sleep, spawn};
use std::time::Duration;
use bitflags::bitflags;
use std::ops::AddAssign;

/// The largest step that can be token when remapping the mman.
const MAX_MMAP_STEP: usize = 1 << 30; //1GB

/// The data file format version.
const VERSION: usize = 2;

/// Represents a marker value to indicate that a file is a Bolt `DB`.
const MAGIC: u32 = 0xED0CDAED;

/// Default values if not set in a `DB` instance.
const DEFAULT_MAX_BATCH_SIZE: isize = 1000;
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
    pub fn open(_path: &'static str, _perm: Permissions) {}

    /// Return the path to currently open database file.
    pub fn path(&self) -> &'static str {
        self.0.path
    }

    fn cleanup(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
// impl DB {
//     pub fn open(_path: &'static str, _perm: Permissions) {}
//
//     /// Return the path to currently open database file.
//     pub fn path(&self) -> &'static str {
//         self.path
//     }
//
//     // page retrives a page reference from the mmap based on the current page size.
//     pub(crate) fn page(&self, pg_id: PgId) -> &Page {
//         let pos = pg_id * self.page_size;
//         self.data
//     }
// }

impl Drop for DB {
    fn drop(&mut self) {
        if Arc::strong_count(&self.0) > 1 {
            return;
        }
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

#[derive(Default, Clone)]
pub(crate) struct Meta {
    magic: u32,
    version: u32,
    page_size: u32,
    flags: u32,
    root: Bucket,
    free_list: PgId,
    pub(crate) pg_id: PgId,
    pub(crate) tx_id: TxId,
    check_sum: u64,
}

// Represents statistics about the database.
pub struct Stats {
    // FreeList stats.
    // total number of free pages on the freelist.
    pending_page_n: u64,
    // total number of pending pages on the freelist.
    free_alloc: u64,
    // total bytes allocated in free pages.
    free_list_inuse: u64,
    // total bytes used by the freelist.
    free_page_n: u64,
    // Transaction stats
    // total number of started read transactions.
    tx_n: u64,
    // number of currently open read transactions.
    open_tx_n: u64,
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
