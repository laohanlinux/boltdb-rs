use crate::error::Error;
use crate::free_list::FreeList;
use crate::tx::TX;
use crate::{Bucket, Page, PgId, TxId};
use memmap::Mmap;
use std::fs::{File, Permissions};
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use test::stats::Stats;

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
    pub struct CheckMode: u8 {

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
    page_size: usize,
    opened: AtomicBool,
    pub(crate) rw_lock: Mutex<()>,
    pub(crate) rw_tx: RwLock<Option<Tx>>,
    pub(crate) txs: RwLock<Vec<TX>>,
    pub(crate) free_list: RwLock<FreeList>,
    pub(crate) stats: RwLock<Stats>,
    pub(crate) batch: Mutex<Option<Batch>>,
    pub(crate) page_pool: Mutex<Vec<Page>>,
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

impl<'a> DB {
    pub fn open(_path: &'static str, _perm: Permissions) {}

    fn cleanup(&mut self) -> Result<(), Error> {}
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
