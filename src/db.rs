use crate::free_list::FreeList;
use crate::{Bucket, PgId, TxId};
use memmap::Mmap;
use std::fs::Permissions;
use std::time::Duration;

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

/// `DB` represents a collection of buckets persisted to a file on disk.
/// All data access is performed through transactions which can be obtained through the `DB`
/// All the functions on `DB` will return a `DatabaseNotOpen` if accessed before Open() is called.
#[derive(Default)]
pub struct DB {
    /// When enabled, the database will perform a Check() after every commit.
    /// A panic is issued if the database is in a inconsistent state. This
    /// flag has a large performance impact so it should only be used for
    /// debugging purpose.
    pub strict_mode: bool,

    path: &'static str,
    pub(crate) mmap: Option<Mmap>,
    pub(crate) mmap_size: usize,
    pub(crate) page_size: usize,
    pub(crate) free_list: FreeList,
}

impl DB {
    pub fn open(_path: &'static str, _perm: Permissions) {}

    /// Return the path to currently open database file.
    pub fn path(&self) -> &'static str {
        self.path
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
