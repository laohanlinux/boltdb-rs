use crate::db::{Meta, DB};
use crate::{Bucket, Page, PgId};
use std::collections::HashMap;
use std::time::Duration;

/// Represents the internal transaction identifier
pub type TxId = u64;

pub(crate) type CommitHandler = Box<dyn FnOnce() + Send>;

/// Represents a `read-only` or `read-write` transaction on the database.
/// `Read-Only` transactions can be user for retrieving values for keys and creating cursors.
/// `Read/Write` transactions can create and remove buckets and create and remove keys.
///
/// *IMPORTANT*: You must commit or rollback transactions when you are done with
/// Them. Pages can not be reclaimed by the writer until no more transactions
/// are using them. A long running read transaction can cause the database to
/// quickly grow.
#[derive(Default)]
pub(crate) struct TX {
    pub(crate) writable: bool,
    pub(crate) managed: bool,
    pub(crate) db: DB,
    pub(crate) meta: Box<Meta>,
    pub(crate) root: Box<Bucket>,
    pub(crate) pages: HashMap<PgId, Page>,
    pub(crate) stats: TxStats,
    pub(crate) commit_handler: Vec<CommitHandler>,

    // write-flag specifies the flag for write-reload methods like write_to().
    // TX opens the database file with th specified flag to copy the data.
    //
    // By default, the flag is unset, which works well for mostly in-memory
    // workloads. For database that are much larger than available RAM,
    // set the flag to syscall.O_DIRECT to avoid trashing the page cache.
    pub(crate) write_flag: isize,
}

impl Clone for TX {
    fn clone(&self) -> Self {
        unimplemented!()
    }
}

impl TX {
    /// Initializes the transaction
    fn init(&mut self, _db: &DB) {}

    /// Returns the transaction id.
    pub(crate) fn id(&self) -> u64 {
        self.meta.tx_id
    }

    /// Return a reference to a database that created the transaction.
    pub(crate) fn size(&self) -> u64 {
        self.meta.pg_id * self.db.page_size as u64
    }

    /// Return a reference to the database that created the transaction
    pub(crate) fn db(&self) -> &DB {
        &self.db
    }

    /// Return a mut reference to the database that created that transaction
    pub(crate) fn db_mut(&mut self) -> &mut DB {
        &mut self.db
    }

    /// Return whether the transaction can perform write operations
    pub(crate) fn writable(&self) -> bool {
        self.writable
    }

    //todo
    pub(crate) fn page(&self, _id: PgId) -> Page {
        Page::default()
    }
}

/// TxStats represents statistics about the actions performed by the transaction.
#[derive(Default, Clone)]
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
    // #[test]
    // fn test_tx_commit_err_tx_closed() {}
    //
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
