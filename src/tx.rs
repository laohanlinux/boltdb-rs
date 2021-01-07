use crate::db::DB;

/// Represents the internal transaction identifier
pub type TxId = u64;

/// Represents a `read-only` or `read-write` transaction on the database.
/// `Read-Only` transactions can be user for retrieving values for keys and creating cursors.
/// `Read/Write` transactions can create and remove buckets and create and remove keys.
///
/// *IMPORTANT*: You must commit or rollback transactions when you are done with
/// Them. Pages can not be reclaimed by the writer until no more transactions
/// are using them. A long running read transaction can cause the database to
/// quickly grow.
struct TX {
    write_able: bool,
    managed: bool,
    db: DB,

    // write-flag specifies the flag for write-reload methods like write_to().
    // TX opens the database file with th specified flag to copy the data.
    //
    // By default, the flag is unset, which works well for mostly in-memory
    // workloads. For database that are much larger than available RAM,
    // set the flag to syscall.O_DIRECT to avoid trashing the page cache.
    write_flag: isize,
}

impl TX {
    // pub fn id(&self) -> isize {
    //
    // }
    /// Return a reference to the database that created the transaction
    pub fn db(&self) -> &DB {
        &self.db
    }

    /// Return a mut reference to the database that created that transaction
    pub fn db_mut(&mut self) -> &mut DB {
        &mut self.db
    }

    // /// Return current database size in bytes as seen by this transaction.
    // pub fn size(&self) -> isize {
    //     self.size
    // }

    /// Return whether the transaction can perform write operations
    pub fn write_able(&self) -> bool {
        self.write_able
    }
}
