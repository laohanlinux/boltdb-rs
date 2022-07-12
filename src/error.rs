use std::io;
use std::io::ErrorKind::Uncategorized;
use std::ops::Not;
use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("invalid Configuration: {0}")]
    Config(String),
    /// Returned when io be opened failed.
    #[error("io error: {0}")]
    Io(String),
    /// Returned when file be resized failed.
    #[error("resize failed")]
    ResizeFail,
    #[error("tx managed")]
    TxManaged,
    #[error("stack empty")]
    StackEmpty,
    /// Returned when check sync failed.
    #[error("check failed, {0}")]
    CheckFailed(String),
    #[error("{0}")]
    Unexpected(&'static str),
    #[error("{0}")]
    Unexpected2(String),

    // -------------------------------------------------------------------
    /// These errors can be returned when opening or calling methods on a DB.
    /// Returned when a DB instance is accessed before it
    /// is opened or after it is closed.
    #[error("database not open")]
    DatabaseNotOpen,
    /// Returned when both meta pages on a database are invalid.
    /// This typically occurs when a file is not a bolt database.
    #[error("invalid database")]
    Invalid,
    /// Returned when the data file was created with a different
    /// version of Bolt.
    #[error("version mismatch")]
    /// Returned when either meta page checksum does not match.
    VersionMismatch,
    #[error("checksum error")]
    Checksum,
    /// TrySolo is a special sentinel error value used for signaling that a
    /// transaction function should be re-run. It should never be seen by
    /// callers.
    #[error("batch function returned an error and should be re-run solo, err: {0}")]
    TrySolo(String),
    // -------------------------------------------------------------------
    // These errors can occur when beginning or committing a Tx.
    /// Returned when performing a write operation on a read-only transaction.
    #[error("tx readonly")]
    TxNoWritable,
    /// Returned when committing or rolling back a transaction
    /// that has already been committed or rolled back.
    #[error("tx closed")]
    TxClosed,
    /// Returned when a mutating transaction is started on a read-only database.
    #[error("database only read")]
    DatabaseOnlyRead,
    // -------------------------------------------------------------------
    // These errors can occur when putting or deleting a value or a bucket.
    /// Returned when trying to access a bucket that has not been created yet.
    #[error("bucket notfound")]
    BucketNotFound,
    /// Returned when creating a bucket that already exists.
    #[error("bucket exists")]
    BucketExists,
    /// Returned when creating a bucket with a blank name.
    #[error("Bucket Name Required")]
    BucketNameRequired,
    /// Returned when inserting a zero-length key.
    #[error("key required")]
    KeyRequired,
    /// Returned when inserting a key that is larger than MAX_KEY_SIZE.
    #[error("key too large")]
    KeyTooLarge,
    /// Returned when inserting a value that is larger than MAX_VALUE_SIZE.
    #[error("value too large")]
    ValueTooLarge,
    /// Returned when trying to create or delete a bucket
    /// on an existing non-bucket key or when trying to create or delete a
    /// non-bucket key on an existing bucket key.
    #[error("incompatible value")]
    IncompatibleValue,
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Self {
        Error::Io(e.kind().to_string())
    }
}

impl From<&'static str> for Error {
    #[inline]
    fn from(s: &'static str) -> Self {
        Self::Unexpected(s)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn is_valid_error(err: &std::io::Error) -> bool {
    err.kind() == Uncategorized && err.to_string() == "Success (os error 0)"
}
