use std::io;
use std::os::macos::raw::stat;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Configuration: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[source] Box<io::Error>),
    #[error("Empty key")]
    EmptyKey,
    #[error("Key too large")]
    KeyTooLarge,
    #[error("Value too large")]
    ValueTooLarge,
    #[error("{0}")]
    OperatorFailed(String),
    #[error("Put failed: {0}")]
    PutFailed(String),
    #[error("cannot free page 0 or 1: {0}")]
    PageFreeFailed(String),
    #[error("page {0} already freed")]
    MmapAllocateFailed(String),
    #[error("Invalid checksum")]
    InvalidChecksum(String),
    #[error("{0}")]
    InvalidNode(String),
    #[error("ResizeFail")]
    ResizeFail,
    #[error("Invalid filename")]
    InvalidFilename(String),
    #[error("Traverser Failed")]
    TraverserFailed,
    #[error("Database Closed")]
    DBClosed,
    #[error("{0}")]
    LogRead(String),
    #[error("Bucket Empty")]
    BucketEmpty,
    #[error("Bucket Exists")]
    BucketExists,
    #[error("Bucket Name Required")]
    BucketNameRequired,
    #[error("Bucket NotFound")]
    BucketNotFound,
    #[error("Name Required")]
    NameRequired,
    #[error("Tx Closed")]
    TxClosed,
    #[error("Tx Read Only")]
    TxReadOnly,
    #[error("Tx Managed")]
    TxManaged,
    #[error("Tx Gone")]
    TxGone,
    #[error("Incompatible Value")]
    IncompatibleValue,
    #[error("Stack Empty")]
    StackEmpty,
    #[error("Check Failed, {0}")]
    CheckFailed(String),
    #[error("{0}")]
    DBOpFailed(String),
    #[error("Database Gone")]
    DatabaseGone,
    #[error("{0}")]
    Unexpected(&'static str),
    #[error("database only read")]
    DatabaseOnlyRead,
    // Returned when a DB instance is accessed before it
    // is opened or after it is closed.
    #[error("database not open")]
    DatabaseNotOpen,
    // Returned when opening a database that is already open.
    #[error("database already open")]
    DatabaseOpen,
    // Returned when both meta pages on a database are invalid.
    // This typically occurs when a file is not a bolt database.
    #[error("invalid database")]
    Invalid,
    // Returned when the data file was created with a different
    // version of Bolt.
    #[error("version mismatch")]
    VersionMismatch,
    // Returned when a database cannot obtain an exclusive lock
    // on the data file after the timeout passed to Open()
    #[error("timeout")]
    Timeout,
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Self {
        Error::Io(Box::new(e))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
