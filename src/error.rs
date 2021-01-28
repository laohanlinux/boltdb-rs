use thiserror::Error;
use std::io;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Configuration: {0}")]
    Config(String),
    #[error("IO error: {0}")]
    Io(#[source] Box<io::Error>),
    #[error("Empty key")]
    EmptyKey,
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
    #[error("Invalid filename")]
    InvalidFilename(String),
    #[error("Database Closed")]
    DBClosed,
    #[error("{0}")]
    LogRead(String),
}


impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Self {
        Error::Io(Box::new(e))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
