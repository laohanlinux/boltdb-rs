#![feature(hash_drain_filter)]
#![feature(drain_filter)]
#![feature(test)]
#![feature(let_chains)]
#![feature(io_error_uncategorized)]

use std::mem::align_of;

extern crate bitflags;
extern crate memoffset;
extern crate test;

pub mod bucket;
mod cursor;
pub mod db;
pub mod error;
mod free_list;
mod node;
mod os;
mod page;
mod test_util;
pub mod tx;

pub use bucket::Bucket;
pub use db::DB;
pub use tx::{TxId, TxStats, TX};
pub use error::Error;

#[allow(dead_code)]
#[inline]
pub(crate) fn must_align<T>(ptr: *const T) {
    let actual = (ptr as usize) % align_of::<T>() == 0;
    assert!(actual);
}
