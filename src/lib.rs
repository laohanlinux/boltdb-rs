#![feature(hash_drain_filter)]
#![feature(drain_filter)]
#![feature(test)]
#![feature(let_chains)]

use std::mem::align_of;

extern crate bitflags;
extern crate memoffset;
extern crate test;

pub mod bucket;
pub mod cursor;
pub mod db;
pub mod error;
pub mod free_list;
pub mod node;
pub mod os;
pub mod page;
mod test_util;
pub mod tx;

pub use bucket::Bucket;
pub use page::{Page, PageInfo, PgId, PgIds};
pub use tx::{TxGuard, TxId, TxStats};

#[allow(dead_code)]
#[inline]
pub fn must_align<T>(ptr: *const T) {
    let actual = (ptr as usize) % align_of::<T>() == 0;
    assert!(actual);
}
