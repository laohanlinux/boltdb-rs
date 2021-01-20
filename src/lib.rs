#![feature(hash_drain_filter)]

use std::mem::align_of;

pub mod bucket;
pub mod db;
pub mod free_list;
pub mod mmap;
pub mod node;
pub mod os;
pub mod page;
pub mod tx;

pub use bucket::Bucket;
pub use page::{PgId, Page, PgIds, PageInfo};
pub use tx::{TxId};

#[allow(dead_code)]
#[inline]
pub fn must_align<T>(ptr: *const T) {
    let actual = (ptr as usize) % align_of::<T>() == 0;
    assert!(actual);
}