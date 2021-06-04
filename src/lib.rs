#![feature(hash_drain_filter)]
#![feature(drain_filter)]
#![feature(test)]

use std::mem::align_of;

extern crate memoffset;
extern crate test;
extern crate bitflags;

pub mod bucket;
pub mod db;
mod error;
pub mod free_list;
pub mod mmap;
pub mod node;
pub mod os;
pub mod page;
pub mod tx;
pub mod cursor;

pub(crate) use bucket::Bucket;
pub use page::{Page, PageInfo, PgId, PgIds};
pub use tx::TxId;

#[allow(dead_code)]
#[inline]
pub fn must_align<T>(ptr: *const T) {
    let actual = (ptr as usize) % align_of::<T>() == 0;
    assert!(actual);
}

/// Rewrite of golang sort.search
#[inline]
pub fn search<F>(n: usize, mut f: F) -> usize
where
    F: FnMut(usize) -> bool,
{
    let mut i = 0;
    let mut j = n;
    while i < j {
        let h = (i + j) / 2;
        if !f(h) {
            i = h + 1;
        } else {
            j = j;
        }
    }
    i
}
