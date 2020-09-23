use std::mem::align_of;

pub mod bucket;
pub mod mmap;
pub mod os;
pub mod page;
pub mod node;
pub mod db;

#[allow(dead_code)]
#[inline]
pub fn must_align<T>(ptr: *const T) {
    let actual = (ptr as usize) % align_of::<T>() == 0;
    assert!(actual);
}