use std::fmt::{self, Display, Formatter};
use crate::must_align;

const MIN_KEYS_PER_PAGE: u64 = 2;

// const BRANCH_PAGE_ELEMENT_SIZE =

const BRANCH_PAGE_FLAG: u16 = 0x01;
const LEAF_PAGE_FLAG: u16 = 0x02;
const META_PAGE_FLAG: u16 = 0x04;
const FREE_LIST_PAGE_FLAG: u16 = 0x10;

const BUCKET_LEAF_FLAG: u16 = 0x10;


type PgId = u64;

pub struct Page {
    pub id: PgId,
    pub flags: u16,
    pub count: u16,
    pub over_flow: u16,
}


impl Page {}

impl Display for Page {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.flags & BRANCH_PAGE_FLAG != 0 {
            writeln!(f, "branch")
        } else if self.flags & LEAF_PAGE_FLAG != 0 {
            writeln!(f, "leaf")
        } else if self.flags & META_PAGE_FLAG != 0 {
            writeln!(f, "meta")
        } else if self.flags & FREE_LIST_PAGE_FLAG != 0 {
            writeln!(f, "freelist")
        } else {
            writeln!(f, "unknown<{:0x}>", self.flags)
        }
    }
}


// represents a node on a branch page.
struct BranchPageElement {
    pos: u32,
    ksize: u32,
    pgid: PgId,
}

// represents a node on a leaf page.
#[derive(Debug)]
#[repr(C)]
struct LeafPageElement {
    flags: u32,
    pos: u32,
    ksize: u32,
    vsize: u32,
}

impl LeafPageElement {
    fn value(&self) -> &[u8] {
        must_align(self);
        unsafe {
            let optr = self as *const Self as *const u8;
            let ptr = optr.add(self.pos as usize);
            ::std::slice::from_raw_parts(ptr, self.ksize as usize)
        }
    }
}


// represents human readable information about a page.
pub struct PageInfo {
    pub id: isize,
    pub typ: String,
    pub count: isize,
    pub over_flow_count: isize,
}

pub struct PgIds {
    inner: Vec<PgId>,
}

impl PgIds {
    // Returns the sorted union of a and b.
    // pub fn merge(&mut self, b: PgIds) -> PgIds {
    //     if self.inner.is_empty() {
    //         return PgIds { inner: b.inner.clone() };
    //     }
    //     if b.inner.is_empty() {
    //         return PgIds { inner: self.inner.clone() };
    //     }
    //
    // }
}
