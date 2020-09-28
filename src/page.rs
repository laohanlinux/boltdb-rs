use std::fmt::{self, Display, Formatter};
use crate::must_align;
use std::fs::Metadata;
use std::slice::Iter;

const MIN_KEYS_PER_PAGE: u64 = 2;

// const BRANCH_PAGE_ELEMENT_SIZE =

const BRANCH_PAGE_FLAG: u16 = 0x01;
const LEAF_PAGE_FLAG: u16 = 0x02;
const META_PAGE_FLAG: u16 = 0x04;
const FREE_LIST_PAGE_FLAG: u16 = 0x10;

const BUCKET_LEAF_FLAG: u16 = 0x10;


pub type PgId = u64;

pub struct Page {
    pub id: PgId,
    pub flags: u16,
    pub count: u16,
    pub over_flow: u16,
}


impl Page {
    // `meta` returns a pointer to the metadata section of the `page`
    // pub(crate) fn meta(&self) -> &Metadata {}
}

impl Display for Page {
    fn fmt(&self, f: &mut Formatter<'_>) -> ::std::fmt::Result {
        if self.flags & BRANCH_PAGE_FLAG != 0 {
            writeln!(f, "branch")
        } else if self.flags & LEAF_PAGE_FLAG != 0 {
            writeln!(f, "leaf")
        } else if self.flags & META_PAGE_FLAG != 0 {
            writeln!(f, "meta")
        } else if self.flags & FREE_LIST_PAGE_FLAG != 0 {
            writeln!(f, "freelist")
        } else {
            writeln!(f, "unknown<{}>", self.flags)
        }
    }
}

// represents a node on a branch page.
#[derive(Debug)]
#[repr(C)]
struct BranchPageElement {
    pos: u32,
    k_size: u32,
    pgid: PgId,
}

impl BranchPageElement {
    pub(crate) fn key(&self) -> &[u8] {
        must_align(self);
        unsafe {
            let optr = self as *const Self as *const u8;
            let ptr = optr.add(self.pos as usize);
            ::std::slice::from_raw_parts(ptr, self.k_size as usize)
        }
    }
}

// represents a node on a leaf page.
#[derive(Debug)]
#[repr(C)]
struct LeafPageElement {
    flags: u32,
    pos: u32,
    k_size: u32,
    v_size: u32,
}

impl LeafPageElement {
    pub(crate) fn key(&self) -> &[u8] {
        must_align(self);
        unsafe {
            let optr = self as *const Self as *const u8;
            let ptr = optr.add(self.pos as usize);
            ::std::slice::from_raw_parts(ptr, self.k_size as usize)
        }
    }

    pub(crate) fn value(&self) -> &[u8] {
        must_align(self);
        unsafe {
            let optr = self as *const Self as *const u8;
            let ptr = optr.add((self.pos + self.k_size) as usize);
            ::std::slice::from_raw_parts(ptr, self.v_size as usize)
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

impl From<Vec<PgId>> for PgIds {
    fn from(v: Vec<u64>) -> Self {
        PgIds { inner: v }
    }
}

impl PgIds {
    pub fn new() -> PgIds {
        PgIds { inner: Vec::new() }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, u64> {
        self.inner.iter()
    }

    #[inline]
    pub fn as_slice(&self) -> &Vec<PgId> {
        &self.inner
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn push(&mut self, pgid: PgId) {
        self.inner.push(pgid);
    }
}

#[test]
fn t_leaf_page_element() {
    let leaf = LeafPageElement {
        flags: 0x10,
        pos: 10,
        k_size: 200,
        v_size: 300,
    };
    assert_eq!(200, leaf.key().len());
    assert_eq!(300, leaf.value().len());
}