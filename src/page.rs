use crate::must_align;
use std::fmt::{Display, Formatter};
use std::slice::Iter;
use std::marker::PhantomData;
use std::mem::size_of;
use crate::db::Meta;
use std::any::Any;

const PAGE_HEADER_SIZE: usize = size_of::<Page>();
const MIN_KEYS_PER_PAGE: u64 = 2;
const BRANCH_PAGE_ELEMENT_SIZE: usize = size_of::<BranchPageElement>();
const LEAF_PAGE_ELEMENT_SIZE: usize = size_of::<LeafPageElement>();

const BRANCH_PAGE_FLAG: u16 = 0x01;
const LEAF_PAGE_FLAG: u16 = 0x02;
const META_PAGE_FLAG: u16 = 0x04;
const FREE_LIST_PAGE_FLAG: u16 = 0x10;

const BUCKET_LEAF_FLAG: u16 = 0x10;

pub type PgId = u64;

#[derive(Debug, Default)]
#[repr(C)]
pub struct Page {
    id: PgId,
    flags: u16,
    count: u16,
    over_flow: u16,
    ptr: PhantomData<u8>,
}

impl Page {
    // `meta` returns a pointer to the metadata section of the `page`
    pub(crate) fn meta(&mut self) -> &mut Meta {
        unsafe {
            &mut *(self.get_data_mut_ptr() as *mut Meta)
        }
    }

    // Retrieves the leaf node by index.
    fn leaf_page_element(&self, index: usize) -> &LeafPageElement {
        &self.leaf_page_elements()[index]
    }

    // TODO add count == 0 check.
    // Retrieves a list of leaf node.
    fn leaf_page_elements(&self) -> &[LeafPageElement] {
        unsafe {
            std::slice::from_raw_parts(self.get_data_ptr() as *const LeafPageElement, self.count as usize)
        }
    }

    // Retrieves a mut list of leaf node.
    fn leaf_page_elements_mut(&mut self) -> &mut [LeafPageElement] {
        unsafe {
            std::slice::from_raw_parts_mut(self.get_data_mut_ptr() as *mut LeafPageElement, self.count as usize)
        }
    }

    // Retrieves the branch node by index.
    fn branch_page_element(&self, index: usize) -> &BranchPageElement {
        &self.branch_page_elements()[index]
    }

    // Retrieves a list of branch nodes.
    fn branch_page_elements(&self) -> &[BranchPageElement] {
        unsafe { std::slice::from_raw_parts(self.get_data_ptr() as *const BranchPageElement, self.count as usize) }
    }

    // Retrieves a mut list of branch nodes
    fn branch_page_elements_mut(&mut self) -> &mut [BranchPageElement] {
        unsafe { std::slice::from_raw_parts_mut(self.get_data_mut_ptr() as *mut BranchPageElement, self.count as usize) }
    }

    #[inline]
    fn get_data_mut_ptr(&mut self) -> *mut u8 {
        &mut self.ptr as *mut PhantomData<u8> as *mut u8
    }

    #[inline]
    fn get_data_ptr(&self) -> *const u8 {
        &self.ptr as *const PhantomData<u8> as *const u8
    }
}

impl Display for Page {
    fn fmt(&self, f: &mut Formatter<'_>) -> ::std::fmt::Result {
        if self.flags & BRANCH_PAGE_FLAG != 0 {
            write!(f, "branch")
        } else if self.flags & LEAF_PAGE_FLAG != 0 {
            write!(f, "leaf")
        } else if self.flags & META_PAGE_FLAG != 0 {
            write!(f, "meta")
        } else if self.flags & FREE_LIST_PAGE_FLAG != 0 {
            write!(f, "freelist")
        } else {
            write!(f, "unknown<{:0x}>", self.flags)
        }
    }
}

// represents a node on a branch page.
#[derive(Debug, Default)]
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
            let optr = self.as_ptr();
            let ptr = optr.add(self.pos as usize);
            ::std::slice::from_raw_parts(ptr, self.k_size as usize)
        }
    }

    #[inline]
    pub const fn as_ptr(&self) -> *const u8 {
        self as *const Self as *const u8
    }
}

// represents a node on a leaf page.
#[derive(Debug, Default)]
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
            let optr = self.as_ptr();
            let ptr = optr.add(self.pos as usize);
            ::std::slice::from_raw_parts(ptr, self.k_size as usize)
        }
    }

    pub(crate) fn value(&self) -> &[u8] {
        must_align(self);
        unsafe {
            let optr = self.as_ptr();
            let ptr = optr.add((self.pos + self.k_size) as usize);
            ::std::slice::from_raw_parts(ptr, self.v_size as usize)
        }
    }

    #[inline]
    pub const fn as_ptr(&self) -> *const u8 {
        self as *const Self as *const u8
    }
}

// represents human readable information about a page.
#[derive(Debug, Default)]
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
fn t_page_type() {
    assert_eq!(Page { flags: BRANCH_PAGE_FLAG, ..Default::default() }.to_string(), "branch");
    assert_eq!(Page { flags: LEAF_PAGE_FLAG, ..Default::default() }.to_string(), "leaf");
    assert_eq!(Page { flags: META_PAGE_FLAG, ..Default::default() }.to_string(), "meta");
    assert_eq!(Page { flags: FREE_LIST_PAGE_FLAG, ..Default::default() }.to_string(), "freelist");
    assert_eq!(Page { flags: 0x4e20, ..Default::default() }.to_string(), "unknown<4e20>");
}
