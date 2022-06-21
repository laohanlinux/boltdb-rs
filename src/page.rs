use crate::db::Meta;
use crate::free_list::FreeList;
use crate::must_align;
// use enumflags2::{bitflags, BitFlags};
use bitflags::bitflags;
use kv_log_macro::debug;
use log::info;
use log::kv::{ToValue, Value};
use serde::{Deserialize, Serialize};
use std::borrow::{Borrow, BorrowMut};
use std::fmt;
use std::fmt::{Debug, Display, Formatter, Pointer};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::RangeBounds;
use std::ops::{Deref, DerefMut};
use std::process::id;
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};

pub(crate) const PAGE_HEADER_SIZE: usize = size_of::<Page>();
pub(crate) const MIN_KEYS_PER_PAGE: usize = 2;
pub(crate) const LEAF_PAGE_ELEMENT_SIZE: usize = size_of::<LeafPageElement>();
pub(crate) const META_PAGE_SIZE: usize = size_of::<Meta>();

bitflags! {
    pub struct ElementSize: usize {
        const Branch = size_of::<BranchPageElement>();
        const Leaf = size_of::<LeafPageElement>();
    }
}

bitflags! {
    /// Defines type of the page
    #[derive(Serialize, Deserialize)]
    pub struct PageFlag: u16 {
        /// Either branch or bucket page
        const Branch = 0x01;
        /// Leaf page
        const Leaf = 0x02;
        /// Meta page
        const Meta = 0x04;
        /// Freelist page
        const FreeList = 0x10;
    }
}

impl fmt::Display for PageFlag {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:0x}", self)
    }
}

// u16
pub(crate) const BUCKET_LEAF_FLAG: u32 = 0x01;

pub type PgId = u64;

// Page Header
// |PgId(u64)|flags(u16)|count(u16)|over_flow
// So, Page Size = count + over_flow*sizeof(Page)
#[derive(Debug, Serialize, Deserialize)]
#[repr(C)]
pub struct Page {
    pub(crate) id: PgId,
    pub(crate) flags: PageFlag,
    pub(crate) count: u16,
    pub(crate) over_flow: u32,
    // PhantomData not occupy real memory
    #[serde(skip_serializing)]
    pub(crate) ptr: PhantomData<u8>,
}

impl Default for Page {
    fn default() -> Self {
        Page {
            id: 0,
            flags: PageFlag::Branch,
            count: 0,
            over_flow: 0,
            ptr: Default::default(),
        }
    }
}

impl Page {
    // `meta` returns a pointer to the metadata section of the `page`
    pub fn meta(&self) -> &Meta {
        unsafe { &*(self.get_data_ptr() as *const Meta) }
    }

    pub fn meta_mut(&mut self) -> &mut Meta {
        unsafe { &mut *(self.get_data_ptr() as *mut Meta) }
    }

    // Retrieves the leaf node by index.
    pub(crate) fn leaf_page_element(&self, index: usize) -> &LeafPageElement {
        &self.leaf_page_elements()[index]
    }

    // Retrieves the mut leaf node by index.
    pub(crate) fn leaf_page_element_mut(&mut self, index: usize) -> &mut LeafPageElement {
        &mut self.leaf_page_elements_mut()[index]
    }

    // TODO add count == 0 check.
    // Retrieves a list of leaf node.
    pub(crate) fn leaf_page_elements(&self) -> &[LeafPageElement] {
        unsafe {
            std::slice::from_raw_parts(
                self.get_data_ptr() as *const LeafPageElement,
                self.count as usize,
            )
        }
    }

    // Retrieves a mut list of leaf node.
    pub(crate) fn leaf_page_elements_mut(&mut self) -> &mut [LeafPageElement] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.get_data_mut_ptr() as *mut LeafPageElement,
                self.count as usize,
            )
        }
    }

    // Retrieves the branch node by index.
    pub(crate) fn branch_page_element(&self, index: usize) -> &BranchPageElement {
        debug!("index is {}", index);
        &self.branch_page_elements()[index]
    }

    // Retrieves the branch node by index.
    pub(crate) fn branch_page_element_mut(&mut self, index: usize) -> &mut BranchPageElement {
        &mut self.branch_page_elements_mut()[index]
    }

    // Retrieves a list of branch nodes.
    pub(crate) fn branch_page_elements(&self) -> &[BranchPageElement] {
        unsafe {
            std::slice::from_raw_parts(
                self.get_data_ptr() as *const BranchPageElement,
                self.count as usize,
            )
        }
    }

    // Retrieves a mut list of branch nodes
    fn branch_page_elements_mut(&mut self) -> &mut [BranchPageElement] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.get_data_mut_ptr() as *mut BranchPageElement,
                self.count as usize,
            )
        }
    }

    // Returns a slice to the free list section of the page.
    pub(crate) fn free_list(&self) -> &[PgId] {
        unsafe {
            std::slice::from_raw_parts(self.get_data_ptr() as *const PgId, self.count as usize)
        }
    }

    // Returns a mut slice to the free list section of the page.
    pub(crate) fn free_list_mut(&mut self) -> &mut [PgId] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.get_data_mut_ptr() as *mut PgId,
                self.count as usize,
            )
        }
    }

    pub(crate) fn pgid(&self, index: usize) -> &PgId {
        &self.pg_ids()[index]
    }

    pub(crate) fn pg_ids(&self) -> &[PgId] {
        unsafe {
            std::slice::from_raw_parts(self.get_data_ptr() as *const PgId, self.count as usize)
        }
    }

    pub(crate) fn pg_ids_mut(&mut self) -> &mut [PgId] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.get_data_mut_ptr() as *mut PgId,
                self.count as usize,
            )
        }
    }

    #[inline]
    pub(crate) fn is_branch(&self) -> bool {
        matches!(self.flags, PageFlag::Branch)
    }

    #[inline]
    pub(crate) fn is_leaf(&self) -> bool {
        matches!(self.flags, PageFlag::Leaf)
    }

    pub(crate) fn is_meta(&self) -> bool {
        matches!(self.flags, PageFlag::Meta)
    }

    #[inline]
    pub(crate) fn get_data_mut_ptr(&mut self) -> *mut u8 {
        &mut self.ptr as *mut PhantomData<u8> as *mut u8
    }

    #[inline]
    pub(crate) fn get_data_ptr(&self) -> *const u8 {
        &self.ptr as *const PhantomData<u8> as *const u8
    }

    #[inline]
    pub(crate) fn get_data_slice(&self) -> &[u8] {
        let ptr = self.get_data_ptr();
        unsafe { from_raw_parts(ptr, self.byte_size() - PAGE_HEADER_SIZE) }
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> &[u8] {
        let ptr = self as *const Page as *const u8;
        unsafe { from_raw_parts(ptr, self.byte_size()) }
    }

    #[inline]
    pub(crate) fn as_slice_mut(&mut self) -> &mut [u8] {
        let ptr = self as *mut Page as *mut u8;
        unsafe { from_raw_parts_mut(ptr, self.byte_size()) }
    }

    #[inline]
    pub(crate) fn from_slice(buffer: &[u8]) -> &Page {
        unsafe { &*(buffer.as_ptr() as *const Page) }
    }

    #[inline]
    pub(crate) fn from_slice_mut(mut buffer: &mut [u8]) -> &mut Self {
        unsafe { &mut *(buffer.as_mut_ptr() as *mut Page) }
    }

    // The size of page, including header and elements.
    #[inline]
    pub(crate) fn copy_from_meta(&mut self, meta: &Meta) {
        self.count = 0;
        self.flags = PageFlag::Meta;
        let sbytes = meta.as_slice();
        unsafe {
            std::ptr::copy_nonoverlapping(
                meta.as_slice_no_checksum().as_ptr(),
                self.get_data_mut_ptr(),
                sbytes.len(),
            )
        }
    }

    #[inline]
    pub(crate) fn copy_from_free_list(&mut self, free_list: &FreeList) {
        self.count = free_list.count() as u16;
        self.flags = PageFlag::FreeList;
    }

    // The size of page, including header and elements.
    #[inline]
    fn byte_size(&self) -> usize {
        let mut size = PAGE_HEADER_SIZE;
        match self.flags {
            PageFlag::Branch => {
                let branch = self.branch_page_elements();
                let len = branch.len();
                if len > 0 {
                    let last_branch = branch.last().unwrap();
                    size += (len - 1) * ElementSize::Branch.bits();
                    size += (last_branch.pos + last_branch.k_size) as usize;
                }
            }
            PageFlag::Leaf => {
                let leaves = self.leaf_page_elements();
                let len = leaves.len();
                if len > 0 {
                    let last_leaf = leaves.last().unwrap();
                    size += (len - 1) * ElementSize::Leaf.bits();
                    size += (last_leaf.pos + last_leaf.k_size + last_leaf.v_size) as usize;
                }
            }
            PageFlag::Meta => {
                size += META_PAGE_SIZE;
            }
            PageFlag::FreeList => {
                size += self.pg_ids().len() * size_of::<PgId>();
            }
            _ => panic!("Unknown page flag: {}", self.flags),
        }
        size
    }
}

impl ToOwned for Page {
    type Owned = OwnedPage;

    fn to_owned(&self) -> Self::Owned {
        let ptr = self as *const Page as *const u8;
        unsafe {
            let slice = from_raw_parts(ptr, self.byte_size()).to_owned();
            OwnedPage::from_vec(slice)
        }
    }
}

impl Display for Page {
    fn fmt(&self, f: &mut Formatter<'_>) -> ::std::fmt::Result {
        if (self.flags & PageFlag::Branch).bits() != 0 {
            write!(f, "branch")
        } else if (self.flags & PageFlag::Leaf).bits() != 0 {
            write!(f, "leaf")
        } else if (self.flags & PageFlag::Meta).bits() != 0 {
            write!(f, "meta")
        } else if (self.flags & PageFlag::FreeList).bits() != 0 {
            write!(f, "freelist")
        } else {
            write!(f, "unknown<{:0x}>", self.flags.bits())
        }
    }
}

impl ToValue for Page {
    fn to_value(&self) -> Value {
        todo!()
    }
}

// represents a node on a branch page.
#[derive(Debug, Default)]
#[repr(C)]
pub(crate) struct BranchPageElement {
    // distinct of the branch page element
    pub(crate) pos: u32,
    pub(crate) k_size: u32,
    pub(crate) pgid: PgId,
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
pub(crate) struct LeafPageElement {
    pub(crate) flags: u32,
    // distinct of the leaf page element
    pub(crate) pos: u32,
    pub(crate) k_size: u32,
    pub(crate) v_size: u32,
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
    pub id: u64,
    pub typ: u16,
    pub count: usize,
    pub over_flow_count: usize,
}

#[derive(Clone, Debug, Default, PartialOrd, PartialEq)]
pub struct PgIds {
    pub(crate) inner: Vec<PgId>,
}

impl From<Vec<PgId>> for PgIds {
    fn from(v: Vec<u64>) -> Self {
        PgIds { inner: v }
    }
}

impl PgIds {
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, u64> {
        self.inner.iter()
    }

    #[inline]
    pub fn sort(&mut self) {
        self.inner.sort();
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

    #[inline]
    pub fn to_vec(self) -> Vec<PgId> {
        self.inner
    }

    #[inline]
    pub fn as_ref_vec(&self) -> &Vec<PgId> {
        &self.inner
    }

    #[inline]
    pub fn drain<R>(&mut self, range: R) -> Vec<u64>
    where
        R: RangeBounds<usize>,
    {
        self.inner.drain(range).collect::<Vec<_>>()
    }

    // TODO: Optz
    #[inline]
    pub fn extend_from_slice(&mut self, slice: Self) {
        self.inner.extend_from_slice(&*slice.inner);
        self.inner.dedup();
        self.inner.sort();
    }
}

#[derive(Clone)]
#[repr(align(64))]
pub struct OwnedPage {
    page: Vec<u8>,
}

impl OwnedPage {
    pub(crate) fn new(size: usize) -> Self {
        Self {
            page: vec![0u8; size],
        }
    }

    pub(crate) fn from_vec(buf: Vec<u8>) -> Self {
        Self { page: buf }
    }

    /// reserve capacity of underlying vector to size
    #[allow(dead_code)]
    pub(crate) fn reserve(&mut self, size: usize) {
        self.page.reserve(size);
    }

    /// Returns pointer to page structure
    #[inline]
    pub(crate) fn as_ptr(&self) -> *const u8 {
        self.page.as_ptr()
    }

    /// Returns pointer to page structure
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.page.as_mut_ptr()
    }

    /// Returns binary serialized buffer pf a page
    #[inline]
    pub(crate) fn buf(&self) -> &[u8] {
        &self.page
    }

    /// Returns binary serialized muttable buffer of a page
    #[inline]
    pub(crate) fn buf_mut(&mut self) -> &mut [u8] {
        &mut self.page
    }

    /// Returns page size
    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.page.len()
    }
}

impl Borrow<Page> for OwnedPage {
    #[inline]
    fn borrow(&self) -> &Page {
        unsafe { &*(self.page.as_ptr() as *const Page) }
    }
}

impl BorrowMut<Page> for OwnedPage {
    #[inline]
    fn borrow_mut(&mut self) -> &mut Page {
        unsafe { &mut *(self.page.as_mut_ptr() as *mut Page) }
    }
}

impl Deref for OwnedPage {
    type Target = Page;
    #[inline]
    fn deref(&self) -> &Page {
        self.borrow()
    }
}

impl DerefMut for OwnedPage {
    #[inline]
    fn deref_mut(&mut self) -> &mut Page {
        self.borrow_mut()
    }
}

impl std::fmt::Debug for OwnedPage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("OwnedPage")
            .field("size", &self.page.len())
            .field("page", self as &Page)
            .finish()
    }
}

#[test]
fn t_page_type() {
    assert_eq!(
        Page {
            flags: PageFlag::Branch,
            ..Default::default()
        }
        .to_string(),
        "branch"
    );
    assert_eq!(
        Page {
            flags: PageFlag::Leaf,
            ..Default::default()
        }
        .to_string(),
        "leaf"
    );
    assert_eq!(
        Page {
            flags: PageFlag::Meta,
            ..Default::default()
        }
        .to_string(),
        "meta"
    );
    assert_eq!(
        Page {
            flags: PageFlag::FreeList,
            ..Default::default()
        }
        .to_string(),
        "freelist"
    );
    // assert_eq!(
    //     Page {
    //         flags: 0x4e20,
    //         ..Default::default()
    //     }
    //     .to_string(),
    //     "unknown<4e20>"
    // );
}

#[test]
fn t_page_buffer() {
    let page = Page {
        id: 2,
        flags: PageFlag::FreeList,
        ..Default::default()
    };
    let new_page = Page::from_slice(page.as_slice());
    assert_eq!(page.as_slice(), new_page.as_slice());
}

#[test]
fn t_new() {
    let mut buf = vec![0u8; 256];
    let mut page = Page::from_slice_mut(&mut buf);
    assert_eq!(page.over_flow, 0);
    assert_eq!(page.count, 0);
    assert_eq!(page.id, 0);

    page.id = 25;
    assert_eq!(page.id, 25);

    page.flags = PageFlag::Meta;
    assert_eq!(page.flags, PageFlag::Meta);
}

#[test]
fn t_read_leaf_nodes() {
    let mut page = OwnedPage::new(4096);
    page.flags = PageFlag::Leaf;
    page.count = 2;
    let mut nodes =
        unsafe { from_raw_parts_mut(page.get_data_mut_ptr() as *mut LeafPageElement, 3) };
    nodes[0] = LeafPageElement {
        flags: 0,
        pos: 32,
        k_size: 3,
        v_size: 4,
    };
    nodes[1] = LeafPageElement {
        flags: 0,
        pos: 23,
        k_size: 10,
        v_size: 3,
    };

    let data =
        unsafe { from_raw_parts_mut(&mut nodes[2] as *mut LeafPageElement as *mut u8, 4096) };
    data[..7].copy_from_slice("barfooz".as_bytes());
    data[7..7 + 13].copy_from_slice("helloworldbye".as_bytes());

    let el = page.leaf_page_element(0);
    assert_eq!(el.k_size, 3);
    assert_eq!(el.v_size, 4);
    assert_eq!(el.pos, 32);

    let el = page.leaf_page_element(1);
    assert_eq!(el.k_size, 10);
    assert_eq!(el.v_size, 3);
    assert_eq!(el.pos, 23);
}

#[test]
fn t_to_owned() {
    crate::test_util::mock_log();
    let mut buf = vec![0u8; 1024];
    let mut page = Page::from_slice_mut(&mut buf);
    page.flags = PageFlag::Leaf;
    page.count = 2;

    let nodes = unsafe { from_raw_parts_mut(page.get_data_ptr() as *mut LeafPageElement, 3) };
    nodes[0] = LeafPageElement {
        flags: 0,
        pos: 32,
        k_size: 3,
        v_size: 4,
    };
    nodes[1] = LeafPageElement {
        flags: 0,
        pos: 23,
        k_size: 10,
        v_size: 3,
    };

    let data =
        unsafe { from_raw_parts_mut(&mut nodes[2] as *mut LeafPageElement as *mut u8, 1024) };
    data[..7].copy_from_slice("barfooz".as_bytes());
    data[7..7 + 13].copy_from_slice("helloworldbye".as_bytes());

    let owned = page.to_owned();
    /// Note: Very interesting
    assert_eq!(
        owned.page.len(),
        PAGE_HEADER_SIZE + ElementSize::Leaf.bits() + 23 + 10 + 3
    );
}
