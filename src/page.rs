use crate::db::Meta;
use crate::free_list::FreeList;
use crate::must_align;
use std::borrow::{Borrow, BorrowMut};
use std::fmt::{Debug, Display, Formatter};
use std::marker::PhantomData;
use std::mem::size_of;
use std::ops::RangeBounds;
use std::ops::{Deref, DerefMut};
use std::slice::{from_raw_parts, from_raw_parts_mut, Iter};

pub(crate) const PAGE_HEADER_SIZE: usize = size_of::<Page>();
pub(crate) const MIN_KEYS_PER_PAGE: usize = 2;
pub(crate) const BRANCH_PAGE_ELEMENT_SIZE: usize = size_of::<BranchPageElement>();
pub(crate) const LEAF_PAGE_ELEMENT_SIZE: usize = size_of::<LeafPageElement>();
pub(crate) const META_PAGE_SIZE: usize = size_of::<Meta>();

pub(crate) const BRANCH_PAGE_FLAG: u16 = 0x01;
pub(crate) const LEAF_PAGE_FLAG: u16 = 0x02;
pub(crate) const META_PAGE_FLAG: u16 = 0x04;
pub(crate) const FREE_LIST_PAGE_FLAG: u16 = 0x10;

// u16
pub(crate) const BUCKET_LEAF_FLAG: u32 = 0x01;

pub type PgId = u64;

// Page Header
// |PgId(u64)|flags(u16)|count(u16)|over_flow
// So, Page Size = count + over_flow*sizeof(Page)
#[derive(Debug, Default, Clone)]
#[repr(C)]
pub struct Page {
    pub(crate) id: PgId,
    pub(crate) flags: u16,
    pub(crate) count: u16,
    pub(crate) over_flow: u32,
    // PhantomData not occupy real memory
    pub(crate) ptr: PhantomData<u8>,
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
        matches!(self.flags, BRANCH_PAGE_FLAG)
    }

    #[inline]
    pub(crate) fn is_leaf(&self) -> bool {
        matches!(self.flags, LEAF_PAGE_FLAG)
    }

    pub(crate) fn is_meta(&self) -> bool {
        matches!(self.flags, META_PAGE_FLAG)
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
        self.flags = BRANCH_PAGE_FLAG;
    }

    #[inline]
    pub(crate) fn copy_from_free_list(&mut self, free_list: &FreeList) {
        self.count = free_list.count() as u16;
        self.flags = FREE_LIST_PAGE_FLAG;
    }

    // The size of page, including header and elements.
    #[inline]
    fn byte_size(&self) -> usize {
        let mut size = PAGE_HEADER_SIZE;
        match self.flags {
            BRANCH_PAGE_FLAG => {
                let branch = self.branch_page_elements();
                let len = branch.len();
                if len > 0 {
                    let last_branch = branch.last().unwrap();
                    size += (len - 1) * BRANCH_PAGE_ELEMENT_SIZE;
                    size += (last_branch.pos + last_branch.k_size) as usize;
                }
            }
            LEAF_PAGE_FLAG => {
                let leaves = self.leaf_page_elements();
                let len = leaves.len();
                if len > 0 {
                    let last_leaf = leaves.last().unwrap();
                    size += (len - 1) * LEAF_PAGE_ELEMENT_SIZE;
                    size += (last_leaf.pos + last_leaf.k_size + last_leaf.v_size) as usize;
                }
            }
            META_PAGE_FLAG => {
                // TODO?
                size += META_PAGE_SIZE;
            }
            FREE_LIST_PAGE_FLAG => {
                size += self.pg_ids().len() * size_of::<PgId>();
            }
            _ => panic!("Unknown page flag: {:0x}", self.flags),
        }
        size
    }

    fn to_vec(self) -> Vec<u8> {
        let v: Vec<u8> = self.into();
        v
    }

    pub(crate) fn to_owned(self) -> Self {
        self
    }
}

impl Into<Vec<u8>> for Page {
    fn into(self) -> Vec<u8> {
        let ptr = &self as *const Page as *const u8;
        unsafe { from_raw_parts(ptr, self.byte_size()).to_vec() }
    }
}

impl From<Vec<u8>> for Page {
    fn from(vec: Vec<u8>) -> Self {
        let page = Page::from_slice(&vec);
        page.to_owned()
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
pub(crate) struct OwnedPage {
    page: Vec<u8>,
}

impl OwnedPage {
    pub(crate) fn new(size: usize) -> Self {
        Self {
            page: vec![0u8; size],
        }
    }

    // return page size
    #[inline]
    pub(crate) fn size(&self) -> usize {
        self.page.len()
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

    /// Returns binary serialized buffer of a page
    #[inline]
    pub(crate) fn buf(&self) -> &[u8] {
        &self.page
    }

    /// Returns binary serliazied muttable buffer of a page
    #[inline]
    pub(crate) fn buf_mut(&mut self) -> &mut [u8] {
        &mut self.page
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
            .field("page", &self as &Page)
            .finish()
    }
}

#[test]
fn t_page_type() {
    assert_eq!(
        Page {
            flags: BRANCH_PAGE_FLAG,
            ..Default::default()
        }
        .to_string(),
        "branch"
    );
    assert_eq!(
        Page {
            flags: LEAF_PAGE_FLAG,
            ..Default::default()
        }
        .to_string(),
        "leaf"
    );
    assert_eq!(
        Page {
            flags: META_PAGE_FLAG,
            ..Default::default()
        }
        .to_string(),
        "meta"
    );
    assert_eq!(
        Page {
            flags: FREE_LIST_PAGE_FLAG,
            ..Default::default()
        }
        .to_string(),
        "freelist"
    );
    assert_eq!(
        Page {
            flags: 0x4e20,
            ..Default::default()
        }
        .to_string(),
        "unknown<4e20>"
    );
}

#[test]
fn t_page_buffer() {
    let page = Page {
        id: 2,
        flags: FREE_LIST_PAGE_FLAG,
        ..Default::default()
    };
    let new_page = Page::from_slice(page.as_slice());
    assert_eq!(page.as_slice(), new_page.as_slice());
}
