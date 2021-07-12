// Cursor represents an iterator that can traverse over all key/value pairs in a bucket in sorted order.
// Cursors see nested buckets with value == nil.
// Cursors can be obtained from a transaction and are valid as long as the transaction is open.
//
// Keys and values returned from the cursor are only valid for the life of the transaction.
//
// Changing data while traversing with a cursor may cause it to be invalidated
// and return unexpected keys and/or values. You must reposition your cursor
// after mutating data.

use crate::error::{Result, Error},
use crate::node::{Node, WeakNode};
use crate::page::{BUCKET_LEAF_FLAG, LEAF_PAGE_FLAG};
use crate::{Bucket, Page, PgId};
use either::Either;
use std::cell::RefCell;
use std::marker::PhantomData;
use std::ops::Deref;
use std::cmp::Ordering;

/// TODO: why use Deref<Target = Bucket>
pub struct Cursor<'a, B: Deref<Target = Bucket> + 'a> {
    pub(crate) bucket: B,
    pub(crate) stack: RefCell<Vec<ElemRef>>,
    _m: PhantomData<CursorItem<'a>>,
}

impl<'a, B: Deref<Target = Bucket> + 'a> Cursor<'a, B> {
    pub(crate) fn new(bucket: B) -> Self {
        Self {
            bucket,
            stack: RefCell::new(Vec::new()),
            _m: PhantomData,
        }
    }

    /// returns reference to bucket which is cursor created from
    pub(crate) fn bucket(&self) -> &Bucket {
        &*self.bucket
    }

    /// returns mutable reference to bucket which is cursor created from
    pub(crate) fn mut_bucket(&mut self) -> &mut Bucket {
        unsafe { &mut *((&*self.bucket) as *const Bucket as *mut Bucket) }
    }

    /// Recursively performs a binary search against a given page/node until it finds a given key.
    fn search(&self, key: &[u8], pg_id: PgId) -> Result<()> {
        let page_node = self.bucket().page_node(pg_id)?;
    }

    fn search_node(&self, key: &[u8], n: &Node) -> Result<()> {
        match n.0.inodes.borrow().binary_search_by(|inode| inode.key.as_slice().cmp(key)) {
            Ok(mut value) => {},

        }

        let (exact, mut index ) = match n.0.inodes.borrow().binary_search_by(|inode| inode.key.as_slice().cmp(key)) {
            Ok(mut value) => {

                let inodes = n.0.inodes.borrow();
                    match inode.key.as_slice().cmp(key) {
                        Ordering::Greater => break,
                        Ordering::Less => break,
                        Ordering::Equal => v = i,
                    }

            }
            Err(v) => (false, v),
        };

        if !exact && index >0 {
            index = -1;
        }

        self.stack.borrow_mut().last_mut().ok_or(Error::Unknown("stack empty")?).index = index;

        // Recursively search to the next page.
        let pg_id = n.0.inodes.borrow()[index].pg_id;
        self.search(key, pg_id)?;
        Ok(())
    }

    /// Returns the node that the cursor is currently positioned on.
    pub(crate) fn node(&mut self) ->Result<Node> {
        if self.stack.borrow().is_empty() {
            return Err(Error::StackEmpty);
        }

        // If the top of the stack is a leaf node then just return it.
        {
            let stack = self.stack.borrow();
            let el = &stack.last().ok_or(Error::StackEmpty)?;
            if el.is_leaf() && el.el.is_right() {
                return Ok(el.el.as_ref().right().unwrap().clone());
            }
        }

        // Start from root and traverse down the hierarchy.
        let mut n = {
            let el_ref = self.stack.borrow()[0].clone();
            match el_ref.upgrade() {
                Either::Left(p) => {
                    let id = p.id;
                    self.mut_bucket().node(id, WeakNode::new())
                }
                Either::Right(n) => {
                    n.clone()
                }
            }
        };

        for refi in self.stack.borrow().iter() {
            assert!(!n.is_leaf(), "expected branch");
            let child = n.child_at(refi.index).map_err(|_| Error::TraverserFailed)?;
            n = child;
        }

        assert!(n.is_leaf(), "expected leaf");
        Ok(n)
    }

    /// Returns the key and value of the current leaf element.
    fn key_value(&self) -> Result<CursorItem<'a>> {
        let stack = self.stack.borrow();
        let el_ref = stack.last().ok_or(Error::Unknown("stack is empty"))?;
        Ok(CursorItem::from(el_ref))
    }

    /// Removes the current key/value under the cursor from the bucket.
    /// Delete fails if current key/value is a bucket or if the transaction is not writable.
    pub fn delete(&mut self) -> Result<()> {
        if !self.bucket.tx()?.opened() {
            return Err(Error::TxClosed);
        }
        if !self.bucket.tx()?.writable() {
            return Err(Error::TxReadOnly);
        }

        let key = {
            let item = self.key_value()?;
            // Return an error if current value is a bucket
            if (item.flags & BUCKET_LEAF_FLAG) != 0 {
                return Err(Error::IncompatibleValue)
            }
            item.key.ok_or(Error::Unknown("key empty"))?.to_vec()
        };
        self.node()?.del(&key)
    }
}

// Represents a reference to an element on a given page/node.
#[derive(Clone, Debug)]
struct ElemRef {
    pub(crate) el: PageNode,
    pub(crate) index: usize,
}

impl Deref for ElemRef {
    type Target = PageNode;

    fn deref(&self) -> &Self::Target {
        &Self::Target(&self.el)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PageNode(Either<*const Page, Node>);

impl Deref for PageNode {
    type Target = Either<*const Page, Node>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<*const Page> for PageNode {
    fn from(p: *const Page) -> Self {
        Self(Either::Left(p))
    }
}

impl From<Node> for PageNode {
    fn from(node: Node) -> Self {
        Self(Either::Right(node))
    }
}

impl PageNode {
    pub(crate) fn get_page(&self) -> &Page {
        match self.0 {
            Either::Left(p) => unsafe { &*p },
            Either::Right(ref _n) => panic!("ElemRef not page"),
        }
    }

    pub(crate) fn upgrade(&self) -> Either<&Page, &Node> {
        match self.0 {
            Either::Left(p) => unsafe { Either::Left(&*p) },
            Either::Right(ref n) => Either::Right(n),
        }
    }

    pub(crate) fn is_leaf(&self) -> bool {
        match self.0 {
            Either::Left(_) => self.get_page().flags == LEAF_PAGE_FLAG,
            Either::Right(ref n) => n.is_leaf(),
        }
    }

    pub(crate) fn count(&self) -> usize {
        match self.0 {
            Either::Left(ref _p) => self.get_page().count as usize,
            Either::Right(ref n) => n.0.inodes.borrow().len(),
        }
    }
}

#[derive(Debug)]
pub struct CursorItem<'a> {
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    pub flags: u32,
}

impl<'a> CursorItem<'a> {
    pub(crate) fn new(key: Option<&'a [u8]>, value: Option<&'a [u8]>, flags: u32) -> Self {
        Self { key, value, flags }
    }

    #[inline]
    pub(crate) fn new_null(key: Option<&'a [u8]>, value: Option<&'a [u8]>, flags: u32) -> Self {
        CursorItem::new(key, value, 0)
    }

    /// returns true if key and value are None.
    pub fn is_none(&self) -> bool {
        self.key.is_none() && self.value.is_none()
    }

    /// returns true if key or value are Some.
    pub fn is_some(&self) -> bool {
        self.key.is_some() || self.value.is_some()
    }

    #[inline]
    pub fn is_bucket(&self) -> bool {
        self.flags & BUCKET_LEAF_FLAG != 0
    }

    /// unwraps item into key, value and flags.
    pub fn unwrap(&self) -> (Option<&'a [u8]>, Option<&'a [u8]>, u32) {
        (self.key, self.value, self.flags)
    }
}

impl<'a> From<&ElemRef> for CursorItem<'a> {
    fn from(el_ref: &ElemRef) -> Self {
        if el_ref.count() == 0 || el_ref.index >= el_ref.count() {
            return Self::new(None, None, 0);
        }

        unsafe {
            match el_ref.upgrade() {
                Either::Left(ref p) => {
                    let elem = p.leaf_page_element(el_ref.index);
                    Self::new(
                        Some(&*(elem.key() as *const [u8])),
                        Some(&*(elem.value() as *const [u8])),
                        elem.flags,
                    )
                }
                Either::Right(ref n) => {
                    let inode = &n.0.inodes.borrow()[el_ref.index];
                    Self::new(
                        Some(&*(inode.key.as_slice() as *const [u8])),
                        Some(&*(inode.value.as_slice() as *const [u8])),
                        inode.flags,
                    )
                }
            }
        }
    }
}
