use crate::cursor::{Cursor, PageNode};
use crate::error::{Error, Result};
use crate::node::{Node, NodeBuilder, WeakNode};
use crate::page::BUCKET_LEAF_FLAG;
use crate::tx::{WeakTx, TX};
use crate::{Page, PgId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::intrinsics::copy_nonoverlapping;
use std::sync::Weak;
use either::Either;
use crate::db::DB;

/// The maximum length of a key, in bytes.
const MAX_KEY_SIZE: u64 = 32768;
/// The maximum length of a value, in bytes.
const MAX_VALUE_SIZE: u64 = (1 << 31) - 2;

const MAX_UINT: u64 = 0;
const MIN_UINT: u64 = 0;
const MAX_INT: i64 = 0;
const MIN_INT: i64 = 0;

pub(crate) const MIN_FILL_PERCENT: f64 = 0.1;
pub(crate) const MAX_FILL_PERCENT: f64 = 1.0;

/// DefaultFillPercent is the percent that split pages are filled.
/// This value can be changed by setting Bucket.FillPercent.
const DEFAULT_FILL_PERCENT: f64 = 0.5;

/// subbucket represents the on-file representation of a bucket.
/// This is stored as the "value" of a bucket key. If the bucket is small enough,
/// then its root page can be stored inline in the "value", after the bucket
/// header. In the case of inline buckets, the "root" will be 0.
#[derive(Default, Debug, Clone)]
#[repr(C)]
pub struct SubBucket {
    /// page id of the bucket's root-level page
    pub root: PgId,
    /// monotonically incrementing, used by next_sequence()
    pub sequence: u64,
}

impl SubBucket {
    pub(crate) const SIZE: usize = std::mem::size_of::<Self>();
    pub(crate) fn new() -> SubBucket {
        SubBucket {
            root: 0,
            sequence: 0,
        }
    }
}

/// Bucket represents a collection of key/value pairs inside the database.
pub struct Bucket {
    pub(crate) sub_bucket: SubBucket,
    // the associated transaction, WeakTx
    pub(crate) tx: WeakTx,
    // subbucket cache
    pub(crate) buckets: RefCell<HashMap<Vec<u8>, Bucket>>,
    // inline page reference
    pub(crate) page: Option<Page>,
    // materialized node for the root page
    pub(crate) root_node: Option<Node>,
    // node cache
    pub(crate) nodes: HashMap<PgId, Node>,
    // Sets the threshold for filling nodes when they split. By default,
    // the bucket will fill to 50% but it can be useful to increase this
    // amount if you know that your write workloads are mostly append-only.
    //
    // This is non-persisted across transactions so it must be set in every Tx.
    pub(crate) fill_percent: f64,
}

impl Debug for Bucket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // let tx = self.tx();
        f.debug_struct("Bucket")
            .field("bucket", &self.sub_bucket)
            // .field("tx", &tx)
            .field("buckets", &self.buckets)
            .field("page", &self.page.as_ref())
            .field("root_node", &self.root_node)
            .field("nodes", &self.nodes)
            .field("fill_percent", &self.fill_percent)
            .finish()
    }
}

impl PartialEq for Bucket {
    fn eq(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

impl Eq for Bucket {}

impl Bucket {
    fn new(tx: WeakTx) -> Self {
        Bucket {
            sub_bucket: Default::default(),
            tx,
            buckets: RefCell::new(Default::default()),
            page: None,
            root_node: None,
            nodes: Default::default(),
            fill_percent: DEFAULT_FILL_PERCENT,
        }
    }

    /// Returns the tx of the bucket.
    pub fn tx(&self) -> Result<TX> {
        self.tx.upgrade().ok_or(Error::TxGone)
    }

    /// Returns the root of the bucket.
    pub(crate) fn root(&self) -> PgId {
        self.sub_bucket.root
    }

    fn root_node(&self) -> Option<Node> {
        self.root_node.clone()
    }

    /// Creates a cursor associated with the bucket.
    /// The cursor is only valid as long as the transaction is open.
    /// Do not use a cursor after the transaction is closed.
    pub fn cursor(&self) -> Result<Cursor<&Bucket>> {
        // update transaction statistics.
        self.tx()?.0.stats.lock().cursor_count += 1;
        // allocate and return a cursor.
        Ok(Cursor::new(self))
    }

    /// Returns whether the bucket is writable.
    pub fn writeable(&self) -> bool {
        self.tx().unwrap().writable()
    }

    /// Allocates and writes the bucket to a byte slice.
    fn write(&mut self) -> Vec<u8> {
        // Allocate the appropriate size.
        let n = self.root_node.as_ref().unwrap();
        let node_size = n.size();
        let mut value = vec![0u8; SubBucket::SIZE + node_size];

        // write a bucket header.
        let bucket_ptr = value.as_mut_ptr() as *mut SubBucket;
        unsafe { copy_nonoverlapping(&self.sub_bucket, bucket_ptr, 1) };

        // Convert byte slice to a fake page and write the root node.
        {
            let mut page = &mut value[SubBucket::SIZE..];
            let mut page = Page::from_slice_mut(&mut page);
            n.write(&mut page);
        }
        value
    }

    /// Attempts to balance all nodes
    pub(crate) fn rebalance(&mut self) {
        for node in self.nodes.values_mut() {
            node.rebalance();
        }
        for child in self.buckets.borrow_mut().values_mut() {
            child.rebalance();
        }
    }

    /// Create a `node` from a `page` and associates it with a given parent.
    pub(crate) fn node(&mut self, pg_id: PgId, parent: WeakNode) -> Node {
        assert!(!self.nodes.is_empty(), "nodes map expected");
        // Retrieve node if it's already been created.
        if let Some(node) = self.nodes.get(&pg_id) {
            return node.clone();
        }
        // Otherwise create a node and cache it.
        let mut node = NodeBuilder::new(self as *const Bucket)
            .parent(parent.clone())
            .build();
        if let Some(parent) = parent.upgrade() {
            parent.child_mut().push(node.clone());
        } else {
            self.root_node.replace(node.clone());
        }
        // Use the page into the node and cache it.

        if let Some(page) = &self.page {
            node.read(&page);
        } else {
            // Read the page into the node and cache it.
            let page = self.tx().unwrap().page(pg_id).unwrap();
            unsafe {
                node.read(&*page);
            }
        }
        self.nodes.insert(pg_id, node.clone());
        // Update statistics.
        self.tx().unwrap().stats().node_count += 1;
        node
    }

    /// Returns the in-memory node, if it exists.
    /// Otherwise returns the underlying page.
    pub(crate) fn page_node(&self, id: PgId) -> Result<PageNode> {
        // Inline buckets have fake page embedded in their value so treat them
        // differently. We'll return the rootNode (if available) or the fake page.
        if self.sub_bucket.root == 0 {
            if id != 0 {
                return Err(Error::Unexpected("inline bucket no-zero page access"));
            }
            if let Some(ref node) = self.root_node {
                return Ok(PageNode::from(node.clone()));
            }
            return Ok(PageNode::from(
                &*self.page.as_ref().ok_or(Error::Unexpected("page empty"))? as *const Page,
            ));
        }

        // Check the node cache for non-inline buckets.
        if let Some(node) = self.nodes.get(&id) {
            return Ok(PageNode::from(node.clone()));
        }

        // TODO Why?
        // Finally lookup the page from the transaction if no node is materialized.
        Ok(PageNode::from(self.tx()?.page(id)?))
    }

    pub fn clear(&mut self) {
        self.buckets.borrow_mut().clear();
        self.nodes.clear();
        self.page = None;
        self.root_node = None;
    }

    // Creates a new bucket at the given key and returns the new bucket.
    // Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
    // The bucket instance is only valid for the lifetime of the transaction.
    pub fn create_bucket(&mut self, key: &[u8]) -> Result<&mut Bucket> {
        {
            let tx = self.tx()?;
            if !tx.opened() {
                return Err(Error::TxClosed);
            }
            if !tx.writable() {
                return Err(Error::TxReadOnly);
            }
            if key.is_empty() {
                return Err(Error::BucketNameRequired);
            }
        }

        let tx = self.tx.clone();
        {
            // Move cursor to correct position.
            let mut cursor = self.cursor()?;
            let (ckey, _, flags) = cursor.seek_to_item(key)?.unwrap();

            // return an error if there is an existing cursor.
            if ckey == Some(key) {
                if (flags & BUCKET_LEAF_FLAG) != 0 {
                    return Err(Error::BucketExists);
                };
                return Err(Error::IncompatibleValue);
            }

            // create empty, inline bucket.
            let mut bucket = Bucket::new(tx);
            bucket.root_node = Some(NodeBuilder::new(&bucket).is_leaf(true).build());
            bucket.fill_percent = DEFAULT_FILL_PERCENT;

            let value = bucket.write();

            // insert into node.
            cursor
                .node()
                .unwrap()
                .put(key, key, value, 0, BUCKET_LEAF_FLAG)?;

            // TODO: why
            // since subbuckets are not allowed on inline buckets, we need to
            // dereference the inline page, if it exists. This will cause the bucket
            // to be treated as a regular, non-inline bucket for the rest of the tx.
            self.page = None;
        }

        self.bucket_mut(key)
            .ok_or_else(|| Error::Unexpected("cannot find bucket"))
    }

    // Creates a new bucket if it doesn't already exist and returns a reference to it.
    // Returns an error if the bucket name is blank, or if the bucket name is too long.
    // The bucket instance is only valid for the lifetime of the transaction.
    pub fn create_bucket_if_not_exists(&mut self, key: &[u8]) -> Result<&mut Bucket> {
        let other_self = unsafe { &mut *(self as *mut Self) };

        match other_self.create_bucket(key) {
            Ok(b) => Ok(b),
            Err(Error::BucketExists) => self
                .bucket_mut(key)
                .ok_or_else(|| Error::Unexpected("can't create bucket")),
            v => v,
        }
    }

    // DeleteBucket deletes a bucket at the given key.
    // Returns an error if the bucket does not exists, or if the key represents a non-bucket value.
    pub fn delete_bucket(&self, key: &[u8]) -> Result<()> {
        {
            let tx = self.tx()?;
            if !tx.opened() {
                return Err(Error::DatabaseNotOpen);
            }
            if !tx.writable() {
                return Err(Error::DatabaseOnlyRead);
            }
            if key.is_empty() {
                return Err(Error::NameRequired);
            }
        }

        let mut c = self.cursor()?;
        {
            let item = c.seek(key)?;
            if item.key.as_deref().map(|v| &*v).unwrap() != key {
                return Err(Error::BucketNotFound);
            }
            if !item.is_bucket() {
                return Err(Error::IncompatibleValue);
            }
        }

        let mut node = c.node()?;
        {
            let child = self
                .bucket_mut(key)
                .ok_or(Error::Unexpected("Can't get bucket"))?;
            let child_buckets = child.buckets();

            for bucket in &child_buckets {
                child.delete_bucket(bucket)?;
            }

            // Release all bucket pages to free_list.
            child.nodes.clear();
            child.root_node = None;
            child.free();
        }

        // self.buckets.borrow_mut().remove(&key);
        // node.del(key);

        Ok(())
    }

    pub(crate) fn buckets(&self) -> Vec<Vec<u8>> {
        self.buckets
            .borrow()
            .keys()
            .map(|key| key.to_owned())
            .collect()
    }

    pub fn bucket(&self, key: &[u8]) -> Option<&Bucket> {
        self.__bucket(key).map(|b| unsafe { &*b })
    }

    pub fn bucket_mut(&self, key: &[u8]) -> Option<&mut Bucket> {
        self.__bucket_mut(key).map(|b| unsafe { &mut *b })
    }

    pub fn free(&mut self) {
        if self.sub_bucket.root == 0 {
            return;
        }

        let tx = self.tx().unwrap();
        let db = tx.db().unwrap();
    }

    /// Iterates over every page in a bucket, including inline pages.
    pub(crate) fn for_each_page<'a>(&self, mut handler: Box<dyn FnMut(&Page, usize) + 'a>) {
        // If we have an inline page then just use that.
        if let Some(ref inline_page) = self.page {
            handler(&inline_page, 0);
            return;
        }

        // Otherwise traverse the page hierarchy.
        // self.tx().unwrap().for_each()
    }
    pub(crate) fn for_each_page_node<F>(&self, mut handler: F)
        where F: FnMut(Either<&Page, &Node>, isize) {
        // If we have an inline page then just use that.
        if let Some(ref page) = self.page {
            handler(Either::Left(page), 0);
        } else {
            self.__for_each_page_node(self.sub_bucket.root, 0, &mut handler);
        }
    }

    // Pre-Order Traversal
    fn __for_each_page_node<F>(&self, pgid: PgId, depth: isize, handler: &mut F) where
        F: FnMut(Either<&Page, &Node>, isize)
    {
        let item = self.page_node(pgid).unwrap();
        handler(item.upgrade(), depth);
        // Recursively loop over children
        match item.upgrade() {
            Either::Left(page) => {
                if page.is_branch() {
                    for i in 0..page.count as usize {
                        let elem = page.branch_page_element(i);
                        self.__for_each_page_node(elem.pgid, depth + 1, handler);
                    }
                }
            }
            Either::Right(node) => {
                if !node.is_leaf() {
                    for inode in &*node.0.inodes.borrow() {
                        self.__for_each_page_node(inode.pg_id, depth + 1, handler)
                    }
                }
            }
        }
    }

    pub fn __bucket(&self, key: &[u8]) -> Option<*const Bucket> {
        if let Some(b) = self.buckets.borrow().get(&key.to_vec()) {
            return Some(b);
        }
        None
    }

    pub(crate) fn __bucket_mut(&self, key: &[u8]) -> Option<*mut Bucket> {
        if let Some(b) = self.buckets.borrow_mut().get_mut(&key.to_vec()) {
            return Some(b);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
