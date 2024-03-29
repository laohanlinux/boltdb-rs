use crate::cursor::{Cursor, PageNode};
use crate::db::{Stats, DB};
use crate::error::{Error, Result};
use crate::node::{Node, NodeBuilder, WeakNode};
use crate::page::{ElementSize, OwnedPage, Page, PgId, BUCKET_LEAF_FLAG, PAGE_HEADER_SIZE};
use crate::tx::{WeakTx, TX};
use either::Either;
use log::{debug, info, warn};
use std::cell::RefCell;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::intrinsics::copy_nonoverlapping;
use std::ops::AddAssign;
use std::sync::atomic::Ordering;
use std::sync::Weak;

/// The maximum length of a key, in bytes.
const MAX_KEY_SIZE: usize = 32768;
/// The maximum length of a value, in bytes.
const MAX_VALUE_SIZE: usize = (1 << 31) - 2;

pub(crate) const MIN_FILL_PERCENT: f64 = 0.1;
pub(crate) const MAX_FILL_PERCENT: f64 = 1.0;

/// DefaultFillPercent is the percent that split pages are filled.
/// This value can be changed by setting Bucket.FillPercent.
pub(crate) const DEFAULT_FILL_PERCENT: f64 = 0.5;

/// subbucket represents the on-file representation of a bucket.
/// This is stored as the "value" of a bucket key. If the bucket is small enough,
/// then its root page can be stored inline in the "value", after the bucket
/// header. In the case of inline buckets, the "root" will be 0.
#[derive(Default, Debug, Clone)]
#[repr(C)]
pub struct TopBucket {
    /// page id of the bucket's root-level page
    pub root: PgId,
    /// monotonically incrementing, used by next_sequence()
    pub sequence: u64,
}

impl TopBucket {
    pub(crate) const SIZE: usize = std::mem::size_of::<Self>();
}

/// Bucket represents a collection of key/value pairs inside the database.
#[derive(Debug)]
pub struct Bucket {
    pub(crate) local_bucket: TopBucket,
    // the associated transaction, WeakTx
    pub(crate) tx: WeakTx,
    // subbucket cache
    pub(crate) buckets: RefCell<HashMap<Vec<u8>, Bucket>>,
    // inline page reference
    pub(crate) page: Option<OwnedPage>,
    // materialized node for the root page
    pub(crate) root_node: Option<Node>,
    // node cache
    // TODO: maybe use refHashMap
    pub(crate) nodes: RefCell<HashMap<PgId, Node>>,
    // Sets the threshold for filling nodes when they split. By default,
    // the bucket will fill to 50% but it can be useful to increase this
    // amount if you know that your write workloads are mostly append-only.
    //
    // This is non-persisted across transactions so it must be set in every Tx.
    pub(crate) fill_percent: f64,
}

impl Bucket {
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
                return Err(Error::TxNoWritable);
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
            info!(node_size = self.nodes.borrow().keys().len() ; "insert a new bucket into node");
            // TODO: why
            // since subbuckets are not allowed on inline buckets, we need to
            // dereference the inline page, if it exists. This will cause the bucket
            // to be treated as a regular, non-inline bucket for the rest of the tx.
            self.page = None;
        }
        self.bucket_mut(key)
            .ok_or(Error::Unexpected("cannot find bucket"))
    }

    // Creates a new bucket if it doesn't already exist and returns a reference to it.
    // Returns an error if the bucket name is blank, or if the bucket name is too long.
    // The bucket instance is only valid for the lifetime of the transaction.
    pub fn create_bucket_if_not_exists(&mut self, key: &[u8]) -> Result<&mut Bucket> {
        let other_self = unsafe { &mut *(self as *mut Self) };

        match other_self.create_bucket(key) {
            Ok(b) => Ok(b),
            Err(Error::BucketExists) => {
                info!("has exists the bucket: {}", String::from_utf8_lossy(key));
                self.bucket_mut(key)
                    .ok_or(Error::Unexpected("can't create bucket"))
            }
            v => v,
        }
    }

    // DeleteBucket deletes a bucket at the given key.
    // Returns an error if the bucket does not exist, or if the key represents a non-bucket value.
    pub fn delete_bucket(&mut self, key: &[u8]) -> Result<()> {
        {
            let tx = self.tx()?;
            if !tx.opened() {
                return Err(Error::DatabaseNotOpen);
            }
            if !tx.writable() {
                return Err(Error::TxNoWritable);
            }
            if key.is_empty() {
                return Err(Error::KeyRequired);
            }
        }

        let mut c = self.cursor()?;
        {
            let item = c.seek(key)?;
            if item.key.unwrap() != key {
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

            // delete children buckets
            for bucket in &child_buckets {
                child.delete_bucket(bucket)?;
            }

            // Release all bucket pages to free_list.
            child.nodes.borrow_mut().clear();
            child.root_node = None;
            child.free();
        }

        self.buckets.borrow_mut().remove(key);
        node.del(key);

        Ok(())
    }

    /// Returns list of subbuckets's keys
    pub fn buckets(&self) -> Vec<Vec<u8>> {
        let mut names = Vec::new();
        self.for_each(|key, value| -> Result<()> {
            if value.is_none() {
                // only return bucket item
                names.push(key.to_vec());
            }
            Ok(())
        })
        .unwrap();
        names
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

    pub fn clear(&mut self) {
        self.buckets.borrow_mut().clear();
        self.nodes.borrow_mut().clear();
        self.page.take();
        self.root_node.take();
    }

    /// Retrieves a nested mutable bucket by name.
    /// Returns None if the bucket does not exist or found item is not bucket.
    pub fn bucket_mut(&mut self, key: &[u8]) -> Option<&mut Bucket> {
        if !self.tx().unwrap().writable() {
            return None;
        }
        self.__bucket(key).map(|b| unsafe { &mut *b })
    }

    /// Retrieves the value for a key in the bucket.
    /// Returns None if the key does not exist of if the key is a nested bucket.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        let (ckey, value, flag) = self.cursor().unwrap().seek(key).unwrap().unwrap();
        if flag & BUCKET_LEAF_FLAG != 0 {
            return None;
        }
        // If our target node isn't the same key as what's passed in then return nil.
        if ckey != Some(key) {
            return None;
        }
        value
    }

    /// Sets the value for a key in the bucket.
    /// If the key already exists then its previous value will be overwritten.
    /// Returns an error if the bucket was created from a read-only transaction, if the key is blank,
    /// if the key is too large, or if the value is too large.
    pub fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        if !self.tx()?.opened() {
            return Err(Error::TxClosed);
        }
        if !self.tx()?.writable() {
            return Err(Error::TxNoWritable);
        }
        if key.is_empty() {
            return Err(Error::KeyRequired);
        }
        if key.len() > MAX_KEY_SIZE {
            return Err(Error::KeyTooLarge);
        }
        if value.len() > MAX_VALUE_SIZE {
            return Err(Error::ValueTooLarge);
        }
        let mut c = self.cursor()?;
        let item = c.seek(key)?;
        if (Some(key) == item.key) && item.is_bucket() {
            return Err(Error::IncompatibleValue);
        }
        c.node().unwrap().put(key, key, value, 0, 0)
    }

    /// Removes a key from the bucket.
    /// If the key does not exist then nothing is done.
    /// Returns the error if the bucket was created from a read-only transaction.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        if self.tx()?.db().is_err() {
            return Err(Error::TxClosed);
        }
        if !self.tx()?.writable() {
            return Err(Error::TxNoWritable);
        }
        let mut c = self.cursor()?;
        let item = c.seek(key)?;

        if item.is_bucket() {
            return Err(Error::IncompatibleValue);
        }
        // info!("it should be not happen");
        c.node().unwrap().del(key);
        Ok(())
    }

    /// Executions a function for each key/value pair in a bucket.
    /// If the provided function returns an error the the interaction is stopped and
    /// the error is returned to the caller.
    pub fn for_each(
        &self,
        mut handler: impl FnMut(&[u8], Option<&[u8]>) -> Result<()>,
    ) -> Result<()> {
        if !self.tx()?.opened() {
            return Err(Error::TxClosed);
        }
        let c = self.cursor()?;
        let mut item = c.first()?;
        loop {
            if item.is_none() {
                break;
            }
            handler(item.key.unwrap(), item.value)?;
            item = c.next()?;
        }

        Ok(())
    }

    /// Returns stats on a bucket.
    /// todo
    pub fn stats(&self) -> BucketStats {
        let mut s = BucketStats::default();
        let mut sub_stats = BucketStats::default();
        let page_size = self.tx().unwrap().db().unwrap().page_size();
        s.bucket_n += 1;
        if self.root() == 0 {
            s.inline_bucket_n += 1;
        }
        let handler = Box::new(|page: &Page, depth: usize| {
            if page.is_leaf() {
                s.key_n += page.count as isize;
                // used totals the used bytes for the page.
                let used = page.byte_size();
                info!("{:?}, {}", page.id, used);
                if self.root() == 0 {
                    // For inlined bucket just update the line stats.
                    s.inline_bucket_inuse += used as isize;
                } else {
                    // For non-inlined bucket update all the leaf stats.
                    s.leaf_page_n += 1;
                    s.leaf_inuse += used as isize;
                    s.leaf_over_flow_n += page.over_flow as isize;

                    // Collect stats from sub-buckets.
                    // Do that by iterating over all element headers
                    // looking for the ones which the BUCKET_LEAF_FLAG
                    for i in 0..page.count as usize {
                        let e = &page.leaf_page_elements()[i];
                        if e.is_bucket_flag() {
                            // For any bucket element, open the element value
                            // and recursively call stats on the contained bucket.
                            sub_stats += self.open_bucket(e.value().to_vec()).stats();
                        }
                    }
                }
            } else if page.is_branch() {
                s.branch_page_n += 1;
                let last_element = page.branch_page_elements().last().unwrap();

                // used totals the used bytes for the page
                // Add header and all element headers.
                s.branch_inuse += page.byte_size() as isize;
                s.branch_over_flow_n += page.over_flow as isize;
            }

            // Keep track of maximum page depth
            if depth + 1 > s.depth as usize {
                s.depth = depth as isize + 1;
            }
        });
        self.for_each_page(handler);

        // Alloc stats can be computed from page counts and page_size.
        s.branch_alloc = (s.branch_page_n + s.branch_over_flow_n) * page_size as isize;
        s.leaf_alloc = (s.leaf_page_n + s.leaf_over_flow_n) * page_size as isize;

        // Add the max depth of sub-buckets to get total nested depth.
        s.depth += sub_stats.depth;
        // Add the stats for all sub-buckets
        s += sub_stats;
        s
    }
}

#[derive(Default, Debug)]
pub struct BucketStats {
    /// page count Stats
    /// number of logical branch pages.
    branch_page_n: isize,
    // number of physical branch overflow page.
    branch_over_flow_n: isize,
    // number of logical leaf pages.
    leaf_page_n: isize,
    // number of physical leaf overflow pages.
    leaf_over_flow_n: isize,

    // Tree statistics.
    // number of keys/value pairs
    key_n: isize,
    // number of levels in B+tree(including inlineable page level)
    depth: isize,

    // Page size utilization.
    // bytes allocated for physical branch pages.
    branch_alloc: isize,
    // bytes actually used for physical branch data.
    branch_inuse: isize,
    // bytes allocated for physical leaf pages.
    leaf_alloc: isize,
    // bytes actually used for physical leaf data.
    leaf_inuse: isize,

    // Bucket statistics.
    // total number of buckets including the top bucket.
    bucket_n: isize,
    // total number of inlined buckets.
    inline_bucket_n: isize,
    // bytes used for inlined buckets (also accounted for in LeafInuse)
    inline_bucket_inuse: isize,
}

impl AddAssign for BucketStats {
    fn add_assign(&mut self, rhs: Self) {
        self.branch_page_n += rhs.branch_page_n;
        self.branch_over_flow_n += rhs.branch_over_flow_n;
        self.leaf_page_n += rhs.leaf_page_n;

        self.key_n += rhs.key_n;
        if self.depth < rhs.depth {
            self.depth = rhs.depth;
        }

        self.branch_alloc += rhs.branch_alloc;
        self.branch_inuse += rhs.branch_inuse;
        self.leaf_alloc += rhs.leaf_alloc;
        self.leaf_inuse += rhs.leaf_inuse;

        self.bucket_n += rhs.bucket_n;
        self.inline_bucket_n += rhs.inline_bucket_n;
        self.inline_bucket_inuse += rhs.inline_bucket_inuse;
    }
}

impl Bucket {
    pub(crate) fn new(tx: WeakTx) -> Self {
        Bucket {
            local_bucket: Default::default(),
            tx,
            buckets: RefCell::new(Default::default()),
            page: None,
            root_node: None,
            nodes: Default::default(),
            fill_percent: DEFAULT_FILL_PERCENT,
        }
    }

    /// Returns the tx of the bucket.
    pub(crate) fn tx(&self) -> Result<TX> {
        self.tx.upgrade().ok_or(Error::TxClosed)
    }

    /// Returns the root of the bucket.
    pub(crate) fn root(&self) -> PgId {
        self.local_bucket.root
    }

    /// Returns whether the bucket is writable.
    pub(crate) fn writeable(&self) -> bool {
        self.tx().unwrap().writable()
    }

    /// Allocates and writes the bucket to a byte slice.
    fn write(&mut self) -> Vec<u8> {
        // Allocate the appropriate size.
        let n = self.root_node.as_ref().unwrap();
        let node_size = n.size();
        let mut value = vec![0u8; TopBucket::SIZE + node_size];

        // write a bucket header.
        let bucket_ptr = value.as_mut_ptr() as *mut TopBucket;
        unsafe { copy_nonoverlapping(&self.local_bucket, bucket_ptr, 1) };

        // Convert byte slice to a fake page and write the root node.
        {
            let mut page = &mut value[TopBucket::SIZE..];
            let mut page = Page::from_slice_mut(&mut page);
            n.write(&mut page);
        }
        value
    }

    /// Attempts to balance all nodes
    pub(crate) fn rebalance(&mut self) {
        let pid = self.local_bucket.root;
        info!(
            "ready to rebalance bucket, pid:{}, nodes:{}, buckets: {}",
            pid,
            self.nodes.borrow().len(),
            self.buckets.borrow().len()
        );
        let mut dirty = Vec::with_capacity(self.nodes.borrow().len());
        for node in self.nodes.borrow().values() {
            node.rebalance(&mut dirty);
        }
        for pgid in dirty {
            if let Some(entry) = self.nodes.borrow_mut().remove(&pgid) {
                entry.free();
            }
        }

        for child in self.buckets.borrow_mut().values_mut() {
            child.rebalance();
        }
    }

    /// Create a `node` from a `page` and associates it with a given parent.
    pub(crate) fn node(&mut self, pg_id: PgId, parent: WeakNode) -> Node {
        debug!("load node from page: {}", pg_id);
        // assert!(!self.nodes.is_empty(), "nodes map expected");
        if !self.tx().unwrap().writable() {
            panic!("tx is read-only");
        }
        // Retrieve node if it's already been created.
        if let Some(node) = self.nodes.borrow().get(&pg_id) {
            return node.clone();
        }
        // Otherwise, create a node and cache it.
        let mut node = NodeBuilder::new(self as *const Bucket)
            .parent(parent.clone())
            .build();
        if let Some(parent) = parent.upgrade() {
            parent.child_mut().push(node.clone());
        } else {
            // Why? Ony root node has not parent, so the node is root node
            // Update it
            self.root_node.replace(node.clone());
        }

        // Use the page into the node and cache it.
        if let Some(page) = &self.page {
            node.read(page);
        } else {
            // Read the page into the node and cache it.
            let page = unsafe { &*self.tx().unwrap().page(pg_id).unwrap() };
            // warn!("read node from inline page:  {:?}", page.as_slice());
            unsafe {
                node.read(page);
            }
        }
        // debug!("cache a node: {:?}", node);
        self.nodes.borrow_mut().insert(pg_id, node.clone());
        // Update statistics.
        self.tx().unwrap().stats().node_count += 1;
        node
    }

    /// Returns the in-memory node, if it exists.
    /// Otherwise returns the underlying page.
    pub(crate) fn page_node(&self, id: PgId) -> Result<PageNode> {
        // Inline buckets have fake page embedded in their value so treat them
        // differently. We'll return the rootNode (if available) or the fake page.
        if self.local_bucket.root == 0 {
            if id != 0 {
                return Err(Error::Unexpected("inline bucket no-zero page access"));
            }
            // TODO: when happen
            if let Some(ref node) = self.root_node {
                // debug!("return a inline root_node");
                return Ok(PageNode::from(node.clone()));
            }
            debug!("return a inline root page");
            return Ok(PageNode::from(
                &**self.page.as_ref().ok_or("page empty")? as *const Page
            ));
        }
        // Check the node cache for non-inline buckets.
        if let Some(node) = self.nodes.borrow().get(&id) {
            return Ok(PageNode::from(node.clone()));
        }

        // TODO Why?
        // Finally lookup the page from the transaction if no node is materialized.
        Ok(PageNode::from(self.tx()?.page(id)?))
    }

    /// Helper method that re-interprets a sub-bucket value
    /// from a parent into a Bucket.
    ///
    /// value is bytes serialized bucket
    pub(crate) fn open_bucket(&self, value: Vec<u8>) -> Bucket {
        let mut child = Bucket::new(self.tx.clone());

        {
            let b = unsafe { (&*(value.as_ptr() as *const TopBucket)).clone() };
            child.local_bucket = b;
        }
        // Save reference to the inline page if the bucket is inline.
        if child.local_bucket.root == 0 {
            let data = unsafe {
                let slice = &value[TopBucket::SIZE..];
                // cull TopBucket
                let mut vec = vec![0u8; slice.len()];
                copy_nonoverlapping(slice.as_ptr(), vec.as_mut_ptr(), slice.len());
                vec
            };
            let page = OwnedPage::from_vec(data);
            child.page = Some(page);
        }

        child
    }

    /// Recursively frees all pages in the bucket.
    pub(crate) fn free(&mut self) {
        // This is an inline bucket, don't need to free it.
        if self.local_bucket.root == 0 {
            debug!("the bucket is inline root, nothing frees");
            return;
        }

        let tx = self.tx().unwrap();
        let db = tx.db().unwrap();
        self.for_each_page_node(|page, _| match page {
            Either::Left(page) => {
                let txid = tx.id();
                db.0.free_list.write().free(txid, page);
            }
            Either::Right(node) => node.clone().free(),
        });
        self.local_bucket.root = 0;
    }

    /// Iterates over every page in a bucket, including inline pages.
    pub(crate) fn for_each_page<'a>(&self, mut handler: Box<dyn FnMut(&Page, usize) + 'a>) {
        // If we have an inline page then just use that.
        if let Some(ref inline_page) = self.page {
            handler(inline_page, 0);
            return;
        }

        // Otherwise traverse the page hierarchy.
        self.tx()
            .unwrap()
            .for_each_page(self.local_bucket.root, 0, handler);
    }

    /// Iterates over every page (or node) in a bucket.
    /// This also includes inline pages.
    pub(crate) fn for_each_page_node<F>(&self, mut handler: F)
    where
        F: FnMut(Either<&Page, &Node>, isize),
    {
        // If we have an inline page then just use that.
        if let Some(ref page) = self.page {
            handler(Either::Left(page), 0);
        } else {
            self.__for_each_page_node(self.local_bucket.root, 0, &mut handler);
        }
    }

    // Pre-Order Traversal
    fn __for_each_page_node<F>(&self, pgid: PgId, depth: isize, handler: &mut F)
    where
        F: FnMut(Either<&Page, &Node>, isize),
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

    /// Writes all the nodes for this bucket to dirty pages.
    pub(crate) fn spill(&mut self) -> Result<()> {
        let root_node = self
            .root_node
            .as_ref()
            .map(|node| node.pg_id())
            .unwrap_or(0);
        debug!(local_bucket = self.local_bucket.root, root_node=root_node; "ready to spill bucket");
        let mutself = unsafe { &mut *(self as *mut Self) };

        // Spill all child buckets first.
        for (name, child) in &mut *self.buckets.borrow_mut() {
            // If the child bucket is small enough and it has no child buckets then
            // write it inline into the parent bucket's page. Otherwise spill it
            // like a normal bucket and make the parent value a pointer to the page.
            let value = if child.inlineable() {
                child.free();
                child.write()
            } else {
                child.spill()?;
                // Update the child bucket header in this bucket.
                let mut vec = vec![0u8; TopBucket::SIZE];
                let bucket_ptr = vec.as_mut_ptr() as *mut TopBucket;
                unsafe { copy_nonoverlapping(&child.local_bucket, bucket_ptr, 1) };
                vec
            };
            // Skip writing the bucket if there are no materialized node.
            if child.root_node.is_none() {
                continue;
            }
            // Update parent node.
            let mut c = mutself.cursor()?;
            let item = c.seek(name)?;
            if item.key != Some(name.as_slice()) {
                return Err(Error::Unexpected2(format!(
                    "misplaced bucket header: {:?} -> {:?}",
                    name,
                    item.key.as_ref().unwrap()
                )));
            }
            if !item.is_bucket() {
                return Err(Error::Unexpected2(format!(
                    "unexpected bucket header flag: {}",
                    item.flags
                )));
            }
            let _ = c.node()?.put(name, name, value, 0, BUCKET_LEAF_FLAG);
        }

        // ignore if there's not a materialized root node.
        if self.root_node.is_none() {
            return Ok(());
        }

        {
            // Spill nodes.
            let mut root_node = self
                .root_node
                .clone()
                .ok_or(Error::Unexpected("root node empty"))?
                .root();
            // info!(
            //     "start to split node from root_node: {:?}, inodes: {}",
            //     root_node.pg_id(),
            //     root_node.0.inodes.borrow().len()
            // );
            root_node.spill()?;
            self.root_node = Some(root_node);

            let pgid = self.root_node.as_ref().unwrap().pg_id();
            let txpgid = self.tx()?.pgid();
            // Update the root node for this bucket.
            if pgid >= txpgid as u64 {
                panic!("pgid ({}) above high water mark ({})", pgid, txpgid);
            }

            self.local_bucket.root = pgid;
        }

        Ok(())
    }

    /// Returns true if a bucket is small enough to be written inline
    /// and if it contains no subbuckets. Otherwise returns false.
    fn inlineable(&self) -> bool {
        let can_inlineable = self.__inlineable();
        // info!(
        //     "bucket(root_node: {:?}) can inlineable: {}",
        //     self.local_bucket,
        //     // self.root_node,
        //     can_inlineable
        // );
        can_inlineable
    }

    fn __inlineable(&self) -> bool {
        if self.root_node.is_none() || !self.root_node.as_ref().unwrap().is_leaf() {
            return false;
        }

        let mut size = PAGE_HEADER_SIZE;
        let node = self.root_node.clone().unwrap();

        for inode in &*node.0.inodes.borrow() {
            if inode.flags & BUCKET_LEAF_FLAG != 0 {
                return false;
            }

            size += ElementSize::Leaf.bits() + inode.key.len() + inode.value.len();
            if size > self.max_inline_bucket_size() {
                return false;
            }
        }

        true
    }

    /// Returns the maximum total size of a bucket to make it a candidate for inlining.
    fn max_inline_bucket_size(&self) -> usize {
        self.tx().unwrap().db().unwrap().page_size() / 4
    }

    /// Retrieves a nested bucket by name.
    /// Returns None if the bucket does not exits or found item is not bucket.
    pub(crate) fn bucket(&self, key: &[u8]) -> Option<&Bucket> {
        self.__bucket(key).map(|b| unsafe { &*b })
    }

    fn __bucket(&self, name: &[u8]) -> Option<*mut Bucket> {
        if let Some(b) = self.buckets.borrow_mut().get_mut(&name.to_vec()) {
            return Some(b);
        }

        let (key, value) = {
            let c = self.cursor().unwrap();
            let (key, value, flags) = c.seek_to_item(name).unwrap().unwrap();

            // Return None if the key doesn't exist or it is not a bucket.
            if key != Some(name) || (flags & BUCKET_LEAF_FLAG) == 0 {
                return None;
            }

            (key.map(|k| k.to_vec()), value.map(|v| v.to_vec()))
        };
        // Otherwise, create a bucket and cache it.
        let child = self.open_bucket(value.unwrap());
        let mut buckets = self.buckets.borrow_mut();
        let bucket = match buckets.entry(key.unwrap()) {
            Vacant(e) => e.insert(child),
            Occupied(e) => {
                let c = e.into_mut();
                *c = child;
                c
            }
        };
        // info!(
        //     "loader a bucket {:?}, root: {:?}, root_node: {}",
        //     String::from_utf8_lossy(name),
        //     bucket.local_bucket,
        //     bucket.root_node.is_some()
        // );
        Some(bucket)
    }

    /// Returns the current integer for the bucket without incrementing it.
    pub(crate) fn sequence(&self) -> u64 {
        self.local_bucket.sequence
    }

    pub(crate) fn next_sequence(&mut self) -> Result<u64> {
        if !self.tx()?.writable() {
            return Err(Error::TxNoWritable);
        }
        if self.root_node.is_none() {
            self.node(self.root(), WeakNode::new());
        }

        self.local_bucket.sequence += 1;
        Ok(self.local_bucket.sequence)
    }

    pub(crate) fn set_sequence(&mut self, seq: u64) -> Result<()> {
        if !self.tx()?.writable() {
            return Err(Error::TxNoWritable);
        }
        // load root node
        if self.root_node.is_none() {
            let pgid = self.root();
            self.node(pgid, WeakNode::new());
        }
        self.local_bucket.sequence = seq;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::error::Error::{IncompatibleValue, Unexpected};
    use crate::test_util::{mock_db, mock_log};
    use byteordered::byteorder::{BigEndian, WriteBytesExt};
    use log::kv::ToKey;
    use log::{error, info};
    use rand::Rng;
    use std::borrow::Borrow;
    use std::io::repeat;
    use std::iter::FromIterator;
    use std::time::SystemTime;

    #[test]
    fn create() {
        let db = mock_db().build().unwrap();
        let mut tx = db.begin_rw_tx().unwrap();
        assert!(tx.bucket(b"foo").is_err());
        let mut bucket = tx.create_bucket(b"foo").unwrap();
        assert!(bucket.get(b"bar").is_none());

        bucket.put(b"foo", b"jaja".to_vec()).unwrap();
        assert!(bucket.get(b"foo").is_some());
        assert_eq!(bucket.get(b"foo").unwrap(), b"jaja");

        bucket.create_bucket(b"subbucket").unwrap();
        assert!(bucket.get(b"subbucket").is_none());
        assert!(bucket.bucket(b"subbucket").is_some());
    }

    #[test]
    fn create_nested_bucket() {
        let path = {
            let db = mock_db().set_auto_remove(false).build().unwrap();
            let mut tx = db.begin_rw_tx().unwrap();
            assert!(tx.bucket(b"foo").is_err());

            tx.create_bucket(b"foo")
                .unwrap()
                .create_bucket(b"foob")
                .unwrap()
                .create_bucket(b"fooc")
                .unwrap()
                .create_bucket(b"food")
                .unwrap()
                .create_bucket(b"fooe")
                .unwrap();

            assert!(tx
                .bucket(b"foo")
                .unwrap()
                .bucket(b"foob")
                .unwrap()
                .bucket(b"fooc")
                .unwrap()
                .bucket(b"food")
                .unwrap()
                .bucket(b"fooe")
                .is_some());

            tx.commit().unwrap();
            db.path().unwrap()
        };

        let db = mock_db().set_path(path).build().unwrap();
        let tx = db.begin_tx().unwrap();
        assert!(tx
            .bucket(b"foo")
            .unwrap()
            .bucket(b"foob")
            .unwrap()
            .bucket(b"fooc")
            .unwrap()
            .bucket(b"food")
            .unwrap()
            .bucket(b"fooe")
            .is_some());
    }

    #[test]
    fn delete_value() {
        let mut db = mock_db().build().unwrap();
        let mut tx = db.begin_rw_tx().unwrap();
        {
            let mut bucket = tx.create_bucket(b"bucket").unwrap();
            bucket.put(b"haley", b"smith".to_vec()).unwrap();
            assert_eq!(bucket.get(b"haley").unwrap(), b"smith");
            bucket.delete(b"haley").unwrap();
            assert_eq!(bucket.get(b"haley"), None);
        }
        tx.commit().unwrap();
    }

    #[test]
    fn delete_bucket_err() {
        let mut db = mock_db().build().unwrap();
        let mut tx = db.begin_rw_tx().unwrap();
        {
            let mut bucket = tx.create_bucket(b"bucket").unwrap();
            bucket.create_bucket(b"stan").unwrap();
            bucket.bucket(b"stan").unwrap();
            assert_eq!(bucket.delete(b"stan").unwrap_err(), IncompatibleValue);
        }
        tx.commit().unwrap();
    }

    #[test]
    fn put_repeat() {
        let mut db = mock_db().build().unwrap();
        db.update(|tx| {
            {
                let mut bucket = tx.create_bucket(b"widgets").unwrap();
                bucket.put(b"foo", b"bar".to_vec()).unwrap();
                bucket.put(b"foo", b"bar".to_vec()).unwrap();
            }
            let value = tx.bucket(b"widgets").unwrap().get(b"foo").unwrap().to_vec();
            assert_eq!(value, Vec::from("bar"));
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn put_large() {
        let mut db = mock_db().build().unwrap();
        let count = 100;
        let factor = 200;
        let ok = db.update(|tx| {
            let mut bucket = tx.create_bucket(b"widgets").unwrap();
            for i in 1..count {
                let key = [0].repeat(i * factor);
                let value = [0].repeat((count - i) * factor);
                let ok = bucket.put(&key, value);
                assert!(ok.is_ok());
            }

            Ok(())
        });
        assert!(ok.is_ok());
    }

    // #[test]
    // fn put_very_large() {
    //     let mut db = mock_db().build().unwrap();
    //     let k_size = 8;
    //     let v_value = 500;
    //     let (n, batch_n) = (400000, 200000);
    //
    //     for i in (0..n).step_by(batch_n) {
    //         db.update(|tx| {
    //             let mut bucket = tx.create_bucket_if_not_exists(b"widgets").unwrap();
    //             for j in 0..batch_n {
    //                 let mut key = [0].repeat(k_size);
    //                 let mut value = [0].repeat(v_value);
    //                 key.write_u32::<BigEndian>((i + j) as u32).unwrap();
    //                 let ok = bucket.put(&key, value);
    //                 assert!(ok.is_ok());
    //             }
    //             Ok(())
    //         });
    //     }
    // }

    #[test]
    fn put_close() {
        let db = mock_db().build().unwrap();
        let mut tx = db.begin_rw_tx().unwrap();

        {
            let bucket = tx.create_bucket(b"widgets").unwrap();
        }
        tx.rollback().unwrap();
    }

    #[test]
    fn bucket_delete() {
        let db = mock_db().build().unwrap();
        db.update(|tx| {
            let mut bucket = tx.create_bucket(b"widgets").unwrap();
            bucket.put(b"foo", b"bar".to_vec()).unwrap();
            bucket.delete(b"foo").unwrap();
            let ok = bucket.get(b"foo");
            assert!(ok.is_none());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn bucket_delete_large() {
        let db = mock_db().build().unwrap();
        db.update(|tx| {
            let mut bucket = tx.create_bucket(b"widgets").unwrap();
            for i in 0..100 {
                let value = vec![0; 1024];
                bucket.put(format!("{}", i).as_bytes(), value).unwrap();
            }
            Ok(())
        })
        .unwrap();

        db.update(|tx| {
            let mut bucket = tx.bucket_mut(b"widgets").unwrap();
            for i in 0..100 {
                bucket.delete(format!("{}", i).as_bytes()).unwrap();
            }
            Ok(())
        })
        .unwrap();

        db.view(|tx| {
            let bucket = tx.bucket(b"widgets").unwrap();
            for i in 0..100 {
                let value = bucket.get(format!("{}", i).as_bytes());
                assert!(value.is_none());
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    #[cfg(feature = "t_overflow")]
    fn bucket_delete_freelist_overflow() {
        use std::io::Write;
        let db = mock_db().build().unwrap();
        let mut k = [0u8; 16].to_vec();
        for i in 0..10000 {
            let ok = db.update(|tx| {
                let mut bucket = tx.create_bucket_if_not_exists(b"0").unwrap();
                for j in 0..1000 {
                    k.write_u64::<BigEndian>(i).unwrap();
                    k.write_u64::<BigEndian>(j).unwrap();
                    let err = bucket.put(&k, vec![]);
                    k.clear();
                    assert!(err.is_ok());
                }
                Ok(())
            });
            assert!(ok.is_ok());
        }

        let err = db.update(|tx| {
            let mut bucket = tx.bucket_mut(b"0").unwrap();
            let mut c = bucket.cursor().unwrap();
            for k in c.next() {
                let err = c.delete();
                assert!(err.is_ok());
            }
            Ok(())
        });
        assert!(err.is_ok());
    }

    #[test]
    fn bucket_nested() {
        let db = mock_db().build().unwrap();
        let err = db.update(|tx| {
            let mut bt = tx.create_bucket(b"widgets").unwrap();
            let _ = bt.create_bucket(b"foo").unwrap();
            let err = bt.put(b"bar", b"0000".to_vec());
            assert!(err.is_ok());
            Ok(())
        });
        assert!(err.is_ok());
        db.must_check();

        let err = db.update(|tx| {
            let mut bucket = tx.bucket_mut(b"widgets").unwrap();
            bucket.put(b"bar", b"xxxx".to_vec()).unwrap();
            Ok(())
        });
        assert!(err.is_ok());
        db.must_check();

        // Cause a split.
        let err = db.update(|tx| {
            let mut bucket = tx.bucket_mut(b"widgets").unwrap();
            for i in 0..10000 {
                let err = bucket.put(
                    format!("{}", i).as_bytes(),
                    format!("{}", i).as_bytes().to_vec(),
                );
                assert!(err.is_ok());
            }
            Ok(())
        });
        assert!(err.is_ok());
        db.must_check();

        // isnert into widgets/foo/baz.
        let err = db.update(|tx| {
            let mut bucket = tx.bucket_mut(b"widgets").unwrap();
            bucket
                .bucket_mut(b"foo")
                .unwrap()
                .put(b"baz", b"yyyy".to_vec())
                .unwrap();
            Ok(())
        });
        db.must_check();

        // verify
        let err = db.view(|tx| {
            let bucket = tx.bucket(b"widgets").unwrap();
            let value = bucket.bucket(b"foo").unwrap().get(b"baz").unwrap();
            assert_eq!(value, b"yyyy".to_vec());
            let value = bucket.get(b"bar").unwrap();
            assert_eq!(value, b"xxxx".to_vec());
            for i in 0..10000 {
                let got = bucket.get(format!("{}", i).as_bytes()).unwrap();
                assert_eq!(format!("{}", i).as_bytes().to_vec(), got);
            }
            Ok(())
        });
        assert!(err.is_ok());
    }

    #[test]
    fn delete_bucket_nest() {
        let db = mock_db().build().unwrap();
        let err = db.update(|tx| {
            {
                let mut widgets = tx.create_bucket(b"widgets").unwrap();
                let foo = widgets.create_bucket(b"foo").unwrap();
                let bar = foo.create_bucket(b"bar").unwrap();
                bar.put(b"baz", b"bat".to_vec()).unwrap();
            }
            Ok(())
        });
        assert!(err.is_ok());

        let err = db.update(|tx| {
            {
                let widgets = tx.bucket(b"widgets").unwrap();
                let foo = widgets.bucket(b"foo").unwrap();
                let bar = foo.bucket(b"bar").unwrap();
                bar.get(b"baz").unwrap();
            }

            tx.delete_bucket(b"widgets").unwrap();
            Ok(())
        });
        assert!(err.is_ok());

        let err = db.view(|tx| {
            let err = tx.bucket(b"widgets");
            assert!(err.is_err());
            Ok(())
        });
        assert!(err.is_ok());
    }
    #[test]
    fn bucket_sequence() {
        let db = mock_db().build().unwrap();
        db.update(|tx| {
            let mut bkt = tx.create_bucket(b"0").unwrap();
            assert_eq!(bkt.sequence(), 0);
            bkt.set_sequence(1000).unwrap();
            assert_eq!(bkt.sequence(), 1000);
            Ok(())
        })
        .unwrap();

        db.view(|tx| {
            let seq = tx.bucket(b"0").unwrap().sequence();
            assert_eq!(seq, 1000);

            Ok(())
        })
        .unwrap()
    }

    #[test]
    fn bucket_next_sequence() {
        let db = mock_db().build().unwrap();
        db.update(|tx| {
            {
                let widgets = tx.create_bucket(b"widgets").unwrap();
            }

            {
                let woojits = tx.create_bucket(b"woojits").unwrap();
            }

            {
                let sequence = tx.bucket_mut(b"widgets").unwrap().next_sequence().unwrap();
                assert_eq!(sequence, 1);
            }
            {
                let sequence = tx.bucket_mut(b"widgets").unwrap().next_sequence().unwrap();
                assert_eq!(sequence, 2);
            }
            {
                let sequence = tx.bucket_mut(b"woojits").unwrap().next_sequence().unwrap();
                assert_eq!(sequence, 1);
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn bucket_next_sequence_persist() {
        let db = mock_db().build().unwrap();
        db.update(|tx| {
            tx.create_bucket(b"widgets").unwrap();
            Ok(())
        })
        .unwrap();
        db.update(|tx| {
            let _ = tx.bucket_mut(b"widgets").unwrap().next_sequence().unwrap();
            Ok(())
        })
        .unwrap();

        db.update(|tx| {
            let seq = tx.bucket_mut(b"widgets").unwrap().next_sequence().unwrap();
            assert_eq!(seq, 2);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn bucket_next_sequence_readonly() {
        // nothind can do
    }

    #[test]
    fn bucket_next_sequence_closed() {
        let db = mock_db().build().unwrap();
        let mut tx = db.begin_rw_tx().unwrap();
        {
            tx.create_bucket(b"widgets").unwrap();
        }

        tx.rollback().unwrap();
        // let err = tx.bucket_mut(b"widgets").unwrap().next_sequence();
        // assert!(err.is_err());
    }

    #[test]
    fn bucket_for_each() {
        let db = mock_db().build().unwrap();
        db.update(|tx| {
            let mut bucket = tx.create_bucket(b"widgets").unwrap();
            bucket.put(b"foo", b"0000".to_vec()).unwrap();
            bucket.put(b"baz", b"0001".to_vec()).unwrap();
            bucket.put(b"bar", b"0002".to_vec()).unwrap();

            let mut index = 0;
            bucket
                .for_each(|key, value| {
                    if index == 0 {
                        assert_eq!(key, b"bar");
                        assert_eq!(value.unwrap(), b"0002");
                    } else if index == 1 {
                        assert_eq!(key, b"baz");
                        assert_eq!(value.unwrap(), b"0001");
                    } else {
                        assert_eq!(key, b"foo");
                        assert_eq!(value.unwrap(), b"0000");
                    }
                    index += 1;
                    Ok(())
                })
                .unwrap();
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn bucket_for_each_short_circuit() {
        let db = mock_db().build().unwrap();
        db.update(|tx| {
            {
                let mut bucket = tx.create_bucket(b"widgets").unwrap();
                bucket.put(b"bar", b"0000".to_vec()).unwrap();
                bucket.put(b"baz", b"0000".to_vec()).unwrap();
                bucket.put(b"foo", b"0000".to_vec()).unwrap();
            }
            let mut index = 0;
            let bucket = tx.bucket(b"widgets").unwrap();
            let err = bucket.for_each(|key, value| {
                index += 1;
                if key == b"baz" {
                    return Err(Unexpected("marker"));
                }
                Ok(())
            });
            assert_eq!(err, Err(Unexpected("marker")));
            assert_eq!(index, 2);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn bucket_for_each_closed() {
        let db = mock_db().build().unwrap();
        let mut tx = db.begin_rw_tx().unwrap();
        tx.create_bucket(b"widgets").unwrap();
        tx.rollback().unwrap();
        let err = tx.bucket(b"widgets").unwrap().for_each(|key, value| Ok(()));
        assert_eq!(Err(crate::Error::TxClosed), err);
    }

    #[test]
    fn bucket_put_empty_key() {
        let db = mock_db().build().unwrap();
        let err = db.update(|tx| {
            let mut bucket = tx.create_bucket(b"widgets").unwrap();
            bucket.put(b"", b"bar".to_vec())
        });
        assert_eq!(Err(crate::Error::KeyRequired), err);
    }

    #[test]
    fn bucket_put_key_too_large() {
        let db = mock_db().build().unwrap();
        let err = db.update(|tx| {
            let mut bucket = tx.create_bucket(b"widgets").unwrap();
            bucket.put(&[0u8; 32769], b"bar".to_vec())
        });
        assert_eq!(Err(crate::Error::KeyTooLarge), err);
    }

    #[test]
    fn bucket_put_value_too_large() {
        let db = mock_db().build().unwrap();
        //
    }

    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    #[test]
    fn bucket_stats() {
        let db = mock_db().build().unwrap();

        let big_key = b"readlly-big-value".to_vec();
        for i in 0..500 {
            db.update(|tx| {
                let mut bucket = tx.create_bucket_if_not_exists(b"woojits").unwrap();
                bucket
                    .put(format!("{:03}", i).as_bytes(), [i as u8].to_vec())
                    .unwrap();
                Ok(())
            })
            .unwrap();
        }
        db.update(|tx| {
            tx.bucket_mut(b"woojits")
                .unwrap()
                .put(&big_key, [42].repeat(10000).to_vec())
                .unwrap();
            Ok(())
        })
        .unwrap();

        db.view(|tx| {
            let stats = tx.bucket(b"woojits").unwrap().stats();
            info!("stats {:?}", stats);
            assert_eq!(stats.branch_page_n, 1);
            assert_eq!(stats.branch_over_flow_n, 0);
            // 16K
            assert_eq!(stats.leaf_page_n, 2);
            assert_eq!(stats.leaf_over_flow_n, 0);
            assert_eq!(stats.branch_alloc, 16384);
            assert_eq!(stats.leaf_alloc, 32768);

            // branch page header
            let mut branch_in_use = 16;
            branch_in_use += 2 * 16; // branch element
            branch_in_use += 2 * 3; // branch keys (6 3-byte keys)
            assert_eq!(stats.branch_inuse, branch_in_use);

            let mut leaf_inuse = 2 * 16; // leaf header
            leaf_inuse += 501 * 16; // leaf elements
            leaf_inuse += 500 * 3 + big_key.len(); // leaf keys
            leaf_inuse += 1 * 10 + 2 * 90 + 3 * 400 + 10000; // leaf value
            assert_eq!(stats.key_n, 501);
            assert_eq!(stats.depth, 2);
            assert_eq!(stats.bucket_n, 1);
            assert_eq!(stats.inline_bucket_n, 0);
            assert_eq!(stats.inline_bucket_inuse, 0);

            Ok(())
        })
        .unwrap();
    }

    fn rand_perm(n: isize) -> Vec<isize> {
        use rand::prelude::*;
        let mut rand = rand::thread_rng();
        let mut rands = (0..n).collect::<Vec<isize>>();
        rands.shuffle(&mut rand);
        rands
    }

    fn rand_chars_perm(n: isize) -> Vec<String> {
        use rand::prelude::*;
        let mut rand = rand::thread_rng();
        let mut rands = (0..n).collect::<Vec<isize>>();
        rands.shuffle(&mut rand);
        rands.iter().map(|v| format!("{}", v)).collect()
    }

    // #[test]
    // fn bucket_stats_random_fill() {
    //     if page_size::get() != 4096 {
    //         info!("skip test");
    //         return;
    //     }
    //     let db = mock_db().build().unwrap();
    //     info!("start ...");
    //     let mut count = 0;
    //     // Add a set of values in random order. It will be the same random
    //     // order, so we can maintain consistency between test runs.
    //     for i in rand_perm(1000).iter() {
    //         db.update(|tx| {
    //             let mut bucket = tx.create_bucket_if_not_exists(b"widgets").unwrap();
    //             bucket.fill_percent = 0.9;
    //             for j in rand_perm(100).iter() {
    //                 let index = (j * 1000) + i;
    //                 bucket
    //                     .put(
    //                         format!("{}000000000000000", index).as_bytes(),
    //                         b"0000000000".to_vec(),
    //                     )
    //                     .unwrap();
    //                 count += 1;
    //             }
    //             Ok(())
    //         })
    //         .unwrap();
    //     }
    //
    //     db.view(|tx| {
    //         let stats = tx.bucket(b"woojits").unwrap().stats();
    //         assert_eq!(stats.key_n, 100000);
    //         assert_eq!(
    //             stats.branch_page_n, 98,
    //             "unexpected branch_page_n: {}",
    //             stats.branch_page_n
    //         );
    //         assert_eq!(
    //             stats.branch_over_flow_n, 0,
    //             "unexpected branch_over_flow_n: {}",
    //             stats.branch_over_flow_n
    //         );
    //         assert_eq!(
    //             stats.branch_inuse, 130984,
    //             "unexpected branch_inuse: {}",
    //             stats.branch_inuse
    //         );
    //         assert_eq!(
    //             stats.branch_alloc, 401408,
    //             "unexpected branch_alloc: {}",
    //             stats.branch_alloc
    //         );
    //         assert_eq!(
    //             stats.leaf_alloc, 13975552,
    //             "unexpected leaf_alloc: {}",
    //             stats.leaf_alloc
    //         );
    //         Ok(())
    //     })
    //     .unwrap();
    // }

    #[test]
    fn bucket_stats_small() {
        let db = mock_db().build().unwrap();
        db.update(|tx| {
            // Add a bucket that fits on a single root leaf.
            let mut bucket = tx.create_bucket(b"whozawhats").unwrap();
            bucket.put(b"foo", b"bar".to_vec()).unwrap();
            Ok(())
        })
        .unwrap();
        db.view(|tx| {
            let bucket = tx.bucket(b"whozawhats").unwrap();
            let stats = bucket.stats();
            assert_eq!(
                stats.branch_page_n, 0,
                "unexpected branch_page_n: {}",
                stats.branch_page_n
            );
            assert_eq!(
                stats.branch_over_flow_n, 0,
                "unexpected branch_over_flow_n: {}",
                stats.branch_over_flow_n
            );
            assert_eq!(
                stats.leaf_page_n, 0,
                "unexpected leaf_page_n: {}",
                stats.leaf_page_n
            );
            assert_eq!(
                stats.leaf_over_flow_n, 0,
                "unexpected leaf_over_flow_n: {}",
                stats.leaf_over_flow_n
            );
            assert_eq!(stats.key_n, 1, "unexpected key_n: {}", stats.key_n);
            assert_eq!(stats.depth, 1, "unexpected depth: {}", stats.depth);
            assert_eq!(
                stats.branch_inuse, 0,
                "unexpected branch_inuse: {}",
                stats.branch_inuse
            );
            assert_eq!(
                stats.leaf_inuse, 0,
                "unexpected leaf_inuse: {}",
                stats.leaf_inuse
            );
            if page_size::get() == 4096 {
                assert_eq!(
                    stats.branch_alloc, 0,
                    "unexpected branch_alloc: {}",
                    stats.branch_alloc
                );
                assert_eq!(
                    stats.leaf_alloc, 0,
                    "unexpected leaf_alloc: {}",
                    stats.leaf_alloc
                );
            }
            assert_eq!(stats.bucket_n, 1, "unexpected bucket_n: {}", stats.bucket_n);
            assert_eq!(
                stats.inline_bucket_n, 1,
                "unexpected inline_bucket_n:{}",
                stats.inline_bucket_n
            );
            assert_eq!(
                stats.inline_bucket_inuse,
                16 + 16 + 6,
                "unexpected inline_bucket_inuse: {}",
                stats.inline_bucket_inuse
            );
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn bucket_stats_empty() {
        let db = mock_db().build().unwrap();
        db.update(|tx| {
            tx.create_bucket(b"whozawhats").unwrap();
            Ok(())
        })
        .unwrap();

        db.view(|tx| {
            let bucket = tx.bucket(b"whozawhats").unwrap();
            let stats = bucket.stats();
            assert_eq!(stats.branch_page_n, 0);
            assert_eq!(stats.branch_over_flow_n, 0);
            assert_eq!(stats.leaf_page_n, 0);
            assert_eq!(stats.leaf_over_flow_n, 0);
            assert_eq!(stats.key_n, 0);
            assert_eq!(stats.depth, 1);
            assert_eq!(stats.branch_inuse, 0);
            assert_eq!(stats.leaf_inuse, 0);

            if page_size::get() == 4096 {
                assert_eq!(stats.leaf_alloc, 0);
                assert_eq!(stats.leaf_alloc, 0);
            }
            assert_eq!(stats.bucket_n, 1);
            assert_eq!(stats.inline_bucket_n, 1);
            assert_eq!(stats.inline_bucket_inuse, 16);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn bucket_stats_nested() {
        let db = mock_db().build().unwrap();
        db.update(|tx| {
            let mut bucket = tx.create_bucket(b"foo").unwrap();
            for i in 0..100 {
                bucket
                    .put(
                        format!("{:02}", i).as_bytes(),
                        format!("{:02}", i).as_bytes().to_vec(),
                    )
                    .unwrap();
            }
            let bar = bucket.create_bucket(b"bar").unwrap();
            for i in 0..10 {
                bar.put(&vec![i], vec![i]).unwrap();
            }

            let baz = bar.create_bucket(b"baz").unwrap();
            for i in 0..10 {
                baz.put(&vec![i], vec![i]).unwrap();
            }
            Ok(())
        })
        .unwrap();

        db.view(|tx| {
            let bucket = tx.bucket(b"foo").unwrap();
            let stats = bucket.stats();
            info!("stats {:?}", stats);
            assert_eq!(stats.branch_page_n, 0);
            assert_eq!(stats.branch_over_flow_n, 0);
            assert_eq!(stats.leaf_page_n, 2);
            assert_eq!(stats.leaf_over_flow_n, 0);
            assert_eq!(stats.key_n, 122);
            assert_eq!(stats.depth, 3);
            assert_eq!(stats.branch_inuse, 0);

            let mut foo = 16; // foo (pghdr)
            foo += 101 * 16; // foo leaf elements
            foo += 100 * 2 + 100 * 2; // foo leaf key/values
            foo += 3 + 16; // foo -> bar key/value

            let mut bar = 16; // bar (pghdr)
            bar += 11 * 16; // bar leaf elements
            bar += 10 + 10; // bar leaf key/values
            bar += 3 + 16; // bar -> baz key/value

            let mut baz = 16; // baz (inline) (pghdr)
            baz += 10 * 16; // baz leaf elements
            baz += 10 + 10; // baz leaf key/values

            assert_eq!(stats.leaf_inuse, foo + bar + baz);

            if page_size::get() == 4096 {
                assert_eq!(stats.branch_alloc, 0);
                assert_eq!(stats.leaf_alloc, 8192);
            }

            assert_eq!(stats.bucket_n, 3);
            assert_eq!(stats.inline_bucket_n, 1);
            assert_eq!(stats.inline_bucket_inuse, baz);

            Ok(())
        })
        .unwrap();
    }

    // #[test]
    // fn bucket_stats_large() {
    //     let db = mock_db().build().unwrap();
    //     for i in 0..100 {
    //         db.update(|tx| {
    //             let mut bucket = tx.create_bucket_if_not_exists(b"widgets").unwrap();
    //             for i in 0..1000 {
    //                 bucket
    //                     .put(
    //                         format!("{}", i).as_bytes(),
    //                         format!("{}", i).as_bytes().to_vec(),
    //                     )
    //                     .unwrap();
    //             }
    //             Ok(())
    //         })
    //         .unwrap();
    //     }
    //
    //     db.view(|tx| {
    //         let stats = tx.bucket(b"widgets").unwrap().stats();
    //         if page_size::get() == 4096 {
    //             assert_eq!(stats.branch_page_n, 13);
    //             assert_eq!(stats.branch_over_flow_n, 13);
    //             assert_eq!(stats.leaf_page_n, 1196);
    //             assert_eq!(stats.leaf_over_flow_n, 0);
    //             assert_eq!(stats.key_n, 100000);
    //             assert_eq!(stats.depth, 3);
    //             assert_eq!(stats.branch_inuse, 25257);
    //             assert_eq!(stats.leaf_inuse, 2596916);
    //             assert_eq!(stats.branch_alloc, 53248);
    //             assert_eq!(stats.leaf_alloc, 4898816);
    //         }
    //
    //         assert_eq!(stats.bucket_n, 1);
    //         assert_eq!(stats.inline_bucket_n, 0);
    //         assert_eq!(stats.inline_bucket_inuse, 0);
    //         Ok(())
    //     })
    //     .unwrap();
    // }
}
