use crate::bucket::{MAX_FILL_PERCENT, MIN_FILL_PERCENT};
use crate::error::Error::BucketEmpty;
use crate::error::{Error, Result};
use crate::page::{
    BranchPageElement, LeafPageElement, BRANCH_PAGE_ELEMENT_SIZE, BRANCH_PAGE_FLAG,
    LEAF_PAGE_ELEMENT_SIZE, LEAF_PAGE_FLAG, MIN_KEYS_PER_PAGE, PAGE_HEADER_SIZE,
};
use crate::{bucket, search, Bucket, Page, PgId};
use kv_log_macro::warn;
use log::info;
use memoffset::ptr::copy_nonoverlapping;
use std::borrow::{Borrow, BorrowMut};
use std::cell::{Cell, Ref, RefCell, RefMut};
use std::fmt::{Debug, Formatter};
use std::ops::{Deref, RangeBounds};
use std::path::Display;
use std::process::id;
use std::rc::{Rc, Weak};
use std::slice::Iter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) type Key = Vec<u8>;
pub(crate) type Value = Vec<u8>;

pub(crate) struct NodeInner {
    // associated bucket.
    bucket: *const Bucket,
    is_leaf: AtomicBool,
    // Just for inner mut
    spilled: AtomicBool,
    unbalanced: AtomicBool,
    key: RefCell<Key>,
    pgid: RefCell<PgId>,
    parent: RefCell<WeakNode>,
    children: RefCell<Vec<Node>>,
    pub(crate) inodes: RefCell<Vec<Inode>>,
}

impl Debug for NodeInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let local_bucket = unsafe { (*self.bucket).local_bucket.root };
        f.debug_struct("node")
            .field("bucket", &local_bucket)
            .field("is_leaf", &self.is_leaf)
            .field("spilled", &self.spilled)
            .field("unbalanced", &self.unbalanced)
            .field("key", &self.key.borrow())
            .field("pgid", &self.pgid.borrow())
            .field("children", &self.children.borrow())
            .field("inodes", &self.inodes.borrow())
            .finish()
    }
}

impl NodeInner {
    fn is_leaf(&self) -> bool {
        self.is_leaf.load(Ordering::Acquire)
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct WeakNode(Weak<NodeInner>);

impl WeakNode {
    pub(crate) fn new() -> Self {
        WeakNode::default()
    }

    pub(crate) fn upgrade(&self) -> Option<Node> {
        self.0.upgrade().map(Node)
    }

    fn from(tx: &Node) -> Self {
        WeakNode(Rc::downgrade(&tx.0))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Node(pub(crate) Rc<NodeInner>);

impl Node {
    pub(crate) fn new(bucket: Bucket, parent: &Node) -> Node {
        unimplemented!()
    }

    // Returns the top-level node this node is attached to.
    pub(crate) fn root(&self) -> Node {
        match self.parent() {
            Some(ref p) => p.root(),
            None => self.clone(),
        }
    }

    pub fn pg_id(&self) -> PgId {
        self.0.pgid.borrow().clone()
    }

    /// Attempts to combine the node with sibling nodes if the node fill
    /// size is below a threshold or if there are not enough keys.
    pub fn rebalance(&mut self) {
        {
            let selfsize = self.size();
            if !self.0.unbalanced.load(Ordering::Acquire) {
                warn!("need not to rebalance: {:?}", self.0);
                return;
            }
            self.0.unbalanced.store(false, Ordering::Relaxed);

            // updates stats and get threshold
            let threshold = {
                let bucket = self.bucket_mut().unwrap();
                let tx = bucket.tx().unwrap();
                tx.0.stats.lock().rebalance += 1;
                tx.db().unwrap().page_size() / 4
            };

            if selfsize > threshold && self.0.inodes.borrow().len() > self.min_keys() as usize {
                warn!("need not to rebalance: {:?}", self.0);
                return;
            }
            warn!("need to rebalance: {:?}", self.0);
            // Root node has special handling
            if self.parent().is_none() {
                let mut inodes = self.0.inodes.borrow_mut();
                if !self.is_leaf() && inodes.len() == 1 {
                    let mut child = self
                        .bucket_mut()
                        .unwrap()
                        .node(inodes[0].pg_id, WeakNode::from(self));
                    self.0.is_leaf.store(child.is_leaf(), Ordering::Release);
                    *inodes = child.0.inodes.borrow_mut().drain(..).collect();
                    *self.0.children.borrow_mut() =
                        child.0.children.borrow_mut().drain(..).collect();

                    // Reparent all child nodes being moved.
                    {
                        let inode_pgids = inodes.iter().map(|i| i.pg_id);
                        let bucket = self.bucket_mut().unwrap();
                        for pgid in inode_pgids {
                            if let Some(child) = bucket.nodes.borrow_mut().get_mut(&pgid) {
                                *child.0.parent.borrow_mut() = WeakNode::from(self);
                            }
                        }
                    }

                    *child.0.parent.borrow_mut() = WeakNode::new();
                    self.bucket_mut()
                        .unwrap()
                        .nodes
                        .borrow_mut()
                        .remove(&child.0.pgid.borrow());
                    child.free();
                }

                return;
            }
        }
        info!("free");
        // If node has no keys then just remove it.
        if self.num_children() == 0 {
            let key = self.0.key.borrow().clone();
            let pgid = *self.0.pgid.borrow();
            let mut parent = self.parent().unwrap();
            parent.del(&key);
            parent.remove_child(self);
            self.bucket_mut().unwrap().nodes.borrow_mut().remove(&pgid);
            self.free();
            parent.rebalance();
            return;
        }

        assert!(
            self.parent().unwrap().num_children() > 1,
            "parent must have at least 2 children"
        );

        let (use_next_sibling, mut target) = {
            let use_next_sibling = self.parent().unwrap().child_index(self) == 0;
            let target = if use_next_sibling {
                self.next_sibling().unwrap()
            } else {
                self.prev_sibling().unwrap()
            };
            (use_next_sibling, target)
        };

        if use_next_sibling {
            let bucket = self.bucket_mut().unwrap();
            for pgid in target.0.inodes.borrow().iter().map(|i| i.pg_id) {
                if let Some(child) = bucket.nodes.borrow_mut().get_mut(&pgid) {
                    child.parent().unwrap().remove_child(child);
                    *child.0.parent.borrow_mut() = WeakNode::from(self);
                    child
                        .parent()
                        .unwrap()
                        .0
                        .children
                        .borrow_mut()
                        .push(child.clone());
                }
            }

            self.0
                .inodes
                .borrow_mut()
                .append(&mut target.0.inodes.borrow_mut());
            {
                let mut parent = self.parent().unwrap();
                parent.del(&target.0.key.borrow().as_ref());
                parent.remove_child(&target);
            }

            self.bucket_mut()
                .unwrap()
                .nodes
                .borrow_mut()
                .remove(&target.pg_id());
            target.free();
        } else {
            for pgid in target.0.inodes.borrow().iter().map(|i| i.pg_id) {
                if let Some(child) = self.bucket_mut().unwrap().nodes.borrow_mut().get_mut(&pgid) {
                    let mut parent = child.parent().unwrap();
                    parent.remove_child(&child);
                    *child.0.parent.borrow_mut() = WeakNode::from(&target);
                    parent.0.children.borrow_mut().push(child.clone());
                }
            }

            target
                .0
                .inodes
                .borrow_mut()
                .append(&mut *self.0.inodes.borrow_mut());
            {
                let mut parent = self.parent().unwrap();
                parent.del(&self.0.key.borrow());
                parent.remove_child(&self);
            }
            self.bucket_mut()
                .unwrap()
                .nodes
                .borrow_mut()
                .remove(&self.pg_id());
            self.free();
        }
        self.parent().unwrap().rebalance();
    }

    fn parent(&self) -> Option<Node> {
        self.0.parent.borrow().upgrade()
    }

    // Returns the minimum number of inodes this node should have.
    fn min_keys(&self) -> usize {
        if self.is_leaf() {
            return 1;
        }
        2
    }

    pub(crate) fn is_leaf(&self) -> bool {
        self.0.is_leaf()
    }

    // Returns the size of the node after serialization.
    pub(crate) fn size(&self) -> usize {
        self.0
            .inodes
            .borrow()
            .iter()
            .fold(PAGE_HEADER_SIZE, |acc, inode| {
                acc + self.page_element_size() + inode.key.len() + inode.value.len()
            })
    }

    // Returns true if the node is less than a given size.
    // This is an optimization to avoid calculation a large node when we only need
    // to know if it fits inside a certain page size.
    fn size_less_than(&self, v: usize) -> bool {
        for i in 0..self.0.inodes.borrow().len() {
            let node = &self.0.inodes.borrow()[i];
            if self.page_element_size() + node.key.len() + node.value.len() >= v {
                return false;
            }
        }
        true
    }

    // Returns the size of each page element based on type of node.
    fn page_element_size(&self) -> usize {
        if self.is_leaf() {
            return LEAF_PAGE_ELEMENT_SIZE;
        }
        BRANCH_PAGE_ELEMENT_SIZE
    }

    // TODO: why?
    // Returns the child node at a given index.
    pub(crate) fn child_at(&self, index: usize) -> Result<Node> {
        if self.is_leaf() {
            return Err(Error::InvalidNode(format!(
                "invalid childAt {} on a leaf node",
                index
            )));
        }
        let pg_id = self.0.inodes.borrow()[index].pg_id;
        // todo: Why?
        Ok(self.bucket_mut().unwrap().node(pg_id, WeakNode::from(self)))
    }

    // todo: unwrap
    // Returns the child node at a given index.
    fn child_index(&self, child: &Node) -> isize {
        // FIXME
        let key = child.0.key.borrow();
        self.0
            .inodes
            .borrow()
            .binary_search_by(|inode| inode.key.cmp(&key))
            .unwrap() as isize
    }

    pub(crate) fn child_mut(&self) -> RefMut<'_, Vec<Node>> {
        self.0.children.borrow_mut()
    }

    pub(super) fn bucket<'a, 'b: 'a>(&'a self) -> Option<&'b Bucket> {
        if self.0.bucket.is_null() {
            return None;
        }
        Some(unsafe { &*(self.0.bucket as *const Bucket) })
    }

    pub(super) fn bucket_mut<'a, 'b: 'a>(&'a self) -> Option<&'b mut Bucket> {
        if self.0.bucket.is_null() {
            return None;
        }
        Some(unsafe { &mut *(self.0.bucket as *mut Bucket) })
    }

    /// Returns the number of children.
    #[inline]
    fn num_children(&self) -> usize {
        self.0.inodes.borrow().len()
    }

    // Returns the next node with the same parent.
    fn next_sibling(&self) -> Option<Node> {
        match self.parent() {
            Some(mut parent) => {
                let index = parent.child_index(self);
                // self is the last node at the level
                if index >= parent.num_children() as isize - 1 {
                    return None;
                }
                parent.child_at((index + 1) as usize).ok()
            }
            None => None,
        }
    }

    // Returns the previous node with the same parent.
    fn prev_sibling(&self) -> Option<Node> {
        if self.parent().is_none() {
            return None;
        }
        let parent = self.parent().unwrap();
        let index = parent.child_index(self);
        if index == 0 {
            return None;
        }
        parent.child_at((index - 1) as usize).ok()
    }

    // Removes a key from the node.
    pub(crate) fn del(&mut self, key: &[u8]) {
        // Find index of key.
        let index = match self
            .0
            .inodes
            .borrow()
            .binary_search_by(|inode| inode.key.cmp(&key.to_vec()))
        {
            Ok(index) => index,
            // Exit if the key isn't found.
            _ => return,
        };
        self.0.inodes.borrow_mut().remove(index);
        self.0.unbalanced.store(true, Ordering::Release);
    }

    // Inserts a key/value.
    pub(crate) fn put(
        &self,
        old_key: &[u8],
        new_key: &[u8],
        value: Value,
        pg_id: PgId,
        flags: u32,
    ) -> Result<()> {
        let bucket = self.bucket().unwrap();
        if pg_id >= bucket.tx().unwrap().meta_mut().pg_id {
            return Err(Error::PutFailed(format!(
                "pgid {:?} above high water mark {:?}",
                pg_id,
                bucket.tx().unwrap().meta_mut().pg_id
            )));
        } else if old_key.len() <= 0 {
            return Err(Error::PutFailed("zero-length old key".to_string()));
        } else if new_key.len() <= 0 {
            return Err(Error::PutFailed("zero-length new key".to_string()));
        }

        // Find insertion index.
        let mut inodes = self.0.inodes.borrow_mut();
        let (extra, index) = match inodes.binary_search_by(|inode| inode.key.cmp(&old_key.to_vec()))
        {
            Ok(n) => (true, n),
            Err(n) => (false, n),
        };
        // Add capacity and shift nodes if we don't have an exact match and need to insert.
        if !extra {
            inodes.insert(index, Inode::default());
        }
        let node = &mut inodes[index];
        node.key = new_key.to_vec();
        node.value = value;
        node.flags = flags;
        node.pg_id = pg_id;
        Ok(())
    }

    // Initializes the node from a page.
    pub(crate) fn read(&mut self, p: &Page) {
        let mut node = self.0.borrow_mut();
        *node.pgid.borrow_mut() = p.id;
        node.is_leaf
            .store(p.flags & LEAF_PAGE_FLAG != 0, Ordering::Release);

        for i in 0..(p.count as usize) {
            let mut inode = &mut node.inodes.borrow_mut()[i];
            if node.is_leaf() {
                let elem = p.leaf_page_element(i);
                inode.flags = elem.flags;
                inode.key = elem.key().to_vec();
                inode.value = elem.value().to_vec();
            } else {
                let elem = p.branch_page_element(i);
                inode.pg_id = elem.pgid;
                inode.key = elem.key().to_vec();
            }
            assert!(!inode.key.is_empty(), "read: zero-length inode key");
        }

        // Save first key so we can find the node in the parent when we spill.
        if node.inodes.borrow().len() > 0 {
            *node.key.borrow_mut() = node.inodes.borrow()[0].key.clone();
            assert!(!node.key.borrow().is_empty(), "read: zero-length inode key");
        } else {
            // todo:
            node.key.borrow_mut().clear();
        }
    }

    // Writes the items onto one or more pages.
    pub(crate) fn write(&self, page: &mut Page) {
        // Initialize page.
        if self.is_leaf() {
            page.flags |= LEAF_PAGE_FLAG;
        } else {
            page.flags |= BRANCH_PAGE_FLAG;
        }
        info!("write node page: {:?}", page);
        // TODO: Why?
        if self.0.inodes.borrow().len() >= 0xFFFF {
            panic!(
                "inode overflow: {} (pg_id={})",
                self.0.inodes.borrow().len(),
                page.id
            );
        }

        page.count = self.0.inodes.borrow().len() as u16;
        // Stop here if there are no items to write.
        if page.count == 0 {
            return;
        }

        // Loop over each item and write it to the page.
        let mut b_ptr = unsafe {
            let offset = self.page_element_size() * self.0.inodes.borrow().len();
            page.get_data_mut_ptr().add(offset)
        };
        for (i, item) in self.0.inodes.borrow().iter().enumerate() {
            assert!(!item.key.is_empty(), "write: zero-length inode key");

            // Write the page element.
            if self.is_leaf() {
                let mut element = page.leaf_page_element_mut(i);
                let element_ptr = element as *const LeafPageElement as *const u8;
                element.pos = unsafe { b_ptr.sub(element_ptr as usize) } as u32;
                element.flags = item.flags as u32;
                element.k_size = item.key.len() as u32;
                element.v_size = item.value.len() as u32;
            } else {
                let page_id = page.id;
                let mut element = page.branch_page_element_mut(i);
                let element_ptr = element as *const BranchPageElement as *const u8;
                element.pos = unsafe { b_ptr.sub(element_ptr as usize) } as u32;
                element.k_size = item.key.len() as u32;
                element.pgid = item.pg_id;
                assert_eq!(element.pgid, page_id, "write: circular dependency occurred");
            }

            // If the length of key+value is larger than the max allocation size
            // then we need to reallocate the byte array pointer.
            //
            // See: https://github.com/boltdb/bolt/pull/335
            // FIXME:
            let (k_len, v_len) = (item.key.len(), item.value.len());

            unsafe {
                copy_nonoverlapping(item.key.as_ptr(), b_ptr, k_len);
                b_ptr = b_ptr.add(k_len);
                copy_nonoverlapping(item.value.as_ptr(), b_ptr, v_len);
                b_ptr = b_ptr.add(v_len);
            }
        }
        // DEBUG ONLY: n.dump()
    }

    // Removes a node from the list of in-memory children.
    // This dose not affect the inodes.
    fn remove_child(&self, target: &Node) {
        self.0
            .children
            .borrow()
            .iter()
            .position(|c| Rc::ptr_eq(&target.0, &c.0))
            .map(|idx| self.0.children.borrow_mut().remove(idx));
    }

    /// Writes the nodes to dirty pages and splits nodes as it goes.
    /// Returns an error if dirty pages cannot be allocated.
    pub fn spill(&mut self) -> Result<()> {
        if self.0.spilled.load(Ordering::Acquire) {
            return Ok(());
        }

        let page_size = self.bucket().unwrap().tx()?.db()?.page_size();
        {
            let mut children = self.0.children.borrow_mut().clone();
            children.sort_by(Node::cmp_by_key);
            for child in &mut *children {
                child.spill()?;
            }
            self.0.children.borrow_mut().clear();
        }

        let mut node_parent = None;
        {
            let mut nodes = match self.split(page_size)? {
                None => vec![self.clone()],
                Some(p) => {
                    node_parent = Some(p.clone());
                    p.0.children.borrow().clone()
                }
            };

            let bucket = self.bucket_mut().unwrap();
            let mut tx = bucket.tx()?;
            let db = tx.db()?;
            let txid = tx.id();

            for node in &mut nodes {
                {
                    // Add node's page to the freelist if it's not new.
                    let node_pgid = *node.0.pgid.borrow_mut();
                    // TODO *Notes*: if node is new that it's pgid was set to 0
                    if node_pgid > 0 {
                        db.0.free_list
                            .try_write()
                            .unwrap()
                            .free(txid, unsafe { &*tx.page(node_pgid)? });
                        *node.0.pgid.borrow_mut() = 0;
                    }
                }

                let page = tx.allocate((node.size() / db.page_size()) as u64 + 1)?;
                let mut page = unsafe { &mut *page };
                {
                    // Write the node.
                    let id = page.id;
                    let txpgid = tx.pgid();
                    if id >= txpgid {
                        panic!("pgid ({}) above high water marl ({})", id, txid);
                    }
                    *node.0.pgid.borrow_mut() = id;
                    node.write(&mut page);
                    node.0.spilled.store(true, Ordering::Release);
                }
            }
        }

        // If the root node split and created a new root then we need to spill that
        // as well. We'll clear out the children to make sure it doesn't try to respill.
        {
            let spill_parent = match node_parent {
                None => None,
                Some(p) => {
                    let pgid_valid = *p.0.pgid.borrow() == 0;
                    if pgid_valid {
                        Some(p)
                    } else {
                        None
                    }
                }
            };
            if let Some(parent) = spill_parent {
                self.0.children.borrow_mut().clear();
                // setting self as parent to hold strong reference
                *self = parent;
                return self.spill();
            }
        }
        Ok(())
    }

    // Breaks up a node into multiple smaller nodes, if appropriate.
    // This should only be called from the spill() function.
    //
    // returns None if no split occurred, or parent Node otherwise (the parent maybe is new if not
    // exists at before).
    fn split(&mut self, page_size: usize) -> Result<Option<Node>> {
        let mut nodes = vec![self.clone()];
        while let Some(n) = nodes.last().unwrap().clone().split_two(page_size)? {
            nodes.push(n);
        }

        if nodes.len() == 1 {
            return Ok(None);
        }

        match self.parent() {
            Some(p) => {
                let mut children = p.0.children.borrow_mut();
                let index = children.iter().position(|ch| Rc::ptr_eq(&self.0, &ch.0));
                assert!(index.is_some());
                // remove old children's reference for parent
                children.remove(index.unwrap());
                // reset all nodes parent reference
                for node in nodes {
                    *node.0.parent.borrow_mut() = WeakNode::from(&p);
                    children.push(node);
                }
                // hard work: 🤣🤣🤣
                drop(children);

                Ok(Some(p))
            }
            None => {
                // generate a new parent and set collection with them.
                let parent = NodeBuilder::new(self.0.bucket).children(nodes).build();
                for ch in &mut *parent.0.children.borrow_mut() {
                    *ch.0.parent.borrow_mut() = WeakNode::from(&parent);
                }
                Ok(Some(parent))
            }
        }
    }

    // Breaks up a node into two smaller nodes, if appropriate.
    // This should only be called from the `split` function.
    fn split_two(&mut self, page_size: usize) -> Result<Option<Node>> {
        // Ignore the split if the page doesn't have at least enough nodes for
        // two pages or if the nodes can fit in a single page.
        let inodes = self.0.inodes.borrow();
        if inodes.len() <= MIN_KEYS_PER_PAGE * 2 || self.size_less_than(page_size) {
            return Ok(None);
        }
        // Determine the threshold before starting a new node.
        let fill_parent = self
            .bucket()
            .ok_or_else(|| BucketEmpty)?
            .fill_percent
            .min(MIN_FILL_PERCENT)
            .max(MAX_FILL_PERCENT);
        let threshold = page_size as f64 * fill_parent;

        // Determine split position and sizes of the two pages.
        let split_index = self.split_index(threshold as usize).0;

        // Split node into two separate nodes.
        // If there's no parent then we'll need to create one.
        let next = NodeBuilder::new(self.0.bucket)
            .is_leaf(self.is_leaf())
            .build();
        let nodes = self
            .0
            .inodes
            .borrow_mut()
            .drain(split_index..)
            .collect::<Vec<_>>();
        // Only the first block has pgid of older(use old memory space), thew second block is set to 0
        *next.0.inodes.borrow_mut() = nodes;
        self.bucket_mut()
            .ok_or_else(|| BucketEmpty)?
            .tx()
            .unwrap()
            .stats()
            .split += 1;
        Ok(Some(next))
    }

    // Finds the position where a page will fill a given threshold.
    // It returns the index as well as the size of the first page.
    // This is only be called from `split`.
    fn split_index(&self, threshold: usize) -> (usize, usize) {
        let mut size = PAGE_HEADER_SIZE;
        let mut index: usize = 0;

        // Loop until we only have the minimum number of keys required for the second page.
        let inodes = self.0.inodes.borrow();
        for i in 0..inodes.len() - MIN_KEYS_PER_PAGE {
            index = i;
            let inode = &inodes[i];
            let el_size = self.page_element_size() + inode.key.len() + inode.value.len();

            // If we have at least the minimum number of keys and adding another
            // node would put us over the threshold then exit and return.
            if i >= MIN_KEYS_PER_PAGE && size + el_size > threshold {
                break;
            }

            // Add the element size to the total size.
            size += el_size;
        }
        (index, size)
    }

    // Adds the node's underlying `page` to the freelist.
    pub(crate) fn free(&mut self) {
        if *self.0.pgid.borrow() == 0 {
            return;
        }
        {
            let bucketmut = self.bucket_mut().unwrap();
            let tx = bucketmut.tx().unwrap();
            let txid = tx.id();
            let page = unsafe { &*tx.page(*self.0.pgid.borrow()).unwrap() };
            let db = tx.db().unwrap();
            db.0.free_list.write().free(txid, &page);
        }

        *self.0.pgid.borrow_mut() = 0;
    }

    fn cmp_by_key(a: &Node, b: &Node) -> std::cmp::Ordering {
        a.0.inodes.borrow()[0].key.cmp(&b.0.inodes.borrow()[0].key)
    }
}

pub(crate) struct NodeBuilder {
    bucket: Option<*const Bucket>,
    is_leaf: bool,
    pg_id: PgId,
    parent: WeakNode,
    children: Vec<Node>,
}

impl NodeBuilder {
    pub(crate) fn new(bucket: *const Bucket) -> Self {
        NodeBuilder {
            bucket: Some(bucket),
            is_leaf: false,
            pg_id: 0,
            parent: Default::default(),
            children: vec![],
        }
    }

    pub(crate) fn is_leaf(mut self, value: bool) -> Self {
        self.is_leaf = value;
        self
    }

    pub(crate) fn parent(mut self, value: WeakNode) -> Self {
        self.parent = value;
        self
    }

    pub(crate) fn children(mut self, value: Vec<Node>) -> Self {
        self.children = value;
        self
    }

    fn bucket(mut self, value: *const Bucket) -> Self {
        self.bucket = Some(value);
        self
    }

    pub(crate) fn build(self) -> Node {
        Node(Rc::new(NodeInner {
            bucket: self.bucket.unwrap(),
            is_leaf: AtomicBool::new(self.is_leaf),
            spilled: AtomicBool::new(false),
            unbalanced: AtomicBool::new(false),
            key: RefCell::new(vec![]),
            pgid: RefCell::new(self.pg_id),
            parent: RefCell::new(self.parent),
            children: RefCell::new(self.children),
            inodes: RefCell::new(vec![]),
        }))
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Inode {
    pub(crate) flags: u32,
    pub(crate) pg_id: PgId,
    pub(crate) key: Key,
    pub(crate) value: Key,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Inodes {
    pub(crate) inner: Vec<Inode>,
}

impl Inodes {
    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    fn get_ref(&self, index: usize) -> &Inode {
        &self.inner[index]
    }

    #[inline]
    fn get_mut_ref(&mut self, index: usize) -> &mut Inode {
        &mut self.inner[index]
    }

    #[inline]
    fn iter(&self) -> Iter<'_, Inode> {
        self.inner.iter()
    }

    #[inline]
    fn as_slice(&self) -> &Vec<Inode> {
        &self.inner
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    fn push(&mut self, inode: Inode) {
        self.inner.push(inode)
    }

    #[inline]
    fn insert(&mut self, index: usize, inode: Inode) {
        self.inner.insert(index, inode);
    }

    #[inline]
    fn remove(&mut self, index: usize) {
        self.inner.remove(index);
    }

    #[inline]
    fn binary_search_by(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        self.inner
            .binary_search_by(|node| node.key.as_slice().cmp(key))
    }
}
#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {}
}
