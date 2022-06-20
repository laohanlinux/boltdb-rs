use crate::bucket::{MAX_FILL_PERCENT, MIN_FILL_PERCENT};
use crate::error::Error::BucketEmpty;
use crate::error::{Error, Result};
use crate::page::{
    BranchPageElement, LeafPageElement, PageFlag, BRANCH_PAGE_ELEMENT_SIZE, BRANCH_PAGE_FLAG,
    LEAF_PAGE_ELEMENT_SIZE, LEAF_PAGE_FLAG, MIN_KEYS_PER_PAGE, PAGE_HEADER_SIZE,
};
use crate::{bucket, search, Bucket, Page, PgId};
use kv_log_macro::{debug, warn};
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
    key: RefCell<Key>,   // be set to inodes[0].key when inodes is not empty
    pgid: RefCell<PgId>, // be set to 0 when node is leaf node
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
    /// The node will be reclaimed to free-list with freelist.free() after merge by it's sibling
    pub fn rebalance(&mut self) {
        {
            let selfsize = self.size();
            if !self.0.unbalanced.load(Ordering::Acquire) {
                warn!("need not to rebalance: {:?}", self.0.pgid);
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
                parent.del(target.0.key.borrow().as_ref());
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
                    let parent = child.parent().unwrap();
                    parent.remove_child(child);
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
                parent.remove_child(self);
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
    // TODO: add inodes size cache
    fn size_less_than(&self, v: usize) -> bool {
        let (mut sz, elsz) = (PAGE_HEADER_SIZE, self.page_element_size());
        self.0.inodes.borrow().iter().all(|node| {
            sz += elsz + node.key.len() + node.value.len();
            return sz < v;
        })
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
            Some(parent) => {
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
        } else if old_key.is_empty() {
            return Err(Error::PutFailed("zero-length old key".to_string()));
        } else if new_key.is_empty() {
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
    pub(crate) fn read(&mut self, page: &Page) {
        *self.0.pgid.borrow_mut() = page.id;
        self.0
            .is_leaf
            .store(matches!(page.flags, LEAF_PAGE_FLAG), Ordering::Release);
        let mut inodes = Vec::<Inode>::with_capacity(page.count as usize);
        let is_leaf = self.is_leaf();

        for i in 0..page.count as usize {
            if is_leaf {
                let elem = page.leaf_page_element(i);
                let inode = Inode {
                    flags: elem.flags,
                    key: elem.key().to_vec(),
                    value: elem.value().to_vec(),
                    pg_id: 0, // *Note*
                };
                inodes.push(inode);
            } else {
                let elem = page.branch_page_element(i);
                let inode = Inode {
                    flags: 0,
                    key: elem.key().to_vec(),
                    value: Vec::new(),
                    pg_id: elem.pgid,
                };
                inodes.push(inode);
            }
        }

        *self.0.inodes.borrow_mut() = inodes;

        {
            let inodes = self.0.inodes.borrow();
            if !inodes.is_empty() {
                *self.0.key.borrow_mut() = inodes[0].key.clone();
            }
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
        info!(_page=log::kv::Value::from_debug(page), inodes_size=self.0.inodes.borrow().len(); "write node page");
        let inodes = self.0.inodes.borrow_mut();
        // TODO: Why?
        if inodes.len() >= 0xFFFF {
            panic!("inode overflow: {} (pg_id={})", inodes.len(), page.id);
        }

        page.count = inodes.len() as u16;
        // Stop here if there are no items to write.
        if inodes.is_empty() {
            debug!("inode is empty: {}", page.id);
            return;
        }
        // Loop over each item and write it to the page.
        let mut b_ptr = unsafe {
            let offset = self.page_element_size() * inodes.len();
            page.get_data_mut_ptr().add(offset)
        };
        let is_leaf = self.is_leaf();
        let pgid = page.id;

        for (i, item) in inodes.iter().enumerate() {
            assert!(!item.key.is_empty(), "write: zero-length inode key");

            // Write the page element.
            if is_leaf {
                let mut element = page.leaf_page_element_mut(i);
                let element_ptr = element as *const LeafPageElement as *const u8;
                element.pos = unsafe { b_ptr.sub(element_ptr as usize) } as u32;
                element.flags = item.flags as u32;
                element.k_size = item.key.len() as u32;
                element.v_size = item.value.len() as u32;
            } else {
                let mut element = page.branch_page_element_mut(i);
                let element_ptr = element as *const BranchPageElement as *const u8;
                element.pos = unsafe { b_ptr.sub(element_ptr as usize) } as u32;
                element.k_size = item.key.len() as u32;
                element.pgid = item.pg_id;
                assert_ne!(element.pgid, pgid, "write: circular dependency occurred");
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
        debug!("succeed to write node into page");
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
    /// *Note* The oldest(first node) will be free into freelist by free.free()
    pub fn spill(&mut self) -> Result<()> {
        if self.0.spilled.load(Ordering::Acquire) {
            return Ok(());
        }

        let page_size = self.bucket().unwrap().tx()?.db()?.page_size();
        {
            let mut children = self.0.children.borrow_mut().clone();
            info!("child size: {}", children.len());
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
                    info!("found parent node: {}", p.pg_id());
                    node_parent = Some(p.clone());
                    p.0.children.borrow().clone()
                }
            };

            info!(
                "after split node, size={}, pid={}",
                nodes.len(),
                self.pg_id()
            );
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

                // Advoid allocate a hole page, when the node size equals to page_size
                let allocate_size = (node.size() + db.page_size()) / db.page_size();
                let page = tx.allocate(allocate_size as u64)?;
                let page = unsafe { &mut *page };
                {
                    // Write the node.
                    let id = page.id;
                    let txpgid = tx.pgid();
                    if id >= txpgid {
                        panic!("pgid ({}) above high water marl ({})", id, txid);
                    }
                    *node.0.pgid.borrow_mut() = id;
                    node.write(page);
                    node.0.spilled.store(true, Ordering::Release);
                }

                // Insert into parent inodes.
                if let Some(p) = node.parent() {
                    let mut okey = node.0.key.borrow().clone();
                    let nkey = node.0.inodes.borrow()[0].key.to_vec();
                    // TODO: Why?, Fix ME
                    if okey.is_empty() {
                        okey = nkey.clone();
                    }

                    let pgid = *node.0.pgid.borrow();
                    p.put(okey.as_slice(), &nkey, vec![], pgid, 0).unwrap();
                    *node.0.key.borrow_mut() = nkey;
                    assert!(
                        !node.0.key.borrow().is_empty(),
                        "spill: zero-length node key"
                    );
                }

                tx.0.stats.lock().split += 1;
            }
        }

        // If the root node split and created a new root then we need to spill that
        // as well. We'll clear out the children to make sure it doesn't try to respill.
        // eg: |a|b|c|d|e|f| --after split--> p0|a, d|, p1=|a|b|c|, p2 = |d|e|f|, p0 is a new
        // parent, so p0 will be split, but p1, p2 don't need split again (`Note`: the current node
        // will be replaced with new-parent node)
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
            warn!("does not split node: {:?}", self.0.key);
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
                debug!("generate a new parent and set collect with them");
                Ok(Some(parent))
            }
        }
    }

    // Breaks up a node into two smaller nodes, if appropriate.
    // This should only be called from the `split` function.
    fn split_two(&mut self, page_size: usize) -> Result<Option<Node>> {
        // Ignore the split if the page doesn't have at least enough nodes for
        // two pages or if the nodes can fit in a single page.
        {
            let inodes = self.0.inodes.borrow();
            if inodes.len() <= MIN_KEYS_PER_PAGE * 2 || self.size_less_than(page_size) {
                return Ok(None);
            }
        }
        let clamp = |n: f64, min: f64, max: f64| -> f64 {
            if n < min {
                return min;
            }
            if n > max {
                return max;
            }
            return n;
        };
        // Determine the threshold before starting a new node.
        let fill_parent = clamp(
            self.bucket().ok_or(BucketEmpty)?.fill_percent,
            MIN_FILL_PERCENT,
            MAX_FILL_PERCENT,
        );
        let threshold = page_size as f64 * fill_parent;
        // debug!("the threshold is {}", threshold);

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
            .ok_or(BucketEmpty)?
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
        let mut rindex = 0;
        let mut pgsize = PAGE_HEADER_SIZE;

        let inodes = self.0.inodes.borrow();
        let pelsize = self.page_element_size();
        let max = inodes.len() - MIN_KEYS_PER_PAGE;

        for (index, inode) in inodes.iter().enumerate().take(max) {
            rindex = index;
            let elsize = pelsize + inode.key.len() + inode.value.len();
            debug!(
                "found split, threshold: {}, index: {}, sz: {}",
                threshold,
                index,
                pgsize + elsize
            );
            if index >= MIN_KEYS_PER_PAGE && (pgsize + elsize) > threshold {
                break;
            }

            pgsize += elsize;
        }

        (rindex, pgsize)
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
            db.0.free_list.write().free(txid, page);
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

#[derive(Default)]
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
    use std::sync::atomic::Ordering;
    use std::{
        mem::size_of,
        slice::{from_raw_parts, from_raw_parts_mut},
    };

    use crate::node::Node;
    use crate::test_util::mock_log;
    use crate::{
        page::{LeafPageElement, OwnedPage, LEAF_PAGE_FLAG},
        test_util::{mock_bucket, mock_node, mock_tx},
        tx::WeakTx,
    };

    #[test]
    fn create() {
        let tx = mock_tx();
        let bucket = mock_bucket(WeakTx::from(&tx));
        let mut n = mock_node(&bucket);
        n.put(b"baz", b"baz", [1, 22, 3].to_vec(), 0, 0);
        n.put(b"foo", b"foo", b"barack".to_vec(), 0, 0);
        n.put(b"bar", b"bar", b"bill".to_vec(), 0, 0);
        n.put(b"foo", b"fold", b"donald".to_vec(), 0, 1);
        let inodes = n.0.inodes.borrow();
        assert_eq!(inodes.len(), 3);

        assert_eq!(inodes[0].key, b"bar");
        assert_eq!(inodes[0].value, b"bill".to_vec());

        assert_eq!(inodes[1].key, b"baz");
        assert_eq!(inodes[1].value, [1, 22, 3].to_vec());

        assert_eq!(inodes[2].key, b"fold");
        assert_eq!(inodes[2].value, b"donald".to_vec());
    }

    #[test]
    fn read_page() {
        let mut page = {
            let mut page = OwnedPage::new(1024);
            page.id = 1;
            page.over_flow = 0;
            page.flags = LEAF_PAGE_FLAG;
            page.count = 2;
            page
        };

        {
            let nodes =
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

            let data = unsafe {
                from_raw_parts_mut(&mut nodes[2] as *mut LeafPageElement as *mut u8, 1024)
            };
            data[..7].copy_from_slice(b"barfooz");
            data[7..7 + 13].copy_from_slice(b"helloworldbye");
        }

        assert!(
            size_of::<LeafPageElement>() == 16,
            "{}",
            size_of::<LeafPageElement>()
        );

        let tx = mock_tx();
        let bucket = mock_bucket(WeakTx::from(&tx));
        let mut n = mock_node(&bucket);
        n.read(&page);
        let inodes = n.0.inodes.borrow();

        assert!(n.is_leaf());
        assert_eq!(inodes.len(), 2);
        assert_eq!(inodes[0].key, b"bar");
        assert_eq!(inodes[0].value, b"fooz");
        assert_eq!(inodes[1].key, b"helloworld");
        assert_eq!(inodes[1].value, b"bye");
    }

    #[test]
    fn write_page() {
        let tx = mock_tx();
        let bucket = mock_bucket(WeakTx::from(&tx));
        let mut n = mock_node(&bucket);
        n.0.is_leaf.store(true, Ordering::Release);
        n.put(b"susy", b"susy", b"que".to_vec(), 0, LEAF_PAGE_FLAG as u32);
        n.put(
            b"ricki",
            b"ricki",
            b"lake".to_vec(),
            0,
            LEAF_PAGE_FLAG as u32,
        );

        n.put(
            b"john",
            b"john",
            b"johnson".to_vec(),
            0,
            LEAF_PAGE_FLAG as u32,
        );

        let mut page = {
            let mut page = OwnedPage::new(4096);
            page.id = 1;
            page.over_flow = 0;
            page.flags = LEAF_PAGE_FLAG;
            page
        };
        n.write(&mut page);

        let nodes = unsafe {
            std::slice::from_raw_parts_mut(page.get_data_mut_ptr() as *mut LeafPageElement, 3)
        };
        assert_eq!(nodes[0].k_size, 4);
        assert_eq!(nodes[0].v_size, 7);
        assert_eq!(nodes[0].pos, 48);
        assert_eq!(nodes[1].k_size, 5);
        assert_eq!(nodes[1].v_size, 4);
        assert_eq!(nodes[1].pos, 43);
        assert_eq!(nodes[2].k_size, 4);
        assert_eq!(nodes[2].v_size, 3);
        assert_eq!(nodes[2].pos, 36);

        let mut n2 = mock_node(&bucket);
        n2.read(&page);
        let inodes = n2.0.inodes.borrow();
        assert!(n2.is_leaf());
        assert_eq!(inodes.len(), 3);
        assert_eq!(inodes[0].key, b"john",);
        assert_eq!(inodes[0].value, b"johnson");
        assert_eq!(inodes[1].key, b"ricki");
        assert_eq!(inodes[1].value, b"lake");
        assert_eq!(inodes[2].key, b"susy");
        assert_eq!(inodes[2].value, b"que");
    }

    #[test]
    fn split_two() {
        mock_log();
        let tx = mock_tx();
        let bucket = mock_bucket(WeakTx::from(&tx));
        let mut n = mock_node(&bucket);
        n.0.is_leaf.store(true, Ordering::Release);
        n.put(b"00000001", b"00000001", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000002", b"00000002", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000003", b"00000003", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000004", b"00000004", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000005", b"00000005", b"0123456701234567".to_vec(), 0, 0);
        // threshold: 0.5 * 100 = 50, MIN_KEYS_PER_PAGE(NOTE): 2
        // Split between 2 & 3.
        // page_header = 16, elementHeader = 32 * 3, (kv) = (8 + 16) * 3
        // = 136
        let next = n.split_two(100).unwrap();

        assert!(next.is_some());
    }

    #[test]
    fn split_two_fail() {
        mock_log();
        let tx = mock_tx();
        let bucket = mock_bucket(WeakTx::from(&tx));
        let mut n = mock_node(&bucket);
        n.0.is_leaf.store(true, Ordering::Release);
        n.put(b"00000001", b"00000001", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000002", b"00000002", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000003", b"00000003", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000004", b"00000004", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000005", b"00000005", b"0123456701234567".to_vec(), 0, 0);

        // Split between 2 & 3.
        let next = n.split_two(4096).unwrap();

        assert!(next.is_none());
    }

    #[test]
    fn split() {
        let tx = mock_tx();
        let bucket = mock_bucket(WeakTx::from(&tx));
        let mut n = mock_node(&bucket);
        n.0.is_leaf.store(true, Ordering::Release);
        n.put(b"00000001", b"00000001", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000002", b"00000002", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000003", b"00000003", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000004", b"00000004", b"0123456701234567".to_vec(), 0, 0);
        n.put(b"00000005", b"00000005", b"0123456701234567".to_vec(), 0, 0);

        // Split between 2 & 3.
        let parent = n.split(100).unwrap();

        assert!(parent.is_some());
        let parent = parent.unwrap();
        let pchildren = parent.0.children.borrow();
        assert_eq!(pchildren.len(), 2);
        assert_eq!(pchildren[1].0.inodes.borrow().len(), 3);
        assert_eq!(pchildren[0].0.inodes.borrow().len(), 2);
    }

    #[test]
    fn split_big() {
        let tx = mock_tx();
        let bucket = mock_bucket(WeakTx::from(&tx));
        let mut n = mock_node(&bucket);
        n.0.is_leaf.store(true, Ordering::Release);
        for i in 1..1000 {
            let key = format!("{:08}", i);
            n.put(
                &key.as_bytes(),
                &key.as_bytes(),
                b"0123456701234567".to_vec(),
                0,
                0,
            );
        }
        let parent = n.split(4096).unwrap().unwrap();

        assert_eq!(parent.0.children.borrow().len(), 19);
    }
}
