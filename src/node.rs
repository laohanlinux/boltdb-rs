use crate::bucket::{MAX_FILL_PERCENT, MIN_FILL_PERCENT};
use crate::error::Error::BucketEmpty;
use crate::error::{Error, Result};
use crate::page::{
    BranchPageElement, LeafPageElement, BRANCH_PAGE_ELEMENT_SIZE, BRANCH_PAGE_FLAG,
    LEAF_PAGE_ELEMENT_SIZE, LEAF_PAGE_FLAG, MIN_KEYS_PER_PAGE, PAGE_HEADER_SIZE,
};
use crate::{bucket, search, Bucket, Page, PgId};
use memoffset::ptr::copy_nonoverlapping;
use std::borrow::{Borrow, BorrowMut};
use std::cell::{Cell, Ref, RefCell, RefMut};
use std::ops::{Deref, RangeBounds};
use std::process::id;
use std::rc::{Rc, Weak};
use std::slice::Iter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

pub(crate) type Key = Vec<u8>;
pub(crate) type Value = Vec<u8>;

#[derive(Debug)]
pub(crate) struct  NodeInner {
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

impl NodeInner {
    fn is_leaf(&self) -> bool {
        self.is_leaf.load(Ordering::Release)
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
    fn root(&self) -> Node {
        match self.parent() {
            Some(ref p) => p.root(),
            None => self.clone(),
        }
    }

    /// Attempts to combine the node with sibling nodes if the node fill
    /// size is below a threshold or if there are not enough keys.
    pub fn rebalance(&mut self) {
        {
            let selfsize = self.size();
            if !self.0.unbalanced.load(Ordering::Acquire) {
                return;
            }
            self.0.unbalanced.store(false, Ordering::Acquire);

            // updates stats and get threshold
            let threshold = {
                let bucket = self.bucket_mut().unwrap();
                let tx = bucket.tx().unwrap();
                tx.0.stats.lock().rebalance += 1;
                tx.db().unwrap().page_size() / 4
            };

            if selfsize > threshold && self.0.inodes.borrow().len() > self.min_keys() as usize {
                return;
            }

            // Root node has special handling
            if self.parent().is_none() {
                let mut inodes = self.0.inodes.borrow_mut();
                if !self.is_leaf() && inodes.len() == 1 {
                    let mut child = self
                        .bucket_mut()
                        .unwrap()
                        .node(inodes[0].pg_id, WeakNode::from(self));
                    self.0.is_leaf.store(child.is_leaf(), Ordering::Release);
                }
            }
        }
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
    fn size(&self) -> usize {
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
    pub(crate) fn del(&mut self, key: &Key) {
        // Find index of key.
        match self
            .0
            .inodes
            .borrow()
            .binary_search_by(|inode| inode.key.cmp(key))
            {
                Ok(index) => {
                    // Delete inode from the node.
                    self.0.inodes.borrow_mut().remove(index);
                    // Mark the node as needing rebalancing.
                    self.0.unbalanced.store(true, Ordering::Release);
                }
                // Exit if the key isn't found.
                _ => return,
            }
    }

    // Inserts a key/value.
    fn put(&self, old_key: Key, new_key: Key, value: Value, pg_id: PgId, flags: u32) -> Result<()> {
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
        let (extra, index) = inodes
            .binary_search_by(|inode| inode.key.cmp(&old_key))
            .map(|index| (true, index))
            .map_err(|index| (false, index))
            .unwrap();
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
    fn write(&mut self, page: &mut Page) {
        // Initialize page.
        if self.is_leaf() {
            page.flags != LEAF_PAGE_FLAG;
        } else {
            page.flags != BRANCH_PAGE_FLAG;
        }

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

    // Writes the nodes to dirty pages and splits nodes as it goes.
    // Returns an error if dirty pages cannot be allocated.
    fn spill(&mut self) -> Result<()> {
        // if self.0.spilled.load(Ordering::Acquire) {
        //     return Ok(());
        // }
        //
        // let page_size = self.bucket().unwrap().tx().db().page_size;
        // {
        //     // Why?
        //     let mut children = self.0.children.borrow_mut().clone();
        //     children.sort_by_key(|node| node.0.key);
        //     for child in &mut *children {
        //         // child.split()?;
        //     }
        //     self.0.children.borrow_mut().clear();
        // }
        Ok(())
    }

    // Breaks up a node into multiple smaller nodes, if appropriate.
    // This should only be called from the spill() function.
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
                children.remove(index.unwrap());
                for node in nodes {
                    *node.0.parent.borrow_mut() = WeakNode::from(&p);
                    children.push(node);
                }
                // hard work: 🤣🤣🤣
                drop(children);

                Ok(Some(p))
            }
            None => {
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
        *next.0.inodes.borrow_mut() = nodes;
        // FIXME: add statistics
        self.bucket_mut()
            .ok_or_else(|| BucketEmpty)?
            .tx().unwrap()
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
    fn free(&mut self) {
        // let pgid = self.0.pgid.borrow().clone();
        // if pgid != 0 {
        //     let mut bucket = self.bucket_mut().unwrap();
        //     let tx = bucket.tx();
        //     let tx_id = tx.id();
        //     let page = unsafe {&*tx.page(pgid).unwrap().unwrap()};
        //     // bucket
        //     //     .tx
        //     //     .db
        //     //     .0
        //     //     .free_list
        //     //     .free(bucket.tx.meta.tx_id, &bucket.tx.page(pgid));
        //     self.0.pgid.replace(0);
        // }
    }

    fn node_builder(bucket: *const Bucket) {}
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
