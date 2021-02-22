use crate::bucket::{MAX_FILL_PERCENT, MIN_FILL_PERCENT};
use crate::error::{Error, Result};
use crate::page::{
    BranchPageElement, LeafPageElement, BRANCH_PAGE_ELEMENT_SIZE, BRANCH_PAGE_FLAG,
    LEAF_PAGE_ELEMENT_SIZE, LEAF_PAGE_FLAG, MIN_KEYS_PER_PAGE, PAGE_HEADER_SIZE,
};
use crate::{bucket, search, Bucket, Page, PgId};
use memoffset::ptr::copy_nonoverlapping;
use std::borrow::Borrow;
use std::cell::Cell;
use std::ops::RangeBounds;
use std::slice::Iter;
use std::sync::Arc;

pub(crate) type Key = Vec<u8>;
pub(crate) type Value = Vec<u8>;

/// Represents an in-memory, deserialized `page`.
#[derive(Default, Clone)]
pub(crate) struct Node {
    pub(crate) bucket: Box<Bucket>,
    pub(crate) is_leaf: bool,
    pub(crate) unbalanced: bool,
    pub(crate) spilled: bool,
    // first element key
    pub(crate) key: Key,
    // page's id
    pub(crate) pg_id: PgId,
    pub(crate) parent: Option<Box<Node>>,
    // child's node pointer
    pub(crate) children: Option<Nodes>,
    // all element metadata(branch: key + pg_id, leaf: kv array)
    pub(crate) inodes: Inodes,
}

impl Node {
    pub fn new() -> Self {
        Node::default()
    }

    // Returns the top-level node this node is attached to.
    fn root(&self) -> &Node {
        if let Some(parent) = &self.parent {
            return parent.root();
        }
        self
    }

    // Returns the minimum number of inodes this node should have.
    fn min_keys(&self) -> usize {
        if self.is_leaf {
            return 1;
        }
        2
    }

    // Returns the size of the node after serialization.
    fn size(&self) -> usize {
        self.inodes.iter().fold(PAGE_HEADER_SIZE, |acc, inode| {
            acc + self.page_element_size() + inode.key.len() + inode.value.len()
        })
    }

    // Returns the inodes list.
    #[inline]
    fn inodes(&self) -> &Vec<Inode> {
        &self.inodes.inner
    }

    // Returns true if the node is less than a given size.
    // This is an optimization to avoid calculation a large node when we only need
    // to know if it fits inside a certain page size.
    fn size_less_than(&self, v: usize) -> bool {
        for i in 0..self.inodes().len() {
            let node = &self.inodes()[i];
            if self.page_element_size() + node.key.len() + node.value.len() >= v {
                return false;
            }
        }
        true
    }

    // Returns the size of each page element based on type of node.
    fn page_element_size(&self) -> usize {
        if self.is_leaf {
            return LEAF_PAGE_ELEMENT_SIZE;
        }
        BRANCH_PAGE_ELEMENT_SIZE
    }

    // TODO: why?
    // Returns the child node at a given index.
    fn child_at(&self, index: usize) -> Result<Node> {
        if self.is_leaf {
            return Err(Error::InvalidNode(format!(
                "invalid childAt {} on a leaf node",
                index
            )));
        }
        let pg_id = self.inodes()[index].pg_id;
        Ok(self.bucket.node(pg_id, self))
    }

    // todo: unwrap
    // Returns the child node at a given index.
    fn child_index(&self, child: &Node) -> usize {
        self.inodes.binary_search_by(child.key.as_slice()).unwrap()
    }

    // Returns the number of children.
    fn num_children(&self) -> usize {
        self.inodes().len()
    }

    // Returns the next node with the same parent.
    fn next_sibling(&self) -> Option<Node> {
        if self.parent.is_none() {
            return None;
        }
        let parent = self.parent.as_ref().unwrap();
        let index = parent.child_index(self);
        // self is the last node at the level
        if index >= parent.num_children() - 1 {
            return None;
        }
        parent.child_at(index + 1).ok()
    }

    // Returns the previous node with the same parent.
    fn prev_sibling(&self) -> Option<Node> {
        if self.parent.is_none() {
            return None;
        }
        let parent = self.parent.as_ref().unwrap();
        let index = parent.child_index(self);
        if index == 0 {
            return None;
        }
        parent.child_at(index - 1).ok()
    }

    // Inserts a key/value.
    fn put(
        &mut self,
        old_key: Key,
        new_key: Key,
        value: Value,
        pg_id: PgId,
        flags: u32,
    ) -> Result<()> {
        if pg_id >= self.bucket.tx.meta.pg_id {
            return Err(Error::PutFailed(format!(
                "pgid {:?} above high water mark {:?}",
                pg_id, self.bucket.tx.meta.pg_id
            )));
        } else if old_key.len() <= 0 {
            return Err(Error::PutFailed("zero-length old key".to_string()));
        } else if new_key.len() <= 0 {
            return Err(Error::PutFailed("zero-length new key".to_string()));
        }

        // Find insertion index.
        let (extra, index) = self
            .inodes
            .binary_search_by(old_key.as_slice())
            .map(|index| (true, index))
            .map_err(|index| (false, index))
            .unwrap();

        // Add capacity and shift nodes if we don't have an exact match and need to insert.
        if !extra {
            self.inodes.insert(index, Inode::default());
        }
        let node = self.inodes.get_mut_ref(index);
        node.key = new_key.to_vec();
        node.value = value;
        node.flags = flags;
        node.pg_id = pg_id;
        Ok(())
    }

    // Removes a key from the node.
    fn del(&mut self, key: Key) {
        // Find index of key.
        match self.inodes.binary_search_by(key.as_slice()) {
            Ok(index) => {
                // Delete inode from the node.
                self.inodes.remove(index);
                // Mark the node as needing rebalancing.
                self.unbalanced = true;
            }
            // Exit if the key isn't found.
            _ => return,
        }
    }

    // Initializes the node from a page.
    fn read(&mut self, p: &Page) {
        self.pg_id = p.id;
        self.is_leaf = p.flags & LEAF_PAGE_FLAG != 0;

        for i in 0..(p.count as usize) {
            let mut inode = self.inodes.get_mut_ref(i);
            if self.is_leaf {
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
        if self.inodes.len() > 0 {
            self.key = self.inodes.get_ref(0).key.clone();
            assert!(!self.key.is_empty(), "read: zero-length inode key");
        } else {
            // todo:
            self.key.clear();
        }
    }

    // Writes the items onto one or more pages.
    fn write(&mut self, page: &mut Page) {
        // Initialize page.
        if self.is_leaf {
            page.flags != LEAF_PAGE_FLAG;
        } else {
            page.flags != BRANCH_PAGE_FLAG;
        }

        // TODO: Why?
        if self.inodes.len() >= 0xFFFF {
            panic!("inode overflow: {} (pg_id={})", self.inodes.len(), page.id);
        }

        page.count = self.inodes.len() as u16;
        // Stop here if there are no items to write.
        if page.count == 0 {
            return;
        }

        // Loop over each item and write it to the page.
        let mut b_ptr = unsafe {
            let offset = self.page_element_size() * self.inodes.len();
            page.get_data_mut_ptr().add(offset)
        };
        for (i, item) in self.inodes.iter().enumerate() {
            assert!(!item.key.is_empty(), "write: zero-length inode key");

            // Write the page element.
            if self.is_leaf {
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

    // // Breaks up a node into multiple smaller nodes, if appropriate.
    // // This should only be called from the `spill` function.
    // fn split(&mut self, page_size: usize) -> &[Node] {}
    //
    // // Breaks up a node into two smaller nodes, if appropriate.
    // // This should only be called from the `split` function.
    // fn split_two(mut self, page_size: usize) -> (Option<Node>, Option<Node>) {
    //     // Ignore the split if the page doesn't have at least enough nodes for
    //     // two pages or if the nodes can fit in a single page.
    //     if self.inodes.len() <= MIN_KEYS_PER_PAGE * 2 || self.size_less_than(page_size) {
    //         return (Some(self), None);
    //     }
    //
    //     // Determine the threshold before starting a new node.
    //     let mut fill_parent = self.bucket.fill_percent;
    //     if fill_parent < MIN_FILL_PERCENT {
    //         fill_parent = MIN_FILL_PERCENT;
    //     } else if fill_parent > MAX_FILL_PERCENT {
    //         fill_parent = MAX_FILL_PERCENT;
    //     }
    //     let threshold = page_size as f64 * fill_percent;
    //
    //     // Determine split position and sizes of the two pages.
    //     let split_index = self.split_index(threshold).0;
    //
    //     // Split node into two separate nodes.
    //     // If there's no parent then we'll need to create one.
    //     if self.parent.is_none() {
    //         let mut parent_node = Node::default();
    //         parent_node.bucket = self.bucket.clone();
    //         parent_node.children =
    //     }
    //     return (None, None);
    // }

    // Finds the position where a page will fill a given threshold.
    // It returns the index as well as the size of the first page.
    // This is only be called from `split`.
    fn split_index(&mut self, threshold: usize) -> (usize, usize) {
        let mut size = PAGE_HEADER_SIZE;
        let mut index: usize = 0;

        // Loop until we only have the minimum number of keys required for the second page.
        for i in 0..self.inodes().len() - MIN_KEYS_PER_PAGE {
            index = i;

            let inode = &self.inodes()[i];
            let el_size =
                self.page_element_size() + inode.clone().key.len() + inode.clone().value.len();

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

    // /// Writes the nodes to dirty pages and splits nodes as it goes.
    // /// Returns an error if dirty pages cannot be allocated.
    // fn spill(&mut self) -> Result<()> {
    //
    // }

    //     /// Attempts to combine the node with sibling nodes if the node fill
    //     /// size is below a threshold or if there are not enough keys.
    //     fn rebalance(&mut self) {
    //     }

    // Removes a node from the list of in-memory children.
    // This dose not affect the inodes.
    fn remove_child(&self, target: &Node) {}

    // Adds the node's underlying `page` to the freelist.
    fn free(&mut self) {
        if self.pg_id != 0 {
            self.bucket
                .tx
                .db
                .free_list
                .free(self.bucket.tx.meta.tx_id, &self.bucket.tx.page(self.pg_id));
            self.pg_id = 0;
        }
    }
}

impl PartialEq for Node {
    fn eq(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

impl Eq for Node {}

impl Drop for Node {
    fn drop(&mut self) {
        unimplemented!()
    }
}

/// Adds the node's underlying page to the freelist.
#[derive(Default, Clone)]
pub(crate) struct Nodes {
    inner: Vec<Node>,
}

impl Nodes {
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, Node> {
        self.inner.iter()
    }

    #[inline]
    pub fn as_slice(&self) -> &Vec<Node> {
        &self.inner
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn push(&mut self, node: Node) {
        self.inner.push(node)
    }
}

#[derive(Default, Clone)]
pub(crate) struct Inode {
    pub(crate) flags: u32,
    pub(crate) pg_id: PgId,
    pub(crate) key: Key,
    pub(crate) value: Key,
}

#[derive(Default, Clone)]
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
    fn is_work() {
        assert_eq!(1, 1)
    }

    // /// Ensure that a node can insert a key/value.
    // #[test]
    // fn test_node_put() {}
    //
    // /// Ensure that a node can deserialize from a leaf page.
    // #[test]
    // fn test_node_read_leaf_page() {}
    //
    // /// Ensure that a node can serialize into a leaf page.
    // #[test]
    // fn test_node_write_leaf_page() {}
    //
    // /// Ensure that a node can split into appropriate subgroups.
    // #[test]
    // fn test_node_split() {}
    //
    // /// Ensure that a page with the minimum number of inodes just returns a single node.
    // #[test]
    // fn test_node_split_min_keys() {}
    //
    // /// Ensure that a node that has keys that all fit on a page just returns one leaf.
    // #[test]
    // fn test_node_split_single_page() {}
}
