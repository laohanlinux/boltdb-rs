use crate::bucket::{MAX_FILL_PERCENT, MIN_FILL_PERCENT};
use crate::error::{Error, Result};
use crate::page::{
    BRANCH_PAGE_ELEMENT_SIZE, LEAF_PAGE_ELEMENT_SIZE, LEAF_PAGE_FLAG, MIN_KEYS_PER_PAGE,
    PAGE_HEADER_SIZE,
};
use crate::{search, Bucket, Page, PgId};
use std::slice::Iter;

pub(crate) type Key = Vec<u8>;
pub(crate) type Value = Vec<u8>;

/// Represents an in-memory, deserialized `page`.
#[derive(Default, Clone)]
pub(crate) struct Node {
    pub(crate) bucket: Box<Bucket>,
    pub(crate) is_leaf: bool,
    pub(crate) unbalanced: bool,
    pub(crate) spilled: bool,
    pub(crate) key: Key,
    pub(crate) pg_id: PgId,
    pub(crate) parent: Option<Box<Node>>,
    pub(crate) children: Option<Nodes>,
    pub(crate) inodes: Inodes,
}

impl Node {
    pub fn new() -> Self {
        Node::default()
    }

    /// Returns the top-level node this node is attached to.
    fn root(&self) -> &Node {
        if let Some(r) = &self.parent {
            return r.root();
        }
        self
    }

    /// Returns the minimum number of inodes this node should have.
    fn min_keys(&self) -> usize {
        if self.is_leaf {
            return 1;
        }
        2
    }

    /// Returns the size of the node after serialization.
    fn size(&self) -> usize {
        self.inodes.iter().fold(PAGE_HEADER_SIZE, |acc, inode| {
            acc + self.page_element_size() + inode.key.len() + inode.value.len()
        })
    }

    /// Returns true if the node is less than a given size.
    /// This is an optimization to avoid calculation a large node when we only need
    /// to know if it fits inside a certain page size.
    fn size_less_than(&self, v: usize) -> bool {
        for i in 0..self.inodes().len() {
            let node = &self.inodes()[i];
            if self.page_element_size() + node.key.len() + node.value.len() >= v {
                return false;
            }
        }
        true
    }

    /// Returns the size of each page element based on type of node.
    #[inline]
    fn page_element_size(&self) -> usize {
        if self.is_leaf {
            return LEAF_PAGE_ELEMENT_SIZE;
        }
        BRANCH_PAGE_ELEMENT_SIZE
    }

    /// Returns the child node at a given index.
    fn child_at(&self, index: usize) -> Result<Node> {
        if self.is_leaf {
            return Err(Error::InvalidNode(format!(
                "invalid childAt{} on a leaf node",
                index
            )));
        }
        let pg_id = self.inodes()[index].pg_id;
        Ok(self.bucket.node(pg_id, self))
    }

    /// Returns the child node at a given index.
    fn child_index(&self, child: Node) -> usize {
        search(self.inodes.len(), |idx| {
            self.inodes()[idx].key.lt(&child.key)
        })
    }

    /// Returns the next node with the same parent.
    fn next_sibling(&self) -> Option<Node> {
        if let Some(parent) = self.parent.clone() {
            let index = parent.child_index(self.clone());
            return match index >= parent.num_children() - 1 {
                true => None,
                false => Some(parent.child_at(index).unwrap()),
            };
        }
        None
    }

    /// Returns the previous node with the same parent.
    fn prev_sibling(&self) -> Option<Node> {
        if let Some(parent) = self.parent.clone() {
            let index = parent.child_index(self.clone());
            return match index == 0 {
                true => None,
                false => Some(parent.child_at(index).unwrap()),
            };
        }
        None
    }

    /// Inserts a key/value
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
        let index = search(self.inodes.len(), |idx| {
            self.inodes()[idx].key.lt(&old_key.clone())
        });

        // Add capacity and shift nodes if we don't have an exact match and need to insert.
        if !(self.inodes().len() > 0
            && index < self.inodes().len()
            && self.inodes()[index].key.eq(&old_key))
        {
            self.inodes.inner.push(Inode::default());
            let src = &mut self.inodes.clone().inner[index..];
            self.inodes.inner[index + 1..].clone_from_slice(src);
        }

        let inode = Inode {
            flags,
            pg_id,
            key: new_key,
            value,
        };
        self.inodes.inner[index] = inode;
        Ok(())
    }

    /// Removes a key from the node.
    fn del(&mut self, key: Key) {
        // Find index of key.
        let index = search(self.inodes.len(), |idx| {
            self.inodes()[idx].key.lt(&key.clone())
        });

        // Exit if the key isn't found.
        if index >= self.inodes.len() || !self.inodes()[index].key.eq(&key) {
            return;
        }

        // Delete inode from the node.
        self.inodes.inner = [
            &self.inodes().clone()[..index],
            &self.inodes().clone()[index + 1..],
        ]
            .concat();

        // Mark the node as needing rebalancing.
        self.unbalanced = true;
    }

    /// Initializes the node from a page.
    fn read(&mut self, p: &Page) {
        self.pg_id = p.id;
        self.is_leaf = p.flags & LEAF_PAGE_FLAG != 0;

        for i in 0..p.count {
            let i = i as usize;
            let mut inode = self.inodes.inner[i].clone();
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
        }

        // Save first key so we can find the node in the parent when we spill.
        if self.inodes.len() > 0 {
            self.key = self.inodes.inner[0].clone().key;
        }
    }

    /// Writes the items onto one or more pages
    fn write(&mut self, _p: &mut Page) -> Result<()> {
        Ok(())
    }

    /// Breaks up a node into multiple smaller nodes, if appropriate.
    /// This should only be called from the `spill` function.
    fn split(&mut self, page_size: usize) -> Vec<Node> {
        let mut nodes: Vec<Node> = vec![];
        let mut node = self.clone();
        loop {
            // split node into two.
            let (a, b) = node.split_two(page_size);
            nodes.push(a.unwrap());

            // if we can't split then exit the loop.
            if b.is_none() {
                break;
            }

            // set node to b so it gets split on the next iteration.
            node = b.unwrap()
        }
        nodes
    }

    /// Breaks up a node into two smaller nodes, if appropriate.
    /// This should only be called from the `split` function.
    fn split_two(&mut self, page_size: usize) -> (Option<Node>, Option<Node>) {
        // Ignore the split if the page doesn't have at least enough nodes for
        // two pages or if the nodes can fit in a single page.
        if self.inodes.len() <= (MIN_KEYS_PER_PAGE * 2) || self.size_less_than(page_size) {
            return (Some(self.clone()), None);
        }

        // Determine the threshold before starting a new node.
        let mut fill_percent = self.bucket.fill_percent;
        if fill_percent < MIN_FILL_PERCENT {
            fill_percent = MIN_FILL_PERCENT;
        } else if fill_percent > MAX_FILL_PERCENT {
            fill_percent = MAX_FILL_PERCENT;
        }
        let threshold = ((page_size as f64) * fill_percent) as usize;

        // Determine split position and sizes of the two pages.
        let (split_index, _) = self.split_index(threshold);

        // Split node into two separate nodes.
        // If there's no parent then we'll need to create one.
        if self.parent.is_none() {
            let n = Node {
                bucket: self.bucket.clone(),
                children: Some(Nodes { inner: vec![self.clone()] }),
                ..Node::default()
            };
            self.parent = Some(Box::new(n));
        }

        // Create a new node and add it to the parent.
        let mut next = Node {
            bucket: self.bucket.clone(),
            is_leaf: self.is_leaf,
            parent: self.parent.clone(),
            ..Node::default()
        };
        self.parent.take().unwrap()
            .children.take().unwrap().inner.push(next.clone());

        // Split inodes across two nodes.
        next.inodes.inner = self.inodes.inner[split_index..].to_vec();
        self.inodes.inner.truncate(split_index);

        // Update the statistics.
        self.bucket.tx.stats.split += 1;

        (Some(self.clone()), Some(next))
    }

    /// Finds the position where a page will fill a given threshold.
    /// It returns the index as well as the size of the first page.
    /// This is only be called from `split`.
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

    /// Writes the nodes to dirty pages and splits nodes as it goes.
    /// Returns an error if dirty pages cannot be allocated.
    fn spill(&mut self) -> Result<()> {
        Ok(())
    }

    /// Attempts to combine the node with sibling nodes if the node fill
    /// size is below a threshold or if there are not enough keys.
    fn rebalance(&mut self) {}

    /// Removes a node from the list of in-memory children.
    /// This dose not affect the inodes.
    fn remove_child(&mut self, target: &Node) {
        // If there is a node in the children that
        // is the same as the given node then delete it.
        self.children.take().unwrap()
            .inner
            .iter().
            filter(|node| *node != target);
    }

    /// Cause the node to copy all its inode key/value references to heap memory.
    /// This is required when the mmap is reallocated so inodes are not pointing to stable data.
    fn dereference(&self) {}

    /// Adds the node's underlying `page` to the freelist.
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

    /// Returns the number of children.
    #[inline]
    fn num_children(&self) -> usize {
        self.inodes().len()
    }

    /// Returns the inodes list.
    #[inline]
    fn inodes(&self) -> &Vec<Inode> {
        &self.inodes.inner
    }
}

impl PartialEq for Node {
    fn eq(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

impl Eq for Node {}

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
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, Inode> {
        self.inner.iter()
    }

    #[inline]
    pub fn as_slice(&self) -> &Vec<Inode> {
        &self.inner
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    #[inline]
    pub fn push(&mut self, inode: Inode) {
        self.inner.push(inode)
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
