use crate::error::{Error, Result};
use crate::node::{Node, NodeBuilder, WeakNode};
use crate::tx::{WeakTx, TX};
use crate::{Page, PgId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Weak;

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

pub(crate) struct Bucket {
    sub_bucket: SubBucket,
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
    /// Returns the tx of the bucket.
    pub fn tx(&mut self) -> Result<TX> {
        self.tx.upgrade().ok_or(Error::TxGone)
    }

    /// Returns the root of the bucket.
    pub fn root(&self) -> PgId {
        self.sub_bucket.root
    }

    /// Returns whether the bucket is writable.
    pub fn writeable(&self) -> bool {
        self.tx.writable()
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
    pub(crate) fn node(&mut self, pg_id: PgId, parent: &WeakNode) -> Node {
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
            let page = self.tx.page(pg_id).unwrap();
            unsafe {
                node.read(&*page);
            }
        }
        self.nodes.insert(pg_id, node.clone());
        // Update statistics.
        self.tx.stats().node_count += 1;
        node
    }

    pub fn clear(&mut self) {
        self.buckets.borrow_mut().clear();
        self.nodes.clear();
        self.page = None;
        self.root_node = None;
    }

    pub fn create_bucket(&self, key: &[u8]) -> Result<&mut Bucket> {
        todo!()
    }

    pub fn create_bucket_if_not_exists(&self, key: &[u8]) -> Result<&mut Bucket> {
        todo!()
    }

    pub fn delete_bucket(&self, key: &[u8]) -> Result<()> {
        todo!()
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
