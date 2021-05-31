use crate::node::{Node, NodeBuilder, WeakNode};
use crate::tx::TX;
use crate::{Page, PgId};
use std::collections::HashMap;

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

#[derive(Default, Clone)]
pub struct SubBucket {
    pub root: PgId,
    pub sequence: u64,
}

#[derive(Default, Clone)]
pub(crate) struct Bucket {
    sub_bucket: SubBucket,
    // the associated transaction
    pub(crate) tx: Box<TX>,
    // subbucket cache
    pub(crate) buckets: HashMap<String, Box<Bucket>>,
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

impl PartialEq for Bucket {
    fn eq(&self, _other: &Self) -> bool {
        unimplemented!()
    }
}

impl Eq for Bucket {}

impl Bucket {
    /// Returns a new `bucket` associated with a transaction.
    pub fn new(tx: TX) -> Self {
        Self {
            tx: Box::new(tx),
            fill_percent: DEFAULT_FILL_PERCENT,
            ..Bucket::default()
        }
    }

    /// Returns the tx of the bucket.
    pub fn tx(&mut self) -> &mut TX {
        &mut self.tx
    }

    /// Returns the root of the bucket.
    pub fn root(&self) -> PgId {
        self.sub_bucket.root
    }

    /// Returns whether the bucket is writable.
    pub fn writeable(&self) -> bool {
        self.tx.writable
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
        let p = self
            .page
            .clone()
            .or_else(|| Some(self.tx.page(pg_id)))
            .unwrap();
        // Read the page into the node and cache it.
        node.read(&p);
        self.nodes.insert(pg_id, node.clone());

        // Update statistics.
        self.tx.stats.node_count += 1;
        node
    }
}
