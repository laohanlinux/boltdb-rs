use crate::node::{Node, NodeBuilder, WeakNode};
use crate::tx::TX;
use crate::{Page, PgId};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

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
    pub fn tx(&mut self) -> &mut TX {
        &mut self.tx
    }

    /// Returns the root of the bucket.
    pub fn root(&self) -> PgId {
        self.sub_bucket.root
    }

    /// Returns whether the bucket is writable.
    pub fn writeable(&self) -> bool {
        self.tx.writable()
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
            .or_else(|| Some(self.tx.page(pg_id).unwrap().unwrap()))
            .unwrap();
        // Read the page into the node and cache it.
        node.read(&p);
        self.nodes.insert(pg_id, node.clone());

        // Update statistics.
        self.tx.stats().node_count += 1;
        node
    }
}
