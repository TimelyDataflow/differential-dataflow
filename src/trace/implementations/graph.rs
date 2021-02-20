//! Trace and batch implementations based on sorted ranges.
//!
//! The types and type aliases in this module start with either
//!
//! * `OrdVal`: Collections whose data have the form `(key, val)` where `key` is ordered.
//! * `OrdKey`: Collections whose data have the form `key` where `key` is ordered.
//!
//! Although `OrdVal` is more general than `OrdKey`, the latter has a simpler representation
//! and should consume fewer resources (computation and memory) when it applies.

// use std::cmp::Ordering;
use std::rc::Rc;

// use ::Diff;
// use lattice::Lattice;

use trace::{Batch, BatchReader, Builder, Merger, Cursor, Trace, TraceReader};
use trace::description::Description;
use trace::rc_blanket_impls::RcBatchCursor;

// use trace::layers::MergeBuilder;

use super::spine_fueled::Spine;
use super::merge_batcher::MergeBatcher;

use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

type Node = u32;

///
struct GraphSpine<N> where N: Ord+Clone+'static {
    spine: Spine<Node, N, Product<RootTimestamp, ()>, isize, Rc<GraphBatch<N>>>
}

impl<N> TraceReader<Node, N, Product<RootTimestamp, ()>, isize> for GraphSpine<N>
where
    N: Ord+Clone+'static,
{
    type Batch = Rc<GraphBatch<N>>;
    type Cursor = RcBatchCursor<Node, N, Product<RootTimestamp, ()>, isize, GraphBatch<N>>;

    fn cursor_through(&mut self, upper: &[Product<RootTimestamp,()>]) -> Option<(Self::Cursor, <Self::Cursor as Cursor<Node, N, Product<RootTimestamp,()>, isize>>::Storage)> {

        let mut batch = Vec::new();
        self.spine.map_batches(|b| batch.push(b.clone()));
        assert!(batch.len() <= 1);

        if upper == &[] {
            batch.pop().map(|b| (b.cursor(), b))
        }
        else { None }
    }
    fn set_logical_compaction(&mut self, frontier: &[Product<RootTimestamp,()>]) {
        self.spine.set_logical_compaction(frontier)
    }
    fn get_logical_compaction(&mut self) -> &[Product<RootTimestamp,()>] { self.spine.get_logical_compaction() }
    fn set_physical_compaction(&mut self, frontier: &[Product<RootTimestamp,()>]) {
        self.spine.set_physical_compaction(frontier)
    }
    fn get_physical_compaction(&mut self) -> &[Product<RootTimestamp,()>] { &self.spine.get_physical_compaction() }

    fn map_batches<F: FnMut(&Self::Batch)>(&mut self, f: F) {
        self.spine.map_batches(f)
    }
}

// A trace implementation for any key type that can be borrowed from or converted into `Key`.
// TODO: Almost all this implementation seems to be generic with respect to the trace and batch types.
impl<N> Trace<Node, N, Product<RootTimestamp,()>, isize> for GraphSpine<N>
where
    N: Ord+Clone+'static,
{

    fn new() -> Self {
        GraphSpine {
            spine: Spine::<Node, N, Product<RootTimestamp, ()>, isize, Rc<GraphBatch<N>>>::new()
        }
    }

    // Ideally, this method acts as insertion of `batch`, even if we are not yet able to begin
    // merging the batch. This means it is a good time to perform amortized work proportional
    // to the size of batch.
    fn insert(&mut self, batch: Self::Batch) {
        self.spine.insert(batch)
    }

    fn close(&mut self) {
        self.spine.close()
    }
}

///
#[derive(Debug, Abomonation)]
pub struct GraphBatch<N> {
    index: usize,
    peers: usize,
    keys: Vec<Node>,
    nodes: Vec<usize>,
    edges: Vec<N>,
    desc: Description<Product<RootTimestamp,()>>,
}

impl<N> BatchReader<Node, N, Product<RootTimestamp,()>, isize> for GraphBatch<N> where N: Ord+Clone+'static {
    type Cursor = GraphCursor;
    fn cursor(&self) -> Self::Cursor { GraphCursor { key: self.index as Node, key_pos: 0, val_pos: 0 } }
    fn len(&self) -> usize { self.edges.len() }
    fn description(&self) -> &Description<Product<RootTimestamp,()>> { &self.desc }
}

impl<N> Batch<Node, N, Product<RootTimestamp,()>, isize> for GraphBatch<N> where N: Ord+Clone+'static {
    type Batcher = MergeBatcher<Node, N, Product<RootTimestamp,()>, isize, Self>;
    type Builder = GraphBuilder<N>;
    type Merger = GraphMerger;

    fn begin_merge(&self, other: &Self) -> Self::Merger {
        GraphMerger::new(self, other)
    }
}

///
pub struct GraphMerger { }

impl<N> Merger<Node, N, Product<RootTimestamp,()>, isize, GraphBatch<N>> for GraphMerger where N: Ord+Clone+'static {
    fn new(_batch1: &GraphBatch<N>, _batch2: &GraphBatch<N>) -> Self {
        panic!("Cannot merge GraphBatch; they are static");
    }
    fn done(self) -> GraphBatch<N> {
        panic!("Cannot merge GraphBatch; they are static");
    }
    fn work(&mut self, _source1: &GraphBatch<N>, _source2: &GraphBatch<N>, _frontier: &Option<Vec<Product<RootTimestamp,()>>>, _fuel: &mut usize) {
        panic!("Cannot merge GraphBatch; they are static");
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct GraphCursor {
    key: Node,
    key_pos: usize,
    val_pos: usize,
}

impl<N> Cursor<Node, N, Product<RootTimestamp,()>, isize> for GraphCursor where N: Ord+Clone {

    type Storage = GraphBatch<N>;

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Node { &storage.keys[self.key_pos] }
    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a N { &storage.edges[self.val_pos] }
    fn map_times<L: FnMut(&Product<RootTimestamp,()>, isize)>(&mut self, _storage: &Self::Storage, mut logic: L) {
        logic(&Product::new(RootTimestamp, ()), 1);
    }
    fn key_valid(&self, storage: &Self::Storage) -> bool { (self.key_pos + 1) < storage.nodes.len() }
    fn val_valid(&self, storage: &Self::Storage) -> bool {
        self.val_pos < storage.nodes[self.key_pos + 1]
    }
    fn step_key(&mut self, storage: &Self::Storage){
        if self.key_valid(storage) {
            self.key_pos += 1;
            self.key += storage.peers as Node;
        }
    }
    fn seek_key(&mut self, storage: &Self::Storage, key: &Node) {
        if self.key_valid(storage) {
            self.key_pos = (*key as usize) / storage.peers;
            if self.key_pos + 1 >= storage.nodes.len() {
                self.key_pos = storage.nodes.len() - 1;
            }
            self.val_pos = storage.nodes[self.key_pos];
            self.key = (storage.peers * self.key_pos + storage.index) as Node;
        }
    }
    fn step_val(&mut self, storage: &Self::Storage) {
        if self.val_valid(storage) {
            self.val_pos += 1;
        }
    }
    fn seek_val(&mut self, storage: &Self::Storage, val: &N) {
        if self.val_valid(storage) {
            let lower = self.val_pos;
            let upper = storage.nodes[self.key_pos + 1];

            self.val_pos += advance(&storage.edges[lower .. upper], |tuple| tuple < val);
        }
    }
    fn rewind_keys(&mut self, storage: &Self::Storage) { self.key_pos = 0; self.key = storage.index as Node; }
    fn rewind_vals(&mut self, storage: &Self::Storage) {
        if self.key_valid(storage) {
            self.val_pos = storage.nodes[self.key_pos];
        }
    }
}

/// A builder for creating layers from unsorted update tuples.
pub struct GraphBuilder<N: Ord> {
    index: usize,
    peers: usize,
    keys: Vec<Node>,
    nodes: Vec<usize>,
    edges: Vec<N>,

}

impl<N> Builder<Node, N, Product<RootTimestamp,()>, isize, GraphBatch<N>> for GraphBuilder<N> where N: Ord+Clone+'static {

    fn new() -> Self { Self::with_capacity(0) }
    fn with_capacity(cap: usize) -> Self {
        GraphBuilder {
            index: 0,
            peers: 1,
            keys: Vec::new(),
            nodes: Vec::new(),
            edges: Vec::with_capacity(cap),
        }
    }

    #[inline]
    fn push(&mut self, (key, val, _time, _diff): (Node, N, Product<RootTimestamp,()>, isize)) {
        while self.nodes.len() <= (key as usize) / self.peers {
            self.keys.push((self.peers * self.nodes.len() + self.index) as Node);
            self.nodes.push(self.edges.len());
        }

        self.edges.push(val);
    }

    #[inline(never)]
    fn done(mut self, lower: &[Product<RootTimestamp,()>], upper: &[Product<RootTimestamp,()>], since: &[Product<RootTimestamp,()>]) -> GraphBatch<N> {
        println!("GraphBuilder::done(): {} nodes, {} edges.", self.nodes.len(), self.edges.len());

        self.nodes.push(self.edges.len());
        GraphBatch {
            index: self.index,
            peers: self.peers,
            keys: self.keys,
            nodes: self.nodes,
            edges: self.edges,
            desc: Description::new(lower, upper, since)
        }
    }
}


/// Reports the number of elements satisfing the predicate.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to
/// count the number of elements in time logarithmic in the result.
#[inline(never)]
pub fn advance<T, F: Fn(&T)->bool>(slice: &[T], function: F) -> usize {

    // start with no advance
    let mut index = 0;
    if index < slice.len() && function(&slice[index]) {

        // advance in exponentially growing steps.
        let mut step = 1;
        while index + step < slice.len() && function(&slice[index + step]) {
            index += step;
            step = step << 1;
        }

        // advance in exponentially shrinking steps.
        step = step >> 1;
        while step > 0 {
            if index + step < slice.len() && function(&slice[index + step]) {
                index += step;
            }
            step = step >> 1;
        }

        index += 1;
    }

    index
}
