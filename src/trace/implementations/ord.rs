//! Trace and batch implementations based on sorted ranges.
//!
//! The types and type aliases in this module start with either
//!
//! * `OrdVal`: Collections whose data have the form `(key, val)` where `key` is ordered.
//! * `OrdKey`: Collections whose data have the form `key` where `key` is ordered.
//!
//! Although `OrdVal` is more general than `OrdKey`, the latter has a simpler representation
//! and should consume fewer resources (computation and memory) when it applies.

use std::rc::Rc;
use std::convert::{TryFrom, TryInto};
use std::marker::PhantomData;
use std::fmt::Debug;

use timely::progress::{Antichain, frontier::AntichainRef};

use ::difference::Semigroup;
use lattice::Lattice;

use trace::layers::{Trie, TupleBuilder};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::ordered::{OrdOffset, OrderedLayer, OrderedBuilder, OrderedCursor};
use trace::layers::ordered_leaf::{OrderedLeaf, OrderedLeafBuilder};
use trace::{Batch, BatchReader, Builder, Merger, Cursor};
use trace::description::Description;

use trace::layers::MergeBuilder;

// use super::spine::Spine;
use super::spine_fueled::Spine;
use super::merge_batcher::MergeBatcher;

use abomonation::abomonated::Abomonated;

/// A trace implementation using a spine of ordered lists.
pub type OrdValSpine<K, V, T, R, O=usize> = Spine<K, V, T, R, Rc<OrdValBatch<K, V, T, R, O>>>;

/// A trace implementation using a spine of abomonated ordered lists.
pub type OrdValSpineAbom<K, V, T, R, O=usize> = Spine<K, V, T, R, Rc<Abomonated<OrdValBatch<K, V, T, R, O>, Vec<u8>>>>;

/// A trace implementation for empty values using a spine of ordered lists.
pub type OrdKeySpine<K, T, R, O=usize> = Spine<K, (), T, R, Rc<OrdKeyBatch<K, T, R, O>>>;

/// A trace implementation for empty values using a spine of abomonated ordered lists.
pub type OrdKeySpineAbom<K, T, R, O=usize> = Spine<K, (), T, R, Rc<Abomonated<OrdKeyBatch<K, T, R, O>, Vec<u8>>>>;


/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug, Abomonation)]
pub struct OrdValBatch<K, V, T, R, O=usize>
where
    K: Ord,
    V: Ord,
    T: Lattice,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    /// Where all the dataz is.
    pub layer: OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>, O>, O>,
    /// Description of the update times this layer represents.
    pub desc: Description<T>,
}

impl<K, V, T, R, O> BatchReader<K, V, T, R> for OrdValBatch<K, V, T, R, O>
where
    K: Ord+Clone+'static,
    V: Ord+Clone+'static,
    T: Lattice+Ord+Clone+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    type Cursor = OrdValCursor<V, T, R, O>;
    fn cursor(&self) -> Self::Cursor { OrdValCursor { cursor: self.layer.cursor() } }
    fn len(&self) -> usize { <OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>, O>, O> as Trie>::tuples(&self.layer) }
    fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, V, T, R, O> Batch<K, V, T, R> for OrdValBatch<K, V, T, R, O>
where
    K: Ord+Clone+'static,
    V: Ord+Clone+'static,
    T: Lattice+timely::progress::Timestamp+Ord+Clone+::std::fmt::Debug+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    type Batcher = MergeBatcher<K, V, T, R, Self>;
    type Builder = OrdValBuilder<K, V, T, R, O>;
    type Merger = OrdValMerger<K, V, T, R, O>;

    fn begin_merge(&self, other: &Self, compaction_frontier: Option<AntichainRef<T>>) -> Self::Merger {
        OrdValMerger::new(self, other, compaction_frontier)
    }
}

impl<K, V, T, R, O> OrdValBatch<K, V, T, R, O>
where
    K: Ord+Clone+'static,
    V: Ord+Clone+'static,
    T: Lattice+Ord+Clone+::std::fmt::Debug+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    fn advance_builder_from(layer: &mut OrderedBuilder<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>, O>, O>, frontier: AntichainRef<T>, key_pos: usize) {

        let key_start = key_pos;
        let val_start: usize = layer.offs[key_pos].try_into().unwrap();
        let time_start: usize = layer.vals.offs[val_start].try_into().unwrap();

        // We have unique ownership of the batch, and can advance times in place.
        // We must still sort, collapse, and remove empty updates.

        // We will zip throught the time leaves, calling advance on each,
        //    then zip through the value layer, sorting and collapsing each,
        //    then zip through the key layer, collapsing each .. ?

        // 1. For each (time, diff) pair, advance the time.
        for i in time_start .. layer.vals.vals.vals.len() {
            layer.vals.vals.vals[i].0.advance_by(frontier);
        }

        // 2. For each `(val, off)` pair, sort the range, compact, and rewrite `off`.
        //    This may leave `val` with an empty range; filtering happens in step 3.
        let mut write_position = time_start;
        for i in val_start .. layer.vals.keys.len() {

            // NB: batch.layer.vals.offs[i+1] will be used next iteration, and should not be changed.
            //     we will change batch.layer.vals.offs[i] in this iteration, from `write_position`'s
            //     initial value.

            let lower: usize = layer.vals.offs[i].try_into().unwrap();
            let upper: usize = layer.vals.offs[i+1].try_into().unwrap();

            layer.vals.offs[i] = O::try_from(write_position).unwrap();

            let updates = &mut layer.vals.vals.vals[..];

            // sort the range by the times (ignore the diffs; they will collapse).
            let count = crate::consolidation::consolidate_slice(&mut updates[lower .. upper]);

            for index in lower .. (lower + count) {
                updates.swap(write_position, index);
                write_position += 1;
            }
        }
        layer.vals.vals.vals.truncate(write_position);
        layer.vals.offs[layer.vals.keys.len()] = O::try_from(write_position).unwrap();

        // 3. For each `(key, off)` pair, (values already sorted), filter vals, and rewrite `off`.
        //    This may leave `key` with an empty range. Filtering happens in step 4.
        let mut write_position = val_start;
        for i in key_start .. layer.keys.len() {

            // NB: batch.layer.offs[i+1] must remain as is for the next iteration.
            //     instead, we update batch.layer.offs[i]

            let lower: usize = layer.offs[i].try_into().unwrap();
            let upper: usize = layer.offs[i+1].try_into().unwrap();

            layer.offs[i] = O::try_from(write_position).unwrap();

            // values should already be sorted, but some might now be empty.
            for index in lower .. upper {
                let val_lower: usize = layer.vals.offs[index].try_into().unwrap();
                let val_upper: usize = layer.vals.offs[index+1].try_into().unwrap();
                if val_lower < val_upper {
                    layer.vals.keys.swap(write_position, index);
                    layer.vals.offs[write_position+1] = layer.vals.offs[index+1];
                    write_position += 1;
                }
            }
            // batch.layer.offs[i+1] = write_position;
        }
        layer.vals.keys.truncate(write_position);
        layer.vals.offs.truncate(write_position + 1);
        layer.offs[layer.keys.len()] = O::try_from(write_position).unwrap();

        // 4. Remove empty keys.
        let mut write_position = key_start;
        for i in key_start .. layer.keys.len() {

            let lower: usize = layer.offs[i].try_into().unwrap();
            let upper: usize = layer.offs[i+1].try_into().unwrap();

            if lower < upper {
                layer.keys.swap(write_position, i);
                // batch.layer.offs updated via `dedup` below; keeps me sane.
                write_position += 1;
            }
        }
        layer.offs.dedup();
        layer.keys.truncate(write_position);
        layer.offs.truncate(write_position+1);
    }
}

/// State for an in-progress merge.
pub struct OrdValMerger<K, V, T, R, O=usize>
where
    K: Ord+Clone+'static,
    V: Ord+Clone+'static,
    T: Lattice+Ord+Clone+::std::fmt::Debug+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    // first batch, and position therein.
    lower1: usize,
    upper1: usize,
    // second batch, and position therein.
    lower2: usize,
    upper2: usize,
    // result that we are currently assembling.
    result: <OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>, O>, O> as Trie>::MergeBuilder,
    description: Description<T>,
    should_compact: bool,
}

impl<K, V, T, R, O> Merger<K, V, T, R, OrdValBatch<K, V, T, R, O>> for OrdValMerger<K, V, T, R, O>
where
    K: Ord+Clone+'static,
    V: Ord+Clone+'static,
    T: Lattice+timely::progress::Timestamp+Ord+Clone+::std::fmt::Debug+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    fn new(batch1: &OrdValBatch<K, V, T, R, O>, batch2: &OrdValBatch<K, V, T, R, O>, compaction_frontier: Option<AntichainRef<T>>) -> Self {

        assert!(batch1.upper() == batch2.lower());

        let mut since = batch1.description().since().join(batch2.description().since());
        if let Some(compaction_frontier) = compaction_frontier {
            since = since.join(&compaction_frontier.to_owned());
        }

        let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);

        OrdValMerger {
            lower1: 0,
            upper1: batch1.layer.keys(),
            lower2: 0,
            upper2: batch2.layer.keys(),
            result: <<OrderedLayer<K, OrderedLayer<V, OrderedLeaf<T, R>, O>, O> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
            description: description,
            should_compact: compaction_frontier.is_some(),
        }
    }
    fn done(self) -> OrdValBatch<K, V, T, R, O> {

        assert!(self.lower1 == self.upper1);
        assert!(self.lower2 == self.upper2);

        OrdValBatch {
            layer: self.result.done(),
            desc: self.description,
        }
    }
    fn work(&mut self, source1: &OrdValBatch<K,V,T,R,O>, source2: &OrdValBatch<K,V,T,R,O>, fuel: &mut isize) {

        let starting_updates = self.result.vals.vals.vals.len();
        let mut effort = 0isize;

        let initial_key_pos = self.result.keys.len();

        // while both mergees are still active
        while self.lower1 < self.upper1 && self.lower2 < self.upper2 && effort < *fuel {
            self.result.merge_step((&source1.layer, &mut self.lower1, self.upper1), (&source2.layer, &mut self.lower2, self.upper2));
            effort = (self.result.vals.vals.vals.len() - starting_updates) as isize;
        }

        // Merging is complete; only copying remains. Copying is probably faster than merging, so could take some liberties here.
        if self.lower1 == self.upper1 || self.lower2 == self.upper2 {
            // Limit merging by remaining fuel.
            let remaining_fuel = *fuel - effort;
            if remaining_fuel > 0 {
                if self.lower1 < self.upper1 {
                    let mut to_copy = remaining_fuel as usize;
                    if to_copy < 1_000 { to_copy = 1_000; }
                    if to_copy > (self.upper1 - self.lower1) { to_copy = self.upper1 - self.lower1; }
                    self.result.copy_range(&source1.layer, self.lower1, self.lower1 + to_copy);
                    self.lower1 += to_copy;
                }
                if self.lower2 < self.upper2 {
                    let mut to_copy = remaining_fuel as usize;
                    if to_copy < 1_000 { to_copy = 1_000; }
                    if to_copy > (self.upper2 - self.lower2) { to_copy = self.upper2 - self.lower2; }
                    self.result.copy_range(&source2.layer, self.lower2, self.lower2 + to_copy);
                    self.lower2 += to_copy;
                }
            }
        }

        effort = (self.result.vals.vals.vals.len() - starting_updates) as isize;

        // if we are supplied a frontier, we should compact.
        if self.should_compact {
            OrdValBatch::advance_builder_from(&mut self.result, self.description.since().borrow(), initial_key_pos);
        }

        *fuel -= effort;

        // if *fuel < -1_000_000 {
        //     eprintln!("Massive deficit OrdVal::work: {}", fuel);
        // }
    }
}

/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdValCursor<V, T, R, O=usize>
where
    V: Ord+Clone,
    T: Lattice+Ord+Clone,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    cursor: OrderedCursor<OrderedLayer<V, OrderedLeaf<T, R>, O>>,
}

impl<K, V, T, R, O> Cursor<K, V, T, R> for OrdValCursor<V, T, R, O>
where
    K: Ord+Clone,
    V: Ord+Clone,
    T: Lattice+Ord+Clone,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    type Storage = OrdValBatch<K, V, T, R, O>;

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { &self.cursor.key(&storage.layer) }
    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a V { &self.cursor.child.key(&storage.layer.vals) }
    fn map_times<L: FnMut(&T, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        self.cursor.child.child.rewind(&storage.layer.vals.vals);
        while self.cursor.child.child.valid(&storage.layer.vals.vals) {
            logic(&self.cursor.child.child.key(&storage.layer.vals.vals).0, &self.cursor.child.child.key(&storage.layer.vals.vals).1);
            self.cursor.child.child.step(&storage.layer.vals.vals);
        }
    }
    fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.valid(&storage.layer) }
    fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.child.valid(&storage.layer.vals) }
    fn step_key(&mut self, storage: &Self::Storage){ self.cursor.step(&storage.layer); }
    fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek(&storage.layer, key); }
    fn step_val(&mut self, storage: &Self::Storage) { self.cursor.child.step(&storage.layer.vals); }
    fn seek_val(&mut self, storage: &Self::Storage, val: &V) { self.cursor.child.seek(&storage.layer.vals, val); }
    fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind(&storage.layer); }
    fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.child.rewind(&storage.layer.vals); }
}


/// A builder for creating layers from unsorted update tuples.
pub struct OrdValBuilder<K, V, T, R, O=usize>
where
    K: Ord,
    V: Ord,
    T: Ord+Lattice,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    builder: OrderedBuilder<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>, O>, O>,
}

impl<K, V, T, R, O> Builder<K, V, T, R, OrdValBatch<K, V, T, R, O>> for OrdValBuilder<K, V, T, R, O>
where
    K: Ord+Clone+'static,
    V: Ord+Clone+'static,
    T: Lattice+timely::progress::Timestamp+Ord+Clone+::std::fmt::Debug+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{

    fn new() -> Self {
        OrdValBuilder {
            builder: OrderedBuilder::<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>, O>, O>::new()
        }
    }
    fn with_capacity(cap: usize) -> Self {
        OrdValBuilder {
            builder: <OrderedBuilder<K, OrderedBuilder<V, OrderedLeafBuilder<T, R>, O>, O> as TupleBuilder>::with_capacity(cap)
        }
    }

    #[inline]
    fn push(&mut self, (key, val, time, diff): (K, V, T, R)) {
        self.builder.push_tuple((key, (val, (time, diff))));
    }

    #[inline(never)]
    fn done(self, lower: Antichain<T>, upper: Antichain<T>, since: Antichain<T>) -> OrdValBatch<K, V, T, R, O> {
        OrdValBatch {
            layer: self.builder.done(),
            desc: Description::new(lower, upper, since)
        }
    }
}




/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Debug, Abomonation)]
pub struct OrdKeyBatch<K, T, R, O=usize>
where
    K: Ord,
    T: Lattice,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    /// Where all the dataz is.
    pub layer: OrderedLayer<K, OrderedLeaf<T, R>, O>,
    /// Description of the update times this layer represents.
    pub desc: Description<T>,
}

impl<K, T, R, O> BatchReader<K, (), T, R> for OrdKeyBatch<K, T, R, O>
where
    K: Ord+Clone+'static,
    T: Lattice+Ord+Clone+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    type Cursor = OrdKeyCursor<T, R, O>;
    fn cursor(&self) -> Self::Cursor {
        OrdKeyCursor {
            empty: (),
            valid: true,
            cursor: self.layer.cursor(),
            phantom: PhantomData
        }
    }
    fn len(&self) -> usize { <OrderedLayer<K, OrderedLeaf<T, R>, O> as Trie>::tuples(&self.layer) }
    fn description(&self) -> &Description<T> { &self.desc }
}

impl<K, T, R, O> Batch<K, (), T, R> for OrdKeyBatch<K, T, R, O>
where
    K: Ord+Clone+'static,
    T: Lattice+timely::progress::Timestamp+Ord+Clone+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    type Batcher = MergeBatcher<K, (), T, R, Self>;
    type Builder = OrdKeyBuilder<K, T, R, O>;
    type Merger = OrdKeyMerger<K, T, R, O>;

    fn begin_merge(&self, other: &Self, compaction_frontier: Option<AntichainRef<T>>) -> Self::Merger {
        OrdKeyMerger::new(self, other, compaction_frontier)
    }
}

impl<K, T, R, O> OrdKeyBatch<K, T, R, O>
where
    K: Ord+Clone+'static,
    T: Lattice+Ord+Clone+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    fn advance_builder_from(layer: &mut OrderedBuilder<K, OrderedLeafBuilder<T, R>, O>, frontier: AntichainRef<T>, key_pos: usize) {

        let key_start = key_pos;
        let time_start: usize = layer.offs[key_pos].try_into().unwrap();

        // We will zip through the time leaves, calling advance on each,
        //    then zip through the value layer, sorting and collapsing each,
        //    then zip through the key layer, collapsing each .. ?

        // 1. For each (time, diff) pair, advance the time.
        for i in time_start .. layer.vals.vals.len() {
            layer.vals.vals[i].0.advance_by(frontier);
        }
        // for time_diff in self.layer.vals.vals.iter_mut() {
        //     time_diff.0 = time_diff.0.advance_by(frontier);
        // }

        // 2. For each `(val, off)` pair, sort the range, compact, and rewrite `off`.
        //    This may leave `val` with an empty range; filtering happens in step 3.
        let mut write_position = time_start;
        for i in key_start .. layer.keys.len() {

            // NB: batch.layer.vals.offs[i+1] will be used next iteration, and should not be changed.
            //     we will change batch.layer.vals.offs[i] in this iteration, from `write_position`'s
            //     initial value.

            let lower: usize = layer.offs[i].try_into().unwrap();
            let upper: usize = layer.offs[i+1].try_into().unwrap();

            layer.offs[i] = O::try_from(write_position).unwrap();

            let updates = &mut layer.vals.vals[..];

            // sort the range by the times (ignore the diffs; they will collapse).
             let count = crate::consolidation::consolidate_slice(&mut updates[lower .. upper]);

            for index in lower .. (lower + count) {
                updates.swap(write_position, index);
                write_position += 1;
            }
        }
        layer.vals.vals.truncate(write_position);
        layer.offs[layer.keys.len()] = O::try_from(write_position).unwrap();

        // 4. Remove empty keys.
        let mut write_position = key_start;
        for i in key_start .. layer.keys.len() {

            let lower: usize = layer.offs[i].try_into().unwrap();
            let upper: usize = layer.offs[i+1].try_into().unwrap();

            if lower < upper {
                layer.keys.swap(write_position, i);
                // batch.layer.offs updated via `dedup` below; keeps me sane.
                write_position += 1;
            }
        }
        layer.offs.dedup();
        layer.keys.truncate(write_position);
        layer.offs.truncate(write_position+1);
    }
}

/// State for an in-progress merge.
pub struct OrdKeyMerger<K, T, R, O=usize>
where
    K: Ord+Clone+'static,
    T: Lattice+Ord+Clone+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    // first batch, and position therein.
    lower1: usize,
    upper1: usize,
    // second batch, and position therein.
    lower2: usize,
    upper2: usize,
    // result that we are currently assembling.
    result: <OrderedLayer<K, OrderedLeaf<T, R>, O> as Trie>::MergeBuilder,
    description: Description<T>,
    should_compact: bool,
}

impl<K, T, R, O> Merger<K, (), T, R, OrdKeyBatch<K, T, R, O>> for OrdKeyMerger<K, T, R, O>
where
    K: Ord+Clone+'static,
    T: Lattice+timely::progress::Timestamp+Ord+Clone+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    fn new(batch1: &OrdKeyBatch<K, T, R, O>, batch2: &OrdKeyBatch<K, T, R, O>, compaction_frontier: Option<AntichainRef<T>>) -> Self {

        assert!(batch1.upper() == batch2.lower());

        let mut since = batch1.description().since().join(batch2.description().since());
        if let Some(compaction_frontier) = compaction_frontier {
            since = since.join(&compaction_frontier.to_owned());
        }

        let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);

        OrdKeyMerger {
            lower1: 0,
            upper1: batch1.layer.keys(),
            lower2: 0,
            upper2: batch2.layer.keys(),
            result: <<OrderedLayer<K, OrderedLeaf<T, R>, O> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
            description: description,
            should_compact: compaction_frontier.is_some(),
        }
    }
    fn done(self) -> OrdKeyBatch<K, T, R, O> {

        assert!(self.lower1 == self.upper1);
        assert!(self.lower2 == self.upper2);

        OrdKeyBatch {
            layer: self.result.done(),
            desc: self.description,
        }
    }
    fn work(&mut self, source1: &OrdKeyBatch<K,T,R,O>, source2: &OrdKeyBatch<K,T,R,O>, fuel: &mut isize) {

        let starting_updates = self.result.vals.vals.len();
        let mut effort = 0isize;

        let initial_key_pos = self.result.keys.len();

        // while both mergees are still active
        while self.lower1 < self.upper1 && self.lower2 < self.upper2 && effort < *fuel {
            self.result.merge_step((&source1.layer, &mut self.lower1, self.upper1), (&source2.layer, &mut self.lower2, self.upper2));
            effort = (self.result.vals.vals.len() - starting_updates) as isize;
        }

        // if self.lower1 == self.upper1 || self.lower2 == self.upper2 {
        //     // these are just copies, so let's bite the bullet and just do them.
        //     if self.lower1 < self.upper1 { self.result.copy_range(&source1.layer, self.lower1, self.upper1); self.lower1 = self.upper1; }
        //     if self.lower2 < self.upper2 { self.result.copy_range(&source2.layer, self.lower2, self.upper2); self.lower2 = self.upper2; }
        // }
        // Merging is complete; only copying remains. Copying is probably faster than merging, so could take some liberties here.
        if self.lower1 == self.upper1 || self.lower2 == self.upper2 {
            // Limit merging by remaining fuel.
            let remaining_fuel = *fuel - effort;
            if remaining_fuel > 0 {
                if self.lower1 < self.upper1 {
                    let mut to_copy = remaining_fuel as usize;
                    if to_copy < 1_000 { to_copy = 1_000; }
                    if to_copy > (self.upper1 - self.lower1) { to_copy = self.upper1 - self.lower1; }
                    self.result.copy_range(&source1.layer, self.lower1, self.lower1 + to_copy);
                    self.lower1 += to_copy;
                }
                if self.lower2 < self.upper2 {
                    let mut to_copy = remaining_fuel as usize;
                    if to_copy < 1_000 { to_copy = 1_000; }
                    if to_copy > (self.upper2 - self.lower2) { to_copy = self.upper2 - self.lower2; }
                    self.result.copy_range(&source2.layer, self.lower2, self.lower2 + to_copy);
                    self.lower2 += to_copy;
                }
            }
        }


        effort = (self.result.vals.vals.len() - starting_updates) as isize;

        // if we are supplied a frontier, we should compact.
        if self.should_compact {
            OrdKeyBatch::advance_builder_from(&mut self.result, self.description.since().borrow(), initial_key_pos);
        }

        *fuel -= effort;

        // if *fuel < -1_000_000 {
        //     eprintln!("Massive deficit OrdKey::work: {}", fuel);
        // }
    }
}


/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdKeyCursor<T: Lattice+Ord+Clone, R: Semigroup, O=usize> {
    valid: bool,
    empty: (),
    cursor: OrderedCursor<OrderedLeaf<T, R>>,
    phantom: PhantomData<O>
}

impl<K, T, R, O> Cursor<K, (), T, R> for OrdKeyCursor<T, R, O>
where
    K: Ord+Clone,
    T: Lattice+Ord+Clone,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{

    type Storage = OrdKeyBatch<K, T, R, O>;

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a K { &self.cursor.key(&storage.layer) }
    fn val<'a>(&self, _storage: &'a Self::Storage) -> &'a () { unsafe { ::std::mem::transmute(&self.empty) } }
    fn map_times<L: FnMut(&T, &R)>(&mut self, storage: &Self::Storage, mut logic: L) {
        self.cursor.child.rewind(&storage.layer.vals);
        while self.cursor.child.valid(&storage.layer.vals) {
            logic(&self.cursor.child.key(&storage.layer.vals).0, &self.cursor.child.key(&storage.layer.vals).1);
            self.cursor.child.step(&storage.layer.vals);
        }
    }
    fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.valid(&storage.layer) }
    fn val_valid(&self, _storage: &Self::Storage) -> bool { self.valid }
    fn step_key(&mut self, storage: &Self::Storage){ self.cursor.step(&storage.layer); self.valid = true; }
    fn seek_key(&mut self, storage: &Self::Storage, key: &K) { self.cursor.seek(&storage.layer, key); self.valid = true; }
    fn step_val(&mut self, _storage: &Self::Storage) { self.valid = false; }
    fn seek_val(&mut self, _storage: &Self::Storage, _val: &()) { }
    fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind(&storage.layer); self.valid = true; }
    fn rewind_vals(&mut self, _storage: &Self::Storage) { self.valid = true; }
}


/// A builder for creating layers from unsorted update tuples.
pub struct OrdKeyBuilder<K, T, R, O=usize>
where
    K: Ord,
    T: Ord+Lattice,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{
    builder: OrderedBuilder<K, OrderedLeafBuilder<T, R>, O>,
}

impl<K, T, R, O> Builder<K, (), T, R, OrdKeyBatch<K, T, R, O>> for OrdKeyBuilder<K, T, R, O>
where
    K: Ord+Clone+'static,
    T: Lattice+timely::progress::Timestamp+Ord+Clone+'static,
    R: Semigroup,
    O: OrdOffset, <O as TryFrom<usize>>::Error: Debug, <O as TryInto<usize>>::Error: Debug
{

    fn new() -> Self {
        OrdKeyBuilder {
            builder: OrderedBuilder::<K, OrderedLeafBuilder<T, R>, O>::new()
        }
    }

    fn with_capacity(cap: usize) -> Self {
        OrdKeyBuilder {
            builder: <OrderedBuilder<K, OrderedLeafBuilder<T, R>, O> as TupleBuilder>::with_capacity(cap)
        }
    }

    #[inline]
    fn push(&mut self, (key, _, time, diff): (K, (), T, R)) {
        self.builder.push_tuple((key, (time, diff)));
    }

    #[inline(never)]
    fn done(self, lower: Antichain<T>, upper: Antichain<T>, since: Antichain<T>) -> OrdKeyBatch<K, T, R, O> {
        OrdKeyBatch {
            layer: self.builder.done(),
            desc: Description::new(lower, upper, since)
        }
    }
}
