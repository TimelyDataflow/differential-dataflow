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

use timely::container::columnation::TimelyStack;
use timely::container::columnation::Columnation;
use timely::progress::{Antichain, frontier::AntichainRef};

use lattice::Lattice;

use trace::layers::{Trie, TupleBuilder, BatchContainer};
use trace::layers::Builder as TrieBuilder;
use trace::layers::Cursor as TrieCursor;
use trace::layers::ordered::{OrderedLayer, OrderedBuilder, OrderedCursor};
use trace::layers::ordered_leaf::{OrderedLeaf, OrderedLeafBuilder};
use trace::{Batch, BatchReader, Builder, Merger, Cursor};
use trace::description::Description;

use trace::layers::MergeBuilder;

// use super::spine::Spine;
use super::spine_fueled::Spine;
use super::merge_batcher::MergeBatcher;

use abomonation::abomonated::Abomonated;
use trace::implementations::merge_batcher_col::ColumnatedMergeBatcher;
use trace::implementations::RetainFrom;

use super::{Update, Layout, Vector, TStack};

/// A trace implementation using a spine of ordered lists.
pub type OrdValSpine<K, V, T, R, O=usize> = Spine<Rc<OrdValBatch<Vector<((K,V),T,R), O>, Vec<((K,V),T,R)>>>>;

/// A trace implementation using a spine of abomonated ordered lists.
pub type OrdValSpineAbom<K, V, T, R, O=usize> = Spine<Rc<Abomonated<OrdValBatch<Vector<((K,V),T,R), O>, Vec<((K,V),T,R)>>, Vec<u8>>>>;

/// A trace implementation for empty values using a spine of ordered lists.
pub type OrdKeySpine<K, T, R, O=usize> = Spine<Rc<OrdKeyBatch<Vector<((K,()),T,R), O>, Vec<((K,()),T,R)>>>>;

/// A trace implementation for empty values using a spine of abomonated ordered lists.
pub type OrdKeySpineAbom<K, T, R, O=usize> = Spine<Rc<Abomonated<OrdKeyBatch<Vector<((K,()),T,R), O>, Vec<((K,()),T,R)>>, Vec<u8>>>>;

/// A trace implementation backed by columnar storage.
pub type ColValSpine<K, V, T, R, O=usize> = Spine<Rc<OrdValBatch<TStack<((K,V),T,R), O>, TimelyStack<((K,V),T,R)>>>>;
/// A trace implementation backed by columnar storage.
pub type ColKeySpine<K, T, R, O=usize> = Spine<Rc<OrdKeyBatch<TStack<((K,()),T,R), O>, TimelyStack<((K,()),T,R)>>>>;

/// An immutable collection of update tuples, from a contiguous interval of logical times.
///
/// The `L` parameter captures the updates should be laid out, and `C` determines which
/// merge batcher to select.
#[derive(Abomonation)]
pub struct OrdValBatch<L: Layout, C> {
    /// Where all the dataz is.
    pub layer: KVTDLayer<L>,
    /// Description of the update times this layer represents.
    pub desc: Description<<L::Target as Update>::Time>,
    /// Phantom data
    pub phantom: PhantomData<C>,
}

// Type aliases to make certain types readable.
type TDLayer<L> = OrderedLeaf<<<L as Layout>::Target as Update>::Time, <<L as Layout>::Target as Update>::Diff>;
type VTDLayer<L> = OrderedLayer<<<L as Layout>::Target as Update>::Val, TDLayer<L>, <L as Layout>::ValOffset, <L as Layout>::ValContainer>;
type KTDLayer<L> = OrderedLayer<<<L as Layout>::Target as Update>::Key, TDLayer<L>, <L as Layout>::KeyOffset, <L as Layout>::KeyContainer>;
type KVTDLayer<L> = OrderedLayer<<<L as Layout>::Target as Update>::Key, VTDLayer<L>, <L as Layout>::KeyOffset, <L as Layout>::KeyContainer>;
type TDBuilder<L> = OrderedLeafBuilder<<<L as Layout>::Target as Update>::Time, <<L as Layout>::Target as Update>::Diff>;
type VTDBuilder<L> = OrderedBuilder<<<L as Layout>::Target as Update>::Val, TDBuilder<L>, <L as Layout>::ValOffset, <L as Layout>::ValContainer>;
type KTDBuilder<L> = OrderedBuilder<<<L as Layout>::Target as Update>::Key, TDBuilder<L>, <L as Layout>::KeyOffset, <L as Layout>::KeyContainer>;
type KVTDBuilder<L> = OrderedBuilder<<<L as Layout>::Target as Update>::Key, VTDBuilder<L>, <L as Layout>::KeyOffset, <L as Layout>::KeyContainer>;

impl<L: Layout, C> BatchReader for OrdValBatch<L, C> {
    type Key = <L::Target as Update>::Key;
    type Val = <L::Target as Update>::Val;
    type Time = <L::Target as Update>::Time;
    type R = <L::Target as Update>::Diff;

    type Cursor = OrdValCursor<L, C>;
    fn cursor(&self) -> Self::Cursor { OrdValCursor { cursor: self.layer.cursor(), phantom: std::marker::PhantomData } }
    fn len(&self) -> usize { <KVTDLayer<L> as Trie>::tuples(&self.layer) }
    fn description(&self) -> &Description<<L::Target as Update>::Time> { &self.desc }
}

impl<L: Layout> Batch for OrdValBatch<L, Vec<L::Target>>
{
    type Batcher = MergeBatcher<Self>;
    type Builder = OrdValBuilder<L>;
    type Merger = OrdValMerger<L>;

    fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self::Merger {
        OrdValMerger::new(self, other, compaction_frontier)
    }
}

impl<L: Layout> Batch for OrdValBatch<L, TimelyStack<L::Target>>
where
    <L as Layout>::Target: Columnation,
    Self::Key: Columnation + 'static,
    Self::Val: Columnation + 'static,
    Self::Time: Columnation + 'static,
    Self::R: Columnation + 'static,
{
    type Batcher = ColumnatedMergeBatcher<Self>;
    type Builder = OrdValBuilder<L>;
    type Merger = OrdValMerger<L>;

    fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self::Merger {
        OrdValMerger::new(self, other, compaction_frontier)
    }
}

impl<L: Layout, C> OrdValBatch<L, C> {
    fn advance_builder_from(layer: &mut KVTDBuilder<L>, frontier: AntichainRef<<L::Target as Update>::Time>, key_pos: usize) {

        let key_start = key_pos;
        let val_start: usize = layer.offs[key_pos].try_into().ok().unwrap();
        let time_start: usize = layer.vals.offs[val_start].try_into().ok().unwrap();

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

            let lower: usize = layer.vals.offs[i].try_into().ok().unwrap();
            let upper: usize = layer.vals.offs[i+1].try_into().ok().unwrap();

            layer.vals.offs[i] = L::ValOffset::try_from(write_position).ok().unwrap();

            let updates = &mut layer.vals.vals.vals[..];

            // sort the range by the times (ignore the diffs; they will collapse).
            let count = crate::consolidation::consolidate_slice(&mut updates[lower .. upper]);

            for index in lower .. (lower + count) {
                updates.swap(write_position, index);
                write_position += 1;
            }
        }
        layer.vals.vals.vals.truncate(write_position);
        layer.vals.offs[layer.vals.keys.len()] = L::ValOffset::try_from(write_position).ok().unwrap();

        // 3. Remove values with empty histories. In addition, we need to update offsets
        //    in `layer.offs` to correctly reference the potentially moved values.
        let mut write_position = val_start;
        let vals_off = &mut layer.vals.offs;
        let mut keys_pos = key_start;
        let keys_off = &mut layer.offs;
        layer.vals.keys.retain_from(val_start, |index, _item| {
            // As we pass each key offset, record its new position.
            if index == keys_off[keys_pos].try_into().ok().unwrap() {
                keys_off[keys_pos] = L::KeyOffset::try_from(write_position).ok().unwrap();
                keys_pos += 1;
            }
            let lower = vals_off[index].try_into().ok().unwrap();
            let upper = vals_off[index+1].try_into().ok().unwrap();
            if lower < upper {
                vals_off[write_position+1] = vals_off[index+1];
                write_position += 1;
                true
            }
            else { false }
        });
        debug_assert_eq!(write_position, layer.vals.keys.len());
        layer.vals.offs.truncate(write_position + 1);
        layer.offs[layer.keys.len()] = L::KeyOffset::try_from(write_position).ok().unwrap();

        // 4. Remove empty keys.
        let offs = &mut layer.offs;
        let mut write_position = key_start;
        layer.keys.retain_from(key_start, |index, _item| {
            let lower = offs[index].try_into().ok().unwrap();
            let upper = offs[index+1].try_into().ok().unwrap();
            if lower < upper {
                offs[write_position+1] = offs[index+1];
                write_position += 1;
                true
            }
            else { false }
        });
        debug_assert_eq!(write_position, layer.keys.len());
        layer.offs.truncate(layer.keys.len()+1);
    }
}

/// State for an in-progress merge.
pub struct OrdValMerger<L: Layout> {
    // first batch, and position therein.
    lower1: usize,
    upper1: usize,
    // second batch, and position therein.
    lower2: usize,
    upper2: usize,
    // result that we are currently assembling.
    result: <KVTDLayer<L> as Trie>::MergeBuilder,
    description: Description<<L::Target as Update>::Time>,
}

impl<L: Layout, C> Merger<OrdValBatch<L, C>> for OrdValMerger<L>
where
    OrdValBatch<L, C>: Batch<Time=<L::Target as Update>::Time>
{
    fn new(batch1: &OrdValBatch<L, C>, batch2: &OrdValBatch<L, C>, compaction_frontier: AntichainRef<<OrdValBatch<L, C> as BatchReader>::Time>) -> Self {

        assert!(batch1.upper() == batch2.lower());

        let mut since = batch1.description().since().join(batch2.description().since());
        since = since.join(&compaction_frontier.to_owned());

        let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);

        OrdValMerger {
            lower1: 0,
            upper1: batch1.layer.keys(),
            lower2: 0,
            upper2: batch2.layer.keys(),
            result: <<KVTDLayer<L> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
            description: description,
        }
    }
    fn done(self) -> OrdValBatch<L, C> {

        assert!(self.lower1 == self.upper1);
        assert!(self.lower2 == self.upper2);

        OrdValBatch {
            layer: self.result.done(),
            desc: self.description,
            phantom: PhantomData,
        }
    }
    fn work(&mut self, source1: &OrdValBatch<L, C>, source2: &OrdValBatch<L, C>, fuel: &mut isize) {

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
        OrdValBatch::<L, C>::advance_builder_from(&mut self.result, self.description.since().borrow(), initial_key_pos);

        *fuel -= effort;

        // if *fuel < -1_000_000 {
        //     eprintln!("Massive deficit OrdVal::work: {}", fuel);
        // }
    }
}

/// A cursor for navigating a single layer.
pub struct OrdValCursor<L: Layout, C> {
    phantom: std::marker::PhantomData<(L, C)>,
    cursor: OrderedCursor<VTDLayer<L>>,
}

impl<L: Layout, C> Cursor for OrdValCursor<L, C> {
    type Key = <L::Target as Update>::Key;
    type Val = <L::Target as Update>::Val;
    type Time = <L::Target as Update>::Time;
    type R = <L::Target as Update>::Diff;

    type Storage = OrdValBatch<L, C>;

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Key { &self.cursor.key(&storage.layer) }
    fn val<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Val { &self.cursor.child.key(&storage.layer.vals) }
    fn map_times<L2: FnMut(&Self::Time, &Self::R)>(&mut self, storage: &Self::Storage, mut logic: L2) {
        self.cursor.child.child.rewind(&storage.layer.vals.vals);
        while self.cursor.child.child.valid(&storage.layer.vals.vals) {
            logic(&self.cursor.child.child.key(&storage.layer.vals.vals).0, &self.cursor.child.child.key(&storage.layer.vals.vals).1);
            self.cursor.child.child.step(&storage.layer.vals.vals);
        }
    }
    fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.valid(&storage.layer) }
    fn val_valid(&self, storage: &Self::Storage) -> bool { self.cursor.child.valid(&storage.layer.vals) }
    fn step_key(&mut self, storage: &Self::Storage){ self.cursor.step(&storage.layer); }
    fn seek_key(&mut self, storage: &Self::Storage, key: &Self::Key) { self.cursor.seek(&storage.layer, key); }
    fn step_val(&mut self, storage: &Self::Storage) { self.cursor.child.step(&storage.layer.vals); }
    fn seek_val(&mut self, storage: &Self::Storage, val: &Self::Val) { self.cursor.child.seek(&storage.layer.vals, val); }
    fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind(&storage.layer); }
    fn rewind_vals(&mut self, storage: &Self::Storage) { self.cursor.child.rewind(&storage.layer.vals); }
}

/// A builder for creating layers from unsorted update tuples.
pub struct OrdValBuilder<L: Layout> {
    builder: KVTDBuilder<L>,
}


impl<L: Layout, C> Builder<OrdValBatch<L, C>> for OrdValBuilder<L>
where
    OrdValBatch<L, C>: Batch<Key=<L::Target as Update>::Key, Val=<L::Target as Update>::Val, Time=<L::Target as Update>::Time, R=<L::Target as Update>::Diff>
{

    fn new() -> Self {
        OrdValBuilder {
            builder: <KVTDBuilder<L>>::new()
        }
    }
    fn with_capacity(cap: usize) -> Self {
        OrdValBuilder {
            builder: <KVTDBuilder<L> as TupleBuilder>::with_capacity(cap)
        }
    }

    #[inline]
    fn push(&mut self, (key, val, time, diff): (<OrdValBatch<L, C> as BatchReader>::Key, <OrdValBatch<L, C> as BatchReader>::Val, <OrdValBatch<L, C> as BatchReader>::Time, <OrdValBatch<L, C> as BatchReader>::R)) {
        self.builder.push_tuple((key, (val, (time, diff))));
    }

    fn copy(&mut self, (key, val, time, diff): (&<OrdValBatch<L, C> as BatchReader>::Key, &<OrdValBatch<L, C> as BatchReader>::Val, &<OrdValBatch<L, C> as BatchReader>::Time, &<OrdValBatch<L, C> as BatchReader>::R)) {
        self.builder.push_tuple((key.clone(), (val.clone(), (time.clone(), diff.clone()))));
    }

    #[inline(never)]
    fn done(self, lower: Antichain<<OrdValBatch<L, C> as BatchReader>::Time>, upper: Antichain<<OrdValBatch<L, C> as BatchReader>::Time>, since: Antichain<<OrdValBatch<L, C> as BatchReader>::Time>) -> OrdValBatch<L, C> {
        OrdValBatch {
            layer: self.builder.done(),
            desc: Description::new(lower, upper, since),
            phantom: PhantomData,
        }
    }
}




/// An immutable collection of update tuples, from a contiguous interval of logical times.
#[derive(Abomonation)]
pub struct OrdKeyBatch<L: Layout, C> {
    /// Where all the dataz is.
    pub layer: KTDLayer<L>,
    /// Description of the update times this layer represents.
    pub desc: Description<<L::Target as Update>::Time>,
    /// Phantom data
    pub phantom: PhantomData<C>,
}

impl<L: Layout, C> BatchReader for OrdKeyBatch<L, C> {
    type Key = <L::Target as Update>::Key;
    type Val = ();
    type Time = <L::Target as Update>::Time;
    type R = <L::Target as Update>::Diff;

    type Cursor = OrdKeyCursor<L, C>;
    fn cursor(&self) -> Self::Cursor {
        OrdKeyCursor {
            valid: true,
            cursor: self.layer.cursor(),
            phantom: PhantomData
        }
    }
    fn len(&self) -> usize { <KTDLayer<L> as Trie>::tuples(&self.layer) }
    fn description(&self) -> &Description<<L::Target as Update>::Time> { &self.desc }
}

impl<L: Layout> Batch for OrdKeyBatch<L, Vec<L::Target>> {
    type Batcher = MergeBatcher<Self>;
    type Builder = OrdKeyBuilder<L>;
    type Merger = OrdKeyMerger<L>;

    fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self::Merger {
        OrdKeyMerger::new(self, other, compaction_frontier)
    }
}

impl<L: Layout> Batch for OrdKeyBatch<L, TimelyStack<L::Target>>
where
    <L as Layout>::Target: Columnation + 'static,
    Self::Key: Columnation + 'static,
    Self::Time: Columnation + 'static,
    Self::R: Columnation + 'static,
{
    type Batcher = ColumnatedMergeBatcher<Self>;
    type Builder = OrdKeyBuilder<L>;
    type Merger = OrdKeyMerger<L>;

    fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self::Merger {
        OrdKeyMerger::new(self, other, compaction_frontier)
    }
}

impl<L: Layout, C> OrdKeyBatch<L, C> {
    fn advance_builder_from(layer: &mut KTDBuilder<L>, frontier: AntichainRef<<L::Target as Update>::Time>, key_pos: usize) {

        let key_start = key_pos;
        let time_start: usize = layer.offs[key_pos].try_into().ok().unwrap();

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

            let lower: usize = layer.offs[i].try_into().ok().unwrap();
            let upper: usize = layer.offs[i+1].try_into().ok().unwrap();

            layer.offs[i] = L::KeyOffset::try_from(write_position).ok().unwrap();

            let updates = &mut layer.vals.vals[..];

            // sort the range by the times (ignore the diffs; they will collapse).
             let count = crate::consolidation::consolidate_slice(&mut updates[lower .. upper]);

            for index in lower .. (lower + count) {
                updates.swap(write_position, index);
                write_position += 1;
            }
        }
        layer.vals.vals.truncate(write_position);
        layer.offs[layer.keys.len()] = L::KeyOffset::try_from(write_position).ok().unwrap();

        // 4. Remove empty keys.
        let offs = &mut layer.offs;
        let mut write_position = key_start;
        layer.keys.retain_from(key_start, |index, _item| {
            let lower = offs[index].try_into().ok().unwrap();
            let upper = offs[index+1].try_into().ok().unwrap();
            if lower < upper {
                offs[write_position+1] = offs[index+1];
                write_position += 1;
                true
            }
            else { false }
        });
        debug_assert_eq!(write_position, layer.keys.len());
        layer.offs.truncate(layer.keys.len()+1);
    }
}

/// State for an in-progress merge.
pub struct OrdKeyMerger<L: Layout> {
    // first batch, and position therein.
    lower1: usize,
    upper1: usize,
    // second batch, and position therein.
    lower2: usize,
    upper2: usize,
    // result that we are currently assembling.
    result: <KTDLayer<L> as Trie>::MergeBuilder,
    description: Description<<L::Target as Update>::Time>,
}

impl<L: Layout, C> Merger<OrdKeyBatch<L, C>> for OrdKeyMerger<L>
where
    OrdKeyBatch<L, C>: Batch<Time=<L::Target as Update>::Time>
{
    fn new(batch1: &OrdKeyBatch<L, C>, batch2: &OrdKeyBatch<L, C>, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self {

        assert!(batch1.upper() == batch2.lower());

        let mut since = batch1.description().since().join(batch2.description().since());
        since = since.join(&compaction_frontier.to_owned());

        let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);

        OrdKeyMerger {
            lower1: 0,
            upper1: batch1.layer.keys(),
            lower2: 0,
            upper2: batch2.layer.keys(),
            result: <<KTDLayer<L> as Trie>::MergeBuilder as MergeBuilder>::with_capacity(&batch1.layer, &batch2.layer),
            description: description,
        }
    }
    fn done(self) -> OrdKeyBatch<L, C> {

        assert!(self.lower1 == self.upper1);
        assert!(self.lower2 == self.upper2);

        OrdKeyBatch {
            layer: self.result.done(),
            desc: self.description,
            phantom: PhantomData,
        }
    }
    fn work(&mut self, source1: &OrdKeyBatch<L, C>, source2: &OrdKeyBatch<L, C>, fuel: &mut isize) {

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
        OrdKeyBatch::<L, C>::advance_builder_from(&mut self.result, self.description.since().borrow(), initial_key_pos);

        *fuel -= effort;

        // if *fuel < -1_000_000 {
        //     eprintln!("Massive deficit OrdKey::work: {}", fuel);
        // }
    }
}


/// A cursor for navigating a single layer.
#[derive(Debug)]
pub struct OrdKeyCursor<L: Layout, C> {
    valid: bool,
    cursor: OrderedCursor<OrderedLeaf<<L::Target as Update>::Time, <L::Target as Update>::Diff>>,
    phantom: PhantomData<(L, C)>,
}

impl<L: Layout, C> Cursor for OrdKeyCursor<L, C> {
    type Key = <L::Target as Update>::Key;
    type Val = ();
    type Time = <L::Target as Update>::Time;
    type R = <L::Target as Update>::Diff;

    type Storage = OrdKeyBatch<L, C>;

    fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Key { &self.cursor.key(&storage.layer) }
    fn val<'a>(&self, _storage: &'a Self::Storage) -> &'a () { &() }
    fn map_times<L2: FnMut(&Self::Time, &Self::R)>(&mut self, storage: &Self::Storage, mut logic: L2) {
        self.cursor.child.rewind(&storage.layer.vals);
        while self.cursor.child.valid(&storage.layer.vals) {
            logic(&self.cursor.child.key(&storage.layer.vals).0, &self.cursor.child.key(&storage.layer.vals).1);
            self.cursor.child.step(&storage.layer.vals);
        }
    }
    fn key_valid(&self, storage: &Self::Storage) -> bool { self.cursor.valid(&storage.layer) }
    fn val_valid(&self, _storage: &Self::Storage) -> bool { self.valid }
    fn step_key(&mut self, storage: &Self::Storage){ self.cursor.step(&storage.layer); self.valid = true; }
    fn seek_key(&mut self, storage: &Self::Storage, key: &Self::Key) { self.cursor.seek(&storage.layer, key); self.valid = true; }
    fn step_val(&mut self, _storage: &Self::Storage) { self.valid = false; }
    fn seek_val(&mut self, _storage: &Self::Storage, _val: &()) { }
    fn rewind_keys(&mut self, storage: &Self::Storage) { self.cursor.rewind(&storage.layer); self.valid = true; }
    fn rewind_vals(&mut self, _storage: &Self::Storage) { self.valid = true; }
}


/// A builder for creating layers from unsorted update tuples.
pub struct OrdKeyBuilder<L: Layout> {
    builder: KTDBuilder<L>,
}

impl<L: Layout, C> Builder<OrdKeyBatch<L, C>> for OrdKeyBuilder<L>
where
    OrdKeyBatch<L, C>: Batch<Key=<L::Target as Update>::Key, Val=(), Time=<L::Target as Update>::Time, R=<L::Target as Update>::Diff>
{

    fn new() -> Self {
        OrdKeyBuilder {
            builder: <KTDBuilder<L>>::new()
        }
    }

    fn with_capacity(cap: usize) -> Self {
        OrdKeyBuilder {
            builder: <KTDBuilder<L> as TupleBuilder>::with_capacity(cap)
        }
    }

    #[inline]
    fn push(&mut self, (key, _, time, diff): (<L::Target as Update>::Key, (), <L::Target as Update>::Time, <L::Target as Update>::Diff)) {
        self.builder.push_tuple((key, (time, diff)));
    }

    #[inline]
    fn copy(&mut self, (key, _, time, diff): (&<L::Target as Update>::Key, &(), &<L::Target as Update>::Time, &<L::Target as Update>::Diff)) {
        self.builder.push_tuple((key.clone(), (time.clone(), diff.clone())));
    }

    #[inline(never)]
    fn done(self, lower: Antichain<<L::Target as Update>::Time>, upper: Antichain<<L::Target as Update>::Time>, since: Antichain<<L::Target as Update>::Time>) -> OrdKeyBatch<L, C> {
        OrdKeyBatch {
            layer: self.builder.done(),
            desc: Description::new(lower, upper, since),
            phantom: PhantomData,
        }
    }
}
