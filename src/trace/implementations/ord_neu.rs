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
use timely::container::columnation::{TimelyStack};
use timely::container::flatcontainer::{Containerized, FlatStack};
use timely::container::flatcontainer::impls::tuple::{TupleABCRegion, TupleABRegion};
use crate::trace::implementations::chunker::{ColumnationChunker, ContainerChunker, VecChunker};

use crate::trace::implementations::spine_fueled::Spine;
use crate::trace::implementations::merge_batcher::{MergeBatcher, VecMerger};
use crate::trace::implementations::merge_batcher_col::ColumnationMerger;
use crate::trace::implementations::merge_batcher_flat::{FlatcontainerMerger, MergerChunk};
use crate::trace::rc_blanket_impls::RcBuilder;

use super::{Update, Layout, Vector, TStack, Preferred, FlatLayout};

pub use self::val_batch::{OrdValBatch, OrdValBuilder};
pub use self::key_batch::{OrdKeyBatch, OrdKeyBuilder};

/// A trace implementation using a spine of ordered lists.
pub type OrdValSpine<K, V, T, R> = Spine<
    Rc<OrdValBatch<Vector<((K,V),T,R)>>>,
    MergeBatcher<Vec<((K,V),T,R)>, VecChunker<((K,V),T,R)>, VecMerger<((K, V), T, R)>, T>,
    RcBuilder<OrdValBuilder<Vector<((K,V),T,R)>, Vec<((K,V),T,R)>>>,
>;
// /// A trace implementation for empty values using a spine of ordered lists.
// pub type OrdKeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<Vector<((K,()),T,R)>>>>;

/// A trace implementation backed by columnar storage.
pub type ColValSpine<K, V, T, R> = Spine<
    Rc<OrdValBatch<TStack<((K,V),T,R)>>>,
    MergeBatcher<Vec<((K,V),T,R)>, ColumnationChunker<((K,V),T,R)>, ColumnationMerger<((K,V),T,R)>, T>,
    RcBuilder<OrdValBuilder<TStack<((K,V),T,R)>, TimelyStack<((K,V),T,R)>>>,
>;

/// A trace implementation backed by flatcontainer storage.
pub type FlatValSpine<L, R, C> = Spine<
    Rc<OrdValBatch<L>>,
    MergeBatcher<C, ContainerChunker<FlatStack<R>>, FlatcontainerMerger<R>, <R as MergerChunk>::TimeOwned>,
    RcBuilder<OrdValBuilder<L, FlatStack<R>>>,
>;

/// A trace implementation backed by flatcontainer storage, using [`FlatLayout`] as the layout.
pub type FlatValSpineDefault<K, V, T, R, C> = FlatValSpine<
    FlatLayout<<K as Containerized>::Region, <V as Containerized>::Region, <T as Containerized>::Region, <R as Containerized>::Region>,
    TupleABCRegion<TupleABRegion<<K as Containerized>::Region, <V as Containerized>::Region>, <T as Containerized>::Region, <R as Containerized>::Region>,
    C,
>;

/// A trace implementation using a spine of ordered lists.
pub type OrdKeySpine<K, T, R> = Spine<
    Rc<OrdKeyBatch<Vector<((K,()),T,R)>>>,
    MergeBatcher<Vec<((K,()),T,R)>, VecChunker<((K,()),T,R)>, VecMerger<((K, ()), T, R)>, T>,
    RcBuilder<OrdKeyBuilder<Vector<((K,()),T,R)>, Vec<((K,()),T,R)>>>,
>;
// /// A trace implementation for empty values using a spine of ordered lists.
// pub type OrdKeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<Vector<((K,()),T,R)>>>>;

/// A trace implementation backed by columnar storage.
pub type ColKeySpine<K, T, R> = Spine<
    Rc<OrdKeyBatch<TStack<((K,()),T,R)>>>,
    MergeBatcher<Vec<((K,()),T,R)>, ColumnationChunker<((K,()),T,R)>, ColumnationMerger<((K,()),T,R)>, T>,
    RcBuilder<OrdKeyBuilder<TStack<((K,()),T,R)>, TimelyStack<((K,()),T,R)>>>,
>;

/// A trace implementation backed by flatcontainer storage.
pub type FlatKeySpine<L, R, C> = Spine<
    Rc<OrdKeyBatch<L>>,
    MergeBatcher<C, ContainerChunker<FlatStack<R>>, FlatcontainerMerger<R>, <R as MergerChunk>::TimeOwned>,
    RcBuilder<OrdKeyBuilder<L, FlatStack<R>>>,
>;

/// A trace implementation backed by flatcontainer storage, using [`FlatLayout`] as the layout.
pub type FlatKeySpineDefault<K,T,R, C> = FlatKeySpine<
    FlatLayout<<K as Containerized>::Region, <() as Containerized>::Region, <T as Containerized>::Region, <R as Containerized>::Region>,
    TupleABCRegion<TupleABRegion<<K as Containerized>::Region, <() as Containerized>::Region>, <T as Containerized>::Region, <R as Containerized>::Region>,
    C,
>;

/// A trace implementation backed by columnar storage.
pub type PreferredSpine<K, V, T, R> = Spine<
    Rc<OrdValBatch<Preferred<K,V,T,R>>>,
    MergeBatcher<Vec<((<K as ToOwned>::Owned,<V as ToOwned>::Owned),T,R)>, ColumnationChunker<((<K as ToOwned>::Owned,<V as ToOwned>::Owned),T,R)>, ColumnationMerger<((<K as ToOwned>::Owned,<V as ToOwned>::Owned),T,R)>,T>,
    RcBuilder<OrdValBuilder<Preferred<K,V,T,R>, TimelyStack<((<K as ToOwned>::Owned,<V as ToOwned>::Owned),T,R)>>>,
>;


// /// A trace implementation backed by columnar storage.
// pub type ColKeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<TStack<((K,()),T,R)>>>>;


mod val_batch {

    use std::marker::PhantomData;
    use abomonation_derive::Abomonation;
    use timely::container::PushInto;
    use timely::progress::{Antichain, frontier::AntichainRef};

    use crate::trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
    use crate::trace::implementations::{BatchContainer, BuilderInput};
    use crate::trace::cursor::IntoOwned;

    use super::{Layout, Update};

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    #[derive(Abomonation, Debug)]
    pub struct OrdValStorage<L: Layout> {
        /// An ordered list of keys, corresponding to entries in `keys_offs`.
        pub keys: L::KeyContainer,
        /// Offsets used to provide indexes from keys to values.
        ///
        /// The length of this list is one longer than `keys`, so that we can avoid bounds logic.
        pub keys_offs: L::OffsetContainer,
        /// Concatenated ordered lists of values, bracketed by offsets in `keys_offs`.
        pub vals: L::ValContainer,
        /// Offsets used to provide indexes from values to updates.
        ///
        /// This list has a special representation that any empty range indicates the singleton
        /// element just before the range, as if the start were decremented by one. The empty
        /// range is otherwise an invalid representation, and we borrow it to compactly encode
        /// single common update values (e.g. in a snapshot, the minimal time and a diff of one).
        ///
        /// The length of this list is one longer than `vals`, so that we can avoid bounds logic.
        pub vals_offs: L::OffsetContainer,
        /// Concatenated ordered lists of update times, bracketed by offsets in `vals_offs`.
        pub times: L::TimeContainer,
        /// Concatenated ordered lists of update diffs, bracketed by offsets in `vals_offs`.
        pub diffs: L::DiffContainer,
    }

    impl<L: Layout> OrdValStorage<L> {
        /// Lower and upper bounds in `self.vals` corresponding to the key at `index`.
        fn values_for_key(&self, index: usize) -> (usize, usize) {
            (self.keys_offs.index(index), self.keys_offs.index(index+1))
        }
        /// Lower and upper bounds in `self.updates` corresponding to the value at `index`.
        fn updates_for_value(&self, index: usize) -> (usize, usize) {
            let mut lower = self.vals_offs.index(index);
            let upper = self.vals_offs.index(index+1);
            // We use equal lower and upper to encode "singleton update; just before here".
            // It should only apply when there is a prior element, so `lower` should be greater than zero.
            if lower == upper {
                assert!(lower > 0);
                lower -= 1;
            }
            (lower, upper)
        }
    }

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    ///
    /// The `L` parameter captures how the updates should be laid out, and `C` determines which
    /// merge batcher to select.
    #[derive(Abomonation)]
    pub struct OrdValBatch<L: Layout> {
        /// The updates themselves.
        pub storage: OrdValStorage<L>,
        /// Description of the update times this layer represents.
        pub description: Description<<L::Target as Update>::Time>,
        /// The number of updates reflected in the batch.
        ///
        /// We track this separately from `storage` because due to the singleton optimization,
        /// we may have many more updates than `storage.updates.len()`. It should equal that 
        /// length, plus the number of singleton optimizations employed.
        pub updates: usize,
    }

    impl<L: Layout> BatchReader for OrdValBatch<L> {
        type Key<'a> = <L::KeyContainer as BatchContainer>::ReadItem<'a>;
        type Val<'a> = <L::ValContainer as BatchContainer>::ReadItem<'a>;
        type Time = <L::Target as Update>::Time;
        type TimeGat<'a> = <L::TimeContainer as BatchContainer>::ReadItem<'a>;
        type Diff = <L::Target as Update>::Diff;
        type DiffGat<'a> = <L::DiffContainer as BatchContainer>::ReadItem<'a>;

        type Cursor = OrdValCursor<L>;
        fn cursor(&self) -> Self::Cursor { 
            OrdValCursor {
                key_cursor: 0,
                val_cursor: 0,
                phantom: PhantomData,
            }
        }
        fn len(&self) -> usize { 
            // Normally this would be `self.updates.len()`, but we have a clever compact encoding.
            // Perhaps we should count such exceptions to the side, to provide a correct accounting.
            self.updates
        }
        fn description(&self) -> &Description<<L::Target as Update>::Time> { &self.description }
    }

    impl<L: Layout> Batch for OrdValBatch<L> {
        type Merger = OrdValMerger<L>;

        fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self::Merger {
            OrdValMerger::new(self, other, compaction_frontier)
        }
    }

    /// State for an in-progress merge.
    pub struct OrdValMerger<L: Layout> {
        /// Key position to merge next in the first batch.
        key_cursor1: usize,
        /// Key position to merge next in the second batch.
        key_cursor2: usize,
        /// result that we are currently assembling.
        result: OrdValStorage<L>,
        /// description
        description: Description<<L::Target as Update>::Time>,

        /// Local stash of updates, to use for consolidation.
        ///
        /// We could emulate a `ChangeBatch` here, with related compaction smarts.
        /// A `ChangeBatch` itself needs an `i64` diff type, which we have not.
        update_stash: Vec<(<L::Target as Update>::Time, <L::Target as Update>::Diff)>,
        /// Counts the number of singleton-optimized entries, that we may correctly count the updates.
        singletons: usize,
    }

    impl<L: Layout> Merger<OrdValBatch<L>> for OrdValMerger<L>
    where
        OrdValBatch<L>: Batch<Time=<L::Target as Update>::Time>,
        for<'a> <L::TimeContainer as BatchContainer>::ReadItem<'a> : IntoOwned<'a, Owned = <L::Target as Update>::Time>,
        for<'a> <L::DiffContainer as BatchContainer>::ReadItem<'a> : IntoOwned<'a, Owned = <L::Target as Update>::Diff>,
    {
        fn new(batch1: &OrdValBatch<L>, batch2: &OrdValBatch<L>, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self {

            assert!(batch1.upper() == batch2.lower());
            use crate::lattice::Lattice;
            let mut since = batch1.description().since().join(batch2.description().since());
            since = since.join(&compaction_frontier.to_owned());

            let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);

            let batch1 = &batch1.storage;
            let batch2 = &batch2.storage;

            let mut storage = OrdValStorage {
                keys: L::KeyContainer::merge_capacity(&batch1.keys, &batch2.keys),
                keys_offs: L::OffsetContainer::with_capacity(batch1.keys_offs.len() + batch2.keys_offs.len()),
                vals: L::ValContainer::merge_capacity(&batch1.vals, &batch2.vals),
                vals_offs: L::OffsetContainer::with_capacity(batch1.vals_offs.len() + batch2.vals_offs.len()),
                times: L::TimeContainer::merge_capacity(&batch1.times, &batch2.times),
                diffs: L::DiffContainer::merge_capacity(&batch1.diffs, &batch2.diffs),
            };

            // Mark explicit types because type inference fails to resolve it.
            let keys_offs: &mut L::OffsetContainer = &mut storage.keys_offs;
            keys_offs.push(0);
            let vals_offs: &mut L::OffsetContainer = &mut storage.vals_offs;
            vals_offs.push(0);

            OrdValMerger {
                key_cursor1: 0,
                key_cursor2: 0,
                result: storage,
                description,
                update_stash: Vec::new(),
                singletons: 0,
            }
        }
        fn done(self) -> OrdValBatch<L> {
            OrdValBatch {
                updates: self.result.times.len() + self.singletons,
                storage: self.result,
                description: self.description,
            }
        }
        fn work(&mut self, source1: &OrdValBatch<L>, source2: &OrdValBatch<L>, fuel: &mut isize) {

            // An (incomplete) indication of the amount of work we've done so far.
            let starting_updates = self.result.times.len();
            let mut effort = 0isize;

            // While both mergees are still active, perform single-key merges.
            while self.key_cursor1 < source1.storage.keys.len() && self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.merge_key(&source1.storage, &source2.storage);
                // An (incomplete) accounting of the work we've done.
                effort = (self.result.times.len() - starting_updates) as isize;
            }

            // Merging is complete, and only copying remains. 
            // Key-by-key copying allows effort interruption, and compaction.
            while self.key_cursor1 < source1.storage.keys.len() && effort < *fuel {
                self.copy_key(&source1.storage, self.key_cursor1);
                self.key_cursor1 += 1;
                effort = (self.result.times.len() - starting_updates) as isize;
            }
            while self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.copy_key(&source2.storage, self.key_cursor2);
                self.key_cursor2 += 1;
                effort = (self.result.times.len() - starting_updates) as isize;
            }

            *fuel -= effort;
        }
    }

    // Helper methods in support of merging batches.
    impl<L: Layout> OrdValMerger<L> {
        /// Copy the next key in `source`.
        ///
        /// The method extracts the key in `source` at `cursor`, and merges it in to `self`.
        /// If the result does not wholly cancel, they key will be present in `self` with the
        /// compacted values and updates.
        ///
        /// The caller should be certain to update the cursor, as this method does not do this.
        fn copy_key(&mut self, source: &OrdValStorage<L>, cursor: usize) {
            // Capture the initial number of values to determine if the merge was ultimately non-empty.
            let init_vals = self.result.vals.len();
            let (mut lower, upper) = source.values_for_key(cursor);
            while lower < upper {
                self.stash_updates_for_val(source, lower);
                if let Some(off) = self.consolidate_updates() {
                    self.result.vals_offs.push(off);
                    self.result.vals.push(source.vals.index(lower));
                }
                lower += 1;
            }            

            // If we have pushed any values, copy the key as well.
            if self.result.vals.len() > init_vals {
                self.result.keys.push(source.keys.index(cursor));
                self.result.keys_offs.push(self.result.vals.len());
            }           
        }
        /// Merge the next key in each of `source1` and `source2` into `self`, updating the appropriate cursors.
        ///
        /// This method only merges a single key. It applies all compaction necessary, and may result in no output
        /// if the updates cancel either directly or after compaction.
        fn merge_key(&mut self, source1: &OrdValStorage<L>, source2: &OrdValStorage<L>) {
            use ::std::cmp::Ordering;
            match source1.keys.index(self.key_cursor1).cmp(&source2.keys.index(self.key_cursor2)) {
                Ordering::Less => { 
                    self.copy_key(source1, self.key_cursor1);
                    self.key_cursor1 += 1;
                },
                Ordering::Equal => {
                    // Keys are equal; must merge all values from both sources for this one key.
                    let (lower1, upper1) = source1.values_for_key(self.key_cursor1);
                    let (lower2, upper2) = source2.values_for_key(self.key_cursor2);
                    if let Some(off) = self.merge_vals((source1, lower1, upper1), (source2, lower2, upper2)) {
                        self.result.keys.push(source1.keys.index(self.key_cursor1));
                        self.result.keys_offs.push(off);
                    }
                    // Increment cursors in either case; the keys are merged.
                    self.key_cursor1 += 1;
                    self.key_cursor2 += 1;
                },
                Ordering::Greater => {
                    self.copy_key(source2, self.key_cursor2);
                    self.key_cursor2 += 1;
                },
            }
        }
        /// Merge two ranges of values into `self`.
        ///
        /// If the compacted result contains values with non-empty updates, the function returns
        /// an offset that should be recorded to indicate the upper extent of the result values.
        fn merge_vals(
            &mut self, 
            (source1, mut lower1, upper1): (&OrdValStorage<L>, usize, usize), 
            (source2, mut lower2, upper2): (&OrdValStorage<L>, usize, usize),
        ) -> Option<usize> {
            // Capture the initial number of values to determine if the merge was ultimately non-empty.
            let init_vals = self.result.vals.len();
            while lower1 < upper1 && lower2 < upper2 {
                // We compare values, and fold in updates for the lowest values;
                // if they are non-empty post-consolidation, we write the value.
                // We could multi-way merge and it wouldn't be very complicated.
                use ::std::cmp::Ordering;
                match source1.vals.index(lower1).cmp(&source2.vals.index(lower2)) {
                    Ordering::Less => { 
                        // Extend stash by updates, with logical compaction applied.
                        self.stash_updates_for_val(source1, lower1);
                        if let Some(off) = self.consolidate_updates() {
                            self.result.vals_offs.push(off);
                            self.result.vals.push(source1.vals.index(lower1));
                        }
                        lower1 += 1;
                    },
                    Ordering::Equal => {
                        self.stash_updates_for_val(source1, lower1);
                        self.stash_updates_for_val(source2, lower2);
                        if let Some(off) = self.consolidate_updates() {
                            self.result.vals_offs.push(off);
                            self.result.vals.push(source1.vals.index(lower1));
                        }
                        lower1 += 1;
                        lower2 += 1;
                    },
                    Ordering::Greater => { 
                        // Extend stash by updates, with logical compaction applied.
                        self.stash_updates_for_val(source2, lower2);
                        if let Some(off) = self.consolidate_updates() {
                            self.result.vals_offs.push(off);
                            self.result.vals.push(source2.vals.index(lower2));
                        }
                        lower2 += 1;
                    },
                }
            }
            // Merging is complete, but we may have remaining elements to push.
            while lower1 < upper1 {
                self.stash_updates_for_val(source1, lower1);
                if let Some(off) = self.consolidate_updates() {
                    self.result.vals_offs.push(off);
                    self.result.vals.push(source1.vals.index(lower1));
                }
                lower1 += 1;
            }
            while lower2 < upper2 {
                self.stash_updates_for_val(source2, lower2);
                if let Some(off) = self.consolidate_updates() {
                    self.result.vals_offs.push(off);
                    self.result.vals.push(source2.vals.index(lower2));
                }
                lower2 += 1;
            }

            // Values being pushed indicate non-emptiness.
            if self.result.vals.len() > init_vals {
                Some(self.result.vals.len())
            } else {
                None
            }
        }

        /// Transfer updates for an indexed value in `source` into `self`, with compaction applied.
        fn stash_updates_for_val(&mut self, source: &OrdValStorage<L>, index: usize) {
            let (lower, upper) = source.updates_for_value(index);
            for i in lower .. upper {
                // NB: Here is where we would need to look back if `lower == upper`.
                let time = source.times.index(i);
                let diff = source.diffs.index(i);
                use crate::lattice::Lattice;
                let mut new_time: <L::Target as Update>::Time = time.into_owned();
                new_time.advance_by(self.description.since().borrow());
                self.update_stash.push((new_time, diff.into_owned()));
            }
        }

        /// Consolidates `self.updates_stash` and produces the offset to record, if any.
        fn consolidate_updates(&mut self) -> Option<usize> {
            use crate::consolidation;
            consolidation::consolidate(&mut self.update_stash);
            if !self.update_stash.is_empty() {
                // If there is a single element, equal to a just-prior recorded update,
                // we push nothing and report an unincremented offset to encode this case.
                let time_diff = self.result.times.last().zip(self.result.diffs.last());
                let last_eq = self.update_stash.last().zip(time_diff).map(|((t1, d1), (t2, d2))| {
                    let t1 = <<L::TimeContainer as BatchContainer>::ReadItem<'_> as IntoOwned>::borrow_as(t1);
                    let d1 = <<L::DiffContainer as BatchContainer>::ReadItem<'_> as IntoOwned>::borrow_as(d1);
                    t1.eq(&t2) && d1.eq(&d2)
                });
                if self.update_stash.len() == 1 && last_eq.unwrap_or(false) {
                    // Just clear out update_stash, as we won't drain it here.
                    self.update_stash.clear();
                    self.singletons += 1;
                }
                else {
                    // Conventional; move `update_stash` into `updates`.
                    for (time, diff) in self.update_stash.drain(..) {
                        self.result.times.push(time);
                        self.result.diffs.push(diff);
                    }
                }
                Some(self.result.times.len())
            } else {
                None
            }
        }
    }

    /// A cursor for navigating a single layer.
    pub struct OrdValCursor<L: Layout> {
        /// Absolute position of the current key.
        key_cursor: usize,
        /// Absolute position of the current value.
        val_cursor: usize,
        /// Phantom marker for Rust happiness.
        phantom: PhantomData<L>,
    }

    impl<L: Layout> Cursor for OrdValCursor<L> {

        type Key<'a> = <L::KeyContainer as BatchContainer>::ReadItem<'a>;
        type Val<'a> = <L::ValContainer as BatchContainer>::ReadItem<'a>;
        type Time = <L::Target as Update>::Time;
        type TimeGat<'a> = <L::TimeContainer as BatchContainer>::ReadItem<'a>;
        type Diff = <L::Target as Update>::Diff;
        type DiffGat<'a> = <L::DiffContainer as BatchContainer>::ReadItem<'a>;

        type Storage = OrdValBatch<L>;

        fn key<'a>(&self, storage: &'a OrdValBatch<L>) -> Self::Key<'a> { storage.storage.keys.index(self.key_cursor) }
        fn val<'a>(&self, storage: &'a OrdValBatch<L>) -> Self::Val<'a> { storage.storage.vals.index(self.val_cursor) }
        fn map_times<L2: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &OrdValBatch<L>, mut logic: L2) {
            let (lower, upper) = storage.storage.updates_for_value(self.val_cursor);
            for index in lower .. upper {
                let time = storage.storage.times.index(index);
                let diff = storage.storage.diffs.index(index);
                logic(time, diff);
            }
        }
        fn key_valid(&self, storage: &OrdValBatch<L>) -> bool { self.key_cursor < storage.storage.keys.len() }
        fn val_valid(&self, storage: &OrdValBatch<L>) -> bool { self.val_cursor < storage.storage.values_for_key(self.key_cursor).1 }
        fn step_key(&mut self, storage: &OrdValBatch<L>){
            self.key_cursor += 1;
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
            else {
                self.key_cursor = storage.storage.keys.len();
            }
        }
        fn seek_key(&mut self, storage: &OrdValBatch<L>, key: Self::Key<'_>) {
            self.key_cursor += storage.storage.keys.advance(self.key_cursor, storage.storage.keys.len(), |x| <L::KeyContainer as BatchContainer>::reborrow(x).lt(&<L::KeyContainer as BatchContainer>::reborrow(key)));
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
        }
        fn step_val(&mut self, storage: &OrdValBatch<L>) {
            self.val_cursor += 1; 
            if !self.val_valid(storage) {
                self.val_cursor = storage.storage.values_for_key(self.key_cursor).1;
            }
        }
        fn seek_val(&mut self, storage: &OrdValBatch<L>, val: Self::Val<'_>) {
            self.val_cursor += storage.storage.vals.advance(self.val_cursor, storage.storage.values_for_key(self.key_cursor).1, |x| <L::ValContainer as BatchContainer>::reborrow(x).lt(&<L::ValContainer as BatchContainer>::reborrow(val)));
        }
        fn rewind_keys(&mut self, storage: &OrdValBatch<L>) {
            self.key_cursor = 0;
            if self.key_valid(storage) {
                self.rewind_vals(storage)
            }
        }
        fn rewind_vals(&mut self, storage: &OrdValBatch<L>) {
            self.val_cursor = storage.storage.values_for_key(self.key_cursor).0;
        }
    }

    /// A builder for creating layers from unsorted update tuples.
    pub struct OrdValBuilder<L: Layout, CI> {
        result: OrdValStorage<L>,
        singleton: Option<(<L::Target as Update>::Time, <L::Target as Update>::Diff)>,
        /// Counts the number of singleton optimizations we performed.
        ///
        /// This number allows us to correctly gauge the total number of updates reflected in a batch,
        /// even though `updates.len()` may be much shorter than this amount.
        singletons: usize,
        _marker: PhantomData<CI>,
    }

    impl<L: Layout, CI> OrdValBuilder<L, CI> {
        /// Pushes a single update, which may set `self.singleton` rather than push.
        ///
        /// This operation is meant to be equivalent to `self.results.updates.push((time, diff))`.
        /// However, for "clever" reasons it does not do this. Instead, it looks for opportunities
        /// to encode a singleton update with an "absert" update: repeating the most recent offset.
        /// This otherwise invalid state encodes "look back one element".
        ///
        /// When `self.singleton` is `Some`, it means that we have seen one update and it matched the
        /// previously pushed update exactly. In that case, we do not push the update into `updates`.
        /// The update tuple is retained in `self.singleton` in case we see another update and need
        /// to recover the singleton to push it into `updates` to join the second update.
        fn push_update(&mut self, time: <L::Target as Update>::Time, diff: <L::Target as Update>::Diff) {
            // If a just-pushed update exactly equals `(time, diff)` we can avoid pushing it.
            if self.result.times.last().map(|t| t == <<L::TimeContainer as BatchContainer>::ReadItem<'_> as IntoOwned>::borrow_as(&time)) == Some(true) &&
               self.result.diffs.last().map(|d| d == <<L::DiffContainer as BatchContainer>::ReadItem<'_> as IntoOwned>::borrow_as(&diff)) == Some(true)
            {
                assert!(self.singleton.is_none());
                self.singleton = Some((time, diff));
            }
            else {
                // If we have pushed a single element, we need to copy it out to meet this one.
                if let Some((time, diff)) = self.singleton.take() {
                    self.result.times.push(time);
                    self.result.diffs.push(diff);
                }
                self.result.times.push(time);
                self.result.diffs.push(diff);
            }
        }
    }

    impl<L, CI> Builder for OrdValBuilder<L, CI>
    where
        L: Layout,
        CI: for<'a> BuilderInput<L::KeyContainer, L::ValContainer, Time=<L::Target as Update>::Time, Diff=<L::Target as Update>::Diff>,
        for<'a> L::KeyContainer: PushInto<CI::Key<'a>>,
        for<'a> L::ValContainer: PushInto<CI::Val<'a>>,
        for<'a> <L::TimeContainer as BatchContainer>::ReadItem<'a> : IntoOwned<'a, Owned = <L::Target as Update>::Time>,
        for<'a> <L::DiffContainer as BatchContainer>::ReadItem<'a> : IntoOwned<'a, Owned = <L::Target as Update>::Diff>,
    {

        type Input = CI;
        type Time = <L::Target as Update>::Time;
        type Output = OrdValBatch<L>;

        fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
            // We don't introduce zero offsets as they will be introduced by the first `push` call.
            Self { 
                result: OrdValStorage {
                    keys: L::KeyContainer::with_capacity(keys),
                    keys_offs: L::OffsetContainer::with_capacity(keys + 1),
                    vals: L::ValContainer::with_capacity(vals),
                    vals_offs: L::OffsetContainer::with_capacity(vals + 1),
                    times: L::TimeContainer::with_capacity(upds),
                    diffs: L::DiffContainer::with_capacity(upds),
                },
                singleton: None,
                singletons: 0,
                _marker: PhantomData,
            }
        }

        #[inline]
        fn push(&mut self, chunk: &mut Self::Input) {
            for item in chunk.drain() {
                let (key, val, time, diff) = CI::into_parts(item);
                // Perhaps this is a continuation of an already received key.
                if self.result.keys.last().map(|k| CI::key_eq(&key, k)).unwrap_or(false) {
                    // Perhaps this is a continuation of an already received value.
                    if self.result.vals.last().map(|v| CI::val_eq(&val, v)).unwrap_or(false) {
                        self.push_update(time, diff);
                    } else {
                        // New value; complete representation of prior value.
                        self.result.vals_offs.push(self.result.times.len());
                        if self.singleton.take().is_some() { self.singletons += 1; }
                        self.push_update(time, diff);
                        self.result.vals.push(val);
                    }
                } else {
                    // New key; complete representation of prior key.
                    self.result.vals_offs.push(self.result.times.len());
                    if self.singleton.take().is_some() { self.singletons += 1; }
                    self.result.keys_offs.push(self.result.vals.len());
                    self.push_update(time, diff);
                    self.result.vals.push(val);
                    self.result.keys.push(key);
                }
            }
        }

        #[inline(never)]
        fn done(mut self, lower: Antichain<Self::Time>, upper: Antichain<Self::Time>, since: Antichain<Self::Time>) -> OrdValBatch<L> {
            // Record the final offsets
            self.result.vals_offs.push(self.result.times.len());
            // Remove any pending singleton, and if it was set increment our count.
            if self.singleton.take().is_some() { self.singletons += 1; }
            self.result.keys_offs.push(self.result.vals.len());
            OrdValBatch {
                updates: self.result.times.len() + self.singletons,
                storage: self.result,
                description: Description::new(lower, upper, since),
            }
        }
    }

}

mod key_batch {

    use std::marker::PhantomData;
    use abomonation_derive::Abomonation;
    use timely::container::PushInto;
    use timely::progress::{Antichain, frontier::AntichainRef};

    use crate::trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
    use crate::trace::implementations::{BatchContainer, BuilderInput};
    use crate::trace::cursor::IntoOwned;

    use super::{Layout, Update};

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    #[derive(Abomonation, Debug)]
    pub struct OrdKeyStorage<L: Layout> {
        /// An ordered list of keys, corresponding to entries in `keys_offs`.
        pub keys: L::KeyContainer,
        /// Offsets used to provide indexes from keys to updates.
        ///
        /// This list has a special representation that any empty range indicates the singleton
        /// element just before the range, as if the start were decremented by one. The empty
        /// range is otherwise an invalid representation, and we borrow it to compactly encode
        /// single common update values (e.g. in a snapshot, the minimal time and a diff of one).
        ///
        /// The length of this list is one longer than `keys`, so that we can avoid bounds logic.
        pub keys_offs: L::OffsetContainer,
        /// Concatenated ordered lists of update times, bracketed by offsets in `vals_offs`.
        pub times: L::TimeContainer,
        /// Concatenated ordered lists of update diffs, bracketed by offsets in `vals_offs`.
        pub diffs: L::DiffContainer,
    }

    impl<L: Layout> OrdKeyStorage<L> {
        /// Lower and upper bounds in `self.vals` corresponding to the key at `index`.
        fn updates_for_key(&self, index: usize) -> (usize, usize) {
            let mut lower = self.keys_offs.index(index);
            let upper = self.keys_offs.index(index+1);
            // We use equal lower and upper to encode "singleton update; just before here".
            // It should only apply when there is a prior element, so `lower` should be greater than zero.
            if lower == upper {
                assert!(lower > 0);
                lower -= 1;
            }
            (lower, upper)
        }
    }

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    ///
    /// The `L` parameter captures how the updates should be laid out, and `C` determines which
    /// merge batcher to select.
    #[derive(Abomonation)]
    pub struct OrdKeyBatch<L: Layout> {
        /// The updates themselves.
        pub storage: OrdKeyStorage<L>,
        /// Description of the update times this layer represents.
        pub description: Description<<L::Target as Update>::Time>,
        /// The number of updates reflected in the batch.
        ///
        /// We track this separately from `storage` because due to the singleton optimization,
        /// we may have many more updates than `storage.updates.len()`. It should equal that
        /// length, plus the number of singleton optimizations employed.
        pub updates: usize,
    }

    impl<L: Layout> BatchReader for OrdKeyBatch<L> {
        
        type Key<'a> = <L::KeyContainer as BatchContainer>::ReadItem<'a>;
        type Val<'a> = &'a ();
        type Time = <L::Target as Update>::Time;
        type TimeGat<'a> = <L::TimeContainer as BatchContainer>::ReadItem<'a>;
        type Diff = <L::Target as Update>::Diff;
        type DiffGat<'a> = <L::DiffContainer as BatchContainer>::ReadItem<'a>;

        type Cursor = OrdKeyCursor<L>;
        fn cursor(&self) -> Self::Cursor {
            OrdKeyCursor {
                key_cursor: 0,
                val_stepped: false,
                phantom: std::marker::PhantomData,
            }
        }
        fn len(&self) -> usize {
            // Normally this would be `self.updates.len()`, but we have a clever compact encoding.
            // Perhaps we should count such exceptions to the side, to provide a correct accounting.
            self.updates
        }
        fn description(&self) -> &Description<<L::Target as Update>::Time> { &self.description }
    }

    impl<L: Layout> Batch for OrdKeyBatch<L> {
        type Merger = OrdKeyMerger<L>;

        fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self::Merger {
            OrdKeyMerger::new(self, other, compaction_frontier)
        }
    }

    /// State for an in-progress merge.
    pub struct OrdKeyMerger<L: Layout> {
        /// Key position to merge next in the first batch.
        key_cursor1: usize,
        /// Key position to merge next in the second batch.
        key_cursor2: usize,
        /// result that we are currently assembling.
        result: OrdKeyStorage<L>,
        /// description
        description: Description<<L::Target as Update>::Time>,

        /// Local stash of updates, to use for consolidation.
        ///
        /// We could emulate a `ChangeBatch` here, with related compaction smarts.
        /// A `ChangeBatch` itself needs an `i64` diff type, which we have not.
        update_stash: Vec<(<L::Target as Update>::Time, <L::Target as Update>::Diff)>,
        /// Counts the number of singleton-optimized entries, that we may correctly count the updates.
        singletons: usize,
    }

    impl<L: Layout> Merger<OrdKeyBatch<L>> for OrdKeyMerger<L>
    where
        OrdKeyBatch<L>: Batch<Time=<L::Target as Update>::Time>,
        for<'a> <L::TimeContainer as BatchContainer>::ReadItem<'a> : IntoOwned<'a, Owned = <L::Target as Update>::Time>,
        for<'a> <L::DiffContainer as BatchContainer>::ReadItem<'a> : IntoOwned<'a, Owned = <L::Target as Update>::Diff>,
    {
        fn new(batch1: &OrdKeyBatch<L>, batch2: &OrdKeyBatch<L>, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self {

            assert!(batch1.upper() == batch2.lower());
            use crate::lattice::Lattice;
            let mut since = batch1.description().since().join(batch2.description().since());
            since = since.join(&compaction_frontier.to_owned());

            let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);

            let batch1 = &batch1.storage;
            let batch2 = &batch2.storage;

            let mut storage = OrdKeyStorage {
                keys: L::KeyContainer::merge_capacity(&batch1.keys, &batch2.keys),
                keys_offs: L::OffsetContainer::with_capacity(batch1.keys_offs.len() + batch2.keys_offs.len()),
                times: L::TimeContainer::merge_capacity(&batch1.times, &batch2.times),
                diffs: L::DiffContainer::merge_capacity(&batch1.diffs, &batch2.diffs),
            };

            let keys_offs: &mut L::OffsetContainer = &mut storage.keys_offs;
            keys_offs.push(0);

            OrdKeyMerger {
                key_cursor1: 0,
                key_cursor2: 0,
                result: storage,
                description,
                update_stash: Vec::new(),
                singletons: 0,
            }
        }
        fn done(self) -> OrdKeyBatch<L> {
            OrdKeyBatch {
                updates: self.result.times.len() + self.singletons,
                storage: self.result,
                description: self.description,
            }
        }
        fn work(&mut self, source1: &OrdKeyBatch<L>, source2: &OrdKeyBatch<L>, fuel: &mut isize) {

            // An (incomplete) indication of the amount of work we've done so far.
            let starting_updates = self.result.times.len();
            let mut effort = 0isize;

            // While both mergees are still active, perform single-key merges.
            while self.key_cursor1 < source1.storage.keys.len() && self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.merge_key(&source1.storage, &source2.storage);
                // An (incomplete) accounting of the work we've done.
                effort = (self.result.times.len() - starting_updates) as isize;
            }

            // Merging is complete, and only copying remains.
            // Key-by-key copying allows effort interruption, and compaction.
            while self.key_cursor1 < source1.storage.keys.len() && effort < *fuel {
                self.copy_key(&source1.storage, self.key_cursor1);
                self.key_cursor1 += 1;
                effort = (self.result.times.len() - starting_updates) as isize;
            }
            while self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.copy_key(&source2.storage, self.key_cursor2);
                self.key_cursor2 += 1;
                effort = (self.result.times.len() - starting_updates) as isize;
            }

            *fuel -= effort;
        }
    }

    // Helper methods in support of merging batches.
    impl<L: Layout> OrdKeyMerger<L> {
        /// Copy the next key in `source`.
        ///
        /// The method extracts the key in `source` at `cursor`, and merges it in to `self`.
        /// If the result does not wholly cancel, they key will be present in `self` with the
        /// compacted values and updates. 
        /// 
        /// The caller should be certain to update the cursor, as this method does not do this.
        fn copy_key(&mut self, source: &OrdKeyStorage<L>, cursor: usize) {
            self.stash_updates_for_key(source, cursor);
            if let Some(off) = self.consolidate_updates() {
                self.result.keys_offs.push(off);
                self.result.keys.push(source.keys.index(cursor));
            }
        }
        /// Merge the next key in each of `source1` and `source2` into `self`, updating the appropriate cursors.
        ///
        /// This method only merges a single key. It applies all compaction necessary, and may result in no output
        /// if the updates cancel either directly or after compaction.
        fn merge_key(&mut self, source1: &OrdKeyStorage<L>, source2: &OrdKeyStorage<L>) {
            use ::std::cmp::Ordering;
            match source1.keys.index(self.key_cursor1).cmp(&source2.keys.index(self.key_cursor2)) {
                Ordering::Less => { 
                    self.copy_key(source1, self.key_cursor1);
                    self.key_cursor1 += 1;
                },
                Ordering::Equal => {
                    // Keys are equal; must merge all updates from both sources for this one key.
                    self.stash_updates_for_key(source1, self.key_cursor1);
                    self.stash_updates_for_key(source2, self.key_cursor2);
                    if let Some(off) = self.consolidate_updates() {
                        self.result.keys_offs.push(off);
                        self.result.keys.push(source1.keys.index(self.key_cursor1));
                    }
                    // Increment cursors in either case; the keys are merged.
                    self.key_cursor1 += 1;
                    self.key_cursor2 += 1;
                },
                Ordering::Greater => {
                    self.copy_key(source2, self.key_cursor2);
                    self.key_cursor2 += 1;
                },
            }
        }

        /// Transfer updates for an indexed value in `source` into `self`, with compaction applied.
        fn stash_updates_for_key(&mut self, source: &OrdKeyStorage<L>, index: usize) {
            let (lower, upper) = source.updates_for_key(index);
            for i in lower .. upper {
                // NB: Here is where we would need to look back if `lower == upper`.
                let time = source.times.index(i);
                let diff = source.diffs.index(i);
                use crate::lattice::Lattice;
                let mut new_time = time.into_owned();
                new_time.advance_by(self.description.since().borrow());
                self.update_stash.push((new_time, diff.into_owned()));
            }
        }

        /// Consolidates `self.updates_stash` and produces the offset to record, if any.
        fn consolidate_updates(&mut self) -> Option<usize> {
            use crate::consolidation;
            consolidation::consolidate(&mut self.update_stash);
            if !self.update_stash.is_empty() {
                // If there is a single element, equal to a just-prior recorded update,
                // we push nothing and report an unincremented offset to encode this case.
                let time_diff = self.result.times.last().zip(self.result.diffs.last());
                let last_eq = self.update_stash.last().zip(time_diff).map(|((t1, d1), (t2, d2))| {
                    let t1 = <<L::TimeContainer as BatchContainer>::ReadItem<'_> as IntoOwned>::borrow_as(t1);
                    let d1 = <<L::DiffContainer as BatchContainer>::ReadItem<'_> as IntoOwned>::borrow_as(d1);
                    t1.eq(&t2) && d1.eq(&d2)
                });
                if self.update_stash.len() == 1 && last_eq.unwrap_or(false) {
                    // Just clear out update_stash, as we won't drain it here.
                    self.update_stash.clear();
                    self.singletons += 1;
                }
                else {
                    // Conventional; move `update_stash` into `updates`.
                    for (time, diff) in self.update_stash.drain(..) {
                        self.result.times.push(time);
                        self.result.diffs.push(diff);
                    }
                }
                Some(self.result.times.len())
            } else {
                None
            }
        }
    }

    /// A cursor for navigating a single layer.
    pub struct OrdKeyCursor<L: Layout> {
        /// Absolute position of the current key.
        key_cursor: usize,
        /// If the value has been stepped for the key, there are no more values.
        val_stepped: bool,
        /// Phantom marker for Rust happiness.
        phantom: PhantomData<L>,
    }

    impl<L: Layout> Cursor for OrdKeyCursor<L> {
        type Key<'a> = <L::KeyContainer as BatchContainer>::ReadItem<'a>;
        type Val<'a> = &'a ();
        type Time = <L::Target as Update>::Time;
        type TimeGat<'a> = <L::TimeContainer as BatchContainer>::ReadItem<'a>;
        type Diff = <L::Target as Update>::Diff;
        type DiffGat<'a> = <L::DiffContainer as BatchContainer>::ReadItem<'a>;

        type Storage = OrdKeyBatch<L>;

        fn key<'a>(&self, storage: &'a Self::Storage) -> Self::Key<'a> { storage.storage.keys.index(self.key_cursor) }
        fn val<'a>(&self, _storage: &'a Self::Storage) -> &'a () { &() }
        fn map_times<L2: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &Self::Storage, mut logic: L2) {
            let (lower, upper) = storage.storage.updates_for_key(self.key_cursor);
            for index in lower .. upper {
                let time = storage.storage.times.index(index);
                let diff = storage.storage.diffs.index(index);
                logic(time, diff);
            }
        }
        fn key_valid(&self, storage: &Self::Storage) -> bool { self.key_cursor < storage.storage.keys.len() }
        fn val_valid(&self, _storage: &Self::Storage) -> bool { !self.val_stepped }
        fn step_key(&mut self, storage: &Self::Storage){
            self.key_cursor += 1;
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
            else {
                self.key_cursor = storage.storage.keys.len();
            }
        }
        fn seek_key(&mut self, storage: &Self::Storage, key: Self::Key<'_>) {
            self.key_cursor += storage.storage.keys.advance(self.key_cursor, storage.storage.keys.len(), |x| <L::KeyContainer as BatchContainer>::reborrow(x).lt(&<L::KeyContainer as BatchContainer>::reborrow(key)));
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
        }
        fn step_val(&mut self, _storage: &Self::Storage) {
            self.val_stepped = true;
        }
        fn seek_val(&mut self, _storage: &Self::Storage, _val: Self::Val<'_>) { }
        fn rewind_keys(&mut self, storage: &Self::Storage) {
            self.key_cursor = 0;
            if self.key_valid(storage) {
                self.rewind_vals(storage)
            }
        }
        fn rewind_vals(&mut self, _storage: &Self::Storage) {
            self.val_stepped = false;
        }
    }

    /// A builder for creating layers from unsorted update tuples.
    pub struct OrdKeyBuilder<L: Layout, CI> {
        result: OrdKeyStorage<L>,
        singleton: Option<(<L::Target as Update>::Time, <L::Target as Update>::Diff)>,
        /// Counts the number of singleton optimizations we performed.
        ///
        /// This number allows us to correctly gauge the total number of updates reflected in a batch,
        /// even though `updates.len()` may be much shorter than this amount.
        singletons: usize,
        _marker: PhantomData<CI>,
    }

    impl<L: Layout, CI> OrdKeyBuilder<L, CI> {
        /// Pushes a single update, which may set `self.singleton` rather than push.
        ///
        /// This operation is meant to be equivalent to `self.results.updates.push((time, diff))`.
        /// However, for "clever" reasons it does not do this. Instead, it looks for opportunities
        /// to encode a singleton update with an "absert" update: repeating the most recent offset.
        /// This otherwise invalid state encodes "look back one element".
        ///
        /// When `self.singleton` is `Some`, it means that we have seen one update and it matched the
        /// previously pushed update exactly. In that case, we do not push the update into `updates`.
        /// The update tuple is retained in `self.singleton` in case we see another update and need
        /// to recover the singleton to push it into `updates` to join the second update.
        fn push_update(&mut self, time: <L::Target as Update>::Time, diff: <L::Target as Update>::Diff) {
            // If a just-pushed update exactly equals `(time, diff)` we can avoid pushing it.
            let t1 = <<L::TimeContainer as BatchContainer>::ReadItem<'_> as IntoOwned>::borrow_as(&time);
            let d1 = <<L::DiffContainer as BatchContainer>::ReadItem<'_> as IntoOwned>::borrow_as(&diff);
            if self.result.times.last().map(|t| t == t1).unwrap_or(false) && self.result.diffs.last().map(|d| d == d1).unwrap_or(false) {
                assert!(self.singleton.is_none());
                self.singleton = Some((time, diff));
            }
            else {
                // If we have pushed a single element, we need to copy it out to meet this one.
                if let Some((time, diff)) = self.singleton.take() {
                    self.result.times.push(time);
                    self.result.diffs.push(diff);
                }
                self.result.times.push(time);
                self.result.diffs.push(diff);
            }
        }
    }

    impl<L: Layout, CI> Builder for OrdKeyBuilder<L, CI>
    where
        L: Layout,
        CI: for<'a> BuilderInput<L::KeyContainer, L::ValContainer, Time=<L::Target as Update>::Time, Diff=<L::Target as Update>::Diff>,
        for<'a> L::KeyContainer: PushInto<CI::Key<'a>>,
        for<'a> <L::TimeContainer as BatchContainer>::ReadItem<'a> : IntoOwned<'a, Owned = <L::Target as Update>::Time>,
        for<'a> <L::DiffContainer as BatchContainer>::ReadItem<'a> : IntoOwned<'a, Owned = <L::Target as Update>::Diff>,
    {

        type Input = CI;
        type Time = <L::Target as Update>::Time;
        type Output = OrdKeyBatch<L>;

        fn with_capacity(keys: usize, _vals: usize, upds: usize) -> Self {
            // We don't introduce zero offsets as they will be introduced by the first `push` call.
            Self { 
                result: OrdKeyStorage {
                    keys: L::KeyContainer::with_capacity(keys),
                    keys_offs: L::OffsetContainer::with_capacity(keys + 1),
                    times: L::TimeContainer::with_capacity(upds),
                    diffs: L::DiffContainer::with_capacity(upds),
                },
                singleton: None,
                singletons: 0,
                _marker: PhantomData,
            }
        }

        #[inline]
        fn push(&mut self, chunk: &mut Self::Input) {
            for item in chunk.drain() {
                let (key, _val, time, diff) = CI::into_parts(item);
                // Perhaps this is a continuation of an already received key.
                if self.result.keys.last().map(|k| CI::key_eq(&key, k)).unwrap_or(false) {
                    self.push_update(time, diff);
                } else {
                    // New key; complete representation of prior key.
                    self.result.keys_offs.push(self.result.times.len());
                    // Remove any pending singleton, and if it was set increment our count.
                    if self.singleton.take().is_some() { self.singletons += 1; }
                    self.push_update(time, diff);
                    self.result.keys.push(key);
                }
            }
        }

        #[inline(never)]
        fn done(mut self, lower: Antichain<Self::Time>, upper: Antichain<Self::Time>, since: Antichain<Self::Time>) -> OrdKeyBatch<L> {
            // Record the final offsets
            self.result.keys_offs.push(self.result.times.len());
            // Remove any pending singleton, and if it was set increment our count.
            if self.singleton.take().is_some() { self.singletons += 1; }
            OrdKeyBatch {
                updates: self.result.times.len() + self.singletons,
                storage: self.result,
                description: Description::new(lower, upper, since),
            }
        }
    }

}
