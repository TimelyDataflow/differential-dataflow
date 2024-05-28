//! Batch implementation based on Robin Hood Hashing.
//! 
//! Items are ordered by `(hash(Key), Key)` rather than `Key`, which means
//! that these implementations should only be used with each other, under 
//! the same `hash` function, or for types that also order by `(hash(X), X)`,
//! for example wrapped types that implement `Ord` that way.

use std::rc::Rc;
use std::cmp::Ordering;

use abomonation_derive::Abomonation;
use timely::container::columnation::TimelyStack;

use crate::Hashable;
use crate::trace::implementations::merge_batcher::{MergeBatcher, VecMerger};
use crate::trace::implementations::merge_batcher_col::ColumnationMerger;
use crate::trace::implementations::spine_fueled::Spine;
use crate::trace::rc_blanket_impls::RcBuilder;

use super::{Update, Layout, Vector, TStack};

use self::val_batch::{RhhValBatch, RhhValBuilder};

/// A trace implementation using a spine of ordered lists.
pub type VecSpine<K, V, T, R> = Spine<
    Rc<RhhValBatch<Vector<((K,V),T,R)>>>,
    MergeBatcher<VecMerger<((K, V), T, R)>, T>,
    RcBuilder<RhhValBuilder<Vector<((K,V),T,R)>, Vec<((K,V),T,R)>>>,
>;
// /// A trace implementation for empty values using a spine of ordered lists.
// pub type OrdKeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<Vector<((K,()),T,R)>>>>;

/// A trace implementation backed by columnar storage.
pub type ColSpine<K, V, T, R> = Spine<
    Rc<RhhValBatch<TStack<((K,V),T,R)>>>,
    MergeBatcher<ColumnationMerger<((K,V),T,R)>, T>,
    RcBuilder<RhhValBuilder<TStack<((K,V),T,R)>, TimelyStack<((K,V),T,R)>>>,
>;
// /// A trace implementation backed by columnar storage.
// pub type ColKeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<TStack<((K,()),T,R)>>>>;

/// A carrier trait indicating that the type's `Ord` and `PartialOrd` implementations are by `Hashable::hashed()`.
pub trait HashOrdered: Hashable { }

impl<'a, T: std::hash::Hash + HashOrdered> HashOrdered for &'a T { }

/// A hash-ordered wrapper that modifies `Ord` and `PartialOrd`.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Abomonation, Default)]
pub struct HashWrapper<T: std::hash::Hash + Hashable> {
    /// The inner value, freely modifiable.
    pub inner: T
}

impl<T: PartialOrd + std::hash::Hash + Hashable> PartialOrd for HashWrapper<T>
where <T as Hashable>::Output: PartialOrd {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let this_hash = self.inner.hashed();
        let that_hash = other.inner.hashed();
        (this_hash, &self.inner).partial_cmp(&(that_hash, &other.inner))
    }
}

impl<T: Ord + PartialOrd + std::hash::Hash + Hashable> Ord for HashWrapper<T> 
where <T as Hashable>::Output: PartialOrd { 
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl<T: std::hash::Hash + Hashable> HashOrdered for HashWrapper<T> { }

impl<T: std::hash::Hash + Hashable> Hashable for HashWrapper<T> {
    type Output = T::Output;
    fn hashed(&self) -> Self::Output { self.inner.hashed() }
}

impl<T: std::hash::Hash + Hashable> HashOrdered for &HashWrapper<T> { }

impl<T: std::hash::Hash + Hashable> Hashable for &HashWrapper<T> {
    type Output = T::Output;
    fn hashed(&self) -> Self::Output { self.inner.hashed() }
}

mod val_batch {

    use std::convert::TryInto;
    use std::marker::PhantomData;
    use abomonation_derive::Abomonation;
    use timely::container::PushInto;
    use timely::progress::{Antichain, frontier::AntichainRef};

    use crate::hashable::Hashable;
    use crate::trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
    use crate::trace::implementations::{BatchContainer, BuilderInput};
    use crate::trace::cursor::IntoOwned;

    use super::{Layout, Update, HashOrdered};

    /// Update tuples organized as a Robin Hood Hash map, ordered by `(hash(Key), Key, Val, Time)`.
    ///
    /// Specifically, this means that we attempt to place any `Key` at `alloc_len * (hash(Key) / 2^64)`, 
    /// and spill onward if the slot is occupied. The cleverness of RHH is that you may instead evict 
    /// someone else, in order to maintain the ordering up above. In fact, that is basically the rule: 
    /// when there is a conflict, evict the greater of the two and attempt to place it in the next slot.
    ///
    /// This RHH implementation uses a repeated `keys_offs` offset to indicate an absent element, as all
    /// keys for valid updates must have some associated values with updates. This is the same type of 
    /// optimization made for repeated updates, and it rules out (here) using that trick for repeated values.
    ///
    /// We will use the `Hashable` trait here, but any consistent hash function should work out ok. 
    /// We specifically want to use the highest bits of the result (we will) because the low bits have
    /// likely been spent shuffling the data between workers (by key), and are likely low entropy.
    #[derive(Abomonation)]
    pub struct RhhValStorage<L: Layout> 
    where 
        <L::Target as Update>::Key: Default + HashOrdered,
    {

        /// The requested capacity for `keys`. We use this when determining where a key with a certain hash
        /// would most like to end up. The `BatchContainer` trait does not provide a `capacity()` method,
        /// otherwise we would just use that.
        pub key_capacity: usize,
        /// A number large enough that when it divides any `u64` the result is at most `self.key_capacity`.
        /// When that capacity is zero or one, this is set to zero instead.
        pub divisor: u64,
        /// The number of present keys, distinct from `keys.len()` which contains 
        pub key_count: usize,

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

    impl<L: Layout> RhhValStorage<L> 
    where 
        <L::Target as Update>::Key: Default + HashOrdered,
        for<'a> <L::KeyContainer as BatchContainer>::ReadItem<'a>: HashOrdered,
    {
        /// Lower and upper bounds in `self.vals` corresponding to the key at `index`.
        fn values_for_key(&self, index: usize) -> (usize, usize) {
            let lower = self.keys_offs.index(index);
            let upper = self.keys_offs.index(index+1);
            // Looking up values for an invalid key indicates something is wrong.
            assert!(lower < upper, "{:?} v {:?} at {:?}", lower, upper, index);
            (lower, upper)
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

        /// Inserts the key at its desired location, or nearby.
        ///
        /// Because there may be collisions, they key may be placed just after its desired location.
        /// If necessary, this method will introduce default keys and copy the offsets to create space
        /// after which to insert the key. These will be indicated by `None` entries in the `hash` vector.
        ///
        /// If `offset` is specified, we will insert it at the appropriate location. If it is not specified,
        /// we leave `keys_offs` ready to receive it as the next `push`. This is so that builders that may
        /// not know the final offset at the moment of key insertion can prepare for receiving the offset.
        fn insert_key(&mut self, key: <L::KeyContainer as BatchContainer>::ReadItem<'_>, offset: Option<usize>) {
            let desired = self.desired_location(&key);
            // Were we to push the key now, it would be at `self.keys.len()`, so while that is wrong, 
            // push additional blank entries in.
            while self.keys.len() < desired {
                // We insert a default (dummy) key and repeat the offset to indicate this.
                let current_offset = self.keys_offs.index(self.keys.len());
                self.keys.push(Default::default());
                self.keys_offs.copy(current_offset);
            }

            // Now we insert the key. Even if it is no longer the desired location because of contention.
            // If an offset has been supplied we insert it, and otherwise leave it for future determination.
            self.keys.copy(key);
            if let Some(offset) = offset {
                self.keys_offs.copy(offset);
            }
            self.key_count += 1;
        }

        /// Indicates both the desired location and the hash signature of the key.
        fn desired_location<K: Hashable>(&self, key: &K) -> usize {
            if self.divisor == 0 { 0 }
            else {
                (key.hashed().into() / self.divisor).try_into().expect("divisor not large enough to force u64 into uisze")
            }
        }

        /// Returns true if one should advance one's index in the search for `key`.
        fn advance_key(&self, index: usize, key: <L::KeyContainer as BatchContainer>::ReadItem<'_>) -> bool {
            // Ideally this short-circuits, as `self.keys[index]` is bogus data.
            !self.live_key(index) || self.keys.index(index).lt(&<L::KeyContainer as BatchContainer>::reborrow(key))
        }

        /// Indicates that a key is valid, rather than dead space, by looking for a valid offset range.
        fn live_key(&self, index: usize) -> bool {
            self.keys_offs.index(index) != self.keys_offs.index(index+1)
        }

        /// Advances `index` until it references a live key, or is `keys.len()`.
        fn advance_to_live_key(&self, index: &mut usize) {
            while *index < self.keys.len() && !self.live_key(*index) {
                *index += 1;
            }
        }

        /// A value large enough that any `u64` divided by it is less than `capacity`.
        ///
        /// This is `2^64 / capacity`, except in the cases where `capacity` is zero or one.
        /// In those cases, we'll return `0` to communicate the exception, for which we should
        /// just return `0` when announcing a target location (and a zero capacity that we insert
        /// into becomes a bug).
        fn divisor_for_capacity(capacity: usize) -> u64 {
            let capacity: u64 = capacity.try_into().expect("usize exceeds u64");
            if capacity == 0 || capacity == 1 { 0 }
            else {
                ((1 << 63) / capacity) << 1
            }
        }
    }

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    ///
    /// The `L` parameter captures how the updates should be laid out, and `C` determines which
    /// merge batcher to select.
    #[derive(Abomonation)]
    pub struct RhhValBatch<L: Layout> 
    where 
        <L::Target as Update>::Key: Default + HashOrdered,
    {
        /// The updates themselves.
        pub storage: RhhValStorage<L>,
        /// Description of the update times this layer represents.
        pub description: Description<<L::Target as Update>::Time>,
        /// The number of updates reflected in the batch.
        ///
        /// We track this separately from `storage` because due to the singleton optimization,
        /// we may have many more updates than `storage.updates.len()`. It should equal that 
        /// length, plus the number of singleton optimizations employed.
        pub updates: usize,
    }

    impl<L: Layout> BatchReader for RhhValBatch<L> 
    where 
        <L::Target as Update>::Key: Default + HashOrdered,
        for<'a> <L::KeyContainer as BatchContainer>::ReadItem<'a>: HashOrdered,
    {
        type Key<'a> = <L::KeyContainer as BatchContainer>::ReadItem<'a>;
        type Val<'a> = <L::ValContainer as BatchContainer>::ReadItem<'a>;
        type Time = <L::Target as Update>::Time;
        type TimeGat<'a> = <L::TimeContainer as BatchContainer>::ReadItem<'a>;
        type Diff = <L::Target as Update>::Diff;
        type DiffGat<'a> = <L::DiffContainer as BatchContainer>::ReadItem<'a>;

        type Cursor = RhhValCursor<L>;
        fn cursor(&self) -> Self::Cursor { 
            let mut cursor = RhhValCursor {
                key_cursor: 0,
                val_cursor: 0,
                phantom: std::marker::PhantomData,
            };
            cursor.step_key(self);
            cursor
        }
        fn len(&self) -> usize { 
            // Normally this would be `self.updates.len()`, but we have a clever compact encoding.
            // Perhaps we should count such exceptions to the side, to provide a correct accounting.
            self.updates
        }
        fn description(&self) -> &Description<<L::Target as Update>::Time> { &self.description }
    }

    impl<L: Layout> Batch for RhhValBatch<L> 
    where 
        <L::Target as Update>::Key: Default + HashOrdered,
        for<'a> <L::KeyContainer as BatchContainer>::ReadItem<'a>: HashOrdered,
    {
        type Merger = RhhValMerger<L>;

        fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self::Merger {
            RhhValMerger::new(self, other, compaction_frontier)
        }
    }

    /// State for an in-progress merge.
    pub struct RhhValMerger<L: Layout> 
    where 
        <L::Target as Update>::Key: Default + HashOrdered,
    {
        /// Key position to merge next in the first batch.
        key_cursor1: usize,
        /// Key position to merge next in the second batch.
        key_cursor2: usize,
        /// result that we are currently assembling.
        result: RhhValStorage<L>,
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

    impl<L: Layout> Merger<RhhValBatch<L>> for RhhValMerger<L>
    where
        <L::Target as Update>::Key: Default + HashOrdered,
        RhhValBatch<L>: Batch<Time=<L::Target as Update>::Time>,
        for<'a> <L::KeyContainer as BatchContainer>::ReadItem<'a>: HashOrdered,
    {
        fn new(batch1: &RhhValBatch<L>, batch2: &RhhValBatch<L>, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self {

            assert!(batch1.upper() == batch2.lower());
            use crate::lattice::Lattice;
            let mut since = batch1.description().since().join(batch2.description().since());
            since = since.join(&compaction_frontier.to_owned());

            let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);

            // This is a massive overestimate on the number of keys, but we don't have better information.
            // An over-estimate can be a massive problem as well, with sparse regions being hard to cross.
            let max_cap = batch1.len() + batch2.len();
            let rhh_cap = 2 * max_cap;

            let batch1 = &batch1.storage;
            let batch2 = &batch2.storage;

            let mut storage = RhhValStorage {
                keys: L::KeyContainer::merge_capacity(&batch1.keys, &batch2.keys),
                keys_offs: L::OffsetContainer::with_capacity(batch1.keys_offs.len() + batch2.keys_offs.len()),
                vals: L::ValContainer::merge_capacity(&batch1.vals, &batch2.vals),
                vals_offs: L::OffsetContainer::with_capacity(batch1.vals_offs.len() + batch2.vals_offs.len()),
                times: L::TimeContainer::merge_capacity(&batch1.times, &batch2.times),
                diffs: L::DiffContainer::merge_capacity(&batch1.diffs, &batch2.diffs),
                key_count: 0,
                key_capacity: rhh_cap,
                divisor: RhhValStorage::<L>::divisor_for_capacity(rhh_cap),
            };

            // Mark explicit types because type inference fails to resolve it.
            let keys_offs: &mut L::OffsetContainer = &mut storage.keys_offs;
            keys_offs.copy(0);
            let vals_offs: &mut L::OffsetContainer = &mut storage.vals_offs;
            vals_offs.copy(0);

            RhhValMerger {
                key_cursor1: 0,
                key_cursor2: 0,
                result: storage,
                description,
                update_stash: Vec::new(),
                singletons: 0,
            }
        }
        fn done(self) -> RhhValBatch<L> {
            RhhValBatch {
                updates: self.result.times.len() + self.singletons,
                storage: self.result,
                description: self.description,
            }
        }
        fn work(&mut self, source1: &RhhValBatch<L>, source2: &RhhValBatch<L>, fuel: &mut isize) {

            // An (incomplete) indication of the amount of work we've done so far.
            let starting_updates = self.result.times.len();
            let mut effort = 0isize;

            source1.storage.advance_to_live_key(&mut self.key_cursor1);
            source2.storage.advance_to_live_key(&mut self.key_cursor2);

            // While both mergees are still active, perform single-key merges.
            while self.key_cursor1 < source1.storage.keys.len() && self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.merge_key(&source1.storage, &source2.storage);
                source1.storage.advance_to_live_key(&mut self.key_cursor1);
                source2.storage.advance_to_live_key(&mut self.key_cursor2);
                    // An (incomplete) accounting of the work we've done.
                effort = (self.result.times.len() - starting_updates) as isize;
            }

            // Merging is complete, and only copying remains. 
            // Key-by-key copying allows effort interruption, and compaction.
            while self.key_cursor1 < source1.storage.keys.len() && effort < *fuel {
                self.copy_key(&source1.storage, self.key_cursor1);
                self.key_cursor1 += 1;
                source1.storage.advance_to_live_key(&mut self.key_cursor1);
                effort = (self.result.times.len() - starting_updates) as isize;
            }
            while self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.copy_key(&source2.storage, self.key_cursor2);
                self.key_cursor2 += 1;
                source2.storage.advance_to_live_key(&mut self.key_cursor2);
                effort = (self.result.times.len() - starting_updates) as isize;
            }

            *fuel -= effort;
        }
    }

    // Helper methods in support of merging batches.
    impl<L: Layout> RhhValMerger<L> 
    where 
        <L::Target as Update>::Key: Default + HashOrdered,
        for<'a> <L::KeyContainer as BatchContainer>::ReadItem<'a>: HashOrdered,
    {
        /// Copy the next key in `source`.
        ///
        /// The method extracts the key in `source` at `cursor`, and merges it in to `self`.
        /// If the result does not wholly cancel, they key will be present in `self` with the
        /// compacted values and updates. 
        /// 
        /// The caller should be certain to update the cursor, as this method does not do this.
        fn copy_key(&mut self, source: &RhhValStorage<L>, cursor: usize) {
            // Capture the initial number of values to determine if the merge was ultimately non-empty.
            let init_vals = self.result.vals.len();
            let (mut lower, upper) = source.values_for_key(cursor);
            while lower < upper {
                self.stash_updates_for_val(source, lower);
                if let Some(off) = self.consolidate_updates() {
                    self.result.vals_offs.copy(off);
                    self.result.vals.copy(source.vals.index(lower));
                }
                lower += 1;
            }            

            // If we have pushed any values, copy the key as well.
            if self.result.vals.len() > init_vals {
                self.result.insert_key(source.keys.index(cursor), Some(self.result.vals.len()));
            }           
        }
        /// Merge the next key in each of `source1` and `source2` into `self`, updating the appropriate cursors.
        ///
        /// This method only merges a single key. It applies all compaction necessary, and may result in no output
        /// if the updates cancel either directly or after compaction.
        fn merge_key(&mut self, source1: &RhhValStorage<L>, source2: &RhhValStorage<L>) {

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
                        self.result.insert_key(source1.keys.index(self.key_cursor1), Some(off));
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
            (source1, mut lower1, upper1): (&RhhValStorage<L>, usize, usize), 
            (source2, mut lower2, upper2): (&RhhValStorage<L>, usize, usize),
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
                            self.result.vals_offs.copy(off);
                            self.result.vals.copy(source1.vals.index(lower1));
                        }
                        lower1 += 1;
                    },
                    Ordering::Equal => {
                        self.stash_updates_for_val(source1, lower1);
                        self.stash_updates_for_val(source2, lower2);
                        if let Some(off) = self.consolidate_updates() {
                            self.result.vals_offs.copy(off);
                            self.result.vals.copy(source1.vals.index(lower1));
                        }
                        lower1 += 1;
                        lower2 += 1;
                    },
                    Ordering::Greater => { 
                        // Extend stash by updates, with logical compaction applied.
                        self.stash_updates_for_val(source2, lower2);
                        if let Some(off) = self.consolidate_updates() {
                            self.result.vals_offs.copy(off);
                            self.result.vals.copy(source2.vals.index(lower2));
                        }
                        lower2 += 1;
                    },
                }
            }
            // Merging is complete, but we may have remaining elements to push.
            while lower1 < upper1 {
                self.stash_updates_for_val(source1, lower1);
                if let Some(off) = self.consolidate_updates() {
                    self.result.vals_offs.copy(off);
                    self.result.vals.copy(source1.vals.index(lower1));
                }
                lower1 += 1;
            }
            while lower2 < upper2 {
                self.stash_updates_for_val(source2, lower2);
                if let Some(off) = self.consolidate_updates() {
                    self.result.vals_offs.copy(off);
                    self.result.vals.copy(source2.vals.index(lower2));
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
        fn stash_updates_for_val(&mut self, source: &RhhValStorage<L>, index: usize) {
            let (lower, upper) = source.updates_for_value(index);
            for i in lower .. upper {
                // NB: Here is where we would need to look back if `lower == upper`.
                let time = source.times.index(i);
                let diff = source.diffs.index(i);
                let mut new_time = time.into_owned();
                use crate::lattice::Lattice;
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


    /// A cursor through a Robin Hood Hashed list of keys, vals, and such.
    ///
    /// The important detail is that not all of `keys` represent valid keys.
    /// We must consult `storage.hashed` to see if the associated data is valid.
    /// Importantly, we should skip over invalid keys, rather than report them as
    /// invalid through `key_valid`: that method is meant to indicate the end of
    /// the cursor, rather than internal state.
    pub struct RhhValCursor<L: Layout> 
    where 
        <L::Target as Update>::Key: Default + HashOrdered,
    {
        /// Absolute position of the current key.
        key_cursor: usize,
        /// Absolute position of the current value.
        val_cursor: usize,
        /// Phantom marker for Rust happiness.
        phantom: PhantomData<L>,
    }

    impl<L: Layout> Cursor for RhhValCursor<L> 
    where 
        <L::Target as Update>::Key: Default + HashOrdered,
        for<'a> <L::KeyContainer as BatchContainer>::ReadItem<'a>: HashOrdered,
    {
        type Key<'a> = <L::KeyContainer as BatchContainer>::ReadItem<'a>;
        type Val<'a> = <L::ValContainer as BatchContainer>::ReadItem<'a>;
        type Time = <L::Target as Update>::Time;
        type TimeGat<'a> = <L::TimeContainer as BatchContainer>::ReadItem<'a>;
        type Diff = <L::Target as Update>::Diff;
        type DiffGat<'a> = <L::DiffContainer as BatchContainer>::ReadItem<'a>;

        type Storage = RhhValBatch<L>;

        fn key<'a>(&self, storage: &'a RhhValBatch<L>) -> Self::Key<'a> { 
            storage.storage.keys.index(self.key_cursor) 
        }
        fn val<'a>(&self, storage: &'a RhhValBatch<L>) -> Self::Val<'a> { storage.storage.vals.index(self.val_cursor) }
        fn map_times<L2: FnMut(Self::TimeGat<'_>, Self::DiffGat<'_>)>(&mut self, storage: &RhhValBatch<L>, mut logic: L2) {
            let (lower, upper) = storage.storage.updates_for_value(self.val_cursor);
            for index in lower .. upper {
                let time = storage.storage.times.index(index);
                let diff = storage.storage.diffs.index(index);
                logic(time, diff);
            }
        }
        fn key_valid(&self, storage: &RhhValBatch<L>) -> bool { self.key_cursor < storage.storage.keys.len() }
        fn val_valid(&self, storage: &RhhValBatch<L>) -> bool { self.val_cursor < storage.storage.values_for_key(self.key_cursor).1 }
        fn step_key(&mut self, storage: &RhhValBatch<L>){
            // We advance the cursor by one for certain, and then as long as we need to find a valid key.
            self.key_cursor += 1;
            storage.storage.advance_to_live_key(&mut self.key_cursor);

            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
            else {
                self.key_cursor = storage.storage.keys.len();
            }
        }
        fn seek_key(&mut self, storage: &RhhValBatch<L>, key: Self::Key<'_>) {
            // self.key_cursor += storage.storage.keys.advance(self.key_cursor, storage.storage.keys.len(), |x| x.lt(key));
            let desired = storage.storage.desired_location(&key);
            // Advance the cursor, if `desired` is ahead of it.
            if self.key_cursor < desired {
                self.key_cursor = desired;
            }
            // Advance the cursor as long as we have not found a value greater or equal to `key`.
            // We may have already passed `key`, and confirmed its absence, but our goal is to
            // find the next key afterwards so that users can, for example, alternately iterate.
            while self.key_valid(storage) && storage.storage.advance_key(self.key_cursor, key) {
                // TODO: Based on our encoding, we could skip logarithmically over empy regions by galloping
                //       through `storage.keys_offs`, which stays put for dead space.
                self.key_cursor += 1;
            }

            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
        }
        fn step_val(&mut self, storage: &RhhValBatch<L>) {
            self.val_cursor += 1; 
            if !self.val_valid(storage) {
                self.val_cursor = storage.storage.values_for_key(self.key_cursor).1;
            }
        }
        fn seek_val(&mut self, storage: &RhhValBatch<L>, val: Self::Val<'_>) {
            self.val_cursor += storage.storage.vals.advance(self.val_cursor, storage.storage.values_for_key(self.key_cursor).1, |x| <L::ValContainer as BatchContainer>::reborrow(x).lt(&<L::ValContainer as BatchContainer>::reborrow(val)));
        }
        fn rewind_keys(&mut self, storage: &RhhValBatch<L>) {
            self.key_cursor = 0;
            storage.storage.advance_to_live_key(&mut self.key_cursor);

            if self.key_valid(storage) {
                self.rewind_vals(storage)
            }
        }
        fn rewind_vals(&mut self, storage: &RhhValBatch<L>) {
            self.val_cursor = storage.storage.values_for_key(self.key_cursor).0;
        }
    }

    /// A builder for creating layers from unsorted update tuples.
    pub struct RhhValBuilder<L: Layout, CI>
    where 
        <L::Target as Update>::Key: Default + HashOrdered,
    {
        result: RhhValStorage<L>,
        singleton: Option<(<L::Target as Update>::Time, <L::Target as Update>::Diff)>,
        /// Counts the number of singleton optimizations we performed.
        ///
        /// This number allows us to correctly gauge the total number of updates reflected in a batch,
        /// even though `updates.len()` may be much shorter than this amount.
        singletons: usize,
        _marker: PhantomData<CI>,
    }

    impl<L: Layout, CI> RhhValBuilder<L, CI>
    where 
        <L::Target as Update>::Key: Default + HashOrdered,
    {
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

    impl<L: Layout, CI> Builder for RhhValBuilder<L, CI>
    where
        <L::Target as Update>::Key: Default + HashOrdered,
        CI: for<'a> BuilderInput<L, Key<'a> = <L::Target as Update>::Key, Time=<L::Target as Update>::Time, Diff=<L::Target as Update>::Diff>,
        for<'a> L::ValContainer: PushInto<CI::Val<'a>>,
        for<'a> <L::KeyContainer as BatchContainer>::ReadItem<'a>: HashOrdered + IntoOwned<'a, Owned = <L::Target as Update>::Key>,
    {
        type Input = CI;
        type Time = <L::Target as Update>::Time;
        type Output = RhhValBatch<L>;

        fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {

            // Double the capacity for RHH; probably excessive.
            let rhh_capacity = 2 * keys;
            let divisor = RhhValStorage::<L>::divisor_for_capacity(rhh_capacity);                        
            // We want some additive slop, in case we spill over.
            // This number magically chosen based on nothing in particular.
            // Worst case, we will re-alloc and copy if we spill beyond this.
            let keys = rhh_capacity + 10;

            // We don't introduce zero offsets as they will be introduced by the first `push` call.
            Self { 
                result: RhhValStorage {
                    keys: L::KeyContainer::with_capacity(keys),
                    keys_offs: L::OffsetContainer::with_capacity(keys + 1),
                    vals: L::ValContainer::with_capacity(vals),
                    vals_offs: L::OffsetContainer::with_capacity(vals + 1),
                    times: L::TimeContainer::with_capacity(upds),
                    diffs: L::DiffContainer::with_capacity(upds),
                    key_count: 0,
                    key_capacity: rhh_capacity,
                    divisor,
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
                        self.result.vals_offs.copy(self.result.times.len());
                        if self.singleton.take().is_some() { self.singletons += 1; }
                        self.push_update(time, diff);
                        self.result.vals.push(val);
                    }
                } else {
                    // New key; complete representation of prior key.
                    self.result.vals_offs.copy(self.result.times.len());
                    if self.singleton.take().is_some() { self.singletons += 1; }
                    self.result.keys_offs.copy(self.result.vals.len());
                    self.push_update(time, diff);
                    self.result.vals.push(val);
                    // Insert the key, but with no specified offset.
                    self.result.insert_key(IntoOwned::borrow_as(&key), None);
                }
            }
        }

        #[inline(never)]
        fn done(mut self, lower: Antichain<Self::Time>, upper: Antichain<Self::Time>, since: Antichain<Self::Time>) -> RhhValBatch<L> {
            // Record the final offsets
            self.result.vals_offs.copy(self.result.times.len());
            // Remove any pending singleton, and if it was set increment our count.
            if self.singleton.take().is_some() { self.singletons += 1; }
            self.result.keys_offs.copy(self.result.vals.len());
            RhhValBatch {
                updates: self.result.times.len() + self.singletons,
                storage: self.result,
                description: Description::new(lower, upper, since),
            }
        }
    }

}

mod key_batch {

    // Copy the above, once it works!

}
