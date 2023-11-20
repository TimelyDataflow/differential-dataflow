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

use trace::implementations::spine_fueled::Spine;

use super::{Update, Layout, Vector, TStack};

use self::val_batch::{OrdValBatch};


/// A trace implementation using a spine of ordered lists.
pub type OrdValSpine<K, V, T, R, O=usize> = Spine<Rc<OrdValBatch<Vector<((K,V),T,R), O>>>>;
// /// A trace implementation for empty values using a spine of ordered lists.
// pub type OrdKeySpine<K, T, R, O=usize> = Spine<Rc<OrdKeyBatch<Vector<((K,()),T,R), O>>>>;

/// A trace implementation backed by columnar storage.
pub type ColValSpine<K, V, T, R, O=usize> = Spine<Rc<OrdValBatch<TStack<((K,V),T,R), O>>>>;
// /// A trace implementation backed by columnar storage.
// pub type ColKeySpine<K, T, R, O=usize> = Spine<Rc<OrdKeyBatch<TStack<((K,()),T,R), O>>>>;

mod val_batch {

    use std::convert::TryInto;
    use timely::progress::{Antichain, frontier::AntichainRef};

    use trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
    use trace::layers::BatchContainer;
    
    use super::{Layout, Update};
    use super::super::merge_batcher::MergeBatcher;

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    #[derive(Abomonation, Debug)]
    pub struct OrdValStorage<L: Layout> {
        /// An ordered list of keys, corresponding to entries in `keys_offs`.
        pub keys: L::KeyContainer,
        /// Offsets used to provide indexes from keys to values.
        ///
        /// The length of this list is one longer than `keys`, so that we can avoid bounds logic.
        pub keys_offs: Vec<L::KeyOffset>,
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
        pub vals_offs: Vec<L::ValOffset>,
        /// Concatenated ordered lists of updates, bracketed by offsets in `vals_offs`.
        pub updates: L::UpdContainer,
    }

    impl<L: Layout> OrdValStorage<L> {
        /// Lower and upper bounds in `self.vals` corresponding to the key at `index`.
        fn values_for_key(&self, index: usize) -> (usize, usize) {
            (self.keys_offs[index].try_into().ok().unwrap(), self.keys_offs[index+1].try_into().ok().unwrap())
        }
        /// Lower and upper bounds in `self.updates` corresponding to the value at `index`.
        fn updates_for_value(&self, index: usize) -> (usize, usize) {
            (self.vals_offs[index].try_into().ok().unwrap(), self.vals_offs[index+1].try_into().ok().unwrap())
        }
    }

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    #[derive(Abomonation)]
    pub struct OrdValBatch<L: Layout> {
        /// The updates themselves.
        pub storage: OrdValStorage<L>,
        /// Description of the update times this layer represents.
        pub description: Description<<L::Target as Update>::Time>,
    }

    impl<L: Layout> BatchReader for OrdValBatch<L> {
        type Key = <L::Target as Update>::Key;
        type Val = <L::Target as Update>::Val;
        type Time = <L::Target as Update>::Time;
        type R = <L::Target as Update>::Diff;

        type Cursor = OrdValCursor<L>;
        fn cursor(&self) -> Self::Cursor { 
            OrdValCursor {
                key_cursor: 0,
                val_cursor: 0,
                phantom: std::marker::PhantomData,
            }
        }
        fn len(&self) -> usize { 
            // Normally this would be `self.updates.len()`, but we have a clever compact encoding.
            // Perhaps we should count such exceptions to the side, to provide a correct accounting.
            self.storage.updates.len()
        }
        fn description(&self) -> &Description<<L::Target as Update>::Time> { &self.description }
    }

    impl<L: Layout> Batch for OrdValBatch<L> {
        type Batcher = MergeBatcher<Self>;
        type Builder = OrdValBuilder<L>;
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
    }

    impl<L: Layout> Merger<OrdValBatch<L>> for OrdValMerger<L> {
        fn new(batch1: &OrdValBatch<L>, batch2: &OrdValBatch<L>, compaction_frontier: AntichainRef<<L::Target as Update>::Time>) -> Self {

            assert!(batch1.upper() == batch2.lower());
            use lattice::Lattice;
            let mut since = batch1.description().since().join(batch2.description().since());
            since = since.join(&compaction_frontier.to_owned());

            let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);

            let batch1 = &batch1.storage;
            let batch2 = &batch2.storage;

            let mut storage = OrdValStorage {
                keys: L::KeyContainer::merge_capacity(&batch1.keys, &batch2.keys),
                keys_offs: Vec::with_capacity(batch1.keys_offs.len() + batch2.keys_offs.len()),
                vals: L::ValContainer::merge_capacity(&batch1.vals, &batch2.vals),
                vals_offs: Vec::with_capacity(batch1.vals_offs.len() + batch2.vals_offs.len()),
                updates: L::UpdContainer::merge_capacity(&batch1.updates, &batch2.updates),
            };

            storage.keys_offs.push(0.try_into().ok().unwrap());
            storage.vals_offs.push(0.try_into().ok().unwrap());

            OrdValMerger {
                key_cursor1: 0,
                key_cursor2: 0,
                result: storage,
                description,
                update_stash: Vec::new(),
            }
        }
        fn done(self) -> OrdValBatch<L> {
            OrdValBatch {
                storage: self.result,
                description: self.description,
            }
        }
        fn work(&mut self, source1: &OrdValBatch<L>, source2: &OrdValBatch<L>, fuel: &mut isize) {

            // An (incomplete) indication of the amount of work we've done so far.
            let starting_updates = self.result.updates.len();
            let mut effort = 0isize;

            // While both mergees are still active, perform single-key merges.
            while self.key_cursor1 < source1.storage.keys.len() && self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.merge_key(&source1.storage, &source2.storage);
                // An (incomplete) accounting of the work we've done.
                effort = (self.result.updates.len() - starting_updates) as isize;
            }

            // Merging is complete, and only copying remains. 
            // Key-by-key copying allows effort interruption, and compaction.
            while self.key_cursor1 < source1.storage.keys.len() && effort < *fuel {
                self.copy_key(&source1.storage, self.key_cursor1);
                self.key_cursor1 += 1;
                effort = (self.result.updates.len() - starting_updates) as isize;
            }
            while self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.copy_key(&source2.storage, self.key_cursor2);
                self.key_cursor2 += 1;
                effort = (self.result.updates.len() - starting_updates) as isize;
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
                    self.result.vals.copy(source.vals.index(lower));
                }
                lower += 1;
            }            

            // If we have pushed any values, copy the key as well.
            if self.result.vals.len() > init_vals {
                self.result.keys.copy(source.keys.index(cursor));
                self.result.keys_offs.push(self.result.vals.len().try_into().ok().unwrap());
            }           
        }
        /// Merge the next key in each of `source1` and `source2` into `self`, updating the appropriate cursors.
        ///
        /// This method only merges a single key. It applies all compaction necessary, and may result in no output
        /// if the updates cancel either directly or after compaction.
        fn merge_key(&mut self, source1: &OrdValStorage<L>, source2: &OrdValStorage<L>) {
            use ::std::cmp::Ordering;
            match source1.keys.index(self.key_cursor1).cmp(source2.keys.index(self.key_cursor2)) {
                Ordering::Less => { 
                    self.copy_key(source1, self.key_cursor1);
                    self.key_cursor1 += 1;
                },
                Ordering::Equal => {
                    // Keys are equal; must merge all values from both sources for this one key.
                    let (lower1, upper1) = source1.values_for_key(self.key_cursor1);
                    let (lower2, upper2) = source2.values_for_key(self.key_cursor2);
                    if let Some(off) = self.merge_vals((source1, lower1, upper1), (source2, lower2, upper2)) {
                        self.result.keys.copy(source1.keys.index(self.key_cursor1));
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
        ) -> Option<L::KeyOffset> {
            // Capture the initial number of values to determine if the merge was ultimately non-empty.
            let init_vals = self.result.vals.len();
            while lower1 < upper1 && lower2 < upper2 {
                // We compare values, and fold in updates for the lowest values;
                // if they are non-empty post-consolidation, we write the value.
                // We could multi-way merge and it wouldn't be very complicated.
                use ::std::cmp::Ordering;
                match source1.vals.index(lower1).cmp(source2.vals.index(lower2)) {
                    Ordering::Less => { 
                        // Extend stash by updates, with logical compaction applied.
                        self.stash_updates_for_val(source1, lower1);
                        if let Some(off) = self.consolidate_updates() {
                            self.result.vals_offs.push(off);
                            self.result.vals.copy(source1.vals.index(lower1));
                        }
                        lower1 += 1;
                    },
                    Ordering::Equal => {
                        self.stash_updates_for_val(source1, lower1);
                        self.stash_updates_for_val(source2, lower2);
                        if let Some(off) = self.consolidate_updates() {
                            self.result.vals_offs.push(off);
                            self.result.vals.copy(source1.vals.index(lower1));
                        }
                        lower1 += 1;
                        lower2 += 1;
                    },
                    Ordering::Greater => { 
                        // Extend stash by updates, with logical compaction applied.
                        self.stash_updates_for_val(source2, lower2);
                        if let Some(off) = self.consolidate_updates() {
                            self.result.vals_offs.push(off);
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
                    self.result.vals_offs.push(off);
                    self.result.vals.copy(source1.vals.index(lower1));
                }
                lower1 += 1;
            }
            while lower2 < upper2 {
                self.stash_updates_for_val(source2, lower2);
                if let Some(off) = self.consolidate_updates() {
                    self.result.vals_offs.push(off);
                    self.result.vals.copy(source2.vals.index(lower2));
                }
                lower2 += 1;
            }

            // Values being pushed indicate non-emptiness.
            if self.result.vals.len() > init_vals {
                Some(self.result.vals.len().try_into().ok().unwrap())
            } else {
                None
            }
        }

        /// Transfer updates for an indexed value in `source` into `self`, with compaction applied.
        fn stash_updates_for_val(&mut self, source: &OrdValStorage<L>, index: usize) {
            let (lower, upper) = source.updates_for_value(index);
            for i in lower .. upper {
                // NB: Here is where we would need to look back if `lower == upper`.
                let (time, diff) = &source.updates.index(i);
                use lattice::Lattice;
                let mut new_time = time.clone();
                new_time.advance_by(self.description.since().borrow());
                self.update_stash.push((new_time, diff.clone()));
            }
        }

        /// Consolidates `self.updates_stash` and produces the offset to record, if any.
        fn consolidate_updates(&mut self) -> Option<L::ValOffset> {
            use consolidation;
            consolidation::consolidate(&mut self.update_stash);
            if !self.update_stash.is_empty() {
                for item in self.update_stash.drain(..) {
                    self.result.updates.push(item);
                }
                Some(self.result.updates.len().try_into().ok().unwrap())
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
        phantom: std::marker::PhantomData<L>,
    }

    impl<L: Layout> Cursor for OrdValCursor<L> {
        type Key = <L::Target as Update>::Key;
        type Val = <L::Target as Update>::Val;
        type Time = <L::Target as Update>::Time;
        type R = <L::Target as Update>::Diff;

        type Storage = OrdValBatch<L>;

        fn key<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Key { &storage.storage.keys.index(self.key_cursor.try_into().ok().unwrap()) }
        fn val<'a>(&self, storage: &'a Self::Storage) -> &'a Self::Val { &storage.storage.vals.index(self.val_cursor.try_into().ok().unwrap()) }
        fn map_times<L2: FnMut(&Self::Time, &Self::R)>(&mut self, storage: &Self::Storage, mut logic: L2) {
            let (lower, upper) = storage.storage.updates_for_value(self.val_cursor);
            for index in lower .. upper {
                let (time, diff) = &storage.storage.updates.index(index);
                logic(time, diff);
            }
        }
        fn key_valid(&self, storage: &Self::Storage) -> bool { self.key_cursor < storage.storage.keys.len() }
        fn val_valid(&self, storage: &Self::Storage) -> bool { self.val_cursor < storage.storage.values_for_key(self.key_cursor).1 }
        fn step_key(&mut self, storage: &Self::Storage){ 
            self.key_cursor += 1;
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
            else {
                self.key_cursor = storage.storage.keys.len();
            }
        }
        fn seek_key(&mut self, storage: &Self::Storage, key: &Self::Key) { 
            self.key_cursor += storage.storage.keys.advance(self.key_cursor, storage.storage.keys.len(), |x| x.lt(key));
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
        }
        fn step_val(&mut self, storage: &Self::Storage) {
            self.val_cursor += 1; 
            if !self.val_valid(storage) {
                self.val_cursor = storage.storage.values_for_key(self.key_cursor).1;
            }
        }
        fn seek_val(&mut self, storage: &Self::Storage, val: &Self::Val) { 
            self.val_cursor += storage.storage.vals.advance(self.val_cursor, storage.storage.values_for_key(self.key_cursor).1, |x| x.lt(val));
        }
        fn rewind_keys(&mut self, storage: &Self::Storage) { 
            self.key_cursor = 0;
            if self.key_valid(storage) {
                self.rewind_vals(storage)
            }
        }
        fn rewind_vals(&mut self, storage: &Self::Storage) { 
            self.val_cursor = storage.storage.values_for_key(self.key_cursor).0;
        }
    }

    /// A builder for creating layers from unsorted update tuples.
    pub struct OrdValBuilder<L: Layout> {
        result: OrdValStorage<L>,
    }

    impl<L: Layout> Builder<OrdValBatch<L>> for OrdValBuilder<L> {

        fn new() -> Self { Self::with_capacity(0) }
        fn with_capacity(cap: usize) -> Self {
            // We don't introduce zero offsets as they will be introduced by the first `push` call.
            Self { 
                result: OrdValStorage {
                    keys: L::KeyContainer::with_capacity(cap),
                    keys_offs: Vec::with_capacity(cap),
                    vals: L::ValContainer::with_capacity(cap),
                    vals_offs: Vec::with_capacity(cap),
                    updates: L::UpdContainer::with_capacity(cap),
                } 
            }
        }

        #[inline]
        fn push(&mut self, (key, val, time, diff): (<L::Target as Update>::Key, <L::Target as Update>::Val, <L::Target as Update>::Time, <L::Target as Update>::Diff)) {

            // Perhaps this is a continuation of an already received key.
            if self.result.keys.last() == Some(&key) {
                // Perhaps this is a continuation of an already received value.
                if self.result.vals.last() == Some(&val) {
                    // TODO: here we could look for repetition, and not push the update in that case.
                    // More logic (and state) would be required to correctly wrangle this.
                    self.result.updates.push((time, diff));
                } else {
                    // New value; complete representation of prior value.
                    self.result.vals_offs.push(self.result.updates.len().try_into().ok().unwrap());
                    self.result.updates.push((time, diff));
                    self.result.vals.push(val);
                }
            } else {
                // New key; complete representation of prior key.
                self.result.vals_offs.push(self.result.updates.len().try_into().ok().unwrap());
                self.result.keys_offs.push(self.result.vals.len().try_into().ok().unwrap());
                self.result.updates.push((time, diff));
                self.result.vals.push(val);
                self.result.keys.push(key);
            }
        }

        #[inline]
        fn copy(&mut self, (key, val, time, diff): (&<L::Target as Update>::Key, &<L::Target as Update>::Val, &<L::Target as Update>::Time, &<L::Target as Update>::Diff)) {

            // Perhaps this is a continuation of an already received key.
            if self.result.keys.last() == Some(key) {
                // Perhaps this is a continuation of an already received value.
                if self.result.vals.last() == Some(val) {
                    // TODO: here we could look for repetition, and not push the update in that case.
                    // More logic (and state) would be required to correctly wrangle this.
                    self.result.updates.push((time.clone(), diff.clone()));
                } else {
                    // New value; complete representation of prior value.
                    self.result.vals_offs.push(self.result.updates.len().try_into().ok().unwrap());
                    self.result.updates.push((time.clone(), diff.clone()));
                    self.result.vals.copy(val);
                }
            } else {
                // New key; complete representation of prior key.
                self.result.vals_offs.push(self.result.updates.len().try_into().ok().unwrap());
                self.result.keys_offs.push(self.result.vals.len().try_into().ok().unwrap());
                self.result.updates.push((time.clone(), diff.clone()));
                self.result.vals.copy(val);
                self.result.keys.copy(key);
            }
        }

        #[inline(never)]
        fn done(mut self, lower: Antichain<<L::Target as Update>::Time>, upper: Antichain<<L::Target as Update>::Time>, since: Antichain<<L::Target as Update>::Time>) -> OrdValBatch<L> {
            // Record the final offsets
            self.result.keys_offs.push(self.result.vals.len().try_into().ok().unwrap());
            self.result.vals_offs.push(self.result.updates.len().try_into().ok().unwrap());

            OrdValBatch {
                storage: self.result,
                description: Description::new(lower, upper, since),
            }
        }
    }

}

mod key_batch {

    // Copy the above, once it works!

}