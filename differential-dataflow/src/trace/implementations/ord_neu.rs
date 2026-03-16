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

use crate::trace::implementations::chunker::VecChunker;
use crate::trace::implementations::spine_fueled::Spine;
use crate::trace::implementations::merge_batcher::{MergeBatcher, VecMerger};
use crate::trace::rc_blanket_impls::RcBuilder;

use super::{Update, Data, Coltainer, OffsetList};

pub use self::val_batch::{OrdValBatch, OrdValBuilder};
pub use self::key_batch::{OrdKeyBatch, OrdKeyBuilder};

/// A trace implementation using a spine of ordered lists.
pub type OrdValSpine<K, V, T, R> = Spine<Rc<OrdValBatch<((K,V),T,R)>>>;
/// A batcher using ordered lists.
pub type OrdValBatcher<K, V, T, R> = MergeBatcher<Vec<((K,V),T,R)>, VecChunker<((K,V),T,R)>, VecMerger<(K, V), T, R>>;
/// A builder using ordered lists.
pub type RcOrdValBuilder<K, V, T, R> = RcBuilder<OrdValBuilder<((K,V),T,R)>>;

/// A trace implementation using a spine of ordered lists.
pub type OrdKeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<((K,()),T,R)>>>;
/// A batcher for ordered lists.
pub type OrdKeyBatcher<K, T, R> = MergeBatcher<Vec<((K,()),T,R)>, VecChunker<((K,()),T,R)>, VecMerger<(K, ()), T, R>>;
/// A builder for ordered lists.
pub type RcOrdKeyBuilder<K, T, R> = RcBuilder<OrdKeyBuilder<((K,()),T,R)>>;

pub use layers::{Vals, Upds};
/// Layers are containers of lists of some type.
///
/// The intent is that they "attach" to an outer layer which has as many values
/// as the layer has lists, thereby associating a list with each outer value.
/// A sequence of layers, each matching the number of values in its predecessor,
/// forms a layered trie: a tree with values of some type on nodes at each depth.
///
/// We will form tries here by layering `[Keys, Vals, Upds]` or `[Keys, Upds]`.
pub mod layers {

    use serde::{Deserialize, Serialize};
    use crate::trace::implementations::BatchContainer;

    /// A container for non-empty lists of values.
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Vals<O, V> {
        /// Offsets used to provide indexes from keys to values.
        ///
        /// The length of this list is one longer than `keys`, so that we can avoid bounds logic.
        pub offs: O,
        /// Concatenated ordered lists of values, bracketed by offsets in `offs`.
        pub vals: V,
    }

    impl<O: for<'a> BatchContainer<ReadItem<'a> = usize>, V: BatchContainer> Default for Vals<O, V> {
        fn default() -> Self { Self::with_capacity(0, 0) }
    }

    impl<O: for<'a> BatchContainer<ReadItem<'a> = usize>, V: BatchContainer> Vals<O, V> {
        /// Lower and upper bounds in `self.vals` of the indexed list.
        #[inline(always)] pub fn bounds(&self, index: usize) -> (usize, usize) {
            (self.offs.index(index), self.offs.index(index+1))
        }
        /// Retrieves a value using relative indexes.
        pub fn get_rel(&self, list_idx: usize, item_idx: usize) -> V::ReadItem<'_> {
            self.get_abs(self.bounds(list_idx).0 + item_idx)
        }

        /// Number of lists in the container.
        pub fn len(&self) -> usize { self.offs.len() - 1 }
        /// Retrieves a value using an absolute rather than relative index.
        pub fn get_abs(&self, index: usize) -> V::ReadItem<'_> {
            self.vals.index(index)
        }
        /// Allocates with capacities for a number of lists and values.
        pub fn with_capacity(o_size: usize, v_size: usize) -> Self {
            let mut offs = <O as BatchContainer>::with_capacity(o_size);
            offs.push_ref(0);
            Self {
                offs,
                vals: <V as BatchContainer>::with_capacity(v_size),
            }
        }
        /// Allocates with enough capacity to contain two inputs.
        pub fn merge_capacity(this: &Self, that: &Self) -> Self {
            let mut offs = <O as BatchContainer>::with_capacity(this.offs.len() + that.offs.len());
            offs.push_ref(0);
            Self {
                offs,
                vals: <V as BatchContainer>::merge_capacity(&this.vals, &that.vals),
            }
        }
    }

    /// A container for non-empty lists of updates.
    ///
    /// This container uses the special representiation of an empty slice to stand in for
    /// "the previous single element". An empty slice is an otherwise invalid representation.
    #[derive(Debug, Serialize, Deserialize)]
    pub struct Upds<O, T, D> {
        /// Offsets used to provide indexes from values to updates.
        pub offs: O,
        /// Concatenated ordered lists of update times, bracketed by offsets in `offs`.
        pub times: T,
        /// Concatenated ordered lists of update diffs, bracketed by offsets in `offs`.
        pub diffs: D,
    }

    impl<O: for<'a> BatchContainer<ReadItem<'a> = usize>, T: BatchContainer, D: BatchContainer> Default for Upds<O, T, D> {
        fn default() -> Self { Self::with_capacity(0, 0) }
    }
    impl<O: for<'a> BatchContainer<ReadItem<'a> = usize>, T: BatchContainer, D: BatchContainer> Upds<O, T, D> {
        /// Lower and upper bounds in `self.times` and `self.diffs` of the indexed list.
        pub fn bounds(&self, index: usize) -> (usize, usize) {
            let mut lower = self.offs.index(index);
            let upper = self.offs.index(index+1);
            // We use equal lower and upper to encode "singleton update; just before here".
            if lower == upper {
                assert!(lower > 0);
                lower -= 1;
            }
            (lower, upper)
        }
        /// Retrieves a value using relative indexes.
        pub fn get_rel(&self, list_idx: usize, item_idx: usize) -> (T::ReadItem<'_>, D::ReadItem<'_>) {
            self.get_abs(self.bounds(list_idx).0 + item_idx)
        }

        /// Number of lists in the container.
        pub fn len(&self) -> usize { self.offs.len() - 1 }
        /// Retrieves a value using an absolute rather than relative index.
        pub fn get_abs(&self, index: usize) -> (T::ReadItem<'_>, D::ReadItem<'_>) {
            (self.times.index(index), self.diffs.index(index))
        }
        /// Allocates with capacities for a number of lists and values.
        pub fn with_capacity(o_size: usize, u_size: usize) -> Self {
            let mut offs = <O as BatchContainer>::with_capacity(o_size);
            offs.push_ref(0);
            Self {
                offs,
                times: <T as BatchContainer>::with_capacity(u_size),
                diffs: <D as BatchContainer>::with_capacity(u_size),
            }
        }
        /// Allocates with enough capacity to contain two inputs.
        pub fn merge_capacity(this: &Self, that: &Self) -> Self {
            let mut offs = <O as BatchContainer>::with_capacity(this.offs.len() + that.offs.len());
            offs.push_ref(0);
            Self {
                offs,
                times: <T as BatchContainer>::merge_capacity(&this.times, &that.times),
                diffs: <D as BatchContainer>::merge_capacity(&this.diffs, &that.diffs),
            }
        }
    }

    /// Helper type for constructing `Upds` containers.
    pub struct UpdsBuilder<T: BatchContainer, D: BatchContainer> {
        stash: Vec<(T::Owned, D::Owned)>,
        total: usize,
        time_con: T,
        diff_con: D,
    }

    impl<T: BatchContainer, D: BatchContainer> Default for UpdsBuilder<T, D> {
        fn default() -> Self { Self { stash: Vec::default(), total: 0, time_con: BatchContainer::with_capacity(1), diff_con: BatchContainer::with_capacity(1) } }
    }

    impl<T, D> UpdsBuilder<T, D>
    where
        T: BatchContainer<Owned: Ord>,
        D: BatchContainer<Owned: crate::difference::Semigroup>,
    {
        /// Stages one update, but does not seal the set of updates.
        pub fn push(&mut self, time: T::Owned, diff: D::Owned) {
            self.stash.push((time, diff));
        }

        /// Consolidate and insert (if non-empty) the stashed updates.
        pub fn seal<O: for<'a> BatchContainer<ReadItem<'a> = usize>>(&mut self, upds: &mut Upds<O, T, D>) -> bool {
            use crate::consolidation;
            consolidation::consolidate(&mut self.stash);
            if self.stash.is_empty() { return false; }
            if self.stash.len() == 1 {
                let (time, diff) = self.stash.last().unwrap();
                self.time_con.clear(); self.time_con.push_own(time);
                self.diff_con.clear(); self.diff_con.push_own(diff);
                if upds.times.last() == self.time_con.get(0) && upds.diffs.last() == self.diff_con.get(0) {
                    self.total += 1;
                    self.stash.clear();
                    upds.offs.push_ref(upds.times.len());
                    return true;
                }
            }
            self.total += self.stash.len();
            for (time, diff) in self.stash.drain(..) {
                upds.times.push_own(&time);
                upds.diffs.push_own(&diff);
            }
            upds.offs.push_ref(upds.times.len());
            true
        }

        /// Completes the building and returns the total updates sealed.
        pub fn total(&self) -> usize { self.total }
    }
}

/// Types related to forming batches with values.
pub mod val_batch {

    use std::marker::PhantomData;
    use timely::container::PushInto;
    use timely::progress::{Antichain, frontier::AntichainRef};

    use crate::trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
    use crate::trace::implementations::{BatchContainer, Data, Coltainer, OffsetList};
    use super::{Update, Vals, Upds, layers::UpdsBuilder};

    /// Storage for an ordered collection of `(key, val, time, diff)` updates.
    pub struct OrdValStorage<U: Update> {
        /// An ordered list of keys.
        pub keys: Coltainer<U::Key>,
        /// For each key in `keys`, a list of values.
        pub vals: Vals<OffsetList, Coltainer<U::Val>>,
        /// For each val in `vals`, a list of (time, diff) updates.
        pub upds: Upds<OffsetList, Coltainer<U::Time>, Coltainer<U::Diff>>,
    }

    // Manual impls because derive can't handle the Update bound well.
    impl<U: Update> Clone for OrdValStorage<U> {
        fn clone(&self) -> Self {
            Self { keys: self.keys.clone(), vals: Vals { offs: self.vals.offs.clone(), vals: self.vals.vals.clone() }, upds: Upds { offs: self.upds.offs.clone(), times: self.upds.times.clone(), diffs: self.upds.diffs.clone() } }
        }
    }
    impl<U: Update> Default for OrdValStorage<U> {
        fn default() -> Self {
            Self {
                keys: Default::default(),
                vals: Default::default(),
                upds: Default::default(),
            }
        }
    }

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    pub struct OrdValBatch<U: Update> {
        /// The updates themselves.
        pub storage: OrdValStorage<U>,
        /// Description of the update times this layer represents.
        pub description: Description<U::Time>,
        /// The number of updates reflected in the batch.
        pub updates: usize,
    }

    impl<U: Update> Clone for OrdValBatch<U> {
        fn clone(&self) -> Self {
            Self { storage: self.storage.clone(), description: self.description.clone(), updates: self.updates }
        }
    }

    impl<U: Update> BatchReader for OrdValBatch<U> {
        type Key = U::Key;
        type Val = U::Val;
        type Time = U::Time;
        type Diff = U::Diff;

        type Cursor = OrdValCursor<U>;
        fn cursor(&self) -> Self::Cursor {
            OrdValCursor {
                key_cursor: 0,
                val_cursor: 0,
                phantom: PhantomData,
            }
        }
        fn len(&self) -> usize { self.updates }
        fn description(&self) -> &Description<U::Time> { &self.description }
    }

    impl<U: Update> Batch for OrdValBatch<U> {
        type Merger = OrdValMerger<U>;

        fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<U::Time>) -> Self::Merger {
            OrdValMerger::new(self, other, compaction_frontier)
        }

        fn empty(lower: Antichain<U::Time>, upper: Antichain<U::Time>) -> Self {
            use timely::progress::Timestamp;
            Self {
                storage: OrdValStorage {
                    keys: Coltainer::default(),
                    vals: Default::default(),
                    upds: Default::default(),
                },
                description: Description::new(lower, upper, Antichain::from_elem(U::Time::minimum())),
                updates: 0,
            }
        }
    }

    /// State for an in-progress merge.
    pub struct OrdValMerger<U: Update> {
        key_cursor1: usize,
        key_cursor2: usize,
        result: OrdValStorage<U>,
        description: Description<U::Time>,
        staging: UpdsBuilder<Coltainer<U::Time>, Coltainer<U::Diff>>,
    }

    impl<U: Update> Merger<OrdValBatch<U>> for OrdValMerger<U> {
        fn new(batch1: &OrdValBatch<U>, batch2: &OrdValBatch<U>, compaction_frontier: AntichainRef<U::Time>) -> Self {
            assert!(batch1.upper() == batch2.lower());
            use crate::lattice::Lattice;
            let mut since = batch1.description().since().join(batch2.description().since());
            since = since.join(&compaction_frontier.to_owned());

            let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);
            let batch1 = &batch1.storage;
            let batch2 = &batch2.storage;

            OrdValMerger {
                key_cursor1: 0,
                key_cursor2: 0,
                result: OrdValStorage {
                    keys: Coltainer::merge_capacity(&batch1.keys, &batch2.keys),
                    vals: Vals::merge_capacity(&batch1.vals, &batch2.vals),
                    upds: Upds::merge_capacity(&batch1.upds, &batch2.upds),
                },
                description,
                staging: UpdsBuilder::default(),
            }
        }
        fn done(self) -> OrdValBatch<U> {
            OrdValBatch {
                updates: self.staging.total(),
                storage: self.result,
                description: self.description,
            }
        }
        fn work(&mut self, source1: &OrdValBatch<U>, source2: &OrdValBatch<U>, fuel: &mut isize) {
            let starting_updates = self.staging.total();
            let mut effort = 0isize;

            while self.key_cursor1 < source1.storage.keys.len() && self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.merge_key(&source1.storage, &source2.storage);
                effort = (self.staging.total() - starting_updates) as isize;
            }

            while self.key_cursor1 < source1.storage.keys.len() && effort < *fuel {
                self.copy_key(&source1.storage, self.key_cursor1);
                self.key_cursor1 += 1;
                effort = (self.staging.total() - starting_updates) as isize;
            }
            while self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.copy_key(&source2.storage, self.key_cursor2);
                self.key_cursor2 += 1;
                effort = (self.staging.total() - starting_updates) as isize;
            }

            *fuel -= effort;
        }
    }

    impl<U: Update> OrdValMerger<U> {
        fn copy_key(&mut self, source: &OrdValStorage<U>, cursor: usize) {
            let init_vals = self.result.vals.vals.len();
            let (mut lower, upper) = source.vals.bounds(cursor);
            while lower < upper {
                self.stash_updates_for_val(source, lower);
                if self.staging.seal(&mut self.result.upds) {
                    self.result.vals.vals.push_ref(source.vals.get_abs(lower));
                }
                lower += 1;
            }
            if self.result.vals.vals.len() > init_vals {
                self.result.keys.push_ref(source.keys.index(cursor));
                self.result.vals.offs.push_ref(self.result.vals.vals.len());
            }
        }

        fn merge_key(&mut self, source1: &OrdValStorage<U>, source2: &OrdValStorage<U>) {
            use ::std::cmp::Ordering;
            match source1.keys.index(self.key_cursor1).cmp(&source2.keys.index(self.key_cursor2)) {
                Ordering::Less => {
                    self.copy_key(source1, self.key_cursor1);
                    self.key_cursor1 += 1;
                },
                Ordering::Equal => {
                    let (lower1, upper1) = source1.vals.bounds(self.key_cursor1);
                    let (lower2, upper2) = source2.vals.bounds(self.key_cursor2);
                    if let Some(off) = self.merge_vals((source1, lower1, upper1), (source2, lower2, upper2)) {
                        self.result.keys.push_ref(source1.keys.index(self.key_cursor1));
                        self.result.vals.offs.push_ref(off);
                    }
                    self.key_cursor1 += 1;
                    self.key_cursor2 += 1;
                },
                Ordering::Greater => {
                    self.copy_key(source2, self.key_cursor2);
                    self.key_cursor2 += 1;
                },
            }
        }

        fn merge_vals(
            &mut self,
            (source1, mut lower1, upper1): (&OrdValStorage<U>, usize, usize),
            (source2, mut lower2, upper2): (&OrdValStorage<U>, usize, usize),
        ) -> Option<usize> {
            let init_vals = self.result.vals.vals.len();
            while lower1 < upper1 && lower2 < upper2 {
                use ::std::cmp::Ordering;
                match source1.vals.get_abs(lower1).cmp(&source2.vals.get_abs(lower2)) {
                    Ordering::Less => {
                        self.stash_updates_for_val(source1, lower1);
                        if self.staging.seal(&mut self.result.upds) {
                            self.result.vals.vals.push_ref(source1.vals.get_abs(lower1));
                        }
                        lower1 += 1;
                    },
                    Ordering::Equal => {
                        self.stash_updates_for_val(source1, lower1);
                        self.stash_updates_for_val(source2, lower2);
                        if self.staging.seal(&mut self.result.upds) {
                            self.result.vals.vals.push_ref(source1.vals.get_abs(lower1));
                        }
                        lower1 += 1;
                        lower2 += 1;
                    },
                    Ordering::Greater => {
                        self.stash_updates_for_val(source2, lower2);
                        if self.staging.seal(&mut self.result.upds) {
                            self.result.vals.vals.push_ref(source2.vals.get_abs(lower2));
                        }
                        lower2 += 1;
                    },
                }
            }
            while lower1 < upper1 {
                self.stash_updates_for_val(source1, lower1);
                if self.staging.seal(&mut self.result.upds) {
                    self.result.vals.vals.push_ref(source1.vals.get_abs(lower1));
                }
                lower1 += 1;
            }
            while lower2 < upper2 {
                self.stash_updates_for_val(source2, lower2);
                if self.staging.seal(&mut self.result.upds) {
                    self.result.vals.vals.push_ref(source2.vals.get_abs(lower2));
                }
                lower2 += 1;
            }

            if self.result.vals.vals.len() > init_vals {
                Some(self.result.vals.vals.len())
            } else {
                None
            }
        }

        fn stash_updates_for_val(&mut self, source: &OrdValStorage<U>, index: usize) {
            let (lower, upper) = source.upds.bounds(index);
            for i in lower .. upper {
                let (time, diff) = source.upds.get_abs(i);
                use crate::lattice::Lattice;
                let mut new_time: U::Time = <U::Time as columnar::Columnar>::into_owned(time);
                new_time.advance_by(self.description.since().borrow());
                self.staging.push(new_time, <U::Diff as columnar::Columnar>::into_owned(diff));
            }
        }
    }

    /// A cursor for navigating a single layer.
    pub struct OrdValCursor<U: Update> {
        key_cursor: usize,
        val_cursor: usize,
        phantom: PhantomData<U>,
    }

    impl<U: Update> Cursor for OrdValCursor<U> {
        type Key = U::Key;
        type Val = U::Val;
        type Time = U::Time;
        type Diff = U::Diff;
        type Storage = OrdValBatch<U>;

        fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Key>> { storage.storage.keys.get(self.key_cursor) }
        fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Val>> { if self.val_valid(storage) { Some(self.val(storage)) } else { None } }

        fn key<'a>(&self, storage: &'a OrdValBatch<U>) -> columnar::Ref<'a, Self::Key> { storage.storage.keys.index(self.key_cursor) }
        fn val<'a>(&self, storage: &'a OrdValBatch<U>) -> columnar::Ref<'a, Self::Val> { storage.storage.vals.get_abs(self.val_cursor) }
        fn map_times<L2: FnMut(columnar::Ref<'_, Self::Time>, columnar::Ref<'_, Self::Diff>)>(&mut self, storage: &OrdValBatch<U>, mut logic: L2) {
            let (lower, upper) = storage.storage.upds.bounds(self.val_cursor);
            for index in lower .. upper {
                let (time, diff) = storage.storage.upds.get_abs(index);
                logic(time, diff);
            }
        }
        fn key_valid(&self, storage: &OrdValBatch<U>) -> bool { self.key_cursor < storage.storage.keys.len() }
        fn val_valid(&self, storage: &OrdValBatch<U>) -> bool { self.val_cursor < storage.storage.vals.bounds(self.key_cursor).1 }
        fn step_key(&mut self, storage: &OrdValBatch<U>){
            self.key_cursor += 1;
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
            else {
                self.key_cursor = storage.storage.keys.len();
            }
        }
        fn seek_key(&mut self, storage: &OrdValBatch<U>, key: columnar::Ref<'_, Self::Key>) {
            self.key_cursor += storage.storage.keys.advance(self.key_cursor, storage.storage.keys.len(), |x| Coltainer::<U::Key>::reborrow(x).lt(&Coltainer::<U::Key>::reborrow(key)));
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
        }
        fn step_val(&mut self, storage: &OrdValBatch<U>) {
            self.val_cursor += 1;
            if !self.val_valid(storage) {
                self.val_cursor = storage.storage.vals.bounds(self.key_cursor).1;
            }
        }
        fn seek_val(&mut self, storage: &OrdValBatch<U>, val: columnar::Ref<'_, Self::Val>) {
            self.val_cursor += storage.storage.vals.vals.advance(self.val_cursor, storage.storage.vals.bounds(self.key_cursor).1, |x| Coltainer::<U::Val>::reborrow(x).lt(&Coltainer::<U::Val>::reborrow(val)));
        }
        fn rewind_keys(&mut self, storage: &OrdValBatch<U>) {
            self.key_cursor = 0;
            if self.key_valid(storage) {
                self.rewind_vals(storage)
            }
        }
        fn rewind_vals(&mut self, storage: &OrdValBatch<U>) {
            self.val_cursor = storage.storage.vals.bounds(self.key_cursor).0;
        }
    }

    /// A builder for creating layers from sorted update tuples.
    pub struct OrdValBuilder<U: Update> {
        /// The in-progress result.
        pub result: OrdValStorage<U>,
        staging: UpdsBuilder<Coltainer<U::Time>, Coltainer<U::Diff>>,
    }

    impl<U: Update> Builder for OrdValBuilder<U> {
        type Input = Vec<((U::Key, U::Val), U::Time, U::Diff)>;
        type Time = U::Time;
        type Output = OrdValBatch<U>;

        fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
            Self {
                result: OrdValStorage {
                    keys: Coltainer::with_capacity(keys),
                    vals: Vals::with_capacity(keys + 1, vals),
                    upds: Upds::with_capacity(vals + 1, upds),
                },
                staging: UpdsBuilder::default(),
            }
        }

        #[inline]
        fn push(&mut self, chunk: &mut Self::Input) {
            for ((key, val), time, diff) in chunk.drain(..) {
                if self.result.keys.is_empty() {
                    self.result.keys.push_own(&key);
                    self.result.vals.vals.push_own(&val);
                    self.staging.push(time, diff);
                }
                else if self.result.keys.last().map(|k| Coltainer::<U::Key>::into_owned(k) == key).unwrap_or(false) {
                    if self.result.vals.vals.last().map(|v| Coltainer::<U::Val>::into_owned(v) == val).unwrap_or(false) {
                        self.staging.push(time, diff);
                    } else {
                        self.staging.seal(&mut self.result.upds);
                        self.staging.push(time, diff);
                        self.result.vals.vals.push_own(&val);
                    }
                } else {
                    self.staging.seal(&mut self.result.upds);
                    self.staging.push(time, diff);
                    self.result.vals.offs.push_ref(self.result.vals.vals.len());
                    self.result.vals.vals.push_own(&val);
                    self.result.keys.push_own(&key);
                }
            }
        }

        #[inline(never)]
        fn done(mut self, description: Description<Self::Time>) -> OrdValBatch<U> {
            self.staging.seal(&mut self.result.upds);
            self.result.vals.offs.push_ref(self.result.vals.vals.len());
            OrdValBatch {
                updates: self.staging.total(),
                storage: self.result,
                description,
            }
        }

        fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
            let mut keys = 0;
            let mut vals = 0;
            let mut upds = 0;
            for link in chain.iter() {
                let mut prev_keyval = None;
                for ((key, val), _, _) in link.iter() {
                    if let Some((p_key, p_val)) = prev_keyval {
                        if p_key != key { keys += 1; vals += 1; }
                        else if p_val != val { vals += 1; }
                    } else { keys += 1; vals += 1; }
                    upds += 1;
                    prev_keyval = Some((key, val));
                }
            }
            let mut builder = Self::with_capacity(keys, vals, upds);
            for mut chunk in chain.drain(..) {
                builder.push(&mut chunk);
            }
            builder.done(description)
        }
    }
}

/// Types related to forming batches of keys.
pub mod key_batch {

    use std::marker::PhantomData;
    use timely::container::PushInto;
    use timely::progress::{Antichain, frontier::AntichainRef};

    use crate::trace::{Batch, BatchReader, Builder, Cursor, Description, Merger};
    use crate::trace::implementations::{BatchContainer, Data, Coltainer, OffsetList};
    use super::{Update, Upds, layers::UpdsBuilder};

    /// Storage for an ordered collection of `(key, time, diff)` updates.
    pub struct OrdKeyStorage<U: Update> {
        /// An ordered list of keys.
        pub keys: Coltainer<U::Key>,
        /// For each key in `keys`, a list of (time, diff) updates.
        pub upds: Upds<OffsetList, Coltainer<U::Time>, Coltainer<U::Diff>>,
    }

    impl<U: Update> Clone for OrdKeyStorage<U> {
        fn clone(&self) -> Self {
            Self { keys: self.keys.clone(), upds: Upds { offs: self.upds.offs.clone(), times: self.upds.times.clone(), diffs: self.upds.diffs.clone() } }
        }
    }
    impl<U: Update> Default for OrdKeyStorage<U> {
        fn default() -> Self { Self { keys: Default::default(), upds: Default::default() } }
    }

    /// An immutable collection of update tuples, from a contiguous interval of logical times.
    pub struct OrdKeyBatch<U: Update> {
        /// The updates themselves.
        pub storage: OrdKeyStorage<U>,
        /// Description of the update times this layer represents.
        pub description: Description<U::Time>,
        /// The number of updates reflected in the batch.
        pub updates: usize,
        /// Single value to return if asked (for key-only batches, val is always `()`).
        pub value: Coltainer<U::Val>,
    }

    impl<U: Update> Clone for OrdKeyBatch<U> {
        fn clone(&self) -> Self {
            Self { storage: self.storage.clone(), description: self.description.clone(), updates: self.updates, value: self.value.clone() }
        }
    }

    impl<U: Update<Val: Default>> OrdKeyBatch<U> {
        /// Creates a container with one value, to slot in to `self.value`.
        pub fn create_value() -> Coltainer<U::Val> {
            let mut value = Coltainer::<U::Val>::with_capacity(1);
            value.push_own(&Default::default());
            value
        }
    }

    impl<U: Update<Val: Default>> BatchReader for OrdKeyBatch<U> {
        type Key = U::Key;
        type Val = U::Val;
        type Time = U::Time;
        type Diff = U::Diff;

        type Cursor = OrdKeyCursor<U>;
        fn cursor(&self) -> Self::Cursor {
            OrdKeyCursor {
                key_cursor: 0,
                val_stepped: false,
                phantom: PhantomData,
            }
        }
        fn len(&self) -> usize { self.updates }
        fn description(&self) -> &Description<U::Time> { &self.description }
    }

    impl<U: Update<Val: Default>> Batch for OrdKeyBatch<U> {
        type Merger = OrdKeyMerger<U>;

        fn begin_merge(&self, other: &Self, compaction_frontier: AntichainRef<U::Time>) -> Self::Merger {
            OrdKeyMerger::new(self, other, compaction_frontier)
        }

        fn empty(lower: Antichain<U::Time>, upper: Antichain<U::Time>) -> Self {
            use timely::progress::Timestamp;
            Self {
                storage: OrdKeyStorage::default(),
                description: Description::new(lower, upper, Antichain::from_elem(U::Time::minimum())),
                updates: 0,
                value: Self::create_value(),
            }
        }
    }

    /// State for an in-progress merge.
    pub struct OrdKeyMerger<U: Update> {
        key_cursor1: usize,
        key_cursor2: usize,
        result: OrdKeyStorage<U>,
        description: Description<U::Time>,
        staging: UpdsBuilder<Coltainer<U::Time>, Coltainer<U::Diff>>,
    }

    impl<U: Update<Val: Default>> Merger<OrdKeyBatch<U>> for OrdKeyMerger<U> {
        fn new(batch1: &OrdKeyBatch<U>, batch2: &OrdKeyBatch<U>, compaction_frontier: AntichainRef<U::Time>) -> Self {
            assert!(batch1.upper() == batch2.lower());
            use crate::lattice::Lattice;
            let mut since = batch1.description().since().join(batch2.description().since());
            since = since.join(&compaction_frontier.to_owned());
            let description = Description::new(batch1.lower().clone(), batch2.upper().clone(), since);
            let batch1 = &batch1.storage;
            let batch2 = &batch2.storage;

            OrdKeyMerger {
                key_cursor1: 0,
                key_cursor2: 0,
                result: OrdKeyStorage {
                    keys: Coltainer::merge_capacity(&batch1.keys, &batch2.keys),
                    upds: Upds::merge_capacity(&batch1.upds, &batch2.upds),
                },
                description,
                staging: UpdsBuilder::default(),
            }
        }
        fn done(self) -> OrdKeyBatch<U> {
            OrdKeyBatch {
                updates: self.staging.total(),
                storage: self.result,
                description: self.description,
                value: OrdKeyBatch::<U>::create_value(),
            }
        }
        fn work(&mut self, source1: &OrdKeyBatch<U>, source2: &OrdKeyBatch<U>, fuel: &mut isize) {
            let starting_updates = self.staging.total();
            let mut effort = 0isize;

            while self.key_cursor1 < source1.storage.keys.len() && self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.merge_key(&source1.storage, &source2.storage);
                effort = (self.staging.total() - starting_updates) as isize;
            }

            while self.key_cursor1 < source1.storage.keys.len() && effort < *fuel {
                self.copy_key(&source1.storage, self.key_cursor1);
                self.key_cursor1 += 1;
                effort = (self.staging.total() - starting_updates) as isize;
            }
            while self.key_cursor2 < source2.storage.keys.len() && effort < *fuel {
                self.copy_key(&source2.storage, self.key_cursor2);
                self.key_cursor2 += 1;
                effort = (self.staging.total() - starting_updates) as isize;
            }

            *fuel -= effort;
        }
    }

    impl<U: Update> OrdKeyMerger<U> {
        fn copy_key(&mut self, source: &OrdKeyStorage<U>, cursor: usize) {
            self.stash_updates_for_key(source, cursor);
            if self.staging.seal(&mut self.result.upds) {
                self.result.keys.push_ref(source.keys.index(cursor));
            }
        }

        fn merge_key(&mut self, source1: &OrdKeyStorage<U>, source2: &OrdKeyStorage<U>) {
            use ::std::cmp::Ordering;
            match source1.keys.index(self.key_cursor1).cmp(&source2.keys.index(self.key_cursor2)) {
                Ordering::Less => {
                    self.copy_key(source1, self.key_cursor1);
                    self.key_cursor1 += 1;
                },
                Ordering::Equal => {
                    self.stash_updates_for_key(source1, self.key_cursor1);
                    self.stash_updates_for_key(source2, self.key_cursor2);
                    if self.staging.seal(&mut self.result.upds) {
                        self.result.keys.push_ref(source1.keys.index(self.key_cursor1));
                    }
                    self.key_cursor1 += 1;
                    self.key_cursor2 += 1;
                },
                Ordering::Greater => {
                    self.copy_key(source2, self.key_cursor2);
                    self.key_cursor2 += 1;
                },
            }
        }

        fn stash_updates_for_key(&mut self, source: &OrdKeyStorage<U>, index: usize) {
            let (lower, upper) = source.upds.bounds(index);
            for i in lower .. upper {
                let (time, diff) = source.upds.get_abs(i);
                use crate::lattice::Lattice;
                let mut new_time: U::Time = <U::Time as columnar::Columnar>::into_owned(time);
                new_time.advance_by(self.description.since().borrow());
                self.staging.push(new_time, <U::Diff as columnar::Columnar>::into_owned(diff));
            }
        }
    }

    /// A cursor for navigating a single layer.
    pub struct OrdKeyCursor<U: Update> {
        key_cursor: usize,
        val_stepped: bool,
        phantom: PhantomData<U>,
    }

    impl<U: Update<Val: Default>> Cursor for OrdKeyCursor<U> {
        type Key = U::Key;
        type Val = U::Val;
        type Time = U::Time;
        type Diff = U::Diff;
        type Storage = OrdKeyBatch<U>;

        fn get_key<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Key>> { storage.storage.keys.get(self.key_cursor) }
        fn get_val<'a>(&self, storage: &'a Self::Storage) -> Option<columnar::Ref<'a, Self::Val>> { if self.val_valid(storage) { Some(self.val(storage)) } else { None } }

        fn key<'a>(&self, storage: &'a OrdKeyBatch<U>) -> columnar::Ref<'a, Self::Key> { storage.storage.keys.index(self.key_cursor) }
        fn val<'a>(&self, storage: &'a OrdKeyBatch<U>) -> columnar::Ref<'a, Self::Val> { storage.value.index(0) }
        fn map_times<L2: FnMut(columnar::Ref<'_, Self::Time>, columnar::Ref<'_, Self::Diff>)>(&mut self, storage: &OrdKeyBatch<U>, mut logic: L2) {
            let (lower, upper) = storage.storage.upds.bounds(self.key_cursor);
            for index in lower .. upper {
                let (time, diff) = storage.storage.upds.get_abs(index);
                logic(time, diff);
            }
        }
        fn key_valid(&self, storage: &OrdKeyBatch<U>) -> bool { self.key_cursor < storage.storage.keys.len() }
        fn val_valid(&self, _storage: &OrdKeyBatch<U>) -> bool { !self.val_stepped }
        fn step_key(&mut self, storage: &OrdKeyBatch<U>){
            self.key_cursor += 1;
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
            else {
                self.key_cursor = storage.storage.keys.len();
            }
        }
        fn seek_key(&mut self, storage: &OrdKeyBatch<U>, key: columnar::Ref<'_, Self::Key>) {
            self.key_cursor += storage.storage.keys.advance(self.key_cursor, storage.storage.keys.len(), |x| Coltainer::<U::Key>::reborrow(x).lt(&Coltainer::<U::Key>::reborrow(key)));
            if self.key_valid(storage) {
                self.rewind_vals(storage);
            }
        }
        fn step_val(&mut self, _storage: &OrdKeyBatch<U>) {
            self.val_stepped = true;
        }
        fn seek_val(&mut self, _storage: &OrdKeyBatch<U>, _val: columnar::Ref<'_, Self::Val>) { }
        fn rewind_keys(&mut self, storage: &OrdKeyBatch<U>) {
            self.key_cursor = 0;
            if self.key_valid(storage) {
                self.rewind_vals(storage)
            }
        }
        fn rewind_vals(&mut self, _storage: &OrdKeyBatch<U>) {
            self.val_stepped = false;
        }
    }

    /// A builder for creating layers from sorted update tuples.
    pub struct OrdKeyBuilder<U: Update> {
        /// The in-progress result.
        pub result: OrdKeyStorage<U>,
        staging: UpdsBuilder<Coltainer<U::Time>, Coltainer<U::Diff>>,
    }

    impl<U: Update<Val: Default>> Builder for OrdKeyBuilder<U> {
        type Input = Vec<((U::Key, U::Val), U::Time, U::Diff)>;
        type Time = U::Time;
        type Output = OrdKeyBatch<U>;

        fn with_capacity(keys: usize, _vals: usize, upds: usize) -> Self {
            Self {
                result: OrdKeyStorage {
                    keys: Coltainer::with_capacity(keys),
                    upds: Upds::with_capacity(keys+1, upds),
                },
                staging: UpdsBuilder::default(),
            }
        }

        #[inline]
        fn push(&mut self, chunk: &mut Self::Input) {
            for ((key, _val), time, diff) in chunk.drain(..) {
                if self.result.keys.is_empty() {
                    self.result.keys.push_own(&key);
                    self.staging.push(time, diff);
                }
                else if self.result.keys.last().map(|k| Coltainer::<U::Key>::into_owned(k) == key).unwrap_or(false) {
                    self.staging.push(time, diff);
                } else {
                    self.staging.seal(&mut self.result.upds);
                    self.staging.push(time, diff);
                    self.result.keys.push_own(&key);
                }
            }
        }

        #[inline(never)]
        fn done(mut self, description: Description<Self::Time>) -> OrdKeyBatch<U> {
            self.staging.seal(&mut self.result.upds);
            OrdKeyBatch {
                updates: self.staging.total(),
                storage: self.result,
                description,
                value: OrdKeyBatch::<U>::create_value(),
            }
        }

        fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
            let mut keys = 0;
            let mut upds = 0;
            for link in chain.iter() {
                let mut prev_key = None;
                for ((key, _), _, _) in link.iter() {
                    if prev_key.map(|pk| pk != key).unwrap_or(true) { keys += 1; }
                    upds += 1;
                    prev_key = Some(key);
                }
            }
            let mut builder = Self::with_capacity(keys, 0, upds);
            for mut chunk in chain.drain(..) {
                builder.push(&mut chunk);
            }
            builder.done(description)
        }
    }
}
