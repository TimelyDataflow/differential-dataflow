//! Columnar arrangement plumbing.
//!
//! - Type aliases (`ValSpine`, `ValBatcher`, `ValBuilder`) glue columnar storage
//!   into DD's trace machinery.
//! - `Coltainer<C>` wraps a columnar `C::Container` as a DD `BatchContainer`.
//! - `TrieChunker` strips `RecordedUpdates` down to `Updates` for the merge batcher.
//! - `batcher` contains required trait stubs for `Updates`.
//! - `trie_merger` is the batch-at-a-time merging logic.
//! - `builder::ValMirror` is the `trace::Builder` that seals melded chunks into
//!   an `OrdValBatch`.

use std::rc::Rc;
use crate::trace::implementations::ord_neu::OrdValBatch;
use crate::trace::rc_blanket_impls::RcBuilder;
use crate::trace::implementations::spine_fueled::Spine;

use super::layout::ColumnarLayout;

pub mod trie_merger;

/// A trace implementation backed by columnar storage.
pub type ValSpine<K, V, T, R> = Spine<Rc<OrdValBatch<ColumnarLayout<(K,V,T,R)>>>>;
/// A batcher for columnar storage.
pub type ValBatcher<K, V, T, R> = ValBatcher2<(K,V,T,R)>;
/// A builder for columnar storage.
pub type ValBuilder<K, V, T, R> = RcBuilder<builder::ValMirror<(K,V,T,R)>>;

/// A batch container implementation for Coltainer<C>.
pub use batch_container::Coltainer;
pub mod batch_container {
    //! [`Coltainer`] wraps a columnar container as a DD [`BatchContainer`].

    use columnar::{Borrow, Columnar, Container, Clear, Push, Index, Len};
    use crate::trace::implementations::BatchContainer;

    /// Container, anchored by `C` to provide an owned type.
    pub struct Coltainer<C: Columnar> {
        /// The underlying columnar container.
        pub container: C::Container,
    }

    impl<C: Columnar> Default for Coltainer<C> {
        fn default() -> Self { Self { container: Default::default() } }
    }

    impl<C: Columnar + Ord + Clone> BatchContainer for Coltainer<C> where for<'a> columnar::Ref<'a, C> : Ord {

        type ReadItem<'a> = columnar::Ref<'a, C>;
        type Owned = C;

        #[inline(always)] fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned { C::into_owned(item) }
        #[inline(always)] fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) { other.copy_from(item) }

        #[inline(always)] fn push_ref(&mut self, item: Self::ReadItem<'_>) { self.container.push(item) }
        #[inline(always)] fn push_own(&mut self, item: &Self::Owned) { self.container.push(item) }

        /// Clears the container. May not release resources.
        fn clear(&mut self) { self.container.clear() }

        /// Creates a new container with sufficient capacity.
        fn with_capacity(_size: usize) -> Self { Self::default() }
        /// Creates a new container with sufficient capacity.
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            Self {
                container: <C as Columnar>::Container::with_capacity_for([cont1.container.borrow(), cont2.container.borrow()].into_iter()),
            }
         }

        /// Converts a read item into one with a narrower lifetime.
        #[inline(always)] fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { columnar::ContainerOf::<C>::reborrow_ref(item) }

        /// Reference to the element at this position.
        #[inline(always)] fn index(&self, index: usize) -> Self::ReadItem<'_> { self.container.borrow().get(index) }

        #[inline(always)] fn len(&self) -> usize { self.container.len() }

        /// Reports the number of elements satisfying the predicate.
        ///
        /// This methods *relies strongly* on the assumption that the predicate
        /// stays false once it becomes false, a joint property of the predicate
        /// and the layout of `Self. This allows `advance` to use exponential search to
        /// count the number of elements in time logarithmic in the result.
        fn advance<F: for<'a> Fn(Self::ReadItem<'a>)->bool>(&self, start: usize, end: usize, function: F) -> usize {

            let borrow = self.container.borrow();

            let small_limit = 8;

            // Exponential search if the answer isn't within `small_limit`.
            if end > start + small_limit && function(borrow.get(start + small_limit)) {

                // start with no advance
                let mut index = small_limit + 1;
                if start + index < end && function(borrow.get(start + index)) {

                    // advance in exponentially growing steps.
                    let mut step = 1;
                    while start + index + step < end && function(borrow.get(start + index + step)) {
                        index += step;
                        step <<= 1;
                    }

                    // advance in exponentially shrinking steps.
                    step >>= 1;
                    while step > 0 {
                        if start + index + step < end && function(borrow.get(start + index + step)) {
                            index += step;
                        }
                        step >>= 1;
                    }

                    index += 1;
                }

                index
            }
            else {
                let limit = std::cmp::min(end, start + small_limit);
                (start .. limit).filter(|x| function(borrow.get(*x))).count()
            }
        }
    }
}

use super::updates::Updates;
use super::RecordedUpdates;
use crate::trace::implementations::merge_batcher::MergeBatcher;
type ValBatcher2<U> = MergeBatcher<RecordedUpdates<U>, TrieChunker<U>, trie_merger::TrieMerger<U>>;

/// A chunker that unwraps `RecordedUpdates` into bare `Updates` for the merge batcher.
///
/// The intended behavior is to produce chunks whose size is within 1-2x `LINK_TARGET`.
/// It ships large batches immediately, accumulates small batches, consolidates as they
/// exceed 2xLINK_TARGET, and ships them unless they drop below 1xLINK_TARGET.
///
/// The flow is into (or around) `self.stage`, then consolidated blocks into `self.ready`,
/// each of which is put in `self.stage`
pub struct TrieChunker<U: super::layout::ColumnarUpdate> {
    /// Insufficiently large updates we haven't figured out how to ship yet.
    blobs: Vec<(Updates<U>, bool)>,
    /// Sum of `len()` across `blobs`.
    blob_records: usize,
    /// Ready-to-emit chunks. Each is sorted and consolidated; size ≥ `LINK_TARGET`
    /// (or smaller, only for the final chunk produced by `finish`).
    ready: std::collections::VecDeque<Updates<U>>,
    /// Staging area for the next pull call.
    stage: Option<Updates<U>>,
}

impl<U: super::layout::ColumnarUpdate> Default for TrieChunker<U> {
    fn default() -> Self {
        Self {
            blobs: Default::default(),
            blob_records: 0,
            ready: Default::default(),
            stage: None,
        }
    }
}

impl<U: super::layout::ColumnarUpdate> TrieChunker<U> {
    /// Consolidate and empty `self.blobs`, into `self.ready` if large enough or else return.
    fn consolidate_blobs(&mut self) -> Updates<U> {
        // Single consolidated entry: pass through, no work.
        if self.blobs.len() == 1 && self.blobs[0].1 {
            let (result, _) = self.blobs.pop().unwrap();
            self.blob_records = 0;
            return result;
        }

        // TODO: Improve consolidation through column-oriented sorts.
        let result = Updates::<U>::form_unsorted(self.blobs.iter().flat_map(|(u, _)| u.iter()));
        self.blobs.clear();
        self.blob_records = 0;
        result
    }

    /// Push a non-empty `Updates` into blobs and update accounting.
    fn absorb(&mut self, updates: Updates<U>, consolidated: bool) {
        self.blob_records += updates.len();
        self.blobs.push((updates, consolidated));
    }
}

impl<'a, U: super::layout::ColumnarUpdate> timely::container::PushInto<&'a mut RecordedUpdates<U>> for TrieChunker<U> {
    fn push_into(&mut self, container: &'a mut RecordedUpdates<U>) {
        // Early return if an empty container (legit, for accountable progress tracking).
        if container.updates.len() == 0 { return; }

        // Our main goal is to only ship links that are 1-2 x LINK_TARGET, using blobs
        // to accumulate updates until they are ready to go or we are asked to finish.
        //
        // Informally, we are aiming to move `container` into or around `self.blobs`.
        // Into if small enough, as we can further consolidate, but if not we need to
        // consolidate and then either ship (if large) or hold (if small) the results.

        let updates = std::mem::take(&mut container.updates);
        let consolidated = container.consolidated;
        let len = updates.len();

        // The input may be ready to ship on its own.
        // This is ideal, if we've used an accumulating container builder elsewhere.
        if consolidated && len >= crate::columnar::LINK_TARGET { self.ready.push_back(updates); }
        // Can move into blobs if the combined length is not too large.
        else if self.blob_records + len < 2 * crate::columnar::LINK_TARGET { self.absorb(updates, consolidated); }
        // Otherwise, we'll need to manage `self.blobs`.
        else {
            // Together `updates` and `self.blobs` exceed 2 * LINK_TARGET.
            // At least one, perhaps both of them, are LINK_TARGET in size.
            // We'll consolidate any that are, and ship or merge the results.
            // We'll end up with at most LINK_TARGET in `self.blobs`, retiring
            // a constant factor of the pending work we started with.

            // Consolidate and move to ready if large; stash otherwise.
            let input_residual = if len >= crate::columnar::LINK_TARGET {
                let cons = if consolidated { updates } else { updates.consolidate() };
                if cons.len() >= crate::columnar::LINK_TARGET { self.ready.push_back(cons); None }
                else if cons.len() > 0 { Some((cons, true)) }
                else { None }
            }
            else { Some((updates, consolidated)) };

            // Consolidate and move to ready if large; stash otherwise.
            let blobs_residual = if self.blob_records >= crate::columnar::LINK_TARGET {
                let cons = self.consolidate_blobs();
                if cons.len() >= crate::columnar::LINK_TARGET { self.ready.push_back(cons); None }
                else if cons.len() > 0 { Some((cons, true)) }
                else { None }
            }
            else { None };

            // Return un-shipped
            if let Some((r, c)) = input_residual { self.absorb(r, c); }
            if let Some((r, c)) = blobs_residual { self.absorb(r, c); }
        }
    }
}

impl<U: super::layout::ColumnarUpdate> timely::container::ContainerBuilder for TrieChunker<U> {
    type Container = Updates<U>;
    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.stage = self.ready.pop_front();
        self.stage.as_mut()
    }
    fn finish(&mut self) -> Option<&mut Self::Container> {
        // Drain whatever's left in blobs as a single (possibly small) final chunk.
        if !self.blobs.is_empty() {
            let cons = self.consolidate_blobs();
            if cons.len() > 0 { self.ready.push_back(cons); }
        }
        self.extract()
    }
}

pub mod batcher {
    //! Batcher trait stubs required to plug `Updates` into DD's merge batcher.

    use columnar::Len;
    use timely::progress::frontier::{Antichain, AntichainRef};
    use crate::trace::implementations::merge_batcher::container::InternalMerge;

    use super::super::layout::ColumnarUpdate as Update;
    use super::super::updates::Updates;

    impl<U: Update> timely::container::SizableContainer for Updates<U> {
        fn at_capacity(&self) -> bool { self.diffs.values.len() >= crate::columnar::LINK_TARGET }
        fn ensure_capacity(&mut self, _stash: &mut Option<Self>) { }
    }

    /// Required by `reduce_abelian`'s bound `Builder::Input: InternalMerge`.
    /// Not called at runtime — our batcher uses `TrieMerger` instead.
    /// TODO: Relax the bound in DD's reduce to remove this requirement.
    impl<U: Update> InternalMerge for Updates<U> {
        type TimeOwned = U::Time;
        fn len(&self) -> usize { unimplemented!() }
        fn clear(&mut self) {
            use columnar::Clear;
            self.keys.clear();
            self.vals.clear();
            self.times.clear();
            self.diffs.clear();
        }
        fn merge_from(&mut self, _others: &mut [Self], _positions: &mut [usize]) { unimplemented!() }
        fn extract(&mut self,
            _position: &mut usize,
            _upper: AntichainRef<U::Time>,
            _frontier: &mut Antichain<U::Time>,
            _keep: &mut Self,
            _ship: &mut Self,
        ) { unimplemented!() }
    }
}

pub mod builder {
    //! [`ValMirror`] trace builder that seals melded chunks into [`OrdValBatch`].

    use crate::trace::implementations::ord_neu::{Vals, Upds};
    use crate::trace::implementations::ord_neu::val_batch::{OrdValBatch, OrdValStorage};
    use crate::trace::Description;

    use super::super::updates::Updates;
    use super::super::layout::ColumnarUpdate as Update;
    use super::super::layout::ColumnarLayout as Layout;
    use super::Coltainer;

    use columnar::{Borrow, IndexAs};
    use columnar::primitive::offsets::Strides;
    use crate::trace::implementations::OffsetList;
    fn strides_to_offset_list(bounds: &Strides, count: usize) -> OffsetList {
        let mut output = OffsetList::with_capacity(count);
        output.push(0);
        let bounds_b = bounds.borrow();
        for i in 0..count {
            output.push(bounds_b.index_as(i) as usize);
        }
        output
    }

    /// Trace [`Builder`](crate::trace::Builder) that accumulates `Updates`
    /// chunks and seals them into a single [`OrdValBatch`].
    pub struct ValMirror<U: Update> {
        chunks: Vec<Updates<U>>,
    }
    impl<U: Update> crate::trace::Builder for ValMirror<U> {
        type Time = U::Time;
        type Input = Updates<U>;
        type Output = OrdValBatch<Layout<U>>;

        fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
            Self { chunks: Vec::new() }
        }
        fn push(&mut self, chunk: &mut Self::Input) {
            if chunk.len() > 0 {
                self.chunks.push(std::mem::take(chunk));
            }
        }
        fn done(self, description: Description<Self::Time>) -> Self::Output {
            let mut chain = self.chunks;
            Self::seal(&mut chain, description)
        }
        fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
            use columnar::Len;

            // Meld sorted, consolidated chain entries in order.
            // Pre-allocate to avoid reallocations during meld.
            use columnar::{Borrow, Container};
            let mut updates = Updates::<U>::default();
            updates.keys.reserve_for(chain.iter().map(|c| c.keys.borrow()));
            updates.vals.reserve_for(chain.iter().map(|c| c.vals.borrow()));
            updates.times.reserve_for(chain.iter().map(|c| c.times.borrow()));
            updates.diffs.reserve_for(chain.iter().map(|c| c.diffs.borrow()));
            let mut builder = super::super::updates::UpdatesBuilder::new_from(updates);
            for chunk in chain.iter() {
                builder.meld(chunk);
            }
            let merged = builder.done();
            chain.clear();

            let updates = Len::len(&merged.diffs.values);
            if updates == 0 {
                let storage = OrdValStorage {
                    keys: Default::default(),
                    vals: Default::default(),
                    upds: Default::default(),
                };
                OrdValBatch { storage, description, updates: 0 }
            } else {
                let val_offs = strides_to_offset_list(&merged.vals.bounds, Len::len(&merged.keys.values));
                let time_offs = strides_to_offset_list(&merged.times.bounds, Len::len(&merged.vals.values));
                let storage = OrdValStorage {
                    keys: Coltainer { container: merged.keys.values },
                    vals: Vals {
                        offs: val_offs,
                        vals: Coltainer { container: merged.vals.values },
                    },
                    upds: Upds {
                        offs: time_offs,
                        times: Coltainer { container: merged.times.values },
                        diffs: Coltainer { container: merged.diffs.values },
                    },
                };
                OrdValBatch { storage, description, updates }
            }
        }
    }
}
