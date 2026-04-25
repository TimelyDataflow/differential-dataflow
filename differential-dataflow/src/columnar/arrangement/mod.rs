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
/// The `records` accounting is discarded here — it has served its purpose for exchange.
///
/// IMPORTANT: This chunker assumes the input `Updates` are sorted and consolidated
/// (as produced by `ValColBuilder::form`). The downstream `InternalMerge` relies on
/// this invariant. If `RecordedUpdates` could carry unsorted data (e.g. from a `map`),
/// we would need either a sorting chunker for that case, or a type-level distinction
/// (e.g. `RecordedUpdates<U, Consolidated>` vs `RecordedUpdates<U, Unsorted>`) to
/// route to the right chunker.
pub struct TrieChunker<U: super::layout::ColumnarUpdate> {
    ready: std::collections::VecDeque<Updates<U>>,
    empty: Option<Updates<U>>,
}

impl<U: super::layout::ColumnarUpdate> Default for TrieChunker<U> {
    fn default() -> Self { Self { ready: Default::default(), empty: None } }
}

impl<'a, U: super::layout::ColumnarUpdate> timely::container::PushInto<&'a mut RecordedUpdates<U>> for TrieChunker<U> {
    fn push_into(&mut self, container: &'a mut RecordedUpdates<U>) {
        let mut updates = std::mem::take(&mut container.updates);
        if !container.consolidated { updates = updates.consolidate(); }
        if updates.len() > 0 { self.ready.push_back(updates); }
    }
}

impl<U: super::layout::ColumnarUpdate> timely::container::ContainerBuilder for TrieChunker<U> {
    type Container = Updates<U>;
    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(ready) = self.ready.pop_front() {
            self.empty = Some(ready);
            self.empty.as_mut()
        } else {
            None
        }
    }
    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.empty = self.ready.pop_front();
        self.empty.as_mut()
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
        fn at_capacity(&self) -> bool { self.diffs.values.len() >= 64 * 1024 }
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
