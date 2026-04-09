//! Columnar container infrastructure for differential dataflow.
//!
//! Provides trie-structured update storage (`Updates`, `RecordedUpdates`),
//! columnar arrangement types (`ValSpine`, `ValBatcher`, `ValBuilder`),
//! container traits for iterative scopes (`Enter`, `Leave`, `Negate`, `ResultsIn`),
//! exchange distribution (`ValPact`), and operators (`join_function`, `leave_dynamic`).
//!
//! Include via `#[path = "columnar_support.rs"] mod columnar_support;`

#![allow(dead_code, unused_imports)]

pub use layout::{ColumnarLayout, ColumnarUpdate};
pub mod layout {

    use std::fmt::Debug;
    use columnar::Columnar;
    use differential_dataflow::trace::implementations::{Layout, OffsetList};
    use differential_dataflow::difference::Semigroup;
    use differential_dataflow::lattice::Lattice;
    use timely::progress::Timestamp;

    /// A layout based on columnar
    pub struct ColumnarLayout<U: ColumnarUpdate> {
        phantom: std::marker::PhantomData<U>,
    }

    impl<K, V, T, R> ColumnarUpdate for (K, V, T, R)
    where
        K: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Clone + 'static,
        V: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Clone + 'static,
        T: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Default + Clone + Lattice + Timestamp,
        R: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Default + Semigroup + 'static,
    {
        type Key = K;
        type Val = V;
        type Time = T;
        type Diff = R;
    }

    use crate::arrangement::Coltainer;
    impl<U: ColumnarUpdate> Layout for ColumnarLayout<U> {
        type KeyContainer    = Coltainer<U::Key>;
        type ValContainer    = Coltainer<U::Val>;
        type TimeContainer   = Coltainer<U::Time>;
        type DiffContainer   = Coltainer<U::Diff>;
        type OffsetContainer = OffsetList;
    }

    /// A type that names constituent update types.
    ///
    /// We will use their associated `Columnar::Container`
    pub trait ColumnarUpdate : Debug + 'static {
        type Key:  Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Clone + 'static;
        type Val:  Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Clone + 'static;
        type Time: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Default + Clone + Lattice + Timestamp;
        type Diff: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Default + Semigroup + 'static;
    }

    /// A container whose references can be ordered.
    pub trait OrdContainer : for<'a> columnar::Container<Ref<'a> : Ord> { }
    impl<C: for<'a> columnar::Container<Ref<'a> : Ord>> OrdContainer for C { }

}

pub use updates::Updates;

/// A thin wrapper around `Updates` that tracks the pre-consolidation record count
/// for timely's exchange accounting. This wrapper is the stream container type;
/// the `TrieChunker` strips it, passing bare `Updates` into the merge batcher.
pub struct RecordedUpdates<U: layout::ColumnarUpdate> {
    pub updates: Updates<U>,
    pub records: usize,
    /// Whether `updates` is known to be sorted and consolidated
    /// (no duplicate (key, val, time) triples, no zero diffs).
    pub consolidated: bool,
}

impl<U: layout::ColumnarUpdate> Default for RecordedUpdates<U> {
    fn default() -> Self { Self { updates: Default::default(), records: 0, consolidated: true } }
}

impl<U: layout::ColumnarUpdate> Clone for RecordedUpdates<U> {
    fn clone(&self) -> Self { Self { updates: self.updates.clone(), records: self.records, consolidated: self.consolidated } }
}

impl<U: layout::ColumnarUpdate> timely::Accountable for RecordedUpdates<U> {
    #[inline] fn record_count(&self) -> i64 { self.records as i64 }
}

impl<U: layout::ColumnarUpdate> timely::dataflow::channels::ContainerBytes for RecordedUpdates<U> {
    fn from_bytes(_bytes: timely::bytes::arc::Bytes) -> Self { unimplemented!() }
    fn length_in_bytes(&self) -> usize { unimplemented!() }
    fn into_bytes<W: std::io::Write>(&self, _writer: &mut W) { unimplemented!() }
}

// Container trait impls for RecordedUpdates, enabling iterative scopes.
mod container_impls {
    use columnar::{Borrow, Columnar, Index, Len, Push};
    use timely::progress::{Timestamp, timestamp::Refines};
    use differential_dataflow::difference::Abelian;
    use differential_dataflow::collection::containers::{Negate, Enter, Leave, ResultsIn};

    use crate::layout::ColumnarUpdate as Update;
    use crate::{RecordedUpdates, Updates};

    impl<U: Update<Diff: Abelian>> Negate for RecordedUpdates<U> {
        fn negate(mut self) -> Self {
            let len = self.updates.diffs.values.len();
            let mut new_diffs = <<U::Diff as Columnar>::Container as Default>::default();
            let mut owned = U::Diff::default();
            for i in 0..len {
                columnar::Columnar::copy_from(&mut owned, self.updates.diffs.values.borrow().get(i));
                owned.negate();
                new_diffs.push(&owned);
            }
            self.updates.diffs.values = new_diffs;
            self
        }
    }

    impl<K, V, T1, T2, R> Enter<T1, T2> for RecordedUpdates<(K, V, T1, R)>
    where
        (K, V, T1, R): Update<Key=K, Val=V, Time=T1, Diff=R>,
        (K, V, T2, R): Update<Key=K, Val=V, Time=T2, Diff=R>,
        T1: Timestamp + Columnar + Default + Clone,
        T2: Refines<T1> + Columnar + Default + Clone,
        K: Columnar, V: Columnar, R: Columnar,
    {
        type InnerContainer = RecordedUpdates<(K, V, T2, R)>;
        fn enter(self) -> Self::InnerContainer {
            // Rebuild the time column; everything else moves as-is.
            let mut new_times = <<T2 as Columnar>::Container as Default>::default();
            let mut t1_owned = T1::default();
            for i in 0..self.updates.times.values.len() {
                Columnar::copy_from(&mut t1_owned, self.updates.times.values.borrow().get(i));
                let t2 = T2::to_inner(t1_owned.clone());
                new_times.push(&t2);
            }
            // TODO: Assumes Enter (to_inner) is order-preserving on times.
            RecordedUpdates {
                consolidated: self.consolidated,
                updates: Updates {
                    keys: self.updates.keys,
                    vals: self.updates.vals,
                    times: crate::updates::Lists { values: new_times, bounds: self.updates.times.bounds },
                    diffs: self.updates.diffs,
                },
                records: self.records,
            }
        }
    }

    impl<K, V, T1, T2, R> Leave<T1, T2> for RecordedUpdates<(K, V, T1, R)>
    where
        (K, V, T1, R): Update<Key=K, Val=V, Time=T1, Diff=R>,
        (K, V, T2, R): Update<Key=K, Val=V, Time=T2, Diff=R>,
        T1: Refines<T2> + Columnar + Default + Clone,
        T2: Timestamp + Columnar + Default + Clone,
        K: Columnar, V: Columnar, R: Columnar,
    {
        type OuterContainer = RecordedUpdates<(K, V, T2, R)>;
        fn leave(self) -> Self::OuterContainer {
            // Flatten, convert times, and reconsolidate via consolidate.
            // Leave can collapse distinct T1 times to the same T2 time,
            // so the trie must be rebuilt with consolidation.
            let mut flat = Updates::<(K, V, T2, R)>::default();
            let mut t1_owned = T1::default();
            for (k, v, t, d) in self.updates.iter() {
                Columnar::copy_from(&mut t1_owned, t);
                let t2: T2 = t1_owned.clone().to_outer();
                flat.push((k, v, &t2, d));
            }
            RecordedUpdates {
                updates: flat.consolidate(),
                records: self.records,
                consolidated: true,
            }
        }
    }

    impl<U: Update> ResultsIn<<U::Time as Timestamp>::Summary> for RecordedUpdates<U> {
        fn results_in(self, step: &<U::Time as Timestamp>::Summary) -> Self {
            use timely::progress::PathSummary;
            // Apply results_in to each time; drop updates whose time maps to None.
            // This must rebuild the trie since some entries may be removed.
            let mut output = Updates::<U>::default();
            let mut time_owned = U::Time::default();
            for (k, v, t, d) in self.updates.iter() {
                Columnar::copy_from(&mut time_owned, t);
                if let Some(new_time) = step.results_in(&time_owned) {
                    output.push((k, v, &new_time, d));
                }
            }
            // TODO: Time advancement may not be order preserving, but .. it could be.
            // TODO: Before this is consolidated the above would need to be `form`ed.
            RecordedUpdates { updates: output, records: self.records, consolidated: false }
        }
    }
}

pub use column_builder::ValBuilder as ValColBuilder;
mod column_builder {

    use std::collections::VecDeque;
    use columnar::{Columnar, Clear, Len, Push};

    use crate::layout::ColumnarUpdate as Update;
    use crate::{Updates, RecordedUpdates};

    type TupleContainer<U> = <(<U as Update>::Key, <U as Update>::Val, <U as Update>::Time, <U as Update>::Diff) as Columnar>::Container;

    /// A container builder that produces `RecordedUpdates` (sorted, consolidated trie + record count).
    pub struct ValBuilder<U: Update> {
        /// Container that we're writing to.
        current: TupleContainer<U>,
        /// Empty allocation.
        empty: Option<RecordedUpdates<U>>,
        /// Completed containers pending to be sent.
        pending: VecDeque<RecordedUpdates<U>>,
    }

    use timely::container::PushInto;
    impl<T, U: Update> PushInto<T> for ValBuilder<U> where TupleContainer<U> : Push<T> {
        #[inline]
        fn push_into(&mut self, item: T) {
            self.current.push(item);
            if self.current.len() > 1024 * 1024 {
                use columnar::{Borrow, Index};
                let records = self.current.len();
                let mut refs = self.current.borrow().into_index_iter().collect::<Vec<_>>();
                refs.sort();
                let updates = Updates::form(refs.into_iter());
                self.pending.push_back(RecordedUpdates { updates, records, consolidated: true });
                self.current.clear();
            }
        }
    }

    impl<U: Update> Default for ValBuilder<U> {
        fn default() -> Self {
            ValBuilder {
                current: Default::default(),
                empty: None,
                pending: Default::default(),
            }
        }
    }

    use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};
    impl<U: Update> ContainerBuilder for ValBuilder<U> {
        type Container = RecordedUpdates<U>;

        #[inline]
        fn extract(&mut self) -> Option<&mut Self::Container> {
            if let Some(container) = self.pending.pop_front() {
                self.empty = Some(container);
                self.empty.as_mut()
            } else {
                None
            }
        }

        #[inline]
        fn finish(&mut self) -> Option<&mut Self::Container> {
            if !self.current.is_empty() {
                use columnar::{Borrow, Index};
                let records = self.current.len();
                let mut refs = self.current.borrow().into_index_iter().collect::<Vec<_>>();
                refs.sort();
                let updates = Updates::form(refs.into_iter());
                self.pending.push_back(RecordedUpdates { updates, records, consolidated: true });
                self.current.clear();
            }
            self.empty = self.pending.pop_front();
            self.empty.as_mut()
        }
    }

    impl<U: Update> LengthPreservingContainerBuilder for ValBuilder<U> { }

}

pub use distributor::ValPact;
mod distributor {

    use std::rc::Rc;

    use columnar::{Borrow, Index, Len};
    use timely::logging::TimelyLogger;
    use timely::dataflow::channels::pushers::{Exchange, exchange::Distributor};
    use timely::dataflow::channels::Message;
    use timely::dataflow::channels::pact::{LogPuller, LogPusher, ParallelizationContract};
    use timely::progress::Timestamp;
    use timely::worker::AsWorker;

    use crate::layout::ColumnarUpdate as Update;
    use crate::{Updates, RecordedUpdates};

    pub struct ValDistributor<U: Update, H> {
        marker: std::marker::PhantomData<U>,
        hashfunc: H,
        pre_lens: Vec<usize>,
    }

    impl<U: Update, H: for<'a> FnMut(columnar::Ref<'a, U::Key>)->u64> Distributor<RecordedUpdates<U>> for ValDistributor<U, H> {
        // TODO: For unsorted Updates (stride-1 outer keys), each key is its own outer group,
        // so the per-group pre_lens snapshot and seal check costs O(keys × workers). Should
        // either batch keys by destination first, or detect stride-1 outer bounds and use a
        // simpler single-pass partitioning that seals once at the end.
        fn partition<T: Clone, P: timely::communication::Push<Message<T, RecordedUpdates<U>>>>(&mut self, container: &mut RecordedUpdates<U>, time: &T, pushers: &mut [P]) {
            use crate::updates::child_range;

            let keys_b = container.updates.keys.borrow();
            let mut outputs: Vec<Updates<U>> = (0..pushers.len()).map(|_| Updates::default()).collect();

            // Each outer key group becomes a separate run in the destination.
            for outer in 0..Len::len(&keys_b) {
                self.pre_lens.clear();
                self.pre_lens.extend(outputs.iter().map(|o| o.keys.values.len()));
                for k in child_range(keys_b.bounds, outer) {
                    let key = keys_b.values.get(k);
                    let idx = ((self.hashfunc)(key) as usize) % pushers.len();
                    outputs[idx].extend_from_keys(&container.updates, k..k+1);
                }
                for (output, &pre) in outputs.iter_mut().zip(self.pre_lens.iter()) {
                    if output.keys.values.len() > pre {
                        output.keys.bounds.push(output.keys.values.len() as u64);
                    }
                }
            }

            // Distribute the input's record count across non-empty outputs.
            let total_records = container.records;
            let non_empty: usize = outputs.iter().filter(|o| !o.keys.values.is_empty()).count();
            let mut first_records = total_records.saturating_sub(non_empty.saturating_sub(1));
            for (pusher, output) in pushers.iter_mut().zip(outputs) {
                if !output.keys.values.is_empty() {
                    let recorded = RecordedUpdates { updates: output, records: first_records, consolidated: container.consolidated };
                    first_records = 1;
                    let mut recorded = recorded;
                    Message::push_at(&mut recorded, time.clone(), pusher);
                }
            }
        }
        fn flush<T: Clone, P: timely::communication::Push<Message<T, RecordedUpdates<U>>>>(&mut self, _time: &T, _pushers: &mut [P]) { }
        fn relax(&mut self) { }
    }

    pub struct ValPact<H> { pub hashfunc: H }

    impl<T, U, H> ParallelizationContract<T, RecordedUpdates<U>> for ValPact<H>
    where
        T: Timestamp,
        U: Update,
        H: for<'a> FnMut(columnar::Ref<'a, U::Key>)->u64 + 'static,
    {
        type Pusher = Exchange<
            T,
            LogPusher<Box<dyn timely::communication::Push<Message<T, RecordedUpdates<U>>>>>,
            ValDistributor<U, H>
        >;
        type Puller = LogPuller<Box<dyn timely::communication::Pull<Message<T, RecordedUpdates<U>>>>>;

        fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: Rc<[usize]>, logging: Option<TimelyLogger>) -> (Self::Pusher, Self::Puller) {
            let (senders, receiver) = allocator.allocate::<Message<T, RecordedUpdates<U>>>(identifier, address);
            let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
            let distributor = ValDistributor {
                marker: std::marker::PhantomData,
                hashfunc: self.hashfunc,
                pre_lens: Vec::new(),
            };
            (Exchange::new(senders, distributor), LogPuller::new(receiver, allocator.index(), identifier, logging.clone()))
        }
    }
}

pub use arrangement::{ValBatcher, ValBuilder, ValSpine};
pub mod arrangement {

    use std::rc::Rc;
    use differential_dataflow::trace::implementations::ord_neu::OrdValBatch;
    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use differential_dataflow::trace::implementations::spine_fueled::Spine;

    use crate::layout::ColumnarLayout;

    /// A trace implementation backed by columnar storage.
    pub type ValSpine<K, V, T, R> = Spine<Rc<OrdValBatch<ColumnarLayout<(K,V,T,R)>>>>;
    /// A batcher for columnar storage.
    pub type ValBatcher<K, V, T, R> = ValBatcher2<(K,V,T,R)>;
    /// A builder for columnar storage.
    pub type ValBuilder<K, V, T, R> = RcBuilder<ValMirror<(K,V,T,R)>>;

    /// A batch container implementation for Coltainer<C>.
    pub use batch_container::Coltainer;
    pub mod batch_container {

        use columnar::{Borrow, Columnar, Container, Clear, Push, Index, Len};
        use differential_dataflow::trace::implementations::BatchContainer;

        /// Container, anchored by `C` to provide an owned type.
        pub struct Coltainer<C: Columnar> {
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
        }
    }

    use crate::{Updates, RecordedUpdates};
    use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
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
    pub struct TrieChunker<U: crate::layout::ColumnarUpdate> {
        ready: std::collections::VecDeque<Updates<U>>,
        empty: Option<Updates<U>>,
    }

    impl<U: crate::layout::ColumnarUpdate> Default for TrieChunker<U> {
        fn default() -> Self { Self { ready: Default::default(), empty: None } }
    }

    impl<'a, U: crate::layout::ColumnarUpdate> timely::container::PushInto<&'a mut RecordedUpdates<U>> for TrieChunker<U> {
        fn push_into(&mut self, container: &'a mut RecordedUpdates<U>) {
            let mut updates = std::mem::take(&mut container.updates);
            if !container.consolidated { updates = updates.consolidate(); }
            if updates.len() > 0 { self.ready.push_back(updates); }
        }
    }

    impl<U: crate::layout::ColumnarUpdate> timely::container::ContainerBuilder for TrieChunker<U> {
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

        use columnar::{Borrow, Columnar, Index, Len, Push};
        use differential_dataflow::difference::{Semigroup, IsZero};
        use timely::progress::frontier::{Antichain, AntichainRef};
        use differential_dataflow::trace::implementations::merge_batcher::container::InternalMerge;

        use crate::ColumnarUpdate as Update;
        use crate::Updates;

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

    pub mod trie_merger {

        use columnar::{Columnar, Len};
        use timely::PartialOrder;
        use timely::progress::frontier::{Antichain, AntichainRef};
        use differential_dataflow::trace::implementations::merge_batcher::Merger;

        use crate::ColumnarUpdate as Update;
        use crate::Updates;

        pub struct TrieMerger<U: Update> {
            _marker: std::marker::PhantomData<U>,
        }

        impl<U: Update> Default for TrieMerger<U> {
            fn default() -> Self { Self { _marker: std::marker::PhantomData } }
        }

        /// A merging iterator over two sorted iterators.
        struct Merging<I1: Iterator, I2: Iterator> {
            iter1: std::iter::Peekable<I1>,
            iter2: std::iter::Peekable<I2>,
        }

        impl<K, V, T, D, I1, I2> Iterator for Merging<I1, I2>
        where
            K: Copy + Ord,
            V: Copy + Ord,
            T: Copy + Ord,
            I1: Iterator<Item = (K, V, T, D)>,
            I2: Iterator<Item = (K, V, T, D)>,
        {
            type Item = (K, V, T, D);
            #[inline]
            fn next(&mut self) -> Option<Self::Item> {
                match (self.iter1.peek(), self.iter2.peek()) {
                    (Some(a), Some(b)) => {
                        if (a.0, a.1, a.2) <= (b.0, b.1, b.2) {
                            self.iter1.next()
                        } else {
                            self.iter2.next()
                        }
                    }
                    (Some(_), None) => self.iter1.next(),
                    (None, Some(_)) => self.iter2.next(),
                    (None, None) => None,
                }
            }
        }

        /// Build sorted `Updates` chunks from a sorted iterator of refs,
        /// using `Updates::form` (which consolidates internally) on batches.
        fn form_chunks<'a, U: Update>(
            sorted: impl Iterator<Item = columnar::Ref<'a, crate::updates::Tuple<U>>>,
            output: &mut Vec<Updates<U>>,
        ) {
            let mut sorted = sorted.peekable();
            while sorted.peek().is_some() {
                let chunk = Updates::<U>::form((&mut sorted).take(64 * 1024));
                if chunk.len() > 0 {
                    output.push(chunk);
                }
            }
        }

        impl<U: Update> Merger for TrieMerger<U>
        where
            U::Time: Ord + PartialOrder + Clone + 'static,
        {
            type Chunk = Updates<U>;
            type Time = U::Time;

            fn merge(
                &mut self,
                list1: Vec<Updates<U>>,
                list2: Vec<Updates<U>>,
                output: &mut Vec<Updates<U>>,
                _stash: &mut Vec<Updates<U>>,
            ) {
                Self::merge_batches(list1, list2, output, _stash);
            }

            fn extract(
                &mut self,
                merged: Vec<Self::Chunk>,
                upper: AntichainRef<Self::Time>,
                frontier: &mut Antichain<Self::Time>,
                ship: &mut Vec<Self::Chunk>,
                kept: &mut Vec<Self::Chunk>,
                _stash: &mut Vec<Self::Chunk>,
            ) {
                // Flatten the sorted, consolidated chain into refs.
                let all = merged.iter().flat_map(|chunk| chunk.iter());

                // Partition into two sorted streams by time.
                let mut time_owned = U::Time::default();
                let mut keep_vec = Vec::new();
                let mut ship_vec = Vec::new();
                for (k, v, t, d) in all {
                    Columnar::copy_from(&mut time_owned, t);
                    if upper.less_equal(&time_owned) {
                        frontier.insert_ref(&time_owned);
                        keep_vec.push((k, v, t, d));
                    } else {
                        ship_vec.push((k, v, t, d));
                    }
                }

                // Build chunks via form (which consolidates internally).
                form_chunks::<U>(keep_vec.into_iter(), kept);
                form_chunks::<U>(ship_vec.into_iter(), ship);
            }

            fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
                use timely::Accountable;
                (chunk.record_count() as usize, 0, 0, 0)
            }
        }

        impl<U: Update> TrieMerger<U>
        where
            U::Time: Ord + PartialOrder + Clone + 'static,
        {
            /// Iterator-based merge: flatten, merge, consolidate, form.
            /// Correct but slow — used as fallback.
            #[allow(dead_code)]
            fn merge_iterator(
                list1: &[Updates<U>],
                list2: &[Updates<U>],
                output: &mut Vec<Updates<U>>,
            ) {
                let iter1 = list1.iter().flat_map(|chunk| chunk.iter());
                let iter2 = list2.iter().flat_map(|chunk| chunk.iter());

                let merged = Merging {
                    iter1: iter1.peekable(),
                    iter2: iter2.peekable(),
                };

                form_chunks::<U>(merged, output);
            }

            /// A merge implementation that operates batch-at-a-time.
            #[inline(never)]
            fn merge_batches(
                list1: Vec<Updates<U>>,
                list2: Vec<Updates<U>>,
                output: &mut Vec<Updates<U>>,
                stash: &mut Vec<Updates<U>>,
            ) {

                // The design for efficient "batch" merginging of chains of links is:
                // 0.   We choose a target link size, K, and will keep the average link size at least K and the max size at 2k.
                //      K should be large enough to amortize some set-up, but not so large that one or two extra break the bank.
                // 1.   We will repeatedly consider pairs of links, and fully merge one with a prefix of the other.
                //      The last elements of each link will tell us which of the two suffixes must be held back.
                // 2.   We then have a chain of as many links as we started with, with potential defects to correct:
                //      a.  A link may contain some number of zeros: we can remove them if we are eager, based on size.
                //      b.  A link may contain more than 2K updates; we can split it.
                //      c.  Two adjacent links may contain fewer than 2K updates; we can meld (careful append) them.
                // 3.   After a pass of the above, we should have restored the invariant.
                //      We can try and me smarter and fuse some of the above work rather than explicitly stage results.
                //
                // The challenging moment is the merge that can start with a suffix of one link, involving a prefix of one link.
                // These could be the same link, different links, and generally there is the potential for complexity here.

                let mut builder = ChainBuilder::default();

                let mut queue1: std::collections::VecDeque<_> = list1.into();
                let mut queue2: std::collections::VecDeque<_> = list2.into();

                // The first unconsumed update in each block, via (k_idx, v_idx, t_idx), or None if exhausted.
                // These are (0,0,0) for a new block, and should become None once there are no remaining updates.
                let mut cursor1 = queue1.pop_front().map(|b| ((0,0,0), b));
                let mut cursor2 = queue2.pop_front().map(|b| ((0,0,0), b));

                // For each pair of batches
                while cursor1.is_some() && cursor2.is_some() {
                    Self::merge_batch(&mut cursor1, &mut cursor2, &mut builder, stash);
                    if cursor1.is_none() { cursor1 = queue1.pop_front().map(|b| ((0,0,0), b)); }
                    if cursor2.is_none() { cursor2 = queue2.pop_front().map(|b| ((0,0,0), b)); }
                }

                // TODO: create batch for the non-empty cursor.
                if let Some(((k,v,t),batch)) = cursor1 {
                    let mut out_batch = stash.pop().unwrap_or_default();
                    let empty: Updates<U> = Default::default();
                    write_from_surveys(
                        &batch,
                        &empty,
                        &[Report::This(0, 1)],
                        &[Report::This(k, batch.keys.values.len())],
                        &[Report::This(v, batch.vals.values.len())],
                        &[Report::This(t, batch.times.values.len())],
                        &mut out_batch,
                    );
                    builder.push(out_batch);
                }
                if let Some(((k,v,t),batch)) = cursor2 {
                    let mut out_batch = stash.pop().unwrap_or_default();
                    let empty: Updates<U> = Default::default();
                    write_from_surveys(
                        &empty,
                        &batch,
                        &[Report::That(0, 1)],
                        &[Report::That(k, batch.keys.values.len())],
                        &[Report::That(v, batch.vals.values.len())],
                        &[Report::That(t, batch.times.values.len())],
                        &mut out_batch,
                    );
                    builder.push(out_batch);
                }

                builder.extend(queue1);
                builder.extend(queue2);
                *output = builder.done();
                // TODO: Tidy output to satisfy structural invariants.
            }

            /// Merge two batches, one completely and another through the corresponding prefix.
            ///
            /// Each invocation determines the maximum amount of both batches we can merge, determined
            /// by comparing the elements at the tails of each batch, and locating the lesser in other.
            /// We will merge the whole of the batch containing the lesser, and the prefix up through
            /// the lesser element in the other batch, setting the cursor to the first element strictly
            /// greater than that lesser element.
            ///
            /// The algorithm uses a list of `Report` findings to map the interleavings of the layers.
            /// Each indicates either a range exclusive to one of the inputs, or a one element common
            /// to the layers from both inputs, which must be further explored. This map would normally
            /// allow the full merge to happen, but we need to carefully start at each cursor, and end
            /// just before the first element greater than the lesser bound.
            ///
            /// The consumed prefix and disjoint suffix should be single report entries, and it seems
            /// fine to first produce all reports and then reflect on the cursors, rather than use the
            /// cursors as part of the mapping.
            #[inline(never)]
            fn merge_batch(
                batch1: &mut Option<((usize, usize, usize), Updates<U>)>,
                batch2: &mut Option<((usize, usize, usize), Updates<U>)>,
                builder: &mut ChainBuilder<U>,
                stash: &mut Vec<Updates<U>>,
            ) {
                let ((k0_idx, v0_idx, t0_idx), updates0) = batch1.take().unwrap();
                let ((k1_idx, v1_idx, t1_idx), updates1) = batch2.take().unwrap();

                use columnar::Borrow;
                let keys0 = updates0.keys.borrow();
                let keys1 = updates1.keys.borrow();
                let vals0 = updates0.vals.borrow();
                let vals1 = updates1.vals.borrow();
                let times0 = updates0.times.borrow();
                let times1 = updates1.times.borrow();

                // Survey the interleaving of the two inputs.
                let mut key_survey = survey::<columnar::ContainerOf<U::Key>>(keys0, keys1, &[Report::Both(0,0)]);
                let mut val_survey = survey::<columnar::ContainerOf<U::Val>>(vals0, vals1, &key_survey);
                let mut time_survey = survey::<columnar::ContainerOf<U::Time>>(times0, times1, &val_survey);

                // We now know enough to start writing into an output batch.
                // We should update the input surveys to reflect the subset
                // of data that we want.
                //
                // At most one cursor should be non-zero (assert!).
                // A non-zero cursor must correspond to the first entry of the surveys,
                // as there is at least one consumed update that precedes the other batch.
                // We need to nudge that report forward to align with the cursor, potentially
                // squeezing the report to nothing (to the upper bound).

                // We start by updating the surveys to reflect the cursors.
                // If either cursor is set, then its batch has an element strictly less than the other batch.
                // We therefore expect to find a prefix of This/That at the start of the survey.
                if (k0_idx, v0_idx, t0_idx) != (0,0,0) {
                    let mut done = false; while !done { if let Report::This(l,u) = &mut key_survey[0] { if *u <= k0_idx { key_survey.remove(0); } else { *l = k0_idx; done = true; } } else { done = true; } }
                    let mut done = false; while !done { if let Report::This(l,u) = &mut val_survey[0] { if *u <= v0_idx { val_survey.remove(0); } else { *l = v0_idx; done = true; } } else { done = true; } }
                    let mut done = false; while !done { if let Report::This(l,u) = &mut time_survey[0] { if *u <= t0_idx { time_survey.remove(0); } else { *l = t0_idx; done = true; } } else { done = true; } }
                }

                if (k1_idx, v1_idx, t1_idx) != (0,0,0) {
                    let mut done = false; while !done { if let Report::That(l,u) = &mut key_survey[0] { if *u <= k1_idx { key_survey.remove(0); } else { *l = k1_idx; done = true; } } else { done = true; } }
                    let mut done = false; while !done { if let Report::That(l,u) = &mut val_survey[0] { if *u <= v1_idx { val_survey.remove(0); } else { *l = v1_idx; done = true; } } else { done = true; } }
                    let mut done = false; while !done { if let Report::That(l,u) = &mut time_survey[0] { if *u <= t1_idx { time_survey.remove(0); } else { *l = t1_idx; done = true; } } else { done = true; } }
                }

                // We want to trim the tails of the surveys to only cover ranges present in both inputs.
                // We can determine which was "longer" by looking at the last entry of the bottom layer,
                // which tells us which input (or both) contained the last element.
                //
                // From the bottom layer up, we'll identify the index of the last item, and then determine
                // the index of the list it belongs to. We use that index in the next layer, to locate the
                // index of the list it belongs to, on upward.
                let next_cursor = match time_survey.last().unwrap() {
                    Report::This(_,_) => {
                        // Collect the last value indexes known to strictly exceed an entry in the other batch.
                        let mut t = times0.values.len();
                        while let Some(Report::This(l,_)) = time_survey.last() { t = *l; time_survey.pop(); }
                        let mut v = vals0.values.len();
                        while let Some(Report::This(l,_)) = val_survey.last() { v = *l; val_survey.pop(); }
                        let mut k = keys0.values.len();
                        while let Some(Report::This(l,_)) = key_survey.last() { k = *l; key_survey.pop(); }
                        // Now we may need to correct by nudging down.
                        if v == times0.len() || times0.bounds.bounds(v).0 > t { v -= 1; }
                        if k == vals0.len() || vals0.bounds.bounds(k).0 > v { k -= 1; }
                        Some(Ok((k,v,t)))
                    }
                    Report::Both(_,_) => { None }
                    Report::That(_,_) => {
                        // Collect the last value indexes known to strictly exceed an entry in the other batch.
                        let mut t = times1.values.len();
                        while let Some(Report::That(l,_)) = time_survey.last() { t = *l; time_survey.pop(); }
                        let mut v = vals1.values.len();
                        while let Some(Report::That(l,_)) = val_survey.last() { v = *l; val_survey.pop(); }
                        let mut k = keys1.values.len();
                        while let Some(Report::That(l,_)) = key_survey.last() { k = *l; key_survey.pop(); }
                        // Now we may need to correct by nudging down.
                        if v == times1.len() || times1.bounds.bounds(v).0 > t { v -= 1; }
                        if k == vals1.len() || vals1.bounds.bounds(k).0 > v { k -= 1; }
                        Some(Err((k,v,t)))
                    }
                };

                // Having updated the surveys, we now copy over the ranges they identify.
                let mut out_batch = stash.pop().unwrap_or_default();
                // TODO: We should be able to size `out_batch` pretty accurately from the survey.
                write_from_surveys(&updates0, &updates1, &[Report::Both(0,0)], &key_survey, &val_survey, &time_survey, &mut out_batch);
                builder.push(out_batch);

                match next_cursor {
                    Some(Ok(kvt)) => { *batch1 = Some((kvt, updates0)); }
                    Some(Err(kvt)) => {*batch2 = Some((kvt, updates1)); }
                    None => { }
                }
            }

        }

        /// Write merged output from four levels of survey reports.
        ///
        /// Each layer is written independently: `write_layer` handles keys, vals,
        /// and times; `write_diffs` handles diff consolidation.
        #[inline(never)]
        fn write_from_surveys<U: Update>(
            updates0: &Updates<U>,
            updates1: &Updates<U>,
            root_survey: &[Report],
            key_survey: &[Report],
            val_survey: &[Report],
            time_survey: &[Report],
            output: &mut Updates<U>,
        ) {
            use columnar::Borrow;

            write_layer(updates0.keys.borrow(), updates1.keys.borrow(), root_survey, key_survey, &mut output.keys);
            write_layer(updates0.vals.borrow(), updates1.vals.borrow(), key_survey, val_survey, &mut output.vals);
            write_layer(updates0.times.borrow(), updates1.times.borrow(), val_survey, time_survey, &mut output.times);
            write_diffs::<U>(updates0.diffs.borrow(), updates1.diffs.borrow(), time_survey, &mut output.diffs);
        }

        /// From two sequences of interleaved lists, map out the interleaving of their values.
        ///
        /// The sequence of input reports identify constraints on the sorted order of lists in the two inputs,
        /// callout out ranges of each that are exclusively order, and elements that have equal prefixes and
        /// therefore "overlap" and should be further investigated through the values of the lists.
        ///
        /// The output should have the same form but for the next layer: subject to the ordering of `reports`,
        /// a similar report for the values of the two lists, appropriate for the next layer.
        #[inline(never)]
        pub fn survey<'a, C: columnar::Container<Ref<'a>: Ord>>(
            lists0: <crate::updates::Lists<C> as columnar::Borrow>::Borrowed<'a>,
            lists1: <crate::updates::Lists<C> as columnar::Borrow>::Borrowed<'a>,
            reports: &[Report],
        ) -> Vec<Report> {
            use columnar::Index;
            let mut output = Vec::with_capacity(reports.len()); // may grow larger, but at least this large.
            for report in reports.iter() {
                match report {
                    Report::This(lower0, upper0) => {
                        let (new_lower, _) = lists0.bounds.bounds(*lower0);
                        let (_, new_upper) = lists0.bounds.bounds(*upper0-1);
                        output.push(Report::This(new_lower, new_upper));
                    }
                    Report::Both(index0, index1) => {

                        // Fetch the bounds from the layers.
                        let (mut lower0, upper0) = lists0.bounds.bounds(*index0);
                        let (mut lower1, upper1) = lists1.bounds.bounds(*index1);

                        // Scour the intersecting range for matches.
                        while lower0 < upper0 && lower1 < upper1 {
                            let val0 = lists0.values.get(lower0);
                            let val1 = lists1.values.get(lower1);
                            match val0.cmp(&val1) {
                                std::cmp::Ordering::Less => {
                                    let start = lower0;
                                    lower0 += 1;
                                    gallop(lists0.values, &mut lower0, upper0, |x| x < val1);
                                    output.push(Report::This(start, lower0));
                                },
                                std::cmp::Ordering::Equal => {
                                    output.push(Report::Both(lower0, lower1));
                                    lower0 += 1;
                                    lower1 += 1;
                                },
                                std::cmp::Ordering::Greater => {
                                    let start = lower1;
                                    lower1 += 1;
                                    gallop(lists1.values, &mut lower1, upper1, |x| x < val0);
                                    output.push(Report::That(start, lower1));
                                },
                            }
                        }
                        if lower0 < upper0 { output.push(Report::This(lower0, upper0)); }
                        if lower1 < upper1 { output.push(Report::That(lower1, upper1)); }

                    }
                    Report::That(lower1, upper1) => {
                        let (new_lower, _) = lists1.bounds.bounds(*lower1);
                        let (_, new_upper) = lists1.bounds.bounds(*upper1-1);
                        output.push(Report::That(new_lower, new_upper));
                    }
                }
            }

            output
        }

        /// Write one layer of merged output from a list survey and item survey.
        ///
        /// The list survey describes which lists to produce (from the layer above).
        /// The item survey describes how the items within those lists interleave.
        /// Both surveys are consumed completely; a mismatch is a bug.
        ///
        /// Pruning (from cursor adjustments) can affect the first and last list
        /// survey entries: the item survey's ranges may not match the natural
        /// bounds of those lists. Middle entries are guaranteed unpruned and can
        /// be bulk-copied.
        #[inline(never)]
        pub fn write_layer<'a, C: columnar::Container<Ref<'a>: Ord>>(
            lists0: <crate::updates::Lists<C> as columnar::Borrow>::Borrowed<'a>,
            lists1: <crate::updates::Lists<C> as columnar::Borrow>::Borrowed<'a>,
            list_survey: &[Report],
            item_survey: &[Report],
            output: &mut crate::updates::Lists<C>,
        ) {
            use columnar::{Container, Index, Len, Push};

            let mut item_idx = 0;

            for (pos, list_report) in list_survey.iter().enumerate() {
                let is_first = pos == 0;
                let is_last = pos == list_survey.len() - 1;
                let may_be_pruned = is_first || is_last;

                match list_report {
                    Report::This(lo, hi) => {
                        let Report::This(item_lo, item_hi) = item_survey[item_idx] else { unreachable!("Expected This in item survey for This list") };
                        item_idx += 1;
                        if may_be_pruned {
                            // Item range may not match natural bounds; copy items in bulk
                            // but compute per-list bounds from natural bounds clamped to
                            // the item range.
                            let base = output.values.len();
                            output.values.extend_from_self(lists0.values, item_lo..item_hi);
                            for i in *lo..*hi {
                                let (_, nat_hi) = lists0.bounds.bounds(i);
                                output.bounds.push((base + nat_hi.min(item_hi) - item_lo) as u64);
                            }
                        } else {
                            output.extend_from_self(lists0, *lo..*hi);
                        }
                    }
                    Report::That(lo, hi) => {
                        let Report::That(item_lo, item_hi) = item_survey[item_idx] else { unreachable!("Expected That in item survey for That list") };
                        item_idx += 1;
                        if may_be_pruned {
                            let base = output.values.len();
                            output.values.extend_from_self(lists1.values, item_lo..item_hi);
                            for i in *lo..*hi {
                                let (_, nat_hi) = lists1.bounds.bounds(i);
                                output.bounds.push((base + nat_hi.min(item_hi) - item_lo) as u64);
                            }
                        } else {
                            output.extend_from_self(lists1, *lo..*hi);
                        }
                    }
                    Report::Both(i0, i1) => {
                        // Merge: consume item survey entries until both sides are covered.
                        let (mut c0, end0) = lists0.bounds.bounds(*i0);
                        let (mut c1, end1) = lists1.bounds.bounds(*i1);
                        while (c0 < end0 || c1 < end1) && item_idx < item_survey.len() {
                            match item_survey[item_idx] {
                                Report::This(lo, hi) => {
                                    if lo >= end0 { break; }
                                    output.values.extend_from_self(lists0.values, lo..hi);
                                    c0 = hi;
                                }
                                Report::That(lo, hi) => {
                                    if lo >= end1 { break; }
                                    output.values.extend_from_self(lists1.values, lo..hi);
                                    c1 = hi;
                                }
                                Report::Both(v0, v1) => {
                                    if v0 >= end0 && v1 >= end1 { break; }
                                    output.values.push(lists0.values.get(v0));
                                    c0 = v0 + 1;
                                    c1 = v1 + 1;
                                }
                            }
                            item_idx += 1;
                        }
                        output.bounds.push(output.values.len() as u64);
                    }
                }
            }
        }

        /// Write the diff layer from a time survey and two diff inputs.
        ///
        /// The time survey is the item-level survey for the time layer, which
        /// doubles as the list survey for diffs (one diff list per time entry).
        ///
        /// - `This(lo, hi)`: bulk-copy diff lists from input 0.
        /// - `That(lo, hi)`: bulk-copy diff lists from input 1.
        /// - `Both(t0, t1)`: consolidate the two singleton diffs. Push `[sum]`
        ///   if non-zero, or an empty list `[]` if they cancel.
        #[inline(never)]
        pub fn write_diffs<U: crate::layout::ColumnarUpdate>(
            diffs0: <crate::updates::Lists<columnar::ContainerOf<U::Diff>> as columnar::Borrow>::Borrowed<'_>,
            diffs1: <crate::updates::Lists<columnar::ContainerOf<U::Diff>> as columnar::Borrow>::Borrowed<'_>,
            time_survey: &[Report],
            output: &mut crate::updates::Lists<columnar::ContainerOf<U::Diff>>,
        ) {
            use columnar::{Columnar, Container, Index, Len, Push};
            use differential_dataflow::difference::{Semigroup, IsZero};

            for report in time_survey.iter() {
                match report {
                    Report::This(lo, hi) => { output.extend_from_self(diffs0, *lo..*hi); }
                    Report::That(lo, hi) => { output.extend_from_self(diffs1, *lo..*hi); }
                    Report::Both(t0, t1) => {
                        // Read singleton diffs via list bounds, consolidate.
                        let (d0_lo, d0_hi) = diffs0.bounds.bounds(*t0);
                        let (d1_lo, d1_hi) = diffs1.bounds.bounds(*t1);
                        assert_eq!(d0_hi - d0_lo, 1, "Expected singleton diff list at t0={t0}");
                        assert_eq!(d1_hi - d1_lo, 1, "Expected singleton diff list at t1={t1}");
                        let mut diff: U::Diff = Columnar::into_owned(diffs0.values.get(d0_lo));
                        diff.plus_equals(&Columnar::into_owned(diffs1.values.get(d1_lo)));
                        if !diff.is_zero() { output.values.push(&diff); }
                        output.bounds.push(output.values.len() as u64);
                    }
                }
            }
        }

        /// Increments `index` until just after the last element of `input` to satisfy `cmp`.
        ///
        /// The method assumes that `cmp` is monotonic, never becoming true once it is false.
        /// If an `upper` is supplied, it acts as a constraint on the interval of `input` explored.
        #[inline(always)]
        pub(crate) fn gallop<C: columnar::Index>(input: C, lower: &mut usize, upper: usize, mut cmp: impl FnMut(<C as columnar::Index>::Ref) -> bool) {
            // if empty input, or already >= element, return
            if *lower < upper && cmp(input.get(*lower)) {
                let mut step = 1;
                while *lower + step < upper && cmp(input.get(*lower + step)) {
                    *lower += step;
                    step <<= 1;
                }

                step >>= 1;
                while step > 0 {
                    if *lower + step < upper && cmp(input.get(*lower + step)) {
                        *lower += step;
                    }
                    step >>= 1;
                }

                *lower += 1;
            }
        }

        /// A report we would expect to see in a sequence about two layers.
        ///
        /// A sequence of these reports reveal an ordered traversal of the keys
        /// of two layers, with ranges exclusive to one, ranges exclusive to the
        /// other, and individual elements (not ranges) common to both.
        #[derive(Copy, Clone, Columnar, Debug)]
        pub enum Report {
            /// Range of indices in this input.
            This(usize, usize),
            /// Range of indices in that input.
            That(usize, usize),
            /// Matching indices in both inputs.
            Both(usize, usize),
        }

        pub struct ChainBuilder<U: crate::layout::ColumnarUpdate> {
            updates: Vec<Updates<U>>,
        }

        impl<U: crate::layout::ColumnarUpdate> Default for ChainBuilder<U> { fn default() -> Self { Self { updates: Default::default() } } }

        impl<U: crate::layout::ColumnarUpdate> ChainBuilder<U> {
            fn push(&mut self, mut link: Updates<U>) {
                link = link.filter_zero();
                if link.len() > 0 {
                    if let Some(last) = self.updates.last_mut() {
                        if last.len() + link.len() < 2 * 64 * 1024 {
                            let mut build = crate::updates::UpdatesBuilder::new_from(std::mem::take(last));
                            build.meld(&link);
                            *last = build.done();
                        }
                        else { self.updates.push(link); }

                    }
                    else { self.updates.push(link); }
                }
            }
            fn extend(&mut self, iter: impl IntoIterator<Item=Updates<U>>) { for link in iter { self.push(link); }}
            fn done(self) -> Vec<Updates<U>> { self.updates }
        }
    }

    use builder::ValMirror;
    pub mod builder {

        use differential_dataflow::trace::implementations::ord_neu::{Vals, Upds};
        use differential_dataflow::trace::implementations::ord_neu::val_batch::{OrdValBatch, OrdValStorage};
        use differential_dataflow::trace::Description;

        use crate::Updates;
        use crate::layout::ColumnarUpdate as Update;
        use crate::layout::ColumnarLayout as Layout;
        use crate::arrangement::Coltainer;

        use columnar::{Borrow, IndexAs};
        use columnar::primitive::offsets::Strides;
        use differential_dataflow::trace::implementations::OffsetList;
        fn strides_to_offset_list(bounds: &Strides, count: usize) -> OffsetList {
            let mut output = OffsetList::with_capacity(count);
            output.push(0);
            let bounds_b = bounds.borrow();
            for i in 0..count {
                output.push(bounds_b.index_as(i) as usize);
            }
            output
        }

        pub struct ValMirror<U: Update> {
            chunks: Vec<Updates<U>>,
        }
        impl<U: Update> differential_dataflow::trace::Builder for ValMirror<U> {
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
                let mut builder = crate::updates::UpdatesBuilder::new_from(updates);
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
}

pub mod updates {

    use columnar::{Columnar, Container, ContainerOf, Vecs, Borrow, Index, IndexAs, Len, Push};
    use columnar::primitive::offsets::Strides;
    use differential_dataflow::difference::{Semigroup, IsZero};

    use crate::layout::ColumnarUpdate as Update;

    /// A `Vecs` using strided offsets.
    pub type Lists<C> = Vecs<C, Strides>;

    /// Trie-structured update storage using columnar containers.
    ///
    /// Four nested layers of `Lists`:
    /// - `keys`: lists of keys (outer lists are independent groups)
    /// - `vals`: per-key, lists of vals
    /// - `times`: per-val, lists of times
    /// - `diffs`: per-time, lists of diffs (singletons when consolidated)
    ///
    /// A flat unsorted input has stride 1 at every level (one key per entry,
    /// one val per key, one time per val, one diff per time).
    /// A fully consolidated trie has a single outer key list, all lists sorted
    /// and deduplicated, and singleton diff lists.
    pub struct Updates<U: Update> {
        pub keys:  Lists<ContainerOf<U::Key>>,
        pub vals:  Lists<ContainerOf<U::Val>>,
        pub times: Lists<ContainerOf<U::Time>>,
        pub diffs: Lists<ContainerOf<U::Diff>>,
    }

    impl<U: Update> Default for Updates<U> {
        fn default() -> Self {
            Self {
                keys: Default::default(),
                vals: Default::default(),
                times: Default::default(),
                diffs: Default::default(),
            }
        }
    }

    impl<U: Update> std::fmt::Debug for Updates<U> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Updates").finish()
        }
    }

    impl<U: Update> Clone for Updates<U> {
        fn clone(&self) -> Self {
            Self {
                keys: self.keys.clone(),
                vals: self.vals.clone(),
                times: self.times.clone(),
                diffs: self.diffs.clone(),
            }
        }
    }

    pub type Tuple<U> = (<U as Update>::Key, <U as Update>::Val, <U as Update>::Time, <U as Update>::Diff);

    /// Returns the value-index range for list `i` given cumulative bounds.
    #[inline]
    pub fn child_range<B: IndexAs<u64>>(bounds: B, i: usize) -> std::ops::Range<usize> {
        let lower = if i == 0 { 0 } else { bounds.index_as(i - 1) as usize };
        let upper = bounds.index_as(i) as usize;
        lower..upper
    }

    /// A streaming consolidation iterator for sorted `(key, val, time, diff)` data.
    ///
    /// Accumulates diffs for equal `(key, val, time)` triples, yielding at most
    /// one output per distinct triple, with a non-zero accumulated diff.
    /// Input must be sorted by `(key, val, time)`.
    pub struct Consolidating<I: Iterator, D> {
        iter: std::iter::Peekable<I>,
        diff: D,
    }

    impl<K, V, T, D, I> Consolidating<I, D>
    where
        K: Copy + Eq,
        V: Copy + Eq,
        T: Copy + Eq,
        D: Semigroup + IsZero + Default,
        I: Iterator<Item = (K, V, T, D)>,
    {
        pub fn new(iter: I) -> Self {
            Self { iter: iter.peekable(), diff: D::default() }
        }
    }

    impl<K, V, T, D, I> Iterator for Consolidating<I, D>
    where
        K: Copy + Eq,
        V: Copy + Eq,
        T: Copy + Eq,
        D: Semigroup + IsZero + Default + Clone,
        I: Iterator<Item = (K, V, T, D)>,
    {
        type Item = (K, V, T, D);
        fn next(&mut self) -> Option<Self::Item> {
            loop {
                let (k, v, t, d) = self.iter.next()?;
                self.diff = d;
                while let Some(&(k2, v2, t2, _)) = self.iter.peek() {
                    if k2 == k && v2 == v && t2 == t {
                        let (_, _, _, d2) = self.iter.next().unwrap();
                        self.diff.plus_equals(&d2);
                    } else {
                        break;
                    }
                }
                if !self.diff.is_zero() {
                    return Some((k, v, t, self.diff.clone()));
                }
            }
        }
    }

    impl<U: Update> Updates<U> {

        pub fn vals_bounds(&self, key_range: std::ops::Range<usize>) -> std::ops::Range<usize> {
            if !key_range.is_empty() {
                let bounds = self.vals.bounds.borrow();
                let lower = if key_range.start == 0 { 0 } else { bounds.index_as(key_range.start - 1) as usize };
                let upper = bounds.index_as(key_range.end - 1) as usize;
                lower..upper
            } else { key_range }
        }
        pub fn times_bounds(&self, val_range: std::ops::Range<usize>) -> std::ops::Range<usize> {
            if !val_range.is_empty() {
                let bounds = self.times.bounds.borrow();
                let lower = if val_range.start == 0 { 0 } else { bounds.index_as(val_range.start - 1) as usize };
                let upper = bounds.index_as(val_range.end - 1) as usize;
                lower..upper
            } else { val_range }
        }
        pub fn diffs_bounds(&self, time_range: std::ops::Range<usize>) -> std::ops::Range<usize> {
            if !time_range.is_empty() {
                let bounds = self.diffs.bounds.borrow();
                let lower = if time_range.start == 0 { 0 } else { bounds.index_as(time_range.start - 1) as usize };
                let upper = bounds.index_as(time_range.end - 1) as usize;
                lower..upper
            } else { time_range }
        }

        /// Copies `other[key_range]` into self, keys and all.
        pub fn extend_from_keys(&mut self, other: &Self, key_range: std::ops::Range<usize>) {
            self.keys.values.extend_from_self(other.keys.values.borrow(), key_range.clone());
            self.vals.extend_from_self(other.vals.borrow(), key_range.clone());
            let val_range = other.vals_bounds(key_range);
            self.times.extend_from_self(other.times.borrow(), val_range.clone());
            let time_range = other.times_bounds(val_range);
            self.diffs.extend_from_self(other.diffs.borrow(), time_range);
        }

        /// Copies a range of vals (with their times and diffs) from `other` into self.
        pub fn extend_from_vals(&mut self, other: &Self, val_range: std::ops::Range<usize>) {
            self.vals.values.extend_from_self(other.vals.values.borrow(), val_range.clone());
            self.times.extend_from_self(other.times.borrow(), val_range.clone());
            let time_range = other.times_bounds(val_range);
            self.diffs.extend_from_self(other.diffs.borrow(), time_range);
        }

        /// Forms a consolidated `Updates` trie from unsorted `(key, val, time, diff)` refs.
        pub fn form_unsorted<'a>(unsorted: impl Iterator<Item = columnar::Ref<'a, Tuple<U>>>) -> Self {
            let mut data = unsorted.collect::<Vec<_>>();
            data.sort();
            Self::form(data.into_iter())
        }

        /// Forms a consolidated `Updates` trie from sorted `(key, val, time, diff)` refs.
        pub fn form<'a>(sorted: impl Iterator<Item = columnar::Ref<'a, Tuple<U>>>) -> Self {

            // Step 1: Streaming consolidation — accumulate diffs, drop zeros.
            let consolidated = Consolidating::new(
                sorted.map(|(k, v, t, d)| (k, v, t, <U::Diff as Columnar>::into_owned(d)))
            );

            // Step 2: Build the trie from consolidated, sorted, non-zero data.
            let mut output = Self::default();
            let mut updates = consolidated;
            if let Some((key, val, time, diff)) = updates.next() {
                let mut prev = (key, val, time);
                output.keys.values.push(key);
                output.vals.values.push(val);
                output.times.values.push(time);
                output.diffs.values.push(&diff);
                output.diffs.bounds.push(output.diffs.values.len() as u64);

                // As we proceed, seal up known complete runs.
                for (key, val, time, diff) in updates {

                    // If keys differ, record key and seal vals and times.
                    if key != prev.0 {
                        output.vals.bounds.push(output.vals.values.len() as u64);
                        output.times.bounds.push(output.times.values.len() as u64);
                        output.keys.values.push(key);
                        output.vals.values.push(val);
                    }
                    // If vals differ, record val and seal times.
                    else if val != prev.1 {
                        output.times.bounds.push(output.times.values.len() as u64);
                        output.vals.values.push(val);
                    }
                    else {
                        // We better not find a duplicate time.
                        assert!(time != prev.2);
                    }

                    // Always record (time, diff).
                    output.times.values.push(time);
                    output.diffs.values.push(&diff);
                    output.diffs.bounds.push(output.diffs.values.len() as u64);

                    prev = (key, val, time);
                }

                // Seal up open lists.
                output.keys.bounds.push(output.keys.values.len() as u64);
                output.vals.bounds.push(output.vals.values.len() as u64);
                output.times.bounds.push(output.times.values.len() as u64);
            }

            output
        }

        /// Consolidates into canonical trie form:
        /// single outer key list, all lists sorted and deduplicated,
        /// diff lists are singletons (or absent if cancelled).
        pub fn consolidate(self) -> Self { Self::form_unsorted(self.iter()) }
        pub fn filter_zero(self) -> Self { Self::form(self.iter()) }

        /// The number of leaf-level diff entries (total updates).
        pub fn len(&self) -> usize { self.diffs.values.len() }
    }

    /// Push a single flat update as a stride-1 entry.
    ///
    /// Each field is independently typed — columnar refs, `&Owned`, owned values,
    /// or any other type the column container accepts via its `Push` impl.
    impl<KP, VP, TP, DP, U: Update> Push<(KP, VP, TP, DP)> for Updates<U>
    where
        ContainerOf<U::Key>: Push<KP>,
        ContainerOf<U::Val>: Push<VP>,
        ContainerOf<U::Time>: Push<TP>,
        ContainerOf<U::Diff>: Push<DP>,
    {
        fn push(&mut self, (key, val, time, diff): (KP, VP, TP, DP)) {
            self.keys.values.push(key);
            self.keys.bounds.push(self.keys.values.len() as u64);
            self.vals.values.push(val);
            self.vals.bounds.push(self.vals.values.len() as u64);
            self.times.values.push(time);
            self.times.bounds.push(self.times.values.len() as u64);
            self.diffs.values.push(diff);
            self.diffs.bounds.push(self.diffs.values.len() as u64);
        }
    }

    /// PushInto for the `((K, V), T, R)` shape that reduce_trace uses.
    impl<U: Update> timely::container::PushInto<((U::Key, U::Val), U::Time, U::Diff)> for Updates<U> {
        fn push_into(&mut self, ((key, val), time, diff): ((U::Key, U::Val), U::Time, U::Diff)) {
            self.push((&key, &val, &time, &diff));
        }
    }

    impl<U: Update> Updates<U> {

        /// Iterate all `(key, val, time, diff)` entries as refs.
        pub fn iter(&self) -> impl Iterator<Item = (
            columnar::Ref<'_, U::Key>,
            columnar::Ref<'_, U::Val>,
            columnar::Ref<'_, U::Time>,
            columnar::Ref<'_, U::Diff>,
        )> {
            let keys_b = self.keys.borrow();
            let vals_b = self.vals.borrow();
            let times_b = self.times.borrow();
            let diffs_b = self.diffs.borrow();

            (0..Len::len(&keys_b))
                .flat_map(move |outer| child_range(keys_b.bounds, outer))
                .flat_map(move |k| {
                    let key = keys_b.values.get(k);
                    child_range(vals_b.bounds, k).map(move |v| (key, v))
                })
                .flat_map(move |(key, v)| {
                    let val = vals_b.values.get(v);
                    child_range(times_b.bounds, v).map(move |t| (key, val, t))
                })
                .flat_map(move |(key, val, t)| {
                    let time = times_b.values.get(t);
                    child_range(diffs_b.bounds, t).map(move |d| (key, val, time, diffs_b.values.get(d)))
                })
        }
    }

    impl<U: Update> timely::Accountable for Updates<U> {
        #[inline] fn record_count(&self) -> i64 { Len::len(&self.diffs.values) as i64 }
    }

    impl<U: Update> timely::dataflow::channels::ContainerBytes for Updates<U> {
        fn from_bytes(_bytes: timely::bytes::arc::Bytes) -> Self { unimplemented!() }
        fn length_in_bytes(&self) -> usize { unimplemented!() }
        fn into_bytes<W: std::io::Write>(&self, _writer: &mut W) { unimplemented!() }
    }

    /// An incremental trie builder that accepts sorted, consolidated `Updates` chunks
    /// and melds them into a single `Updates` trie.
    ///
    /// The internal `Updates` has open (unsealed) bounds at the keys, vals, and times
    /// levels — the last group at each level has its values pushed but no corresponding
    /// bounds entry. `diffs.bounds` is always 1:1 with `times.values`.
    ///
    /// `meld` accepts a consolidated `Updates` whose first `(key, val, time)` is
    /// strictly greater than the builder's last `(key, val, time)`. The key and val
    /// may equal the builder's current open key/val, as long as the time is greater.
    ///
    /// `done` seals all open bounds and returns the completed `Updates`.
    pub struct UpdatesBuilder<U: Update> {
        /// Non-empty, consolidated updates.
        updates: Updates<U>,
    }

    impl<U: Update> UpdatesBuilder<U> {
        /// Construct a new builder from consolidated, sealed updates.
        ///
        /// Unseals the last group at keys, vals, and times levels so that
        /// subsequent `meld` calls can extend the open groups.
        /// If the updates are not consolidated none of this works.
        pub fn new_from(mut updates: Updates<U>) -> Self {
            use columnar::Len;
            if Len::len(&updates.keys.values) > 0 {
                updates.keys.bounds.pop();
                updates.vals.bounds.pop();
                updates.times.bounds.pop();
            }
            Self { updates }
        }

        /// Meld a sorted, consolidated `Updates` chunk into this builder.
        ///
        /// The chunk's first `(key, val, time)` must be strictly greater than
        /// the builder's last `(key, val, time)`. Keys and vals may overlap
        /// (continue the current group), but times must be strictly increasing
        /// within the same `(key, val)`.
        pub fn meld(&mut self, chunk: &Updates<U>) {
            use columnar::{Borrow, Index, Len};

            if chunk.len() == 0 { return; }

            // Empty builder: clone the chunk and unseal it.
            if Len::len(&self.updates.keys.values) == 0 {
                self.updates = chunk.clone();
                self.updates.keys.bounds.pop();
                self.updates.vals.bounds.pop();
                self.updates.times.bounds.pop();
                return;
            }

            // Pre-compute boundary comparisons before mutating.
            let keys_match = {
                let skb = self.updates.keys.values.borrow();
                let ckb = chunk.keys.values.borrow();
                skb.get(Len::len(&skb) - 1) == ckb.get(0)
            };
            let vals_match = keys_match && {
                let svb = self.updates.vals.values.borrow();
                let cvb = chunk.vals.values.borrow();
                svb.get(Len::len(&svb) - 1) == cvb.get(0)
            };

            let chunk_num_keys = Len::len(&chunk.keys.values);
            let chunk_num_vals = Len::len(&chunk.vals.values);
            let chunk_num_times = Len::len(&chunk.times.values);

            // Child ranges for the first element at each level of the chunk.
            let first_key_vals = child_range(chunk.vals.borrow().bounds, 0);
            let first_val_times = child_range(chunk.times.borrow().bounds, 0);

            // There is a first position where coordinates disagree.
            // Strictly beyond that position: seal bounds, extend lists, re-open the last bound.
            // At that position: meld the first list, extend subsequent lists, re-open.
            let mut differ = false;

            // --- Keys ---
            if keys_match {
                // Skip the duplicate first key; add remaining keys.
                if chunk_num_keys > 1 {
                    self.updates.keys.values.extend_from_self(chunk.keys.values.borrow(), 1..chunk_num_keys);
                }
            } else {
                // All keys are new.
                self.updates.keys.values.extend_from_self(chunk.keys.values.borrow(), 0..chunk_num_keys);
                differ = true;
            }

            // --- Vals ---
            if differ {
                // Keys differed: seal open val group, extend all val lists, unseal last.
                self.updates.vals.bounds.push(Len::len(&self.updates.vals.values) as u64);
                self.updates.vals.extend_from_self(chunk.vals.borrow(), 0..chunk_num_keys);
                self.updates.vals.bounds.pop();
            } else {
                // Keys matched: meld vals for the shared key.
                if vals_match {
                    // Skip the duplicate first val; add remaining vals from the first key's list.
                    if first_key_vals.len() > 1 {
                        self.updates.vals.values.extend_from_self(
                            chunk.vals.values.borrow(),
                            (first_key_vals.start + 1)..first_key_vals.end,
                        );
                    }
                } else {
                    // First val differs: add all vals from the first key's list.
                    self.updates.vals.values.extend_from_self(
                        chunk.vals.values.borrow(),
                        first_key_vals.clone(),
                    );
                    differ = true;
                }
                // Seal the matched key's val group, extend remaining keys' val lists, unseal.
                if chunk_num_keys > 1 {
                    self.updates.vals.bounds.push(Len::len(&self.updates.vals.values) as u64);
                    self.updates.vals.extend_from_self(chunk.vals.borrow(), 1..chunk_num_keys);
                    self.updates.vals.bounds.pop();
                }
            }

            // --- Times ---
            if differ {
                // Seal open time group, extend all time lists, unseal last.
                self.updates.times.bounds.push(Len::len(&self.updates.times.values) as u64);
                self.updates.times.extend_from_self(chunk.times.borrow(), 0..chunk_num_vals);
                self.updates.times.bounds.pop();
            } else {
                // Keys and vals matched. Times must be strictly greater (precondition),
                // so we always set differ = true here.
                debug_assert!({
                    let stb = self.updates.times.values.borrow();
                    let ctb = chunk.times.values.borrow();
                    stb.get(Len::len(&stb) - 1) != ctb.get(0)
                }, "meld: duplicate time within same (key, val)");
                // Add times from the first val's time list into the open group.
                self.updates.times.values.extend_from_self(
                    chunk.times.values.borrow(),
                    first_val_times.clone(),
                );
                differ = true;
                // Seal the matched val's time group, extend remaining vals' time lists, unseal.
                if chunk_num_vals > 1 {
                    self.updates.times.bounds.push(Len::len(&self.updates.times.values) as u64);
                    self.updates.times.extend_from_self(chunk.times.borrow(), 1..chunk_num_vals);
                    self.updates.times.bounds.pop();
                }
            }

            // --- Diffs ---
            // Diffs are always sealed (1:1 with times). By the precondition that
            // times are strictly increasing for the same (key, val), differ is
            // always true by this point — just extend all diff lists.
            debug_assert!(differ);
            self.updates.diffs.extend_from_self(chunk.diffs.borrow(), 0..chunk_num_times);
        }

        /// Seal all open bounds and return the completed `Updates`.
        pub fn done(mut self) -> Updates<U> {
            use columnar::Len;
            if Len::len(&self.updates.keys.values) > 0 {
                // Seal the open time group.
                self.updates.times.bounds.push(Len::len(&self.updates.times.values) as u64);
                // Seal the open val group.
                self.updates.vals.bounds.push(Len::len(&self.updates.vals.values) as u64);
                // Seal the outer key group.
                self.updates.keys.bounds.push(Len::len(&self.updates.keys.values) as u64);
            }
            self.updates
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use columnar::Push;

        type TestUpdate = (u64, u64, u64, i64);

        fn collect(updates: &Updates<TestUpdate>) -> Vec<(u64, u64, u64, i64)> {
            updates.iter().map(|(k, v, t, d)| (*k, *v, *t, *d)).collect()
        }

        #[test]
        fn test_push_and_consolidate_basic() {
            let mut updates = Updates::<TestUpdate>::default();
            updates.push((&1, &10, &100, &1));
            updates.push((&1, &10, &100, &2));
            updates.push((&2, &20, &200, &5));
            assert_eq!(updates.len(), 3);
            assert_eq!(collect(&updates.consolidate()), vec![(1, 10, 100, 3), (2, 20, 200, 5)]);
        }

        #[test]
        fn test_cancellation() {
            let mut updates = Updates::<TestUpdate>::default();
            updates.push((&1, &10, &100, &3));
            updates.push((&1, &10, &100, &-3));
            updates.push((&2, &20, &200, &1));
            assert_eq!(collect(&updates.consolidate()), vec![(2, 20, 200, 1)]);
        }

        #[test]
        fn test_multiple_vals_and_times() {
            let mut updates = Updates::<TestUpdate>::default();
            updates.push((&1, &10, &100, &1));
            updates.push((&1, &10, &200, &2));
            updates.push((&1, &20, &100, &3));
            updates.push((&1, &20, &100, &4));
            assert_eq!(collect(&updates.consolidate()), vec![(1, 10, 100, 1), (1, 10, 200, 2), (1, 20, 100, 7)]);
        }

        #[test]
        fn test_val_cancellation_propagates() {
            let mut updates = Updates::<TestUpdate>::default();
            updates.push((&1, &10, &100, &5));
            updates.push((&1, &10, &100, &-5));
            updates.push((&1, &20, &100, &1));
            assert_eq!(collect(&updates.consolidate()), vec![(1, 20, 100, 1)]);
        }

        #[test]
        fn test_empty() {
            let updates = Updates::<TestUpdate>::default();
            assert_eq!(collect(&updates.consolidate()), vec![]);
        }

        #[test]
        fn test_total_cancellation() {
            let mut updates = Updates::<TestUpdate>::default();
            updates.push((&1, &10, &100, &1));
            updates.push((&1, &10, &100, &-1));
            assert_eq!(collect(&updates.consolidate()), vec![]);
        }

        #[test]
        fn test_unsorted_input() {
            let mut updates = Updates::<TestUpdate>::default();
            updates.push((&3, &30, &300, &1));
            updates.push((&1, &10, &100, &2));
            updates.push((&2, &20, &200, &3));
            assert_eq!(collect(&updates.consolidate()), vec![(1, 10, 100, 2), (2, 20, 200, 3), (3, 30, 300, 1)]);
        }

        #[test]
        fn test_first_key_cancels() {
            let mut updates = Updates::<TestUpdate>::default();
            updates.push((&1, &10, &100, &5));
            updates.push((&1, &10, &100, &-5));
            updates.push((&2, &20, &200, &3));
            assert_eq!(collect(&updates.consolidate()), vec![(2, 20, 200, 3)]);
        }

        #[test]
        fn test_middle_time_cancels() {
            let mut updates = Updates::<TestUpdate>::default();
            updates.push((&1, &10, &100, &1));
            updates.push((&1, &10, &200, &2));
            updates.push((&1, &10, &200, &-2));
            updates.push((&1, &10, &300, &3));
            assert_eq!(collect(&updates.consolidate()), vec![(1, 10, 100, 1), (1, 10, 300, 3)]);
        }

        #[test]
        fn test_first_val_cancels() {
            let mut updates = Updates::<TestUpdate>::default();
            updates.push((&1, &10, &100, &1));
            updates.push((&1, &10, &100, &-1));
            updates.push((&1, &20, &100, &5));
            assert_eq!(collect(&updates.consolidate()), vec![(1, 20, 100, 5)]);
        }

        #[test]
        fn test_interleaved_cancellations() {
            let mut updates = Updates::<TestUpdate>::default();
            updates.push((&1, &10, &100, &1));
            updates.push((&1, &10, &100, &-1));
            updates.push((&2, &20, &200, &7));
            updates.push((&3, &30, &300, &4));
            updates.push((&3, &30, &300, &-4));
            assert_eq!(collect(&updates.consolidate()), vec![(2, 20, 200, 7)]);
        }
    }
}

/// A columnar flat_map: iterates RecordedUpdates, calls logic per (key, val, time, diff),
/// joins output times with input times, multiplies output diffs with input diffs.
///
/// This subsumes map, filter, negate, and enter_at for columnar collections.
pub fn join_function<U, I, L>(
    input: differential_dataflow::Collection<U::Time, RecordedUpdates<U>>,
    mut logic: L,
) -> differential_dataflow::Collection<U::Time, RecordedUpdates<U>>
where
    U::Time: differential_dataflow::lattice::Lattice,
    U: layout::ColumnarUpdate<Diff: differential_dataflow::difference::Multiply<U::Diff, Output = U::Diff>>,
    I: IntoIterator<Item = (U::Key, U::Val, U::Time, U::Diff)>,
    L: FnMut(
        columnar::Ref<'_, U::Key>,
        columnar::Ref<'_, U::Val>,
        columnar::Ref<'_, U::Time>,
        columnar::Ref<'_, U::Diff>,
    ) -> I + 'static,
{
    use timely::dataflow::operators::generic::Operator;
    use timely::dataflow::channels::pact::Pipeline;
    use differential_dataflow::AsCollection;
    use differential_dataflow::difference::Multiply;
    use differential_dataflow::lattice::Lattice;
    use columnar::Columnar;

    input
        .inner
        .unary::<ValColBuilder<U>, _, _, _>(Pipeline, "JoinFunction", move |_, _| {
            move |input, output| {
                let mut t1o = U::Time::default();
                let mut d1o = U::Diff::default();
                input.for_each(|time, data| {
                    let mut session = output.session_with_builder(&time);
                    for (k1, v1, t1, d1) in data.updates.iter() {
                        Columnar::copy_from(&mut t1o, t1);
                        Columnar::copy_from(&mut d1o, d1);
                        for (k2, v2, t2, d2) in logic(k1, v1, t1, d1) {
                            let t3 = t2.join(&t1o);
                            let d3 = d2.multiply(&d1o);
                            session.give((&k2, &v2, &t3, &d3));
                        }
                    }
                });
            }
        })
        .as_collection()
}

type DynTime = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;

/// Leave a dynamic iterative scope, truncating PointStamp coordinates.
///
/// Uses OperatorBuilder (not unary) for the custom input connection summary
/// that tells timely how the PointStamp is affected (retain `level - 1` coordinates).
///
/// Consolidates after truncation since distinct PointStamp coordinates can collapse.
pub fn leave_dynamic<K, V, R>(
    input: differential_dataflow::Collection<DynTime, RecordedUpdates<(K, V, DynTime, R)>>,
    level: usize,
) -> differential_dataflow::Collection<DynTime, RecordedUpdates<(K, V, DynTime, R)>>
where
    K: columnar::Columnar,
    V: columnar::Columnar,
    R: columnar::Columnar,
    (K, V, DynTime, R): layout::ColumnarUpdate<Key = K, Val = V, Time = DynTime, Diff = R>,
{
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
    use timely::dataflow::operators::generic::OutputBuilder;
    use timely::order::Product;
    use timely::progress::Antichain;
    use timely::container::{ContainerBuilder, PushInto};
    use differential_dataflow::AsCollection;
    use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};
    use columnar::Columnar;

    let mut builder = OperatorBuilder::new("LeaveDynamic".to_string(), input.inner.scope());
    let (output, stream) = builder.new_output();
    let mut output = OutputBuilder::from(output);
    let mut op_input = builder.new_input_connection(
        input.inner,
        Pipeline,
        [(
            0,
            Antichain::from_elem(Product {
                outer: Default::default(),
                inner: PointStampSummary {
                    retain: Some(level - 1),
                    actions: Vec::new(),
                },
            }),
        )],
    );

    builder.build(move |_capability| {
        let mut col_builder = ValColBuilder::<(K, V, DynTime, R)>::default();
        let mut time = DynTime::default();
        move |_frontier| {
            let mut output = output.activate();
            op_input.for_each(|cap, data| {
                // Truncate the capability's timestamp.
                let mut new_time = cap.time().clone();
                let mut vec = std::mem::take(&mut new_time.inner).into_inner();
                vec.truncate(level - 1);
                new_time.inner = PointStamp::new(vec);
                let new_cap = cap.delayed(&new_time, 0);
                // Push updates with truncated times into the builder.
                // The builder's form call on flush sorts and consolidates,
                // handling the duplicate times that truncation can produce.
                // TODO: The input trie is already sorted; a streaming form
                // that accepts pre-sorted, potentially-collapsing timestamps
                // could avoid the re-sort inside the builder.
                for (k, v, t, d) in data.updates.iter() {
                    Columnar::copy_from(&mut time, t);
                    let mut inner_vec = std::mem::take(&mut time.inner).into_inner();
                    inner_vec.truncate(level - 1);
                    time.inner = PointStamp::new(inner_vec);
                    col_builder.push_into((k, v, &time, d));
                }
                let mut session = output.session(&new_cap);
                while let Some(container) = col_builder.finish() {
                    session.give_container(container);
                }
            });
        }
    });

    stream.as_collection()
}

/// Extract a `Collection<_, RecordedUpdates<U>>` from a columnar `Arranged`.
///
/// Cursors through each batch and pushes `(key, val, time, diff)` refs into
/// a `ValColBuilder`, which sorts and consolidates on flush.
pub fn as_recorded_updates<U>(
    arranged: differential_dataflow::operators::arrange::Arranged<
        U::Time,
        differential_dataflow::operators::arrange::TraceAgent<ValSpine<U::Key, U::Val, U::Time, U::Diff>>,
    >,
) -> differential_dataflow::Collection<U::Time, RecordedUpdates<U>>
where
    U: layout::ColumnarUpdate,
{
    use timely::dataflow::operators::generic::Operator;
    use timely::dataflow::channels::pact::Pipeline;
    use differential_dataflow::trace::{BatchReader, Cursor};
    use differential_dataflow::AsCollection;

    arranged.stream
        .unary::<ValColBuilder<U>, _, _, _>(Pipeline, "AsRecordedUpdates", |_, _| {
            move |input, output| {
                input.for_each(|time, batches| {
                    let mut session = output.session_with_builder(&time);
                    for batch in batches.drain(..) {
                        let mut cursor = batch.cursor();
                        while cursor.key_valid(&batch) {
                            while cursor.val_valid(&batch) {
                                let key = cursor.key(&batch);
                                let val = cursor.val(&batch);
                                cursor.map_times(&batch, |time, diff| {
                                    session.give((key, val, time, diff));
                                });
                                cursor.step_val(&batch);
                            }
                            cursor.step_key(&batch);
                        }
                    }
                });
            }
        })
        .as_collection()
}
