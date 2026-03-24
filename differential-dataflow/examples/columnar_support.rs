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
}

impl<U: layout::ColumnarUpdate> Default for RecordedUpdates<U> {
    fn default() -> Self { Self { updates: Default::default(), records: 0 } }
}

impl<U: layout::ColumnarUpdate> Clone for RecordedUpdates<U> {
    fn clone(&self) -> Self { Self { updates: self.updates.clone(), records: self.records } }
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
            RecordedUpdates {
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
            RecordedUpdates { updates: output, records: self.records }
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
                self.pending.push_back(RecordedUpdates { updates, records });
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
                self.pending.push_back(RecordedUpdates { updates, records });
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
                    let recorded = RecordedUpdates { updates: output, records: first_records };
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
    use differential_dataflow::trace::implementations::merge_batcher::{MergeBatcher, InternalMerger};
    type ValBatcher2<U> = MergeBatcher<RecordedUpdates<U>, TrieChunker<U>, InternalMerger<Updates<U>>>;

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
            self.ready.push_back(std::mem::take(&mut container.updates));
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

        use std::ops::Range;
        use columnar::{Borrow, Columnar, Container, Index, Len, Push};
        use differential_dataflow::difference::{Semigroup, IsZero};
        use timely::progress::frontier::{Antichain, AntichainRef};
        use differential_dataflow::trace::implementations::merge_batcher::container::InternalMerge;

        use crate::ColumnarUpdate as Update;
        use crate::Updates;

        impl<U: Update> timely::container::SizableContainer for Updates<U> {
            fn at_capacity(&self) -> bool {
                use columnar::Len;
                self.diffs.values.len() >= 64 * 1024
            }
            fn ensure_capacity(&mut self, _stash: &mut Option<Self>) { }
        }

        impl<U: Update> InternalMerge for Updates<U> {

            type TimeOwned = U::Time;

            fn len(&self) -> usize { self.diffs.values.len() }
            fn clear(&mut self) { *self = Self::default(); }

            #[inline(never)]
            fn merge_from(&mut self, others: &mut [Self], positions: &mut [usize]) {
                match others.len() {
                    0 => {},
                    1 => {
                        // Bulk copy: take remaining keys from position onward.
                        let other = &mut others[0];
                        let pos = &mut positions[0];
                        if self.keys.values.len() == 0 && *pos == 0 {
                            std::mem::swap(self, other);
                            return;
                        }
                        let other_len = other.keys.values.len();
                        self.extend_from_keys(other, *pos .. other_len);
                        *pos = other_len;
                    },
                    2 => {
                        let mut this_sum = U::Diff::default();
                        let mut that_sum = U::Diff::default();

                        let (left, right) = others.split_at_mut(1);
                        let this = &left[0];
                        let that = &right[0];
                        let this_keys = this.keys.values.borrow();
                        let that_keys = that.keys.values.borrow();
                        let mut this_key_range = positions[0] .. this_keys.len();
                        let mut that_key_range = positions[1] .. that_keys.len();

                        while !this_key_range.is_empty() && !that_key_range.is_empty() && !timely::container::SizableContainer::at_capacity(self) {
                            let this_key = this_keys.get(this_key_range.start);
                            let that_key = that_keys.get(that_key_range.start);
                            match this_key.cmp(&that_key) {
                                std::cmp::Ordering::Less => {
                                    let lower = this_key_range.start;
                                    gallop(this_keys, &mut this_key_range, |x| x < that_key);
                                    self.extend_from_keys(this, lower .. this_key_range.start);
                                },
                                std::cmp::Ordering::Equal => {
                                    let values_len = self.vals.values.len();
                                    let mut this_val_range = this.vals_bounds(this_key_range.start .. this_key_range.start+1);
                                    let mut that_val_range = that.vals_bounds(that_key_range.start .. that_key_range.start+1);
                                    while !this_val_range.is_empty() && !that_val_range.is_empty() {
                                        let this_val = this.vals.values.borrow().get(this_val_range.start);
                                        let that_val = that.vals.values.borrow().get(that_val_range.start);
                                        match this_val.cmp(&that_val) {
                                            std::cmp::Ordering::Less => {
                                                let lower = this_val_range.start;
                                                gallop(this.vals.values.borrow(), &mut this_val_range, |x| x < that_val);
                                                self.extend_from_vals(this, lower .. this_val_range.start);
                                            },
                                            std::cmp::Ordering::Equal => {
                                                let updates_len = self.times.values.len();
                                                let mut this_time_range = this.times_bounds(this_val_range.start .. this_val_range.start+1);
                                                let mut that_time_range = that.times_bounds(that_val_range.start .. that_val_range.start+1);
                                                while !this_time_range.is_empty() && !that_time_range.is_empty() {
                                                    let this_time = this.times.values.borrow().get(this_time_range.start);
                                                    let this_diff = this.diffs.values.borrow().get(this_time_range.start);
                                                    let that_time = that.times.values.borrow().get(that_time_range.start);
                                                    let that_diff = that.diffs.values.borrow().get(that_time_range.start);
                                                    match this_time.cmp(&that_time) {
                                                        std::cmp::Ordering::Less => {
                                                            let lower = this_time_range.start;
                                                            gallop(this.times.values.borrow(), &mut this_time_range, |x| x < that_time);
                                                            self.times.values.extend_from_self(this.times.values.borrow(), lower .. this_time_range.start);
                                                            self.diffs.extend_from_self(this.diffs.borrow(), lower .. this_time_range.start);
                                                        },
                                                        std::cmp::Ordering::Equal => {
                                                            this_sum.copy_from(this_diff);
                                                            that_sum.copy_from(that_diff);
                                                            this_sum.plus_equals(&that_sum);
                                                            if !this_sum.is_zero() {
                                                                self.times.values.push(this_time);
                                                                self.diffs.values.push(&this_sum);
                                                                self.diffs.bounds.push(self.diffs.values.len() as u64);
                                                            }
                                                            this_time_range.start += 1;
                                                            that_time_range.start += 1;
                                                        },
                                                        std::cmp::Ordering::Greater => {
                                                            let lower = that_time_range.start;
                                                            gallop(that.times.values.borrow(), &mut that_time_range, |x| x < this_time);
                                                            self.times.values.extend_from_self(that.times.values.borrow(), lower .. that_time_range.start);
                                                            self.diffs.extend_from_self(that.diffs.borrow(), lower .. that_time_range.start);
                                                        },
                                                    }
                                                }
                                                // Remaining from this side
                                                if !this_time_range.is_empty() {
                                                    self.times.values.extend_from_self(this.times.values.borrow(), this_time_range.clone());
                                                    self.diffs.extend_from_self(this.diffs.borrow(), this_time_range.clone());
                                                }
                                                // Remaining from that side
                                                if !that_time_range.is_empty() {
                                                    self.times.values.extend_from_self(that.times.values.borrow(), that_time_range.clone());
                                                    self.diffs.extend_from_self(that.diffs.borrow(), that_time_range.clone());
                                                }
                                                if self.times.values.len() > updates_len {
                                                    self.times.bounds.push(self.times.values.len() as u64);
                                                    self.vals.values.push(this_val);
                                                }
                                                this_val_range.start += 1;
                                                that_val_range.start += 1;
                                            },
                                            std::cmp::Ordering::Greater => {
                                                let lower = that_val_range.start;
                                                gallop(that.vals.values.borrow(), &mut that_val_range, |x| x < this_val);
                                                self.extend_from_vals(that, lower .. that_val_range.start);
                                            },
                                        }
                                    }
                                    self.extend_from_vals(this, this_val_range);
                                    self.extend_from_vals(that, that_val_range);
                                    if self.vals.values.len() > values_len {
                                        self.vals.bounds.push(self.vals.values.len() as u64);
                                        self.keys.values.push(this_key);
                                    }
                                    this_key_range.start += 1;
                                    that_key_range.start += 1;
                                },
                                std::cmp::Ordering::Greater => {
                                    let lower = that_key_range.start;
                                    gallop(that_keys, &mut that_key_range, |x| x < this_key);
                                    self.extend_from_keys(that, lower .. that_key_range.start);
                                },
                            }
                        }
                        // Copy remaining from whichever side has data, up to capacity.
                        while !this_key_range.is_empty() && !timely::container::SizableContainer::at_capacity(self) {
                            let lower = this_key_range.start;
                            this_key_range.start = this_key_range.end; // take all remaining
                            self.extend_from_keys(this, lower .. this_key_range.start);
                        }
                        while !that_key_range.is_empty() && !timely::container::SizableContainer::at_capacity(self) {
                            let lower = that_key_range.start;
                            that_key_range.start = that_key_range.end;
                            self.extend_from_keys(that, lower .. that_key_range.start);
                        }
                        positions[0] = this_key_range.start;
                        positions[1] = that_key_range.start;
                    },
                    n => unimplemented!("{n}-way merge not supported"),
                }
            }

            fn extract(
                &mut self,
                upper: AntichainRef<U::Time>,
                frontier: &mut Antichain<U::Time>,
                keep: &mut Self,
                ship: &mut Self,
            ) {
                let mut time = U::Time::default();
                for key_idx in 0 .. self.keys.values.len() {
                    let key = self.keys.values.borrow().get(key_idx);
                    let keep_vals_len = keep.vals.values.len();
                    let ship_vals_len = ship.vals.values.len();
                    for val_idx in self.vals_bounds(key_idx..key_idx+1) {
                        let val = self.vals.values.borrow().get(val_idx);
                        let keep_times_len = keep.times.values.len();
                        let ship_times_len = ship.times.values.len();
                        for time_idx in self.times_bounds(val_idx..val_idx+1) {
                            let t = self.times.values.borrow().get(time_idx);
                            let diff = self.diffs.values.borrow().get(time_idx);
                            time.copy_from(t);
                            if upper.less_equal(&time) {
                                frontier.insert_ref(&time);
                                keep.times.values.push(t);
                                keep.diffs.values.push(diff);
                                keep.diffs.bounds.push(keep.diffs.values.len() as u64);
                            }
                            else {
                                ship.times.values.push(t);
                                ship.diffs.values.push(diff);
                                ship.diffs.bounds.push(ship.diffs.values.len() as u64);
                            }
                        }
                        if keep.times.values.len() > keep_times_len {
                            keep.times.bounds.push(keep.times.values.len() as u64);
                            keep.vals.values.push(val);
                        }
                        if ship.times.values.len() > ship_times_len {
                            ship.times.bounds.push(ship.times.values.len() as u64);
                            ship.vals.values.push(val);
                        }
                    }
                    if keep.vals.values.len() > keep_vals_len {
                        keep.vals.bounds.push(keep.vals.values.len() as u64);
                        keep.keys.values.push(key);
                    }
                    if ship.vals.values.len() > ship_vals_len {
                        ship.vals.bounds.push(ship.vals.values.len() as u64);
                        ship.keys.values.push(key);
                    }
                }
            }
        }

        #[inline(always)]
        pub(crate) fn gallop<TC: columnar::Index>(input: TC, range: &mut Range<usize>, mut cmp: impl FnMut(<TC as columnar::Index>::Ref) -> bool) {
            // if empty input, or already >= element, return
            if !Range::<usize>::is_empty(range) && cmp(input.get(range.start)) {
                let mut step = 1;
                while range.start + step < range.end && cmp(input.get(range.start + step)) {
                    range.start += step;
                    step <<= 1;
                }

                step >>= 1;
                while step > 0 {
                    if range.start + step < range.end && cmp(input.get(range.start + step)) {
                        range.start += step;
                    }
                    step >>= 1;
                }

                range.start += 1;
            }
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
            current: Updates<U>,
        }
        impl<U: Update> differential_dataflow::trace::Builder for ValMirror<U> {
            type Time = U::Time;
            type Input = Updates<U>;
            type Output = OrdValBatch<Layout<U>>;

            fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
                Self { current: Updates::default() }
            }
            fn push(&mut self, chunk: &mut Self::Input) {
                use columnar::Len;
                let len = chunk.keys.values.len();
                if len > 0 {
                    self.current.extend_from_keys(chunk, 0..len);
                }
            }
            fn done(self, description: Description<Self::Time>) -> Self::Output {
                let mut chain = if self.current.len() > 0 {
                    vec![self.current]
                } else {
                    vec![]
                };
                Self::seal(&mut chain, description)
            }
            fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
                if chain.len() == 0 {
                    let storage = OrdValStorage {
                        keys: Default::default(),
                        vals: Default::default(),
                        upds: Default::default(),
                    };
                    OrdValBatch { storage, description, updates: 0 }
                }
                else if chain.len() == 1 {
                    use columnar::Len;
                    let storage = chain.pop().unwrap();
                    let updates = storage.diffs.values.len();
                    let val_offs = strides_to_offset_list(&storage.vals.bounds, storage.keys.values.len());
                    let time_offs = strides_to_offset_list(&storage.times.bounds, storage.vals.values.len());
                    let storage = OrdValStorage {
                        keys: Coltainer { container: storage.keys.values },
                        vals: Vals {
                            offs: val_offs,
                            vals: Coltainer { container: storage.vals.values },
                        },
                        upds: Upds {
                            offs: time_offs,
                            times: Coltainer { container: storage.times.values },
                            diffs: Coltainer { container: storage.diffs.values },
                        },
                    };
                    OrdValBatch { storage, description, updates }
                }
                else {
                    use columnar::Len;
                    let mut merged = chain.remove(0);
                    for other in chain.drain(..) {
                        let len = other.keys.values.len();
                        if len > 0 {
                            merged.extend_from_keys(&other, 0..len);
                        }
                    }
                    chain.push(merged);
                    Self::seal(chain, description)
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

        /// Forms a consolidated `Updates` from sorted `(key, val, time, diff)` refs.
        ///
        /// Tracks a `prev` reference to the previous element. On each new element,
        /// compares against `prev` to detect key/val/time changes. Only pushes
        /// accumulated diffs when they are nonzero, and only emits times/vals/keys
        /// that have at least one nonzero diff beneath them.
        pub fn form<'a>(mut sorted: impl Iterator<Item = columnar::Ref<'a, Tuple<U>>>) -> Self {

            let mut output = Self::default();
            let mut diff_stash = U::Diff::default();
            let mut diff_temp = U::Diff::default();

            if let Some(first) = sorted.next() {

                let mut prev = first;
                Columnar::copy_from(&mut diff_stash, prev.3);

                for curr in sorted {
                    let key_differs = ContainerOf::<U::Key>::reborrow_ref(curr.0) != ContainerOf::<U::Key>::reborrow_ref(prev.0);
                    let val_differs = key_differs || ContainerOf::<U::Val>::reborrow_ref(curr.1) != ContainerOf::<U::Val>::reborrow_ref(prev.1);
                    let time_differs = val_differs || ContainerOf::<U::Time>::reborrow_ref(curr.2) != ContainerOf::<U::Time>::reborrow_ref(prev.2);

                    if time_differs {
                        // Flush the accumulated diff for prev's (key, val, time).
                        if !diff_stash.is_zero() {
                            // We have a real update to emit. Push time (and val/key
                            // if this is the first time under them).
                            let times_len = output.times.values.len();
                            let vals_len = output.vals.values.len();

                            if val_differs {
                                // Seal the previous val's time list, if any times were emitted.
                                if times_len > 0 {
                                    output.times.bounds.push(times_len as u64);
                                }
                                if key_differs {
                                    // Seal the previous key's val list, if any vals were emitted.
                                    if vals_len > 0 {
                                        output.vals.bounds.push(vals_len as u64);
                                    }
                                    output.keys.values.push(prev.0);
                                }
                                output.vals.values.push(prev.1);
                            }
                            output.times.values.push(prev.2);
                            output.diffs.values.push(&diff_stash);
                            output.diffs.bounds.push(output.diffs.values.len() as u64);
                        }
                        Columnar::copy_from(&mut diff_stash, curr.3);
                    } else {
                        // Same (key, val, time): accumulate diff.
                        Columnar::copy_from(&mut diff_temp, curr.3);
                        diff_stash.plus_equals(&diff_temp);
                    }
                    prev = curr;
                }

                // Flush the final accumulated diff.
                if !diff_stash.is_zero() {
                    let keys_len = output.keys.values.len();
                    let vals_len = output.vals.values.len();
                    let times_len = output.times.values.len();
                    let need_key = keys_len == 0 || ContainerOf::<U::Key>::reborrow_ref(prev.0) != output.keys.values.borrow().get(keys_len - 1);
                    let need_val = need_key || vals_len == 0 || ContainerOf::<U::Val>::reborrow_ref(prev.1) != output.vals.values.borrow().get(vals_len - 1);

                    if need_val {
                        if times_len > 0 {
                            output.times.bounds.push(times_len as u64);
                        }
                        if need_key {
                            if vals_len > 0 {
                                output.vals.bounds.push(vals_len as u64);
                            }
                            output.keys.values.push(prev.0);
                        }
                        output.vals.values.push(prev.1);
                    }
                    output.times.values.push(prev.2);
                    output.diffs.values.push(&diff_stash);
                    output.diffs.bounds.push(output.diffs.values.len() as u64);
                }

                // Seal the final groups at each level.
                if !output.times.values.is_empty() {
                    output.times.bounds.push(output.times.values.len() as u64);
                }
                if !output.vals.values.is_empty() {
                    output.vals.bounds.push(output.vals.values.len() as u64);
                }
                if !output.keys.values.is_empty() {
                    output.keys.bounds.push(output.keys.values.len() as u64);
                }
            }

            output
        }

        /// Consolidates into canonical trie form:
        /// single outer key list, all lists sorted and deduplicated,
        /// diff lists are singletons (or absent if cancelled).
        pub fn consolidate(self) -> Self {

            let Self { keys, vals, times, diffs } = self;

            let keys_b = keys.borrow();
            let vals_b = vals.borrow();
            let times_b = times.borrow();
            let diffs_b = diffs.borrow();

            // Flatten to index tuples: [key_abs, val_abs, time_abs, diff_abs].
            let mut tuples: Vec<[usize; 4]> = Vec::new();
            for outer in 0..Len::len(&keys_b) {
                for k in child_range(keys_b.bounds, outer) {
                    for v in child_range(vals_b.bounds, k) {
                        for t in child_range(times_b.bounds, v) {
                            for d in child_range(diffs_b.bounds, t) {
                                tuples.push([k, v, t, d]);
                            }
                        }
                    }
                }
            }

            // Sort by (key, val, time). Diff is payload.
            tuples.sort_by(|a, b| {
                keys_b.values.get(a[0]).cmp(&keys_b.values.get(b[0]))
                    .then_with(|| vals_b.values.get(a[1]).cmp(&vals_b.values.get(b[1])))
                    .then_with(|| times_b.values.get(a[2]).cmp(&times_b.values.get(b[2])))
            });

            // Build consolidated output, bottom-up cancellation.
            let mut output = Self::default();
            let mut diff_stash = U::Diff::default();
            let mut diff_temp = U::Diff::default();

            let mut idx = 0;
            while idx < tuples.len() {
                let key_ref = keys_b.values.get(tuples[idx][0]);
                let key_start_vals = output.vals.values.len();

                // All entries with this key.
                while idx < tuples.len() && keys_b.values.get(tuples[idx][0]) == key_ref {
                    let val_ref = vals_b.values.get(tuples[idx][1]);
                    let val_start_times = output.times.values.len();

                    // All entries with this (key, val).
                    while idx < tuples.len()
                        && keys_b.values.get(tuples[idx][0]) == key_ref
                        && vals_b.values.get(tuples[idx][1]) == val_ref
                    {
                        let time_ref = times_b.values.get(tuples[idx][2]);

                        // Sum all diffs for this (key, val, time).
                        Columnar::copy_from(&mut diff_stash, diffs_b.values.get(tuples[idx][3]));
                        idx += 1;
                        while idx < tuples.len()
                            && keys_b.values.get(tuples[idx][0]) == key_ref
                            && vals_b.values.get(tuples[idx][1]) == val_ref
                            && times_b.values.get(tuples[idx][2]) == time_ref
                        {
                            Columnar::copy_from(&mut diff_temp, diffs_b.values.get(tuples[idx][3]));
                            diff_stash.plus_equals(&diff_temp);
                            idx += 1;
                        }

                        // Emit time + singleton diff if nonzero.
                        if !diff_stash.is_zero() {
                            output.times.values.push(time_ref);
                            output.diffs.values.push(&diff_stash);
                            output.diffs.bounds.push(output.diffs.values.len() as u64);
                        }
                    }

                    // Seal time list for this val; emit val if any times survived.
                    if output.times.values.len() > val_start_times {
                        output.times.bounds.push(output.times.values.len() as u64);
                        output.vals.values.push(val_ref);
                    }
                }

                // Seal val list for this key; emit key if any vals survived.
                if output.vals.values.len() > key_start_vals {
                    output.vals.bounds.push(output.vals.values.len() as u64);
                    output.keys.values.push(key_ref);
                }
            }

            // Seal the single outer key list.
            if !output.keys.values.is_empty() {
                output.keys.bounds.push(output.keys.values.len() as u64);
            }

            output
        }

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
pub fn join_function<G, U, I, L>(
    input: differential_dataflow::Collection<G, RecordedUpdates<U>>,
    mut logic: L,
) -> differential_dataflow::Collection<G, RecordedUpdates<U>>
where
    G: timely::dataflow::Scope,
    G::Timestamp: differential_dataflow::lattice::Lattice,
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
                input.for_each(|time, data| {
                    let mut session = output.session_with_builder(&time);
                    for (k1, v1, t1, d1) in data.updates.iter() {
                        let t1o: U::Time = Columnar::into_owned(t1);
                        let d1o: U::Diff = Columnar::into_owned(d1);
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
pub fn leave_dynamic<G, K, V, R>(
    input: differential_dataflow::Collection<G, RecordedUpdates<(K, V, DynTime, R)>>,
    level: usize,
) -> differential_dataflow::Collection<G, RecordedUpdates<(K, V, DynTime, R)>>
where
    G: timely::dataflow::Scope<Timestamp = DynTime>,
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
                    let mut time: DynTime = Columnar::into_owned(t);
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
pub fn as_recorded_updates<G, U>(
    arranged: differential_dataflow::operators::arrange::Arranged<
        G,
        differential_dataflow::operators::arrange::TraceAgent<ValSpine<U::Key, U::Val, U::Time, U::Diff>>,
    >,
) -> differential_dataflow::Collection<G, RecordedUpdates<U>>
where
    G: timely::dataflow::Scope<Timestamp = U::Time>,
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
