//! Wordcount based on `columnar`.

use {
    timely::container::{Container, CapacityContainerBuilder},
    timely::dataflow::channels::pact::ExchangeCore,
    timely::dataflow::InputHandleCore,
    timely::dataflow::ProbeHandle,
};

use differential_dataflow::trace::implementations::ord_neu::ColKeySpine;

use differential_dataflow::operators::arrange::arrangement::arrange_core;

fn main() {

    type WordCount = ((String, ()), u64, i64);
    type Container = Column<WordCount>;

    let _config = timely::Config {
        communication: timely::CommunicationConfig::ProcessBinary(3),
        worker: timely::WorkerConfig::default(),
    };

    let keys: usize = std::env::args().nth(1).expect("missing argument 1").parse().unwrap();
    let size: usize = std::env::args().nth(2).expect("missing argument 2").parse().unwrap();

    let timer1 = ::std::time::Instant::now();
    let timer2 = timer1;

    // initializes and runs a timely dataflow.
    // timely::execute(_config, move |worker| {
    timely::execute_from_args(std::env::args(), move |worker| {

        let mut data_input = <InputHandleCore<_, CapacityContainerBuilder<Container>>>::new();
        let mut keys_input = <InputHandleCore<_, CapacityContainerBuilder<Container>>>::new();
        let probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<u64, _, _>(|scope| {

            let data = data_input.to_stream(scope);
            let keys = keys_input.to_stream(scope);

            let data_pact = ExchangeCore::<ColumnBuilder<((String,()),u64,i64)>,_>::new_core(|x: &((&str,()),&u64,&i64)| (x.0).0.as_bytes().iter().map(|x| *x as u64).sum::<u64>());
            let keys_pact = ExchangeCore::<ColumnBuilder<((String,()),u64,i64)>,_>::new_core(|x: &((&str,()),&u64,&i64)| (x.0).0.as_bytes().iter().map(|x| *x as u64).sum::<u64>());

            let data = arrange_core::<_,_,Col2KeyBatcher<_,_,_>, ColKeyBuilder<_,_,_>, ColKeySpine<_,_,_>>(&data, data_pact, "Data");
            let keys = arrange_core::<_,_,Col2KeyBatcher<_,_,_>, ColKeyBuilder<_,_,_>, ColKeySpine<_,_,_>>(&keys, keys_pact, "Keys");

            keys.join_core(&data, |_k, &(), &()| Option::<()>::None)
                .probe_with(&probe);

        });

        // Resources for placing input data in containers.
        use std::fmt::Write;
        let mut buffer = String::default();
        let mut container = Container::default();

        // Load up data in batches.
        let mut counter = 0;
        while counter < 10 * keys {
            let mut i = worker.index();
            let time = *data_input.time();
            while i < size {
                let val = (counter + i) % keys;
                write!(buffer, "{:?}", val).unwrap();
                container.push(((&buffer, ()), time, 1));
                buffer.clear();
                i += worker.peers();
            }
            data_input.send_batch(&mut container);
            container.clear();
            counter += size;
            data_input.advance_to(data_input.time() + 1);
            keys_input.advance_to(keys_input.time() + 1);
            while probe.less_than(data_input.time()) {
                worker.step();
            }
        }
        println!("{:?}\tloading complete", timer1.elapsed());

        let mut queries = 0;

        while queries < 10 * keys {
            let mut i = worker.index();
            let time = *data_input.time();
            while i < size {
                let val = (queries + i) % keys;
                write!(buffer, "{:?}", val).unwrap();
                container.push(((&buffer, ()), time, 1));
                buffer.clear();
                i += worker.peers();
            }
            keys_input.send_batch(&mut container);
            container.clear();
            queries += size;
            data_input.advance_to(data_input.time() + 1);
            keys_input.advance_to(keys_input.time() + 1);
            while probe.less_than(data_input.time()) {
                worker.step();
            }
        }

        println!("{:?}\tqueries complete", timer1.elapsed());


    })
    .unwrap();

    println!("{:?}\tshut down", timer2.elapsed());
}


pub use container::Column;
mod container {

    use columnar::Columnar;
    use columnar::Container as FooBozzle;

    use timely::bytes::arc::Bytes;

    /// A container based on a columnar store, encoded in aligned bytes.
    pub enum Column<C: Columnar> {
        /// The typed variant of the container.
        Typed(C::Container),
        /// The binary variant of the container.
        Bytes(Bytes),
        /// Relocated, aligned binary data, if `Bytes` doesn't work for some reason.
        ///
        /// Reasons could include misalignment, cloning of data, or wanting
        /// to release the `Bytes` as a scarce resource.
        Align(Box<[u64]>),
    }

    impl<C: Columnar> Default for Column<C> {
        fn default() -> Self { Self::Typed(Default::default()) }
    }

    impl<C: Columnar<Container: Clone>> Clone for Column<C> {
        fn clone(&self) -> Self {
            match self {
                Column::Typed(t) => Column::Typed(t.clone()),
                Column::Bytes(b) => {
                    assert_eq!(b.len() % 8, 0);
                    let mut alloc: Vec<u64> = vec![0; b.len() / 8];
                    bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&b[..]);
                    Self::Align(alloc.into())
                },
                Column::Align(a) => Column::Align(a.clone()),
            }
        }
    }

    use columnar::{Clear, Len, Index, FromBytes};
    use columnar::bytes::{EncodeDecode, Indexed};
    use columnar::common::IterOwn;

    impl<C: Columnar> Column<C> {
        pub fn borrow(&self) -> <C::Container as columnar::Container<C>>::Borrowed<'_> {
            match self {
                Column::Typed(t) => t.borrow(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(a)),
            }
        }
        pub fn get(&self, index: usize) -> C::Ref<'_> {
            self.borrow().get(index)
        }
    }

    use timely::Container;
    impl<C: Columnar> Container for Column<C> {
        fn len(&self) -> usize {
            match self {
                Column::Typed(t) => t.len(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).len(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'_> as FromBytes>::from_bytes(&mut Indexed::decode(a)).len(),
            }
        }
        // This sets the `Bytes` variant to be an empty `Typed` variant, appropriate for pushing into.
        fn clear(&mut self) {
            match self {
                Column::Typed(t) => t.clear(),
                Column::Bytes(_) => *self = Column::Typed(Default::default()),
                Column::Align(_) => *self = Column::Typed(Default::default()),
            }
        }

        type ItemRef<'a> = C::Ref<'a>;
        type Iter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;
        fn iter<'a>(&'a self) -> Self::Iter<'a> {
            match self {
                Column::Typed(t) => t.borrow().into_index_iter(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).into_index_iter(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(a)).into_index_iter(),
            }
        }

        type Item<'a> = C::Ref<'a>;
        type DrainIter<'a> = IterOwn<<C::Container as columnar::Container<C>>::Borrowed<'a>>;
        fn drain<'a>(&'a mut self) -> Self::DrainIter<'a> {
            match self {
                Column::Typed(t) => t.borrow().into_index_iter(),
                Column::Bytes(b) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).into_index_iter(),
                Column::Align(a) => <<C::Container as columnar::Container<C>>::Borrowed<'a> as FromBytes>::from_bytes(&mut Indexed::decode(a)).into_index_iter(),
            }
        }
    }

    use timely::container::SizableContainer;
    impl<C: Columnar> SizableContainer for Column<C> {
        fn at_capacity(&self) -> bool {
            match self {
                Self::Typed(t) => {
                    let length_in_bytes = Indexed::length_in_bytes(&t.borrow());
                    length_in_bytes >= (1 << 20)
                },
                Self::Bytes(_) => true,
                Self::Align(_) => true,
            }
        }
        fn ensure_capacity(&mut self, _stash: &mut Option<Self>) { }
    }

    use timely::container::PushInto;
    impl<T, C: Columnar<Container: columnar::Push<T>>> PushInto<T> for Column<C> {
        #[inline(always)]
        fn push_into(&mut self, item: T) {
            use columnar::Push;
            match self {
                Column::Typed(t) => t.push(item),
                Column::Align(_) | Column::Bytes(_) => {
                    // We really oughtn't be calling this in this case.
                    // We could convert to owned, but need more constraints on `C`.
                    unimplemented!("Pushing into Column::Bytes without first clearing");
                }
            }
        }
    }

    use timely::dataflow::channels::ContainerBytes;
    impl<C: Columnar> ContainerBytes for Column<C> {
        fn from_bytes(bytes: timely::bytes::arc::Bytes) -> Self {
            // Our expectation / hope is that `bytes` is `u64` aligned and sized.
            // If the alignment is borked, we can relocate. IF the size is borked,
            // not sure what we do in that case.
            assert_eq!(bytes.len() % 8, 0);
            if bytemuck::try_cast_slice::<_, u64>(&bytes).is_ok() {
                Self::Bytes(bytes)
            }
            else {
                // println!("Re-locating bytes for alignment reasons");
                let mut alloc: Vec<u64> = vec![0; bytes.len() / 8];
                bytemuck::cast_slice_mut(&mut alloc[..]).copy_from_slice(&bytes[..]);
                Self::Align(alloc.into())
            }
        }

        fn length_in_bytes(&self) -> usize {
            match self {
                // We'll need one u64 for the length, then the length rounded up to a multiple of 8.
                Column::Typed(t) => Indexed::length_in_bytes(&t.borrow()),
                Column::Bytes(b) => b.len(),
                Column::Align(a) => 8 * a.len(),
            }
        }

        fn into_bytes<W: ::std::io::Write>(&self, writer: &mut W) {
            match self {
                Column::Typed(t) => Indexed::write(writer, &t.borrow()).unwrap(),
                Column::Bytes(b) => writer.write_all(b).unwrap(),
                Column::Align(a) => writer.write_all(bytemuck::cast_slice(a)).unwrap(),
            }
        }
    }
}


use builder::ColumnBuilder;
mod builder {
    use std::collections::VecDeque;

    use columnar::{Columnar, Clear, Len, Push};
    use columnar::bytes::{EncodeDecode, Indexed};

    use super::Column;

    /// A container builder for `Column<C>`.
    pub struct ColumnBuilder<C: Columnar> {
        /// Container that we're writing to.
        current: C::Container,
        /// Empty allocation.
        empty: Option<Column<C>>,
        /// Completed containers pending to be sent.
        pending: VecDeque<Column<C>>,
    }

    use timely::container::PushInto;
    impl<T, C: Columnar<Container: columnar::Push<T>>> PushInto<T> for ColumnBuilder<C> {
        #[inline(always)]
        fn push_into(&mut self, item: T) {
            self.current.push(item);
            // If there is less than 10% slop with 2MB backing allocations, mint a container.
            use columnar::Container;
            let words = Indexed::length_in_words(&self.current.borrow());
            let round = (words + ((1 << 18) - 1)) & !((1 << 18) - 1);
            if round - words < round / 10 {
                /// Move the contents from `current` to an aligned allocation, and push it to `pending`.
                /// The contents must fit in `round` words (u64).
                #[cold]
                fn outlined_align<C>(
                    current: &mut C::Container,
                    round: usize,
                    pending: &mut VecDeque<Column<C>>,
                    empty: Option<Column<C>>,
                ) where
                    C: Columnar,
                {
                    let mut alloc = if let Some(Column::Align(allocation)) = empty {
                        let mut alloc: Vec<_> = allocation.into();
                        alloc.clear();
                        if alloc.capacity() < round {
                            alloc.reserve(round - alloc.len());
                        }
                        alloc
                    } else {
                        Vec::with_capacity(round)
                    };
                    Indexed::encode(&mut alloc, &current.borrow());
                    pending.push_back(Column::Align(alloc.into_boxed_slice()));
                    current.clear();
                }

                outlined_align(&mut self.current, round, &mut self.pending, self.empty.take());
            }
        }
    }

    impl<C: Columnar> Default for ColumnBuilder<C> {
        fn default() -> Self {
            ColumnBuilder {
                current: Default::default(),
                empty: None,
                pending: Default::default(),
            }
        }
    }

    use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};
    impl<C: Columnar<Container: Clone>> ContainerBuilder for ColumnBuilder<C> {
        type Container = Column<C>;

        #[inline(always)]
        fn extract(&mut self) -> Option<&mut Self::Container> {
            if let Some(container) = self.pending.pop_front() {
                self.empty = Some(container);
                self.empty.as_mut()
            } else {
                None
            }
        }

        #[inline(always)]
        fn finish(&mut self) -> Option<&mut Self::Container> {
            if !self.current.is_empty() {
                use columnar::Container;
                let words = Indexed::length_in_words(&self.current.borrow());
                let mut alloc = if let Some(Column::Align(allocation)) = self.empty.take() {
                    let mut alloc: Vec<_> = allocation.into();
                    alloc.clear();
                    if alloc.capacity() < words {
                        alloc.reserve(words - alloc.len());
                    }
                    alloc
                } else {
                    Vec::with_capacity(words)
                };
                Indexed::encode(&mut alloc, &self.current.borrow());
                self.pending.push_back(Column::Align(alloc.into_boxed_slice()));
                self.current.clear();
            }
            self.extract()
        }
    }

    impl<C: Columnar<Container: Clone>> LengthPreservingContainerBuilder for ColumnBuilder<C> { }
}

use batcher::Col2KeyBatcher;

/// Types for consolidating, merging, and extracting columnar update collections.
pub mod batcher {

    use columnar::Columnar;
    use timely::Container;
    use timely::container::{ContainerBuilder, PushInto};
    use differential_dataflow::difference::Semigroup;
    use differential_dataflow::trace::implementations::merge_batcher::container::ContainerMerger;
    use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;

    use crate::Column;
    use crate::builder::ColumnBuilder;

    /// A batcher for columnar storage.
    pub type Col2ValBatcher<K, V, T, R> = MergeBatcher<Column<((K,V),T,R)>, Chunker<ColumnBuilder<((K,V),T,R)>>, ContainerMerger<ColumnBuilder<((K,V),T,R)>>>;
    pub type Col2KeyBatcher<K, T, R> = Col2ValBatcher<K, (), T, R>;

    // First draft: build a "chunker" and a "merger".

    #[derive(Default)]
    pub struct Chunker<CB> {
        /// Builder to absorb sorted data.
        builder: CB,
    }

    impl<CB: ContainerBuilder> ContainerBuilder for Chunker<CB> {
        type Container = CB::Container;

        fn extract(&mut self) -> Option<&mut Self::Container> {
            self.builder.extract()
        }

        fn finish(&mut self) -> Option<&mut Self::Container> {
            self.builder.finish()
        }
    }

    impl<'a, D, T, R, CB> PushInto<&'a mut Column<(D, T, R)>> for Chunker<CB>
    where
        D: for<'b> Columnar<Ref<'b>: Ord>,
        T: for<'b> Columnar<Ref<'b>: Ord>,
        R: for<'b> Columnar<Ref<'b>: Ord> + for<'b> Semigroup<R::Ref<'b>>,
        CB: ContainerBuilder + for<'b, 'c> PushInto<(D::Ref<'b>, T::Ref<'b>, &'c R)>,
    {
        fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {
            // Sort input data
            // TODO: consider `Vec<usize>` that we retain, containing indexes.
            let mut permutation = Vec::with_capacity(container.len());
            permutation.extend(container.drain());
            permutation.sort();

            // Iterate over the data, accumulating diffs for like keys.
            let mut iter = permutation.drain(..);
            if let Some((data, time, diff)) = iter.next() {

                let mut prev_data = data;
                let mut prev_time = time;
                let mut prev_diff = <R as Columnar>::into_owned(diff);

                for (data, time, diff) in iter {
                    if (&prev_data, &prev_time) == (&data, &time) {
                        prev_diff.plus_equals(&diff);
                    }
                    else {
                        if !prev_diff.is_zero() {
                            let tuple = (prev_data, prev_time, &prev_diff);
                            self.builder.push_into(tuple);
                        }
                        prev_data = data;
                        prev_time = time;
                        prev_diff = <R as Columnar>::into_owned(diff);
                    }
                }

                if !prev_diff.is_zero() {
                    let tuple = (prev_data, prev_time, &prev_diff);
                    self.builder.push_into(tuple);
                }
            }
        }
    }

    /// Implementations of `ContainerQueue` and `MergerChunk` for `Column` containers (columnar).
    pub mod merger {
        use timely::progress::{Antichain, frontier::AntichainRef, Timestamp};
        use columnar::{Columnar, Index};
        use crate::builder::ColumnBuilder;
        use crate::container::Column;
        use differential_dataflow::difference::Semigroup;

        use differential_dataflow::trace::implementations::merge_batcher::container::{ContainerQueue, MergerChunk, PushAndAdd};

        /// A queue for extracting items from a `Column` container.
        pub struct ColumnQueue<'a, T: Columnar> {
            list: <T::Container as columnar::Container<T>>::Borrowed<'a>,
            head: usize,
        }

        impl<'a, D, T, R> ContainerQueue<'a, Column<(D, T, R)>> for ColumnQueue<'a, (D, T, R)>
        where
            D: for<'b> Columnar<Ref<'b>: Ord>,
            T: for<'b> Columnar<Ref<'b>: Ord>,
            R: Columnar,
        {
            type Item = <(D, T, R) as Columnar>::Ref<'a>;
            type SelfGAT<'b> = ColumnQueue<'b, (D, T, R)>;
            fn pop(&mut self) -> Self::Item {
                self.head += 1;
                self.list.get(self.head - 1)
            }
            fn is_empty(&mut self) -> bool {
                use columnar::Len;
                self.head == self.list.len()
            }
            fn cmp_heads<'b>(&mut self, other: &mut ColumnQueue<'b, (D, T, R)>) -> std::cmp::Ordering {
                let (data1, time1, _) = self.peek();
                let (data2, time2, _) = other.peek();

                let data1 = D::reborrow(data1);
                let time1 = T::reborrow(time1);
                let data2 = D::reborrow(data2);
                let time2 = T::reborrow(time2);

                (data1, time1).cmp(&(data2, time2))
            }
            fn new(list: &'a mut Column<(D, T, R)>) -> ColumnQueue<'a, (D, T, R)>{
                ColumnQueue { list: list.borrow(), head: 0 }
            }
        }

        impl<'a, T: Columnar> ColumnQueue<'a, T> {
            fn peek(&self) -> T::Ref<'a> {
                self.list.get(self.head)
            }
        }

        impl<D, T, R> MergerChunk for Column<(D, T, R)>
        where
            D: for<'a> Columnar<Ref<'a>: Ord>,
            T: for<'a> Columnar<Ref<'a>: Ord> + Timestamp,
            for<'a> <T as Columnar>::Ref<'a> : Copy,
            R: Default + Semigroup + Columnar,
        {
            type ContainerQueue<'a> = ColumnQueue<'a, (D, T, R)>;
            type TimeOwned = T;

            fn time_kept((_, time, _): &Self::Item<'_>, upper: &AntichainRef<Self::TimeOwned>, frontier: &mut Antichain<Self::TimeOwned>, stash: &mut T) -> bool {
                stash.copy_from(*time);
                if upper.less_equal(stash) {
                    frontier.insert_ref(stash);
                    true
                }
                else { false }
            }
            fn account(&self) -> (usize, usize, usize, usize) {
                (0, 0, 0, 0)
                // unimplemented!()
                // use timely::Container;
                // let (mut size, mut capacity, mut allocations) = (0, 0, 0);
                // let cb = |siz, cap| {
                //     size += siz;
                //     capacity += cap;
                //     allocations += 1;
                // };
                // self.heap_size(cb);
                // (self.len(), size, capacity, allocations)
            }
        }

        impl<D, T, R> PushAndAdd for ColumnBuilder<(D, T, R)>
            where
            D: Columnar,
            T: timely::PartialOrder + Columnar,
            R: Default + Semigroup + Columnar,
        {
            type Item<'a> = <(D, T, R) as Columnar>::Ref<'a>;
            type DiffOwned = R;

            fn push_and_add(&mut self, (data, time, diff1): Self::Item<'_>, (_, _, diff2): Self::Item<'_>, stash: &mut Self::DiffOwned) {
                stash.copy_from(diff1);
                let stash2: R = R::into_owned(diff2);
                stash.plus_equals(&stash2);
                if !stash.is_zero() {
                    use timely::container::PushInto;
                    self.push_into((data, time, &*stash));
                }
            }
        }
    }

}

use dd_builder::ColKeyBuilder;

pub mod dd_builder {

    use columnar::Columnar;

    use timely::container::PushInto;

    use differential_dataflow::IntoOwned;
    use differential_dataflow::trace::Builder;
    use differential_dataflow::trace::Description;
    use differential_dataflow::trace::implementations::Layout;
    use differential_dataflow::trace::implementations::Update;
    use differential_dataflow::trace::implementations::BatchContainer;
    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, val_batch::OrdValStorage, OrdKeyBatch};
    use differential_dataflow::trace::implementations::ord_neu::key_batch::OrdKeyStorage;
    use crate::Column;


    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use differential_dataflow::trace::implementations::TStack;

    pub type ColValBuilder<K, V, T, R> = RcBuilder<OrdValBuilder<TStack<((K,V),T,R)>>>;
    pub type ColKeyBuilder<K, T, R> = RcBuilder<OrdKeyBuilder<TStack<((K,()),T,R)>>>;

    // Utility types to save some typing and avoid visual noise. Extract the owned type and
    // the batch's read items from a layout.
    type OwnedKey<L> = <<L as Layout>::KeyContainer as BatchContainer>::Owned;
    type ReadItemKey<'a, L> = <<L as Layout>::KeyContainer as BatchContainer>::ReadItem<'a>;
    type OwnedVal<L> = <<L as Layout>::ValContainer as BatchContainer>::Owned;
    type ReadItemVal<'a, L> = <<L as Layout>::ValContainer as BatchContainer>::ReadItem<'a>;
    type OwnedTime<L> = <<L as Layout>::TimeContainer as BatchContainer>::Owned;
    type ReadItemTime<'a, L> = <<L as Layout>::TimeContainer as BatchContainer>::ReadItem<'a>;
    type OwnedDiff<L> = <<L as Layout>::DiffContainer as BatchContainer>::Owned;
    type ReadItemDiff<'a, L> = <<L as Layout>::DiffContainer as BatchContainer>::ReadItem<'a>;

    /// A builder for creating layers from unsorted update tuples.
    pub struct OrdValBuilder<L: Layout> {
        /// The in-progress result.
        ///
        /// This is public to allow container implementors to set and inspect their container.
        pub result: OrdValStorage<L>,
        singleton: Option<(<L::Target as Update>::Time, <L::Target as Update>::Diff)>,
        /// Counts the number of singleton optimizations we performed.
        ///
        /// This number allows us to correctly gauge the total number of updates reflected in a batch,
        /// even though `updates.len()` may be much shorter than this amount.
        singletons: usize,
    }

    impl<L: Layout> OrdValBuilder<L> {
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
            if self.result.times.last().map(|t| t == ReadItemTime::<L>::borrow_as(&time)) == Some(true) &&
                self.result.diffs.last().map(|d| d == ReadItemDiff::<L>::borrow_as(&diff)) == Some(true)
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

    // The layout `L` determines the key, val, time, and diff types.
    impl<L> Builder for OrdValBuilder<L>
    where
        L: Layout,
        OwnedKey<L>: Columnar,
        OwnedVal<L>: Columnar,
        OwnedTime<L>: Columnar,
        OwnedDiff<L>: Columnar,
        // These two constraints seem .. like we could potentially replace by `Columnar::Ref<'a>`.
        for<'a> L::KeyContainer: PushInto<&'a OwnedKey<L>>,
        for<'a> L::ValContainer: PushInto<&'a OwnedVal<L>>,
        for<'a> ReadItemTime<'a, L> : IntoOwned<'a, Owned = <L::Target as Update>::Time>,
        for<'a> ReadItemDiff<'a, L> : IntoOwned<'a, Owned = <L::Target as Update>::Diff>,
    {
        type Input = Column<((OwnedKey<L>,OwnedVal<L>),OwnedTime<L>,OwnedDiff<L>)>;
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
            }
        }

        #[inline]
        fn push(&mut self, chunk: &mut Self::Input) {
            use timely::Container;

            // NB: Maintaining owned key and val across iterations to track the "last", which we clone into,
            // is somewhat appealing from an ease point of view. Might still allocate, do work we don't need,
            // but avoids e.g. calls into `last()` and breaks horrid trait requirements.
            // Owned key and val would need to be members of `self`, as this method can be called multiple times,
            // and we need to correctly cache last for reasons of correctness, not just performance.

            let mut owned_key = None;
            let mut owned_val = None;

            for ((key,val),time,diff) in chunk.drain() {
                let key = if let Some(owned_key) = owned_key.as_mut() {
                    OwnedKey::<L>::copy_from(owned_key, key);
                    owned_key
                } else {
                    owned_key.insert(OwnedKey::<L>::into_owned(key))
                };
                let val = if let Some(owned_val) = owned_val.as_mut() {
                    OwnedVal::<L>::copy_from(owned_val, val);
                    owned_val
                } else {
                    owned_val.insert(OwnedVal::<L>::into_owned(val))
                };

                let time = OwnedTime::<L>::into_owned(time);
                let diff = OwnedDiff::<L>::into_owned(diff);

                // Perhaps this is a continuation of an already received key.
                if self.result.keys.last().map(|k| ReadItemKey::<L>::borrow_as(key).eq(&k)).unwrap_or(false) {
                    // Perhaps this is a continuation of an already received value.
                    if self.result.vals.last().map(|v| ReadItemVal::<L>::borrow_as(val).eq(&v)).unwrap_or(false) {
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
        fn done(mut self, description: Description<Self::Time>) -> OrdValBatch<L> {
            // Record the final offsets
            self.result.vals_offs.push(self.result.times.len());
            // Remove any pending singleton, and if it was set increment our count.
            if self.singleton.take().is_some() { self.singletons += 1; }
            self.result.keys_offs.push(self.result.vals.len());
            OrdValBatch {
                updates: self.result.times.len() + self.singletons,
                storage: self.result,
                description,
            }
        }

        fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
            // let (keys, vals, upds) = Self::Input::key_val_upd_counts(&chain[..]);
            // let mut builder = Self::with_capacity(keys, vals, upds);
            let mut builder = Self::with_capacity(0, 0, 0);
            for mut chunk in chain.drain(..) {
                builder.push(&mut chunk);
            }

            builder.done(description)
        }
    }

    /// A builder for creating layers from unsorted update tuples.
    pub struct OrdKeyBuilder<L: Layout> {
        /// The in-progress result.
        ///
        /// This is public to allow container implementors to set and inspect their container.
        pub result: OrdKeyStorage<L>,
        singleton: Option<(<L::Target as Update>::Time, <L::Target as Update>::Diff)>,
        /// Counts the number of singleton optimizations we performed.
        ///
        /// This number allows us to correctly gauge the total number of updates reflected in a batch,
        /// even though `updates.len()` may be much shorter than this amount.
        singletons: usize,
    }

    impl<L: Layout> OrdKeyBuilder<L> {
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
            if self.result.times.last().map(|t| t == ReadItemTime::<L>::borrow_as(&time)) == Some(true) &&
                self.result.diffs.last().map(|d| d == ReadItemDiff::<L>::borrow_as(&diff)) == Some(true)
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

    // The layout `L` determines the key, val, time, and diff types.
    impl<L> Builder for OrdKeyBuilder<L>
    where
        L: Layout,
        OwnedKey<L>: Columnar,
        OwnedVal<L>: Columnar,
        OwnedTime<L>: Columnar,
        OwnedDiff<L>: Columnar,
    // These two constraints seem .. like we could potentially replace by `Columnar::Ref<'a>`.
        for<'a> L::KeyContainer: PushInto<&'a OwnedKey<L>>,
        for<'a> L::ValContainer: PushInto<&'a OwnedVal<L>>,
        for<'a> ReadItemTime<'a, L> : IntoOwned<'a, Owned = <L::Target as Update>::Time>,
        for<'a> ReadItemDiff<'a, L> : IntoOwned<'a, Owned = <L::Target as Update>::Diff>,
    {
        type Input = Column<((OwnedKey<L>,OwnedVal<L>),OwnedTime<L>,OwnedDiff<L>)>;
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
            }
        }

        #[inline]
        fn push(&mut self, chunk: &mut Self::Input) {
            use timely::Container;

            // NB: Maintaining owned key and val across iterations to track the "last", which we clone into,
            // is somewhat appealing from an ease point of view. Might still allocate, do work we don't need,
            // but avoids e.g. calls into `last()` and breaks horrid trait requirements.
            // Owned key and val would need to be members of `self`, as this method can be called multiple times,
            // and we need to correctly cache last for reasons of correctness, not just performance.

            let mut owned_key = None;

            for ((key,_val),time,diff) in chunk.drain() {
                let key = if let Some(owned_key) = owned_key.as_mut() {
                    OwnedKey::<L>::copy_from(owned_key, key);
                    owned_key
                } else {
                    owned_key.insert(OwnedKey::<L>::into_owned(key))
                };

                let time = OwnedTime::<L>::into_owned(time);
                let diff = OwnedDiff::<L>::into_owned(diff);

                // Perhaps this is a continuation of an already received key.
                if self.result.keys.last().map(|k| ReadItemKey::<L>::borrow_as(key).eq(&k)).unwrap_or(false) {
                    self.push_update(time, diff);
                } else {
                    // New key; complete representation of prior key.
                    self.result.keys_offs.push(self.result.times.len());
                    if self.singleton.take().is_some() { self.singletons += 1; }
                    self.push_update(time, diff);
                    self.result.keys.push(key);
                }
            }
        }

        #[inline(never)]
        fn done(mut self, description: Description<Self::Time>) -> OrdKeyBatch<L> {
            // Record the final offsets
            self.result.keys_offs.push(self.result.times.len());
            // Remove any pending singleton, and if it was set increment our count.
            if self.singleton.take().is_some() { self.singletons += 1; }
            OrdKeyBatch {
                updates: self.result.times.len() + self.singletons,
                storage: self.result,
                description,
            }
        }

        fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
            let mut builder = Self::with_capacity(0, 0, 0);
            for mut chunk in chain.drain(..) {
                builder.push(&mut chunk);
            }

            builder.done(description)
        }
    }
}
