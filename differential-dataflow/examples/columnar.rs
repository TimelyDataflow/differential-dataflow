//! Wordcount based on `columnar`.

use {
    timely::container::{Container, CapacityContainerBuilder},
    timely::dataflow::channels::pact::ExchangeCore,
    timely::dataflow::InputHandleCore,
    timely::dataflow::ProbeHandle,
};


// use differential_dataflow::trace::implementations::ord_neu::ColKeyBuilder;
use differential_dataflow::trace::implementations::ord_neu::ColValSpine;

use differential_dataflow::operators::arrange::arrangement::arrange_core;

fn main() {

    type WordCount = ((String, ()), u64, i64);
    type Container = Column<WordCount>;

    let _config = timely::Config {
        communication: timely::CommunicationConfig::ProcessBinary(3),
        worker: timely::WorkerConfig::default(),
    };

    let keys: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let size: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    let timer1 = ::std::time::Instant::now();
    let timer2 = timer1.clone();

    // initializes and runs a timely dataflow.
    // timely::execute(_config, move |worker| {
    timely::execute_from_args(std::env::args(), move |worker| {

        let mut data_input = <InputHandleCore<_, CapacityContainerBuilder<Container>>>::new();
        let mut keys_input = <InputHandleCore<_, CapacityContainerBuilder<Container>>>::new();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<u64, _, _>(|scope| {

            let data = data_input.to_stream(scope);
            let keys = keys_input.to_stream(scope);

            let data_pact = ExchangeCore::<ColumnBuilder<((String,()),u64,i64)>,_>::new_core(|x: &((&str,()),&u64,&i64)| (x.0).0.as_bytes().iter().map(|x| *x as u64).sum::<u64>() as u64);
            let keys_pact = ExchangeCore::<ColumnBuilder<((String,()),u64,i64)>,_>::new_core(|x: &((&str,()),&u64,&i64)| (x.0).0.as_bytes().iter().map(|x| *x as u64).sum::<u64>() as u64);

            let data = arrange_core::<_,_,Col2KeyBatcher<_,_,_>, ColKeyBuilder<_,_,_>, ColValSpine<_,_,_,_>>(&data, data_pact, "Data");
            let keys = arrange_core::<_,_,Col2KeyBatcher<_,_,_>, ColKeyBuilder<_,_,_>, ColValSpine<_,_,_,_>>(&keys, keys_pact, "Keys");

            keys.join_core(&data, |_k, &(), &()| Option::<()>::None)
                .probe_with(&mut probe);

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
                    assert!(b.len() % 8 == 0);
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
        #[inline]
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
            assert!(bytes.len() % 8 == 0);
            if let Ok(_) = bytemuck::try_cast_slice::<_, u64>(&bytes) {
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
        #[inline]
        fn push_into(&mut self, item: T) {
            self.current.push(item);
            // If there is less than 10% slop with 2MB backing allocations, mint a container.
            use columnar::Container;
            let words = Indexed::length_in_words(&self.current.borrow());
            let round = (words + ((1 << 18) - 1)) & !((1 << 18) - 1);
            if round - words < round / 10 {
                let mut alloc = Vec::with_capacity(words);
                Indexed::encode(&mut alloc, &self.current.borrow());
                self.pending.push_back(Column::Align(alloc.into_boxed_slice()));
                self.current.clear();
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
                use columnar::Container;
                let words = Indexed::length_in_words(&self.current.borrow());
                let mut alloc = Vec::with_capacity(words);
                Indexed::encode(&mut alloc, &self.current.borrow());
                self.pending.push_back(Column::Align(alloc.into_boxed_slice()));
                self.current.clear();
            }
            self.empty = self.pending.pop_front();
            self.empty.as_mut()
        }
    }

    impl<C: Columnar<Container: Clone>> LengthPreservingContainerBuilder for ColumnBuilder<C> { }
}

use batcher::Col2KeyBatcher;

/// Types for consolidating, merging, and extracting columnar update collections.
pub mod batcher {

    use std::collections::VecDeque;
    use columnar::Columnar;
    use timely::Container;
    use timely::container::{ContainerBuilder, PushInto};
    use differential_dataflow::difference::Semigroup;
    use crate::Column;

    use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;

    /// A batcher for columnar storage.
    pub type Col2ValBatcher<K, V, T, R> = MergeBatcher<Column<((K,V),T,R)>, Chunker<Column<((K,V),T,R)>>, merger::ColumnMerger<(K,V),T,R>>;
    pub type Col2KeyBatcher<K, T, R> = Col2ValBatcher<K, (), T, R>;    

    // First draft: build a "chunker" and a "merger".

    #[derive(Default)]
    pub struct Chunker<C> {
        /// Buffer into which we'll consolidate.
        ///
        /// Also the buffer where we'll stage responses to `extract` and `finish`.
        /// When these calls return, the buffer is available for reuse.
        empty: C,
        /// Consolidated buffers ready to go.
        ready: VecDeque<C>,
    }

    impl<C: Container + Clone + 'static> ContainerBuilder for Chunker<C> {
        type Container = C;

        fn extract(&mut self) -> Option<&mut Self::Container> {
            if let Some(ready) = self.ready.pop_front() {
                self.empty = ready;
                Some(&mut self.empty)
            } else {
                None
            }
        }

        fn finish(&mut self) -> Option<&mut Self::Container> {
            self.extract()
        }
    }

    impl<'a, D, T, R, C2> PushInto<&'a mut Column<(D, T, R)>> for Chunker<C2>
    where
        D: for<'b> Columnar<Ref<'b>: Ord>,
        T: for<'b> Columnar<Ref<'b>: Ord>,
        R: for<'b> Columnar<Ref<'b>: Ord> + for<'b> Semigroup<R::Ref<'b>>,
        C2: Container + for<'b> PushInto<&'b (D, T, R)>,
    {
        fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {

            // Scoped to let borrow through `permutation` drop.
            {
                // Sort input data
                // TODO: consider `Vec<usize>` that we retain, containing indexes.
                let mut permutation = Vec::with_capacity(container.len());
                permutation.extend(container.drain());
                permutation.sort();

                self.empty.clear();
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
                                self.empty.push_into(tuple);
                            }
                            prev_data = data;
                            prev_time = time;
                            prev_diff = <R as Columnar>::into_owned(diff);
                        }
                    }

                    if !prev_diff.is_zero() {
                        let tuple = (prev_data, prev_time, &prev_diff);
                        self.empty.push_into(tuple);
                    }
                }
            }

            if !self.empty.is_empty() {
                self.ready.push_back(std::mem::take(&mut self.empty));
            }
        }
    }

    /// Implementations of `ContainerQueue` and `MergerChunk` for `Column` containers (columnar).
    pub mod merger {

        use timely::progress::{Antichain, frontier::AntichainRef};
        use columnar::Columnar;

        use crate::container::Column;
        use differential_dataflow::difference::Semigroup;

        use differential_dataflow::trace::implementations::merge_batcher::container::{ContainerQueue, MergerChunk};
        use differential_dataflow::trace::implementations::merge_batcher::container::ContainerMerger;

        /// A `Merger` implementation backed by `Column` containers (Columnar).
        pub type ColumnMerger<D, T, R> = ContainerMerger<Column<(D,T,R)>,ColumnQueue<(D, T, R)>>;


        /// TODO
        pub struct ColumnQueue<T: Columnar> {
            list: Column<T>,
            head: usize,
        }

        impl<D: Ord + Columnar, T: Ord + Columnar, R: Columnar> ContainerQueue<Column<(D, T, R)>> for ColumnQueue<(D, T, R)> {
            fn next_or_alloc(&mut self) -> Result<<(D, T, R) as Columnar>::Ref<'_>, Column<(D, T, R)>> {
                if self.is_empty() {
                    Err(std::mem::take(&mut self.list))
                }
                else {
                    Ok(self.pop())
                }
            }
            fn is_empty(&self) -> bool {
                use timely::Container;
                self.head == self.list.len()
            }
            fn cmp_heads(&self, other: &Self) -> std::cmp::Ordering {
                let (data1, time1, _) = self.peek();
                let (data2, time2, _) = other.peek();

                let data1 = <D as Columnar>::into_owned(data1);
                let data2 = <D as Columnar>::into_owned(data2);
                let time1 = <T as Columnar>::into_owned(time1);
                let time2 = <T as Columnar>::into_owned(time2);

                (data1, time1).cmp(&(data2, time2))
            }
            fn from(list: Column<(D, T, R)>) -> Self {
                ColumnQueue { list, head: 0 }
            }
        }

        impl<T: Columnar> ColumnQueue<T> {
            fn pop(&mut self) -> T::Ref<'_> {
                self.head += 1;
                self.list.get(self.head - 1)
            }

            fn peek(&self) -> T::Ref<'_> {
                self.list.get(self.head)
            }
        }

        impl<D, T, R> MergerChunk for Column<(D, T, R)> 
        where
            D: Ord + Columnar + 'static,
            T: Ord + timely::PartialOrder + Clone + Columnar + 'static,
            for<'a> <T as Columnar>::Ref<'a> : Copy,
            R: Default + Semigroup + Columnar + 'static
        {
            type TimeOwned = T;
            type DiffOwned = R;

            fn time_kept((_, time, _): &Self::Item<'_>, upper: &AntichainRef<Self::TimeOwned>, frontier: &mut Antichain<Self::TimeOwned>) -> bool {
                let time = T::into_owned(*time);
                // let time = unimplemented!();
                if upper.less_equal(&time) {
                    frontier.insert(time);
                    true
                }
                else { false }
            }
            fn push_and_add<'a>(&mut self, item1: Self::Item<'a>, item2: Self::Item<'a>, stash: &mut Self::DiffOwned) {
                let (data, time, diff1) = item1;
                let (_data, _time, diff2) = item2;
                stash.copy_from(diff1);
                let stash2: R = R::into_owned(diff2);
                stash.plus_equals(&stash2);
                if !stash.is_zero() {
                    use timely::Container;
                    self.push((data, time, &*stash));
                }
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
    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, val_batch::OrdValStorage};

    use crate::Column;


    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use differential_dataflow::trace::implementations::TStack;
    
    pub type ColValBuilder<K, V, T, R> = RcBuilder<OrdValBuilder<TStack<((K,V),T,R)>>>;
    pub type ColKeyBuilder<K, T, R> = RcBuilder<OrdValBuilder<TStack<((K,()),T,R)>>>;    

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

    // The layout `L` determines the key, val, time, and diff types.
    impl<L> Builder for OrdValBuilder<L>
    where
        L: Layout,
        <L::KeyContainer as BatchContainer>::Owned: Columnar,
        <L::ValContainer as BatchContainer>::Owned: Columnar,
        <L::TimeContainer as BatchContainer>::Owned: Columnar,
        <L::DiffContainer as BatchContainer>::Owned: Columnar,
        // These two constraints seem .. like we could potentially replace by `Columnar::Ref<'a>`.
        for<'a> L::KeyContainer: PushInto<&'a <L::KeyContainer as BatchContainer>::Owned>,
        for<'a> L::ValContainer: PushInto<&'a <L::ValContainer as BatchContainer>::Owned>,
        for<'a> <L::TimeContainer as BatchContainer>::ReadItem<'a> : IntoOwned<'a, Owned = <L::Target as Update>::Time>,
        for<'a> <L::DiffContainer as BatchContainer>::ReadItem<'a> : IntoOwned<'a, Owned = <L::Target as Update>::Diff>,
    {
        type Input = Column<((<L::KeyContainer as BatchContainer>::Owned,<L::ValContainer as BatchContainer>::Owned),<L::TimeContainer as BatchContainer>::Owned,<L::DiffContainer as BatchContainer>::Owned)>;
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

            for ((key,val),time,diff) in chunk.drain() {
                // It would be great to avoid.
                let key  = <<L::KeyContainer as BatchContainer>::Owned as Columnar>::into_owned(key);
                let val  = <<L::ValContainer as BatchContainer>::Owned as Columnar>::into_owned(val);
                // These feel fine (wrt the other versions)
                let time = <<L::TimeContainer as BatchContainer>::Owned as Columnar>::into_owned(time);
                let diff = <<L::DiffContainer as BatchContainer>::Owned as Columnar>::into_owned(diff);

                // Perhaps this is a continuation of an already received key.
                if self.result.keys.last().map(|k| <<L::KeyContainer as BatchContainer>::ReadItem<'_> as IntoOwned>::borrow_as(&key).eq(&k)).unwrap_or(false) {
                    // Perhaps this is a continuation of an already received value.
                    if self.result.vals.last().map(|v| <<L::ValContainer as BatchContainer>::ReadItem<'_> as IntoOwned>::borrow_as(&val).eq(&v)).unwrap_or(false) {
                        self.push_update(time, diff);
                    } else {
                        // New value; complete representation of prior value.
                        self.result.vals_offs.push(self.result.times.len());
                        if self.singleton.take().is_some() { self.singletons += 1; }
                        self.push_update(time, diff);
                        self.result.vals.push(&val);
                    }
                } else {
                    // New key; complete representation of prior key.
                    self.result.vals_offs.push(self.result.times.len());
                    if self.singleton.take().is_some() { self.singletons += 1; }
                    self.result.keys_offs.push(self.result.vals.len());
                    self.push_update(time, diff);
                    self.result.vals.push(&val);
                    self.result.keys.push(&key);
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
}