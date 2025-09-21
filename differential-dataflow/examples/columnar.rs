//! Wordcount based on `columnar`.

use timely::container::{CapacityContainerBuilder, PushInto};
use timely::dataflow::channels::pact::ExchangeCore;
use timely::dataflow::InputHandleCore;
use timely::dataflow::ProbeHandle;

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

            use differential_dataflow::Hashable;
            use timely::dataflow::channels::pact::DistributorPact;
            let data_pact = DistributorPact(|peers| KeyDistributor::new(peers, |k: columnar::Ref<'_, Vec<u8>>| k.hashed()));
            let keys_pact = DistributorPact(|peers| KeyDistributor::new(peers, |k: columnar::Ref<'_, Vec<u8>>| k.hashed()));

            let data = arrange_core::<_,_,Col2KeyBatcher<_,_,_>, ColKeyBuilder<_,_,_>, ColKeySpine<_,_,_>>(&data, data_pact, "Data");
            let keys = arrange_core::<_,_,Col2KeyBatcher<_,_,_>, ColKeyBuilder<_,_,_>, ColKeySpine<_,_,_>>(&keys, keys_pact, "Keys");

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
                container.push_into(((&buffer, ()), time, 1));
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
                container.push_into(((&buffer, ()), time, 1));
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

    type BorrowedOf<'a, C> = <<C as Columnar>::Container as columnar::Container>::Borrowed<'a>;

    impl<C: Columnar> Column<C> {
        pub fn borrow(&self) -> BorrowedOf<'_, C> {
            match self {
                Column::Typed(t) => t.borrow(),
                Column::Bytes(b) => <BorrowedOf<C> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))),
                Column::Align(a) => <BorrowedOf<C> as FromBytes>::from_bytes(&mut Indexed::decode(a)),
            }
        }
        pub fn get(&self, index: usize) -> columnar::Ref<'_, C> {
            self.borrow().get(index)
        }
        // This sets the `Bytes` variant to be an empty `Typed` variant, appropriate for pushing into.
        pub(crate) fn clear(&mut self) {
            match self {
                Column::Typed(t) => t.clear(),
                Column::Bytes(_) => *self = Column::Typed(Default::default()),
                Column::Align(_) => *self = Column::Typed(Default::default()),
            }
        }
        #[inline]
        pub fn len(&self) -> usize {
            match self {
                Column::Typed(t) => t.len(),
                Column::Bytes(b) => <BorrowedOf<C> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).len(),
                Column::Align(a) => <BorrowedOf<C> as FromBytes>::from_bytes(&mut Indexed::decode(a)).len(),
            }
        }
    }

    impl<C: Columnar> timely::Accountable for Column<C> {
        #[inline] fn record_count(&self) -> i64 { self.len() as i64 }
    }

    impl<C: Columnar> timely::container::IterContainer for Column<C> {
        type ItemRef<'a> = columnar::Ref<'a, C>;
        type Iter<'a> = IterOwn<BorrowedOf<'a, C>>;
        fn iter<'a>(&'a self) -> Self::Iter<'a> {
            match self {
                Column::Typed(t) => t.borrow().into_index_iter(),
                Column::Bytes(b) => <BorrowedOf<C> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).into_index_iter(),
                Column::Align(a) => <BorrowedOf<C> as FromBytes>::from_bytes(&mut Indexed::decode(a)).into_index_iter(),
            }
        }
    }

    impl<C: Columnar> timely::container::DrainContainer for Column<C> {
        type Item<'a> = columnar::Ref<'a, C>;
        type DrainIter<'a> = IterOwn<BorrowedOf<'a, C>>;
        fn drain<'a>(&'a mut self) -> Self::DrainIter<'a> {
            match self {
                Column::Typed(t) => t.borrow().into_index_iter(),
                Column::Bytes(b) => <BorrowedOf<C> as FromBytes>::from_bytes(&mut Indexed::decode(bytemuck::cast_slice(b))).into_index_iter(),
                Column::Align(a) => <BorrowedOf<C> as FromBytes>::from_bytes(&mut Indexed::decode(a)).into_index_iter(),
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
    use timely::container::{ContainerBuilder, DrainContainer, PushInto, SizableContainer};
    use differential_dataflow::difference::Semigroup;
    use crate::Column;

    use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;

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

    impl<K, T, R> ColumnarUpdate for (K, T, R)
    where
        K: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Clone + 'static,
        T: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Default + Clone + Lattice + Timestamp,
        R: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Default + Semigroup + 'static,
    {
        type Key = K;
        type Val = ();
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

pub use storage::val::ValStorage;
pub use storage::key::KeyStorage;
pub mod storage {

    pub mod val {

        use std::fmt::Debug;
        use columnar::{Container, ContainerOf, Index, Len, Push};
        use columnar::Vecs;

        use crate::layout::ColumnarUpdate as Update;

        /// Trie-shaped update storage.
        #[derive(Debug)]
        pub struct ValStorage<U: Update> {
            /// An ordered list of keys.
            pub keys: ContainerOf<U::Key>,
            /// For each key in `keys`, a list of values.
            pub vals: Vecs<ContainerOf<U::Val>>,
            /// For each val in `vals`, a list of (time, diff) updates.
            pub upds: Vecs<(ContainerOf<U::Time>, ContainerOf<U::Diff>)>,
        }

        impl<U: Update> Default for ValStorage<U> { fn default() -> Self { Self { keys: Default::default(), vals: Default::default(), upds: Default::default(), } } }
        impl<U: Update> Clone for ValStorage<U> { fn clone(&self) -> Self { Self { keys: self.keys.clone(), vals: self.vals.clone(), upds: self.upds.clone(), } } }

        pub type Tuple<U> = (<U as Update>::Key, <U as Update>::Val, <U as Update>::Time, <U as Update>::Diff);

        use std::ops::Range;
        impl<U: Update> ValStorage<U> {

            /// Forms `Self` from sorted update tuples.
            pub fn form<'a>(sorted: impl Iterator<Item = columnar::Ref<'a, Tuple<U>>>) -> Self {

                let mut output = Self::default();
                let mut sorted = sorted.peekable();

                if let Some((key,val,time,diff)) = sorted.next() {
                    output.keys.push(key);
                    output.vals.values.push(val);
                    output.upds.values.push((time, diff));
                    for (key,val,time,diff) in sorted {
                        let mut differs = false;
                        // We would now iterate over layers.
                        // We'll do that manually, as the types are all different.
                        // Keys first; non-standard logic because they are not (yet) a list of lists.
                        let keys_len = output.keys.len();
                        differs |= ContainerOf::<U::Key>::reborrow_ref(key) != output.keys.borrow().get(keys_len-1);
                        if differs { output.keys.push(key); }
                        // Vals next
                        let vals_len = output.vals.values.len();
                        if differs { output.vals.bounds.push(vals_len as u64); }
                        differs |= ContainerOf::<U::Val>::reborrow_ref(val) != output.vals.values.borrow().get(vals_len-1);
                        if differs { output.vals.values.push(val); }
                        // Upds last
                        let upds_len = output.upds.values.len();
                        if differs { output.upds.bounds.push(upds_len as u64); }
                        // differs |= ContainerOf::<(U::Time,U::Diff)>::reborrow_ref((time,diff)) != output.upds.values.borrow().get(upds_len-1);
                        differs = true;
                        if differs { output.upds.values.push((time,diff)); }
                    }
                    // output.keys.bounds.push(vals_len as u64);
                    output.vals.bounds.push(output.vals.values.len() as u64);
                    output.upds.bounds.push(output.upds.values.len() as u64);
                }

                assert_eq!(output.keys.len(), output.vals.len());
                assert_eq!(output.vals.values.len(), output.upds.len());

                output
            }

            pub fn vals_bounds(&self, range: Range<usize>) -> Range<usize> {
                if !range.is_empty() {
                    let lower = if range.start == 0 { 0 } else { Index::get(self.vals.bounds.borrow(), range.start-1) as usize };
                    let upper = Index::get(self.vals.bounds.borrow(), range.end-1) as usize;
                    lower .. upper
                } else { range }
            }

            pub fn upds_bounds(&self, range: Range<usize>) -> Range<usize> {
                if !range.is_empty() {
                    let lower = if range.start == 0 { 0 } else { Index::get(self.upds.bounds.borrow(), range.start-1) as usize };
                    let upper = Index::get(self.upds.bounds.borrow(), range.end-1) as usize;
                    lower .. upper
                } else { range }
            }

            /// Copies `other[range]` into self, keys and all.
            pub fn extend_from_keys(&mut self, other: &Self, range: Range<usize>) {
                self.keys.extend_from_self(other.keys.borrow(), range.clone());
                self.vals.extend_from_self(other.vals.borrow(), range.clone());
                self.upds.extend_from_self(other.upds.borrow(), other.vals_bounds(range));
            }

            pub fn extend_from_vals(&mut self, other: &Self, range: Range<usize>) {
                self.vals.values.extend_from_self(other.vals.values.borrow(), range.clone());
                self.upds.extend_from_self(other.upds.borrow(), range);
            }
        }

        impl<U: Update> timely::Accountable for ValStorage<U> {
            #[inline] fn record_count(&self) -> i64 { use columnar::Len; self.upds.values.len() as i64 }
        }

        use timely::dataflow::channels::ContainerBytes;
        impl<U: Update> ContainerBytes for ValStorage<U> {
            fn from_bytes(_bytes: timely::bytes::arc::Bytes) -> Self { unimplemented!() }
            fn length_in_bytes(&self) -> usize { unimplemented!() }
            fn into_bytes<W: ::std::io::Write>(&self, _writer: &mut W) { unimplemented!() }
        }
    }

    pub mod key {

        use std::fmt::Debug;
        use columnar::{Container, ContainerOf, Index, Len, Push};
        use columnar::Vecs;

        use crate::layout::ColumnarUpdate as Update;

        /// Trie-shaped update storage.
        #[derive(Debug)]
        pub struct KeyStorage<U: Update> {
            /// An ordered list of keys.
            pub keys: ContainerOf<U::Key>,
            /// For each key in `keys`, a list of (time, diff) updates.
            pub upds: Vecs<(ContainerOf<U::Time>, ContainerOf<U::Diff>)>,
        }

        impl<U: Update> Default for KeyStorage<U> { fn default() -> Self { Self { keys: Default::default(), upds: Default::default(), } } }
        impl<U: Update> Clone for KeyStorage<U> { fn clone(&self) -> Self { Self { keys: self.keys.clone(), upds: self.upds.clone(), } } }

        pub type Tuple<U> = (<U as Update>::Key, <U as Update>::Time, <U as Update>::Diff);

        use std::ops::Range;
        impl<U: Update> KeyStorage<U> {

            /// Forms `Self` from sorted update tuples.
            pub fn form<'a>(sorted: impl Iterator<Item = columnar::Ref<'a, Tuple<U>>>) -> Self {

                let mut output = Self::default();
                let mut sorted = sorted.peekable();

                if let Some((key,time,diff)) = sorted.next() {
                    output.keys.push(key);
                    output.upds.values.push((time, diff));
                    for (key,time,diff) in sorted {
                        let mut differs = false;
                        // We would now iterate over layers.
                        // We'll do that manually, as the types are all different.
                        // Keys first; non-standard logic because they are not (yet) a list of lists.
                        let keys_len = output.keys.len();
                        differs |= ContainerOf::<U::Key>::reborrow_ref(key) != output.keys.borrow().get(keys_len-1);
                        if differs { output.keys.push(key); }
                        // Upds last
                        let upds_len = output.upds.values.len();
                        if differs { output.upds.bounds.push(upds_len as u64); }
                        // differs |= ContainerOf::<(U::Time,U::Diff)>::reborrow_ref((time,diff)) != output.upds.values.borrow().get(upds_len-1);
                        differs = true;
                        if differs { output.upds.values.push((time,diff)); }
                    }
                    output.upds.bounds.push(output.upds.values.len() as u64);
                }

                assert_eq!(output.keys.len(), output.upds.len());

                output
            }

            pub fn upds_bounds(&self, range: Range<usize>) -> Range<usize> {
                if !range.is_empty() {
                    let lower = if range.start == 0 { 0 } else { Index::get(self.upds.bounds.borrow(), range.start-1) as usize };
                    let upper = Index::get(self.upds.bounds.borrow(), range.end-1) as usize;
                    lower .. upper
                } else { range }
            }

            /// Copies `other[range]` into self, keys and all.
            pub fn extend_from_keys(&mut self, other: &Self, range: Range<usize>) {
                self.keys.extend_from_self(other.keys.borrow(), range.clone());
                self.upds.extend_from_self(other.upds.borrow(), range.clone());
            }
        }

        impl<U: Update> timely::Accountable for KeyStorage<U> {
            #[inline] fn record_count(&self) -> i64 { use columnar::Len; self.upds.values.len() as i64 }
        }

        use timely::dataflow::channels::ContainerBytes;
        impl<U: Update> ContainerBytes for KeyStorage<U> {
            fn from_bytes(_bytes: timely::bytes::arc::Bytes) -> Self { unimplemented!() }
            fn length_in_bytes(&self) -> usize { unimplemented!() }
            fn into_bytes<W: ::std::io::Write>(&self, _writer: &mut W) { unimplemented!() }
        }
    }
}

pub use column_builder::{val::ValBuilder as ValColBuilder, key::KeyBuilder as KeyColBuilder};
mod column_builder {

    pub mod val {

        use std::collections::VecDeque;
        use columnar::{Columnar, Clear, Len, Push};

        use crate::layout::ColumnarUpdate as Update;
        use crate::ValStorage;

        type TupleContainer<U> = <(<U as Update>::Key, <U as Update>::Val, <U as Update>::Time, <U as Update>::Diff) as Columnar>::Container;

        /// A container builder for `Column<C>`.
        pub struct ValBuilder<U: Update> {
            /// Container that we're writing to.
            current: TupleContainer<U>,
            /// Empty allocation.
            empty: Option<ValStorage<U>>,
            /// Completed containers pending to be sent.
            pending: VecDeque<ValStorage<U>>,
        }

        use timely::container::PushInto;
        impl<T, U: Update> PushInto<T> for ValBuilder<U> where TupleContainer<U> : Push<T> {
            #[inline]
            fn push_into(&mut self, item: T) {
                self.current.push(item);
                if self.current.len() > 1024 {
                    // TODO: Consolidate the batch?
                    use columnar::{Container, Index};
                    let mut refs = self.current.borrow().into_index_iter().collect::<Vec<_>>();
                    refs.sort();
                    let storage = ValStorage::form(refs.into_iter());
                    self.pending.push_back(storage);
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
            type Container = ValStorage<U>;

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
                    // TODO: Consolidate the batch?
                    use columnar::{Container, Index};
                    let mut refs = self.current.borrow().into_index_iter().collect::<Vec<_>>();
                    refs.sort();
                    let storage = ValStorage::form(refs.into_iter());
                    self.pending.push_back(storage);
                    self.current.clear();
                }
                self.empty = self.pending.pop_front();
                self.empty.as_mut()
            }
        }

        impl<U: Update> LengthPreservingContainerBuilder for ValBuilder<U> { }
    }

    pub mod key {

        use std::collections::VecDeque;
        use columnar::{Columnar, Clear, Len, Push};

        use crate::layout::ColumnarUpdate as Update;
        use crate::KeyStorage;

        type TupleContainer<U> = <(<U as Update>::Key, <U as Update>::Time, <U as Update>::Diff) as Columnar>::Container;

        /// A container builder for `Column<C>`.
        pub struct KeyBuilder<U: Update> {
            /// Container that we're writing to.
            current: TupleContainer<U>,
            /// Empty allocation.
            empty: Option<KeyStorage<U>>,
            /// Completed containers pending to be sent.
            pending: VecDeque<KeyStorage<U>>,
        }

        use timely::container::PushInto;
        impl<T, U: Update> PushInto<T> for KeyBuilder<U> where TupleContainer<U> : Push<T> {
            #[inline]
            fn push_into(&mut self, item: T) {
                self.current.push(item);
                if self.current.len() > 1024 {
                    // TODO: Consolidate the batch?
                    use columnar::{Container, Index};
                    let mut refs = self.current.borrow().into_index_iter().collect::<Vec<_>>();
                    refs.sort();
                    let storage = KeyStorage::form(refs.into_iter());
                    self.pending.push_back(storage);
                    self.current.clear();
                }
            }
        }

        impl<U: Update> Default for KeyBuilder<U> { fn default() -> Self { KeyBuilder { current: Default::default(), empty: None, pending: Default::default(), } } }

        use timely::container::{ContainerBuilder, LengthPreservingContainerBuilder};
        impl<U: Update> ContainerBuilder for KeyBuilder<U> {
            type Container = KeyStorage<U>;

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
                    // TODO: Consolidate the batch?
                    use columnar::{Container, Index};
                    let mut refs = self.current.borrow().into_index_iter().collect::<Vec<_>>();
                    refs.sort();
                    let storage = KeyStorage::form(refs.into_iter());
                    self.pending.push_back(storage);
                    self.current.clear();
                }
                self.empty = self.pending.pop_front();
                self.empty.as_mut()
            }
        }

        impl<U: Update> LengthPreservingContainerBuilder for KeyBuilder<U> { }
    }
}

use distributor::key::KeyDistributor;
mod distributor {

    pub mod key {

        use columnar::{Container, Index, Len};
        use timely::container::{ContainerBuilder, PushInto};
        use timely::dataflow::channels::{Message, pushers::exchange::Distributor};

        use crate::layout::ColumnarUpdate as Update;
        use crate::{KeyColBuilder, KeyStorage};

        pub struct KeyDistributor<U: Update, H> {
            builders: Vec<KeyColBuilder<U>>,
            hashfunc: H,
        }

        impl<U: Update, H: for<'a> FnMut(columnar::Ref<'a, U::Key>)->u64> Distributor<KeyStorage<U>> for KeyDistributor<U, H> {
            fn partition<T: Clone, P: timely::communication::Push<Message<T, KeyStorage<U>>>>(&mut self, container: &mut KeyStorage<U>, time: &T, pushers: &mut [P]) {
                // For each key, partition and copy (key, time, diff) into the appropriate self.builder.
                for index in 0 .. container.keys.len() {
                    let key = container.keys.borrow().get(index);
                    let idx = ((self.hashfunc)(key) as usize) % self.builders.len();
                    for (t, diff) in container.upds.borrow().get(index).into_index_iter() {
                        self.builders[idx].push_into((key, t, diff));
                    }
                    while let Some(produced) = self.builders[idx].extract() {
                        Message::push_at(produced, time.clone(), &mut pushers[idx]);
                    }
                }
            }
            fn flush<T: Clone, P: timely::communication::Push<Message<T, KeyStorage<U>>>>(&mut self, time: &T, pushers: &mut [P]) {
                for (builder, pusher) in self.builders.iter_mut().zip(pushers.iter_mut()) {
                    while let Some(container) = builder.finish() {
                        Message::push_at(container, time.clone(), pusher);
                    }
                }
            }
            fn relax(&mut self) { }
        }

        impl<U: Update, H> KeyDistributor<U, H> {
            pub fn new(peers: usize, hashfunc: H) -> Self {
                Self {
                    builders: std::iter::repeat_with(Default::default).take(peers).collect(),
                    hashfunc,
                }
            }
        }
    }
}

pub use arrangement::{ValBatcher, ValBuilder, ValSpine, KeyBatcher, KeyBuilder, KeySpine};
pub mod arrangement {

    use std::rc::Rc;
    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdKeyBatch};
    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use differential_dataflow::trace::implementations::spine_fueled::Spine;

    use crate::layout::ColumnarLayout;

    /// A trace implementation backed by columnar storage.
    pub type ValSpine<K, V, T, R> = Spine<Rc<OrdValBatch<ColumnarLayout<(K,V,T,R)>>>>;
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

    impl<C: Container> ContainerBuilder for Chunker<C> {
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
        D: for<'b> Columnar,
        for<'b> columnar::Ref<'b, D>: Ord,
        T: for<'b> Columnar,
        for<'b> columnar::Ref<'b, T>: Ord,
        R: for<'b> Columnar + for<'b> Semigroup<columnar::Ref<'b, R>>,
        for<'b> columnar::Ref<'b, R>: Ord,
        C2: Container + SizableContainer + for<'b, 'c> PushInto<(columnar::Ref<'b, D>, columnar::Ref<'b, T>, &'c R)>,
    {
        fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {
            let mut target: C2 = Default::default();
            target.ensure_capacity(&mut Some(std::mem::take(&mut self.empty)));

            // Scoped to let borrow through `permutation` drop.
            {
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
                                target.push_into(tuple);
                            }
                            prev_data = data;
                            prev_time = time;
                            prev_diff = <R as Columnar>::into_owned(diff);
                        }
                    }

                    if !prev_diff.is_zero() {
                        let tuple = (prev_data, prev_time, &prev_diff);
                        target.push_into(tuple);
                    }
                }
            }

            if !target.is_empty() {
                self.ready.push_back(target);
            }
        }
    }

    /// Implementations of `ContainerQueue` and `MergerChunk` for `Column` containers (columnar).
    pub mod merger {

        use timely::progress::{Antichain, frontier::AntichainRef};
        use columnar::Columnar;
        use timely::container::PushInto;
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

        impl<D, T, R> ContainerQueue<Column<(D, T, R)>> for ColumnQueue<(D, T, R)>
        where
            D: for<'a> Columnar,
            for<'b> columnar::Ref<'b, D>: Ord,
            T: for<'a> Columnar,
            for<'b> columnar::Ref<'b, T>: Ord,
            R: Columnar,
        {
            fn next_or_alloc(&mut self) -> Result<columnar::Ref<'_, (D, T, R)>, Column<(D, T, R)>> {
                if self.is_empty() {
                    Err(std::mem::take(&mut self.list))
                }
                else {
                    Ok(self.pop())
                }
            }
            fn is_empty(&self) -> bool {
                self.head == self.list.len()
            }
            fn cmp_heads(&self, other: &Self) -> std::cmp::Ordering {
                let (data1, time1, _) = self.peek();
                let (data2, time2, _) = other.peek();

                (data1, time1).cmp(&(data2, time2))
            }
            fn from(list: Column<(D, T, R)>) -> Self {
                ColumnQueue { list, head: 0 }
            }
        }

        impl<T: Columnar> ColumnQueue<T> {
            fn pop(&mut self) -> columnar::Ref<'_, T> {
                self.head += 1;
                self.list.get(self.head - 1)
            }

            fn peek(&self) -> columnar::Ref<'_, T> {
                self.list.get(self.head)
            }
        }

        impl<D, T, R> MergerChunk for Column<(D, T, R)>
        where
            D: Columnar + 'static,
            T: timely::PartialOrder + Clone + Columnar + 'static,
            R: Default + Semigroup + Columnar + 'static
        {
            type TimeOwned = T;
            type DiffOwned = R;

            fn time_kept((_, time, _): &Self::Item<'_>, upper: &AntichainRef<Self::TimeOwned>, frontier: &mut Antichain<Self::TimeOwned>) -> bool {
                let time = T::into_owned(*time);
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
                    self.push_into((data, time, &*stash));
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
            #[inline] fn clear(&mut self) { self.clear() }
        }
    }

}

use dd_builder::ColKeyBuilder;

pub mod dd_builder {

    use columnar::Columnar;
    use timely::container::DrainContainer;
    use differential_dataflow::trace::Builder;
    use differential_dataflow::trace::Description;
    use differential_dataflow::trace::implementations::Layout;
    use differential_dataflow::trace::implementations::layout;
    use differential_dataflow::trace::implementations::BatchContainer;
    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, val_batch::OrdValStorage, OrdKeyBatch, Vals, Upds, layers::UpdsBuilder};
    use differential_dataflow::trace::implementations::ord_neu::key_batch::OrdKeyStorage;
    use crate::Column;


    use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
    use differential_dataflow::trace::implementations::TStack;

    pub type ColValBuilder<K, V, T, R> = RcBuilder<OrdValBuilder<TStack<((K,V),T,R)>>>;
    pub type ColKeyBuilder<K, T, R> = RcBuilder<OrdKeyBuilder<TStack<((K,()),T,R)>>>;

    /// A builder for creating layers from unsorted update tuples.
    pub struct OrdValBuilder<L: Layout> {
        /// The in-progress result.
        ///
        /// This is public to allow container implementors to set and inspect their container.
        pub result: OrdValStorage<L>,
        staging: UpdsBuilder<L::TimeContainer, L::DiffContainer>,
    }

    // The layout `L` determines the key, val, time, and diff types.
    impl<L> Builder for OrdValBuilder<L>
    where
        L: Layout,
        layout::Key<L>: Columnar,
        layout::Val<L>: Columnar,
        layout::Time<L>: Columnar,
        layout::Diff<L>: Columnar,
    {
        type Input = Column<((layout::Key<L>,layout::Val<L>),layout::Time<L>,layout::Diff<L>)>;
        type Time = layout::Time<L>;
        type Output = OrdValBatch<L>;

        fn with_capacity(keys: usize, vals: usize, upds: usize) -> Self {
            Self {
                result: OrdValStorage {
                    keys: L::KeyContainer::with_capacity(keys),
                    vals: Vals::with_capacity(keys + 1, vals),
                    upds: Upds::with_capacity(vals + 1, upds),
                },
                staging: UpdsBuilder::default(),
            }
        }

        #[inline]
        fn push(&mut self, chunk: &mut Self::Input) {
            // NB: Maintaining owned key and val across iterations to track the "last", which we clone into,
            // is somewhat appealing from an ease point of view. Might still allocate, do work we don't need,
            // but avoids e.g. calls into `last()` and breaks horrid trait requirements.
            // Owned key and val would need to be members of `self`, as this method can be called multiple times,
            // and we need to correctly cache last for reasons of correctness, not just performance.

            let mut key_con = L::KeyContainer::with_capacity(1);
            let mut val_con = L::ValContainer::with_capacity(1);

            for ((key,val),time,diff) in chunk.drain() {
                // It would be great to avoid.
                let key  = <layout::Key<L> as Columnar>::into_owned(key);
                let val  = <layout::Val<L> as Columnar>::into_owned(val);
                // These feel fine (wrt the other versions)
                let time = <layout::Time<L> as Columnar>::into_owned(time);
                let diff = <layout::Diff<L> as Columnar>::into_owned(diff);

                key_con.clear(); key_con.push_own(&key);
                val_con.clear(); val_con.push_own(&val);

                // Pre-load the first update.
                if self.result.keys.is_empty() {
                    self.result.vals.vals.push_own(&val);
                    self.result.keys.push_own(&key);
                    self.staging.push(time, diff);
                }
                // Perhaps this is a continuation of an already received key.
                else if self.result.keys.last() == key_con.get(0) {
                    // Perhaps this is a continuation of an already received value.
                    if self.result.vals.vals.last() == val_con.get(0) {
                        self.staging.push(time, diff);
                    } else {
                        // New value; complete representation of prior value.
                        self.staging.seal(&mut self.result.upds);
                        self.staging.push(time, diff);
                        self.result.vals.vals.push_own(&val);
                    }
                } else {
                    // New key; complete representation of prior key.
                    self.staging.seal(&mut self.result.upds);
                    self.staging.push(time, diff);
                    self.result.vals.offs.push_ref(self.result.vals.len());
                    self.result.vals.vals.push_own(&val);
                    self.result.keys.push_own(&key);
                }
            }
        }

        #[inline(never)]
        fn done(mut self, description: Description<Self::Time>) -> OrdValBatch<L> {
            self.staging.seal(&mut self.result.upds);
            self.result.vals.offs.push_ref(self.result.vals.len());
            OrdValBatch {
                updates: self.staging.total(),
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
        staging: UpdsBuilder<L::TimeContainer, L::DiffContainer>,
    }

    // The layout `L` determines the key, val, time, and diff types.
    impl<L> Builder for OrdKeyBuilder<L>
    where
        L: Layout,
        layout::Key<L>: Columnar,
        layout::Val<L>: Columnar,
        layout::Time<L>: Columnar,
        layout::Diff<L>: Columnar,
    {
        type Input = Column<((layout::Key<L>,layout::Val<L>),layout::Time<L>,layout::Diff<L>)>;
        type Time = layout::Time<L>;
        type Output = OrdKeyBatch<L>;

        fn with_capacity(keys: usize, _vals: usize, upds: usize) -> Self {
            Self {
                result: OrdKeyStorage {
                    keys: L::KeyContainer::with_capacity(keys),
                    upds: Upds::with_capacity(keys + 1, upds),
                },
                staging: UpdsBuilder::default(),
            }
        }

        #[inline]
        fn push(&mut self, chunk: &mut Self::Input) {
            // NB: Maintaining owned key and val across iterations to track the "last", which we clone into,
            // is somewhat appealing from an ease point of view. Might still allocate, do work we don't need,
            // but avoids e.g. calls into `last()` and breaks horrid trait requirements.
            // Owned key and val would need to be members of `self`, as this method can be called multiple times,
            // and we need to correctly cache last for reasons of correctness, not just performance.

            let mut key_con = L::KeyContainer::with_capacity(1);

            for ((key,_val),time,diff) in chunk.drain() {
                // It would be great to avoid.
                let key  = <layout::Key<L> as Columnar>::into_owned(key);
                // These feel fine (wrt the other versions)
                let time = <layout::Time<L> as Columnar>::into_owned(time);
                let diff = <layout::Diff<L> as Columnar>::into_owned(diff);

                key_con.clear(); key_con.push_own(&key);

                // Pre-load the first update.
                if self.result.keys.is_empty() {
                    self.result.keys.push_own(&key);
                    self.staging.push(time, diff);
                }
                // Perhaps this is a continuation of an already received key.
                else if self.result.keys.last() == key_con.get(0) {
                    self.staging.push(time, diff);
                } else {
                    // New key; complete representation of prior key.
                    self.staging.seal(&mut self.result.upds);
                    self.staging.push(time, diff);
                    self.result.keys.push_own(&key);
                }
            }
        }

        #[inline(never)]
        fn done(mut self, description: Description<Self::Time>) -> OrdKeyBatch<L> {
            self.staging.seal(&mut self.result.upds);
            OrdKeyBatch {
                updates: self.staging.total(),
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
