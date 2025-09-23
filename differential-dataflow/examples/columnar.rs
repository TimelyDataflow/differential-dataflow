//! Wordcount based on `columnar`.

use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::InputHandleCore;
use timely::dataflow::ProbeHandle;

use differential_dataflow::operators::arrange::arrangement::arrange_core;

fn main() {

    type WordCount = (Vec<u8>, u64, i64);
    type Builder = KeyColBuilder<WordCount>;

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

        let mut data_input = <InputHandleCore<_, Builder>>::new_with_builder();
        let mut keys_input = <InputHandleCore<_, Builder>>::new_with_builder();
        let mut probe = ProbeHandle::new();

        // create a new input, exchange data, and inspect its output
        worker.dataflow::<u64, _, _>(|scope| {

            let data = data_input.to_stream(scope);
            let keys = keys_input.to_stream(scope);

            use differential_dataflow::Hashable;
            let data_pact = KeyPact { hashfunc: |k: columnar::Ref<'_, Vec<u8>>| k.hashed() };
            let keys_pact = KeyPact { hashfunc: |k: columnar::Ref<'_, Vec<u8>>| k.hashed() };

            let data = arrange_core::<_,_,KeyBatcher<_,_,_>, KeyBuilder<_,_,_>, KeySpine<_,_,_>>(&data, data_pact, "Data");
            let keys = arrange_core::<_,_,KeyBatcher<_,_,_>, KeyBuilder<_,_,_>, KeySpine<_,_,_>>(&keys, keys_pact, "Keys");

            keys.join_core(&data, |_k, (), ()| { Option::<()>::None })
                .probe_with(&mut probe);
        });

        // Resources for placing input data in containers.
        use std::fmt::Write;
        let mut buffer = String::default();
        let mut builder = KeyColBuilder::<WordCount>::default();

        // Load up data in batches.
        let mut counter = 0;
        while counter < 10 * keys {
            let mut i = worker.index();
            let time = *data_input.time();
            while i < size {
                let val = (counter + i) % keys;
                write!(buffer, "{:?}", val).unwrap();
                builder.push_into((buffer.as_bytes(), time, 1));
                buffer.clear();
                i += worker.peers();
            }
            while let Some(container) = builder.finish() {
                data_input.send_batch(container);
            }
            counter += size;
            data_input.advance_to(data_input.time() + 1);
            keys_input.advance_to(keys_input.time() + 1);
            while probe.less_than(data_input.time()) {
                worker.step_or_park(None);
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
                builder.push_into((buffer.as_bytes(), time, 1));
                buffer.clear();
                i += worker.peers();
            }
            while let Some(container) = builder.finish() {
                keys_input.send_batch(container);
            }
            queries += size;
            data_input.advance_to(data_input.time() + 1);
            keys_input.advance_to(keys_input.time() + 1);
            while probe.less_than(data_input.time()) {
                worker.step_or_park(None);
            }
        }
        println!("{:?}\tqueries complete", timer1.elapsed());

    })
    .unwrap();

    println!("{:?}\tshut down", timer2.elapsed());
}

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

use distributor::key::KeyPact;
mod distributor {

    pub mod key {

        use std::rc::Rc;

        use columnar::{Container, Index, Len};
        use timely::container::{ContainerBuilder, PushInto};
        use timely::logging::TimelyLogger;
        use timely::dataflow::channels::pushers::{Exchange, exchange::Distributor};
        use timely::dataflow::channels::Message;
        use timely::dataflow::channels::pact::{LogPuller, LogPusher, ParallelizationContract};
        use timely::progress::Timestamp;
        use timely::worker::AsWorker;

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

        pub struct KeyPact<H> { pub hashfunc: H }

        // Exchange uses a `Box<Pushable>` because it cannot know what type of pushable will return from the allocator.
        impl<T, U, H> ParallelizationContract<T, KeyStorage<U>> for KeyPact<H>
        where
            T: Timestamp,
            U: Update,
            H: for<'a> FnMut(columnar::Ref<'a, U::Key>)->u64 + 'static,
        {
            type Pusher = Exchange<
                T,
                LogPusher<Box<dyn timely::communication::Push<Message<T, KeyStorage<U>>>>>,
                KeyDistributor<U, H>
            >;
            type Puller = LogPuller<Box<dyn timely::communication::Pull<Message<T, KeyStorage<U>>>>>;

            fn connect<A: AsWorker>(self, allocator: &mut A, identifier: usize, address: Rc<[usize]>, logging: Option<TimelyLogger>) -> (Self::Pusher, Self::Puller) {
                let (senders, receiver) = allocator.allocate::<Message<T, KeyStorage<U>>>(identifier, address);
                let senders = senders.into_iter().enumerate().map(|(i,x)| LogPusher::new(x, allocator.index(), i, identifier, logging.clone())).collect::<Vec<_>>();
                let distributor = KeyDistributor {
                    builders: std::iter::repeat_with(Default::default).take(allocator.peers()).collect(),
                    hashfunc: self.hashfunc,
                };
                (Exchange::new(senders, distributor), LogPuller::new(receiver, allocator.index(), identifier, logging.clone()))
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
    pub type ValBatcher<K, V, T, R> = ValBatcher2<(K,V,T,R)>;
    /// A builder for columnar storage.
    pub type ValBuilder<K, V, T, R> = RcBuilder<ValMirror<(K,V,T,R)>>;

    /// A trace implementation backed by columnar storage.
    pub type KeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<ColumnarLayout<(K,T,R)>>>>;
    /// A batcher for columnar storage
    pub type KeyBatcher<K, T, R> = KeyBatcher2<(K,T,R)>;
    /// A builder for columnar storage
    pub type KeyBuilder<K, T, R> = RcBuilder<KeyMirror<(K,T,R)>>;

    /// A batch container implementation for Column<C>.
    pub use batch_container::Coltainer;
    pub mod batch_container {

        use columnar::{Columnar, Container, Clear, Push, Index, Len};
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

    use crate::{ColumnarUpdate, ValStorage, KeyStorage};
    use differential_dataflow::trace::implementations::chainless_batcher as chainless;
    type ValBatcher2<U> = chainless::Batcher<<U as ColumnarUpdate>::Time, ValStorage<U>>;
    type KeyBatcher2<U> = chainless::Batcher<<U as ColumnarUpdate>::Time, KeyStorage<U>>;
    pub mod batcher {

        use std::ops::Range;
        use columnar::{Columnar, Container, Index, Len, Push};
        use differential_dataflow::trace::implementations::chainless_batcher as chainless;
        use differential_dataflow::difference::{Semigroup, IsZero};
        use timely::progress::frontier::{Antichain, AntichainRef};

        use crate::ColumnarUpdate as Update;
        use crate::{ValStorage, KeyStorage};

        impl<U: Update> chainless::BatcherStorage<U::Time> for ValStorage<U> {

            fn len(&self) -> usize { self.upds.values.len() }

            fn merge(self, other: Self) -> Self {

                let mut this_sum = U::Diff::default();
                let mut that_sum = U::Diff::default();

                let mut merged = Self::default();
                let this = self;
                let that = other;
                let mut this_key_range = 0 .. this.keys.len();
                let mut that_key_range = 0 .. that.keys.len();
                while !this_key_range.is_empty() && !that_key_range.is_empty() {
                    let this_key = this.keys.borrow().get(this_key_range.start);
                    let that_key = that.keys.borrow().get(that_key_range.start);
                    match this_key.cmp(&that_key) {
                        std::cmp::Ordering::Less => {
                            let lower = this_key_range.start;
                            gallop(this.keys.borrow(), &mut this_key_range, |x| x < that_key);
                            merged.extend_from_keys(&this, lower .. this_key_range.start);
                        },
                        std::cmp::Ordering::Equal => {
                            // keys are equal; must make a bespoke vals list.
                            // only push the key if merged.vals.values.len() advances.
                            let values_len = merged.vals.values.len();
                            let mut this_val_range = this.vals_bounds(this_key_range.start .. this_key_range.start+1);
                            let mut that_val_range = that.vals_bounds(that_key_range.start .. that_key_range.start+1);
                            while !this_val_range.is_empty() && !that_val_range.is_empty() {
                                let this_val = this.vals.values.borrow().get(this_val_range.start);
                                let that_val = that.vals.values.borrow().get(that_val_range.start);
                                match this_val.cmp(&that_val) {
                                    std::cmp::Ordering::Less => {
                                        let lower = this_val_range.start;
                                        gallop(this.vals.values.borrow(), &mut this_val_range, |x| x < that_val);
                                        merged.extend_from_vals(&this, lower .. this_val_range.start);
                                    },
                                    std::cmp::Ordering::Equal => {
                                        // vals are equal; must make a bespoke upds list.
                                        // only push the val if merged.upds.values.len() advances.
                                        let updates_len = merged.upds.values.len();
                                        let mut this_upd_range = this.upds_bounds(this_val_range.start .. this_val_range.start+1);
                                        let mut that_upd_range = that.upds_bounds(that_val_range.start .. that_val_range.start+1);

                                        while !this_upd_range.is_empty() && !that_upd_range.is_empty() {
                                            let (this_time, this_diff) = this.upds.values.borrow().get(this_upd_range.start);
                                            let (that_time, that_diff) = that.upds.values.borrow().get(that_upd_range.start);
                                            match this_time.cmp(&that_time) {
                                                std::cmp::Ordering::Less => {
                                                    let lower = this_upd_range.start;
                                                    gallop(this.upds.values.0.borrow(), &mut this_upd_range, |x| x < that_time);
                                                    merged.upds.values.extend_from_self(this.upds.values.borrow(), lower .. this_upd_range.start);
                                                },
                                                std::cmp::Ordering::Equal => {
                                                    // times are equal; must add diffs.
                                                    this_sum.copy_from(this_diff);
                                                    that_sum.copy_from(that_diff);
                                                    this_sum.plus_equals(&that_sum);
                                                    if !this_sum.is_zero() { merged.upds.values.push((this_time, &this_sum)); }
                                                    // Advance the update ranges by one.
                                                    this_upd_range.start += 1;
                                                    that_upd_range.start += 1;
                                                },
                                                std::cmp::Ordering::Greater => {
                                                    let lower = that_upd_range.start;
                                                    gallop(that.upds.values.0.borrow(), &mut that_upd_range, |x| x < this_time);
                                                    merged.upds.values.extend_from_self(that.upds.values.borrow(), lower .. that_upd_range.start);
                                                },
                                            }
                                        }
                                        // Extend with the remaining this and that updates.
                                        merged.upds.values.extend_from_self(this.upds.values.borrow(), this_upd_range);
                                        merged.upds.values.extend_from_self(that.upds.values.borrow(), that_upd_range);
                                        // Seal the updates and push the val.
                                        if merged.upds.values.len() > updates_len {
                                            merged.upds.bounds.push(merged.upds.values.len() as u64);
                                            merged.vals.values.push(this_val);
                                        }
                                        // Advance the val ranges by one.
                                        this_val_range.start += 1;
                                        that_val_range.start += 1;
                                    },
                                    std::cmp::Ordering::Greater => {
                                        let lower = that_val_range.start;
                                        gallop(that.vals.values.borrow(), &mut that_val_range, |x| x < this_val);
                                        merged.extend_from_vals(&that, lower .. that_val_range.start);
                                    },
                                }
                            }
                            // Extend with the remaining this and that values.
                            merged.extend_from_vals(&this, this_val_range);
                            merged.extend_from_vals(&that, that_val_range);
                            // Seal the values and push the key.
                            if merged.vals.values.len() > values_len {
                                merged.vals.bounds.push(merged.vals.values.len() as u64);
                                merged.keys.push(this_key);
                            }
                            // Advance the key ranges by one.
                            this_key_range.start += 1;
                            that_key_range.start += 1;
                        },
                        std::cmp::Ordering::Greater => {
                            let lower = that_key_range.start;
                            gallop(that.keys.borrow(), &mut that_key_range, |x| x < this_key);
                            merged.extend_from_keys(&that, lower .. that_key_range.start);
                        },
                    }
                }
                // Extend with the remaining this and that keys.
                merged.extend_from_keys(&this, this_key_range);
                merged.extend_from_keys(&that, that_key_range);

                merged
            }

            fn split(&mut self, frontier: AntichainRef<U::Time>) -> Self {
                // Unfortunately the times are at the leaves, so there can be no bulk copying.
                let mut ship = Self::default();
                let mut keep = Self::default();
                let mut time = U::Time::default();
                for key_idx in 0 .. self.keys.len() {
                    let key = self.keys.borrow().get(key_idx);
                    let keep_vals_len = keep.vals.values.len();
                    let ship_vals_len = ship.vals.values.len();
                    for val_idx in self.vals_bounds(key_idx..key_idx+1) {
                        let val = self.vals.values.borrow().get(val_idx);
                        let keep_upds_len = keep.upds.values.len();
                        let ship_upds_len = ship.upds.values.len();
                        for upd_idx in self.upds_bounds(val_idx..val_idx+1) {
                            let (t, diff) = self.upds.values.borrow().get(upd_idx);
                            time.copy_from(t);
                            if frontier.less_equal(&time) {
                                keep.upds.values.push((t, diff));
                            }
                            else {
                                ship.upds.values.push((t, diff));
                            }
                        }
                        if keep.upds.values.len() > keep_upds_len {
                            keep.upds.bounds.push(keep.upds.values.len() as u64);
                            keep.vals.values.push(val);
                        }
                        if ship.upds.values.len() > ship_upds_len {
                            ship.upds.bounds.push(ship.upds.values.len() as u64);
                            ship.vals.values.push(val);
                        }
                    }
                    if keep.vals.values.len() > keep_vals_len {
                        keep.vals.bounds.push(keep.vals.values.len() as u64);
                        keep.keys.push(key);
                    }
                    if ship.vals.values.len() > ship_vals_len {
                        ship.vals.bounds.push(ship.vals.values.len() as u64);
                        ship.keys.push(key);
                    }
                }

                *self = keep;
                ship
            }

            fn lower(&self, frontier: &mut Antichain<U::Time>) {
                use columnar::Columnar;
                let mut times = self.upds.values.0.borrow().into_index_iter();
                if let Some(time_ref) = times.next() {
                    let mut time = <U::Time as Columnar>::into_owned(time_ref);
                    frontier.insert_ref(&time);
                    for time_ref in times {
                        <U::Time as Columnar>::copy_from(&mut time, time_ref);
                        frontier.insert_ref(&time);
                    }
                }
            }
        }

        impl<U: Update> chainless::BatcherStorage<U::Time> for KeyStorage<U> {

            fn len(&self) -> usize { self.upds.values.len() }

            fn merge(self, other: Self) -> Self {

                let mut this_sum = U::Diff::default();
                let mut that_sum = U::Diff::default();

                let mut merged = Self::default();
                let this = self;
                let that = other;
                let mut this_key_range = 0 .. this.keys.len();
                let mut that_key_range = 0 .. that.keys.len();
                while !this_key_range.is_empty() && !that_key_range.is_empty() {
                    let this_key = this.keys.borrow().get(this_key_range.start);
                    let that_key = that.keys.borrow().get(that_key_range.start);
                    match this_key.cmp(&that_key) {
                        std::cmp::Ordering::Less => {
                            let lower = this_key_range.start;
                            gallop(this.keys.borrow(), &mut this_key_range, |x| x < that_key);
                            merged.extend_from_keys(&this, lower .. this_key_range.start);
                        },
                        std::cmp::Ordering::Equal => {
                            // keys are equal; must make a bespoke vals list.
                            // only push the key if merged.vals.values.len() advances.
                            let updates_len = merged.upds.values.len();
                            let mut this_upd_range = this.upds_bounds(this_key_range.start .. this_key_range.start+1);
                            let mut that_upd_range = that.upds_bounds(that_key_range.start .. that_key_range.start+1);

                            while !this_upd_range.is_empty() && !that_upd_range.is_empty() {
                                let (this_time, this_diff) = this.upds.values.borrow().get(this_upd_range.start);
                                let (that_time, that_diff) = that.upds.values.borrow().get(that_upd_range.start);
                                match this_time.cmp(&that_time) {
                                    std::cmp::Ordering::Less => {
                                        let lower = this_upd_range.start;
                                        gallop(this.upds.values.0.borrow(), &mut this_upd_range, |x| x < that_time);
                                        merged.upds.values.extend_from_self(this.upds.values.borrow(), lower .. this_upd_range.start);
                                    },
                                    std::cmp::Ordering::Equal => {
                                        // times are equal; must add diffs.
                                        this_sum.copy_from(this_diff);
                                        that_sum.copy_from(that_diff);
                                        this_sum.plus_equals(&that_sum);
                                        if !this_sum.is_zero() { merged.upds.values.push((this_time, &this_sum)); }
                                        // Advance the update ranges by one.
                                        this_upd_range.start += 1;
                                        that_upd_range.start += 1;
                                    },
                                    std::cmp::Ordering::Greater => {
                                        let lower = that_upd_range.start;
                                        gallop(that.upds.values.0.borrow(), &mut that_upd_range, |x| x < this_time);
                                        merged.upds.values.extend_from_self(that.upds.values.borrow(), lower .. that_upd_range.start);
                                    },
                                }
                            }
                            // Extend with the remaining this and that updates.
                            merged.upds.values.extend_from_self(this.upds.values.borrow(), this_upd_range);
                            merged.upds.values.extend_from_self(that.upds.values.borrow(), that_upd_range);
                            // Seal the values and push the key.
                            if merged.upds.values.len() > updates_len {
                                merged.upds.bounds.push(merged.upds.values.len() as u64);
                                merged.keys.push(this_key);
                            }
                            // Advance the key ranges by one.
                            this_key_range.start += 1;
                            that_key_range.start += 1;
                        },
                        std::cmp::Ordering::Greater => {
                            let lower = that_key_range.start;
                            gallop(that.keys.borrow(), &mut that_key_range, |x| x < this_key);
                            merged.extend_from_keys(&that, lower .. that_key_range.start);
                        },
                    }
                }
                // Extend with the remaining this and that keys.
                merged.extend_from_keys(&this, this_key_range);
                merged.extend_from_keys(&that, that_key_range);

                merged
            }

            fn split(&mut self, frontier: AntichainRef<U::Time>) -> Self {
                // Unfortunately the times are at the leaves, so there can be no bulk copying.
                let mut ship = Self::default();
                let mut keep = Self::default();
                let mut time = U::Time::default();
                for key_idx in 0 .. self.keys.len() {
                    let key = self.keys.borrow().get(key_idx);
                    let keep_upds_len = keep.upds.values.len();
                    let ship_upds_len = ship.upds.values.len();
                    for upd_idx in self.upds_bounds(key_idx..key_idx+1) {
                        let (t, diff) = self.upds.values.borrow().get(upd_idx);
                        time.copy_from(t);
                        if frontier.less_equal(&time) {
                            keep.upds.values.push((t, diff));
                        }
                        else {
                            ship.upds.values.push((t, diff));
                        }
                    }
                    if keep.upds.values.len() > keep_upds_len {
                        keep.upds.bounds.push(keep.upds.values.len() as u64);
                        keep.keys.push(key);
                    }
                    if ship.upds.values.len() > ship_upds_len {
                        ship.upds.bounds.push(ship.upds.values.len() as u64);
                        ship.keys.push(key);
                    }
                }

                *self = keep;
                ship
            }

            fn lower(&self, frontier: &mut Antichain<U::Time>) {
                use columnar::Columnar;
                let mut times = self.upds.values.0.borrow().into_index_iter();
                if let Some(time_ref) = times.next() {
                    let mut time = <U::Time as Columnar>::into_owned(time_ref);
                    frontier.insert_ref(&time);
                    for time_ref in times {
                        <U::Time as Columnar>::copy_from(&mut time, time_ref);
                        frontier.insert_ref(&time);
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

    use builder::val::ValMirror;
    use builder::key::KeyMirror;
    pub mod builder {

        pub mod val {

            use differential_dataflow::trace::implementations::ord_neu::{Vals, Upds};
            use differential_dataflow::trace::implementations::ord_neu::val_batch::{OrdValBatch, OrdValStorage};
            use differential_dataflow::trace::Description;

            use crate::ValStorage;
            use crate::layout::ColumnarUpdate as Update;
            use crate::layout::ColumnarLayout as Layout;
            use crate::arrangement::Coltainer;

            use differential_dataflow::trace::implementations::OffsetList;
            fn vec_u64_to_offset_list(list: Vec<u64>) -> OffsetList {
                let mut output = OffsetList::with_capacity(list.len());
                output.push(0);
                for item in list { output.push(item as usize); }
                output
            }

            pub struct ValMirror<U: Update> { marker: std::marker::PhantomData<U> }
            impl<U: Update> differential_dataflow::trace::Builder for ValMirror<U> {
                type Time = U::Time;
                type Input = ValStorage<U>;
                type Output = OrdValBatch<Layout<U>>;

                fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self { Self { marker: std::marker::PhantomData } }
                fn push(&mut self, _chunk: &mut Self::Input) { unimplemented!() }
                fn done(self, _description: Description<Self::Time>) -> Self::Output { unimplemented!() }
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
                        let updates = storage.upds.len();
                        let storage = OrdValStorage {
                            keys: Coltainer { container: storage.keys },
                            vals: Vals {
                                offs: vec_u64_to_offset_list(storage.vals.bounds),
                                vals: Coltainer { container: storage.vals.values },
                            },
                            upds: Upds {
                                offs: vec_u64_to_offset_list(storage.upds.bounds),
                                times: Coltainer { container: storage.upds.values.0 },
                                diffs: Coltainer { container: storage.upds.values.1 },
                            },
                        };
                        OrdValBatch { storage, description, updates }
                    }
                    else {
                        println!("chain length: {:?}", chain.len());
                        unimplemented!()
                    }
                }
            }
        }

        pub mod key {

            use differential_dataflow::trace::implementations::ord_neu::Upds;
            use differential_dataflow::trace::implementations::ord_neu::key_batch::{OrdKeyBatch, OrdKeyStorage};
            use differential_dataflow::trace::Description;

            use crate::KeyStorage;
            use crate::layout::ColumnarUpdate as Update;
            use crate::layout::ColumnarLayout as Layout;
            use crate::arrangement::Coltainer;

            use differential_dataflow::trace::implementations::OffsetList;
            fn vec_u64_to_offset_list(list: Vec<u64>) -> OffsetList {
                let mut output = OffsetList::with_capacity(list.len());
                output.push(0);
                for item in list { output.push(item as usize); }
                output
            }

            pub struct KeyMirror<U: Update> { marker: std::marker::PhantomData<U> }
            impl<U: Update<Val=()>> differential_dataflow::trace::Builder for KeyMirror<U> {
                type Time = U::Time;
                type Input = KeyStorage<U>;
                type Output = OrdKeyBatch<Layout<U>>;

                fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self { Self { marker: std::marker::PhantomData } }
                fn push(&mut self, _chunk: &mut Self::Input) { unimplemented!() }
                fn done(self, _description: Description<Self::Time>) -> Self::Output { unimplemented!() }
                fn seal(chain: &mut Vec<Self::Input>, description: Description<Self::Time>) -> Self::Output {
                    if chain.len() == 0 {
                        let storage = OrdKeyStorage {
                            keys: Default::default(),
                            upds: Default::default(),
                        };
                        OrdKeyBatch { storage, description, updates: 0, value: OrdKeyBatch::<Layout<U>>::create_value() }
                    }
                    else if chain.len() == 1 {
                        use columnar::Len;
                        let storage = chain.pop().unwrap();
                        let updates = storage.upds.len();
                        let storage = OrdKeyStorage {
                            keys: Coltainer { container: storage.keys },
                            upds: Upds {
                                offs: vec_u64_to_offset_list(storage.upds.bounds),
                                times: Coltainer { container: storage.upds.values.0 },
                                diffs: Coltainer { container: storage.upds.values.1 },
                            },
                        };
                        OrdKeyBatch { storage, description, updates, value: OrdKeyBatch::<Layout<U>>::create_value() }
                    }
                    else {
                        println!("chain length: {:?}", chain.len());
                        unimplemented!()
                    }
                }
            }
        }
    }
}
