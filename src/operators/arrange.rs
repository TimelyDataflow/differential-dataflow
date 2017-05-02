//! Arranges a collection into a re-usable trace structure.
//!
//! The `arrange` operator applies to a differential dataflow `Collection` and returns an `Arranged` 
//! structure, provides access to both an indexed form of accepted updates as well as a stream of 
//! batches of newly arranged updates.
//!
//! Several operators (`join`, `group`, and `cogroup`, among others) are implemented against `Arranged`,
//! and can be applied directly to arranged data instead of the collection. Internally, the operators 
//! will borrow the shared state, and listen on the timely stream for shared batches of data. The 
//! resources to index the collection---communication, computation, and memory---are spent only once,
//! and only one copy of the index needs to be maintained as the collection changes.
//! 
//! The arranged collection is stored in a trace, whose append-only operation means that it is safe to 
//! share between the single `arrange` writer and multiple readers. Each reader is expected to interrogate 
//! the trace only at times for which it knows the trace is complete, as indicated by the frontiers on its
//! incoming channels. Failing to do this is "safe" in the Rust sense of memory safety, but the reader may
//! see ill-defined data at times for which the trace is not complete. (All current implementations 
//! commit only completed data to the trace).

use std::rc::{Rc, Weak};
use std::cell::RefCell;
use std::default::Default;
use std::ops::DerefMut;
use std::collections::VecDeque;

use timely::dataflow::operators::{Enter, Map};
use timely::order::PartialOrder;
use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
// use timely::progress::nested::product::Product;
// use timely::progress::frontier::MutableAntichain;
use timely::progress::Timestamp;
use timely::dataflow::operators::Capability;
use timely::dataflow::scopes::Child;

use timely_sort::Unsigned;

use hashable::{HashOrdered, HashableWrapper, OrdWrapper};

use ::{Data, Diff, Collection, AsCollection, Hashable};
use lattice::Lattice;
use trace::{Trace, TraceReader, Batch, BatchReader, Batcher, Cursor};
// use trace::implementations::hash::HashValSpine as DefaultValTrace;
// use trace::implementations::hash::HashKeySpine as DefaultKeyTrace;
use trace::implementations::ord::OrdValSpine as DefaultValTrace;
use trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;

use trace::wrappers::enter::{TraceEnter, BatchEnter};
use trace::wrappers::rc::TraceRc;

/// Wrapper type to permit transfer of `Rc` types, as in batch.
///
/// The `BatchWrapper`s sole purpose in life is to implement `Abomonation` with methods that panic
/// when called. This allows the wrapped data to be transited along timely's `Pipeline` channels. 
/// The wrapper cannot fake out `Send`, and so cannot be used on timely's `Exchange` channels, which
/// is good.
#[derive(Clone,Eq,PartialEq,Debug)]
pub struct BatchWrapper<T> {
    /// The wrapped item.
    pub item: T,
}

// NOTE: This is all horrible. Don't look too hard.
impl<T> ::abomonation::Abomonation for BatchWrapper<T> {
   unsafe fn entomb(&self, _writer: &mut Vec<u8>) { panic!("BatchWrapper Abomonation impl") }
   unsafe fn embalm(&mut self) { panic!("BatchWrapper Abomonation impl") }
   unsafe fn exhume<'a,'b>(&'a mut self, _bytes: &'b mut [u8]) -> Option<&'b mut [u8]> { panic!("BatchWrapper Abomonation impl")  }
}

/// A `TraceReader` wrapper which tracks others interested in batches and progress.
pub struct TraceAgent<K, V, T, R, Tr> 
where T: Lattice+Clone+'static, Tr: TraceReader<K,V,T,R> {
    phantom: ::std::marker::PhantomData<(K, V, R)>,
    trace: Tr,
    queues: Rc<RefCell<Vec<Weak<RefCell<VecDeque<(Vec<T>, Option<(T, Tr::Batch)>)>>>>>>,
}

impl<K, V, T, R, Tr> TraceReader<K, V, T, R> for TraceAgent<K, V, T, R, Tr> 
where T: Lattice+Clone+'static, Tr: TraceReader<K,V,T,R> {
    type Batch = Tr::Batch;
    type Cursor = Tr::Cursor;
    fn advance_by(&mut self, frontier: &[T]) { self.trace.advance_by(frontier) }
    fn advance_frontier(&mut self) -> &[T] { &self.trace.advance_frontier() }
    fn distinguish_since(&mut self, frontier: &[T]) { self.trace.distinguish_since(frontier) }
    fn distinguish_frontier(&mut self) -> &[T] { &self.trace.distinguish_frontier() }
    fn cursor_through(&mut self, frontier: &[T]) -> Option<Tr::Cursor> { self.trace.cursor_through(frontier) }
    fn map_batches<F: FnMut(&Self::Batch)>(&mut self, f: F) { self.trace.map_batches(f) }
}

impl<K, V, T, R, Tr> TraceAgent<K, V, T, R, Tr> 
where T: Lattice+Clone+'static, Tr: TraceReader<K,V,T,R> {

    /// Creates a new agent from a trace reader.
    pub fn new(trace: Tr) -> Self {
        TraceAgent {
            phantom: ::std::marker::PhantomData,
            trace: trace,
            queues: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Inserts a batch in the trace, and signals each of the listeners.
    pub fn insert_at(&mut self, frontier: &[T], sent: Option<(T, Tr::Batch)>) where Tr: Trace<K,V,T,R>, Tr::Batch: Batch<K,V,T,R> {

        let mut borrow = self.queues.borrow_mut();
        for queue in borrow.iter_mut() {
            queue.upgrade().map(|queue| {
                queue.borrow_mut().push_back((frontier.to_vec(), sent.clone()));
            });
        }
        borrow.retain(|w| w.upgrade().is_some());

        if let Some((_, batch)) = sent {
            self.trace.insert(batch);
        }
    }

    /// Attaches a new shared queue to the trace.
    ///
    /// The queue will be immediately populated with existing historical batches from the trace, and until the reference 
    /// is dropped the queue will receive new batches as produced by the source `arrange` operator.
    pub fn new_listener(&mut self) -> Rc<RefCell<VecDeque<(Vec<T>, Option<(T, <Tr as TraceReader<K,V,T,R>>::Batch)>)>>> where T: Default {

        // create a new queue for progress and batch information.
        let mut queue = VecDeque::new();

        // add the existing batches from the trace
        self.trace.map_batches(|batch| queue.push_back((vec![T::default()], Some((T::default(), batch.clone())))));

        // wraps the queue in a ref-counted ref cell and enqueue/return it.
        let reference = Rc::new(RefCell::new(queue));
        let mut borrow = self.queues.borrow_mut();
        borrow.push(Rc::downgrade(&reference));
        reference
    }
}

impl<K, V, T, R, Tr> TraceRc<K, V, T, R, TraceAgent<K, V, T, R, Tr>>
where T: Lattice+Clone+'static, Tr: TraceReader<K,V,T,R> {

    /// Copies an existing collection into the supplied scope.
    /// 
    /// This method creates an `Arranged` collection that should appear indistinguishable from applying `arrange` 
    /// directly to the source collection brought into the local scope. The only caveat is that the initial state 
    /// of the collection is its current state, and updates occur from this point forward. The historical changes
    /// the collection experienced in the past are accumulated, and the distinctions from the initial collection 
    /// are no longer evident.
    pub fn import<G: Scope<Timestamp=T>>(&self, scope: &G) -> Arranged<G, K, V, R, TraceAgent<K, V, T, R, Tr>> where T: Timestamp {

    // pub fn create_in<G: Scope<Timestamp=T>>(&mut self, scope: &G) -> Arranged<G, K, V, R, Tr> where T: Timestamp {
        
        let queue = self.wrapper.borrow_mut().trace.new_listener();

        let collection = ::timely::dataflow::operators::operator::source(scope, "ArrangedSource", move |capability| {
            
            // capabilities the source maintains.
            let mut capabilities = vec![capability];
            
            move |output| {

                let mut borrow = queue.borrow_mut();
                while let Some((frontier, sent)) = borrow.pop_front() {
                    // if data are associated, send em!
                    if let Some((time, batch)) = sent {
                        if let Some(cap) = capabilities.iter().find(|c| c.time().less_equal(&time)) {
                            let delayed = cap.delayed(&time);
                            output.session(&delayed).give(BatchWrapper { item: batch });
                        }
                        else {
                            panic!("failed to find capability for {:?} in {:?}", time, capabilities);
                        }
                    }

                    // advance capabilities to look like `frontier`.
                    let mut new_capabilities = Vec::new();
                    for time in frontier.iter() {
                        if let Some(cap) = capabilities.iter().find(|c| c.time().less_equal(&time)) {
                            new_capabilities.push(cap.delayed(&time));
                        }
                        else {
                            panic!("failed to find capability for {:?} in {:?}", time, capabilities);
                        }
                    }
                    capabilities = new_capabilities;
                }
            }
        });

        Arranged {
            stream: collection,
            trace: self.clone(),
        }
    }
}

/// An arranged collection of `(K,V)` values.
///
/// An `Arranged` allows multiple differential operators to share the resources (communication, 
/// computation, memory) required to produce and maintain an indexed representation of a collection.
pub struct Arranged<G: Scope, K, V, R, T> where G::Timestamp: Lattice, T: TraceReader<K, V, G::Timestamp, R> {
    /// A stream containing arranged updates.
    ///
    /// This stream contains the same batches of updates the trace itself accepts, so there should
    /// be no additional overhead to receiving these records. The batches can be navigated just as
    /// the batches in the trace, by key and by value.
    pub stream: Stream<G, BatchWrapper<T::Batch>>,
    /// A shared trace, updated by the `Arrange` operator and readable by others.
    pub trace: TraceRc<K, V, G::Timestamp, R, T>,
    // TODO : We might have an `Option<Collection<G, (K, V)>>` here, which `as_collection` sets and
    // returns when invoked, so as to not duplicate work with multiple calls to `as_collection`.
}

impl<G: Scope, K, V, R, T> Arranged<G, K, V, R, T> where G::Timestamp: Lattice, T: TraceReader<K, V, G::Timestamp, R> {
    
    // /// Allocates a new handle to the shared trace, with independent frontier tracking.
    // pub fn new_handle(&self) -> TraceHandle<K, V, G::Timestamp, R, T> {
    //     self.trace.clone()
    // }

    /// Brings an arranged collection into a nested scope.
    ///
    /// This method produces a proxy trace handle that uses the same backing data, but acts as if the timestamps
    /// have all been extended with an additional coordinate with the default value. The resulting collection does
    /// not vary with the new timestamp coordinate.
    pub fn enter<'a, TInner>(&self, child: &Child<'a, G, TInner>)
        -> Arranged<Child<'a, G, TInner>, K, V, R, TraceEnter<K, V, G::Timestamp, R, TraceRc<K, V, G::Timestamp, R, T>, TInner>>
        where 
            // OtherTraceWrapper<K, V, G::Timestamp, R, T, TInner>: Trace<K, V, Product<G::Timestamp, TInner>, R>,
            T::Batch: Clone, K: 'static, V: 'static, G::Timestamp: Clone+Default+'static, TInner: Lattice+Timestamp+Clone+Default+'static, R: 'static {

        // unimplemented!()

        Arranged {
            stream: self.stream.enter(child).map(|bw| BatchWrapper { item: BatchEnter::make_from(bw.item) }),
            trace: TraceRc::make_from(TraceEnter::make_from(self.trace.clone())).0,
        }
    }

    /// Flattens the stream into a `Collection`.
    ///
    /// The underlying `Stream<G, BatchWrapper<T::Batch>>` is a much more efficient way to access the data,
    /// and this method should only be used when the data need to be transformed or exchanged, rather than
    /// supplied as arguments to an operator using the same key-value structure.
    pub fn as_collection<D: Data, L>(&self, logic: L) -> Collection<G, D, R>
        where
            R: Diff,
            T::Batch: Clone+'static,
            K: Clone, V: Clone,
            L: Fn(&K, &V) -> D+'static,
    {
        self.stream.unary_stream(Pipeline, "AsCollection", move |input, output| {

            input.for_each(|time, data| {
                let mut session = output.session(&time);
                for wrapper in data.drain(..) {
                    let batch = wrapper.item;
                    let mut cursor = batch.cursor();
                    while cursor.key_valid() {
                        let key: K = cursor.key().clone();      // TODO: pass ref in map_times
                        while cursor.val_valid() {
                            let val: V = cursor.val().clone();  // TODO: pass ref in map_times
                            cursor.map_times(|time, diff| {
                                session.give((logic(&key, &val), time.clone(), diff.clone()));
                            });
                            cursor.step_val();
                        }
                        cursor.step_key();
                    }
                }
            });
        })
        .as_collection()
    }
}

/// Arranges something as `(Key,Val)` pairs according to a type `T` of trace.
pub trait Arrange<G: Scope, K, V, R: Diff> where G::Timestamp: Lattice {
    /// Arranges a stream of `(Key, Val)` updates by `Key`. Accepts an empty instance of the trace type.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn arrange<T>(&self, empty_trace: T) -> Arranged<G, K, V, R, TraceAgent<K, V, G::Timestamp, R, T>> 
        where 
            T: Trace<K, V, G::Timestamp, R>+'static,
            T::Batch: Batch<K, V, G::Timestamp, R>;
}

impl<G: Scope, K: Data+HashOrdered, V: Data, R: Diff> Arrange<G, K, V, R> for Collection<G, (K, V), R> where G::Timestamp: Lattice+Ord {

    fn arrange<T>(&self, empty_trace: T) -> Arranged<G, K, V, R, TraceAgent<K, V, G::Timestamp, R, T>> 
        where 
            T: Trace<K, V, G::Timestamp, R>+'static,
            T::Batch: Batch<K, V, G::Timestamp, R> {

        let agent = TraceAgent::new(empty_trace);
        let queues = Rc::downgrade(&agent.queues);

        let (handle, source) = TraceRc::make_from(agent);
        let source = Rc::downgrade(&source);

        // // create a trace to share with downstream consumers.
        // let handle = TraceHandle::new(empty_trace, &[<G::Timestamp as Lattice>::min()], &[<G::Timestamp as Lattice>::min()]);

        // // acquire local downgraded copies of the references. 
        // // downgrading means that these instances will not keep the targets alive, especially important for the trace.
        // let source = Rc::downgrade(&handle.wrapper);
        // let queues = Rc::downgrade(&handle.queues);

        // Where we will deposit received updates, and from which we extract batches.
        let mut batcher = <T::Batch as Batch<K,V,G::Timestamp,R>>::Batcher::new();

        // Capabilities for the lower envelope of updates in `batcher`.
        let mut capabilities = Vec::<Capability<G::Timestamp>>::new();

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        let exchange = Exchange::new(move |update: &((K,V),G::Timestamp,R)| (update.0).0.hashed().as_u64());
        let stream = self.inner.unary_notify(exchange, "Arrange", vec![], move |input, output, notificator| {

            // As we receive data, we need to (i) stash the data and (ii) keep *enough* capabilities.
            // We don't have to keep all capabilities, but we need to be able to form output messages
            // when we realize that time intervals are complete.

            input.for_each(|cap, data| {

                // add the capability to our list of capabilities.
                capabilities.retain(|c| !cap.time().less_than(&c.time()));
                if !capabilities.iter().any(|c| c.time().less_equal(&cap.time())) { 
                    capabilities.push(cap);
                }

                batcher.push_batch(data.deref_mut());
            });

            // Timely dataflow currently only allows one capability per message, and we may have multiple
            // incomparable times for which we need to send data. This would normally require shattering
            // all updates we might send into multiple batches, each associated with a capability. 
            //
            // Instead! We can cheat a bit. We can extract one batch, and just make sure to send all of 
            // capabilities along in separate messages. This is a bit dubious, and we will want to make 
            // sure that each operator that consumes batches (group, join, as_collection) understands this.
            // 
            // At the moment this is painful for non-group operators, who each rely on having the correct 
            // capabilities at hand, and must find the right capability record-by-record otherwise. But, 
            // something like this should ease some pain. (we could also just fix timely).

            // If there is at least one capability no longer in advance of the input frontier ...
            if capabilities.iter().any(|c| !notificator.frontier(0).iter().any(|t| t.less_equal(&c.time()))) {

                // For each capability not in advance of the input frontier ... 
                for index in 0 .. capabilities.len() {
                    if !notificator.frontier(0).iter().any(|t| t.less_equal(&capabilities[index].time())) {

                        // Assemble the upper bound on times we can commit with this capabilities.
                        // This is determined both by the input frontier, and by subsequent capabilities
                        // which may shadow this capability for some times.
                        let mut upper = notificator.frontier(0).to_vec();
                        for capability in &capabilities[(index + 1) .. ] {
                            let time = capability.time().clone();
                            if !upper.iter().any(|t| t.less_equal(&time)) {
                                upper.retain(|t| !time.less_equal(t));
                                upper.push(time);
                            }
                        }

                        // Extract updates not in advance of `upper`.
                        let batch = batcher.seal(&upper[..]);

                        // If the source is still active, commit the extracted batch.
                        // The source may become inactive if all downsteam users of the trace drop their references.
                        source.upgrade().map(|trace| {
                            trace.borrow_mut().trace.trace.insert(batch.clone());
                        });

                        // If we still have listeners, send each a copy of the input frontier and current batch.
                        // NOTE: We could have used agent.insert_at, but .. source is discarded when handles to 
                        //       the trace drop, though there could still be consumers of the streams (e.g. the 
                        //       as_collection operator, which doesn't hold the trace).
                        queues.upgrade().map(|queues| {
                            let mut borrow = queues.borrow_mut();
                            for queue in borrow.iter_mut() {
                                queue.upgrade().map(|queue| {
                                    queue.borrow_mut().push_back((upper.clone(), Some((capabilities[index].time().clone(), batch.clone()))));
                                });
                            }
                            borrow.retain(|w| w.upgrade().is_some());
                        });

                        // send the batch to downstream consumers, empty or not.
                        output.session(&capabilities[index]).give(BatchWrapper { item: batch });
                    }
                }

                // Having extracted and sent batches between each capability and the input frontier,
                // we should downgrade all capabilities to match the batcher's lower update frontier.
                // This may involve discarding capabilities, which is fine as any new updates arrive 
                // in messages with new capabilities.

                let mut new_capabilities = Vec::new();
                for time in batcher.frontier() {
                    if let Some(capability) = capabilities.iter().find(|c| c.time().less_equal(time)) {
                        new_capabilities.push(capability.delayed(time));
                    }
                }

                capabilities = new_capabilities;

                // This very aggressively pushes frontier information along. We may want to dial it back 
                // if we find that we are spamming folks.
                queues.upgrade().map(|queues| {
                    let mut borrow = queues.borrow_mut();
                    for queue in borrow.iter_mut() {
                        queue.upgrade().map(|queue| {
                            queue.borrow_mut().push_back((notificator.frontier(0).to_vec(), None));
                        });
                    }
                    borrow.retain(|w| w.upgrade().is_some());
                });

            }
        });

        Arranged { stream: stream, trace: handle }
    }
}

/// Arranges something as `(Key,Val)` pairs according to a type `T` of trace.
///
/// This arrangement requires `Key: Hashable`, and uses the `hashed()` method to place keys in a hashed
/// map. This can result in many hash calls, and in some cases it may help to first transform `K` to the
/// pair `(u64, K)` of hash value and key.
pub trait ArrangeByKey<G: Scope, K: Data+Default+Hashable, V: Data, R: Diff> 
where G::Timestamp: Lattice+Ord {
    /// Arranges a collection of `(Key, Val)` records by `Key`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange_by_key_hashed(&self) -> Arranged<G, OrdWrapper<K>, V, R, TraceAgent<OrdWrapper<K>, V, G::Timestamp, R, DefaultValTrace<OrdWrapper<K>, V, G::Timestamp, R>>>;
    /// Arranges a collection of `(Key, Val)` records by `Key`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange_by_key_hashed_cached(&self) -> Arranged<G, HashableWrapper<K>, V, R, TraceAgent<HashableWrapper<K>, V, G::Timestamp, R, DefaultValTrace<HashableWrapper<K>, V, G::Timestamp, R>>>
    where <K as Hashable>::Output: Default+Data;
}

impl<G: Scope, K: Data+Default+Hashable, V: Data, R: Diff> ArrangeByKey<G, K, V, R> for Collection<G, (K,V), R>
where G::Timestamp: Lattice+Ord {        
    fn arrange_by_key_hashed(&self) -> Arranged<G, OrdWrapper<K>, V, R, TraceAgent<OrdWrapper<K>, V, G::Timestamp, R, DefaultValTrace<OrdWrapper<K>, V, G::Timestamp, R>>> {
        self.map(|(k,v)| (OrdWrapper {item:k},v))
            .arrange(DefaultValTrace::new())
    }
    fn arrange_by_key_hashed_cached(&self) -> Arranged<G, HashableWrapper<K>, V, R, TraceAgent<HashableWrapper<K>, V, G::Timestamp, R, DefaultValTrace<HashableWrapper<K>, V, G::Timestamp, R>>> 
    where <K as Hashable>::Output: Default+Data {
        self.map(|(k,v)| (HashableWrapper::from(k),v))
            .arrange(DefaultValTrace::new())
    }
}

/// Arranges something as `(Key, ())` pairs according to a type `T` of trace.
///
/// This arrangement requires `Key: Hashable`, and uses the `hashed()` method to place keys in a hashed
/// map. This can result in many hash calls, and in some cases it may help to first transform `K` to the
/// pair `(u64, K)` of hash value and key.
pub trait ArrangeBySelf<G: Scope, K: Data+Default+Hashable, R: Diff> 
where G::Timestamp: Lattice+Ord {
    /// Arranges a collection of `Key` records by `Key`.
    ///
    /// This operator arranges a collection of records into a shared trace, whose contents it maintains.
    /// This trace is current for all times complete in the output stream, which can be used to safely
    /// identify the stable times and values in the trace.
    fn arrange_by_self(&self) -> Arranged<G, OrdWrapper<K>, (), R, TraceAgent<OrdWrapper<K>, (), G::Timestamp, R, DefaultKeyTrace<OrdWrapper<K>, G::Timestamp, R>>>;
}


impl<G: Scope, K: Data+Default+Hashable, R: Diff> ArrangeBySelf<G, K, R> for Collection<G, K, R>
where G::Timestamp: Lattice+Ord {
    fn arrange_by_self(&self) -> Arranged<G, OrdWrapper<K>, (), R, TraceAgent<OrdWrapper<K>, (), G::Timestamp, R, DefaultKeyTrace<OrdWrapper<K>, G::Timestamp, R>>> {
        self.map(|k| (OrdWrapper {item:k}, ()))
            .arrange(DefaultKeyTrace::new())
    }
}
