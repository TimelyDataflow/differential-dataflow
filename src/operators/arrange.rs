//! Arranges a collection into a shareable trace structure.
//!
//! The `arrange` operator applies to a differential dataflow `Collection` and returns an `Arranged` 
//! structure, which maintains the collection's records in an indexed manner.
//!
//! Several operators (`join`, `group`, and `cogroup`, among others) are implemented against `Arranged`,
//! and can be applied directly to arranged data instead of the collection. Internally, the operators 
//! will borrow the shared state, and listen on the timely stream for shared batches of data. The 
//! resources to index the collection---communication, computation, and memory---are spent only once,
//! and only one copy of the index needs to be maintained as the collection changes.
//! 
//! The arranged collection is stored in a trace, whose append-only operation means that it is safe to 
//! share between the single writer and multiple readers. Each reader is expected to interrogate the 
//! trace only at times for which it knows the trace is complete, as indicated by the frontiers on its
//! incoming channels. Failing to do this is "safe" in the Rust sense, but the reader may see ill-defined
//! data at times for which the trace is not complete. (This being said, all current implementations 
//! commit only completed data to the trace).
//! 
//! Internally, the shared trace is wrapped in a `TraceWrapper` type which maintains information about 
//! the frontiers of all of its referees. Each referee has a `TraceHandle`, which acts as a reference 
//! counted pointer, and which mediates the advancement of frontiers. Ideally, a `TraceHandle` looks a
//! lot like a trace, though this isn't beatifully masked at the moment (it can't implement the trait
//! because we can't insert at it; it does implement `advance_by` and could implement `cursor`). 

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;

use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::progress::frontier::MutableAntichain;
use timely::dataflow::operators::Capability;

use timely_sort::Unsigned;

use hashable::{HashableWrapper, OrdWrapper};

use ::{Data, Ring, Collection, AsCollection, Hashable};
use lattice::Lattice;
use trace::{Trace, Batch, Batcher, Cursor};
// use trace::implementations::trie::Spine as OrdSpine;
// use trace::implementations::keys::Spine as KeyOrdSpine;
use trace::implementations::rhh::Spine as HashSpine;
use trace::implementations::rhh_k::Spine as KeyHashSpine;

/// Wrapper type to permit transfer of `Rc` types, as in batch.
///
/// The `BatchWrapper`s sole purpose in life is to implement `Abomonation` with methods that panic
/// when called. This allows the wrapped data to be transited along timely's `Pipeline` channels. 
/// The wrapper cannot fake out `Send`, and so cannot be used on timely's `Exchange` channels. 
#[derive(Clone,Ord,PartialOrd,Eq,PartialEq,Debug)]
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


/// A wrapper around a trace which tracks the frontiers of all referees.
pub struct TraceWrapper<K, V, T, R, Tr: Trace<K,V,T,R>> where T: Lattice+Ord+Clone+'static {
    phantom: ::std::marker::PhantomData<(K, V, R)>,
    frontiers: MutableAntichain<T>,
    /// The wrapped trace.
    pub trace: Tr,
}

impl<K,V,T,R,Tr: Trace<K,V,T,R>> TraceWrapper<K,V,T,R,Tr> where T: Lattice+Ord+Clone+'static {
    /// Allocates a new trace wrapper.
    fn new(empty: Tr) -> Self {
        TraceWrapper {
            phantom: ::std::marker::PhantomData,
            frontiers: MutableAntichain::new(),
            trace: empty,
        }
    }
    /// Reports the current frontier of the trace.
    fn _frontier(&self) -> &[T] { self.frontiers.elements() }
    /// Replaces elements of `lower` with those of `upper`.
    fn adjust_frontier(&mut self, lower: &[T], upper: &[T]) {
        for element in upper { self.frontiers.update_and(element, 1, |_,_| {}); }
        for element in lower { self.frontiers.update_and(element, -1, |_,_| {}); }
        self.trace.advance_by(self.frontiers.elements());
    }
}

/// A handle to a shared trace which maintains its own frontier information.
///
/// As long as the handle exists, the wrapped trace should continue to exist and will not advance its 
/// timestamps past the frontier maintained by the handle.
pub struct TraceHandle<K,V,T,R,Tr: Trace<K,V,T,R>> where T: Lattice+Ord+Clone+'static {
    frontier: Vec<T>,
    /// Wrapped trace. Please be gentle when using.
    pub wrapper: Rc<RefCell<TraceWrapper<K,V,T,R,Tr>>>,
}

impl<K,V,T,R,Tr: Trace<K,V,T,R>> TraceHandle<K,V,T,R,Tr> where T: Lattice+Ord+Clone+'static {
    /// Allocates a new handle from an existing wrapped wrapper.
    pub fn new(trace: Tr, frontier: &[T]) -> Self {

        let mut wrapper = TraceWrapper::new(trace);
        wrapper.adjust_frontier(&[], frontier);

        TraceHandle {
            frontier: frontier.to_vec(),
            wrapper: Rc::new(RefCell::new(wrapper)),
        }
    }
    /// Sets frontier to now be elements in `frontier`.
    ///
    /// This change may not have immediately observable effects. It informs the shared trace that this 
    /// handle no longer requires access to times other than those in the future of `frontier`, but if
    /// there are other handles to the same trace, it may not yet be able to compact.
    pub fn advance_by(&mut self, frontier: &[T]) {
        self.wrapper.borrow_mut().adjust_frontier(&self.frontier[..], frontier);
        self.frontier = frontier.to_vec();
    }
    /// Creates a new cursor over the wrapped trace.
    pub fn cursor(&self) -> Tr::Cursor {
        ::std::cell::RefCell::borrow(&self.wrapper).trace.cursor()
    }
}

impl<K, V, T: Lattice+Ord+Clone, R, Tr: Trace<K, V, T, R>> Clone for TraceHandle<K, V, T, R, Tr> {
    fn clone(&self) -> Self {
        // increase ref counts for this frontier
        self.wrapper.borrow_mut().adjust_frontier(&[], &self.frontier[..]);
        TraceHandle {
            frontier: self.frontier.clone(),
            wrapper: self.wrapper.clone(),
        }
    }
}

impl<K, V, T, R, Tr: Trace<K, V, T, R>> Drop for TraceHandle<K, V, T, R, Tr> 
    where T: Lattice+Ord+Clone+'static {
    fn drop(&mut self) {
        self.wrapper.borrow_mut().adjust_frontier(&self.frontier[..], &[]);
        self.frontier = Vec::new();
    }
}

/// A collection of `(K,V)` values as a timely stream and shared trace.
///
/// An `Arranged` performs the task of arranging a keyed collection once, 
/// allowing multiple differential operators to use the same trace. This 
/// saves on computation and memory, in exchange for some cognitive overhead
/// in writing differential operators: each must pay enough care to signals
/// from the `stream` field to know the subset of `trace` it has logically 
/// received.
pub struct Arranged<G: Scope, K, V, R, T: Trace<K, V, G::Timestamp, R>> where G::Timestamp: Lattice+Ord {
    /// A stream containing arranged updates.
    ///
    /// This stream contains the same batches of updates the trace itself accepts, so there should
    /// be no additional overhead to receiving these records. The batches can be navigated just as
    /// the batches in the trace, by key and by value.
    pub stream: Stream<G, BatchWrapper<T::Batch>>,
    /// A shared trace, updated by the `Arrange` operator and readable by others.
    pub trace: TraceHandle<K, V, G::Timestamp, R, T>,
    // TODO : We might have an `Option<Collection<G, (K, V)>>` here, which `as_collection` sets and
    // returns when invoked, so as to not duplicate work with multiple calls to `as_collection`.
}

impl<G: Scope, K, V, R, T: Trace<K, V, G::Timestamp, R>> Arranged<G, K, V, R, T> where G::Timestamp: Lattice+Ord {
    
    /// Allocates a new handle to the shared trace, with independent frontier tracking.
    pub fn new_handle(&self) -> TraceHandle<K, V, G::Timestamp, R, T> {
        self.trace.clone()
    }

    /// Flattens the stream into a `Collection`.
    ///
    /// The underlying `Stream<G, BatchWrapper<T::Batch>>` is a much more efficient way to access the data,
    /// and this method should only be used when the data need to be transformed or exchanged, rather than
    /// supplied as arguments to an operator using the same key-value structure.
    pub fn as_collection<D: Data, L>(&self, logic: L) -> Collection<G, D, R>
        where
            R: Ring,
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
pub trait Arrange<G: Scope, K, V, R: Ring> where G::Timestamp: Lattice+Ord {
    /// Arranges a stream of `(Key, Val)` updates by `Key`. Accepts an empty instance of the trace type.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange<T, K2:'static, V2:'static, L>(&self, map: L, empty: T) -> Arranged<G, K2, V2, R, T> 
        where 
            T: Trace<K2, V2, G::Timestamp, R>+'static,
            L: Fn(K,V)->(K2,V2)+'static;
}

impl<G: Scope, K: Data+Hashable, V: Data, R: Ring> Arrange<G, K, V, R> for Collection<G, (K, V), R> where G::Timestamp: Lattice+Ord {

    fn arrange<T, K2:'static, V2:'static, L>(&self, map: L, empty: T) -> Arranged<G, K2, V2, R, T> 
        where 
            T: Trace<K2, V2, G::Timestamp, R>+'static,
            L: Fn(K,V)->(K2,V2)+'static {

        // create a trace to share with downstream consumers.
        let handle = TraceHandle::new(empty, &[Default::default()]);
        let source = Rc::downgrade(&handle.wrapper);

        // Where we will deposit received updates, and from which we extract batches.
        let mut batcher = <T::Batch as Batch<K2,V2,G::Timestamp,R>>::Batcher::new();

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
                capabilities.retain(|c| !c.time().gt(&cap.time()));
                if !capabilities.iter().any(|c| c.time().le(&cap.time())) { 
                    capabilities.push(cap);
                }

                // add the updates to our batcher.
                for ((key, val), time, diff) in data.drain(..) {
                    let (key, val) = map(key, val);
                    batcher.push((key, val, time, diff));
                }
            });

            // Because we can only use one capability per message, on each notification we will need
            // to pull out the updates greater or equal to the notified time, and less than the current
            // frontier. We may have to do this a few times with different notified times, creating 
            // messages which could have been bundled into one, but that is how it is for the moment.

            // Whenever there is a gap between our capabilities and the input frontier, we can (and should)
            // extract the corresponding updates and produce a batch. Note, there may not actually be updates,
            // due to cancelation, but we should discover this and advance the batcher's frontier nonetheless.

            // We plan to advance our capabilities to match or exceed the input frontier. This means that 
            // some updates must be transmitted. For each capability in turn, we extract the batch that 
            // corresponds to the time interval to our capabilities just before and just after retiring 
            // the capability. At the end of the process, all of our capabilities should be in line with 
            // the input frontier, but we can (and must) advance them to track the lower envelope of the 
            // updates in the batcher.

            // We now swing through our capabilities, and determine which updates must be transmitted when
            // we downgrade each capability to one matching our input frontier. If at any point we find a 
            // non-empty batch whose lower envelope does not match `trace_upper`, we must add an empty 
            // batch to the trace to ensure contiguity.

            let mut sent_any = false;
            for index in 0 .. capabilities.len() {

                if !notificator.frontier(0).iter().any(|t| t == &capabilities[index].time()) {

                    sent_any = true;

                    // Assemble the upper frontier, from subsequent capabilities and the input frontier.
                    // TODO: this is cubic, I think, and could be quadratic if we went in the reverse direction.
                    // We would want to insert the batches in *this* order, though. Perhaps with clever sorting
                    // and such we could get this to linear-ish (plus sorting). Not clear how expensive this 
                    // will be until we understand how large frontiers end up being (could be unboundedly large).
                    let mut upper = Vec::new();
                    for after in (index + 1) .. capabilities.len() {
                        upper.push(capabilities[after].time());
                    }
                    for time in notificator.frontier(0) {
                        if !upper.iter().any(|t| t.le(time)) {
                            upper.push(time.clone());
                        }
                    }

                    // extract updates between `capabilities[index].time()` and `upper`.
                    let batch = batcher.seal(&upper[..]);

                    // If the source is still active, commit the extracted batch.
                    source.upgrade().map(|trace| {
                        let trace: &mut T = &mut trace.borrow_mut().trace;
                        trace.insert(batch.clone())
                    });

                    // send the batch to downstream consumers, empty or not.
                    output.session(&capabilities[index]).give(BatchWrapper { item: batch });
                }
            }

            // The trace should now contain batches with intervals up to the input frontier. Test?

            // Having extracted and sent batches between each capability and the input frontier,
            // we should advance all capabilities to match the batcher's lower update frontier.
            // This may involve discarding capabilties, which is fine as any new updates arrive 
            // in messages with new capabilities.

            // TODO: Perhaps this could be optimized when not much changes?
            if sent_any {
                let mut new_capabilities = Vec::new();
                for time in batcher.frontier() {
                    if let Some(capability) = capabilities.iter().find(|c| c.time().le(time)) {
                        new_capabilities.push(capability.delayed(time));
                    }
                }
                capabilities = new_capabilities;
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
pub trait ArrangeByKey<G: Scope, K: Data+Default+Hashable, V: Data, R: Ring> 
where G::Timestamp: Lattice+Ord {
    /// Arranges a collection of `(Key, Val)` records by `Key`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange_by_key_hashed(&self) -> Arranged<G, OrdWrapper<K>, V, R, HashSpine<OrdWrapper<K>, V, G::Timestamp, R>>;
    /// Arranges a collection of `(Key, Val)` records by `Key`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange_by_key_hashed_cached(&self) -> Arranged<G, HashableWrapper<K>, V, R, HashSpine<HashableWrapper<K>, V, G::Timestamp, R>> where <K as Hashable>::Output: Default;
}

impl<G: Scope, K: Data+Default+Hashable, V: Data, R: Ring> ArrangeByKey<G, K, V, R> for Collection<G, (K,V), R>
where G::Timestamp: Lattice+Ord {        
    fn arrange_by_key_hashed(&self) -> Arranged<G, OrdWrapper<K>, V, R, HashSpine<OrdWrapper<K>, V, G::Timestamp, R>> {
        self.arrange(|k,v| (OrdWrapper {item:k},v), HashSpine::new(Default::default()))
    }
    fn arrange_by_key_hashed_cached(&self) -> Arranged<G, HashableWrapper<K>, V, R, HashSpine<HashableWrapper<K>, V, G::Timestamp, R>> where <K as Hashable>::Output: Default {
        self.arrange(|k,v| (HashableWrapper::from(k),v), HashSpine::new(Default::default()))
    }
}

/// Arranges something as `(Key, ())` pairs according to a type `T` of trace.
///
/// This arrangement requires `Key: Hashable`, and uses the `hashed()` method to place keys in a hashed
/// map. This can result in many hash calls, and in some cases it may help to first transform `K` to the
/// pair `(u64, K)` of hash value and key.
pub trait ArrangeBySelf<G: Scope, K: Data+Default+Hashable, R: Ring> 
where G::Timestamp: Lattice+Ord {
    /// Arranges a collection of `Key` records by `Key`.
    ///
    /// This operator arranges a collection of records into a shared trace, whose contents it maintains.
    /// This trace is current for all times complete in the output stream, which can be used to safely
    /// identify the stable times and values in the trace.
    fn arrange_by_self(&self) -> Arranged<G, OrdWrapper<K>, (), R, KeyHashSpine<OrdWrapper<K>, G::Timestamp, R>>;
}


impl<G: Scope, K: Data+Default+Hashable, R: Ring> ArrangeBySelf<G, K, R> for Collection<G, K, R>
where G::Timestamp: Lattice+Ord {
    fn arrange_by_self(&self) -> Arranged<G, OrdWrapper<K>, (), R, KeyHashSpine<OrdWrapper<K>, G::Timestamp, R>> {
        self.map(|k| (k,()))
            .arrange(|k,v| (OrdWrapper {item:k}, v), KeyHashSpine::new(Default::default()))
    }
}
