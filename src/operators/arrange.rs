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

use linear_map::LinearMap;

use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::progress::frontier::MutableAntichain;

use timely_sort::Unsigned;

use hashable::OrdWrapper;

use ::{Data, Collection, AsCollection, Hashable};
use lattice::Lattice;
use trace::{Trace, Batch, Builder, Cursor};
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
   unsafe fn entomb(&self, _writer: &mut Vec<u8>) { panic!("BatchWrapper Abomonation impl reached") }
   unsafe fn embalm(&mut self) { panic!("BatchWrapper Abomonation impl reached") }
   unsafe fn exhume<'a,'b>(&'a mut self, _bytes: &'b mut [u8]) -> Option<&'b mut [u8]> { panic!("BatchWrapper Abomonation impl reached")  }
}


/// A wrapper around a trace which tracks the frontiers of all referees.
pub struct TraceWrapper<K, V, T, Tr: Trace<K,V,T>> where T: Lattice+Ord+Clone+'static {
    phantom: ::std::marker::PhantomData<(K, V)>,
    frontiers: MutableAntichain<T>,
    /// The wrapped trace.
    pub trace: Tr,
}

impl<K,V,T,Tr: Trace<K,V,T>> TraceWrapper<K,V,T,Tr> where T: Lattice+Ord+Clone+'static {
    /// Allocates a new trace wrapper.
    pub fn new(empty: Tr) -> Self {
        TraceWrapper {
            phantom: ::std::marker::PhantomData,
            frontiers: MutableAntichain::new(),
            trace: empty,
        }
    }
    /// Reports the current frontier of the trace.
    fn frontier(&self) -> &[T] { self.frontiers.elements() }
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
pub struct TraceHandle<K,V,T,Tr: Trace<K,V,T>> where T: Lattice+Ord+Clone+'static {
    frontier: Vec<T>,
    /// Wrapped trace. Please be gentle when using.
    pub wrapper: Rc<RefCell<TraceWrapper<K,V,T,Tr>>>,
}

impl<K,V,T,Tr: Trace<K,V,T>> TraceHandle<K,V,T,Tr> where T: Lattice+Ord+Clone+'static {
    /// Allocates a new handle from an existing wrapped wrapper.
    pub fn new(wrapper: &Rc<RefCell<TraceWrapper<K,V,T,Tr>>>) -> Self {
        let frontier = ::std::cell::RefCell::borrow(wrapper).frontier().to_vec();
        wrapper.borrow_mut().adjust_frontier(&[], &frontier[..]);
        TraceHandle {
            frontier: frontier,
            wrapper: wrapper.clone(),
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

impl<K, V, T, Tr: Trace<K, V, T>> Drop for TraceHandle<K, V, T, Tr> 
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
pub struct Arranged<G: Scope, K, V, T: Trace<K, V, G::Timestamp>> where G::Timestamp: Lattice+Ord {
    /// A stream containing arranged updates.
    ///
    /// This stream contains the same batches of updates the trace itself accepts, so there should
    /// be no additional overhead to receiving these records. The batches can be navigated just as
    /// the batches in the trace, by key and by value.
    pub stream: Stream<G, BatchWrapper<T::Batch>>,
    /// A shared trace, updated by the `Arrange` operator and readable by others.
    pub trace: Rc<RefCell<TraceWrapper<K, V, G::Timestamp, T>>>,
    // TODO : We might have an `Option<Collection<G, (K, V)>>` here, which `as_collection` sets and
    // returns when invoked, so as to not duplicate work with multiple calls to `as_collection`.
}

impl<G: Scope, K, V, T: Trace<K, V, G::Timestamp>> Arranged<G, K, V, T> where G::Timestamp: Lattice+Ord {
    
    /// Allocates a new handle to the shared trace, with independent frontier tracking.
    pub fn new_handle(&self) -> TraceHandle<K, V, G::Timestamp, T> {
        TraceHandle::new(&self.trace)
    }

    /// Flattens the stream into a `Collection`.
    ///
    /// The underlying `Stream<G, BatchWrapper<T::Batch>>` is a much more efficient way to access the data,
    /// and this method should only be used when the data need to be transformed or exchanged, rather than
    /// supplied as arguments to an operator using the same key-value structure.
    pub fn as_collection<D: Data, L>(&self, logic: L) -> Collection<G, D>
        where
            T::Batch: Clone+'static,
            K: Clone, V: Clone,
            L: Fn(&K, &V) -> D+'static,
    {
        self.stream.unary_stream(Pipeline, "AsCollection", move |input, output| {

            // TODO : This strongly assumes single time per batch, which *WILL* break in the future.
            input.for_each(|time, data| {
                let mut session = output.session(&time);
                for wrapper in data.drain(..) {
                    let batch = wrapper.item;
                    let mut cursor = batch.cursor();
                    while cursor.key_valid() {
                        let key: K = cursor.key().clone();      // TODO: pass ref in map_times
                        while cursor.val_valid() {
                            let val: V = cursor.val().clone();  // TODO: pass ref in map_times
                            cursor.map_times(|_time, diff| {
                                debug_assert!(_time == &time.time());
                                session.give((logic(&key, &val), diff));
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
pub trait Arrange<G: Scope, K, V> where G::Timestamp: Lattice+Ord {
    /// Arranges a stream of `(Key, Val)` updates by `Key`. Accepts an empty instance of the trace type.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange<T, K2:'static, V2:'static, L>(&self, map: L, empty: T) -> Arranged<G, K2, V2, T> 
        where 
            T: Trace<K2, V2, G::Timestamp>+'static,
            L: Fn(K,V)->(K2,V2)+'static;
}

impl<G: Scope, K: Data+Hashable, V: Data> Arrange<G, K, V> for Collection<G, (K, V)> where G::Timestamp: Lattice+Ord {

    fn arrange<T, K2:'static, V2:'static, L>(&self, map: L, empty: T) -> Arranged<G, K2, V2, T> 
        where 
            T: Trace<K2, V2, G::Timestamp>+'static,
            L: Fn(K,V)->(K2,V2)+'static {

        // create a trace to share with downstream consumers.
        let trace = Rc::new(RefCell::new(TraceWrapper::new(empty)));
        let source = Rc::downgrade(&trace);

        // A map from times to received (key, val, wgt) triples.
        let mut inputs = LinearMap::new();

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        let exchange = Exchange::new(move |update: &((K,V),isize)| (update.0).0.hashed().as_u64());
        let stream = self.inner.unary_notify(exchange, "ArrangeByKey", vec![], move |input, output, notificator| {

            input.for_each(|time, data| {
                inputs.entry(time.time())
                      .or_insert_with(|| { 
                        notificator.notify_at(time.clone()); 
                        <T::Batch as Batch<K2,V2,G::Timestamp>>::Builder::new() 
                      })
                      .extend(data.drain(..).map(|((key, val),diff)| {
                        let (key,val) = map(key, val);
                        (key, val, time.time(), diff) 
                      }));
            });

            notificator.for_each(|index, _count, _notificator| {
                if let Some(builder) = inputs.remove(&index) {
                    let batch = builder.done(&[], &[]);
                    source.upgrade().map(|trace| {
                        let trace: &mut T = &mut trace.borrow_mut().trace;
                        trace.insert(batch.clone())
                    });
                    output.session(&index).give(BatchWrapper { item: batch });
                }
            });
        });

        Arranged { stream: stream, trace: trace }
    }
}

/// Arranges something as `(Key,Val)` pairs according to a type `T` of trace.
///
/// This arrangement requires `Key: Hashable`, and uses the `hashed()` method to place keys in a hashed
/// map. This can result in many hash calls, and in some cases it may help to first transform `K` to the
/// pair `(u64, K)` of hash value and key.
pub trait ArrangeByKey<G: Scope, K: Data+Default+Hashable, V: Data> 
where G::Timestamp: Lattice+Ord {
    /// Arranges a collection of `(Key, Val)` records by `Key`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange_by_key(&self) -> Arranged<G, OrdWrapper<K>, V, HashSpine<OrdWrapper<K>, V, G::Timestamp>>;
}

impl<G: Scope, K: Data+Default+Hashable, V: Data> ArrangeByKey<G, K, V> for Collection<G, (K,V)>
where G::Timestamp: Lattice+Ord  {        
    fn arrange_by_key(&self) -> Arranged<G, OrdWrapper<K>, V, HashSpine<OrdWrapper<K>, V, G::Timestamp>> {
        self.arrange(|k,v| (OrdWrapper {item:k},v), HashSpine::new(Default::default()))
    }
}

/// Arranges something as `(Key, ())` pairs according to a type `T` of trace.
///
/// This arrangement requires `Key: Hashable`, and uses the `hashed()` method to place keys in a hashed
/// map. This can result in many hash calls, and in some cases it may help to first transform `K` to the
/// pair `(u64, K)` of hash value and key.
pub trait ArrangeBySelf<G: Scope, K: Data+Default+Hashable> 
where G::Timestamp: Lattice+Ord {
    /// Arranges a collection of `Key` records by `Key`.
    ///
    /// This operator arranges a collection of records into a shared trace, whose contents it maintains.
    /// This trace is current for all times complete in the output stream, which can be used to safely
    /// identify the stable times and values in the trace.
    fn arrange_by_self(&self) -> Arranged<G, OrdWrapper<K>, (), KeyHashSpine<OrdWrapper<K>, G::Timestamp>>;
}


impl<G: Scope, K: Data+Default+Hashable> ArrangeBySelf<G, K> for Collection<G, K>
where G::Timestamp: Lattice+Ord {
    fn arrange_by_self(&self) -> Arranged<G, OrdWrapper<K>, (), KeyHashSpine<OrdWrapper<K>, G::Timestamp>> {
        self.map(|k| (k,()))
            .arrange(|k,v| (OrdWrapper {item:k}, v), KeyHashSpine::new(Default::default()))
    }
}
