//! The `arrange` operator and its variants arrange a collection into a shareable trace structure.
//!
//! The `arrange` operator is applied to a differential dataflow `Collection` and return an `Arranged`.
//! Several operators (`join`, `group`, and `cogroup`, among others) are implemented in terms of this
//! structure, and can be applied directly to arranged data. Internally, the operators will borrow the
//! shared state, and listen on the timely stream for shared batches of data. The resources to assemble
//! the collection, both computation and memory, are spent only once.
//! 
//! The arranged collection is stored in a trace, whose append-only behavior means that it is safe to 
//! share between the single writer and multiple readers. Each reader is expected to interrogate the 
//! trace only at times for which it knows the trace is complete, as indicated by the frontiers on its
//! incoming channels. Failing to do this is "safe", but may result in undefined semantics.
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
use timely::dataflow::channels::pact::Exchange;
use timely::progress::frontier::MutableAntichain;

use ::{Data, Collection};
use lattice::Lattice;
use trace::{Trace, Batch, Builder};

use trace::implementations::trie::Spine as TrieSpine;
use trace::implementations::keys::Spine as KeysSpine;

/// Wrapper type to permit transfer of `Rc` types, as in batch.
#[derive(Clone)]
pub struct BatchWrapper<T> {
    /// The wrapped item.
    pub item: T,
}

// NOTE: This is all horrible. Don't look too hard.
impl<T> ::abomonation::Abomonation for BatchWrapper<T> {
   unsafe fn entomb(&self, _writer: &mut Vec<u8>) { panic!() }
   unsafe fn embalm(&mut self) { panic!() }
   unsafe fn exhume<'a,'b>(&'a mut self, _bytes: &'b mut [u8]) -> Option<&'b mut [u8]> { panic!()  }
}

/// A wrapper around a trace which tracks the frontiers of all referees.
pub struct TraceWrapper<K: Ord, V: Ord, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, Tr: Trace<K, V, T>> {
    phantom: ::std::marker::PhantomData<(K, V)>,
    frontiers: MutableAntichain<T>,
    /// The wrapped trace.
    pub trace: Tr,
}

impl<K: Ord, V: Ord, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, Tr: Trace<K, V, T>> TraceWrapper<K, V, T, Tr> {
    /// Allocates a new trace wrapper.
    pub fn new(default: T) -> Self {
        TraceWrapper {
            phantom: ::std::marker::PhantomData,
            frontiers: MutableAntichain::new(),
            trace: Tr::new(default)
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
/// As long as the handle exists, it should protect the trace from advancing past the associated frontier.
/// This protection advances as the handle's frontier is advanced. When the handle is dropped the protection
/// is removed.
pub struct TraceHandle<K: Ord, V: Ord, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, Tr: Trace<K, V, T>> {
    phantom: ::std::marker::PhantomData<(K, V)>,
    frontier: Vec<T>,
    /// Wrapped trace. Please be gentle when using.
    pub wrapper: Rc<RefCell<TraceWrapper<K, V, T, Tr>>>,
}

impl<K: Ord, V: Ord, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, Tr: Trace<K, V, T>> TraceHandle<K, V, T, Tr> {
    /// Allocates a new handle from an existing wrapped wrapper.
    pub fn new(wrapper: &Rc<RefCell<TraceWrapper<K, V, T, Tr>>>) -> Self {
        let frontier = wrapper.borrow().frontier().to_vec();
        wrapper.borrow_mut().adjust_frontier(&[], &frontier[..]);
        TraceHandle {
            phantom: ::std::marker::PhantomData,
            frontier: frontier,
            wrapper: wrapper.clone(),
        }
    }
    /// Sets frontier to now be elements in `frontier`.
    pub fn advance_by(&mut self, frontier: &[T]) {
        self.wrapper.borrow_mut().adjust_frontier(&self.frontier[..], frontier);
        self.frontier = frontier.to_vec();
    }
    /// Creates a new cursor over the wrapped trace.
    pub fn cursor(&self) -> Tr::Cursor {
        self.wrapper.borrow().trace.cursor()
    }
}

impl<K: Ord, V: Ord, T: Lattice+Ord+Clone+::std::fmt::Debug+'static, Tr: Trace<K, V, T>> Drop for TraceHandle<K, V, T, Tr> {
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
pub struct Arranged<G: Scope, K: Data+Ord, V: Data+Ord, T: Trace<K, V, G::Timestamp>>
    where G::Timestamp: Lattice+Ord { 
    /// A stream containing arranged updates.
    ///
    /// This stream may be reduced to just the keys, with the expectation that looking up the
    /// updated values in the associated trace should be more efficient than cloning the values.
    /// Users who need the values can obviously use the original stream.
    pub stream: Stream<G, BatchWrapper<T::Batch>>,
    /// A shared trace, updated by the `Arrange` operator and readable by others.
    pub trace: Rc<RefCell<TraceWrapper<K, V, G::Timestamp, T>>>,
    /// If dereferenced, we build this collection.
    pub collection: Option<Collection<G, (K, V)>>,
}

impl<G: Scope, K: Data+Ord, V: Data+Ord, T: Trace<K, V, G::Timestamp>> Arranged<G, K, V, T> 
    where G::Timestamp: Lattice+Ord,
          T::Batch: Clone+'static {
    
    /// Allocates a new handle to the shared trace, with independent frontier tracking.
    pub fn new_handle(&self) -> TraceHandle<K, V, G::Timestamp, T> {
        TraceHandle::new(&self.trace)
    }

    /// Flattens the stream into a `Collection`.
    ///
    /// This operator is not obviously more efficient than using the `Collection` that is input
    /// to the `Arrange` operator. However, this may be the only way to recover a collection from 
    /// only the `Arranged` type.
    pub fn as_collection(&self) -> Collection<G, (K, V)> {
        unimplemented!()
    }
}

// impl<G: Scope, K: Data+Ord, V: Data+Ord, T: Trace<K, V, G::Timestamp>> Deref for Arranged<G, K, V, T> 
//     where G::Timestamp: Lattice+Ord,
//           T::Batch: Clone+'static {
//     type Target = Collection<G, (K, V)>;
//     fn deref(&self) -> &Self::Target {
//         if self.collection.is_none() {
//             self.collection = Some(self.as_collection());
//         }
//         self.collection.as_ref().unwrap()
//     }
// }

/// Arranges something as `(Key,Val)` pairs according to a type `T` of trace.
pub trait Arrange<G: Scope, K: Data+Ord, V: Data+Ord, T: Trace<K, V, G::Timestamp>> 
where G::Timestamp: Lattice+Ord,
      T::Batch: Clone+'static {
    /// Arranges a stream of `(Key, Val)` updates by `Key`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange(&self) -> Arranged<G, K, V, T>;
}

impl<G: Scope, K: Data+Ord, V: Data+Ord, T: Trace<K, V, G::Timestamp>+'static> Arrange<G, K, V, T> for Collection<G, (K, V)>
where G::Timestamp: Lattice+Ord,
      T::Batch: Clone+'static {
    fn arrange(&self) -> Arranged<G, K, V, T> {

        // create a trace to share with downstream consumers.
        let trace = Rc::new(RefCell::new(TraceWrapper::new(Default::default())));
        let source = Rc::downgrade(&trace);

        // A map from times to received (key, val, wgt) triples.
        let mut inputs = LinearMap::new();

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        let exchange = Exchange::new(|x: &((K,V),isize)| (x.0).0.hashed());
        let stream = self.inner.unary_notify(exchange, "ArrangeByKey", vec![], move |input, output, notificator| {

            input.for_each(|time, data| {
                inputs.entry(time.time())
                      .or_insert_with(|| { notificator.notify_at(time.clone()); <T::Batch as Batch<K,V,G::Timestamp>>::Builder::new() })
                      .extend(data.drain(..).map(|(kv,d)| (kv.0, kv.1, time.time(), d)));
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

        Arranged { stream: stream, trace: trace, collection: None }
    }
}

/// Arranges something as `(Key,Val)` pairs according to a type `T` of trace.
pub trait ArrangeByKey<G: Scope, K: Data+Ord, V: Data+Ord> 
where G::Timestamp: Lattice+Ord {
    /// Arranges a stream of `(Key, Val)` updates by `Key`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange_by_key(&self) -> Arranged<G, K, V, TrieSpine<K, V, G::Timestamp>>;
}

impl<G: Scope, K: Data+Ord, V: Data+Ord> ArrangeByKey<G, K, V> for Collection<G, (K,V)>
where G::Timestamp: Lattice+Ord  {
        
    fn arrange_by_key(&self) -> Arranged<G, K, V, TrieSpine<K, V, G::Timestamp>> {
        self.arrange()
    }
}

/// Arranges something as `(Key,Val)` pairs according to a type `T` of trace.
pub trait ArrangeBySelf<G: Scope, K: Data+Ord> 
where G::Timestamp: Lattice+Ord {
    /// Arranges a stream of `(Key, Val)` updates by `Key`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange_by_self(&self) -> Arranged<G, K, (), KeysSpine<K, G::Timestamp>>;
}


impl<G: Scope, K: Data+Ord> ArrangeBySelf<G, K> for Collection<G, K>
where G::Timestamp: Lattice+Ord {
    fn arrange_by_self(&self) -> Arranged<G, K, (), KeysSpine<K, G::Timestamp>> {
        self.map(|k| (k,()))
            .arrange()
    }
}
