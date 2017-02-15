//! The `arrange` operator is used internally by differential to arrange a collection of (key,val)
//! data by key, and present an Rc<Trace<..>> to other operators that need the data ArrangedByKey this way.
//!
//! The intent is that multiple users of the same data can share the resources needed to arrange and 
//! maintain the data. 

use std::rc::Rc;
use std::cell::RefCell;
use std::default::Default;
use std::ops::DerefMut;

use linear_map::LinearMap;

use timely::dataflow::*;
use timely::dataflow::operators::{Map, Unary};
use timely::dataflow::channels::pact::Exchange;

use ::{Data, Collection};
use lattice::Lattice;
use trace::Trace;
use trace::implementations::trie::{Layer, LayerBuilder, Spine};


/// A collection of `(K,V)` values as a timely stream and shared trace.
///
/// An `Arranged` performs the task of arranging a keyed collection once, 
/// allowing multiple differential operators to use the same trace. This 
/// saves on computation and memory, in exchange for some cognitive overhead
/// in writing differential operators: each must pay enough care to signals
/// from the `stream` field to know the subset of `trace` it has logically 
/// received.
pub struct ArrangedAlt<G: Scope, K: Data+Ord, V: Data+Ord, T: Trace<K, V, G::Timestamp>>
    where G::Timestamp: Lattice+Ord,
          T::Batch: ::timely::Data+Clone { 
    /// A stream containing arranged updates.
    ///
    /// This stream may be reduced to just the keys, with the expectation that looking up the
    /// updated values in the associated trace should be more efficient than cloning the values.
    /// Users who need the values can obviously use the original stream.
    pub stream: Stream<G, T::Batch>,
    /// A shared trace, updated by the `Arrange` operator and readable by others.
    pub trace: Rc<RefCell<T>>,
}

impl<G: Scope, K: Data+Ord, V: Data+Ord, T: Trace<K, V, G::Timestamp>> ArrangedAlt<G, K, V, T> 
    where G::Timestamp: Lattice+Ord,
          T::Batch: ::timely::Data+Clone {
    
    /// Flattens the stream into a `Collection`.
    ///
    /// This operator is not obviously more efficient than using the `Collection` that is input
    /// to the `Arrange` operator. However, this may be the only way to recover a collection from 
    /// only the `Arranged` type.
    pub fn as_collection(&self) -> Collection<G, (K, V)> {
        unimplemented!()
    }
}

/// Arranges something as `(Key,Val)` pairs. 
pub trait ArrangeByKey<G: Scope, K: Data+Ord, V: Data+Ord> where G::Timestamp: Lattice+Ord {
    /// Arranges a stream of `(Key, Val)` updates by `Key`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange_by_key(&self) -> ArrangedAlt<G, K, V, Spine<K,V,G::Timestamp>>;
}

impl<G: Scope, K: Data+Ord, V: Data+Ord> ArrangeByKey<G, K, V> for Collection<G, (K, V)> where G::Timestamp: Lattice+Ord {
    fn arrange_by_key(&self) -> ArrangedAlt<G, K, V, Spine<K,V,G::Timestamp>> {

        // create a trace to share with downstream consumers.
        let trace = Rc::new(RefCell::new(Spine::new(Default::default())));
        let source = Rc::downgrade(&trace);

        // A map from times to received (key, val, wgt) triples.
        let mut inputs = LinearMap::new();

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        let exchange = Exchange::new(|x: &((K,V),i32)| (x.0).0.hashed());
        let stream = self.inner.unary_notify(exchange, "ArrangeByKey", vec![], move |input, output, notificator| {

            input.for_each(|time, data| {
                inputs.entry_or_insert(time.time(), || { notificator.notify_at(time); LayerBuilder::new() })
                      .extend(data.drain(..));                
            });

            notificator.for_each(|index, _count, _notificator| {
                if let Some(mut builder) = inputs.remove_key(&index) {
                    let batch = builder.done(&[], &[]);
                    source.upgrade().map(|trace| trace.borrow_mut().insert(batch.clone()));
                    output.session(&index).give(batch);
                }
            });
        });

        ArrangedAlt { stream: stream, trace: trace }
    }
}
