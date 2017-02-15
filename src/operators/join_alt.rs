//! Match pairs of records based on a key.

use std::rc::Rc;
use std::default::Default;

use linear_map::LinearMap;

use timely::progress::{Timestamp, Antichain};
use timely::dataflow::Scope;
use timely::dataflow::operators::Binary;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Capability;

use ::{Data, Collection};
use lattice::Lattice;
use collection::Lookup;
use operators::group_alt::BatchCompact;
use trace::{Batch, Layer, Cursor, Trace, Spine, consolidate};

/// Matches the elements of two arranged traces.
///
/// This method is used by the various `join` implementations, but it can also be used 
/// directly in the event that one has a handle to an `Arranged<G,T>`, perhaps because
/// the arrangement is available for re-use, or from the output of a `group` operator.
pub trait JoinAlt<G: Scope, K: Data, V: Data> where G::Timestamp: Lattice+Ord {
    /// Joins two arranged collections with the same key type.
    ///
    /// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function, 
    /// producing a corresponding output record.
    ///
    /// This trait is implemented for arrangements (`Arranged<G, T>`) rather than collections. The `Join` trait 
    /// contains the implementations for collections.
    fn join_alt<V2,R,RF> (&self, stream2: &Collection<G,(K,V2)>, result: RF) -> Collection<G,R>
    where 
        V2: Data+Ord,
        R: Data,
        RF: Fn(&K,&V,&V2)->R+'static;
}

impl<TS: Timestamp, G: Scope<Timestamp=TS>, K: Data, V: Data> JoinAlt<G, K, V> for Collection<G, (K,V)> 
    where 
        G::Timestamp: Lattice+Ord {
    fn join_alt<V2,R,RF>(&self, other: &Collection<G,(K,V2)>, result: RF) -> Collection<G,R> 
    where 
        V2: Data,
        R: Data,
        RF: Fn(&K,&V,&V2)->R+'static {

        let mut trace1 = Some(Spine::new(Default::default()));
        let mut trace2 = Some(Spine::new(Default::default()));

        let mut inputs1 = LinearMap::new();
        let mut inputs2 = LinearMap::new();

        let mut capabilities = Vec::new();

        let exchange1 = Exchange::new(|&(ref k,_): &((K,V),i32) | k.hashed());
        let exchange2 = Exchange::new(|&(ref k,_): &((K,V2),i32)| k.hashed());

        let result = self.inner.binary_notify(&other.inner, exchange1, exchange2, "JoinAlt", vec![], move |input1, input2, output, notificator| {

            let frontier1 = notificator.frontier(0);
            let frontier2 = notificator.frontier(1);

            // shut down the trace if the frontier is empty and inputs are done. else, advance frontier.
            if trace2.is_some() && frontier1.len() == 0 && inputs1.len() == 0 { trace2 = None; }
            if let Some(ref mut trace) = trace2 {
                let mut antichain = Antichain::new();
                for time in frontier1 {
                    antichain.insert(time.clone());
                }
                for time in inputs1.keys() {
                    let time: &G::Timestamp = time;
                    let time: G::Timestamp = time.clone();
                    antichain.insert(time);
                }
                trace.advance_by(antichain.elements());
            }

            // shut down the trace if the frontier is empty and inputs are done. else, advance frontier.
            if trace1.is_some() && frontier2.len() == 0 && inputs2.len() == 0 { trace1 = None; }
            if let Some(ref mut trace) = trace1 {
                let mut antichain = Antichain::new();
                for time in frontier2 {
                    antichain.insert(time.clone());
                }
                for time in inputs2.keys() {
                    let time: &G::Timestamp = time;
                    let time: G::Timestamp = time.clone();
                    antichain.insert(time);
                }
                trace.advance_by(antichain.elements());
            }

            // read input 1, push all data to queues
            input1.for_each(|time, data| {
                inputs1.entry_or_insert(time.time(), || BatchCompact::new())
                       .extend(data.drain(..).map(|(keyval,diff)| (keyval, diff as isize)));

                if !capabilities.iter().any(|c: &Capability<G::Timestamp>| c.time() == time.time()) { capabilities.push(time); }
            });

            // read input 2, push all data to queues
            input2.for_each(|time, data| {
                inputs2.entry_or_insert(time.time(), || BatchCompact::new())
                       .extend(data.drain(..).map(|(keyval, diff)| (keyval, diff as isize)));

                if !capabilities.iter().any(|c| c.time() == time.time()) { capabilities.push(time); }
            });

            capabilities.sort_by(|x,y| x.time().cmp(&y.time()));

            let mut output_buffer = Vec::new();

            if let Some(position) = capabilities.iter().position(|c| !frontier1.iter().any(|t| t.le(&c.time())) &&
                                                                     !frontier2.iter().any(|t| t.le(&c.time())) ) {
                let capability = capabilities.remove(position);
                let time = capability.time();

                if let Some(buffer) = inputs1.remove_key(&time) {
                    let layer1 = Rc::new(Layer::new(buffer.done().into_iter().map(|((k,v),d)| (k,v,time.clone(),d)), &[], &[]));
                    trace2.as_ref().map(|trace2| cross_product(&layer1, trace2, &mut output_buffer, |k,v1,v2| result(k,v1,v2)));
                    trace1.as_mut().map(|trace1| trace1.insert(layer1));
                }

                if let Some(buffer) = inputs2.remove_key(&time) {
                    let layer2 = Rc::new(Layer::new(buffer.done().into_iter().map(|((k,v),d)| (k,v,time.clone(),d)), &[], &[]));
                    trace1.as_ref().map(|trace1| cross_product(&layer2, trace1, &mut output_buffer, |k,v2,v1| result(k,v1,v2)));
                    trace2.as_mut().map(|trace2| trace2.insert(layer2));
                }                

                consolidate(&mut output_buffer, 0);

                if output_buffer.len() > 0 {

                    // now sorted by times; count entries for each time to plan sessions.
                    let mut times = Vec::new();
                    times.push(((output_buffer[0].0).0, 1));
                    for index in 1 .. output_buffer.len() {
                        if (output_buffer[index].0).0 != (output_buffer[index-1].0).0 {
                            times.push(((output_buffer[index].0).0, 1));
                        }
                        else {
                            let len = times.len();
                            times[len-1].1 += 1;
                        }
                    }

                    let mut drain = output_buffer.drain(..);
                    for (time, count) in times.drain(..) {
                        let cap = capability.delayed(&time);
                        let mut session = output.session(&cap);
                        for ((_, r), d) in drain.by_ref().take(count) {
                            session.give((r, d as i32));
                        }
                    }
                }

                assert!(output_buffer.len() == 0);

            }
        });

        Collection::new(result)
    }
}

/// Forms the cross product of `layer` and `trace`, applying `result` and populating `buffer`.
fn cross_product<K, V1, V2, T, R, RF>(layer: &Rc<Layer<K,V1,T>>, trace: &Spine<K,V2,T>, buffer: &mut Vec<((T,R),isize)>, result: RF) 
where 
    K: Data,
    V1: Data,
    V2: Data,
    T: Lattice+Ord+::std::fmt::Debug+Clone,
    R: Data,
    RF: Fn(&K,&V1,&V2)->R,
{
    // construct cursors for the layer and the trace.
    let mut layer_cursor = layer.cursor();
    let mut trace_cursor = trace.cursor();

    while layer_cursor.key_valid() {
        let buffer_len = buffer.len(); 
        trace_cursor.seek_key(layer_cursor.key());
        if trace_cursor.key_valid() && trace_cursor.key() == layer_cursor.key() {
            while trace_cursor.val_valid() {
                while layer_cursor.val_valid() {
                    layer_cursor.map_times(|time2, diff2| {
                        trace_cursor.map_times(|time1, diff1| {
                            let r = result(layer_cursor.key(), layer_cursor.val(), trace_cursor.val());
                            buffer.push(((time1.join(time2), r), diff1 * diff2));
                        });                                            
                    });

                    layer_cursor.step_val();
                }

                layer_cursor.rewind_vals();
                trace_cursor.step_val();
            }
        }
        layer_cursor.step_key();
        consolidate(buffer, buffer_len);
    }
}
