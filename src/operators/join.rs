//! Match pairs of records based on a key.

use std::rc::Rc;
use std::default::Default;
use std::collections::HashMap;

use linear_map::LinearMap;
use vec_map::VecMap;

use timely::progress::{Timestamp, Antichain};
use timely::dataflow::Scope;
use timely::dataflow::operators::Binary;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Capability;
use timely_sort::Unsigned;

use ::{Data, Collection};
use lattice::Lattice;
use collection::Lookup;
use collection::trace::{Trace, TraceReference};
use operators::arrange::{Arranged, ArrangeByKey, ArrangeBySelf};
use operators::group_alt::BatchCompact;
use trace::{Layer, Cursor, consolidate};
use trace::Trace as TraceAlt;
use trace::trace_trait::{Batch, TimeCursor, ValCursor, KeyCursor};

/// Join implementations for `(key,val)` data.
pub trait Join<G: Scope, K: Data, V: Data> {

    /// Matches pairs `(key,val1)` and `(key,val2)` based on `key`.
    ///
    /// The `join` method requires that the two collections both be over pairs of records, and the
    /// first element of the pair must be of the same type. Given two such collections, each pair
    /// of records `(key,val1)` and `(key,val2)` with a matching `key` produces a `(key, val1, val2)`
    /// output record.
    ///
    /// #Examples
    /// ```ignore
    /// extern crate timely;
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    /// use differential_dataflow::operators::Join;
    ///
    /// let data = timely::example(|scope| {
    ///     let col1 = vec![((0,0),1),((1,2),1)].into_iter().to_stream(scope);
    ///     let col2 = vec![((0,'a'),1),((1,'B'),1)].into_iter().to_stream(scope);
    ///
    ///     // should produce triples `(0,0,'a')` and `(1,2,'B')`.
    ///     col1.join(&col2).capture();
    /// });
    ///
    /// let extracted = data.extract();
    /// assert_eq!(extracted.len(), 1);
    /// assert_eq!(extracted[0].1, vec![((0,0,'a'),1), ((1,2,'B'),1)]);
    /// ```
    fn join<V2: Data>(&self, other: &Collection<G, (K,V2)>) -> Collection<G, (K,V,V2)> {
        self.join_map(other, |k,v1,v2| (k.clone(), v1.clone(), v2.clone()))
    }
    /// Matches pairs `(key,val1)` and `(key,val2)` based on `key` and then applies a function.
    ///
    /// #Examples
    /// ```ignore
    /// extern crate timely;
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    /// use differential_dataflow::operators::Join;
    ///
    /// let data = timely::example(|scope| {
    ///     let col1 = vec![((0,0),1),((1,2),1)].into_iter().to_stream(scope);
    ///     let col2 = vec![((0,'a'),1),((1,'B'),1)].into_iter().to_stream(scope);
    ///
    ///     // should produce records `(0 + 0,'a')` and `(1 + 2,'B')`.
    ///     col1.join_map(&col2, |k,v1,v2| (*k + *v1, *v2)).capture();
    /// });
    ///
    /// let extracted = data.extract();
    /// assert_eq!(extracted.len(), 1);
    /// assert_eq!(extracted[0].1, vec![((0,'a'),1), ((3,'B'),1)]);
    /// ```
    fn join_map<V2: Data, D: Data, R: Fn(&K, &V, &V2)->D+'static>(&self, other: &Collection<G, (K,V2)>, logic: R) -> Collection<G, D>;
    /// Matches pairs `(key,val1)` and `key` based on `key`, filtering the first collection by values present in the second.
    ///
    /// #Examples
    /// ```ignore
    /// extern crate timely;
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    /// use differential_dataflow::operators::Join;
    ///
    /// let data = timely::example(|scope| {
    ///     let col1 = vec![((0,0),1),((1,2),1)].into_iter().to_stream(scope);
    ///     let col2 = vec![(0,1)].into_iter().to_stream(scope);
    ///
    ///     // should retain record `(0,0)` and discard `(1,2)`.
    ///     col1.semijoin(&col2).capture();
    /// });
    ///
    /// let extracted = data.extract();
    /// assert_eq!(extracted.len(), 1);
    /// assert_eq!(extracted[0].1, vec![((0,0),1)]);
    /// ```
    fn semijoin(&self, other: &Collection<G, K>) -> Collection<G, (K, V)>;
    /// Matches pairs `(key,val1)` and `key` based on `key`, discarding values 
    /// in the first collection if their key is present in the second.
    ///
    /// #Examples
    /// ```ignore
    /// extern crate timely;
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    /// use differential_dataflow::operators::Join;
    ///
    /// let data = timely::example(|scope| {
    ///     let col1 = vec![((0,0),1),((1,2),1)].into_iter().to_stream(scope);
    ///     let col2 = vec![(0,1)].into_iter().to_stream(scope);
    ///
    ///     // should retain record `(0,0)` and discard `(1,2)`.
    ///     col1.semijoin(&col2).capture();
    /// });
    ///
    /// let extracted = data.extract();
    /// assert_eq!(extracted.len(), 1);
    /// assert_eq!(extracted[0].1, vec![((1,2),1)]);
    /// ```
    fn antijoin(&self, other: &Collection<G, K>) -> Collection<G, (K, V)>;

    /// Joins two collections with dense unsigned integer keys.
    ///
    /// #Examples
    /// ```ignore
    /// extern crate timely;
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    /// use differential_dataflow::operators::Join;
    ///
    /// let data = timely::example(|scope| {
    ///     let col1 = vec![((0,0),1),((1,2),1)].into_iter().to_stream(scope);
    ///     let col2 = vec![((0,'a'),1),((1,'B'),1)].into_iter().to_stream(scope);
    ///
    ///     // should produce triples `(0,0,'a')` and `(1,2,'B')`.
    ///     col1.join_u(&col2).capture();
    /// });
    ///
    /// let extracted = data.extract();
    /// assert_eq!(extracted.len(), 1);
    /// assert_eq!(extracted[0].1, vec![((0,0,'a'),1), ((1,2,'B'),1)]);
    /// ```
    fn join_u<V2: Data>(&self, other: &Collection<G, (K,V2)>) -> Collection<G, (K,V,V2)> where K: Unsigned+Default {
        self.join_map_u(other, |k,v1,v2| (k.clone(), v1.clone(), v2.clone()))
    }
    /// Joins two collections with dense unsigned integer keys and then applies a map function.
    ///
    /// #Examples
    /// ```ignore
    /// extern crate timely;
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    /// use differential_dataflow::operators::Join;
    ///
    /// let data = timely::example(|scope| {
    ///     let col1 = vec![((0,0),1),((1,2),1)].into_iter().to_stream(scope);
    ///     let col2 = vec![((0,'a'),1),((1,'B'),1)].into_iter().to_stream(scope);
    ///
    ///     // should produce records `(0 + 0,'a')` and `(1 + 2,'B')`.
    ///     col1.join_map_u(&col2, |k,v1,v2| (*k + *v1, *v2)).capture();
    /// });
    ///
    /// let extracted = data.extract();
    /// assert_eq!(extracted.len(), 1);
    /// assert_eq!(extracted[0].1, vec![((0,'a'),1), ((3,'B'),1)]);
    /// ```
    fn join_map_u<V2: Data, D: Data, R: Fn(&K, &V, &V2)->D+'static>(&self, other: &Collection<G, (K,V2)>, logic: R) -> Collection<G, D> where K: Unsigned+Default;
    /// Semijoins a collection with dense unsigned integer keys against a set of such keys.
    ///
    /// #Examples
    /// ```ignore
    /// extern crate timely;
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    /// use differential_dataflow::operators::Join;
    ///
    /// let data = timely::example(|scope| {
    ///     let col1 = vec![((0,0),1),((1,2),1)].into_iter().to_stream(scope);
    ///     let col2 = vec![(0,1)].into_iter().to_stream(scope);
    ///
    ///     // should retain record `(0,0)` and discard `(1,2)`.
    ///     col1.semijoin(&col2).capture();
    /// });
    ///
    /// let extracted = data.extract();
    /// assert_eq!(extracted.len(), 1);
    /// assert_eq!(extracted[0].1, vec![((0,0),1)]);
    /// ```
    fn semijoin_u(&self, other: &Collection<G, K>) -> Collection<G, (K, V)> where K: Unsigned+Default;
    /// Antijoins a collection with dense unsigned integer keys against a set of such keys.
    ///
    /// #Examples
    /// ```ignore
    /// extern crate timely;
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    /// use differential_dataflow::operators::Join;
    ///
    /// let data = timely::example(|scope| {
    ///     let col1 = vec![((0,0),1),((1,2),1)].into_iter().to_stream(scope);
    ///     let col2 = vec![(0,1)].into_iter().to_stream(scope);
    ///
    ///     // should retain record `(0,0)` and discard `(1,2)`.
    ///     col1.semijoin(&col2).capture();
    /// });
    ///
    /// let extracted = data.extract();
    /// assert_eq!(extracted.len(), 1);
    /// assert_eq!(extracted[0].1, vec![((0,0),1)]);
    /// ```
    fn antijoin_u(&self, other: &Collection<G, K>) -> Collection<G, (K, V)> where K: Unsigned+Default;
} 

impl<G: Scope, K: Data, V: Data> Join<G, K, V> for Collection<G, (K, V)> where G::Timestamp: Lattice {
    /// Matches pairs of `(key,val1)` and `(key,val2)` records based on `key` and applies a reduction function.
    fn join_map<V2: Data, D: Data, R>(&self, other: &Collection<G, (K, V2)>, logic: R) -> Collection<G, D>
    where R: Fn(&K, &V, &V2)->D+'static {
        let arranged1 = self.arrange_by_key(|k| k.hashed(), |_| HashMap::new());
        let arranged2 = other.arrange_by_key(|k| k.hashed(), |_| HashMap::new());
        arranged1.join(&arranged2, logic)
    }
    fn semijoin(&self, other: &Collection<G, K>) -> Collection<G, (K, V)> {
        let arranged1 = self.arrange_by_key(|k| k.hashed(), |_| HashMap::new());
        let arranged2 = other.arrange_by_self(|k| k.hashed(), |_| HashMap::new());
        arranged1.join(&arranged2, |k,v,_| (k.clone(), v.clone()))
    }
    fn antijoin(&self, other: &Collection<G, K>) -> Collection<G, (K, V)> {
        self.concat(&self.semijoin(other).negate())
    }

    fn join_map_u<V2: Data, D: Data, R: Fn(&K, &V, &V2)->D+'static>(&self, other: &Collection<G, (K,V2)>, logic: R) -> Collection<G, D> where K: Unsigned+Default {
        let arranged1 = self.arrange_by_key(|k| k.clone(), |x| (VecMap::new(), x));
        let arranged2 = other.arrange_by_key(|k| k.clone(), |x| (VecMap::new(), x));
        arranged1.join(&arranged2, logic)
    }
    fn semijoin_u(&self, other: &Collection<G, K>) -> Collection<G, (K, V)> where K: Unsigned+Default {
        let arranged1 = self.arrange_by_key(|k| k.clone(), |x| (VecMap::new(), x));
        let arranged2 = other.arrange_by_self(|k| k.clone(), |x| (VecMap::new(), x));
        arranged1.join(&arranged2, |k,v,_| (k.clone(), v.clone()))        
    }
    fn antijoin_u(&self, other: &Collection<G, K>) -> Collection<G, (K, V)> where K: Unsigned+Default {
        self.concat(&self.semijoin_u(other).negate())
    }
}

/// Matches the elements of two arranged traces.
///
/// This method is used by the various `join` implementations, but it can also be used 
/// directly in the event that one has a handle to an `Arranged<G,T>`, perhaps because
/// the arrangement is available for re-use, or from the output of a `group` operator.
pub trait JoinArranged<G: Scope, K: Data, V: Data> where G::Timestamp: Lattice {
    /// Joins two arranged collections with the same key type.
    ///
    /// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function, 
    /// producing a corresponding output record.
    ///
    /// This trait is implemented for arrangements (`Arranged<G, T>`) rather than collections. The `Join` trait 
    /// contains the implementations for collections.
    fn join<T2,R,RF> (&self, stream2: &Arranged<G,T2>, result: RF) -> Collection<G,R>
    where 
        T2: Trace<Key=K,Index=G::Timestamp>+'static,
        for<'a> T2: TraceReference<'a>+'static,
        R: Data,
        RF: Fn(&K,&V,&T2::Value)->R+'static;
}

impl<TS: Timestamp, G: Scope<Timestamp=TS>, T> JoinArranged<G, T::Key, T::Value> for Arranged<G, T> 
    where 
        T: Trace<Index=TS>+'static,
        for<'a> T: TraceReference<'a>+'static,
        G::Timestamp: Lattice {
    fn join<T2,R,RF>(&self, other: &Arranged<G,T2>, result: RF) -> Collection<G,R> 
    where 
        T2: Trace<Key=T::Key, Index=G::Timestamp>+'static,
        for<'a> T2: TraceReference<'a>+'static,
        R: Data,
        RF: Fn(&T::Key,&T::Value,&T2::Value)->R+'static {

        let mut trace1 = Some(self.trace.clone());
        let mut trace2 = Some(other.trace.clone());

        let mut inputs1 = LinearMap::new();
        let mut inputs2 = LinearMap::new();
        let mut outbuf = LinearMap::new();

        // upper envelope of notified times; 
        // used to restrict diffs processed.
        let mut acknowledged = Vec::new();

        let result = self.stream.binary_notify(&other.stream, Pipeline, Pipeline, "Join", vec![], move |input1, input2, output, notificator| {

            // shut down a trace if the opposing input has been closed out.
            // TODO : more generally, we would like to announce our frontier to each trace, so that it may
            // TODO : be compacted when the frontiers of all of its referees have advanced past some point.
            if trace2.is_some() && notificator.frontier(0).len() == 0 && inputs1.len() == 0 { trace2 = None; }
            if trace1.is_some() && notificator.frontier(1).len() == 0 && inputs2.len() == 0 { trace1 = None; }

            // read input 1, push all data to queues
            input1.for_each(|time, data| {
                assert!(data.len() == 1);
                inputs1.entry_or_insert(time.time(), || data.drain(..).next().unwrap());
                notificator.notify_at(time);
            });

            // read input 2, push all data to queues
            input2.for_each(|time, data| {
                assert!(data.len() == 1);
                inputs2.entry_or_insert(time.time(), || data.drain(..).next().unwrap());
                notificator.notify_at(time);
            });

            // Notification means we have inputs to process or outputs to send.
            notificator.for_each(|capability, _count, notificator| {

                let time = capability.time();

                // We must be careful to only respond to pairs of differences at `time` with
                // one output record, not two. To do this correctly, we acknowledge the time
                // after processing the first input, so that it is as if we added the inputs 
                // to the traces in this order.

                // TODO : Evaluate only looking up values by key rather than receiving them.

                // TODO : It is probably wise to compact the results from each key before sending.
                // TODO : At least, "changes" in join inputs often result in cancellations.

                // TODO : Consider delaying the production of output tuples until the time
                // TODO : has been notified; like `Group`. This would address the issues above
                // TODO : by looking up data only once it is present, ensuring only non-cancelled
                // TODO : records would be transmitted (also a good thing).

                // compare fresh data on the first input against stale data on the second
                if let Some(ref trace) = trace2 {
                    if let Some((keys, cnts, vals)) = inputs1.remove_key(&time) {
                        let mut vals = vals.iter();
                        for (key, &cnt) in keys.iter().zip(cnts.iter()) {
                            let borrow = trace.borrow();
                            for (t, diffs) in borrow.trace(key) {
                                if acknowledged.iter().any(|t2| t <= t2) {
                                    let mut output = outbuf.entry_or_insert(time.join(t), || Vec::new());
                                    for (ref val2, wgt2) in diffs {
                                        for &(ref val1, wgt1) in vals.clone().take(cnt as usize) {
                                            output.push((result(key, val1, val2), wgt1 * wgt2));
                                        }
                                    }
                                }
                            }
                            for _ in 0..cnt { vals.next(); }
                        }
                    }
                }

                // acknowledge the time, so we can use it below
                acknowledged.retain(|t| !(t <= &time));
                acknowledged.push(time.clone());

                // compare fresh data on the second input against fresh data on the first
                if let Some(ref trace) = trace1 {         
                    if let Some((keys, cnts, vals)) = inputs2.remove_key(&time) {
                        let mut vals = vals.iter();
                        for (key, &cnt) in keys.iter().zip(cnts.iter()) {
                            let borrow = trace.borrow();
                            for (t, diffs) in borrow.trace(key) {
                                if acknowledged.iter().any(|t2| t <= t2) {
                                    let mut output = outbuf.entry_or_insert(time.join(t), || Vec::new());
                                    for (ref val1, wgt1) in diffs {
                                        for &(ref val2, wgt2) in vals.clone().take(cnt as usize) {
                                            output.push((result(key, val1, val2), wgt1 * wgt2));
                                        }
                                    }
                                }
                            }
                            for _ in 0..cnt { vals.next(); }
                        }
                    }
                }

                // TODO : This only sends data at the current time.
                // TODO : this may be unwise, as the `join` may produce
                // TODO : more data than can be easily stored without 
                // TODO : aggregation. It may be that we should send everything
                // TODO : and let the receiver store the data as it sees fit.
                //
                // TODO : See note above about delaying evalutation of time 
                // TODO : until notification that each input has reach time.
                // TODO : Likely more expensive, but keeps memory footprint
                // TODO : proportional to input, rather than output sizes.
                if let Some(mut buffer) = outbuf.remove_key(&time) {
                    output.session(&capability).give_iterator(buffer.drain(..));
                }

                // make sure we hold capabilities for each time still to send at.
                for (new_time, _) in &outbuf {
                    // NOTE : WHOA THIS IS MESSED UP;
                    if capability.time().le(new_time) {
                        notificator.notify_at(capability.delayed(new_time));
                    }
                }
            });
        });

        Collection::new(result)
    }
}



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

        let mut trace1 = Some(TraceAlt::new(Default::default()));
        let mut trace2 = Some(TraceAlt::new(Default::default()));

        let mut inputs1 = LinearMap::new();
        let mut inputs2 = LinearMap::new();

        let mut capabilities = Vec::new();

        let exchange1 = Exchange::new(|&(ref k,_): &((K,V),i32) | k.hashed());
        let exchange2 = Exchange::new(|&(ref k,_): &((K,V2),i32)| k.hashed());

        let result = self.inner.binary_notify(&other.inner, exchange1, exchange2, "Join", vec![], move |input1, input2, output, notificator| {

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
                trace.advance_frontier(antichain.elements());
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
                trace.advance_frontier(antichain.elements());
            }

            // read input 1, push all data to queues
            input1.for_each(|time, data| {
                let buffer = inputs1.entry_or_insert(time.time(), || BatchCompact::new());
                for ((key,val),diff) in data.drain(..) {
                    buffer.push(((key,val), diff as isize));
                }

                if !capabilities.iter().any(|c: &Capability<G::Timestamp>| c.time() == time.time()) { capabilities.push(time); }
            });

            // read input 2, push all data to queues
            input2.for_each(|time, data| {
                let buffer = inputs2.entry_or_insert(time.time(), || BatchCompact::new());
                for ((key,val),diff) in data.drain(..) {
                    buffer.push(((key,val), diff as isize));
                }

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
                    if let Some(ref trace2) = trace2 {
                        cross_product(&layer1, trace2, &mut output_buffer, |k,v1,v2| result(k, v1, v2));
                    }
                    if let Some(ref mut trace) = trace1 {
                        trace.insert(layer1);
                    }
                }

                if let Some(buffer) = inputs2.remove_key(&time) {
                    let layer2 = Rc::new(Layer::new(buffer.done().into_iter().map(|((k,v),d)| (k,v,time.clone(),d)), &[], &[]));
                    if let Some(ref trace1) = trace1 {
                        cross_product(&layer2, trace1, &mut output_buffer, |k,v2,v1| result(k,v1,v2));
                    }
                    if let Some(ref mut trace) = trace2 {
                        trace.insert(layer2);
                    }
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
fn cross_product<K, V1, V2, T, R, RF>(layer: &Rc<Layer<K,V1,T>>, trace: &TraceAlt<K,V2,T>, buffer: &mut Vec<((T,R),isize)>, result: RF) 
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
        if let Some(mut key_view) = trace_cursor.seek_key(layer_cursor.key()) {
            while let Some(val_view) = key_view.next_val() {
                layer_cursor.rewind_vals();
                while layer_cursor.val_valid() {
                    layer_cursor.map_times(|time2, diff2| {
                        val_view.map(|time1, diff1| {
                            let r = result(layer_cursor.key(), layer_cursor.val(), val_view.val());
                            buffer.push(((time1.join(time2), r), diff1 * diff2));
                        });                                            
                    });

                    layer_cursor.step_val();
                }
            }
        }
        layer_cursor.step_key();
        consolidate(buffer, buffer_len);
    }
}
