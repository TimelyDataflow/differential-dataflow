//! Match pairs of records based on a key.

use linear_map::LinearMap;

use timely::progress::{Timestamp, Antichain};
use timely::dataflow::Scope;
use timely::dataflow::operators::Binary;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;

use ::{Data, Delta, Collection};
use lattice::Lattice;
use operators::arrange_alt::{Arranged, ArrangeByKey, ArrangeBySelf};
use trace::{Batch, Cursor, Trace, consolidate};

/// Join implementations for `(key,val)` data.
pub trait Join<G: Scope, K: Data+Ord, V: Data+Ord> {

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
} 

impl<G: Scope, K: Data+Ord, V: Data+Ord> Join<G, K, V> for Collection<G, (K, V)> where G::Timestamp: Lattice+Ord {
    /// Matches pairs of `(key,val1)` and `(key,val2)` records based on `key` and applies a reduction function.
    fn join_map<V2: Data+Ord, D: Data, R>(&self, other: &Collection<G, (K, V2)>, logic: R) -> Collection<G, D>
    where R: Fn(&K, &V, &V2)->D+'static {
        let arranged1 = self.arrange_by_key();
        let arranged2 = other.arrange_by_key();
        arranged1.join_arranged(&arranged2, logic)
    }
    fn semijoin(&self, other: &Collection<G, K>) -> Collection<G, (K, V)> {
        let arranged1 = self.arrange_by_key();
        let arranged2 = other.arrange_by_self();
        arranged1.join_arranged(&arranged2, |k,v,_| (k.clone(), v.clone()))
    }
    fn antijoin(&self, other: &Collection<G, K>) -> Collection<G, (K, V)> {
        self.concat(&self.semijoin(other).negate())
    }
}


/// Matches the elements of two arranged traces.
///
/// This method is used by the various `join` implementations, but it can also be used 
/// directly in the event that one has a handle to an `Arranged<G,T>`, perhaps because
/// the arrangement is available for re-use, or from the output of a `group` operator.
pub trait JoinArranged<G: Scope, K: Data, V: Data> where G::Timestamp: Lattice+Ord {
    /// Joins two arranged collections with the same key type.
    ///
    /// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function, 
    /// producing a corresponding output record.
    ///
    /// This trait is implemented for arrangements (`Arranged<G, T>`) rather than collections. The `Join` trait 
    /// contains the implementations for collections.
    fn join_arranged<V2,T2,R,RF> (&self, stream2: &Arranged<G,K,V2,T2>, result: RF) -> Collection<G,R>
    where 
        V2: Data+Ord,
        T2: Trace<K,V2,G::Timestamp>,
        T2: 'static,
        T2::Batch: 'static,
        R: Data,
        RF: Fn(&K,&V,&V2)->R+'static;
}

impl<G: Scope, K: Data, V: Data, T1: Trace<K,V,G::Timestamp>> JoinArranged<G, K, V> for Arranged<G,K,V,T1> 
    where 
        G::Timestamp: Lattice+Ord,
        T1: 'static,
        T1::Batch: 'static,
         {
    fn join_arranged<V2,T2,R,RF>(&self, other: &Arranged<G,K,V2,T2>, result: RF) -> Collection<G,R> 
    where 
        V2: Data,
        T2: Trace<K,V2,G::Timestamp>,
        T2: 'static,
        T2::Batch: 'static,
        R: Data,
        RF: Fn(&K,&V,&V2)->R+'static {

        let mut trace1 = Some(self.new_handle());
        let mut trace2 = Some(other.new_handle());

        let mut inputs1 = LinearMap::new();
        let mut inputs2 = LinearMap::new();

        let mut capabilities = Vec::new();
        let mut acknowledged = Vec::new();

        // let exchange1 = Exchange::new(|&(ref k,_): &((K,V),Delta) | k.hashed());
        // let exchange2 = Exchange::new(|&(ref k,_): &((K,V2),Delta)| k.hashed());

        let mut output_buffer = Vec::new();
        let mut times = Vec::new();

        let mut todo: Vec<Deferred<K,V,V2,G::Timestamp,T1,T2>> = Vec::new();     // readied outputs to enumerate.

        let stream = self.stream.binary_notify(&other.stream, Pipeline, Pipeline, "JoinAlt", vec![], move |input1, input2, output, notificator| {

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
                inputs1.insert(time.time(), data.drain(..).next().unwrap());
                if !capabilities.iter().any(|c: &Capability<G::Timestamp>| c.time() == time.time()) { capabilities.push(time); }
            });

            // read input 2, push all data to queues
            input2.for_each(|time, data| {
                inputs2.insert(time.time(), data.drain(..).next().unwrap());
                if !capabilities.iter().any(|c| c.time() == time.time()) { capabilities.push(time); }
            });

            // look for input work that we can now perform.
            capabilities.sort_by(|x,y| x.time().cmp(&y.time()));
            if let Some(position) = capabilities.iter().position(|c| !frontier1.iter().any(|t| t.le(&c.time())) &&
                                                                     !frontier2.iter().any(|t| t.le(&c.time())) ) {
                let capability = capabilities.remove(position);
                let time = capability.time();

                // create input batch, insert into trace, prepare work.
                let work1 = inputs1.remove(&time).and_then(|batch| {
                    trace2.as_ref().map(|t| (batch.item.cursor(), t.cursor()))
                });

                // create input batch, insert into trace, prepare work.
                let work2 = inputs2.remove(&time).and_then(|batch| {
                    trace1.as_ref().map(|t| (batch.item.cursor(), t.cursor()))
                });

                // enqueue work item if either input needs it.
                if work1.is_some() || work2.is_some() {
                    todo.push(Deferred::new(work1, work2, capability, &acknowledged[..]));
                }

                acknowledged.retain(|t| !(t <= &time));
                acknowledged.push(time);
            }

            // do some work on outstanding computation. maybe not all work.
            if todo.len() > 0 {

                todo[0].work(&mut output_buffer, &result, 1_000_000);

                consolidate(&mut output_buffer, 0); // co-locating like times.

                if output_buffer.len() > 0 {

                    // now sorted by times; count entries for each time to plan sessions.
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
                        let cap = todo[0].capability().delayed(&time);
                        let mut session = output.session(&cap);
                        for ((_, r), d) in drain.by_ref().take(count) {
                            session.give((r, d));
                        }
                    }
                }

                if !todo[0].work_remains() { todo.remove(0); }

                assert!(output_buffer.len() == 0);
                assert!(times.len() == 0);
            }
        });

        Collection::new(stream)
    }
}

/// Deferred join computation.
///
/// The structure wraps cursors which allow 
struct Deferred<K: Ord, V1: Ord, V2: Ord, T: Timestamp+Lattice+Ord, T1: Trace<K, V1, T>, T2: Trace<K, V2, T>> {
    work1: Option<(<T1::Batch as Batch<K,V1,T>>::Cursor, T2::Cursor)>,
    work2: Option<(<T2::Batch as Batch<K,V2,T>>::Cursor, T1::Cursor)>,
    capability: Capability<T>,
    acknowledged: Vec<T>,
}

impl<K: Ord, V1: Ord, V2: Ord, T: Timestamp+Lattice+Ord, T1: Trace<K, V1, T>, T2: Trace<K, V2, T>> Deferred<K, V1, V2, T, T1, T2> {
    fn new(work1: Option<(<T1::Batch as Batch<K,V1,T>>::Cursor, T2::Cursor)>, 
           work2: Option<(<T2::Batch as Batch<K,V2,T>>::Cursor, T1::Cursor)>, 
           capability: Capability<T>,
           acknowledged: &[T]) -> Self {
        Deferred {
            work1: work1,
            work2: work2,
            capability: capability,
            acknowledged: acknowledged.to_vec(),
        }
    }

    fn capability(&self) -> &Capability<T> { &self.capability }

    fn work_remains(&self) -> bool { 
        let work1 = self.work1.as_ref().map(|x| x.0.key_valid());
        let work2 = self.work2.as_ref().map(|x| x.0.key_valid());
        work1 == Some(true) || work2 == Some(true)
    }

    /// Process keys until at least `limit` output tuples produced, or work exhausted.
    fn work<R: Ord+Clone, RF: Fn(&K, &V1, &V2)->R>(&mut self, output: &mut Vec<((T, R), Delta)>, logic: &RF, limit: usize) {

        // work on the first bit of work.
        let acknowledged = &self.acknowledged;
        let time = self.capability.time();

        if let Some((ref mut layer, ref mut trace)) = self.work1 {
            while layer.key_valid() && output.len() < limit {
                let output_len = output.len();
                trace.seek_key(layer.key());
                if trace.key_valid() && trace.key() == layer.key() {
                    while trace.val_valid() {
                        while layer.val_valid() {
                            let r = logic(layer.key(), layer.val(), trace.val());
                            trace.map_times(|time1, diff1| {
                                if acknowledged.iter().any(|t| time1 <= t) {
                                    layer.map_times(|time2, diff2| {
                                        output.push(((time1.join(time2), r.clone()), diff1 * diff2));
                                    });
                                }
                            });

                            layer.step_val();
                        }

                        layer.rewind_vals();
                        trace.step_val();
                    }
                }
                layer.step_key();
                consolidate(output, output_len);
            }
        }

        // work on the second bit of work.
        if let Some((ref mut layer, ref mut trace)) = self.work2 {
            while layer.key_valid() && output.len() < limit {
                let output_len = output.len(); 
                trace.seek_key(layer.key());
                if trace.key_valid() && trace.key() == layer.key() {
                    while trace.val_valid() {
                        while layer.val_valid() {
                            let r = logic(layer.key(), trace.val(), layer.val());
                            trace.map_times(|time1, diff1| {
                                if time1 <= &time || acknowledged.iter().any(|t| time1 <= t) {
                                    layer.map_times(|time2, diff2| {
                                        output.push(((time1.join(time2), r.clone()), diff1 * diff2));
                                    });                   
                                }                         
                            });

                            layer.step_val();
                        }

                        layer.rewind_vals();
                        trace.step_val();
                    }
                }
                layer.step_key();
                consolidate(output, output_len);
            }
        }
    }
}