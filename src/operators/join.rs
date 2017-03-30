//! Match pairs of records based on a key.

use std::fmt::Debug;
use std::borrow::Borrow;

use linear_map::LinearMap;

use timely::progress::{Timestamp, Antichain};
use timely::dataflow::Scope;
use timely::dataflow::operators::Binary;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely_sort::Unsigned;

use hashable::{Hashable, UnsignedWrapper};
use ::{Data, Ring, Collection};
use lattice::Lattice;
use operators::arrange::{Arrange, Arranged, ArrangeByKey, ArrangeBySelf};
use trace::{Batch, Cursor, Trace, consolidate};

use trace::implementations::rhh::Spine as HashSpine;
use trace::implementations::rhh_k::Spine as KeyHashSpine;

/// Join implementations for `(key,val)` data.
pub trait Join<G: Scope, K: Data, V: Data, R: Ring> {

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
    fn join<V2: Data>(&self, other: &Collection<G, (K,V2), R>) -> Collection<G, (K,V,V2), R> {
        self.join_map(other, |k,v,v2| (k.clone(),v.clone(),v2.clone()))
    }
    /// Like `join`, but with an randomly distributed unsigned key.
    fn join_u<V2: Data>(&self, other: &Collection<G, (K,V2), R>) -> Collection<G, (K,V,V2), R> where K: Unsigned+Copy {
        self.join_map_u(other, |k,v,v2| (k.clone(),v.clone(),v2.clone()))
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
    fn join_map<V2, D, L>(&self, other: &Collection<G, (K,V2), R>, logic: L) -> Collection<G, D, R>
    where V2: Data, D: Data, L: Fn(&K, &V, &V2)->D+'static;
    /// Like `join_map`, but with a randomly distributed unsigned key.
    fn join_map_u<V2, D, L>(&self, other: &Collection<G, (K,V2), R>, logic: L) -> Collection<G, D, R> 
    where K: Unsigned+Copy, V2: Data, D: Data, L: Fn(&K, &V, &V2)->D+'static;
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
    fn semijoin(&self, other: &Collection<G, K, R>) -> Collection<G, (K, V), R>;
    /// Like `semijoin`, but with a randomly distributed unsigned key.    
    fn semijoin_u(&self, other: &Collection<G, K, R>) -> Collection<G, (K, V), R> where K: Unsigned+Copy;
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
    ///     // should retain record `(1,2)` and discard `(0,0)`.
    ///     col1.antijoin(&col2).consolidate().capture();
    /// });
    ///
    /// let extracted = data.extract();
    /// assert_eq!(extracted.len(), 1);
    /// assert_eq!(extracted[0].1, vec![((1,2),1)]);
    /// ```
    fn antijoin(&self, other: &Collection<G, K, R>) -> Collection<G, (K, V), R>;
    /// Like `antijoin`, but with a randomly distributed unsigned key.
    fn antijoin_u(&self, other: &Collection<G, K, R>) -> Collection<G, (K, V), R> where K: Unsigned+Copy;
} 


impl<G, K, V, R> Join<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope, 
    K: Data+Default+Hashable, 
    V: Data,
    R: Ring,
    G::Timestamp: Lattice+Ord+Copy,
{
    fn join_map<V2: Data, D: Data, L>(&self, other: &Collection<G, (K, V2), R>, logic: L) -> Collection<G, D, R>
    where L: Fn(&K, &V, &V2)->D+'static {
        let arranged1 = self.arrange_by_key_hashed();
        let arranged2 = other.arrange_by_key_hashed();
        arranged1.join_arranged(&arranged2, move |k,v1,v2| logic(&k.item,v1,v2))
    }
    fn semijoin(&self, other: &Collection<G, K, R>) -> Collection<G, (K, V), R> {
        let arranged1 = self.arrange_by_key_hashed();
        let arranged2 = other.arrange_by_self();
        arranged1.join_arranged(&arranged2, |k,v,_| (k.item.clone(), v.clone()))
    }
    fn antijoin(&self, other: &Collection<G, K, R>) -> Collection<G, (K, V), R> {
        self.concat(&self.semijoin(other).negate())
    }

    fn join_map_u<V2: Data, D: Data, L>(&self, other: &Collection<G, (K, V2), R>, logic: L) -> Collection<G, D, R>
    where L: Fn(&K, &V, &V2)->D+'static, K: Unsigned+Copy {
        let arranged1 = self.arrange(|k,v| (UnsignedWrapper::from(k), v), HashSpine::new(Default::default()));
        let arranged2 = other.arrange(|k,v| (UnsignedWrapper::from(k), v), HashSpine::new(Default::default()));
        arranged1.join_arranged(&arranged2, move |k,v1,v2| logic(&k.item,v1,v2))
    }
    fn semijoin_u(&self, other: &Collection<G, K, R>) -> Collection<G, (K, V), R> where K: Unsigned+Copy {
        let arranged1 = self.arrange(|k,v| (UnsignedWrapper::from(k), v), HashSpine::new(Default::default()));
        let arranged2 = other.map(|k| (k,())).arrange(|k,v| (UnsignedWrapper::from(k), v), KeyHashSpine::new(Default::default()));
        arranged1.join_arranged(&arranged2, |k,v,_| (k.item.clone(), v.clone()))
    }
    fn antijoin_u(&self, other: &Collection<G, K, R>) -> Collection<G, (K, V), R> where K: Unsigned+Copy {
        self.concat(&self.semijoin(other).negate())
    }
}

/// Matches the elements of two arranged traces.
///
/// This method is used by the various `join` implementations, but it can also be used 
/// directly in the event that one has a handle to an `Arranged<G,T>`, perhaps because
/// the arrangement is available for re-use, or from the output of a `group` operator.
pub trait JoinArranged<G: Scope, K: 'static, V: 'static, R: Ring> where G::Timestamp: Lattice+Ord {
    /// Joins two arranged collections with the same key type.
    ///
    /// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function, 
    /// producing a corresponding output record.
    ///
    /// This trait is implemented for arrangements (`Arranged<G, T>`) rather than collections. The `Join` trait 
    /// contains the implementations for collections.
    fn join_arranged<V2,T2,D,L> (&self, stream2: &Arranged<G,K,V2,R,T2>, result: L) -> Collection<G,D,R>
    where 
        V2: 'static,
        T2: Trace<K, V2, G::Timestamp, R>+'static,
        T2: 'static,
        T2::Batch: 'static,
        D: Data,
        L: Fn(&K,&V,&V2)->D+'static;
}

impl<G, K, V, R, T1> JoinArranged<G, K, V, R> for Arranged<G,K,V,R,T1> 
    where 
        G: Scope, 
        K: Eq+'static, 
        V: 'static, 
        R: Ring,
        T1: Trace<K,V,G::Timestamp, R>,
        G::Timestamp: Lattice+Ord+Debug+Copy,
        T1: 'static,
        T1::Batch: 'static+Debug {
    fn join_arranged<V2,T2,D,L>(&self, other: &Arranged<G,K,V2,R,T2>, result: L) -> Collection<G,D,R> 
    where 
        V2: 'static,
        T2: Trace<K,V2,G::Timestamp,R>,
        T2: 'static,
        T2::Batch: 'static,
        D: Data,
        L: Fn(&K,&V,&V2)->D+'static {

        let mut trace1 = Some(self.new_handle());
        let mut trace2 = Some(other.new_handle());

        let mut inputs1 = LinearMap::<G::Timestamp, T1::Batch>::new();
        let mut inputs2 = LinearMap::<G::Timestamp, T2::Batch>::new();

        let mut capabilities = Vec::<Capability<G::Timestamp>>::new();

        let mut output_buffer = Vec::new();

        let mut todo: Vec<Deferred<K,V,V2,G::Timestamp,R,T1,T2>> = Vec::new();     // readied outputs to enumerate.

        let stream = self.stream.binary_notify(&other.stream, Pipeline, Pipeline, "JoinAlt", vec![], move |input1, input2, output, notificator| {

            let frontier1 = notificator.frontier(0);
            let frontier2 = notificator.frontier(1);

            // The acknowledged frontier
            //
            // The `ack_frontier` antichain tracks those times at which updates have not yet been acknowledged.
            // It is defined by the two input frontiers, and the set of as-yet unstarted work. When we start
            // to work on a time the current acknowledged frontier is captured, and the time is considered 
            // committed. Even though we have not yet produced the outputs, we have accounted for their 
            // eventual production. The acknowledged frontier is just about accounting for outputs, rather
            // than enforcing anything about how they are produced.

            let mut ack_frontier = Antichain::new();

            for time in frontier1 { ack_frontier.insert(time.clone()); }
            for time in frontier2 { ack_frontier.insert(time.clone()); }
            for cap in &capabilities { ack_frontier.insert(cap.time()); }

            // println!("ack_frontier: {:?}", ack_frontier.elements());

            // Advancing the input collections
            //
            // Join's use of input collections is unlike `group`'s use of input collections. Each input 
            // collection has its updates
            //
            //   (i) filtered by "acknowledged times" `ack_frontier`, and then 
            //   (ii) crossed with new updates from the other collection. 
            //
            // We must not remove the distinction between updates with respect to any time in the frontier
            // of times we may yet acknowledge, as these distinctions are what include or exclude updates
            // from the corresponding cross product. This frontier as already defined includes the times 
            // of future updates, which define the second constraint. We should be ok using `ack_frontier` 
            // for the calls to `advance_by`. 

            // shut down trace1 if the frontier2 is empty and inputs2 are done. else, advance its frontier.
            if trace2.is_some() && frontier1.len() == 0 && inputs1.len() == 0 { trace2 = None; }
            if let Some(ref mut trace) = trace2 {
                trace.advance_by(ack_frontier.elements());
            }

            // shut down trace1 if the frontier2 is empty and inputs2 are done. else, advance its frontier.
            if trace1.is_some() && frontier2.len() == 0 && inputs2.len() == 0 { trace1 = None; }
            if let Some(ref mut trace) = trace1 {
                trace.advance_by(ack_frontier.elements());
            }

            // read input 1, push all data to queues
            input1.for_each(|time, data| {
                // println!("join1 recv: {:?}", time);
                // for elt in data.iter() { println!("  {:?}", elt); }
                inputs1.insert(time.time(), data.drain(..).next().unwrap().item);
                if !capabilities.iter().any(|c| c.time() == time.time()) { capabilities.push(time); }
            });

            // read input 2, push all data to queues
            input2.for_each(|time, data| {
                inputs2.insert(time.time(), data.drain(..).next().unwrap().item);
                if !capabilities.iter().any(|c| c.time() == time.time()) { capabilities.push(time); }
            });

            // look for input work that we can now perform.
            capabilities.sort_by(|x,y| x.time().cmp(&y.time()));
            if let Some(position) = capabilities.iter().position(|c| !frontier1.iter().any(|t| t.le(&c.time())) &&
                                                                     !frontier2.iter().any(|t| t.le(&c.time())) ) {
                let capability = capabilities.remove(position);
                let time = capability.time();

                // println!("join working: {:?}", time);

                // create input batch, insert into trace, prepare work.
                let work1 = inputs1.remove(&time).and_then(|batch| {
                    trace2.as_ref().map(|t| (batch.cursor(), t.cursor()))
                });

                // create input batch, insert into trace, prepare work.
                let work2 = inputs2.remove(&time).and_then(|batch| {
                    trace1.as_ref().map(|t| (batch.cursor(), t.cursor()))
                });

                // enqueue work item if either input needs it.
                if work1.is_some() || work2.is_some() {
                    // println!("join work started");
                    todo.push(Deferred::new(work1, work2, capability, ack_frontier));
                }
            }

            // perform some outstanding computation. perhaps not all of it.
            if todo.len() > 0 {

                // produces at least 1_000_000 outputs, or exhausts `todo[0]`.
                todo[0].work(&mut output_buffer, &result, 1_000_000);
                // println!("join_work.len(): {:?}", output_buffer.len());

                {   // need scope to let session drop, so we can ditch todo[0].
                    let mut session = output.session(todo[0].capability());
                    for ((time, data), diff) in output_buffer.drain(..) {
                        session.give((data, time, diff));
                    }
                }

                // TODO : A queue would be better than a stack. :P
                if !todo[0].work_remains() { todo.remove(0); }
            }
        });

        Collection::new(stream)
    }
}

/// Deferred join computation.
///
/// The structure wraps cursors which allow us to play out join computation at whatever rate we like.
/// This allows us to avoid producing and buffering massive amounts 
struct Deferred<K, V1, V2, T, R, T1, T2> 
where 
    T: Timestamp+Lattice+Ord+Debug, 
    T1: Trace<K, V1, T, R>, 
    T2: Trace<K, V2, T, R>,
{
    work1: Option<(<T1::Batch as Batch<K,V1,T,R>>::Cursor, T2::Cursor)>,
    work2: Option<(<T2::Batch as Batch<K,V2,T,R>>::Cursor, T1::Cursor)>,
    capability: Capability<T>,
    ack_frontier: Antichain<T>,
}

impl<K, V1, V2, T, R, T1, T2> Deferred<K, V1, V2, T, R, T1, T2>
where
    K: Eq,
    T: Timestamp+Lattice+Ord+Debug+Copy,
    R: Ring, 
    T1: Trace<K,V1,T,R>, 
    T2: Trace<K,V2,T,R>,
{
    fn new(work1: Option<(<T1::Batch as Batch<K,V1,T,R>>::Cursor, T2::Cursor)>, 
           work2: Option<(<T2::Batch as Batch<K,V2,T,R>>::Cursor, T1::Cursor)>, 
           capability: Capability<T>,
           ack_frontier: Antichain<T>) -> Self {
        Deferred {
            work1: work1,
            work2: work2,
            capability: capability,
            ack_frontier: ack_frontier,
        }
    }

    fn capability(&self) -> &Capability<T> { &self.capability }

    fn work_remains(&self) -> bool { 
        let work1 = self.work1.as_ref().map(|x| x.0.key_valid());
        let work2 = self.work2.as_ref().map(|x| x.0.key_valid());
        work1 == Some(true) || work2 == Some(true)
    }

    /// Process keys until at least `limit` output tuples produced, or the work is exhausted.
    ///
    /// This method steps through keys in both update layers, doing complete keys worth of work until
    /// `output` contains at least `limit` records. This may involve much more than `limit` computation,
    /// as updates may cancel, and for massive keys it may result in arbitrarily large output anyhow.
    ///
    /// To make the method more responsive, we could count the number of diffs processed, and allow the 
    /// computation to break out at any point.
    #[inline(never)]
    fn work<D: Ord+Clone, L: Fn(&K, &V1, &V2)->D>(&mut self, output: &mut Vec<((T, D), R)>, logic: &L, limit: usize) {

        let ack_frontier = &self.ack_frontier;
        let time = self.capability.time();
        let mut temp = Vec::new();

        // work on the first bit of work.
        if let Some((ref mut layer, ref mut trace)) = self.work1 {
            while layer.key_valid() && output.len() < limit {
                trace.seek_key(layer.key().borrow());
                if trace.key_valid() && trace.key() == layer.key() {
                    while trace.val_valid() {
                        while layer.val_valid() {
                            let r = logic(layer.key(), layer.val(), trace.val());
                            trace.map_times(|time1, diff1| {
                                if !ack_frontier.elements().iter().any(|t| t <= time1) {
                                    layer.map_times(|time2, diff2| {
                                        temp.push((time1.join(time2), diff1 * diff2));
                                    });
                                }
                            });

                            consolidate(&mut temp, 0);
                            output.extend(temp.drain(..).map(|(t,d)| ((t,r.clone()), d)));
                            layer.step_val();
                        }

                        layer.rewind_vals();
                        trace.step_val();
                    }
                }
                layer.step_key();
            }
        }

        // work on the second bit of work.
        if let Some((ref mut layer, ref mut trace)) = self.work2 {
            while layer.key_valid() && output.len() < limit {
                trace.seek_key(layer.key().borrow());
                if trace.key_valid() && trace.key() == layer.key() {
                    while trace.val_valid() {
                        while layer.val_valid() {
                            let r = logic(layer.key(), trace.val(), layer.val());
                            trace.map_times(|time1, diff1| {
                                if time1 <= &time || !ack_frontier.elements().iter().any(|t| t <= time1) {
                                    layer.map_times(|time2, diff2| {
                                        temp.push((time1.join(time2), diff1 * diff2));
                                    });                   
                                }                         
                            });

                            consolidate(&mut temp, 0);
                            output.extend(temp.drain(..).map(|(t,d)| ((t,r.clone()), d)));
                            layer.step_val();
                        }

                        layer.rewind_vals();
                        trace.step_val();
                    }
                }
                layer.step_key();
            }
        }
    }
}