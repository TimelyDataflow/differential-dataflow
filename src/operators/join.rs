//! Match pairs of records based on a key.

use std::fmt::Debug;
use std::ops::Mul;

use timely::progress::Timestamp;
use timely::dataflow::Scope;
use timely::dataflow::operators::Binary;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely_sort::Unsigned;

use timely::dataflow::operators::OutputHandle;
use timely::dataflow::channels::pushers::tee::Tee;


use hashable::{Hashable, UnsignedWrapper};
use ::{Data, Diff, Collection, AsCollection};
use lattice::Lattice;
use operators::arrange::{Arrange, Arranged, ArrangeByKey, ArrangeBySelf};
use trace::{Batch, Cursor, Trace, consolidate};
use operators::ValueHistory2;

// use trace::implementations::hash::HashValSpine as DefaultValTrace;
// use trace::implementations::hash::HashKeySpine as DefaultKeyTrace;
use trace::implementations::ord::OrdValSpine as DefaultValTrace;
use trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;

/// Join implementations for `(key,val)` data.
pub trait Join<G: Scope, K: Data, V: Data, R: Diff> {

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
    fn join<V2: Data, R2: Diff>(&self, other: &Collection<G, (K,V2), R2>) -> Collection<G, (K,V,V2), <R as Mul<R2>>::Output> 
    where R: Mul<R2>, <R as Mul<R2>>::Output: Diff
    {
        self.join_map(other, |k,v,v2| (k.clone(),v.clone(),v2.clone()))
    }
    /// Like `join`, but with an randomly distributed unsigned key.
    fn join_u<V2: Data, R2: Diff>(&self, other: &Collection<G, (K,V2), R2>) -> Collection<G, (K,V,V2), <R as Mul<R2>>::Output>
    where K: Unsigned+Copy, R: Mul<R2>, <R as Mul<R2>>::Output: Diff {
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
    fn join_map<V2, R2: Diff, D, L>(&self, other: &Collection<G, (K,V2), R2>, logic: L) -> Collection<G, D, <R as Mul<R2>>::Output>
    where V2: Data, R: Mul<R2>, <R as Mul<R2>>::Output: Diff, D: Data, L: Fn(&K, &V, &V2)->D+'static;
    /// Like `join_map`, but with a randomly distributed unsigned key.
    fn join_map_u<V2, R2: Diff, D, L>(&self, other: &Collection<G, (K,V2), R2>, logic: L) -> Collection<G, D, <R as Mul<R2>>::Output> 
    where K: Unsigned+Copy, R: Mul<R2>, <R as Mul<R2>>::Output: Diff, V2: Data, D: Data, L: Fn(&K, &V, &V2)->D+'static;
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
    fn semijoin<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), <R as Mul<R2>>::Output> 
    where R2: Diff, R: Mul<R2>, <R as Mul<R2>>::Output: Diff;
    /// Like `semijoin`, but with a randomly distributed unsigned key.    
    fn semijoin_u<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), <R as Mul<R2>>::Output> 
    where K: Unsigned+Copy, R2: Diff, R: Mul<R2>, <R as Mul<R2>>::Output: Diff;
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
    fn antijoin<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), R>
    where R2: Diff, R: Mul<R2, Output = R>;
    /// Like `antijoin`, but with a randomly distributed unsigned key.
    fn antijoin_u<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), R>
    where K: Unsigned+Copy, R2: Diff, R: Mul<R2, Output=R>;
} 


impl<G, K, V, R> Join<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope, 
    K: Data+Default+Hashable, 
    V: Data,
    R: Diff,
    G::Timestamp: Lattice+Ord+Copy,
{
    fn join_map<V2: Data, R2: Diff, D: Data, L>(&self, other: &Collection<G, (K, V2), R2>, logic: L) -> Collection<G, D, <R as Mul<R2>>::Output>
    where R: Mul<R2>, <R as Mul<R2>>::Output: Diff, L: Fn(&K, &V, &V2)->D+'static {
        let arranged1 = self.arrange_by_key_hashed();
        let arranged2 = other.arrange_by_key_hashed();
        arranged1.join_arranged(&arranged2, move |k,v1,v2| logic(&k.item,v1,v2))
    }
    fn semijoin<R2: Diff>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), <R as Mul<R2>>::Output> 
    where R: Mul<R2>, <R as Mul<R2>>::Output: Diff {
        let arranged1 = self.arrange_by_key_hashed();
        let arranged2 = other.arrange_by_self();
        arranged1.join_arranged(&arranged2, |k,v,_| (k.item.clone(), v.clone()))
    }
    fn antijoin<R2: Diff>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), R>
    where R: Mul<R2, Output=R> {
        self.concat(&self.semijoin(other).negate())
    }

    fn join_map_u<V2, R2, D, L>(&self, other: &Collection<G, (K, V2), R2>, logic: L) -> Collection<G, D, <R as Mul<R2>>::Output>
    where K: Unsigned+Copy, V2: Data, R2: Diff, R: Mul<R2>, <R as Mul<R2>>::Output: Diff, D: Data, L: Fn(&K, &V, &V2)->D+'static {
        let arranged1 = self.map(|(k,v)| (UnsignedWrapper::from(k), v))
                            .arrange(DefaultValTrace::new());
        let arranged2 = other.map(|(k,v)| (UnsignedWrapper::from(k), v))
                             .arrange(DefaultValTrace::new());
        arranged1.join_arranged(&arranged2, move |k,v1,v2| logic(&k.item,v1,v2))
    }
    fn semijoin_u<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), <R as Mul<R2>>::Output>
    where K: Unsigned+Copy, R2: Diff, R: Mul<R2>, <R as Mul<R2>>::Output: Diff {
        let arranged1 = self.map(|(k,v)| (UnsignedWrapper::from(k), v))
                            .arrange(DefaultValTrace::new());
        let arranged2 = other.map(|k| (UnsignedWrapper::from(k), ()))
                             .arrange(DefaultKeyTrace::new());
        arranged1.join_arranged(&arranged2, |k,v,_| (k.item.clone(), v.clone()))
    }
    fn antijoin_u<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), R>
    where K: Unsigned+Copy, R2: Diff, R: Mul<R2, Output=R> {
        self.concat(&self.semijoin(other).negate())
    }
}

/// Matches the elements of two arranged traces.
///
/// This method is used by the various `join` implementations, but it can also be used 
/// directly in the event that one has a handle to an `Arranged<G,T>`, perhaps because
/// the arrangement is available for re-use, or from the output of a `group` operator.
pub trait JoinArranged<G: Scope, K: 'static, V: 'static, R: Diff> where G::Timestamp: Lattice+Ord {
    /// Joins two arranged collections with the same key type.
    ///
    /// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function, 
    /// producing a corresponding output record.
    ///
    /// This trait is implemented for arrangements (`Arranged<G, T>`) rather than collections. The `Join` trait 
    /// contains the implementations for collections.
    fn join_arranged<V2,T2,R2,D,L> (&self, stream2: &Arranged<G,K,V2,R2,T2>, result: L) -> Collection<G,D,<R as Mul<R2>>::Output>
    where 
        V2: Ord+Clone+Debug+'static,
        T2: Trace<K, V2, G::Timestamp, R2>+'static,
        T2::Batch: 'static,
        R2: Diff,
        R: Mul<R2>,
        <R as Mul<R2>>::Output: Diff,
        D: Data,
        L: Fn(&K,&V,&V2)->D+'static;
}

impl<G, K, V, R1, T1> JoinArranged<G, K, V, R1> for Arranged<G,K,V,R1,T1> 
    where 
        G: Scope, 
        K: Debug+Eq+'static, 
        V: Ord+Clone+Debug+'static, 
        R1: Diff,
        T1: Trace<K,V,G::Timestamp, R1>+'static,
        G::Timestamp: Lattice+Ord+Debug+Copy,
        T1::Batch: 'static+Debug {
    fn join_arranged<V2,T2,R2,D,L>(&self, other: &Arranged<G,K,V2,R2,T2>, result: L) -> Collection<G,D,<R1 as Mul<R2>>::Output> 
    where 
        V2: Ord+Clone+Debug+'static,
        T2: Trace<K,V2,G::Timestamp,R2>+'static,
        T2::Batch: 'static,
        R2: Diff,
        R1: Mul<R2>,
        <R1 as Mul<R2>>::Output: Diff,
        D: Data,
        L: Fn(&K,&V,&V2)->D+'static {

        // handles to shared trace data structures.
        let mut trace1 = Some(self.new_handle());
        let mut trace2 = Some(other.new_handle());

        // acknowledged frontier for each input.
        let mut acknowledged1 = vec![G::Timestamp::min()];
        let mut acknowledged2 = vec![G::Timestamp::min()];

        // deferred work of batches from each input.
        let mut todo1: Vec<Deferred<K,V2,V,G::Timestamp,R2,R1,<R1 as Mul<R2>>::Output,T2::Cursor,<T1::Batch as Batch<K,V,G::Timestamp,R1>>::Cursor,_>> = Vec::new();
        let mut todo2: Vec<Deferred<K,V,V2,G::Timestamp,R1,R2,<R1 as Mul<R2>>::Output,T1::Cursor,<T2::Batch as Batch<K,V2,G::Timestamp,R2>>::Cursor,_>> = Vec::new();

        self.stream.binary_notify(&other.stream, Pipeline, Pipeline, "Join", vec![], move |input1, input2, output, notificator| {

            // The join computation repeatedly accepts batches of updates from each of its inputs.
            //
            // For each accepted batch, it prepares a work-item to join the batch against previously "accepted"
            // updates from its other input. It is important to track which updates have been accepted, through
            // a combination of the input's frontier and the most recently received batch's upper bound, because
            // we use a shared trace and there may be updates present that are in advance of this accepted bound.

            // drain input 1, prepare work.
            input1.for_each(|capability, data| {
                if let Some(ref trace2) = trace2 {
                    for batch1 in data.drain(..) {
                        let trace2_cursor = trace2.cursor_through(&acknowledged2[..]).unwrap();
                        let batch1_cursor = batch1.item.cursor();
                        todo1.push(Deferred::new(trace2_cursor, batch1_cursor, capability.clone(), |r2,r1| *r1 * *r2));
                        debug_assert!(batch1.item.description().lower() == &acknowledged1[..]);
                        acknowledged1 = batch1.item.description().upper().to_vec();
                    }
                }
            });

            // drain input 2, prepare work.
            input2.for_each(|capability, data| {
                if let Some(ref trace1) = trace1 {
                    for batch2 in data.drain(..) {
                        let trace1_cursor = trace1.cursor_through(&acknowledged1[..]).unwrap();
                        let batch2_cursor = batch2.item.cursor();
                        todo2.push(Deferred::new(trace1_cursor, batch2_cursor, capability.clone(), |r1,r2| *r1 * *r2));
                        debug_assert!(batch2.item.description().lower() == &acknowledged2[..]);
                        acknowledged2 = batch2.item.description().upper().to_vec();
                    }
                }
            });

            // shut down or advance trace2. if the frontier is empty we can shut it down,
            // and otherwise we can advance the trace by the acknowledged elements of the other input,
            // as we may still use them as thresholds (ie we must preserve `le` wrt `acknowledged`).
            if trace2.is_some() && notificator.frontier(0).len() == 0 { trace2 = None; }
            if let Some(ref mut trace2) = trace2 {
                trace2.advance_by(notificator.frontier(0));
                trace2.distinguish_since(&acknowledged2[..]);
            }

            // shut down or advance trace1.
            if trace1.is_some() && notificator.frontier(1).len() == 0 { trace1 = None; }
            if let Some(ref mut trace1) = trace1 {
                trace1.advance_by(notificator.frontier(1));
                trace1.distinguish_since(&acknowledged1[..]);
            }

            let mut fuel = 1_000_000;

            // perform some amount of outstanding work. 
            while todo1.len() > 0 && fuel > 0 {
                todo1[0].work(output, &|k,v2,v1| result(k,v1,v2), &mut fuel);
                if !todo1[0].work_remains() { todo1.remove(0); }
            }

            // perform some amount of outstanding work. 
            while todo2.len() > 0 && fuel > 0 {
                todo2[0].work(output, &|k,v1,v2| result(k,v1,v2), &mut fuel);
                if !todo2[0].work_remains() { todo2.remove(0); }
            }

        })
        .as_collection()
    }
}

/// Deferred join computation.
///
/// The structure wraps cursors which allow us to play out join computation at whatever rate we like.
/// This allows us to avoid producing and buffering massive amounts of data, without giving the timely
/// dataflow system a chance to run operators that can consume and aggregate the data.
struct Deferred<K, V1, V2, T, R1, R2, R3, C1, C2, M> 
where 
    V1: Ord+Clone,
    V2: Ord+Clone,
    T: Timestamp+Lattice+Ord+Debug, 
    C1: Cursor<K, V1, T, R1>,
    C2: Cursor<K, V2, T, R2>,
    M: Fn(&R1,&R2)->R3,
{
    phant: ::std::marker::PhantomData<(K, V1, V2, R1, R2)>,
    trace: C1,
    batch: C2,
    capability: Capability<T>,
    mult: M,
}

impl<K, V1, V2, T, R1, R2, R3, C1, C2, M> Deferred<K, V1, V2, T, R1, R2, R3, C1, C2, M>
where
    K: Debug+Eq,
    V1: Ord+Clone+Debug,
    V2: Ord+Clone+Debug,
    T: Timestamp+Lattice+Ord+Debug+Copy,
    R1: Diff, 
    R2: Diff, 
    R3: Diff,
    C1: Cursor<K, V1, T, R1>,
    C2: Cursor<K, V2, T, R2>,
    M: Fn(&R1,&R2)->R3,
{
    fn new(trace: C1, batch: C2, capability: Capability<T>, mult: M) -> Self {
        Deferred {
            phant: ::std::marker::PhantomData,
            trace: trace,
            batch: batch,
            capability: capability,
            mult: mult,
        }
    }

    fn work_remains(&self) -> bool { 
        self.batch.key_valid()
    }

    /// Process keys until at least `limit` output tuples produced, or the work is exhausted.
    #[inline(never)]
    fn work<D, L>(&mut self, output: &mut OutputHandle<T, (D, T, R3), Tee<T, (D, T, R3)>>, logic: &L, fuel: &mut usize) 
    where D: Ord+Clone+Data, L: Fn(&K, &V1, &V2)->D {

        let meet = self.capability.time();

        let mut effort = 0;
        let mut session = output.session(&self.capability);

        let trace = &mut self.trace;
        let batch = &mut self.batch;
        let mult = &self.mult;

        let mut temp = Vec::new();
        let mut thinker = JoinThinker::<V1, V2, T, R1, R2>::new();

        while batch.key_valid() && effort < *fuel {

            trace.seek_key(batch.key());
            if trace.key_valid() && trace.key() == batch.key() {

                thinker.history1.edits.load(trace, |time| time.join(&meet));
                thinker.history2.edits.load(batch, |time| time.clone());

                // populate `temp` with the results in the best way we know how.
                thinker.think(|v1,v2,t,r1,r2| temp.push(((logic(batch.key(), v1, v2), t), mult(r1,r2))));
                consolidate(&mut temp, 0);

                effort += temp.len();
                for ((d, t), r) in temp.drain(..) {
                    session.give((d, t, r));
                }

            }

            batch.step_key();
        }

        if effort > *fuel { *fuel = 0; }
        else              { *fuel -= effort; }
    }
}

struct JoinThinker<V1: Ord+Clone, V2: Ord+Clone, T: Lattice+Ord+Clone, R1: Diff, R2: Diff> {
    pub history1: ValueHistory2<V1, T, R1>,
    pub history2: ValueHistory2<V2, T, R2>,
}

impl<V1: Ord+Clone, V2: Ord+Clone, T: Lattice+Ord+Clone, R1: Diff, R2: Diff> JoinThinker<V1, V2, T, R1, R2> 
where V1: Debug, V2: Debug, T: Debug
{
    fn new() -> Self {
        JoinThinker {
            history1: ValueHistory2::new(),
            history2: ValueHistory2::new(),
        }
    }

    fn think<F: FnMut(&V1,&V2,T,&R1,&R2)>(&mut self, mut results: F) {

        // for reasonably sized edits, do the dead-simple thing.
        if self.history1.edits.len() < 1000 || self.history2.edits.len() < 1000 {
            self.history1.edits.map(|v1, t1, d1| {
                self.history2.edits.map(|v2, t2, d2| {
                    results(v1, v2, t1.join(t2), &d1, &d2);
                })
            })
        }
        else {

            self.history1.order();
            self.history2.order();

            while !self.history1.is_done() && !self.history2.is_done() {

                if self.history1.time().unwrap().cmp(&self.history2.time().unwrap()) == ::std::cmp::Ordering::Less {
                    self.history2.advance_buffer_by(self.history1.meet().unwrap());
                    for &((ref val2, ref time2), ref diff2) in &self.history2.buffer {
                        let (val1, time1, ref diff1) = self.history1.edit().unwrap();
                        results(val1, val2, time1.join(time2), diff1, diff2);
                    }
                    self.history1.step();
                }
                else {
                    self.history1.advance_buffer_by(self.history2.meet().unwrap());
                    for &((ref val1, ref time1), ref diff1) in &self.history1.buffer {
                        let (val2, time2, ref diff2) = self.history2.edit().unwrap();
                        results(val1, val2, time1.join(time2), diff1, diff2);
                    }
                    self.history2.step();
                }
            }

            while !self.history1.is_done() {
                self.history2.advance_buffer_by(self.history1.meet().unwrap());
                for &((ref val2, ref time2), ref diff2) in &self.history2.buffer {
                    let (val1, time1, ref diff1) = self.history1.edit().unwrap();
                    results(val1, val2, time1.join(time2), diff1, diff2);
                }
                self.history1.step();                
            }
            while !self.history2.is_done() {
                self.history1.advance_buffer_by(self.history2.meet().unwrap());
                for &((ref val1, ref time1), ref diff1) in &self.history1.buffer {
                    let (val2, time2, ref diff2) = self.history2.edit().unwrap();
                    results(val1, val2, time1.join(time2), diff1, diff2);
                }
                self.history2.step();                
            }
        }
    }
}