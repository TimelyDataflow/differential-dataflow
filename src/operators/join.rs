//! Match pairs of records based on a key.
//!
//! The various `join` implementations require that the units of each collection can be multiplied, and that 
//! the multiplication distributes over addition. That is, we will repeatedly evaluate (a + b) * c as (a * c)
//! + (b * c), and if this is not equal to the former term, little is known about the actual output.
use std::fmt::Debug;
use std::ops::Mul;
use std::cmp::Ordering;

use timely::progress::Timestamp;
use timely::dataflow::Scope;
use timely::dataflow::operators::generic::{Binary, OutputHandle};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::channels::pushers::tee::Tee;

use hashable::Hashable;
use ::{Data, Diff, Collection, AsCollection};
use lattice::Lattice;
use operators::arrange::{Arranged, ArrangeByKey, ArrangeBySelf};
use trace::{BatchReader, Cursor, consolidate};
use operators::ValueHistory2;

use trace::TraceReader;

/// Join implementations for `(key,val)` data.
pub trait Join<G: Scope, K: Data, V: Data, R: Diff> {

    /// Matches pairs `(key,val1)` and `(key,val2)` based on `key` and then applies a function.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1;
    ///         let z = scope.new_collection_from(vec![(0, 1, 'a'), (1, 3, 'b')]).1;
    ///
    ///         x.join(&y)
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn join<V2: Data, R2: Diff>(&self, other: &Collection<G, (K,V2), R2>) -> Collection<G, (K,V,V2), <R as Mul<R2>>::Output> 
    where R: Mul<R2>, <R as Mul<R2>>::Output: Diff
    {
        self.join_map(other, |k,v,v2| (k.clone(),v.clone(),v2.clone()))
    }

    /// Matches pairs `(key,val1)` and `(key,val2)` based on `key` and then applies a function.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1;
    ///         let z = scope.new_collection_from(vec![(1, 'a'), (3, 'b')]).1;
    ///
    ///         x.join_map(&y, |_key, &a, &b| (a,b))
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn join_map<V2, R2: Diff, D, L>(&self, other: &Collection<G, (K,V2), R2>, logic: L) -> Collection<G, D, <R as Mul<R2>>::Output>
    where V2: Data, R: Mul<R2>, <R as Mul<R2>>::Output: Diff, D: Data, L: Fn(&K, &V, &V2)->D+'static;

    /// Matches pairs `(key, val)` and `key` based on `key`, producing the former with frequencies multiplied.
    ///
    /// When the second collection contains frequencies that are either zero or one this is the more traditional
    /// relational semijoin. When the second collection may contain multiplicities, this operation may scale up
    /// the counts of the records in the first input.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![0, 2]).1;
    ///         let z = scope.new_collection_from(vec![(0, 1)]).1;
    ///
    ///         x.semijoin(&y)
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn semijoin<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), <R as Mul<R2>>::Output> 
    where R2: Diff, R: Mul<R2>, <R as Mul<R2>>::Output: Diff;
    /// Matches pairs `(key, val)` and `key` based on `key`, discarding values 
    /// in the first collection if their key is present in the second.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Join;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0, 1), (1, 3)]).1;
    ///         let y = scope.new_collection_from(vec![0, 2]).1;
    ///         let z = scope.new_collection_from(vec![(1, 3)]).1;
    ///
    ///         x.antijoin(&y)
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn antijoin<R2>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), R>
    where R2: Diff, R: Mul<R2, Output = R>;
} 

impl<G, K, V, R> Join<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope, 
    K: Data+Hashable,
    V: Data,
    R: Diff,
    G::Timestamp: Lattice+Ord,
{
    fn join_map<V2: Data, R2: Diff, D: Data, L>(&self, other: &Collection<G, (K, V2), R2>, logic: L) -> Collection<G, D, <R as Mul<R2>>::Output>
    where R: Mul<R2>, <R as Mul<R2>>::Output: Diff, L: Fn(&K, &V, &V2)->D+'static {
        let arranged1 = self.arrange_by_key();
        let arranged2 = other.arrange_by_key();
        arranged1.join_core(&arranged2, move |k,v1,v2| Some(logic(k,v1,v2)))
    }

    fn semijoin<R2: Diff>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), <R as Mul<R2>>::Output> 
    where R: Mul<R2>, <R as Mul<R2>>::Output: Diff {
        let arranged1 = self.arrange_by_key();
        let arranged2 = other.arrange_by_self();
        arranged1.join_core(&arranged2, |k,v,_| Some((k.clone(), v.clone())))
    }

    fn antijoin<R2: Diff>(&self, other: &Collection<G, K, R2>) -> Collection<G, (K, V), R>
    where R: Mul<R2, Output=R> {
        self.concat(&self.semijoin(other).negate())
    }
}

/// Matches the elements of two arranged traces.
///
/// This method is used by the various `join` implementations, but it can also be used 
/// directly in the event that one has a handle to an `Arranged<G,T>`, perhaps because
/// the arrangement is available for re-use, or from the output of a `group` operator.
pub trait JoinCore<G: Scope, K: 'static, V: 'static, R: Diff> where G::Timestamp: Lattice+Ord {
    /// Joins two arranged collections with the same key type.
    ///
    /// Each matching pair of records `(key, val1)` and `(key, val2)` are subjected to the `result` function, 
    /// which produces something implementing `IntoIterator`, where the output collection will have 
    ///
    /// This trait is implemented for arrangements (`Arranged<G, T>`) rather than collections. The `Join` trait 
    /// contains the implementations for collections.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::arrange::Arrange;
    /// use differential_dataflow::operators::join::JoinCore;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::ord::OrdValSpine;
    /// use differential_dataflow::hashable::OrdWrapper;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(vec![(0u32, 1), (1, 3)]).1
    ///                      .map(|(x,y)| (OrdWrapper { item: x }, y))
    ///                      .arrange(OrdValSpine::new());
    ///         let y = scope.new_collection_from(vec![(0, 'a'), (1, 'b')]).1
    ///                      .map(|(x,y)| (OrdWrapper { item: x }, y))
    ///                      .arrange(OrdValSpine::new());
    ///
    ///         let z = scope.new_collection_from(vec![(1, 'a'), (3, 'b')]).1;
    ///
    ///         x.join_core(&y, |_key, &a, &b| Some((a, b)))
    ///          .assert_eq(&z);
    ///     });
    /// }
    /// ```
    fn join_core<V2,T2,R2,I,L> (&self, stream2: &Arranged<G,K,V2,R2,T2>, result: L) -> Collection<G,I::Item,<R as Mul<R2>>::Output>
    where 
        V2: Ord+Clone+Debug+'static,
        T2: TraceReader<K, V2, G::Timestamp, R2>+Clone+'static,
        T2::Batch: BatchReader<K, V2, G::Timestamp, R2>+'static,
        R2: Diff,
        R: Mul<R2>,
        <R as Mul<R2>>::Output: Diff,
        I: IntoIterator,
        I::Item: Data,
        L: Fn(&K,&V,&V2)->I+'static,
        ;
}


impl<G, K, V, R> JoinCore<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope,
    K: Data+Hashable,
    V: Data,
    R: Diff,
    G::Timestamp: Lattice+Ord,
{
    fn join_core<V2,T2,R2,I,L> (&self, stream2: &Arranged<G,K,V2,R2,T2>, result: L) -> Collection<G,I::Item,<R as Mul<R2>>::Output>
    where 
        V2: Ord+Clone+Debug+'static,
        T2: TraceReader<K, V2, G::Timestamp, R2>+Clone+'static,
        T2::Batch: BatchReader<K, V2, G::Timestamp, R2>+'static,
        R2: Diff,
        R: Mul<R2>,
        <R as Mul<R2>>::Output: Diff,
        I: IntoIterator,
        I::Item: Data,
        L: Fn(&K,&V,&V2)->I+'static {

        self.arrange_by_key()
            .join_core(stream2, result)
    }
}

impl<G, K, V, R1, T1> JoinCore<G, K, V, R1> for Arranged<G,K,V,R1,T1> 
    where 
        K: Ord,
        G: Scope, 
        G::Timestamp: Lattice+Ord+Debug,
        K: Debug+Eq+'static, 
        V: Ord+Clone+Debug+'static, 
        R1: Diff,
        T1: TraceReader<K,V,G::Timestamp, R1>+Clone+'static,
        T1::Batch: BatchReader<K,V,G::Timestamp,R1>+'static+Debug {
    fn join_core<V2,T2,R2,I,L>(&self, other: &Arranged<G,K,V2,R2,T2>, result: L) -> Collection<G,I::Item,<R1 as Mul<R2>>::Output> 
    where 
        V2: Ord+Clone+Debug+'static,
        T2: TraceReader<K,V2,G::Timestamp,R2>+Clone+'static,
        T2::Batch: BatchReader<K, V2, G::Timestamp, R2>+'static,
        R2: Diff,
        R1: Mul<R2>,
        <R1 as Mul<R2>>::Output: Diff,
        I: IntoIterator,
        I::Item: Data,
        L: Fn(&K,&V,&V2)->I+'static {

        // handles to shared trace data structures.
        let mut trace1 = Some(self.trace.clone());
        let mut trace2 = Some(other.trace.clone());

        // acknowledged frontier for each input.
        let mut acknowledged1 = vec![G::Timestamp::minimum()];
        let mut acknowledged2 = vec![G::Timestamp::minimum()];

        // deferred work of batches from each input.
        let mut todo1 = Vec::new();
        let mut todo2 = Vec::new();

        self.stream.binary_notify(&other.stream, Pipeline, Pipeline, "Join", vec![], move |input1, input2, output, notificator| {

            // The join computation repeatedly accepts batches of updates from each of its inputs.
            //
            // For each accepted batch, it prepares a work-item to join the batch against previously "accepted"
            // updates from its other input. It is important to track which updates have been accepted, through
            // a combination of the input's frontier and the most recently received batch's upper bound, because
            // we use a shared trace and there may be updates present that are in advance of this accepted bound.

            // drain input 1, prepare work.
            input1.for_each(|capability, data| {
                if let Some(ref mut trace2) = trace2 {
                    for batch1 in data.drain(..) {
                        let (trace2_cursor, trace2_storage) = trace2.cursor_through(&acknowledged2[..]).unwrap();
                        let (batch1_cursor, batch1_storage) = batch1.item.cursor();
                        todo1.push(Deferred::new(trace2_cursor, trace2_storage, batch1_cursor, batch1_storage, capability.clone(), |r2,r1| *r1 * *r2));
                        debug_assert!(batch1.item.description().lower() == &acknowledged1[..]);
                        acknowledged1 = batch1.item.description().upper().to_vec();
                    }
                }
            });

            // drain input 2, prepare work.
            input2.for_each(|capability, data| {
                if let Some(ref mut trace1) = trace1 {
                    for batch2 in data.drain(..) {
                        let (trace1_cursor, trace1_storage) = trace1.cursor_through(&acknowledged1[..]).unwrap();
                        let (batch2_cursor, batch2_storage) = batch2.item.cursor();
                        todo2.push(Deferred::new(trace1_cursor, trace1_storage, batch2_cursor, batch2_storage, capability.clone(), |r1,r2| *r1 * *r2));
                        debug_assert!(batch2.item.description().lower() == &acknowledged2[..]);
                        acknowledged2 = batch2.item.description().upper().to_vec();
                    }
                }
            });

            // An arbitrary number, whose value guides the "responsiveness" of `join`; the operator
            // yields after producing this many records, to allow downstream operators to work and 
            // move the produced records around.
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

            // shut down or advance trace2. if the frontier is empty we can shut it down,
            // and otherwise we can advance the trace by the acknowledged elements of the other input,
            // as we may still use them as thresholds (ie we must preserve `le` wrt `acknowledged`).
            // NOTE: We release capabilities here to allow light work to complete, which may result in 
            //       unique ownership which would enable `advance_mut`.
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

        })
        .as_collection()
    }
}

/// Deferred join computation.
///
/// The structure wraps cursors which allow us to play out join computation at whatever rate we like.
/// This allows us to avoid producing and buffering massive amounts of data, without giving the timely
/// dataflow system a chance to run operators that can consume and aggregate the data.
struct Deferred<K, V1, V2, T, R1, R2, R3, C1, C2, M, D> 
where 
    V1: Ord+Clone,
    V2: Ord+Clone,
    T: Timestamp+Lattice+Ord+Debug, 
    R1: Diff, 
    R2: Diff, 
    C1: Cursor<K, V1, T, R1>,
    C2: Cursor<K, V2, T, R2>,
    M: Fn(&R1,&R2)->R3,
    D: Ord+Clone+Data,
{
    phant: ::std::marker::PhantomData<(K, V1, V2, R1, R2)>,
    trace: C1,
    trace_storage: C1::Storage,
    batch: C2,
    batch_storage: C2::Storage,
    capability: Capability<T>,
    mult: M,
    done: bool,
    temp: Vec<((D, T), R3)>,
    // thinker: JoinThinker<V1, V2, T, R1, R2>,
}

impl<K, V1, V2, T, R1, R2, R3, C1, C2, M, D> Deferred<K, V1, V2, T, R1, R2, R3, C1, C2, M, D>
where
    K: Ord+Debug+Eq,
    V1: Ord+Clone+Debug,
    V2: Ord+Clone+Debug,
    T: Timestamp+Lattice+Ord+Debug,
    R1: Diff, 
    R2: Diff, 
    R3: Diff,
    C1: Cursor<K, V1, T, R1>,
    C2: Cursor<K, V2, T, R2>,
    M: Fn(&R1,&R2)->R3,
    D: Ord+Clone+Data, 
{
    fn new(trace: C1, trace_storage: C1::Storage, batch: C2, batch_storage: C2::Storage, capability: Capability<T>, mult: M) -> Self {
        Deferred {
            phant: ::std::marker::PhantomData,
            trace: trace,
            trace_storage: trace_storage,
            batch: batch,
            batch_storage: batch_storage,
            capability: capability,
            mult: mult,
            done: false,
            temp: Vec::new(),
            // thinker: JoinThinker::new(),
        }
    }

    fn work_remains(&self) -> bool { 
        !self.done
    }

    /// Process keys until at least `limit` output tuples produced, or the work is exhausted.
    #[inline(never)]
    fn work<L, I>(&mut self, output: &mut OutputHandle<T, (D, T, R3), Tee<T, (D, T, R3)>>, logic: &L, fuel: &mut usize) 
    where I: IntoIterator<Item=D>, L: Fn(&K, &V1, &V2)->I {

        let meet = self.capability.time();

        let mut effort = 0;
        let mut session = output.session(&self.capability);

        let trace_storage = &self.trace_storage;
        let batch_storage = &self.batch_storage;

        let trace = &mut self.trace;
        let batch = &mut self.batch;
        let mult = &self.mult;

        let temp = &mut self.temp;
        // let thinker = &mut self.thinker;
        let mut thinker = JoinThinker::new();

        while batch.key_valid(batch_storage) && trace.key_valid(trace_storage) && effort < *fuel {

            match trace.key(trace_storage).cmp(batch.key(batch_storage)) {
                Ordering::Less => trace.seek_key(trace_storage, batch.key(batch_storage)),
                Ordering::Greater => batch.seek_key(batch_storage, trace.key(trace_storage)),
                Ordering::Equal => {

                    thinker.history1.edits.load(trace, trace_storage, |time| time.join(&meet));
                    thinker.history2.edits.load(batch, batch_storage, |time| time.clone());

                    assert_eq!(temp.len(), 0);

                    // populate `temp` with the results in the best way we know how.
                    thinker.think(|v1,v2,t,r1,r2| 
                        for result in logic(batch.key(batch_storage), v1, v2) {
                            temp.push(((result, t.clone()), mult(r1, r2)));
                        }
                    );

                    // TODO: This consolidation is optional, and it may not be very 
                    //       helpful. We might try harder to understand whether we
                    //       should do this work here, or downstream at consumers.
                    // TODO: Perhaps `thinker` should have the buffer, do smarter
                    //       consolidation, and then deposit results in `session`.
                    consolidate(temp, 0);

                    effort += temp.len();
                    for ((d, t), r) in temp.drain(..) {
                        session.give((d, t, r));
                    }

                    batch.step_key(batch_storage);
                    trace.step_key(trace_storage);

                    thinker.history1.clear();
                    thinker.history2.clear();
                }
            }
        }

        self.done = !batch.key_valid(batch_storage) || !trace.key_valid(trace_storage);

        if effort > *fuel { *fuel = 0; }
        else              { *fuel -= effort; }
    }
}

struct JoinThinker<'a, V1: Ord+Clone+'a, V2: Ord+Clone+'a, T: Lattice+Ord+Clone, R1: Diff, R2: Diff> {
    pub history1: ValueHistory2<'a, V1, T, R1>,
    pub history2: ValueHistory2<'a, V2, T, R2>,
}

impl<'a, V1: Ord+Clone, V2: Ord+Clone, T: Lattice+Ord+Clone, R1: Diff, R2: Diff> JoinThinker<'a, V1, V2, T, R1, R2> 
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
        if self.history1.edits.len() < 10 || self.history2.edits.len() < 10 {
            self.history1.edits.map(|v1, t1, d1| {
                self.history2.edits.map(|v2, t2, d2| {
                    results(v1, v2, t1.join(t2), &d1, &d2);
                })
            })
        }
        else {

            self.history1.order();
            self.history2.order();

            // TODO: It seems like there is probably a good deal of redundant `advance_buffer_by`
            //       in here. If a time is ever repeated, for example, the call will be identical
            //       and accomplish nothing. If only a single record has been added, it may not
            //       be worth the time to collapse (advance, re-sort) the data when a linear scan
            //       is sufficient.

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