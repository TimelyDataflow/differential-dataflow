//! Match pairs of records based on a key.

use std::fmt::Debug;

use timely::progress::Timestamp;
use timely::dataflow::Scope;
use timely::dataflow::operators::Binary;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely_sort::Unsigned;

use timely::dataflow::operators::OutputHandle;
use timely::dataflow::channels::pushers::tee::Tee;


use hashable::{Hashable, UnsignedWrapper};
use ::{Data, Ring, Collection, AsCollection};
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
        V2: Ord+Clone+Debug+'static,
        T2: Trace<K, V2, G::Timestamp, R>+'static,
        T2::Batch: 'static,
        D: Data,
        L: Fn(&K,&V,&V2)->D+'static;
}

impl<G, K, V, R, T1> JoinArranged<G, K, V, R> for Arranged<G,K,V,R,T1> 
    where 
        G: Scope, 
        K: Debug+Eq+'static, 
        V: Ord+Clone+Debug+'static, 
        R: Ring,
        T1: Trace<K,V,G::Timestamp, R>+'static,
        G::Timestamp: Lattice+Ord+Debug+Copy,
        T1::Batch: 'static+Debug {
    fn join_arranged<V2,T2,D,L>(&self, other: &Arranged<G,K,V2,R,T2>, result: L) -> Collection<G,D,R> 
    where 
        V2: Ord+Clone+Debug+'static,
        T2: Trace<K,V2,G::Timestamp,R>+'static,
        T2::Batch: 'static,
        D: Data,
        L: Fn(&K,&V,&V2)->D+'static {

        // handles to shared trace data structures.
        let mut trace1 = Some(self.new_handle());
        let mut trace2 = Some(other.new_handle());

        // acknowledged frontier for each input.
        let mut acknowledged1 = vec![G::Timestamp::min()];
        let mut acknowledged2 = vec![G::Timestamp::min()];

        // deferred work of batches from each input.
        let mut todo1: Vec<Deferred<K,V2,V,G::Timestamp,R,T2::Cursor,<T1::Batch as Batch<K,V,G::Timestamp,R>>::Cursor>> = Vec::new();
        let mut todo2: Vec<Deferred<K,V,V2,G::Timestamp,R,T1::Cursor,<T2::Batch as Batch<K,V2,G::Timestamp,R>>::Cursor>> = Vec::new();

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
                        todo1.push(Deferred::new(trace2.cursor(), batch1.item.cursor(), capability.clone(), acknowledged2.clone()));
                        debug_assert!(batch1.item.description().upper().iter().all(|t| acknowledged1.iter().any(|t2| t2.le(t))));
                        acknowledged1 = batch1.item.description().upper().to_vec();
                    }
                }
            });

            // drain input 2, prepare work.
            input2.for_each(|capability, data| {
                if let Some(ref trace1) = trace1 {
                    for batch2 in data.drain(..) {
                        todo2.push(Deferred::new(trace1.cursor(), batch2.item.cursor(), capability.clone(), acknowledged1.clone()));
                        debug_assert!(batch2.item.description().upper().iter().all(|t| acknowledged2.iter().any(|t2| t2.le(t))));
                        acknowledged2 = batch2.item.description().upper().to_vec();
                    }
                }
            });

            // shut down or advance trace2. if the frontier is empty we can shut it down,
            // and otherwise we can advance the trace by the acknowledged elements of the other input,
            // as we may still use them as thresholds (ie we must preserve `le` wrt `acknowledged`).
            if trace2.is_some() && notificator.frontier(0).len() == 0 { trace2 = None; }
            if let Some(ref mut trace2) = trace2 {
                let mut frontier = acknowledged2.clone();
                for time in notificator.frontier(0) {
                    frontier.retain(|t| !time.lt(t));
                    if !frontier.iter().any(|t| t.le(time)) { frontier.push(time.clone()); }
                }
                trace2.advance_by(&frontier[..]);
            }

            // shut down or advance trace1.
            if trace1.is_some() && notificator.frontier(1).len() == 0 { trace1 = None; }
            if let Some(ref mut trace1) = trace1 {
                let mut frontier = acknowledged1.clone();
                for time in notificator.frontier(1) {
                    frontier.retain(|t| !time.lt(t));
                    if !frontier.iter().any(|t| t.le(time)) { frontier.push(time.clone()); }
                }
                trace1.advance_by(&frontier[..]);
            }

            // perform some amount of outstanding work. 
            if todo1.len() > 0 {
                todo1[0].work(output, &|k,v2,v1| result(k,v1,v2), 1_000_000);
                if !todo1[0].work_remains() { todo1.remove(0); }
            }

            // perform some amount of outstanding work. 
            if todo2.len() > 0 {
                todo2[0].work(output, &|k,v1,v2| result(k,v1,v2), 1_000_000);
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
struct Deferred<K, V1, V2, T, R, C1, C2> 
where 
    V1: Ord+Clone,
    V2: Ord+Clone,
    T: Timestamp+Lattice+Ord+Debug, 
    C1: Cursor<K, V1, T, R>,
    C2: Cursor<K, V2, T, R>,
{
    phant: ::std::marker::PhantomData<(K, V1, V2, R)>,
    trace: C1,
    batch: C2,
    capability: Capability<T>,
    acknowledged: Vec<T>,
}

impl<K, V1, V2, T, R, C1, C2> Deferred<K, V1, V2, T, R, C1, C2>
where
    K: Debug+Eq,
    V1: Ord+Clone+Debug,
    V2: Ord+Clone+Debug,
    T: Timestamp+Lattice+Ord+Debug+Copy,
    R: Ring, 
    C1: Cursor<K, V1, T, R>,
    C2: Cursor<K, V2, T, R>,
{
    fn new(trace: C1, batch: C2, capability: Capability<T>, acknowledged: Vec<T>) -> Self {
        Deferred {
            phant: ::std::marker::PhantomData,
            trace: trace,
            batch: batch,
            capability: capability,
            acknowledged: acknowledged,
        }
    }

    fn work_remains(&self) -> bool { 
        self.batch.key_valid()
    }

    /// Process keys until at least `limit` output tuples produced, or the work is exhausted.
    #[inline(never)]
    fn _work<D, L>(&mut self, output: &mut OutputHandle<T, (D, T, R), Tee<T, (D, T, R)>>, logic: &L, limit: usize) 
    where D: Ord+Clone+Data, L: Fn(&K, &V1, &V2)->D {

        let acknowledged = &self.acknowledged;
        let time = self.capability.time();

        let mut effort = 0;
        let mut session = output.session(&self.capability);

        let trace = &mut self.trace;
        let batch = &mut self.batch;

        // TODO: This implementation can be quadratic in the input for each key, despite producing a linear sized
        // output. We can change the implementation to process times in-order, which can reduce this particular
        // worst-case performance (though perhaps maintaining collections over time is also expensive).
 
        let mut temp = Vec::new();
        let mut temp1 = Vec::new();
        let mut temp2 = Vec::new();

        while batch.key_valid() && effort < limit {
            trace.seek_key(batch.key());
            if trace.key_valid() && trace.key() == batch.key() {
                while trace.val_valid() {
                    while batch.val_valid() {

                        // we now want to compute the cross-product of times and diffs, 
                        // but this could take quadratic work if done badly. do better!

                        let r = logic(batch.key(), trace.val(), batch.val());
                        trace.map_times(|time1, diff1|
                            if !acknowledged.iter().any(|t| t <= time1) {
                                temp1.push((time.join(time1), diff1));
                            }
                        );
                        batch.map_times(|time2, diff2| temp2.push((time2.clone(), diff2)));

                        consolidate(&mut temp1, 0);
                        consolidate(&mut temp2, 0);

                        for &(ref time1, diff1) in &temp1[..] {
                            for &(ref time2, diff2) in &temp2[..] {
                                temp.push((time1.join(time2), diff1 * diff2));
                            }
                        }

                        temp1.clear();
                        temp2.clear();

                        consolidate(&mut temp, 0);

                        effort += temp.len();
                        for (t, d) in temp.drain(..) {
                            session.give((r.clone(), t, d));
                        }
                        batch.step_val();
                    }

                    batch.rewind_vals();
                    trace.step_val();
                }
            }
            batch.step_key();
        }
    }


    /// Process keys until at least `limit` output tuples produced, or the work is exhausted.
    #[inline(never)]
    fn work<D, L>(&mut self, output: &mut OutputHandle<T, (D, T, R), Tee<T, (D, T, R)>>, logic: &L, limit: usize) 
    where D: Ord+Clone+Data, L: Fn(&K, &V1, &V2)->D {

        let acknowledged = &self.acknowledged;
        let time = self.capability.time();

        let mut effort = 0;
        let mut session = output.session(&self.capability);

        let trace = &mut self.trace;
        let batch = &mut self.batch;

        // TODO: This implementation can be quadratic in the input for each key, despite producing a linear sized
        // output. We can change the implementation to process times in-order, which can reduce this particular
        // worst-case performance (though perhaps maintaining collections over time is also expensive).
 
        let mut temp = Vec::new();
        let mut thinker = JoinThinker::<V1, V2, T, R>::new();

        while batch.key_valid() && effort < limit {
            trace.seek_key(batch.key());
            if trace.key_valid() && trace.key() == batch.key() {

                // exfiltrate trace history.
                thinker.history1.clear();
                while trace.val_valid() {
                    let val: V1 = trace.val().clone();
                    trace.map_times(|time1, diff1| 
                        if !acknowledged.iter().any(|t| t <= time1) {
                            thinker.history1.push(val.clone(), time.join(time1), diff1);
                        }
                    );
                    trace.step_val();
                }

                // exfiltrate batch history.
                thinker.history2.clear();
                while batch.val_valid() {
                    let val: V2 = batch.val().clone();
                    batch.map_times(|time2, diff2| {
                        thinker.history2.push(val.clone(), time2.clone(), diff2);
                    });
                    batch.step_val();
                }

                // populate `temp` with the results in the best way we know how.
                thinker.think(|v1,v2,t,r| temp.push(((logic(batch.key(), v1, v2), t), r)));
                consolidate(&mut temp, 0);
                effort += temp.len();
                for ((d, t), r) in temp.drain(..) {
                    session.give((d, t, r));
                }

            }
            batch.step_key();
        }
    }
}

struct JoinThinker<V1: Ord+Clone, V2: Ord+Clone, T: Lattice+Ord+Clone, R: Ring> {
    pub history1: ValueHistory<V1, T, R>,
    pub history2: ValueHistory<V2, T, R>,
}

impl<V1: Ord+Clone, V2: Ord+Clone, T: Lattice+Ord+Clone, R: Ring> JoinThinker<V1, V2, T, R> {
    fn new() -> Self {
        JoinThinker {
            history1: ValueHistory::new(),
            history2: ValueHistory::new(),
        }
    }

    fn think<F: FnMut(&V1,&V2,T,R)>(&mut self, mut results: F) {

        // for reasonably sized edits, do the dead-simple thing.
        if self.history1.edits.len() < 10 || self.history2.edits.len() < 10 {
            for &((ref time1, ref val1), diff1) in &self.history1.edits[..] {
                for &((ref time2, ref val2), diff2) in &self.history2.edits[..] {
                    results(val1, val2, time1.join(time2), diff1 * diff2);
                }
            }
        }
        else {

            self.history1.order();
            self.history2.order();

            while self.history1.not_done() && self.history2.not_done() {

                if self.history1.time().cmp(&self.history2.time()) == ::std::cmp::Ordering::Less {
                    self.history2.advance_buffer_by(&self.history1.frontier[..]);
                    for &((ref time2, ref val2), diff2) in &self.history2.buffer {
                        let &((ref time1, ref val1), diff1) = self.history1.edit();
                        results(val1, val2, time1.join(time2), diff1 * diff2);
                    }
                    self.history1.step();
                }
                else {
                    self.history1.advance_buffer_by(&self.history2.frontier[..]);
                    for &((ref time1, ref val1), diff1) in &self.history1.buffer {
                        let &((ref time2, ref val2), diff2) = self.history2.edit();
                        results(val1, val2, time1.join(time2), diff1 * diff2);
                    }
                    self.history2.step();
                }
            }

            while self.history1.not_done() {
                self.history2.advance_buffer_by(&self.history1.frontier[..]);
                for &((ref time2, ref val2), diff2) in &self.history2.buffer {
                    let &((ref time1, ref val1), diff1) = self.history1.edit();
                    results(val1, val2, time1.join(time2), diff1 * diff2);
                }
                self.history1.step();                
            }
            while self.history2.not_done() {
                self.history1.advance_buffer_by(&self.history2.frontier[..]);
                for &((ref time1, ref val1), diff1) in &self.history1.buffer {
                    let &((ref time2, ref val2), diff2) = self.history2.edit();
                    results(val1, val2, time1.join(time2), diff1 * diff2);
                }
                self.history2.step();                
            }

        }
    }
}

struct ValueHistory<V: Ord+Clone, T: Lattice+Ord+Clone, R: Ring> {
    edits: Vec<((T, V), R)>,
    buffer: Vec<((T, V), R)>,
    frontier: Vec<T>,
    entrance: Vec<(T, usize)>,
    cursor: usize,
}

impl<V: Ord+Clone, T: Lattice+Ord+Clone, R: Ring> ValueHistory<V, T, R> {
    fn new() -> Self {
        ValueHistory {
            edits: Vec::new(),
            buffer: Vec::new(),
            frontier: Vec::new(),
            entrance: Vec::new(),
            cursor: 0,
        }
    }
    fn clear(&mut self) {
        self.edits.clear();
        self.buffer.clear();
        self.frontier.clear();
        self.entrance.clear();
    }
    fn push(&mut self, val: V, time: T, ring: R) {
        self.edits.push(((time, val), ring));
    }
    fn order(&mut self) {
        
        consolidate(&mut self.edits, 0);

        self.buffer.clear();
        self.cursor = 0;
        self.frontier.clear();
        self.entrance.clear();

        self.entrance.reserve(self.edits.len());
        let mut position = self.edits.len();
        while position > 0 {
            position -= 1;
            // "add" edits[position] and seeing who drops == who is exposed when edits[position] removed.
            let mut index = 0;
            while index < self.frontier.len() {
                if (self.edits[position].0).0.le(&self.frontier[index]) {
                    self.entrance.push((self.frontier.swap_remove(index), position));
                }
                else {
                    index += 1;
                }
            }
            self.frontier.push((self.edits[position].0).0.clone());
        }
    }

    fn advance_buffer_by(&mut self, frontier: &[T]) {
        for &mut ((ref mut time, _), _) in &mut self.buffer {
            *time = time.advance_by(frontier).unwrap();
        }
        consolidate(&mut self.buffer, 0);
    }

    fn time(&self) -> &T { &(self.edits[self.cursor].0).0 }
    fn edit(&self) -> &((T, V), R) { &self.edits[self.cursor] }
    fn step(&mut self) { 

        // a. remove time from frontier; it's not there any more.
        let new_time = &(self.edits[self.cursor].0).0;
        self.frontier.retain(|x| !x.eq(new_time));
        // b. add any indicated elements
        while self.entrance.last().map(|x| x.1) == Some(self.cursor) {
            self.frontier.push(self.entrance.pop().unwrap().0);
        }

        self.buffer.push(self.edits[self.cursor].clone());
        self.cursor += 1; 

    }
    fn not_done(&self) -> bool { self.cursor < self.edits.len() }
}