//! Match pairs of records based on a key.

use std::default::Default;
use std::collections::HashMap;

// <<<<<<< HEAD
use timely::progress::Timestamp;
use timely::dataflow::Scope;
use timely::dataflow::operators::Binary;
use timely::dataflow::channels::pact::Pipeline;
// =======
// use std::ops::DerefMut;

// use ::{Data, Collection};
// use timely::dataflow::Scope;
// use timely::dataflow::operators::{Map, Binary};
// use timely::dataflow::channels::pact::Exchange;
// >>>>>>> master
use timely::drain::DrainExt;

use ::{Data, Collection};
use collection::{LeastUpperBound, Lookup};
use collection::trace::{Traceable,TraceRef};
use radix_sort::{Unsigned};
use operators::arrange::{Arranged, ArrangeByKey, ArrangeBySelf};

/// Join implementations for `(key,val)` data.
///
/// The `Join` trait provides default implementations of `join` for streams of data with
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
    /// use timely::dataflow::operators::{ToStream, Inspect};
    /// use differential_dataflow::operators::Join;
    ///
    /// timely::example(|scope| {
    ///     let col1 = vec![((0,0),1),((1,2),1)].into_iter().to_stream(scope);
    ///     let col2 = vec![((0,'a'),1),((1,'B'),1)].into_iter().to_stream(scope);
    ///
    ///     // should produce triples `(0,0,'a')` and `(1,2,'B')`.
    ///     col1.join(&col2).inspect(|x| println!("observed: {:?}", x));
    /// });
    /// ```
    fn join<V2: Data>(&self, other: &Collection<G, (K,V2)>) -> Collection<G, (K,V,V2)> {
        self.join_map(other, |k,v1,v2| (k.clone(), v1.clone(), v2.clone()))
    }
    fn join_map<V2: Data, D: Data, R: Fn(&K, &V, &V2)->D+'static>(&self, other: &Collection<G, (K,V2)>, logic: R) -> Collection<G, D>;
    fn semijoin(&self, other: &Collection<G, K>) -> Collection<G, (K, V)>;
} 

impl<G: Scope, K: Data, V: Data> Join<G, K, V> for Collection<G, (K, V)> where G::Timestamp: LeastUpperBound {
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
}

/// Matches pairs `(key, val1)` and `(key, val2)` for dense unsigned integer keys.
///
/// These methods are optimizations of the general `Join` trait to use `Vec` indices rather than
/// a more generic hash map. This can substantially reduce the amount of computation and memory
/// required, but it will allocate as much memory as the largest identifier and so may have poor
/// performance if the absolute range of keys is large.
pub trait JoinUnsigned<G: Scope, U: Unsigned+Data+Default, V: Data> where G::Timestamp: LeastUpperBound {
    fn join_u<V2: Data>(&self, other: &Collection<G, (U,V2)>) -> Collection<G, (U,V,V2)> {
        self.join_map_u(other, |k,v1,v2| (k.clone(), v1.clone(), v2.clone()))
    }
    fn join_map_u<V2: Data, D: Data, R: Fn(&U, &V, &V2)->D+'static>(&self, other: &Collection<G, (U,V2)>, logic: R) -> Collection<G, D>;
    fn semijoin(&self, other: &Collection<G, U>) -> Collection<G, (U, V)>;
}

impl<G: Scope, U: Unsigned+Data+Default, V: Data> JoinUnsigned<G, U, V> for Collection<G, (U, V)> where G::Timestamp: LeastUpperBound {
    fn join_map_u<V2: Data, D: Data, R: Fn(&U, &V, &V2)->D+'static>(&self, other: &Collection<G, (U,V2)>, logic: R) -> Collection<G, D> {
        let arranged1 = self.arrange_by_key(|k| k.clone(), |x| (Vec::new(), x));
        let arranged2 = other.arrange_by_key(|k| k.clone(), |x| (Vec::new(), x));
        arranged1.join(&arranged2, logic)
    }
    fn semijoin(&self, other: &Collection<G, U>) -> Collection<G, (U, V)> {
        let arranged1 = self.arrange_by_key(|k| k.clone(), |x| (Vec::new(), x));
        let arranged2 = other.arrange_by_self(|k| k.clone(), |x| (Vec::new(), x));
        arranged1.join(&arranged2, |k,v,_| (k.clone(), v.clone()))        
    }
}

/// Matches the elements of two arranged traces.
///
/// This method is used by the various `join` implementations, but it can also be used 
/// directly in the event that one has a handle to an `Arranged<G,T>`, perhaps because
/// the arrangement is available for re-use, or from the output of a `group` operator.
pub trait JoinArranged<G: Scope, K: Data, V: Data> where G::Timestamp: LeastUpperBound {
    /// Matches the elements of two arranged traces.
    ///
    /// The arrangements must have matching keys and indices, but the values may be arbitrary.
    fn join<T2,R,RF> (&self, stream2: &Arranged<G,T2>, result: RF) -> Collection<G,R>
    where 
        T2: Traceable<Key=K,Index=G::Timestamp>+'static,
        R: Data,
        RF: Fn(&K,&V,&T2::Value)->R+'static,
        for<'a> &'a T2: TraceRef<'a,T2::Key,T2::Index,T2::Value>
        ;
}

impl<TS: Timestamp, G: Scope<Timestamp=TS>, T: Traceable<Index=TS>+'static> JoinArranged<G, T::Key, T::Value> for Arranged<G, T> 
    where 
        G::Timestamp: LeastUpperBound, 
        for<'a> &'a T: TraceRef<'a, T::Key, T::Index, T::Value> {
    fn join<T2,R,RF>(&self, other: &Arranged<G,T2>, result: RF) -> Collection<G,R> 
    where 
        T2: Traceable<Key=T::Key, Index=G::Timestamp>+'static,
        R: Data,
        RF: Fn(&T::Key,&T::Value,&T2::Value)->R+'static,
        for<'a> &'a T2: TraceRef<'a,T2::Key,T2::Index,T2::Value> {

        let mut trace1 = Some(self.trace.clone());
        let mut trace2 = Some(other.trace.clone());

        let mut inputs1 = Vec::new();
        let mut inputs2 = Vec::new();
        let mut outbuf = Vec::new();

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
            while let Some((time, data)) = input1.next() {
                assert!(data.len() == 1);
                notificator.notify_at(&time);
                inputs1.entry_or_insert(time.clone(), || data.drain_temp().next().unwrap());
            }

            // read input 2, push all data to queues
            while let Some((time, data)) = input2.next() {
                assert!(data.len() == 1);
                notificator.notify_at(&time);
                inputs2.entry_or_insert(time.clone(), || data.drain_temp().next().unwrap());
            }

            // Notification means we have inputs to process or outputs to send.
            while let Some((time, _count)) = notificator.next() {

                // We must be careful to only respond to pairs of differences at `time` with
                // one output record, not two. To do this correctly, we acknowledge the time
                // after processing the first input, so that it is as if we added the inputs 
                // to the traces in this order.

                // compare fresh data on the first input against stale data on the second
                if let Some(ref trace) = trace2 {
                    if let Some((keys, cnts, vals)) = inputs1.remove_key(&time) {
                        let mut vals = vals.iter();
                        for (key, &cnt) in keys.iter().zip(cnts.iter()) {
                            let borrow = trace.borrow();
                            for (t, diffs) in borrow.trace(key) {
                                if acknowledged.iter().any(|t2| t <= t2) {
                                    let mut output = outbuf.entry_or_insert(time.least_upper_bound(t), || Vec::new());
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
                                    let mut output = outbuf.entry_or_insert(time.least_upper_bound(t), || Vec::new());
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
                if let Some(mut buffer) = outbuf.remove_key(&time) {
                    output.session(&time).give_iterator(buffer.drain_temp());
                }

                // make sure we hold capabilities for each time still to send at.
                for &(ref time, _) in &outbuf {
                    notificator.notify_at(time);
                }
            }
        });

        Collection::new(result)
    }
}
