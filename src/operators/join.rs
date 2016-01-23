//! Match pairs of records based on a key.

use std::fmt::Debug;
use std::default::Default;
use std::hash::Hasher;
use std::collections::HashMap;
use std::rc::Rc;

use std::ops::DerefMut;

use ::{Data, Collection};
use timely::dataflow::Scope;
use timely::dataflow::operators::{Map, Binary};
use timely::dataflow::channels::pact::Exchange;

use timely_communication::Allocate;

use collection::{Trace, LeastUpperBound, Lookup, Offset};
use collection::compact::Compact;
use collection::robin_hood::RHHMap;
use timely_sort::{LSBRadixSorter, Unsigned};

/// Join implementations for `(key,val)` data.
///
/// The `Join` trait provides default implementations of `join` for streams of data with
pub trait Join<G: Scope, K: Data, V: Data> : JoinBy<G, (K,V)> where G::Timestamp: LeastUpperBound {

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
        self.join_by_core::<K, V, V2, (K,V2),_,_,_,_,_,_,_,_,_,_>(
            other,
            |x: (K,V)| x,
            |x| x,
            |&(ref k,_)| k.hashed(),
            |&(ref k,_)| k.hashed(),
            |k| k.hashed(),
            |k,v1,v2| (k.clone(), v1.clone(), v2.clone()),
            // &|_| HashMap::new(),
            &|_| RHHMap::new(|x: &K| x.hashed() as usize)
        )
    }
    /// Matches pairs of `(key,val1)` and `(key,val2)` records based on `key` and applies a reduction function.
    fn join_map<V2: Data, D: Data, R>(&self, other: &Collection<G, (K, V2)>, logic: R) -> Collection<G, D>
    where R: Fn(&K, &V, &V2)->D+'static {
        self.join_by_core(other, |x| x, |x| x, |&(ref k,_)| k.hashed(), |&(ref k,_)| k.hashed(), |k| k.hashed(), logic, &|_| HashMap::new())
    }
}

impl<G: Scope, K: Data+Default, V: Data+Default, S> Join<G, K, V> for S
where G::Timestamp: LeastUpperBound, S: JoinBy<G, (K,V)> { }


/// Join implementations for `(unsigned_int, val)` data.
pub trait JoinUnsigned<G: Scope, U: Unsigned+Data+Default, V: Data> : JoinBy<G, (U,V)> where G::Timestamp: LeastUpperBound {

    /// Matches pairs of `(key, val1)` and `(key, val2)` data based `key`.
    fn join_u<V2>(&self, other: &Collection<G, (U, V2)>) -> Collection<G, (U, V, V2)>
    where V2: Data,
          G::Timestamp: LeastUpperBound+Debug {
        self.join_by_core(
            other, |x| x, |x| x,
            |&(ref k,_)| k.as_u64(),
            |&(ref k,_)| k.as_u64(),
            |k| k.clone(),
            |k,v1,v2| (k.clone(), v1.clone(), v2.clone()),
            &|x| (Vec::new(), x))
    }
    /// Matches pairs of `(key,val1)` and `(key,val2)` records based on `key` and applies a reduction function.
    fn join_map_u<V2, D, R>(&self, other: &Collection<G, (U, V2)>, logic: R) -> Collection<G, D>
    where V2: Data,
          D: Data,
          R: Fn(&U, &V, &V2)->D+'static,
          G::Timestamp: LeastUpperBound+Debug {
        self.join_by_core(other, |x| x, |x| x, |&(ref k,_)| k.as_u64(), |&(ref k,_)| k.as_u64(), |k| k.clone(), logic, &|x| (Vec::new(), x))
    }

}

impl<G: Scope, U: Unsigned+Data+Default, V: Data, S> JoinUnsigned<G, U, V> for S
where G::Timestamp: LeastUpperBound,
      S: JoinBy<G, (U,V)> { }

impl<G: Scope, D1: Data+Ord> JoinBy<G, D1> for Collection<G, D1>
where G::Timestamp: LeastUpperBound {
    fn join_by_u<U, V1, V2, D2, F1, F2, R, RF>

        (&self, other: &Collection<G, D2>, kv1: F1, kv2: F2, result: RF) -> Collection<G, R>

        where
            U:  Unsigned+Data+Default,
            V1: Data,
            V2: Data,
            D2: Data,
            F1: Fn(D1)->(U,V1)+'static,
            F2: Fn(D2)->(U,V2)+'static,
            R:  Data,
            RF: Fn(&U,&V1,&V2)->R+'static, {

        self.map(kv1)
            .join_by_core(&other.map(kv2),
                        |x|x,
                        |x|x,
                        |&(ref k,_)| k.as_u64(),
                        |&(ref k,_)| k.as_u64(),
                        |k| k.clone(),
                        result,
                        &|x| (Vec::new(), x))
    }

    fn join_by<
        K:  Data,
        V1: Data,
        V2: Data,
        D2: Data,
        F1: Fn(D1)->(K,V1)+'static,
        F2: Fn(D2)->(K,V2)+'static,
        U:  Unsigned+Data+Default,
        KH: Fn(&K)->U+'static,
        R:  Data,
        RF: Fn(&K,&V1,&V2)->R+'static,
    >
        (&self, other: &Collection<G, D2>,
        kv1: F1, kv2: F2,
        key_h: KH, result: RF)
    -> Collection<G, R> {

        let kh1 = Rc::new(key_h);
        let kh2 = kh1.clone();
        let kh3 = kh1.clone();

        self.map(kv1)
            .join_by_core(
                &other.map(kv2),
                |x| x,
                |x| x,
                move |&(ref k,_)| kh1(k).as_u64(),
                move |&(ref k,_)| kh2(k).as_u64(),
                move |k| kh3(k),
                result,
                // &|_| HashMap::new()
                &|_| RHHMap::new(|x: &K| x.hashed() as usize)
            )
    }
}

/// Join implementations with parameterizable key selector functions.
pub trait JoinBy<G: Scope, D1: Data> : JoinByCore<G, D1> where G::Timestamp: LeastUpperBound {
    /// Matches elements of two streams using unsigned integers as the keys.
    ///
    /// `join_by_u` takes a second input stream, two key-val selector functions, and a reduction
    /// function from an unsigned integer (the key) and two value references to the output type.
    fn join_by_u<U, V1, V2, D2, F1, F2, R, RF>

        (&self, other: &Collection<G, D2>, kv1: F1, kv2: F2, result: RF) -> Collection<G, R>

        where
            U:  Unsigned+Data+Default,
            V1: Data,
            V2: Data,
            D2: Data,
            F1: Fn(D1)->(U,V1)+'static,
            F2: Fn(D2)->(U,V2)+'static,
            R:  Data,
            RF: Fn(&U,&V1,&V2)->R+'static;

    /// Restricts the input stream to those elements whose unsigned integer key is present in the
    /// second stream.
    ///
    /// `semijoin_by_u` takes a second stream, a key-val selector function, and a reconstruction function.
    /// Records are produced in the output if their key matches an unsigned integer present in the
    /// second stream. The key-val selector and reconstruction function are available to help avoid
    /// storing a redundant copy of the key in the value payload.
    fn semijoin_by_u<
        U:  Unsigned+Data+Default,
        V1: Data+Default+'static,
        F1: Fn(D1)->(U,V1)+'static,
        RF: Fn(&U,&V1)->D1+'static,
    >
        (&self, other: &Collection<G, U>, kv1: F1, result: RF) -> Collection<G, D1> {

        self.join_by_u(&other, kv1, |u| (u, ()), move |x,y,_| result(x,y))
    }

    /// Matches elements of two streams using a key function.
    fn join_by<
        K:  Data,
        V1: Data,
        V2: Data,
        D2: Data,
        F1: Fn(D1)->(K,V1)+'static,
        F2: Fn(D2)->(K,V2)+'static,
        U:  Unsigned+Data+Default,
        KH: Fn(&K)->U+'static,
        R:  Data,
        RF: Fn(&K,&V1,&V2)->R+'static,
    >
        (&self, other: &Collection<G, D2>,
        kv1: F1, kv2: F2,
        key_h: KH, result: RF)
    -> Collection<G, R>;

    /// Restricts the input stream to those elements whose key is present in the second stream.
    fn semijoin_by<
        K:  Data,
        V1: Data,
        F1: Fn(D1)->(K,V1)+'static,
        KH: Fn(&K)->u64+'static,
        RF: Fn(&K,&V1)->D1+'static,
    >
        (&self, other: &Collection<G, K>,
        kv1: F1,
        key_h: KH, result: RF)
    -> Collection<G, D1> {
        self.join_by(&other, kv1, |k| (k,()), key_h, move |x,y,_| result(x,y))
    }
}

pub trait JoinByCore<G: Scope, D1: Data> {
    fn join_by_core<
        K:  Data,
        V1: Data,
        V2: Data,
        D2: Data,
        F1: Fn(D1)->(K,V1)+'static,
        F2: Fn(D2)->(K,V2)+'static,
        H1: Fn(&D1)->u64+'static,
        H2: Fn(&D2)->u64+'static,
        U:  Unsigned+Data+Default,
        KH: Fn(&K)->U+'static,
        R:  Data,
        RF: Fn(&K,&V1,&V2)->R+'static,
        LC: Lookup<K, Offset>+'static,
        GC: Fn(u64)->LC,
    >
            (&self,
             stream2: &Collection<G, D2>,
             kv1: F1,
             kv2: F2,
             part1: H1,
             part2: H2,
             key_h: KH,
             result: RF,
             look:  &GC)  -> Collection<G, R>;
}

impl<G: Scope, D1: Data> JoinByCore<G, D1> for Collection<G, D1> where G::Timestamp: LeastUpperBound {
    fn join_by_core<
        K:  Data,
        V1: Data,
        V2: Data,
        D2: Data,
        F1: Fn(D1)->(K,V1)+'static,
        F2: Fn(D2)->(K,V2)+'static,
        H1: Fn(&D1)->u64+'static,
        H2: Fn(&D2)->u64+'static,
        U:  Unsigned+Data+Default,
        KH: Fn(&K)->U+'static,
        R:  Data,
        RF: Fn(&K,&V1,&V2)->R+'static,
        LC: Lookup<K, Offset>+'static,
        GC: Fn(u64)->LC,
    >
            (&self,
             stream2: &Collection<G, D2>,
             kv1: F1,
             kv2: F2,
             part1: H1,
             part2: H2,
             key_h: KH,
             result: RF,
             look:  &GC)  -> Collection<G, R> {

        // TODO : pay more attention to the number of peers
        // TODO : find a better trait to sub-trait so we can read .builder
        // assert!(self.builder.peers() == 1);
        let peers = self.inner.scope().peers();
        let mut log_peers = 0;
        while (1 << (log_peers + 1)) <= peers {
            log_peers += 1;
        }


        let mut trace1 = Some(Trace::new(look(log_peers)));
        let mut trace2 = Some(Trace::new(look(log_peers)));

        let mut inputs1 = Vec::new();    // Vec<(T, Vec<(K, V1, i32)>)>;
        let mut inputs2 = Vec::new();    // Vec<(T, Vec<(K, V2, i32)>)>;

        let mut outbuf = Vec::new();    // Vec<(T, Vec<(R,i32)>)> for buffering output.

        let exch1 = Exchange::new(move |&(ref r, _)| part1(r));
        let exch2 = Exchange::new(move |&(ref r, _)| part2(r));

        let mut sorter1 = LSBRadixSorter::new();
        let mut sorter2 = LSBRadixSorter::new();

        Collection::new(self.inner.binary_notify(&stream2.inner, exch1, exch2, "Join", vec![], move |input1, input2, output, notificator| {

            // consider shutting down each trace if the opposing input has closed out
            if trace2.is_some() && notificator.frontier(0).len() == 0 && inputs1.len() == 0 { trace2 = None; }
            if trace1.is_some() && notificator.frontier(1).len() == 0 && inputs2.len() == 0 { trace1 = None; }

            // read input 1, push key, (val,wgt) to queues
            while let Some((time, data)) = input1.next() {
                notificator.notify_at(&time);
                inputs1.entry_or_insert(time.clone(), || Vec::new())
                       .push(::std::mem::replace(data.deref_mut(), Vec::new()));
            }

            // read input 2, push key, (val,wgt) to queues
            while let Some((time, data)) = input2.next() {
                notificator.notify_at(&time);
                inputs2.entry_or_insert(time.clone(), || Vec::new())
                       .push(::std::mem::replace(data.deref_mut(), Vec::new()));
            }

            // check to see if we have inputs to process
            while let Some((time, _count)) = notificator.next() {

                if let Some(mut queue) = inputs1.remove_key(&time) {

                    // sort things; radix if many, .sort_by if few.
                    let compact = if queue.len() > 1 {
                        for element in queue.into_iter() {
                            sorter1.extend(element.into_iter().map(|(d,w)| (kv1(d),w)), &|x| key_h(&(x.0).0));
                        }
                        let mut sorted = sorter1.finish(&|x| key_h(&(x.0).0));
                        let result = Compact::from_radix(&mut sorted, &|k| key_h(k));
                        sorted.truncate(256);
                        sorter1.recycle(sorted);
                        result
                    }
                    else {
                        let mut vec = queue.pop().unwrap();
                        let mut vec = vec.drain(..).map(|(d,w)| (kv1(d),w)).collect::<Vec<_>>();

                        vec.sort_by(|x,y| key_h(&(x.0).0).cmp(&key_h((&(y.0).0))));
                        Compact::from_radix(&mut vec![vec], &|k| key_h(k))
                    };

                    if let Some(compact) = compact {
                        if let Some(trace) = trace2.as_ref() {
                            process_diffs(&time, &compact, &trace, &result, &mut outbuf);
                        }

                        if let Some(trace) = trace1.as_mut() {
                            trace.set_difference(time.clone(), compact);
                        }
                    }
                }

                if let Some(mut queue) = inputs2.remove_key(&time) {

                    // sort things; radix if many, .sort_by if few.
                    let compact = if queue.len() > 1 {
                        for element in queue.into_iter() {
                            sorter2.extend(element.into_iter().map(|(d,w)| (kv2(d),w)), &|x| key_h(&(x.0).0));
                        }
                        let mut sorted = sorter2.finish(&|x| key_h(&(x.0).0));
                        let result = Compact::from_radix(&mut sorted, &|k| key_h(k));
                        sorted.truncate(256);
                        sorter2.recycle(sorted);
                        result
                    }
                    else {
                        let mut vec = queue.pop().unwrap();
                        let mut vec = vec.drain(..).map(|(d,w)| (kv2(d),w)).collect::<Vec<_>>();
                        vec.sort_by(|x,y| key_h(&(x.0).0).cmp(&key_h((&(y.0).0))));
                        Compact::from_radix(&mut vec![vec], &|k| key_h(k))
                    };

                    if let Some(compact) = compact {
                        if let Some(trace) = trace1.as_ref() {
                            process_diffs(&time, &compact, &trace, &|k,x,y| result(k,y,x), &mut outbuf);
                        }
                        if let Some(trace) = trace2.as_mut() {
                            trace.set_difference(time.clone(), compact);
                        }
                    }
                }

                // TODO : Sending data at future times may be sub-optimal.
                // TODO : We could sit on the data, accumulating until `time` is notified.
                // TODO : It might be polite to coalesce the data before sending, in this case.
                // Unlike other data, these accumulations are not by-key, because we don't yet know
                // what the downstream keys will be (nor will there be only one). So, per-record
                // accumulation, which can be weird (no hash function, so sorting would be "slow").
                // Related: it may not be safe to sit on lots of data; downstream the data are
                // partitioned and may be coalesced, but no such guarantee here; may overflow mem.

                if let Some(mut buffer) = outbuf.remove_key(&time) {
                    output.session(&time).give_iterator(buffer.drain(..));
                }

                for &(ref time, _) in &outbuf {
                    notificator.notify_at(time);
                }

                // for (time, mut vals) in outbuf.drain(..) {
                //     output.session(&time).give_iterator(vals.drain(..));
                // }
            }
        }))
    }
}

fn process_diffs<K, T, V1: Debug, V2, L, R: Ord, RF>(time: &T,
                                         compact: &Compact<K, V1>,
                                         trace: &Trace<K,T,V2,L>,
                                         result: &RF,
                                         outbuf: &mut Vec<(T, Vec<(R,i32)>)>)
where T: Eq+LeastUpperBound+Clone+Debug,
      K: Ord+Debug,
      V2: Ord+Debug,
      RF: Fn(&K,&V1,&V2)->R,
      L: Lookup<K, Offset> {

    let mut vals = compact.vals.iter();

    for (key, &cnt) in compact.keys.iter().zip(compact.cnts.iter()) {
        for (t, vals2) in trace.trace(key) {
            let mut output = outbuf.entry_or_insert(time.least_upper_bound(t), || Vec::new());
            for &(ref val, wgt) in vals.clone().take(cnt as usize) {
                for (val2, wgt2) in vals2.clone() {
                    output.push((result(key, val, val2), wgt * wgt2));
                }
            }
        }

        for _ in 0..cnt { vals.next(); }
    }
}
