//! Match pairs of records based on a key.

use std::fmt::Debug;
use std::default::Default;
use std::hash::{Hash, Hasher};
use std::collections::HashMap;
use std::rc::Rc;

use ::Data;
use timely::dataflow::*;
use timely::dataflow::operators::{Map, Binary};
use timely::dataflow::channels::pact::Exchange;
use timely::drain::DrainExt;


use collection::{Trace, LeastUpperBound, Lookup, Offset};

use sort::radix_merge::{Compact};
use radix_sort::{RadixSorter, Unsigned};

/// Join implementations for `(key,val)` data.
pub trait Join<G: Scope, K: Data+Clone, V: Data+Clone> : JoinBy<G, (K,V)> {

    /// Matches pairs of `(key,val1)` and `(key,val2)` records based on `key`.
    fn join<V2>(&self, other: &Stream<G, ((K,V2),i32)>) -> Stream<G, ((K,V,V2),i32)>
    where V2: Data+Clone+Debug+'static,
          G::Timestamp: LeastUpperBound {
        self.join_by_core(other, |x| x, |x| x, |&(ref k,_)| k.hashed(), |&(ref k,_)| k.hashed(), |k| k.hashed(), |k,v1,v2| (k.clone(), v1.clone(), v2.clone()), &|_| HashMap::new())
    }
    /// Matches pairs of `(key,val1)` and `(key,val2)` records based on `key` and applies a reduction function.
    fn join_map<V2, D, R>(&self, other: &Stream<G, ((K,V2),i32)>, logic: R) -> Stream<G, (D,i32)>
    where V2: Data+Clone+Debug+'static,
          D: Data,
          R: Fn(&K, &V, &V2)->D+'static,
          G::Timestamp: LeastUpperBound {
        self.join_by_core(other, |x| x, |x| x, |&(ref k,_)| k.hashed(), |&(ref k,_)| k.hashed(), |k| k.hashed(), logic, &|_| HashMap::new())
    }
}

impl<G: Scope, K: Data+Default, V: Data+Default, S> Join<G, K, V> for S
where G::Timestamp: LeastUpperBound,
      S: JoinBy<G, (K,V)> { }


/// Join implementations for `(unsigned_int, val)` data.
pub trait JoinUnsigned<G: Scope, U: Unsigned+Data+Default, V: Data+Ord+Clone+Debug> : JoinBy<G, (U,V)> {

    /// Matches pairs of `(key, val1)` and `(key, val2)` data based `key`.
    fn join_u<V2>(&self, other: &Stream<G, ((U,V2),i32)>) -> Stream<G, ((U,V,V2),i32)>
    where V2: Data+Clone+Debug+'static,
          G::Timestamp: LeastUpperBound {
        self.join_by_core(other, |x| x, |x| x, |&(ref k,_)| k.as_u64(), |&(ref k,_)| k.as_u64(), |k| k.clone(), |k,v1,v2| (k.clone(), v1.clone(), v2.clone()), &|x| (Vec::new(), x))
    }
    /// Matches pairs of `(key,val1)` and `(key,val2)` records based on `key` and applies a reduction function.
    fn join_map_u<V2, D, R>(&self, other: &Stream<G, ((U,V2),i32)>, logic: R) -> Stream<G, (D,i32)>
    where V2: Data+Clone+Debug+'static,
          D: Data,
          R: Fn(&U, &V, &V2)->D+'static,
          G::Timestamp: LeastUpperBound {
        self.join_by_core(other, |x| x, |x| x, |&(ref k,_)| k.as_u64(), |&(ref k,_)| k.as_u64(), |k| k.clone(), logic, &|x| (Vec::new(), x))
    }

}

impl<G: Scope, U: Unsigned+Data+Default, V: Data+Ord+Default+Debug, S> JoinUnsigned<G, U, V> for S
where G::Timestamp: LeastUpperBound,
      S: JoinBy<G, (U,V)> { }

impl<G: Scope, D1: Data+Ord, S> JoinBy<G, D1> for S
where G::Timestamp: LeastUpperBound,
      S: Binary<G, (D1, i32)>+Map<G, (D1, i32)> { }

/// Join implementations with parameterizable key selector functions.
pub trait JoinBy<G: Scope, D1: Data+Ord> : Binary<G, (D1, i32)>+Map<G, (D1, i32)>
where G::Timestamp: LeastUpperBound {
    /// Matches elements of two streams using unsigned integers as the keys.
    ///
    /// `join_by_u` takes a second input stream, two key-val selector functions, and a reduction
    /// function from an unsigned integer (the key) and two value references to the output type.
    fn join_by_u<
        U:  Unsigned+Data+Default,
        V1: Data+Ord+Clone+Default+Debug+'static,
        V2: Data+Ord+Clone+Default+Debug+'static,
        D2: Data+Eq,
        F1: Fn(D1)->(U,V1)+'static,
        F2: Fn(D2)->(U,V2)+'static,
        R:  Ord+Data,
        RF: Fn(&U,&V1,&V2)->R+'static,
    >
        (&self, other: &Stream<G, (D2, i32)>, kv1: F1, kv2: F2, result: RF) -> Stream<G, (R, i32)> {
        self.map(move |(x,w)| (kv1(x),w))
            .join_by_core(&other.map(move |(x,w)| (kv2(x),w)),
                        |x|x,
                        |x|x,
                        |&(ref k,_)| k.as_u64(),
                        |&(ref k,_)| k.as_u64(),
                        |k| k.clone(),
                        result,
                        &|x| (Vec::new(), x))
    }
    /// Restricts the input stream to those elements whose unsigned integer key is present in the
    /// second stream.
    ///
    /// `semijoin_by_u` takes a second stream, a key-val selector function, and a reconstruction function.
    /// Records are produced in the output if their key matches an unsigned integer present in the
    /// second stream. The key-val selector and reconstruction function are available to help avoid
    /// storing a redundant copy of the key in the value payload.
    fn semijoin_by_u<
        U:  Unsigned+Data+Default,
        V1: Data+Ord+Clone+Default+Debug+'static,
        F1: Fn(D1)->(U,V1)+'static,
        RF: Fn(&U,&V1)->D1+'static,
    >
        (&self, other: &Stream<G, (U, i32)>, kv1: F1, result: RF) -> Stream<G, (D1, i32)> {

        self.join_by_u(&other, kv1, |u| (u, ()), move |x,y,_| result(x,y))
    }

    /// Matches elements of two streams using a key function.
    fn join_by<
        K:  Data+Ord+Clone+Hash+Debug+'static,
        V1: Data+Ord+Clone+Debug+'static,
        V2: Data+Ord+Clone+Debug+'static,
        D2: Data,
        F1: Fn(D1)->(K,V1)+'static,
        F2: Fn(D2)->(K,V2)+'static,
        U:  Unsigned+Data+Default,
        KH: Fn(&K)->U+'static,
        R:  Ord+Data,
        RF: Fn(&K,&V1,&V2)->R+'static,
    >
        (&self, other: &Stream<G, (D2, i32)>,
        kv1: F1, kv2: F2,
        key_h: KH, result: RF)
    -> Stream<G, (R, i32)> {

        let kh1 = Rc::new(key_h);
        let kh2 = kh1.clone();
        let kh3 = kh1.clone();

        self.map(move |(x,w)| (kv1(x),w))
            .join_by_core(
                &other.map(move |(x,w)| (kv2(x),w)),
                |x| x,
                |x| x,
                move |&(ref k,_)| kh1(k).as_u64(),
                move |&(ref k,_)| kh2(k).as_u64(),
                move |k| kh3(k),
                result,
                &|_| HashMap::new())
    }

    /// Restricts the input stream to those elements whose key is present in the second stream.
    fn semijoin_by<
        K:  Data+Ord+Clone+Hash+Debug+'static,
        V1: Data+Ord+Clone+Debug+'static,
        F1: Fn(D1)->(K,V1)+'static,
        KH: Fn(&K)->u64+'static,
        RF: Fn(&K,&V1)->D1+'static,
    >
        (&self, other: &Stream<G, (K, i32)>,
        kv1: F1,
        key_h: KH, result: RF)
    -> Stream<G, (D1, i32)> {
        self.join_by(&other, kv1, |k| (k,()), key_h, move |x,y,_| result(x,y))
    }
    fn join_by_core<
        K:  Ord+Clone+Debug+'static,
        V1: Ord+Clone+Debug+'static,
        V2: Ord+Clone+Debug+'static,
        D2: Data+Eq,
        F1: Fn(D1)->(K,V1)+'static,
        F2: Fn(D2)->(K,V2)+'static,
        H1: Fn(&D1)->u64+'static,
        H2: Fn(&D2)->u64+'static,
        U:  Unsigned+Data+Default,
        KH: Fn(&K)->U+'static,
        R:  Ord+Data,
        RF: Fn(&K,&V1,&V2)->R+'static,
        LC: Lookup<K, Offset>+'static,
        GC: Fn(u64)->LC,
    >
            (&self,
             stream2: &Stream<G, (D2, i32)>,
             kv1: F1,
             kv2: F2,
             part1: H1,
             part2: H2,
             key_h: KH,
             result: RF,
             look:  &GC)  -> Stream<G, (R, i32)> {

        // TODO : pay more attention to the number of peers
        // TODO : find a better trait to sub-trait so we can read .builder
        // assert!(self.builder.peers() == 1);
        let mut trace1 = Some(Trace::new(look(0)));
        let mut trace2 = Some(Trace::new(look(0)));

        let mut inputs1 = Vec::new();    // Vec<(T, Vec<(K, V1, i32)>)>;
        let mut inputs2 = Vec::new();    // Vec<(T, Vec<(K, V2, i32)>)>;

        let mut outbuf = Vec::new();    // Vec<(T, Vec<(R,i32)>)> for buffering output.

        let exch1 = Exchange::new(move |&(ref r, _)| part1(r));
        let exch2 = Exchange::new(move |&(ref r, _)| part2(r));

        self.binary_notify(stream2, exch1, exch2, "Join", vec![], move |input1, input2, output, notificator| {

            // consider shutting down each trace if the opposing input has closed out
            if trace2.is_some() && notificator.frontier(0).len() == 0 && inputs1.len() == 0 { trace2 = None; }
            if trace1.is_some() && notificator.frontier(1).len() == 0 && inputs2.len() == 0 { trace1 = None; }

            // read input 1, push key, (val,wgt) to queues
            while let Some((time, data)) = input1.next() {
                notificator.notify_at(&time);
                inputs1.entry_or_insert(time.clone(), || RadixSorter::new())
                       .extend(data.drain_temp().map(|(d,w)| (kv1(d),w)),  &|x| key_h(&(x.0).0));
            }

            // read input 2, push key, (val,wgt) to queues
            while let Some((time, data)) = input2.next() {
                notificator.notify_at(&time);
                inputs2.entry_or_insert(time.clone(), || RadixSorter::new())
                       .extend(data.drain_temp().map(|(d,w)| (kv2(d),w)),  &|x| key_h(&(x.0).0));
            }

            // check to see if we have inputs to process
            while let Some((time, _count)) = notificator.next() {

                if let Some(mut queue) = inputs1.remove_key(&time) {
                    let radix_sorted = queue.finish(&|x| key_h(&(x.0).0));
                    if let Some(compact) = Compact::from_radix(radix_sorted, &|k| key_h(k)) {
                        if let Some(trace) = trace2.as_ref() {
                            process_diffs(&time, &compact, &trace, &result, &mut outbuf);
                        }

                        if let Some(trace) = trace1.as_mut() {
                            trace.set_difference(time.clone(), compact);
                        }
                    }
                }

                if let Some(mut queue) = inputs2.remove_key(&time) {
                    let radix_sorted = queue.finish(&|x| key_h(&(x.0).0));
                    if let Some(compact) = Compact::from_radix(radix_sorted, &|k| key_h(k)) {
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
                for (time, mut vals) in outbuf.drain_temp() {
                    output.session(&time).give_iterator(vals.drain_temp());
                }
            }
        })
    }
}

fn process_diffs<K, T, V1: Debug, V2, L, R: Ord, RF>(time: &T,
                                         compact: &Compact<K, V1>,
                                         trace: &Trace<K,T,V2,L>,
                                         result: &RF,
                                         outbuf: &mut Vec<(T, Vec<(R,i32)>)>)
where T: Eq+LeastUpperBound+Clone,
      K: Ord+Debug,
      V2: Ord+Debug,
      RF: Fn(&K,&V1,&V2)->R,
      L: Lookup<K, Offset> {

    let func = |&(w,c)| ::std::iter::repeat(w).take(c as usize);
    let mut vals = compact.vals.iter().zip(compact.wgts.iter().flat_map(&func));

    for (key, &cnt) in compact.keys.iter().zip(compact.cnts.iter()) {
        for (t, vals2) in trace.trace(key) {
            let mut output = outbuf.entry_or_insert(time.least_upper_bound(t), || Vec::new());
            for (val, wgt) in vals.clone().take(cnt as usize) {
                for (val2, wgt2) in vals2.clone() {
                    output.push((result(key, val, val2), wgt * wgt2));
                }
            }
        }

        for _ in 0..cnt { vals.next(); }
    }
}
