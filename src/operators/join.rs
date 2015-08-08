use std::fmt::Debug;
use std::default::Default;
use std::hash::Hash;
use std::collections::HashMap;
use std::rc::Rc;

use timely::Data;
use timely::dataflow::*;
use timely::dataflow::operators::{Map, Binary};
use timely::dataflow::channels::pact::Exchange;

use collection_trace::CollectionTrace;
use collection_trace::lookup::UnsignedInt;
use collection_trace::{LeastUpperBound, Lookup, Offset};

use sort::*;

use timely::drain::DrainExt;

impl<G: Scope, D1: Data+Ord, S> JoinExt<G, D1> for S
where G::Timestamp: LeastUpperBound,
      S: Binary<G, (D1, i32)>+Map<G, (D1, i32)> { }

pub trait JoinExt<G: Scope, D1: Data+Ord> : Binary<G, (D1, i32)>+Map<G, (D1, i32)>
where G::Timestamp: LeastUpperBound {
    fn join_u<
        U:  UnsignedInt+Debug,
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
            .join_raw(&other.map(move |(x,w)| (kv2(x),w)),
                        |x|x,
                        |x|x,
                        |&(k,_)| k.as_u64(),
                        |&(k,_)| k.as_u64(),
                        |k| k.as_u64(),
                        result,
                        &|x| (Vec::new(), x))
    }
    fn semijoin_u<
        U:  UnsignedInt+Debug,
        V1: Data+Ord+Clone+Default+Debug+'static,
        F1: Fn(D1)->(U,V1)+'static,
        RF: Fn(&U,&V1)->D1+'static,
    >
        (&self, other: &Stream<G, (U, i32)>, kv1: F1, result: RF) -> Stream<G, (D1, i32)> {

        self.join_u(&other, kv1, |u| (u, ()), move |x,y,_| result(x,y))
    }

    fn join<
        K:  Data+Ord+Clone+Hash+Debug+'static,
        V1: Data+Ord+Clone+Debug+'static,
        V2: Data+Ord+Clone+Debug+'static,
        D2: Data,
        F1: Fn(D1)->(K,V1)+'static,
        F2: Fn(D2)->(K,V2)+'static,
        // H1: Fn(&D1)->u64+'static,
        // H2: Fn(&D2)->u64+'static,
        KH: Fn(&K)->u64+'static,
        R:  Ord+Data,
        RF: Fn(&K,&V1,&V2)->R+'static,
    >
        (&self, other: &Stream<G, (D2, i32)>,
        kv1: F1, kv2: F2,
        // part1: H1, part2: H2,
        key_h: KH, result: RF)
    -> Stream<G, (R, i32)> {

        let kh1 = Rc::new(key_h);
        let kh2 = kh1.clone();
        let kh3 = kh1.clone();

        self.map(move |(x,w)| (kv1(x),w))
            .join_raw(&other.map(move |(x,w)| (kv2(x),w)), |x| x, |x| x, move |&(ref k,_)| kh1(k), move |&(ref k,_)| kh2(k), move |k| kh3(k), result, &|_| HashMap::new())
    }
    fn semijoin<
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
        self.join(&other, kv1, |k| (k,()), key_h, move |x,y,_| result(x,y))
    }
    fn join_raw<
        K:  Ord+Clone+Debug+'static,
        V1: Ord+Clone+Debug+'static,
        V2: Ord+Clone+Debug+'static,
        D2: Data+Eq,
        F1: Fn(D1)->(K,V1)+'static,
        F2: Fn(D2)->(K,V2)+'static,
        H1: Fn(&D1)->u64+'static,
        H2: Fn(&D2)->u64+'static,
        KH: Fn(&K)->u64+'static,
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
        let mut trace1 = Some(CollectionTrace::new(look(0)));
        let mut trace2 = Some(CollectionTrace::new(look(0)));

        let mut stage1 = Vec::new();    // Vec<(T, Vec<(K, V1, i32)>)>;
        let mut stage2 = Vec::new();    // Vec<(T, Vec<(K, V2, i32)>)>;

        let mut outbuf = Vec::new();    // Vec<(T, Vec<(R,i32)>)> for buffering output.

        let exch1 = Exchange::new(move |&(ref r, _)| part1(r));
        let exch2 = Exchange::new(move |&(ref r, _)| part2(r));

        self.binary_notify(stream2, exch1, exch2, "Join", vec![], move |input1, input2, output, notificator| {

            // consider shutting down each trace if the opposing input has closed out
            if trace2.is_some() && notificator.frontier(0).len() == 0 && stage1.len() == 0 { trace2 = None; }
            if trace1.is_some() && notificator.frontier(1).len() == 0 && stage2.len() == 0 { trace1 = None; }

            // read input 1, push key, (val,wgt) to vecs
            while let Some((time, data1)) = input1.next() {
                notificator.notify_at(&time);
                let mut vecs = stage1.entry_or_insert(time.clone(), || (Vec::new(), Vec::new()));
                for (datum, wgt) in data1.drain_temp() {
                    let (key, val) = kv1(datum);
                    vecs.0.push(key);
                    vecs.1.push((val, wgt));
                }
            }

            // read input 2, push key, (val,wgt) to vecs
            while let Some((time, data2)) = input2.next() {
                notificator.notify_at(&time);
                let mut vecs = stage2.entry_or_insert(time.clone(), || (Vec::new(), Vec::new()));
                for (datum, wgt) in data2.drain_temp() {
                    let (key, val) = kv2(datum);
                    vecs.0.push(key);
                    vecs.1.push((val, wgt));
                }
            }

            // check to see if we have inputs to process
            while let Some((time, _count)) = notificator.next() {

                if let Some((mut keys, mut vals)) = stage1.remove_key(&time) {
                    coalesce_kv8(&mut keys, &mut vals, &key_h);
                    if let Some(trace) = trace2.as_ref() {
                        process_diffs(&time, &keys, &vals, &trace, &|k,v1,v2| result(k,v1,v2), &mut outbuf)
                    }

                    if let Some(trace) = trace1.as_mut() {
                        trace.install_differences(time.clone(), &mut keys, vals);
                    }
                }

                if let Some((mut keys, mut vals)) = stage2.remove_key(&time) {
                    coalesce_kv8(&mut keys, &mut vals, &key_h);
                    if let Some(trace) = trace1.as_ref() {
                        process_diffs(&time, &keys, &vals, &trace, &|k,v1,v2| result(k,v2,v1), &mut outbuf)
                    }

                    if let Some(trace) = trace2.as_mut() {
                        trace.install_differences(time.clone(), &mut keys, vals);
                    }
                }

                // transmit data for each output time
                for (time, mut vals) in outbuf.drain_temp() {
                    coalesce(&mut vals);
                    output.session(&time).give_iterator(vals.drain_temp());
                }
            }
        })
    }
}

fn process_diffs<K, T, V1: Debug, V2: Debug, L, R, RF>(time: &T, keys: &[K], vals: &[(V1,i32)],
                                         trace: &CollectionTrace<K,T,V2,L>,
                                         result: &RF,
                                         outbuf: &mut Vec<(T, Vec<(R,i32)>)>)
where T: Eq+LeastUpperBound+Clone,
      K: Ord+Clone,
      V2: Ord+Clone,
      RF: Fn(&K,&V1,&V2)->R,
      L: Lookup<K, Offset> {

    let mut lower = 0;
    while lower < keys.len() {
        let mut upper = lower + 1;
        while upper < keys.len() && keys[lower] == keys[upper] {
            upper += 1;
        }

        let vals1 = &vals[lower..upper];
        for (t, vals2) in trace.trace(&keys[lower]) {
            let mut output = outbuf.entry_or_insert(time.least_upper_bound(t), || Vec::new());
            for &(ref v1, w1) in vals1 {
                for &(ref v2, w2) in vals2 {
                    output.push((result(&keys[lower], v1, v2), w1 * w2));
                }
            }
        }

        lower = upper;
    }
}
