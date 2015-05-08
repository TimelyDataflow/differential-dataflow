use std::fmt::Debug;
use std::default::Default;
use std::hash::Hash;
use std::collections::HashMap;

use timely::example_shared::*;
use timely::example_shared::operators::*;

use timely::communication::*;
use timely::communication::pact::Exchange;

use columnar::Columnar;

// use collection_trace::batch_trace::BinaryTrace;
use collection_trace::CollectionTrace;

use collection_trace::lookup::UnsignedInt;
use collection_trace::{LeastUpperBound, Lookup, Offset};
use sort::coalesce;
use std::iter;

impl<G: GraphBuilder, D1: Data+Columnar, S: BinaryNotifyExt<G, (D1, i32)>+MapExt<G, (D1, i32)>> JoinExt<G, D1> for S where G::Timestamp: LeastUpperBound {}

pub trait JoinExt<G: GraphBuilder, D1: Data+Columnar> : BinaryNotifyExt<G, (D1, i32)>+MapExt<G, (D1, i32)> where G::Timestamp: LeastUpperBound {
    fn join_u<
            U:  UnsignedInt,
            V1: Data+Columnar+Ord+Clone+Default+Debug+'static,
            V2: Data+Columnar+Ord+Clone+Default+Debug+'static,
            D2: Data+Columnar,
            F1: Fn(D1)->(U,V1)+'static,
            F2: Fn(D2)->(U,V2)+'static,
            R:  Data+Columnar,
            RF: Fn(&U,&V1,&V2)->R+'static,
            >
        (&self, other: &Stream<G, (D2, i32)>, kv1: F1, kv2: F2, result: RF) -> Stream<G, (R, i32)> {
        self.map(move |(x,w)| (kv1(x),w))
            .join_inner(&other.map(move |(x,w)| (kv2(x),w)), |x|x, |x|x, |&(k,_)| k.as_usize() as u64, |&(k,_)| k.as_usize() as u64, result, &|x| (Vec::new(), x))
    }

    fn join<K:  Ord+Clone+Hash+Debug+'static,
            V1: Ord+Clone+Debug+'static,
            V2: Ord+Clone+Debug+'static,
            D2: Data+Columnar,
            F1: Fn(D1)->(K,V1)+'static,
            F2: Fn(D2)->(K,V2)+'static,
            H1: Fn(&D1)->u64+'static,
            H2: Fn(&D2)->u64+'static,
            R:  Data+Columnar,
            RF: Fn(&K,&V1,&V2)->R+'static,
            >
        (&self, other: &Stream<G, (D2, i32)>, kv1: F1, kv2: F2, part1: H1, part2: H2, result: RF) -> Stream<G, (R, i32)> {
        self.join_inner(other, kv1, kv2, part1, part2, result, &|_| HashMap::new())
    }
    fn join_inner<
                K:  Ord+Clone+Debug+'static,
                V1: Ord+Clone+Debug+'static,
                V2: Ord+Clone+Debug+'static,
                D2: Data+Columnar,
                F1: Fn(D1)->(K,V1)+'static,
                F2: Fn(D2)->(K,V2)+'static,
                H1: Fn(&D1)->u64+'static,
                H2: Fn(&D2)->u64+'static,
                R:  Data+Columnar,
                RF: Fn(&K,&V1,&V2)->R+'static,
                LC: Lookup<K, Offset>,
                GC: Fn(u64)->LC,
                >
            (&self,
             stream2: &Stream<G, (D2, i32)>,
             kv1: F1,
             kv2: F2,
             part1: H1,
             part2: H2,
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

        self.binary_notify(stream2, exch1, exch2, format!("Join"), vec![], move |input1, input2, output, notificator| {

            // consider shutting down each trace if the opposing input has closed out
            if trace2.is_some() && notificator.frontier(0).len() == 0 && stage1.len() == 0 { trace2 = None; }
            if trace1.is_some() && notificator.frontier(1).len() == 0 && stage2.len() == 0 { trace1 = None; }

            while let Some((time, data1)) = input1.pull() {
                stage1.entry_or_insert(time.clone(), || { notificator.notify_at(&time); Vec::new() })
                      .extend(data1.drain(..).map(|(datum, delta)| (kv1(datum), delta)));
            }

            while let Some((time, data2)) = input2.pull() {
                stage2.entry_or_insert(time.clone(), || { notificator.notify_at(&time); Vec::new() })
                      .extend(data2.drain(..).map(|(datum, delta)| (kv2(datum), delta)));
            }

            while let Some((time, _count)) = notificator.next() {
                if let Some(mut data) = stage1.remove_key(&time) {
                    coalesce(&mut data);

                    let mut list = Vec::new();
                    let mut cursor = 0;
                    while cursor < data.len() {
                        let key = ((data[cursor].0).0).clone();
                        while cursor < data.len() && key == (data[cursor].0).0 {
                            let ((_, val), wgt) = data[cursor].clone();
                            list.push((val, wgt));
                            cursor += 1;
                        }

                        if let Some(trace) = trace2.as_ref() {
                            trace.map_over_times(&key, |index, values| {
                                outbuf.entry_or_insert(time.least_upper_bound(index), || Vec::new())
                                      .extend(values.iter().flat_map(|x| iter::repeat(x).zip(list.iter()).map(|(x,y)| (result(&key, &y.0, &x.0), y.1 * x.1))));

                            });
                        }

                        trace1.as_mut().map(|x| x.set_difference(key, time.clone(), list.drain(..)));
                        list.clear();
                    }
                }

                if let Some(mut data) = stage2.remove_key(&time) {
                    coalesce(&mut data);

                    let mut list = Vec::new();
                    let mut cursor = 0;
                    while cursor < data.len() {
                        let key = ((data[cursor].0).0).clone();
                        while cursor < data.len() && key == (data[cursor].0).0 {
                            let ((_, val), wgt) = data[cursor].clone();
                            list.push((val, wgt));
                            cursor += 1;
                        }

                        trace1.as_ref().map(|x| x.map_over_times(&key, |index, values| {
                            outbuf.entry_or_insert(time.least_upper_bound(index), || Vec::new())
                                  .extend(values.iter().flat_map(|x| iter::repeat(x).zip(list.iter()).map(|(x,y)| (result(&key, &x.0, &y.0), x.1 * y.1))));
                        }));

                        trace2.as_mut().map(|x| x.set_difference(key, time.clone(), list.drain(..)));
                        list.clear();
                    }
                }

                for (time, mut vals) in outbuf.drain(..) {
                    output.give_at(&time, vals.drain(..));
                }

                // println!("join size at {:?}: ({:?}, {:?})", time, trace1.as_ref().map(|x| x.size()).unwrap_or((0,0)), trace2.as_ref().map(|x|x.size()).unwrap_or((0,0)));
            }
        })
    }
}
