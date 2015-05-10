use std::default::Default;
use std::hash::Hash;
// use std::collections::HashSet;
use std::collections::HashMap;

use timely::example_shared::*;
use timely::example_shared::operators::*;

use timely::communication::*;
use timely::communication::pact::Exchange;

use columnar::Columnar;

use collection_trace::{LeastUpperBound, Lookup, BinaryOperatorTrace, Offset};
use collection_trace::lookup::UnsignedInt;
use sort::*;

impl<G: GraphBuilder, D1: Data+Columnar, S: BinaryNotifyExt<G, (D1, i32)>+MapExt<G, (D1, i32)>> GroupByExt<G, D1> for S where G::Timestamp: LeastUpperBound {}


pub trait CoGroupByExt<G: GraphBuilder, D1: Data+Columnar> : BinaryNotifyExt<G, (D1, i32)>+MapExt<G, (D1, i32)> where G::Timestamp: LeastUpperBound {
    fn co_group_by<
                K:     Hash+Ord+Clone+'static,
                V1:    Ord+Clone+Default+'static,
                V2:    Ord+Clone+Default+'static,
                V3:    Ord+Clone+Default+'static,
                D2:    Data+Columnar,
                D3:    Data+Columnar,
                KV1:   Fn(D1)->(K,V1)+'static,
                KV2:   Fn(D2)->(K,V2)+'static,
                Part1: Fn(&D1)->u64+'static,
                Part2: Fn(&D2)->u64+'static,
                Logic: Fn(&K, &[(V1,i32)], &mut Vec<(V3, i32)>)+'static,
                Reduc: Fn(&K, &V3)->D3+'static,
                >
            (&self, kv1: KV1, kv2: KV2, part1: Part1, part2: Part2, reduc: Reduc, logic: Logic) -> Stream<G, (D2, i32)> {
                self.group_by_inner(kv1, kv2, part1, part2, reduc, |_| HashMap::new(), logic)
            }
    fn co_group_by_u<
                U:     UnsignedInt,
                V1:    Data+Columnar+Ord+Clone+Default+'static,        // TODO : Is Clone needed?
                V2:    Ord+Clone+Default+'static,
                V3:    Ord+Clone+Default+'static,
                D2:    Data+Columnar,
                D3:    Data+Columnar,
                KV1:   Fn(D1)->(U,V1)+'static,
                KV2:   Fn(D2)->(U,V2)+'static,
                Logic: Fn(&U, &[(V1,i32)], &[(V2,i32)], &mut Vec<(V3, i32)>)+'static,
                Reduc: Fn(&U, &V3)->D3+'static,
                >
            (&self, kv: KV, reduc: Reduc, logic: Logic) -> Stream<G, (D2, i32)> {
                self.map(move |(x,w)| (kv1(x),w))
                    .co_group_by_inner(&other.map(move |(x,w)| (kv2(x),w))

                    |x|x, |x|x, |&(k,_)| k.as_usize() as u64, |&(k,_)| k.as_usize() as u64, reduc, |x| (Vec::new(), x), logic)
    }

    fn co_group_by_inner<
                        K:     Hash+Ord+Clone+'static,
                        V1:    Ord+Clone+Default+'static,
                        V2:    Ord+Clone+Default+'static,
                        V3:    Ord+Clone+Default+'static,
                        D2:    Data+Columnar,
                        D3:    Data+Columnar,
                        KV1:   Fn(D1)->(K,V1)+'static,
                        KV2:   Fn(D2)->(K,V2)+'static,
                        Part1: Fn(&D1)->u64+'static,
                        Part2: Fn(&D2)->u64+'static,
                        Look:  Lookup<K, Offset>,
                        LookG: Fn(u64)->Look,
                        Logic: Fn(&K, &[(V1,i32)], &[(V2,i32)], &mut Vec<(V3, i32)>)+'static,
                        Reduc: Fn(&K, &V3)->D3+'static,
                        >
                    (&self, kv1: KV1, kv2: KV2, part1: Part1, part2: Part2, reduc: Reduc, look: LookG, logic: Logic) -> Stream<G, (D2, i32)> {

        // TODO : pay more attention to the number of peers
        // TODO : find a better trait to sub-trait so we can read .builder
        // assert!(self.builder.peers() == 1);
        let mut trace =  BinaryOperatorTrace::<K, G::Timestamp, V1, V2, Look>::new(|| look(0));

        let mut inputs1 = Vec::new();
        let mut inputs2 = Vec::new();

        let mut to_do =  Vec::new();

        // temporary storage for the operator
        let mut idx = Vec::new();   // Vec<G::Timestamp>,

        let exch = Exchange::new(move |&(ref x,_)| part(x));
        self.binary_notify(exch, format!("GroupBy"), vec![], move |input1, input2, output, notificator| {

            // 1. read each input, and stash it in our staging area
            while let Some((time, mut data)) = input1.pull() {
                inputs1.entry_or_insert(time.clone(), || { notificator.notify_at(&time); Vec::new() })
                       .extend(data.drain(..).map(|(datum, delta)| (kv1(datum), delta)));
            }

            while let Some((time, mut data)) = input2.pull() {
                inputs2.entry_or_insert(time.clone(), || { notificator.notify_at(&time); Vec::new() })
                       .extend(data.drain(..).map(|(datum, delta)| (kv2(datum), delta)));
            }


            // 2. go through each time of interest that has reached completion
            while let Some((index, _count)) = notificator.next() {

                // 2a. if we have some input data to process
                if let Some(mut data) = inputs.remove_key(&index) {
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

                        // TODO : This get more tedious with two traces; must union times and then close under lub
                        trace.source.set_difference(key.clone(), index.clone(), list.drain(..));
                        trace.source.interesting_times(&key, &index, &mut idx);
                        for update in idx.drain(..) {
                            to_do.entry_or_insert(update, || { notificator.notify_at(&update); Vec::new() })
                                 .push(key.clone());
                        }
                    }
                }

                // 2b. if we have work to do at this notification (probably)
                if let Some(mut keys) = to_do.remove_key(&index) {
                    let mut session = output.session(&index);
                    qsort(&mut keys[..]);
                    keys.dedup();
                    for key in keys {
                        trace.set_collection_with(&key, &index, |k,s1,s2,r| logic(k,s1,s2,r));
                        for &(ref result, weight) in trace.result.get_difference(&key, &index)  {
                            session.give((reduc(&key, &result), weight));
                        }
                    }
                }

                // println!("groupby size at {:?}: ({:?}, {:?})", index, trace.source.size(), trace.result.size());
            }
        })
    }
}
