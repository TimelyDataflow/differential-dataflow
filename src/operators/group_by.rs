use std::default::Default;
use std::hash::Hash;
use std::collections::HashSet;
use std::collections::HashMap;

use timely::example_shared::*;
use timely::example_shared::operators::*;

use timely::communication::*;
use timely::communication::pact::Exchange;

use columnar::Columnar;

use collection_trace::{LeastUpperBound, Lookup, OperatorTrace, Offset};
use collection_trace::lookup::UnsignedInt;
use sort::*;

impl<G: GraphBuilder, D1: Data+Columnar, S: UnaryNotifyExt<G, (D1, i32)>+MapExt<G, (D1, i32)>> GroupByExt<G, D1> for S where G::Timestamp: LeastUpperBound {}


pub trait GroupByExt<G: GraphBuilder, D1: Data+Columnar> : UnaryNotifyExt<G, (D1, i32)>+MapExt<G, (D1, i32)> where G::Timestamp: LeastUpperBound {
    fn group_by<
                K:     Hash+Ord+Clone+'static,
                V1:    Ord+Clone+Default+'static,        // TODO : Is Clone needed?
                V2:    Ord+Clone+Default+'static,        // TODO : Is Clone needed?
                D2:    Data+Columnar,
                KV:    Fn(D1)->(K,V1)+'static,
                Part:  Fn(&D1)->u64+'static,
                Logic: Fn(&K, &[(V1,i32)], &mut Vec<(V2, i32)>)+'static,
                Reduc: Fn(&K, &V2)->D2+'static,
                >
            (&self, kv: KV, part: Part, reduc: Reduc, logic: Logic) -> Stream<G, (D2, i32)> {
                self.group_by_inner(kv, part, reduc, |_| HashMap::new(), logic)
            }
    fn group_by_u<
                U:     UnsignedInt,
                V1:    Data+Columnar+Ord+Clone+Default+'static,        // TODO : Is Clone needed?
                V2:    Ord+Clone+Default+'static,        // TODO : Is Clone needed?
                D2:    Data+Columnar,
                KV:    Fn(D1)->(U,V1)+'static,
                Logic: Fn(&U, &[(V1,i32)], &mut Vec<(V2, i32)>)+'static,
                Reduc: Fn(&U, &V2)->D2+'static,
                >
            (&self, kv: KV, reduc: Reduc, logic: Logic) -> Stream<G, (D2, i32)> {
                self.map(move |(x,w)| (kv(x),w))
                    .group_by_inner(|x|x, |&(k,_)| k.as_usize() as u64, reduc, |x| (Vec::new(), x), logic)
    }

    fn group_by_inner<
                        K:     Hash+Ord+Clone+'static,
                        V1:    Ord+Clone+Default+'static,        // TODO : Is Clone needed?
                        V2:    Ord+Clone+Default+'static,        // TODO : Is Clone needed?
                        D2:    Data+Columnar,
                        KV:    Fn(D1)->(K,V1)+'static,
                        Part:  Fn(&D1)->u64+'static,
                        Look:  Lookup<K, Offset>,
                        LookG: Fn(u64)->Look,
                        Logic: Fn(&K, &[(V1,i32)], &mut Vec<(V2, i32)>)+'static,
                        Reduc: Fn(&K, &V2)->D2+'static,
                        >
                    (&self, kv: KV, part: Part, reduc: Reduc, look: LookG, logic: Logic) -> Stream<G, (D2, i32)> {

        // TODO : pay more attention to the number of peers
        // TODO : find a better trait to sub-trait so we can read .builder
        // assert!(self.builder.peers() == 1);
        let mut trace =  OperatorTrace::<K, G::Timestamp, V1, V2, Look>::new(|| look(0));
        let mut inputs = Vec::new();
        let mut to_do =  Vec::new();

        // temporary storage for the operator
        let mut idx = Vec::new();   // Vec<G::Timestamp>,

        let exch = Exchange::new(move |&(ref x,_)| part(x));
        self.unary_notify(exch, format!("GroupBy"), vec![], move |input, output, notificator| {

            // 1. read each input, and stash it in our staging area
            while let Some((time, mut data)) = input.pull() {
                inputs.entry_or_insert(time.clone(), || { notificator.notify_at(&time); Vec::new() })
                      .extend(data.drain(..).map(|(datum, delta)| (kv(datum), delta)));
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

                        trace.source.set_difference(key.clone(), index.clone(), list.drain(..));
                        trace.source.interesting_times(&key, &index, &mut idx);
                        for update in idx.drain(..) {
                            to_do.entry_or_insert(update, || { notificator.notify_at(&update); HashSet::new() })
                                 .insert(key.clone());
                        }
                    }
                }

                // 2b. if we have work to do at this notification (probably)
                if let Some(keys) = to_do.remove_key(&index) {
                    let mut session = output.session(&index);
                    for key in keys {
                        trace.set_collection_with(&key, &index, |k,s,r| logic(k,s,r));
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
