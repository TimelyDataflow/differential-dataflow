use std::default::Default;
use std::hash::Hash;
use std::collections::HashMap;
use std::iter::Peekable;

use timely::example_shared::*;
use timely::example_shared::operators::*;

use timely::communication::*;
use timely::communication::pact::Exchange;
use timely::serialization::Serializable;
// use columnar::Columnar;

use collection_trace::{LeastUpperBound, Lookup, OperatorTrace, Offset};
use collection_trace::lookup::UnsignedInt;
use collection_trace::collection_trace::MergeIterator;

use sort::*;

use timely::drain::DrainExt;

impl<G: GraphBuilder, D: Data+Serializable, S> GroupByExt<G, D> for S
where G::Timestamp: LeastUpperBound,
      S: UnaryNotifyExt<G, (D, i32)>+MapExt<G, (D, i32)> { }


pub trait GroupByExt<G: GraphBuilder, D1: Data+Serializable> : UnaryNotifyExt<G, (D1, i32)>+MapExt<G, (D1, i32)>
where G::Timestamp: LeastUpperBound {
    fn group_by<
                K:     Hash+Ord+Clone+'static,
                V1:    Ord+Clone+Default+'static,
                V2:    Ord+Clone+Default+'static,
                D2:    Data+Serializable,
                KV:    Fn(D1)->(K,V1)+'static,
                Part:  Fn(&D1)->u64+'static,
                KH:    Fn(&K)->u64+'static,
                Logic: Fn(&K, Peekable<MergeIterator<V1>>, &mut Vec<(V2, i32)>)+'static,
                Reduc: Fn(&K, &V2)->D2+'static,
                >
            (&self, kv: KV, part: Part, key_h: KH, reduc: Reduc, logic: Logic) -> Stream<G, (D2, i32)> {
                self.group_by_inner(kv, part, key_h, reduc, |_| HashMap::new(), logic)
            }
    fn group_by_u<
                  U:     UnsignedInt,
                  V1:    Data+Serializable+Ord+Clone+Default+'static,
                  V2:    Ord+Clone+Default+'static,
                  D2:    Data+Serializable,
                  KV:    Fn(D1)->(U,V1)+'static,
                  Logic: Fn(&U, Peekable<MergeIterator<V1>>, &mut Vec<(V2, i32)>)+'static,
                  Reduc: Fn(&U, &V2)->D2+'static,
                  >
            (&self, kv: KV, reduc: Reduc, logic: Logic) -> Stream<G, (D2, i32)> {
                self.map(move |(x,w)| (kv(x),w))
                    .group_by_inner(|x|x,
                                    |&(k,_)| k.as_u64(),
                                    |k| k.as_u64(),
                                    reduc,
                                    |x| (Vec::new(), x),
                                    logic)
    }

    fn group_by_inner<
                      K:     Hash+Ord+Clone+'static,
                      V1:    Ord+Clone+Default+'static,
                      V2:    Ord+Clone+Default+'static,
                      D2:    Data+Serializable,
                      KV:    Fn(D1)->(K,V1)+'static,
                      Part:  Fn(&D1)->u64+'static,
                      KH:    Fn(&K)->u64+'static,
                      Look:  Lookup<K, Offset>+'static,
                      LookG: Fn(u64)->Look,
                      Logic: Fn(&K, Peekable<MergeIterator<V1>>, &mut Vec<(V2, i32)>)+'static,
                      Reduc: Fn(&K, &V2)->D2+'static,
                      >
                    (&self, kv: KV, part: Part, key_h: KH, reduc: Reduc, look: LookG, logic: Logic)
                 -> Stream<G, (D2, i32)> {

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

                notificator.notify_at(&time);
                let mut queues = inputs.entry_or_insert(time.clone(), || (Vec::new(), Vec::new()));
                for (datum, delta) in data.drain_temp() {
                    let (key, val) = kv(datum);
                    queues.0.push(key);
                    queues.1.push((val, delta));
                }
            }

            // 2. go through each time of interest that has reached completion
            while let Some((index, _count)) = notificator.next() {

                // 2a. if we have some input data to process
                if let Some((mut keys, mut vals)) = inputs.remove_key(&index) {
                    coalesce_kv8(&mut keys, &mut vals, &key_h);

                    trace.source.install_differences(index.clone(), &mut keys, vals);

                    // iterate over keys to find interesting times
                    let mut lower = 0;
                    while lower < keys.len() {
                        let mut upper = lower + 1;
                        while upper < keys.len() && keys[lower] == keys[upper] {
                            upper += 1;
                        }

                        trace.source.interesting_times(&keys[lower], &index, &mut idx);
                        for update in idx.drain_temp() {
                            to_do.entry_or_insert(update, || { notificator.notify_at(&update); Vec::new() })
                                 .push(keys[lower].clone());
                         }

                        lower = upper;
                    }
                }

                // 2b. if we have work to do at this notification (probably)
                if let Some(mut keys) = to_do.remove_key(&index) {
                    let mut session = output.session(&index);
                    qsort(&mut keys[..]);
                    keys.dedup();
                    for key in keys {
                        trace.set_collection_from(&key, &index, |k,s,r| logic(k,s,r));
                        for &(ref result, weight) in trace.result.get_difference(&key, &index)  {
                            session.give((reduc(&key, &result), weight));
                        }
                    }
                }
            }
        })
    }
}
