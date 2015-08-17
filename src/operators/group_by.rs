//! Implements the `group_by` differential dataflow operator.
//!
//! The `group_by` operator acts on data that can be viewed as pairs `(key, val)`.
//! It groups together values with the same key, and then applies user-defined logic to each pair
//! of `key` and `[val]`, where the latter is presented as an iterator. The user logic is expected
//! to write output to a supplied `&mut Vec<(output, u32)>` of outputs and counts.
//!
//! The operator is invoked only on non-empty groups; the provided iterator uses `Peekable` and it
//! is safe to call `.peek().unwrap()` on the iterator.

use std::default::Default;
use std::hash::Hash;
use std::collections::HashMap;
use std::fmt::Debug;

use itertools::Itertools;

use timely::Data;
use timely::dataflow::*;
use timely::dataflow::operators::{Map, Unary};
use timely::dataflow::channels::pact::Exchange;
use timely::drain::DrainExt;

use collection_trace::{LeastUpperBound, Lookup, Trace, Offset};
use collection_trace::lookup::UnsignedInt;
use collection_trace::trace::CollectionIterator;

use iterators::coalesce::Coalesce;

use sort::radix_merge::{Accumulator, Compact};
use sort::*;

// implement `GroupByExt` for any stream implementing `Unary` and `Map` (most of them).
impl<G: Scope, D: Data+Eq, S> GroupByExt<G, D> for S
where G::Timestamp: LeastUpperBound,
      S: Unary<G, (D, i32)>+Map<G, (D, i32)> { }

/// Extension trait for the `group_by` and `group_by_u` differential dataflow methods.
pub trait GroupByExt<G: Scope, D1: Data+Eq> : Unary<G, (D1, i32)>+Map<G, (D1, i32)>
where G::Timestamp: LeastUpperBound {
    fn group_by<
        K:     Hash+Ord+Clone+Debug+'static,        //  type of the key
        V1:    Ord+Clone+Default+Debug+'static,     //  type of the input value
        V2:    Ord+Clone+Default+Debug+'static,     //  type of the output value
        D2:    Data,                                //  type of the output data
        KV:    Fn(D1)->(K,V1)+'static,              //  function from data to (key,val)
        Part:  Fn(&D1)->u64+'static,                //  partitioning function; should match KH
        KH:    Fn(&K)->u64+'static,                 //  partitioning function for key; should match Part.

        // user-defined operator logic, from a key and value iterator, populating an output vector.
        Logic: Fn(&K, &mut CollectionIterator<V1>, &mut Vec<(V2, i32)>)+'static,

        // function from key and output value to output data.
        Reduc: Fn(&K, &V2)->D2+'static,
    >
    (&self, kv: KV, part: Part, key_h: KH, reduc: Reduc, logic: Logic) -> Stream<G, (D2, i32)> {
        self.group_by_inner(kv, part, key_h, reduc, |_| HashMap::new(), logic)
    }
    fn group_by_u<
        U:     UnsignedInt+Debug,
        V1:    Data+Ord+Clone+Default+Debug+'static,
        V2:    Ord+Clone+Default+Debug+'static,
        D2:    Data,
        KV:    Fn(D1)->(U,V1)+'static,
        Logic: Fn(&U, &mut CollectionIterator<V1>, &mut Vec<(V2, i32)>)+'static,
        Reduc: Fn(&U, &V2)->D2+'static,
    >
            (&self, kv: KV, reduc: Reduc, logic: Logic) -> Stream<G, (D2, i32)> {
                self.map(move |(x,w)| (kv(x),w))
                    .group_by_inner(|x| x,
                                    |&(k,_)| k.as_u64(),
                                    |k| k.as_u64(),
                                    reduc,
                                    |x| (Vec::new(), x),
                                    logic)
    }

    fn group_by_inner<
        K:     Hash+Ord+Clone+Debug+'static,
        V1:    Ord+Clone+Default+Debug+'static,
        V2:    Ord+Clone+Default+Debug+'static,
        D2:    Data,
        KV:    Fn(D1)->(K,V1)+'static,
        Part:  Fn(&D1)->u64+'static,
        KH:    Fn(&K)->u64+'static,
        Look:  Lookup<K, Offset>+'static,
        LookG: Fn(u64)->Look,
        Logic: Fn(&K, &mut CollectionIterator<V1>, &mut Vec<(V2, i32)>)+'static,
        Reduc: Fn(&K, &V2)->D2+'static,
    >
    (&self, kv: KV, part: Part, key_h: KH, reduc: Reduc, look: LookG, logic: Logic) -> Stream<G, (D2, i32)> {

        // A pair of source and result `CollectionTrace` instances.
        // TODO : The hard-coded 0 means we don't know how many bits we can shave off of each int
        // TODO : key, which is fine for `HashMap` but less great for integer keyed maps, which use
        // TODO : dense vectors (sparser as number of workers increases).
        // TODO : At the moment, we don't have access to the stream's underlying .scope() method,
        // TODO : which is what would let us see the number of peers, because we only know that
        // TODO : the type also implements the `Unary` and `Map` traits, not that it is a `Stream`.
        // TODO : We could implement this just for `Stream`, but would have to repeat the trait

        // TODO : method signature boiler-plate, rather than use default implemenations.
        // let mut trace =  OperatorTrace::<K, G::Timestamp, V1, V2, Look>::new(|| look(0));
        let mut sources = (0..256).map(|_| Trace::new(look(8))).collect::<Vec<_>>();
        let mut results = (0..256).map(|_| Trace::new(look(8))).collect::<Vec<_>>();

        // A map from times to received (key, val, wgt) triples.
        let mut inputs = Vec::new();

        // A map from times to a list of keys that need processing at that time.
        let mut to_do = (0..256).map(|_| Vec::new()).collect::<Vec<_>>();

        // create an exchange channel based on the supplied Fn(&D1)->u64.
        let exch = Exchange::new(move |&(ref x,_)| part(x));

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        self.unary_notify(exch, "GroupBy", vec![], move |input, output, notificator| {

            // 1. read each input, and stash it in our staging area
            while let Some((time, data)) = input.next() {

                // request a notification for when all records with this time have been received.
                notificator.notify_at(&time);

                // fetch (and create, if needed) key and value queues for this time.
                let mut accums = inputs.entry_or_insert(time.clone(), || {
                    let mut result = Vec::with_capacity(256);
                    for _ in 0..256 { result.push(Accumulator::new()); }
                    result
                });

                // push each record to the appropriate accumulator
                for (datum, wgt) in data.drain_temp() {
                    let (key, val) = kv(datum);
                    // println!("groupby recving ({:?}, {:?}, {})", key, val, wgt);
                    let part = key_h(&key) as usize % 256;
                    accums[part].push(key, val, wgt, &key_h);
                }
            }

            // 2. go through each time of interest that has reached completion
            // times are interesting either because we received data, or because we conclude
            // in the processing of a time that a future time will be interesting.
            while let Some((index, _count)) = notificator.next() {

                // println!("groupby notificating at {:?}", index);

                // 2a. fetch any data associated with this time.
                if let Some(accumulation) = inputs.remove_key(&index) {

                    // println!("groupby accumulation found at {:?}", index);

                    // for each key, determine if there are new times that may
                    // require comparing the output to logic(input) at the time.
                    for (part, accum) in accumulation.into_iter().enumerate() {
                        if let Some(compact) = accum.done(&key_h) {
                            // println!("groupby accum found for part {:?}", part);
                            // println!("  keys: {:?}", compact.keys);
                            let source = &mut sources[part];
                            for key in &compact.keys {
                                for time in source.interesting_times(key, index.clone()).iter() {
                                    let mut queue: &mut Vec<K> = to_do[part].entry_or_insert((*time).clone(), || {
                                                    // println!("groupby enqueueing work for part {} at {:?} for key {:?}", part, time, key);
                                                    notificator.notify_at(time);
                                                    Vec::new()
                                                });
                                    queue.push((*key).clone());
                                }
                            }

                            // add the accumulation to the trace source.
                            // println!("setting source differences; {}", compact.vals.len());
                            source.set_differences(index.clone(), compact);
                        }
                    }
                }

                // we may need to produce output at index
                let mut session = output.session(&index);

                // temporary storage for operator implementations to populate
                let mut buffer = vec![];

                for part in 0..256 {

                    let source = &mut sources[part];
                    let result = &mut results[part];

                    // 2b. We must now determine for each interesting key at this time, how does the
                    // currently reported output match up with what we need as output. Should we send
                    // more output differences, and what are they?

                    // Much of this logic used to hide in `OperatorTrace` and `CollectionTrace`.
                    // They are now gone and simpler, respectively.
                    if let Some(mut keys) = to_do[part].remove_key(&index) {

                        // println!("groupby doing a thing at {:?}", index);

                        // we would like these keys in a particular order.
                        // TODO : use a radix sort since we have `key_h`.
                        keys.sort_by(|x,y| (key_h(&x), x).cmp(&(key_h(&y), y)));
                        keys.dedup();

                        // accumulations for installation into result
                        let mut accumulation = Compact::new(0,0,0);

                        let mut heap1 = vec![];
                        let mut heap2 = vec![];

                        for key in keys {

                            // println!("groupby doing a thing for {:?} at {:?}", key, index);

                            // acquire an iterator over the collection at `time`.
                            let mut input = source.get_collection_using(&key, &index, &mut heap1);

                            // if we have some data, invoke logic to populate self.dst
                            if input.peek().is_some() { logic(&key, &mut input, &mut buffer); }

                            buffer.sort_by(|x,y| x.0.cmp(&y.0));

                            // push differences in to Compact.
                            let mut compact = accumulation.session();
                            for (val, wgt) in Coalesce::coalesce(result.get_collection_using(&key, &index, &mut heap2)
                                                                       .map(|(v, w)| (v,-w))
                                                                       .merge_by(buffer.iter().map(|&(ref v, w)| (v, w)), |x,y| {
                                                                            x.0.cmp(&y.0)
                                                                       }))
                            {
                                session.give((reduc(&key, val), wgt));
                                compact.push(val.clone(), wgt);
                            }
                            compact.done(key);
                            buffer.clear();
                        }

                        if accumulation.vals.len() > 0 {
                            result.set_differences(index.clone(), accumulation);
                        }
                    }
                }
            }
        })
    }
}
