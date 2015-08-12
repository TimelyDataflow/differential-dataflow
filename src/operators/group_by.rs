//! Implements the `group_by` differential dataflow operator.
//!
//! The `group_by` operator acts on data that can be viewed as pairs `(key, val)`.
//! It groups together values with the same key, and then applies user-defined logic to each pair
//! of `key` and `[val]`, where the latter is presented as an iterator. The user logic is expected
//! to write output to a supplied `&mut Vec<(output, u32)>` of outputs and counts.
//!
//! The operator is invoked only on non-empty groups; the provided iterator uses `Peekable` and it
//! is safe to call `.peek().unwrap()` on the iterator.

use std::rc::Rc;
use std::default::Default;
use std::hash::Hash;
use std::collections::HashMap;
use std::fmt::Debug;

use timely::Data;
use timely::dataflow::*;
use timely::dataflow::operators::{Map, Unary};
use timely::dataflow::channels::pact::Exchange;
use timely::drain::DrainExt;

use collection_trace::{LeastUpperBound, Lookup, OperatorTrace, Offset};
use collection_trace::lookup::UnsignedInt;
use collection_trace::collection_trace::CollectionIterator;
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

        // we will need at least two copies of the key hash function, so we wrap it in a refcount.
        let key_h = Rc::new(key_h);
        let clone = key_h.clone();

        // A pair of source and result `CollectionTrace` instances.
        // TODO : The hard-coded 0 means we don't know how many bits we can shave off of each int
        // TODO : key, which is fine for `HashMap` but less great for integer keyed maps, which use
        // TODO : dense vectors (sparser as number of workers increases).
        // TODO : At the moment, we don't have access to the stream's underlying .scope() method,
        // TODO : which is what would let us see the number of peers, because we only know that
        // TODO : the type also implements the `Unary` and `Map` traits, not that it is a `Stream`.
        // TODO : We could implement this just for `Stream`, but would have to repeat the trait
        // TODO : method signature boiler-plate, rather than use default implemenations.
        let mut trace =  OperatorTrace::<K, G::Timestamp, V1, V2, Look>::new(|| look(0));
        // A map from times to received (key, val, wgt) triples.
        let mut inputs = Vec::new();
        // A map from times to a list of keys that need processing at that time.
        let mut to_do =  Vec::new();

        // temporary storage for interesting times.
        // TODO : perhaps the collection trace should own this and return a &[G::Timestamp]?
        let mut idx = Vec::new();

        // create an exchange channel based on the supplied Fn(&D1)->u64.
        let exch = Exchange::new(move |&(ref x,_)| part(x));

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        self.unary_notify(exch, "GroupBy", vec![], move |input, output, notificator| {

            // 1. read each input, and stash it in our staging area
            while let Some((time, data)) = input.next() {

                // request a notification for when all records with this time have been received.
                notificator.notify_at(&time);

                // fetch (and create, if needed) key and value queues for this time.
                let mut queues = inputs.entry_or_insert(time.clone(), || (Vec::new(), Vec::new()));
                for (datum, wgt) in data.drain_temp() {
                    // transform the data into key,val pairs and enqueue.
                    // TODO : opportunity for a smarter, sorting/coalescing queue.
                    // TODO : see the inappropriately named radix_merge::Merge struct.
                    let (key, val) = kv(datum);
                    queues.0.push(key);
                    queues.1.push((val, wgt));
                }
            }

            // 2. go through each time of interest that has reached completion
            // times are interesting either because we received data, or because we conclude
            // in the processing of a time that a future time will be interesting.
            while let Some((index, _count)) = notificator.next() {

                // 2a. fetch any data associated with this time.
                if let Some((mut keys, mut vals)) = inputs.remove_key(&index) {

                    // coalesce the (key, val, wgt) triples, using a helpful &*clone : Fn(&K)->u64.
                    // TODO : make this sort in a specific order, rather than best-effort top-down
                    // TODO : radix sort bailing out to qsort (shame!). bottom-up radix, please.
                    coalesce_kv8(&mut keys, &mut vals, &*clone);

                    // defer to trace.source to install the (time, keys, vals).
                    // this mostly just updates a map from key to a linked list head indicating
                    // an offset in vals.
                    trace.source.install_differences(index.clone(), &mut keys, vals);

                    // finally, we need to consult each of the installed keys (could have done at
                    // install time, with lightly tweaked logic) to determine if there are future
                    // times at which this key should be re-considered, because there will be
                    // installed differences coming into effect. badly explained, sorry.
                    // TODO : install_differences will probably want to take ownership/drain keys,
                    // TODO : so we'll need to do this earlier, or as part of install_differences.
                    // TODO : I suspect interesting_times could do a better job of determining
                    // TODO : times that are *newly interesting*, i.e. not previously produced.
                    keys.dedup();
                    for key in keys.into_iter() {
                        trace.source.interesting_times(&key, &index, &mut idx);
                        // for each time interesting to the key,
                        for update in idx.drain_temp() {
                            // add the key to a list indexed by the time.
                            to_do.entry_or_insert(update, || { notificator.notify_at(&update); Vec::new() })
                                 .push(key.clone());
                         }
                    }
                }

                // 2b. We must now determine for each interesting key at this time, how does the
                // currently reported output match up with what we need as output. Should we send
                // more output differences, and what are they?
                if let Some(mut keys) = to_do.remove_key(&index) {
                    let mut session = output.session(&index);
                    // would be great to sort these keys using `clone`.
                    qsort(&mut keys[..]);
                    keys.dedup();
                    for key in keys {
                        // set_collection_from updates trace.result so that its differences will
                        // accumulate to `logic` applied to the accumulation of trace.source.
                        trace.set_collection_from(&key, &index, |k,s,r| logic(k,s,r));
                        // send each difference (possibly none, which would be great).
                        for &(ref result, weight) in trace.result.get_difference(&key, &index)  {
                            session.give((reduc(&key, &result), weight));
                        }
                    }
                }
            }
        })
    }
}
