//! Group records by a key, and apply a reduction function.
//!
//! The `group` operators act on data that can be viewed as pairs `(key, val)`. They group records
//! with the same key, and apply user supplied functions to the key and a list of values, which are
//! expected to populate a list of output values.
//!
//! Several variants of `group` exist which allow more precise control over how grouping is done.
//! For example, the `_by` suffixed variants take arbitrary data, but require a key-value selector
//! to be applied to each record. The `_u` suffixed variants use unsigned integers as keys, and
//! will use a dense array rather than a `HashMap` to store their keys.
//!
//! The list of values are presented as an iterator which internally merges sorted lists of values.
//! This ordering can be exploited in several cases to avoid computation when only the first few
//! elements are required.
//!
//! #Examples
//!
//! This example groups a stream of `(key,val)` pairs by `key`, and yields only the most frequently
//! occurring value for each key.
//!
//! ```ignore
//! stream.group(|key, vals, output| {
//!     let (mut max_val, mut max_wgt) = vals.next().unwrap();
//!     for (val, wgt) in vals {
//!         if wgt > max_wgt {
//!             max_wgt = wgt;
//!             max_val = val;
//!         }
//!     }
//!     output.push((max_val.clone(), max_wgt));
//! })
//! ```

use std::default::Default;
use std::collections::HashMap;
use std::ops::DerefMut;

use itertools::Itertools;

use ::{Data, Collection, Delta};
use timely::dataflow::*;
use timely::dataflow::operators::{Map, Unary};
use timely::dataflow::channels::pact::Exchange;
use timely_sort::{LSBRadixSorter, Unsigned};

use collection::{LeastUpperBound, Lookup, Trace, BasicTrace, Offset};
use collection::trace::CollectionIterator;
use collection::basic::DifferenceIterator;
use collection::compact::Compact;

use iterators::coalesce::Coalesce;

/// Extension trait for the `group` differential dataflow method
pub trait Group<G: Scope, K: Data, V: Data> : GroupBy<G, (K,V)>
    where G::Timestamp: LeastUpperBound {

    /// Groups records by their first field, and applies reduction logic to the associated values.
    fn group<L, V2: Data>(&self, logic: L) -> Collection<G, (K,V2)>
        where L: Fn(&K, &mut CollectionIterator<DifferenceIterator<V>>, &mut Vec<(V2, Delta)>)+'static;
}

impl<G: Scope, K: Data+Default, V: Data+Default> Group<G, K, V> for Collection<G, (K,V)>
where G::Timestamp: LeastUpperBound {
    fn group<L, V2: Data>(&self, logic: L) -> Collection<G, (K,V2)>
        where L: Fn(&K, &mut CollectionIterator<DifferenceIterator<V>>, &mut Vec<(V2, Delta)>)+'static {
            self.group_by_core(
                |x| x,
                |&(ref k,_)| k.hashed(),
                |k| k.hashed(),
                |k,v2| ((*k).clone(), (*v2).clone()),
                |_| HashMap::new(),//RHHMap::new(|x: &K| x.hashed() as usize),
                logic
            )
    }
}

pub trait GroupUnsigned<G: Scope, U: Unsigned+Data+Default, V: Data> : GroupBy<G, (U,V)>
    where G::Timestamp: LeastUpperBound {
    fn group_u<L, V2: Data>(&self, logic: L) -> Collection<G, (U, V2)>
        where L: Fn(&U, &mut CollectionIterator<DifferenceIterator<V>>, &mut Vec<(V2, i32)>)+'static {
            self.group_by_core(
                |x| x,
                |&(ref k,_)| k.as_u64(),
                |k| k.clone(),
                |k, v| (k.clone(), (*v).clone()),
                |x| (Vec::new(), x),
                logic)
    }
}

// implement `GroupBy` for any stream implementing `Unary` and `Map` (most of them).
impl<G: Scope, U: Unsigned+Data+Default, V: Data, S> GroupUnsigned<G, U, V> for S
where G::Timestamp: LeastUpperBound,
      S: GroupBy<G, (U,V)> { }


// implement `GroupBy` for any collection.
impl<G: Scope, D1: Data> GroupBy<G, D1> for Collection<G, D1>
where G::Timestamp: LeastUpperBound {
    fn group_by_u<
        U:     Data+Unsigned+Default,
        V1:    Data,
        V2:    Data,
        D2:    Data,
        KV:    Fn(D1)->(U,V1)+'static,
        Logic: Fn(&U, &mut CollectionIterator<DifferenceIterator<V1>>, &mut Vec<(V2, i32)>)+'static,
        Reduc: Fn(&U, &V2)->D2+'static,
    >
            (&self, kv: KV, reduc: Reduc, logic: Logic) -> Collection<G, D2> {
                self.map(kv)
                    .group_by_core(|x| x,
                                    |&(ref k,_)| k.as_u64(),
                                    |k| k.clone(),
                                    reduc,
                                    |x| (Vec::new(), x),
                                    logic)
    }
}


/// Extension trait for the `group_by` and `group_by_u` differential dataflow methods.
pub trait GroupBy<G: Scope, D1: Data> : GroupByCore<G, D1>
where G::Timestamp: LeastUpperBound {

    /// Groups input records together by key and applies a reduction function.
    ///
    /// `group_by` transforms a stream of records of type `D1` into a stream of records of type `D2`,
    /// by first transforming each input record into a `(key, val): (K, V1)` pair. For each key with
    /// some values, `logic` is invoked on the key and an value enumerator which presents `(V1, i32)`
    /// pairs, indicating for each value its multiplicity. `logic` is expected to populate its third
    /// argument, a `&mut Vec<(V2, i32)>` indicating multiplicities of output records. Finally, for
    /// each `(key,val) : (K,V2)` pair produced, `reduc` is applied to produce an output `D2` record.
    ///
    /// This all may seem overcomplicated, and it may indeed become simpler in the future. For the
    /// moment it is designed to allow as much programmability as possible.
    fn group_by<
        K:     Data,        //  type of the key
        V1:    Data,     //  type of the input value
        V2:    Data,     //  type of the output value
        D2:    Data,                                //  type of the output data
        KV:    Fn(D1)->(K,V1)+'static,              //  function from data to (key,val)
        Part:  Fn(&D1)->u64+'static,                //  partitioning function; should match KH
        U:     Unsigned+Default,
        KH:    Fn(&K)->U+'static,                   //  partitioning function for key; should match Part.

        // user-defined operator logic, from a key and value iterator, populating an output vector.
        Logic: Fn(&K, &mut CollectionIterator<DifferenceIterator<V1>>, &mut Vec<(V2, i32)>)+'static,

        // function from key and output value to output data.
        Reduc: Fn(&K, &V2)->D2+'static,
    >
    (&self, kv: KV, part: Part, key_h: KH, reduc: Reduc, logic: Logic) -> Collection<G, D2> {
        self.group_by_core(kv, part, key_h, reduc, |_| HashMap::new(), logic)
    }

    /// A specialization of the `group_by` method to the case that the key type `K` is an unsigned
    /// integer, and the strategy for indexing by key is simply to index into a vector.
    fn group_by_u<
        U:     Data+Unsigned+Default,
        V1:    Data,
        V2:    Data,
        D2:    Data,
        KV:    Fn(D1)->(U,V1)+'static,
        Logic: Fn(&U, &mut CollectionIterator<DifferenceIterator<V1>>, &mut Vec<(V2, i32)>)+'static,
        Reduc: Fn(&U, &V2)->D2+'static,
    >
            (&self, kv: KV, reduc: Reduc, logic: Logic) -> Collection<G, D2>;
}

pub trait GroupByCore<G: Scope, D1: Data> {

    fn group_by_core<
        K:     Data,
        V1:    Data,
        V2:    Data,
        D2:    Data,
        KV:    Fn(D1)->(K,V1)+'static,
        Part:  Fn(&D1)->u64+'static,
        U:     Unsigned+Default,
        KH:    Fn(&K)->U+'static,
        Look:  Lookup<K, Offset>+'static,
        LookG: Fn(u64)->Look,
        Logic: Fn(&K, &mut CollectionIterator<DifferenceIterator<V1>>, &mut Vec<(V2, i32)>)+'static,
        Reduc: Fn(&K, &V2)->D2+'static,
    >
    (&self, kv: KV, part: Part, key_h: KH, reduc: Reduc, look: LookG, logic: Logic) -> Collection<G, D2>;

}

impl<G: Scope, D1: Data> GroupByCore<G, D1> for Collection<G, D1> where G::Timestamp: LeastUpperBound {

    /// The lowest level `group*` implementation, which is parameterized by the type of storage to
    /// use for mapping keys `K` to `Offset`, an internal `CollectionTrace` type. This method should
    /// probably rarely be used directly.
    fn group_by_core<
        K:     Data,
        V1:    Data,
        V2:    Data,
        D2:    Data,
        KV:    Fn(D1)->(K,V1)+'static,
        Part:  Fn(&D1)->u64+'static,
        U:     Unsigned+Default,
        KH:    Fn(&K)->U+'static,
        Look:  Lookup<K, Offset>+'static,
        LookG: Fn(u64)->Look,
        Logic: Fn(&K, &mut CollectionIterator<DifferenceIterator<V1>>, &mut Vec<(V2, i32)>)+'static,
        Reduc: Fn(&K, &V2)->D2+'static,
    >
    (&self, kv: KV, part: Part, key_h: KH, reduc: Reduc, look: LookG, logic: Logic) -> Collection<G, D2> {

        let peers = self.scope().peers();
        let mut log_peers = 0;
        while (1 << (log_peers + 1)) <= peers {
            log_peers += 1;
        }

        let mut source = BasicTrace::new(look(log_peers));
        let mut result = BasicTrace::new(look(log_peers));

        // A map from times to received (key, val, wgt) triples.
        let mut inputs = Vec::new();

        // A map from times to a list of keys that need processing at that time.
        let mut to_do = Vec::new();

        // temporary storage for operator implementations to populate
        let mut buffer = vec![];
        // let mut heap1 = vec![];
        // let mut heap2 = vec![];

        // create an exchange channel based on the supplied Fn(&D1)->u64.
        let exch = Exchange::new(move |&(ref x,_)| part(x));

        let mut sorter = LSBRadixSorter::new();

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        Collection::new(self.inner.unary_notify(exch, "GroupBy", vec![], move |input, output, notificator| {

            // 1. read each input, and stash it in our staging area
            while let Some((time, data)) = input.next() {
                notificator.notify_at(&time);
                inputs.entry_or_insert(time.clone(), || Vec::new())
                      .push(::std::mem::replace(data.deref_mut(), Vec::new()));
            }

            // 2. go through each time of interest that has reached completion
            // times are interesting either because we received data, or because we conclude
            // in the processing of a time that a future time will be interesting.
            while let Some((index, _count)) = notificator.next() {

                // 2a. fetch any data associated with this time.
                if let Some(mut queue) = inputs.remove_key(&index) {

                    // sort things; radix if many, .sort_by if few.
                    let compact = if queue.len() > 1 {
                        for element in queue.into_iter() {
                            sorter.extend(element.into_iter().map(|(d,w)| (kv(d),w)), &|x| key_h(&(x.0).0));
                        }
                        let mut sorted = sorter.finish(&|x| key_h(&(x.0).0));
                        let result = Compact::from_radix(&mut sorted, &|k| key_h(k));
                        sorted.truncate(256);
                        sorter.recycle(sorted);
                        result
                    }
                    else {
                        let mut vec = queue.pop().unwrap();
                        let mut vec = vec.drain(..).map(|(d,w)| (kv(d),w)).collect::<Vec<_>>();
                        vec.sort_by(|x,y| key_h(&(x.0).0).cmp(&key_h((&(y.0).0))));
                        Compact::from_radix(&mut vec![vec], &|k| key_h(k))
                    };

                    if let Some(compact) = compact {

                        let mut stash = Vec::new();
                        for key in &compact.keys {
                            stash.push(index.clone());
                            source.interesting_times(key, &index, &mut stash);
                            for time in &stash {
                                let mut queue = to_do.entry_or_insert((*time).clone(), || { notificator.notify_at(time); Vec::new() });
                                queue.push((*key).clone());
                            }
                            stash.clear();
                        }

                        // add the accumulation to the trace source.
                        // println!("group1");
                        source.set_difference(index.clone(), compact);
                    }
                }

                // 2b. We must now determine for each interesting key at this time, how does the
                // currently reported output match up with what we need as output. Should we send
                // more output differences, and what are they?

                if let Some(mut keys) = to_do.remove_key(&index) {

                    // we may need to produce output at index
                    let mut session = output.session(&index);

                    // we would like these keys in a particular order.
                    keys.sort_by(|x,y| (key_h(&x), x).cmp(&(key_h(&y), y)));
                    keys.dedup();

                    // accumulations for installation into result
                    let mut accumulation = Compact::new(0,0);

                    for key in keys {

                        // acquire an iterator over the collection at `time`.
                        let mut input = source.get_collection(&key, &index);

                        // if we have some data, invoke logic to populate self.dst
                        if input.peek().is_some() { logic(&key, &mut input, &mut buffer); }

                        buffer.sort_by(|x,y| x.0.cmp(&y.0));

                        // push differences in to Compact.
                        let mut compact = accumulation.session();
                        for (val, wgt) in Coalesce::coalesce(result.get_collection(&key, &index)
                                                                   .map(|(v, w)| (v,-w))
                                                                   .merge_by(buffer.iter().map(|&(ref v, w)| (v, w)), |x,y| {
                                                                        x.0 <= y.0
                                                                   }))
                        {
                            let result = (reduc(&key, val), wgt);
                            session.give(result);
                            compact.push(val.clone(), wgt);
                        }
                        compact.done(key);
                        buffer.clear();
                    }

                    if accumulation.vals.len() > 0 {
                        result.set_difference(index.clone(), accumulation);
                    }
                }
            }
        }))
    }
}
