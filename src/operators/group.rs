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
// use std::hash::Hasher;
use std::collections::HashMap;
use std::ops::DerefMut;

use itertools::Itertools;

use ::{Data, Collection, Delta};
use timely::dataflow::*;
use timely::dataflow::operators::{Map, Unary};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::drain::DrainExt;
use timely::progress::Timestamp;

use collection::{LeastUpperBound, Lookup, Trace, Offset};
use collection::trace::{CollectionIterator, DifferenceIterator, Traceable, TraceRef};

use iterators::coalesce::Coalesce;
use radix_sort::{RadixSorter, Unsigned};
// use collection::robin_hood::RHHMap;
use collection::compact::Compact;
use operators::arrange::{Arranged, ArrangeByKey, ArrangeBySelf};

type ColIter<'a,K,T,V,L> = CollectionIterator<<&'a Trace<K,T,V,L> as TraceRef<'a,K,T,V>>::VIterator>;

/// Extension trait for the `group` differential dataflow method
pub trait Group<G: Scope, K: Data, V: Data>
    where G::Timestamp: LeastUpperBound {

    /// Groups records by their first field, and applies reduction logic to the associated values.
    fn group<L, V2: Data>(&self, logic: L) -> Collection<G,(K,V2)>
        where L: for<'a> Fn(&K, ColIter<'a,K,G::Timestamp,V,HashMap<K,Offset>>, &mut Vec<(V2, i32)>)+'static;
        // where L: for<'a> Fn(&K, CollectionIterator<DifferenceIterator<'a,V>>, &mut Vec<(V2, i32)>)+'static;

}

impl<G: Scope, K: Data+Default, V: Data+Default> Group<G, K, V> for Collection<G,(K,V)>
where G::Timestamp: LeastUpperBound { 

    fn group<L, V2: Data>(&self, logic: L) -> Collection<G,(K,V2)>
        where L: for<'a> Fn(&K, ColIter<'a,K,G::Timestamp,V,HashMap<K,Offset>>, &mut Vec<(V2, i32)>)+'static {
        // where L: for<'a> Fn(&K, CollectionIterator<DifferenceIterator<'a,V>>, &mut Vec<(V2, i32)>)+'static {

        self.arrange_by_key(|k| k.hashed(), |_| HashMap::new())
            .group(Trace::new(HashMap::new()), logic)
    }
}


pub trait GroupUnsigned<G: Scope, U: Unsigned+Data+Default, V: Data>
    where G::Timestamp: LeastUpperBound {
    fn group_u<L, V2: Data>(&self, logic: L) -> Collection<G,(U,V2)>
        where L: for<'a> Fn(&U, ColIter<'a,U,G::Timestamp,V,(Vec<Option<Offset>>, u64)>, &mut Vec<(V2, i32)>)+'static;
}

// implement `GroupBy` for any stream implementing `Unary` and `Map` (most of them).
impl<G: Scope, U: Unsigned+Data+Default, V: Data> GroupUnsigned<G, U, V> for Collection<G, (U,V)>
where G::Timestamp: LeastUpperBound { 

    fn group_u<L, V2: Data>(&self, logic: L) -> Collection<G,(U,V2)>
        where L: for<'a> Fn(&U, ColIter<'a,U,G::Timestamp,V,(Vec<Option<Offset>>, u64)>, &mut Vec<(V2, i32)>)+'static {

        let peers = self.scope().peers();
        let mut log_peers = 0;
        while (1 << (log_peers + 1)) <= peers {
            log_peers += 1;
        }

        self.arrange_by_key(|k| k.clone(), |x| (Vec::new(), x))
            .group(Trace::new((Vec::new(), log_peers)), logic)
    }
}


pub trait GroupArranged<G: Scope, T: Traceable<Index=G::Timestamp>> 
where 
    G::Timestamp: LeastUpperBound,
    for<'a> &'a T: TraceRef<'a,T::Key,T::Index,T::Value>, {
    /// Matches the elements of two arranged traces.
    ///
    /// The arrangements must have matching keys and indices, but the values may be arbitrary.
    fn group<T2,Logic> (&self, trace: T2, logic: Logic) -> Collection<G,(T2::Key,T2::Value)>
    where 
        T2: Traceable<Key=T::Key,Index=G::Timestamp>+'static,
        for<'a> &'a T2: TraceRef<'a,T2::Key,T2::Index,T2::Value>,
        Logic: for<'a> Fn(
            &T::Key, 
            CollectionIterator<<&'a T as TraceRef<'a,T::Key,T::Index,T::Value>>::VIterator>, 
            &mut Vec<(T2::Value, i32)>
        )+'static;
}

impl<TS,G,T> GroupArranged<G,T> for Arranged<G,T> 
    where
        TS: Timestamp,
        G: Scope<Timestamp=TS>,
        G::Timestamp: LeastUpperBound, 
        T: Traceable<Index=TS>+'static,
        for<'a> &'a T: TraceRef<'a,T::Key,T::Index,T::Value> {
    fn group<T2,Logic> (&self, trace: T2, logic: Logic) -> Collection<G,(T2::Key,T2::Value)>
    where 
        T2: Traceable<Key=T::Key,Index=T::Index>+'static,
        for<'a> &'a T2: TraceRef<'a,T2::Key,T2::Index,T2::Value>,
        Logic: for<'a> Fn(
            &T::Key, 
            CollectionIterator<<&'a T as TraceRef<'a,T::Key,T::Index,T::Value>>::VIterator>, 
            &mut Vec<(T2::Value, i32)>
        )+'static {

        let source = self.trace.clone();
        let mut result = trace;

        let mut inputs = Vec::new();    // A map from times to received (key, val, wgt) triples.
        let mut to_do = Vec::new();     // A map from times to a list of keys that need processing at that time.
        let mut buffer = vec![];        // temporary storage for operator implementations to populate

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        self.stream.unary_notify(Pipeline, "Group", vec![], move |input, output, notificator| {

            // read input, push all data to queues
            while let Some((time, data)) = input.next() {
                assert!(data.len() == 1);
                notificator.notify_at(&time);
                inputs.entry_or_insert(time.clone(), || data.drain_temp().next().unwrap());
            }

            // a notification means we may have input, and keys to validate.
            while let Some((index, _count)) = notificator.next() {

                // scan input keys and determine interesting times.
                // TODO : we aren't filtering by acknowledged time, so there
                // TODO : may be some (large?) amount of redundancy here.
                if let Some((keys,_,_)) = inputs.remove_key(&index) {
                    let borrow = source.borrow();
                    let mut stash = Vec::new();
                    for key in keys {
                        stash.push(index.clone());
                        borrow.interesting_times(&key, &index, &mut stash);
                        for time in stash.drain_temp() {
                            to_do.entry_or_insert(time, || { notificator.notify_at(&time); Vec::new() })
                                 .push(key.clone());
                        }
                    }
                }

                // ensure `output == logic(input)` for keys.
                if let Some(mut keys) = to_do.remove_key(&index) {

                    let mut borrow = source.borrow_mut();
    
                    // we may need to produce output at index
                    let mut session = output.session(&index);

                    // the keys *must* be distinct, and should be in some order.
                    keys.sort();    // keys.sort_by(|x,y| (key_h(&x), x).cmp(&(key_h(&y), y)));
                    keys.dedup();

                    // accumulations for installation into result
                    let mut accumulation = Compact::new(0,0);

                    for key in keys {

                        // if the input is non-empty, invoke logic to populate buffer.
                        let mut input = borrow.get_collection(&key, &index);
                        if input.peek().is_some() { 
                            logic(&key, input, &mut buffer);
                            // don't trust `logic` to sort the results.
                            buffer.sort_by(|x,y| x.0.cmp(&y.0));    
                        }
                        
                        // push differences in to Compact.
                        let mut compact = accumulation.session();
                        for (val, wgt) in Coalesce::coalesce(
                            result.get_collection(&key, &index)
                                  .map(|(v, w)| (v,-w))
                                  .merge_by(buffer.iter().map(|&(ref v, w)| (v, w)), |x,y| {
                                      x.0 <= y.0
                                  }))
                        {
                            session.give(((key.clone(), val.clone()), wgt));
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
        })
    }
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
    (&self, kv: KV, part: Part, key_h: KH, reduc: Reduc, look: LookG, logic: Logic) -> Stream<G, (D2, i32)>;

}

impl<G: Scope, D1: Data> GroupByCore<G, D1> for Stream<G, (D1, i32)> where G::Timestamp: LeastUpperBound {

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
    (&self, kv: KV, part: Part, key_h: KH, reduc: Reduc, look: LookG, logic: Logic) -> Stream<G, (D2, i32)> {

        let peers = self.scope().peers();
        let mut log_peers = 0;
        while (1 << (log_peers + 1)) <= peers {
            log_peers += 1;
        }

        let mut source = Trace::new(look(log_peers));
        let mut result = Trace::new(look(log_peers));

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

        let mut sorter = RadixSorter::new();

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        self.unary_notify(exch, "GroupBy", vec![], move |input, output, notificator| {

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
                        let mut vec = vec.drain_temp().map(|(d,w)| (kv(d),w)).collect::<Vec<_>>();
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
        })
    }
}
