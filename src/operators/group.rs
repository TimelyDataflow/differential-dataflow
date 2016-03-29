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

use rc::Rc;
use std::cell::RefCell;
use std::default::Default;
use std::collections::HashMap;

use itertools::Itertools;
use linear_map::LinearMap;
use vec_map::VecMap;

use ::{Data, Collection, Delta};
use timely::dataflow::*;
use timely::dataflow::operators::{Map, Unary};
use timely::dataflow::channels::pact::Pipeline;
use timely_sort::Unsigned;

use collection::{LeastUpperBound, Lookup, Trace, BasicTrace, Offset};
use collection::trace::CollectionIterator;
use collection::basic::DifferenceIterator;
use collection::compact::Compact;

use iterators::coalesce::Coalesce;

use operators::arrange::{Arranged, ArrangeByKey};

/// Extension trait for the `group` differential dataflow method
pub trait Group<G: Scope, K: Data, V: Data>
    where G::Timestamp: LeastUpperBound {

    /// Groups records by their first field, and applies reduction logic to the associated values.
    ///
    /// It would be nice for this to be generic over possible arrangements of data, but it seems that Rust
    /// ICEs when I try this. I'm not exactly sure how one would specify the arrangement (the type is used
    /// in `logic`, by way of its associated iterator types, but not clearly enough to drive type inference),
    /// but we don't have that problem yet anyhow.
    ///
    /// In any case, it would be great if the same implementation could handle `(K,V)` pairs and `K` elements
    /// treated as `(K, ())` pairs.
    fn group<L, V2: Data>(&self, logic: L) -> Collection<G, (K,V2)>
        where L: Fn(&K, &mut CollectionIterator<DifferenceIterator<V>>, &mut Vec<(V2, Delta)>)+'static;
}

impl<G: Scope, K: Data+Default, V: Data+Default> Group<G, K, V> for Collection<G, (K,V)>
where G::Timestamp: LeastUpperBound 
{
    fn group<L, V2: Data>(&self, logic: L) -> Collection<G, (K,V2)>
        where L: Fn(&K, &mut CollectionIterator<DifferenceIterator<V>>, &mut Vec<(V2, Delta)>)+'static {
            self.arrange_by_key(|k| k.hashed(), |_| HashMap::new())
                .group(|k| k.hashed(), |_| HashMap::new(), logic)
                .as_collection()
    }
}


/// Extension trait for the `group_u` differential dataflow method.
pub trait GroupUnsigned<G: Scope, U: Unsigned+Data+Default, V: Data>
    where G::Timestamp: LeastUpperBound {
    /// Groups records by their first field, when this field implements `Unsigned`. 
    ///
    /// This method uses a `Vec<Option<_>>` as its internal storage, allocating
    /// enough memory to directly index based on the first fields. This can be 
    /// very useful when these fields are small integers, but it can be expensive
    /// if they are large and sparse.
    fn group_u<L, V2: Data>(&self, logic: L) -> Collection<G, (U, V2)>
        where L: Fn(&U, &mut CollectionIterator<DifferenceIterator<V>>, &mut Vec<(V2, i32)>)+'static;
}

impl<G: Scope, U: Unsigned+Data+Default, V: Data> GroupUnsigned<G, U, V> for Collection<G, (U,V)> 
where G::Timestamp: LeastUpperBound {
    fn group_u<L, V2: Data>(&self, logic: L) -> Collection<G, (U, V2)>
        where L: Fn(&U, &mut CollectionIterator<DifferenceIterator<V>>, &mut Vec<(V2, i32)>)+'static {
            self.arrange_by_key(|k| k.as_u64(), |x| (VecMap::new(), x))
                .group(|k| k.as_u64(), |x| (VecMap::new(), x), logic)
                .as_collection()
        }
}


/// Extension trait for the `group` operator on `Arrange<_>` data.
pub trait GroupArranged<G: Scope, K: Data, V: Data> {
    /// Groups arranged data using a key hash function, a lookup generator, and user logic.
    ///
    /// This method is used by `group` and `group_u` as defined on `Collection`, and it can
    /// also be called directly by user code that wants access to the arranged output. This
    /// can be helpful when the resulting data are re-used immediately with the same keys. 
    fn group<V2, U, KH, Look, LookG, Logic>(&self, key_h: KH, look: LookG, logic: Logic) 
        -> Arranged<G, BasicTrace<K,G::Timestamp,V2,Look>> 
    where
        G::Timestamp: LeastUpperBound,
        V2:    Data,
        U:     Unsigned+Default,
        KH:    Fn(&K)->U+'static,
        Look:  Lookup<K, Offset>+'static,
        LookG: Fn(u64)->Look,
        Logic: Fn(&K, &mut CollectionIterator<DifferenceIterator<V>>, &mut Vec<(V2, i32)>)+'static;
}

impl<G, K, V, L> GroupArranged<G, K, V> for Arranged<G, BasicTrace<K, G::Timestamp, V, L>>
where 
    G: Scope,
    K: Data,
    V: Data,
    L: Lookup<K, Offset>+'static,
    G::Timestamp: LeastUpperBound {

    fn group<V2, U, KH, Look, LookG, Logic>(&self, key_h: KH, look: LookG, logic: Logic) 
        -> Arranged<G, BasicTrace<K,G::Timestamp,V2,Look>> 
    where
        V2:    Data,
        U:     Unsigned+Default,
        KH:    Fn(&K)->U+'static,
        Look:  Lookup<K, Offset>+'static,
        LookG: Fn(u64)->Look,
        Logic: Fn(&K, &mut CollectionIterator<DifferenceIterator<V>>, &mut Vec<(V2, i32)>)+'static {

        let peers = self.stream.scope().peers();
        let mut log_peers = 0;
        while (1 << (log_peers + 1)) <= peers {
            log_peers += 1;
        }

        let source = self.trace.clone();
        let result = Rc::new(RefCell::new(BasicTrace::new(look(log_peers))));
        let target = result.clone();

        // A map from times to received (key, val, wgt) triples.
        let mut inputs = LinearMap::new();

        // A map from times to a list of keys that need processing at that time.
        let mut to_do = LinearMap::new();

        // temporary storage for operator implementations to populate
        let mut buffer = vec![];

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        let stream = self.stream.unary_notify(Pipeline, "GroupArranged", vec![], move |input, output, notificator| {

            // 1. read each input, and stash it in our staging area.
            // only stash the keys, because vals etc are in self.trace
            while let Some((time, data)) = input.next() {
                inputs.entry_or_insert(time.time(), || { notificator.notify_at(time); data.drain(..).next().unwrap().0 });
            }

            // 2. go through each time of interest that has reached completion
            // times are interesting either because we received data, or because we conclude
            // in the processing of a time that a future time will be interesting.
            while let Some((capability, _count)) = notificator.next() {

                let time = capability.time();

                // 2a. If we received any keys, determine the interesting times for each.
                //     We then enqueue the keys at the corresponding time, for use later.
                if let Some(queue) = inputs.remove_key(&time) {

                    let source = source.borrow();
                    let mut stash = Vec::new();
                    for key in queue {
                        if source.get_difference(&key, &time).is_some() {
                            stash.push(capability.time());
                            source.interesting_times(&key, &time, &mut stash);
                            for new_time in &stash {
                                to_do.entry_or_insert(time.clone(), || { notificator.notify_at(capability.delayed(new_time)); Vec::new() })
                                     .push(key.clone());
                            }
                            stash.clear();
                        }
                    }
                }

                // 2b. Process any interesting keys at this time.
                if let Some(mut keys) = to_do.remove_key(&time) {

                    // We would like these keys in a particular order. 
                    // We also want to de-duplicate them, in case there are dupes. 
                    keys.sort_by(|x,y| (key_h(&x), x).cmp(&(key_h(&y), y)));
                    keys.dedup();

                    // accumulations for installation into result
                    let mut accumulation = Compact::new(0,0);

                    // borrow `source` to avoid constant re-borrowing.
                    let mut source_borrow = source.borrow_mut();

                    for key in keys {

                        // acquire an iterator over the collection at `time`.
                        let mut input = source_borrow.get_collection(&key, &time);

                        // if we have some input data, invoke logic to populate buffer.
                        if input.peek().is_some() { logic(&key, &mut input, &mut buffer); }

                        // sort the buffer, because we can't trust user code to do that.
                        buffer.sort_by(|x,y| x.0.cmp(&y.0));

                        // push differences in to Compact.
                        let mut compact = accumulation.session();
                        for (val, wgt) in Coalesce::coalesce(target.borrow_mut()
                                                                   .get_collection(&key, &time)
                                                                   .map(|(v, w)| (v,-w))
                                                                   .merge_by(buffer.iter().map(|&(ref v, w)| (v, w)), |x,y| {
                                                                        x.0 <= y.0
                                                                   }))
                        {
                            compact.push(val.clone(), wgt);
                        }
                        compact.done(key);
                        buffer.clear();
                    }

                    if accumulation.vals.len() > 0 {
                        output.session(&capability).give((accumulation.keys.clone(), accumulation.cnts.clone(), accumulation.vals.clone()));
                        target.borrow_mut().set_difference(time, accumulation);
                    }
                }
            }
        });

        Arranged { stream: stream, trace: result }
    }
}
