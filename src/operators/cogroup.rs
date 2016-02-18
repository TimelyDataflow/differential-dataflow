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
//!     let (mut max_val, mut max_wgt) = vals.peek().unwrap();
//!     for (val, wgt) in vals {
//!         if wgt > max_wgt {
//!             max_wgt = wgt;
//!             max_val = val;
//!         }
//!     }
//!     output.push((max_val.clone(), max_wgt));
//! })
//! ```

use std::rc::Rc;
use std::default::Default;
use std::hash::Hasher;
use std::ops::DerefMut;

use itertools::Itertools;

use ::{Collection, Data};
use timely::dataflow::*;
use timely::dataflow::operators::{Map, Binary};
use timely::dataflow::channels::pact::Exchange;
use timely_sort::{LSBRadixSorter, Unsigned};

use collection::{LeastUpperBound, Lookup, Trace, Offset};
use collection::trace::{CollectionIterator, DifferenceIterator, Traceable};

use iterators::coalesce::Coalesce;
use collection::compact::Compact;

/// Extension trait for the `group_by` and `group_by_u` differential dataflow methods.
pub trait CoGroupBy<G: Scope, K: Data, V1: Data> where G::Timestamp: LeastUpperBound {

    /// A primitive binary version of `group_by`, which acts on a `Collection<G, (K, V1)>` and a `Collection<G, (K, V2)>`.
    ///
    /// The two streams must already be key-value pairs, which is too bad. Also, in addition to the
    /// normal arguments (another stream, a hash for the key, a reduction function, and per-key logic),
    /// the user must specify a function implmenting `Fn(u64) -> Look`, where `Look: Lookup<K, Offset>` is something you shouldn't have to know about yet.
    /// The right thing to use here, for the moment, is `|_| HashMap::new()`.
    ///
    /// There are better options if you know your key is an unsigned integer, namely `|x| (Vec::new(), x)`.
    fn cogroup_by_inner<
        D:     Data,
        V2:    Data+Default,
        V3:    Data+Default,
        U:     Unsigned+Default,
        KH:    Fn(&K)->U+'static,
        Look:  Lookup<K, Offset>+'static,
        LookG: Fn(u64)->Look,
        Logic: Fn(&K, &mut CollectionIterator<DifferenceIterator<V1>>, &mut CollectionIterator<DifferenceIterator<V2>>, &mut Vec<(V3, i32)>)+'static,
        Reduc: Fn(&K, &V3)->D+'static,
    >
    (&self, other: &Collection<G, (K, V2)>, key_h: KH, reduc: Reduc, look: LookG, logic: Logic) -> Collection<G, D>;
}

impl<G: Scope, K: Data, V1: Data> CoGroupBy<G, K, V1> for Collection<G, (K, V1)>
where G::Timestamp: LeastUpperBound {
    fn cogroup_by_inner<
        D:     Data,
        V2:    Data+Default,
        V3:    Data+Default,
        U:     Unsigned+Default,
        KH:    Fn(&K)->U+'static,
        Look:  Lookup<K, Offset>+'static,
        LookG: Fn(u64)->Look,
        Logic: Fn(&K, &mut CollectionIterator<V1>, &mut CollectionIterator<V2>, &mut Vec<(V3, i32)>)+'static,
        Reduc: Fn(&K, &V3)->D+'static,
    >
    (&self, other: &Collection<G, (K, V2)>, key_h: KH, reduc: Reduc, look: LookG, logic: Logic) -> Collection<G, D> {

        let mut source1 = Trace::new(look(0));
        let mut source2 = Trace::new(look(0));
        let mut result = Trace::new(look(0));

        // A map from times to received (key, val, wgt) triples.
        let mut inputs1 = Vec::new();
        let mut inputs2 = Vec::new();

        // A map from times to a list of keys that need processing at that time.
        let mut to_do = Vec::new();

        // temporary storage for operator implementations to populate
        let mut buffer = vec![];

        let key_h = Rc::new(key_h);
        let key_1 = key_h.clone();
        let key_2 = key_h.clone();

        // create an exchange channel based on the supplied Fn(&D1)->u64.
        let exch1 = Exchange::new(move |&((ref k, _),_)| key_1(k).as_u64());
        let exch2 = Exchange::new(move |&((ref k, _),_)| key_2(k).as_u64());

        let mut sorter1 = LSBRadixSorter::new();
        let mut sorter2 = LSBRadixSorter::new();

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        Collection::new(self.inner.binary_notify(&other.inner, exch1, exch2, "CoGroupBy", vec![], move |input1, input2, output, notificator| {

            // 1. read each input, and stash it in our staging area
            while let Some((time, data)) = input1.next() {
                inputs1.entry_or_insert(time.time(), || Vec::new())
                       .push(::std::mem::replace(data.deref_mut(), Vec::new()));
                notificator.notify_at(time);
            }

            // 1. read each input, and stash it in our staging area
            while let Some((time, data)) = input2.next() {
                inputs2.entry_or_insert(time.time(), || Vec::new())
                       .push(::std::mem::replace(data.deref_mut(), Vec::new()));
                notificator.notify_at(time);
            }

            // 2. go through each time of interest that has reached completion
            // times are interesting either because we received data, or because we conclude
            // in the processing of a time that a future time will be interesting.
            while let Some((index, _count)) = notificator.next() {

                let mut stash = Vec::new();

                panic!("interesting times needs to do LUB of union of times for each key, input");

                // 2a. fetch any data associated with this time.
                if let Some(mut queue) = inputs1.remove_key(&index) {

                    // sort things; radix if many, .sort_by if few.
                    let compact = if queue.len() > 1 {
                        for element in queue.into_iter() {
                            sorter1.extend(element.into_iter(), &|x| key_h(&(x.0).0));
                        }
                        let mut sorted = sorter1.finish(&|x| key_h(&(x.0).0));
                        let result = Compact::from_radix(&mut sorted, &|k| key_h(k));
                        sorted.truncate(256);
                        sorter1.recycle(sorted);
                        result
                    }
                    else {
                        let mut vec = queue.pop().unwrap();
                        let mut vec = vec.drain(..).collect::<Vec<_>>();
                        vec.sort_by(|x,y| key_h(&(x.0).0).cmp(&key_h((&(y.0).0))));
                        Compact::from_radix(&mut vec![vec], &|k| key_h(k))
                    };

                    if let Some(compact) = compact {

                        for key in &compact.keys {
                            stash.push(index.clone());
                            source1.interesting_times(key, &index, &mut stash);
                            for time in &stash {
                                let mut queue = to_do.entry_or_insert((*time).clone(), || { notificator.notify_at(index.delayed(time)); Vec::new() });
                                queue.push((*key).clone());
                            }
                            stash.clear();
                        }

                        source1.set_difference(index.time(), compact);
                    }
                }

                // 2a. fetch any data associated with this time.
                if let Some(mut queue) = inputs2.remove_key(&index) {

                    // sort things; radix if many, .sort_by if few.
                    let compact = if queue.len() > 1 {
                        for element in queue.into_iter() {
                            sorter2.extend(element.into_iter(), &|x| key_h(&(x.0).0));
                        }
                        let mut sorted = sorter2.finish(&|x| key_h(&(x.0).0));
                        let result = Compact::from_radix(&mut sorted, &|k| key_h(k));
                        sorted.truncate(256);
                        sorter2.recycle(sorted);
                        result
                    }
                    else {
                        let mut vec = queue.pop().unwrap();
                        let mut vec = vec.drain(..).collect::<Vec<_>>();
                        vec.sort_by(|x,y| key_h(&(x.0).0).cmp(&key_h((&(y.0).0))));
                        Compact::from_radix(&mut vec![vec], &|k| key_h(k))
                    };

                    if let Some(compact) = compact {

                        for key in &compact.keys {
                            stash.push(index.clone());
                            source2.interesting_times(key, &index, &mut stash);
                            for time in &stash {
                                let mut queue = to_do.entry_or_insert((*time).clone(), || { notificator.notify_at(index.delayed(time)); Vec::new() });
                                queue.push((*key).clone());
                            }
                            stash.clear();
                        }

                        source2.set_difference(index.time(), compact);
                    }
                }

                // we may need to produce output at index
                let mut session = output.session(&index);


                    // 2b. We must now determine for each interesting key at this time, how does the
                    // currently reported output match up with what we need as output. Should we send
                    // more output differences, and what are they?

                // Much of this logic used to hide in `OperatorTrace` and `CollectionTrace`.
                // They are now gone and simpler, respectively.
                if let Some(mut keys) = to_do.remove_key(&index) {

                    // we would like these keys in a particular order.
                    // TODO : use a radix sort since we have `key_h`.
                    keys.sort_by(|x,y| (key_h(&x), x).cmp(&(key_h(&y), y)));
                    keys.dedup();

                    // accumulations for installation into result
                    let mut accumulation = Compact::new(0,0);

                    for key in keys {

                        // acquire an iterator over the collection at `time`.
                        let mut input1 = source1.get_collection(&key, &index);
                        let mut input2 = source2.get_collection(&key, &index);

                        // if we have some data, invoke logic to populate self.dst
                        if input1.peek().is_some() || input2.peek().is_some() { logic(&key, &mut input1, &mut input2, &mut buffer); }

                        buffer.sort_by(|x,y| x.0.cmp(&y.0));

                        // push differences in to Compact.
                        let mut compact = accumulation.session();
                        for (val, wgt) in Coalesce::coalesce(result.get_collection(&key, &index)
                                                                   .map(|(v, w)| (v,-w))
                                                                   .merge_by(buffer.iter().map(|&(ref v, w)| (v, w)), |x,y| {
                                                                        x.0 <= y.0
                                                                   }))
                        {
                            session.give((reduc(&key, val), wgt));
                            compact.push(val.clone(), wgt);
                        }
                        compact.done(key);
                        buffer.clear();
                    }

                    if accumulation.vals.len() > 0 {
                        // println!("group2");
                        result.set_difference(index.time(), accumulation);
                    }
                }
            }
        }))
    }
}
