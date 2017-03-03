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

use std::rc::Rc;
use std::cell::RefCell;
use std::borrow::Borrow;

use linear_map::LinearMap;

use hashable::{Hashable, UnsignedWrapper};
use ::{Data, Collection, Delta};

use timely::progress::Antichain;
use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely_sort::Unsigned;

use operators::arrange::{Arrange, Arranged, ArrangeByKey, ArrangeBySelf, BatchWrapper, TraceHandle, TraceWrapper};
use lattice::Lattice;
use trace::{Batch, Cursor, Trace, Builder};
// use trace::implementations::trie::Spine as OrdSpine;
// use trace::implementations::keys::Spine as KeysSpine;
use trace::implementations::rhh::Spine as HashSpine;
// use trace::implementations::rhh::HashWrapper;
use trace::implementations::rhh_k::Spine as KeyHashSpine;

// use hashable::{HashableWrapper, UnsignedWrapper};

/// Extension trait for the `group` differential dataflow method.
pub trait Group<G: Scope, K: Data, V: Data> where G::Timestamp: Lattice+Ord {
    /// Groups records by their first field, and applies reduction logic to the associated values.
    fn group<L, V2: Data>(&self, logic: L) -> Collection<G, (K, V2)>
        where L: Fn(&K, &[(V, Delta)], &mut Vec<(V2, Delta)>)+'static;
    /// Groups records by their first field, and applies reduction logic to the associated values.
    fn group_u<L, V2: Data>(&self, logic: L) -> Collection<G, (K, V2)>
        where L: Fn(&K, &[(V, Delta)], &mut Vec<(V2, Delta)>)+'static, K: Unsigned+Copy;
}

impl<G: Scope, K: Data+Default+Hashable, V: Data> Group<G, K, V> for Collection<G, (K, V)> 
    where G::Timestamp: Lattice+Ord+::std::fmt::Debug {
    fn group<L, V2: Data>(&self, logic: L) -> Collection<G, (K, V2)>
        where L: Fn(&K, &[(V, Delta)], &mut Vec<(V2, Delta)>)+'static {
        self.arrange_by_key()
            .group_arranged(move |k,s,t| logic(&k.item,s,t), HashSpine::new(Default::default()))
            .as_collection(|k,v| (k.item.clone(), v.clone()))
    }
    fn group_u<L, V2: Data>(&self, logic: L) -> Collection<G, (K, V2)>
        where L: Fn(&K, &[(V, Delta)], &mut Vec<(V2, Delta)>)+'static, K: Unsigned+Copy {
        self.arrange(|k,v| (UnsignedWrapper::from(k), v), HashSpine::new(Default::default()))
            .group_arranged(move |k,s,t| logic(&k.item,s,t), HashSpine::new(Default::default()))
            .as_collection(|k,v| (k.item.clone(), v.clone()))
    }
}

/// Extension trait for the `distinct` differential dataflow method.
pub trait Distinct<G: Scope, K: Data> where G::Timestamp: Lattice+Ord {
    /// Reduces the collection to one occurrence of each distinct element.
    fn distinct(&self) -> Collection<G, K>;
    /// Reduces the collection to one occurrence of each distinct element.
    fn distinct_u(&self) -> Collection<G, K> where K: Unsigned+Copy;
}

impl<G: Scope, K: Data+Default+Hashable> Distinct<G, K> for Collection<G, K> where G::Timestamp: Lattice+Ord+::std::fmt::Debug {
    fn distinct(&self) -> Collection<G, K> {
        self.arrange_by_self()
            .group_arranged(|_k,_s,t| t.push(((), 1)), KeyHashSpine::new(Default::default()))
            .as_collection(|k,_| k.item.clone())
    }
    fn distinct_u(&self) -> Collection<G, K> where K: Unsigned+Copy {
        self.map(|k| (k,()))
            .arrange(|k,v| (UnsignedWrapper::from(k), v), KeyHashSpine::new(Default::default()))
            .group_arranged(|_k,_s,t| t.push(((), 1)), KeyHashSpine::new(Default::default()))
            .as_collection(|k,_| k.item.clone())
    }
}


/// Extension trait for the `count` differential dataflow method.
pub trait Count<G: Scope, K: Data> where G::Timestamp: Lattice+Ord {
    /// Counts the number of occurrences of each element.
    fn count(&self) -> Collection<G, (K, isize)>;
    /// Counts the number of occurrences of each element.
    fn count_u(&self) -> Collection<G, (K, isize)> where K: Unsigned+Copy;
}

impl<G: Scope, K: Data+Default+Hashable> Count<G, K> for Collection<G, K> where G::Timestamp: Lattice+Ord+::std::fmt::Debug {
    fn count(&self) -> Collection<G, (K, isize)> {
        self.arrange_by_self()
            .group_arranged(|_k,s,t| t.push((s[0].1, 1)), HashSpine::new(Default::default()))
            .as_collection(|k,&c| (k.item.clone(), c))
    }
    fn count_u(&self) -> Collection<G, (K, isize)> where K: Unsigned+Copy {
        self.map(|k| (k,()))
            .arrange(|k,v| (UnsignedWrapper::from(k), v), KeyHashSpine::new(Default::default()))
            .group_arranged(|_k,s,t| t.push((s[0].1, 1)), HashSpine::new(Default::default()))
            .as_collection(|k,&c| (k.item.clone(), c))
    }
}


/// Extension trace for the group_arranged differential dataflow method.
pub trait GroupArranged<G: Scope, K: Data, V: Data> where G::Timestamp: Lattice+Ord {
    /// Applies `group` to arranged data, and returns an arrangement of output data.
    fn group_arranged<L, V2, T2>(&self, logic: L, empty: T2) -> Arranged<G, K, V2, T2>
        where
            V2: Data,
            T2: Trace<K, V2, G::Timestamp>+'static,
            L: Fn(&K, &[(V, Delta)], &mut Vec<(V2, Delta)>)+'static
            ; 
}

impl<G: Scope, K: Data, V: Data, T1: Trace<K, V, G::Timestamp>+'static> GroupArranged<G, K, V> for Arranged<G, K, V, T1>
where 
    G::Timestamp: Lattice+Ord, 
    // T1::Batch: Clone+'static,
    // T1::Key: Clone+Ord+Borrow<K>+From<K>,
    // T1::Val: Borrow<V>+From<V>,
    // T1::Cursor: ::std::fmt::Debug
{
    fn group_arranged<L, V2, T2>(&self, logic: L, empty: T2) -> Arranged<G, K, V2, T2>
        where 
            V2: Data,
            T2: Trace<K, V2, G::Timestamp>+'static,
            L: Fn(&K, &[(V, Delta)], &mut Vec<(V2, Delta)>)+'static {

        let mut source_trace = self.new_handle();
        let result_trace = Rc::new(RefCell::new(TraceWrapper::new(empty)));
        let mut output_trace = TraceHandle::new(&result_trace);

        let mut to_do = LinearMap::new();   // A map from times to a list of keys that need processing at that time.
        let mut capabilities = Vec::new();  // notificator replacement (for capabilities).

        let mut time_stage = Vec::new();    // staging for building up interesting times.
        let mut input_stage = Vec::new();   // staging for per-key input (pre-iterator).
        let mut output_stage = Vec::new();  // staging for per-key output.

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        let stream = self.stream.unary_notify(Pipeline, "GroupArrange", vec![], move |input, output, notificator| {

            // println!("hello!");

            // 1. read each input, and stash it in our staging area.
            // We expect *exactly one* record for each time, as we are getting arranged batches.
            input.for_each(|time, data| {
                // replace existing entry with the received batch.
                capabilities.retain(|x: &(Capability<G::Timestamp>, _)| x.0.time() != time.time());
                capabilities.push((time, data.drain(..).next()));
            });

            // 2. go through each time of interest that has reached completion. 
            // times are interesting if we have received data for the time, or
            // if we have determined that some keys must be re-evaluated to see
            // if the output remains unchanged.
            capabilities.sort_by(|x,y| x.0.time().cmp(&y.0.time()));
            if let Some(position) = capabilities.iter().position(|c| !notificator.frontier(0).iter().any(|t| t.le(&c.0.time()))) {

                // 2.a: form frontier, advance traces.
                let mut frontier = Antichain::new();
                for entry in capabilities.iter() {
                    frontier.insert(entry.0.time());
                }
                for time in notificator.frontier(0).iter() {
                    frontier.insert(time.clone());
                }

                source_trace.advance_by(frontier.elements());
                output_trace.advance_by(frontier.elements());

                // *now* safe to pop capability out (not before 2.a).
                let (capability, wrapped) = capabilities.swap_remove(position);
                let time = capability.time();

                // 2.b: order inputs; enqueue work (keys); update source trace.
                if let Some(wrapped) = wrapped {
                    let batch = wrapped.item;
                    let mut todo = to_do.entry(time.clone()).or_insert(BatchDistinct::new());
                    let mut batch_cursor = batch.cursor();
                    while batch_cursor.key_valid() {
                        todo.push((*batch_cursor.key()).clone());
                        batch_cursor.step_key();
                    }
                }

                // 2.c: Process interesting keys at this time.
                if let Some(keys) = to_do.remove(&time) {

                    // let source_borrow = source_trace.wrapper.borrow();
                    let mut source_cursor: T1::Cursor = source_trace.cursor();
                    let mut output_cursor: T2::Cursor = output_trace.cursor();

                    // changes to output_trace we build up (and eventually send).
                    // let mut output_builder = <T2::Batch as Batch<K,V2,G::Timestamp>>::Builder::new();
                    let mut output_builder = <T2::Batch as Batch<K,V2,G::Timestamp>>::OrderedBuilder::new();

                    // sort, dedup keys and iterate.                    
                    for key in keys.done() {

                        input_stage.clear();
                        output_stage.clear();

                        // 1. build up input collection; capture unused times as interesting.
                        // NOTE: for this to be correct with an iterator approach (that may 
                        // not look at all input updates) we need to look at the times in 
                        // the output trace as well. Even this may not be right (needs math).

                        source_cursor.seek_key(&key);
                        if source_cursor.key_valid() && source_cursor.key().borrow() == &key {
                            while source_cursor.val_valid() {
                                let mut sum = 0;
                                source_cursor.map_times(|t,d| {
                                    if t.le(&time) { sum += d; }
                                    else {
                                        // capture time for future re-evaluation
                                        let join = t.join(&time);
                                        if !time_stage.contains(&join) {
                                            time_stage.push(join);
                                        }
                                    }
                                });
                                if sum > 0 {
                                    // TODO : currently cloning values; references would be better.
                                    input_stage.push((source_cursor.val().borrow().clone(), sum));
                                }
                                source_cursor.step_val();
                            }
                        }

                        // 2. apply user logic (only if non-empty).
                        if input_stage.len() > 0 {
                            logic(&key, &input_stage[..], &mut output_stage);
                        }

                        // 3. subtract existing output differences.
                        output_cursor.seek_key(&key);
                        if output_cursor.key_valid() && output_cursor.key().borrow() == &key {
                            while output_cursor.val_valid() {
                                let mut sum = 0;
                                output_cursor.map_times(|t,d| { 
                                    if t.le(&time) { sum += d; }
                                    // // NOTE : this is important for interesting times opt; don't forget
                                    // else {
                                    //     let join = t.join(&time);
                                    //     if !time_stage.contains(&join) {
                                    //         time_stage.push(join);
                                    //     }
                                    // }
                                });
                                if sum != 0 {
                                    let val_ref: &V2 = output_cursor.val().borrow();
                                    let val: V2 = val_ref.clone();
                                    output_stage.push((val, -sum));
                                }

                                output_cursor.step_val();
                            }
                        }
                        consolidate(&mut output_stage);

                        // 5. register interesting (key, time) pairs.
                        // We do this here to avoid cloning the key if we don't need to.
                        for new_time in &time_stage {
                            to_do.entry(new_time.clone())
                                 .or_insert_with(|| {
                                    let delayed = capability.delayed(new_time);
                                    if !capabilities.iter().any(|c| c.0.time() == delayed.time()) {
                                        capabilities.push((delayed, None));
                                    }
                                    BatchDistinct::new()
                                 })
                                .push(key.clone());
                        }
                        time_stage.clear(); 

                        // 4. send output differences, assemble output layer
                        // TODO : introduce "ordered builder" trait, impls.
                        if output_stage.len() > 0 {
                            for (val, diff) in output_stage.drain(..) {
                                output_builder.push((key.clone(), val.clone(), time.clone(), diff));
                            }
                        }

                    }

                    // send and commit output updates
                    let output_batch = output_builder.done(&[], &[]);
                    output.session(&capability).give(BatchWrapper { item: output_batch.clone() });
                    let output_borrow: &mut T2 = &mut output_trace.wrapper.borrow_mut().trace;
                    output_borrow.insert(output_batch);
                }
            }
        });

        Arranged { stream: stream, trace: result_trace }
    }
}

// /// Extension trait for the `group` differential dataflow method
// pub trait GroupMulti<G: Scope, K: Data, V: Data> where G::Timestamp: Lattice {

//     /// Groups records by their first field, and applies reduction logic to the associated values.
//     fn group_multi<L, V2: Data>(&self, logic: L) -> Collection<G, (K,V2)>
//         where L: Fn(&K, &[(V, Delta)], &mut Vec<(V2, Delta)>)+'static;
// }

// impl<G: Scope, K: Data+Default, V: Data+Default> GroupMulti<G, K, V> for Collection<G, (K,V)>
// where G::Timestamp: Lattice
// {
//     fn group_multi<L, V2: Data>(&self, logic: L) -> Collection<G, (K,V2)>
//         where L: Fn(&K, &[(V, Delta)], &mut Vec<(V2, Delta)>)+'static {

//             let mut updates = Vec::new();       // stash for input differences.
//             let mut interest = Vec::new();      // list of (key, time) pairs to re-process.
//             let mut capabilities = Vec::new():  // lower envelope of times we should process.
//             let mut state = HashMap::new();     // state by key, then by val, then list of times.

//             let mut input_stage = Vec::new();   // staging input collection
//             let mut output_stage = Vec::new();  // staging output collection

//             let mut to_send = Vec::new();       // output diffs here, before shipping them.

//             let exchange = Exchange::new(|&((ref key, _),_)| key.hashed());
//             self.unary_notify(exchange, "GroupAlt", Vec::new(), |input, output, notificator| {

//                 // stash each update with its time
//                 input.for_each(|time, data| {
//                     for ((key, val), diff) in data {
//                         updates.push((key, val, time.time(), diff));
//                     }
//                 });

//                 // establish lower and upper frontiers.
//                 // we can optimize the frontiers by discarding common elements.
//                 let mut lower = Vec::new();
//                 for cap in capabilities.iter() {
//                     if !notificator.frontier(0).contains(cap.time()) {
//                         lower.push(cap.time());
//                     }
//                 }
//                 let mut upper = Vec::new();
//                 for time in notificator.frontier(0).iter() {
//                     if !capabilities.any(|cap| cap.time() == time) {
//                         upper.push(time.clone());
//                     }
//                 }

//                 let mut now_updates = Vec::new();
//                 extract(&mut updates, &mut now_updates, |&(_,_,ref time,_)| lower.any(|t| t.le(time)) && !upper.any(|t| t.le(time)));
//                 now_updates.sort_by(|x,y| x.0.cmp(&y.0));

//                 let mut now_interest = Vec::new();
//                 extract(&mut interest, &mut now_interest, |&(_,ref time)| lower.any(|t| t.le(time)) && !upper.any(|t| t.le(time)));
//                 now_interest.sort_by(|x,y| x.0.cmp(&y.0));

//                 let mut updates_index = 0;
//                 let mut interest_index = 0;

//                 // walk through the updates and interesting times, key by key.
//                 while updates_index < now_updates.len() || interest_index < now_interest.len() {

//                     // 1. pick out the next key
//                     let key = match (updates_index < now_updates.len(), interest_index < now_interest.len()) {
//                         (true, true)    => ::std::cmp::min(now_updates[updates_index].0, now_interest[interest_index].0),
//                         (true, false)   => now_updates[updates_index].0,
//                         (false, true)   => now_interest[interest_index].0,
//                         (false, false)  => unreachable!(),
//                     };

//                     let state = state.entry(key.clone()).or_insert((HashMap::new(), HashMap::new()));

//                     // 2. determine ranges of updates and interesting times.
//                     let mut updates_upper = updates_index;
//                     while updates_upper < now_updates.len() && now_updates[updates_upper].0 == key {
//                         updates_upper += 1;
//                     }

//                     let mut interest_upper = interest_index;
//                     while interest_upper < now_interest.len() && now_interest[interest_upper].0 == key {
//                         interest_upper += 1;
//                     }

//                     let updates = &mut updates[updates_index .. updates_upper];
//                     let interest = &interest[interest_index .. interest_upper];
//                     updates_index = updates_upper;
//                     interest_index = interest_upper;

//                     // 3. determine new interesting times, incorporate updates
//                     let mut times = BatchDistinct::new();
//                     updates.sort_by(|x,y| x.2.cmp(&y.2));
//                     for index in 0 .. updates.len() {
//                         // consider new interesting times if time changes.
//                         if index == 0 || updates[index].2 != updates[index-1].2 {
//                             let time = updates[index].2;
//                             for old_time in state.0.iter().flat_map(|(_,times)| times.iter().map(|&(ref time,_)| time)) {
//                                 let new_time = time.join(old_time);
//                                 if new_time != old_time {
//                                     times.push(new_time);
//                                 }
//                             }
//                         }
//                     }

//                     let mut times = times.done();
//                     close_under_join(&mut times);
//                     for time in interest.iter().map(|kt| kt.1) {
//                         times.push(time);
//                     }

//                     times.sort();
//                     times.dedup();

//                     // 4. merge in updates.
//                     for &(_, val, time, diff) in updates {
//                         state.0.entry(val).or_insert(Vec::new()).push((time, diff));
//                     }

//                     // 5. compact representation (optional?).
//                     for (_, times) in state.0.iter_mut() {
//                         for time_diff in times.iter_mut() {
//                             time_diff.0 = advance(&time_diff.0, lower);
//                         }
//                         times.sort_by(|x,y| x.0.cmp(&y.0));
//                         for index in 1 .. times.len() {
//                             if times[index].0 == times[index-1].0 {
//                                 times[index].1 += times[index-1].1
//                                 times[index-1].0 = 0;
//                             }
//                         }
//                         times.retain(|x| x.1 != 0);
//                         // TODO : if the list has length zero, delete I guess?
//                     }

//                     // 6. swing through times, acting on those between lower and upper.
//                     let mut left_over = Vec::new();
//                     for time in times.drain(..) {
//                         if lower.any(|t| t.le(time)) && !upper.any(|t| t.le(time)) {

//                             input_stage.clear();
//                             output_stage.clear();

//                             // assemble collection 
//                             for (val, times) in state.0.iter() {
//                                 let sum = times.filter(|(t,_)| t.le(time))
//                                                .map(|(t,d)| d)
//                                                .sum();
//                                 if sum > 0 {
//                                     input_stage.push((val, sum));
//                                 }
//                             }
//                             input_stage.sort();

//                             // run the user logic (if input data exist)
//                             if input_stage.len() > 0 {
//                                 logic(&key, &mut input_stage, &mut output_stage);
//                             }

//                             // subtract output diffs
//                             for (val, times) in state.1.iter() {
//                                 let sum = times.filter(|(t,_)| t.le(time))
//                                                .map(|(t,d)| d)
//                                                .sum();

//                                 output_stage.push((val, -sum));
//                             }
//                             output_stage.sort();
//                             for index in 1 .. output_stage.len() {
//                                 if output_stage[index].0 == output_stage[index-1].0 {
//                                     output_stage[index].1 += output_stage[index-1].1;
//                                     output_stage[index-1].1 = 0;
//                                 }
//                             }
//                             output_stage.retain(|x| x.1 != 0);

//                             // commit output corrections
//                             for (val, diff) in output_stage.drain(..) {
//                                 state.1.entry(val)
//                                        .or_insert(Vec::new())
//                                        .push((time, diff));

//                                 // need to send, but must find capability first!
//                                 to_send.push((key, val, time, wgt));
//                             }
//                         }
//                         else {
//                             left_over.push(time);
//                         }
//                     }
//                 }

//                 // sort output to co-locate like timestamps.
//                 to_send.sort_by(|x,y| x.2.cmp(&y.2));

//             }).as_collection()


//     }
// }

// fn extract<T, L: Fn(&T)->bool>(source: &mut Vec<T>, target: &mut Vec<T>, logic: L) {
//     let mut index = 0;
//     while index < source.len() {
//         if logic(&source[index]) {
//             target.push(source.swap_remove(index));
//         }
//         else {
//             index += 1;
//         }
//     }
// }

// /// Compacts `(T, isize)` pairs lazily.
// pub struct BatchCompact<T: Ord> {
//     sorted: usize,
//     buffer: Vec<(T, isize)>,
// }

// impl<T: Ord> BatchCompact<T> {
//     /// Allocates a new batch compacter.
//     pub fn new() -> BatchCompact<T> {
//         BatchCompact {
//             sorted: 0,
//             buffer: Vec::new(),
//         }
//     }

//     /// Adds an element to the batch compacter.
//     pub fn push(&mut self, element: (T, isize)) {
//         self.buffer.push(element);
//         if self.buffer.len() > ::std::cmp::max(self.sorted * 2, 1 << 20) {
//             self.buffer.sort();
//             for index in 1 .. self.buffer.len() {
//                 if self.buffer[index].0 == self.buffer[index-1].0 {
//                     self.buffer[index].1 += self.buffer[index-1].1;
//                     self.buffer[index-1].1 = 0;
//                 }
//             }
//             self.buffer.retain(|x| x.1 != 0);
//             self.sorted = self.buffer.len();
//         }
//     }
//     /// Adds several elements to the batch compacted.
//     pub fn extend<I: Iterator<Item=(T, isize)>>(&mut self, iter: I) {
//         for item in iter {
//             self.push(item);
//         }
//     }
//     /// Finishes compaction, returns results.
//     pub fn done(mut self) -> Vec<(T, isize)> {
//         if self.buffer.len() > self.sorted {
//             self.buffer.sort();
//             for index in 1 .. self.buffer.len() {
//                 if self.buffer[index].0 == self.buffer[index-1].0 {
//                     self.buffer[index].1 += self.buffer[index-1].1;
//                     self.buffer[index-1].1 = 0;
//                 }
//             }
//             self.buffer.retain(|x| x.1 != 0);
//             self.sorted = self.buffer.len();
//         }
//         self.buffer
//     }
// }

struct BatchDistinct<T: Ord> {
    sorted: usize,
    buffer: Vec<T>,
}

impl<T: Ord> BatchDistinct<T> {
    fn new() -> BatchDistinct<T> {
        BatchDistinct {
            sorted: 0,
            buffer: Vec::new(),
        }
    }
    fn push(&mut self, element: T) {
        self.buffer.push(element);
        // if self.buffer.len() > self.sorted * 2 {
        //     self.buffer.sort();
        //     self.buffer.dedup();
        //     self.sorted = self.buffer.len();
        // }
    }

    fn done(mut self) -> Vec<T> {
        if self.buffer.len() > self.sorted {
            self.buffer.sort();
            self.buffer.dedup();
            self.sorted = self.buffer.len();
        }
        self.buffer
    }
}

fn consolidate<T: Ord>(list: &mut Vec<(T, isize)>) {
    list.sort_by(|x,y| x.0.cmp(&y.0));
    for index in 1 .. list.len() {
        if list[index].0 == list[index-1].0 {
            list[index].1 += list[index-1].1;
            list[index-1].1 = 0;
        }
    }
    list.retain(|x| x.1 != 0);
}