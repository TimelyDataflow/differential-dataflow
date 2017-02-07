//! Group records by a key, and apply a reduction function.

use std::collections::HashMap;

use std::hash::Hash;

use linear_map::LinearMap;

use ::{Data, Collection, Delta};
use ::lattice::close_under_join;
use stream::AsCollection;

use timely::progress::Antichain;
use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Capability;

use lattice::Lattice;
use collection::Lookup;

/// Extension trait for the `group` differential dataflow method
pub trait GroupAlt<G: Scope, K: Data+Hash, V: Data+Hash> where G::Timestamp: Lattice+Ord {

    /// Groups records by their first field, and applies reduction logic to the associated values.
    fn group_alt<L, V2: Data+Hash>(&self, logic: L) -> Collection<G, (K,V2)>
        where L: Fn(&K, &[(V, Delta)], &mut Vec<(V2, Delta)>)+'static;
}

impl<G: Scope, K: Data+Hash, V: Data+Hash> GroupAlt<G, K, V> for Collection<G, (K,V)>
where G::Timestamp: Lattice+Ord
{
    fn group_alt<L, V2: Data+Hash>(&self, logic: L) -> Collection<G, (K,V2)>
        where L: Fn(&K, &[(V, Delta)], &mut Vec<(V2, Delta)>)+'static {

        let mut state = HashMap::new();     // key -> (input_trace, output_trace)
        let mut inputs = LinearMap::new();  // A map from times to received (key, val, wgt) triples.
        let mut to_do = LinearMap::new();   // A map from times to a list of keys that need processing at that time.
        let mut capabilities = Vec::new();  // notificator replacement (for capabilities)

        let exchange = Exchange::new(|x: &((K,V),i32)| (x.0).0.hashed());

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        self.inner.unary_notify(exchange, "GroupAlt", vec![], move |input, output, notificator| {

            // 1. read each input, and stash it in our staging area.
            input.for_each(|time, data| {
                let updates = inputs.entry_or_insert(time.time(), || {
                    if !capabilities.iter().any(|c: &Capability<G::Timestamp>| c.time() == time.time()) {
                        capabilities.push(time);
                    }
                    HashMap::new()
                });

                for ((key, val), diff) in data.drain(..) {
                    updates.entry(key)
                           .or_insert(Vec::new())
                           .push((val, diff));
                }
            });

            // 2. go through each time of interest that has reached completion
            // times are interesting either because we received data, or because we conclude
            // in the processing of a time that a future time will be interesting.
            capabilities.sort_by(|x,y| x.time().cmp(&y.time()));
            if let Some(position) = capabilities.iter().position(|c| !notificator.frontier(0).iter().any(|t| t.le(&c.time()))) {

                // form frontier for compaction purposes.
                let mut frontier = Antichain::new();
                for capability in capabilities.iter() {
                    frontier.insert(capability.time());
                }
                for time in notificator.frontier(0).iter() {
                    frontier.insert(time.clone());
                }

                let capability = capabilities.swap_remove(position);
                let time = capability.time();

                // 2a. If we received any keys, determine the interesting times for each.
                //     We then enqueue the keys at the corresponding time, for use later.
                if let Some(mut queue) = inputs.remove_key(&time) {

                    let mut stash = Vec::new();
                    for (key, mut updates) in queue.drain() {

                        // consolidate updates
                        consolidate(&mut updates);

                        // only continue if updates exist
                        if updates.len() > 0 {

                            let state = state.entry(key.clone())
                                             .or_insert((HashMap::new(), HashMap::new()));

                            // determine new interesting times
                            stash.push(capability.time());
                            for (_, times) in state.0.iter() {
                                for &(ref time2,_) in times {
                                    let joined = time.join(time2);
                                    if joined != time && !stash.contains(&joined) {
                                        stash.push(joined);
                                    }
                                }
                            }
                            close_under_join(&mut stash);

                            // notificate for times, add key to todo list.
                            for new_time in &stash {
                                to_do.entry_or_insert(new_time.clone(), || {
                                    let delayed = capability.delayed(new_time);
                                    if !capabilities.iter().any(|c| c.time() == delayed.time()) {
                                        capabilities.push(delayed);
                                    }
                                    Vec::new()
                                })
                                .push(key.clone());
                            }
                            stash.clear();

                            // commit updates
                            for (val, diff) in updates.drain(..) {
                                state.0.entry(val).or_insert(Vec::new()).push((time.clone(), diff));
                            }

                            // advance times, consolidate inputs
                            for (_, times) in state.0.iter_mut() {
                                for time_diff in times.iter_mut() {
                                    time_diff.0 = advance(&time_diff.0, frontier.elements());
                                }
                                consolidate(times);
                            }
                        }
                    }
                }

                // 2b. Process any interesting keys at this time.
                if let Some(mut keys) = to_do.remove_key(&time) {

                    // We would like these keys in a particular order.
                    // We also want to de-duplicate them, in case there are dupes.
                    keys.sort();
                    keys.dedup();

                    // staging areas for input and output of user code.
                    let mut input_stage = Vec::new();
                    let mut output_stage = Vec::new();

                    // session for sending produced output.
                    let mut session = output.session(&capability);

                    for key in keys {

                        if let Some(mut entry) = state.get_mut(&key) {

                            input_stage.clear();
                            output_stage.clear();

                            // 1. form the input collection
                            for (val, times) in entry.0.iter() {
                                let sum = accumulate(&times[..], &time);
                                if sum > 0 { input_stage.push((val.clone(), sum)); }
                            }
                            input_stage.sort();

                            // 2. apply user logic
                            if input_stage.len() > 0 {
                                logic(&key, &input_stage[..], &mut output_stage);
                            }

                            // 3. subtract existing output differences
                            for (val, times) in entry.1.iter() {
                                let val: &V2 = val;
                                let times: &Vec<(G::Timestamp, i32)> = times;
                                let sum = accumulate(&times[..], &time);
                                if sum != 0 { output_stage.push((val.clone(), -sum)); }
                            }
                            consolidate(&mut output_stage);

                            // 4. commit and send output differences
                            for (val, diff) in output_stage.drain(..) {
                                entry.1.entry(val.clone()).or_insert(Vec::new()).push((time.clone(), diff));
                                session.give(((key.clone(), val), diff));
                            }

                            // consolidate output (it might need it too!)
                            for (_, times) in entry.1.iter_mut() {
                                for time_diff in times.iter_mut() {
                                    time_diff.0 = advance(&time_diff.0, frontier.elements());
                                }
                                consolidate(times);
                            }
                        }
                    }
                }
            }
        })
        .as_collection()
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

// struct BatchDistinct<T: Ord> {
//     sorted: usize,
//     buffer: Vec<T>,
// }

// impl<T: Ord> BatchDistinct<T> {
//     fn new() -> BatchDistinct<T> {
//         BatchDistinct {
//             sorted: 0,
//             buffer: Vec::new(),
//         }
//     }
//     fn push(&mut self, element: T) {
//         self.buffer.push(element);
//         if self.buffer.len() > self.sorted * 2 {
//             self.buffer.sort();
//             self.buffer.dedup();
//             self.sorted = self.buffer.len();
//         }
//     }

//     fn done(self) -> Vec<T> {
//         if self.buffer.len() > self.sorted {
//             self.buffer.sort();
//             self.buffer.dedup();
//             self.sorted = self.buffer.len();
//         }
//         self.buffer
//     }
// }

/// Advances a time to the largest time equivalent under all comparisons to other times in the future of frontier.
fn advance<T: Lattice>(time: &T, frontier: &[T]) -> T {
    let mut result = time.join(&frontier[0]);
    for index in 1 .. frontier.len() {
        result = result.meet(&time.join(&frontier[index]));
    }
    result
}

fn consolidate<T: Ord>(list: &mut Vec<(T, i32)>) {
    list.sort_by(|x,y| x.0.cmp(&y.0));
    for index in 1 .. list.len() {
        if list[index].0 == list[index-1].0 {
            list[index].1 += list[index-1].1;
            list[index-1].1 = 0;
        }
    }
    list.retain(|x| x.1 != 0);
}

fn accumulate<T: Lattice>(list: &[(T, i32)], time: &T) -> i32 {
    let mut sum = 0;
    for pair in list.iter() {
        if pair.0.le(time) {
            sum += pair.1
        }
    }
    sum
}
