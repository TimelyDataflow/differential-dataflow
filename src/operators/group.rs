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

use hashable::{Hashable, UnsignedWrapper};
use ::{Data, Collection, Delta};

use timely::progress::Antichain;
use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely_sort::Unsigned;

use operators::arrange::{Arrange, Arranged, ArrangeByKey, ArrangeBySelf, BatchWrapper, TraceHandle};
use lattice::Lattice;
use trace::{Batch, Cursor, Trace, Builder};
// use trace::implementations::trie::Spine as OrdSpine;
// use trace::implementations::keys::Spine as KeysSpine;
use trace::implementations::rhh::Spine as HashSpine;
use trace::implementations::rhh_k::Spine as KeyHashSpine;

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

impl<G: Scope, K: Data, V: Data, T1> GroupArranged<G, K, V> for Arranged<G, K, V, T1>
where 
    G::Timestamp: Lattice+Ord,
    T1: Trace<K, V, G::Timestamp>+'static {
        
    fn group_arranged<L, V2, T2>(&self, logic: L, empty: T2) -> Arranged<G, K, V2, T2>
        where 
            V2: Data,
            T2: Trace<K, V2, G::Timestamp>+'static,
            L: Fn(&K, &[(V, Delta)], &mut Vec<(V2, Delta)>)+'static {

        let mut source_trace = self.new_handle();
        let mut output_trace = TraceHandle::new(empty, &[Default::default()]);
        let result_trace = output_trace.clone();

        // Our implementation maintains a list `interesting` of pairs `(key, time)` which must be reconsidered.
        // We must also maintain capabilities tracking the lower bound of interesting times in this pile. 
        // Each invocation, we want to extract all newly available work items (not greater or equal to an element
        // of the input frontier) and process each of them. So doing may produce output, and may result in newly 
        // interesting `(key, time)` pairs. Once done, we update our capabilities to again track the lower frontier
        // of interesting times.

        // TODO: Perhaps this should be more like `Batcher`: something radix-sort friendly, as we are going to do
        // the same sort of thing often. We dedup the results instead of of cancelation, but otherwise similar.
        let mut interesting = Vec::<(K, G::Timestamp)>::new();
        let mut capabilities = Vec::<Capability<G::Timestamp>>::new();

        // In the course of processing interesting pairs, we will need space to stage inputs, outputs, and future
        // times that may be interesting for the processed key.

        let mut time_stage = Vec::new();    // staging for building up interesting times.
        let mut input_stage = Vec::new();   // staging for per-key input (pre-iterator).
        let mut output_stage = Vec::<(V2, isize)>::new();  // staging for per-key output.

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        let stream = self.stream.unary_notify(Pipeline, "GroupArrange", vec![], move |input, output, notificator| {

            // println!("group: start");

            // TODO: I think we could keep most of `interesting` in the received layer.
            //       We do need to break out "derived" interesting times, and stash them,
            //       but that is different from putting an entry for every tuple in the 
            //       input batch.

            // 1. Read input batches, stash capabilities, populate `interesting`.
            input.for_each(|cap, data| {
                // add the capability to `capabilities`.
                capabilities.retain(|c| !c.time().gt(&cap.time()));
                if !capabilities.iter().any(|c| c.time().le(&cap.time())) {
                    capabilities.push(cap);
                }

                // Push each (key, time) pair into `interesting`. 
                // TODO: This is pretty inefficient if many values and just one time. Optimize that case?
                for batch in data.drain(..) {
                    let mut cursor = batch.item.cursor();
                    while cursor.key_valid() {
                        let key = cursor.key().clone();
                        while cursor.val_valid() {
                            cursor.map_times(|time, _| interesting.push((key.clone(), time.clone())));
                            cursor.step_val();
                        }
                        cursor.step_key();
                    }
                }
            });

            // 2. Consider each capability, and whether downgrading it to our input frontier would 
            //    expose elements of `interesting`. A pair `(key, time)` is exposed if the time is
            //    greater or equal to that of the capability, and not greater or equal to the time
            //    of a later held capability, or a time in the input frontier.

            // The following pattern looks a lot like what is done in `arrange`. In fact, we've 
            // stolen some code from there, so if either looks wrong, make sure to check the other
            // as well.

            let mut counts = Vec::new();

            for index in 0 .. capabilities.len() {

                if !notificator.frontier(0).iter().any(|t| t == &capabilities[index].time()) {

                    // Assemble an upper bound on exposed times.
                    let mut upper = Vec::new();
                    for after in (index + 1) .. capabilities.len() {
                        upper.push(capabilities[after].time());
                    }
                    for time in notificator.frontier(0) {
                        if !upper.iter().any(|t| t.le(time)) {
                            upper.push(time.clone());
                        }
                    }

                    // deduplicate, order by key.
                    // TODO: this could be much more efficiently done; e.g. radix sorting.
                    // TODO: perhaps think out whether we could avoid re-sorting sorted elements.
                    // interesting.dedup();
                    // interesting.sort();
                    // interesting.dedup();
                    sort_dedup(&mut interesting);

                    // extract exposed times
                    let mut new_interesting = Vec::new();
                    let mut exposed = Vec::new();

                    segment(&mut interesting, &mut exposed, &mut new_interesting, |&(_, ref time)| {
                        capabilities[index].time().le(&time) && !upper.iter().any(|t| t.le(&time))
                    });

                    interesting = new_interesting;


                    // cursors for navigating input and output traces.
                    let mut source_cursor: T1::Cursor = source_trace.cursor();
                    let mut output_cursor: T2::Cursor = output_trace.cursor();

                    // changes to output_trace we build up (and eventually commit and send).
                    let mut output_builder = <T2::Batch as Batch<K,V2,G::Timestamp>>::Builder::new();

                    // Time to process all of these interesting pairs!
                    // We want to process in batches defined by keys, and within each back in some order on
                    // times that is compatible with the partial order (before we process a time, we want 
                    // all strictly less times processed for the key). As we process a key we may discover 
                    // new interesting times for the key! When this happens, we either (i) want to introduce
                    // the time (at a compatible moment) if it is not greater than `upper`, or (ii) add the
                    // pair to `interesting` for future processing.

                    // priority queue for interesting times.
                    // unfortunately, BinaryHeap is a max heap. T.T
                    let mut times = Vec::new();

                    let mut position = 0;
                    while position < exposed.len() {

                        let key = exposed[position].0.clone();
                        source_cursor.seek_key(&key);

                        // add the first time
                        times.push(exposed[position].1);
                        position += 1;

                        // add further times for the same key.
                        while position < exposed.len() && exposed[position].0 == key {
                            times.push(exposed[position].1);
                            position += 1;
                        }

                        let mut output_accum = Vec::<(V2, G::Timestamp, isize)>::new();

                        // for each interesting time (where the set may grow as we run) ..
                        times.sort_by(|x,y| y.cmp(&x));
                        times.dedup();

                        // At this point, all queries about the input (and output) will be for times
                        // in the future of elements of `times`. We could determine the meet and join
                        // of these times, accumulating those updates less than the meet and capturing
                        // those updates less than the join (but not less than the meet). This could 
                        // simplify our computation when we have several times.

                        let mut dirty = false;

                        let mut counter = 0;

                        while let Some(time) = times.pop() {

                            counter += 1;

                            if dirty {
                                source_cursor.rewind_vals();
                                output_cursor.rewind_vals();
                            }

                            dirty = true;

                            // This is the body of the `group` logic. It is here that we process
                            // an interesting pair `(key, time)`, by assembling input, applying 
                            // user logic, differencing from the output, and updating interesting
                            // times for the key.

                            input_stage.clear();
                            output_stage.clear();

                            // 1. build up input collection; capture unused times as interesting.
                            // NOTE: for this to be correct with an iterator approach (that may 
                            // not look at all input updates) we need to look at the times in 
                            // the output trace as well. Even this may not be right (needs math).

                            if source_cursor.key_valid() && source_cursor.key() == &key {
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
                                        input_stage.push((source_cursor.val().clone(), sum));
                                    }
                                    source_cursor.step_val();
                                }
                            }

                            // 2. apply user logic (only if non-empty).
                            if input_stage.len() > 0 {
                                logic(&key, &input_stage[..], &mut output_stage);
                            }

                            // println!("key: {:?}, input: {:?}, ouput: {:?} @ {:?}", key, input_stage, output_stage, time);

                            // 3. subtract existing output differences.
                            output_cursor.seek_key(&key);
                            if output_cursor.key_valid() && output_cursor.key() == &key {
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
                                        output_stage.push((output_cursor.val().clone(), -sum));
                                    }
                                    output_cursor.step_val();
                                }
                            }
                            // incorporate uncommitted output updates.
                            for &(ref val, ref t, diff) in &output_accum {
                                if t.le(&time) {
                                    output_stage.push((val.clone(), -diff));
                                }
                            }
                            consolidate(&mut output_stage);

                            // 5. register interesting (key, time) pairs.
                            // We do this here to avoid cloning the key if we don't need to.
                            time_stage.sort();
                            time_stage.dedup();
                            for new_time in time_stage.drain(..) {
                                if !upper.iter().any(|t| t.le(&new_time)) {
                                    assert!(new_time != time);
                                    times.push(new_time);
                                }
                                else {
                                    interesting.push((key.clone(), new_time));
                                }
                            }

                            // 4. send output differences, assemble output layer
                            // TODO : introduce "ordered builder" trait, impls.
                            if output_stage.len() > 0 {
                                for (val, diff) in output_stage.drain(..) {
                                    output_accum.push((val, time.clone(), diff));
                                }
                            }

                            // because we have a crap heap.
                            times.sort_by(|x,y| y.cmp(&x));
                            times.dedup();
                        }

                        while counts.len() <= counter {
                            counts.push(0);
                        }
                        counts[counter] += 1;

                        for (val, time, diff) in output_accum.drain(..) {
                            output_builder.push((key.clone(), val, time, diff));
                        }
                    }

                    // having processed all exposed keys and times, we should commit and send the batch.
                    let output_batch = output_builder.done(&[], &[]);     // TODO: fix this nonsense.
                    output.session(&capabilities[index]).give(BatchWrapper { item: output_batch.clone() });
                    let output_borrow: &mut T2 = &mut output_trace.wrapper.borrow_mut().trace;
                    output_borrow.insert(output_batch);
                }
            }

            for (index, &count) in counts.iter().enumerate() {
                if count > 0 {
                    println!("counts[{}]:\t{}", index, count);
                }
            }
            if counts.len() > 0 {
                println!("");
            }

            // 3. Having now processed all capabilities, we must advance them to track the frontier
            //    of times in `interesting`. 
            let mut new_frontier = Antichain::new();
            for &(_, ref time) in &interesting {
                new_frontier.insert(time.clone());
            }

            // TODO: Perhaps this could be optimized when not much changes?
            let mut new_capabilities = Vec::new();
            for time in new_frontier.elements() {
                if let Some(capability) = capabilities.iter().find(|c| c.time().le(time)) {
                    new_capabilities.push(capability.delayed(time));
                }
            }
            capabilities = new_capabilities;

            for time in notificator.frontier(0) {
                new_frontier.insert(time.clone());
            }

            // assert!(new_frontier.elements().len() > 0);
            source_trace.advance_by(new_frontier.elements());
            output_trace.advance_by(new_frontier.elements());


            // println!("group: done");

        });

        Arranged { stream: stream, trace: result_trace }
    }
}

#[inline(never)]
fn sort_dedup<T: Ord>(list: &mut Vec<T>) {
    list.dedup();
    list.sort();
    list.dedup();
}

#[inline(never)]
fn segment<T, F: Fn(&T)->bool>(source: &mut Vec<T>, dest1: &mut Vec<T>, dest2: &mut Vec<T>, pred: F) {
    for element in source.drain(..) {
        if pred(&element) {
            dest1.push(element);
        }
        else {
            dest2.push(element);
        }
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