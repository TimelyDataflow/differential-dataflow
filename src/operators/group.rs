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
use ::{Data, Collection, Ring};

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
pub trait Group<G: Scope, K: Data, V: Data, R: Ring> where G::Timestamp: Lattice+Ord {
    /// Groups records by their first field, and applies reduction logic to the associated values.
    fn group<L, V2: Data, R2: Ring>(&self, logic: L) -> Collection<G, (K, V2), R2>
        where L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static;
    /// Groups records by their first field, and applies reduction logic to the associated values.
    fn group_u<L, V2: Data, R2: Ring>(&self, logic: L) -> Collection<G, (K, V2), R2>
        where L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static, K: Unsigned+Copy;
}

impl<G: Scope, K: Data+Default+Hashable, V: Data, R: Ring> Group<G, K, V, R> for Collection<G, (K, V), R> 
    where G::Timestamp: Lattice+Ord+::std::fmt::Debug {
    fn group<L, V2: Data, R2: Ring>(&self, logic: L) -> Collection<G, (K, V2), R2>
        where L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static {
        self.arrange_by_key()
            .group_arranged(move |k,s,t| logic(&k.item,s,t), HashSpine::new(Default::default()))
            .as_collection(|k,v| (k.item.clone(), v.clone()))
    }
    fn group_u<L, V2: Data, R2: Ring>(&self, logic: L) -> Collection<G, (K, V2), R2>
        where L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static, K: Unsigned+Copy {
        self.arrange(|k,v| (UnsignedWrapper::from(k), v), HashSpine::new(Default::default()))
            .group_arranged(move |k,s,t| logic(&k.item,s,t), HashSpine::new(Default::default()))
            .as_collection(|k,v| (k.item.clone(), v.clone()))
    }
}

/// Extension trait for the `distinct` differential dataflow method.
pub trait Distinct<G: Scope, K: Data> where G::Timestamp: Lattice+Ord {
    /// Reduces the collection to one occurrence of each distinct element.
    fn distinct(&self) -> Collection<G, K, isize>;
    /// Reduces the collection to one occurrence of each distinct element.
    fn distinct_u(&self) -> Collection<G, K, isize> where K: Unsigned+Copy;
}

impl<G: Scope, K: Data+Default+Hashable> Distinct<G, K> for Collection<G, K, isize> 
where G::Timestamp: Lattice+Ord+::std::fmt::Debug {
    fn distinct(&self) -> Collection<G, K, isize> {
        self.arrange_by_self()
            .group_arranged(|_k,_s,t| t.push(((), 1)), KeyHashSpine::new(Default::default()))
            .as_collection(|k,_| k.item.clone())
    }
    fn distinct_u(&self) -> Collection<G, K, isize> where K: Unsigned+Copy {
        self.map(|k| (k,()))
            .arrange(|k,v| (UnsignedWrapper::from(k), v), KeyHashSpine::new(Default::default()))
            .group_arranged(|_k,_s,t| t.push(((), 1)), KeyHashSpine::new(Default::default()))
            .as_collection(|k,_| k.item.clone())
    }
}


/// Extension trait for the `count` differential dataflow method.
pub trait Count<G: Scope, K: Data, R: Ring> where G::Timestamp: Lattice+Ord {
    /// Counts the number of occurrences of each element.
    fn count(&self) -> Collection<G, (K, R), isize>;
    /// Counts the number of occurrences of each element.
    fn count_u(&self) -> Collection<G, (K, R), isize> where K: Unsigned+Copy;
}

impl<G: Scope, K: Data+Default+Hashable, R: Ring> Count<G, K, R> for Collection<G, K, R>
 where G::Timestamp: Lattice+Ord+::std::fmt::Debug {
    fn count(&self) -> Collection<G, (K, R), isize> {
        self.arrange_by_self()
            .group_arranged(|_k,s,t| t.push((s[0].1, 1)), HashSpine::new(Default::default()))
            .as_collection(|k,&c| (k.item.clone(), c))
    }
    fn count_u(&self) -> Collection<G, (K, R), isize> where K: Unsigned+Copy {
        self.map(|k| (k,()))
            .arrange(|k,v| (UnsignedWrapper::from(k), v), KeyHashSpine::new(Default::default()))
            .group_arranged(|_k,s,t| t.push((s[0].1, 1)), HashSpine::new(Default::default()))
            .as_collection(|k,&c| (k.item.clone(), c))
    }
}


/// Extension trace for the group_arranged differential dataflow method.
pub trait GroupArranged<G: Scope, K: Data, V: Data, R: Ring> where G::Timestamp: Lattice+Ord {
    /// Applies `group` to arranged data, and returns an arrangement of output data.
    fn group_arranged<L, V2, T2, R2>(&self, logic: L, empty: T2) -> Arranged<G, K, V2, R2, T2>
        where
            V2: Data,
            R2: Ring,
            T2: Trace<K, V2, G::Timestamp, R2>+'static,
            L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static
            ; 
}

impl<G: Scope, K: Data, V: Data, T1, R: Ring> GroupArranged<G, K, V, R> for Arranged<G, K, V, R, T1>
where 
    G::Timestamp: Lattice+Ord,
    T1: Trace<K, V, G::Timestamp, R>+'static {
        
    fn group_arranged<L, V2, T2, R2>(&self, logic: L, empty: T2) -> Arranged<G, K, V2, R2, T2>
        where 
            V2: Data,
            R2: Ring,
            T2: Trace<K, V2, G::Timestamp, R2>+'static,
            L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static {

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

        // experimental accumulators
        // let mut input_accumulator = Accumulator::new();
        // let mut output_accumulator = Accumulator::new();
        let mut yielded_accumulator = Accumulator::<V2, G::Timestamp, R2>::new();

        // input accumulation and conditional edits.
        let mut input_stage = Vec::<(V, R)>::new();                     // accumulated input updates.
        let mut input_edits = Vec::<(V, G::Timestamp, R)>::new();       // conditional input updates.

        // output accumualtion and conditional edits.
        let mut output_logic = Vec::<(V2, R2)>::new();                  // output of user logic.
        let mut output_stage = Vec::<(V2, R2)>::new();                  // accumulated output updates.
        let mut output_edits = Vec::<(V2, G::Timestamp, R2)>::new();    // conditional output updates.
        let mut output_yield = Vec::<(V2, G::Timestamp, R2)>::new();    // newly produced output.

        // working space for per-key interesting times.
        let mut interesting_times = Vec::new();

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        let stream = self.stream.unary_notify(Pipeline, "GroupArrange", vec![], move |input, output, notificator| {

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
                // TODO: We should be able to keep times in the batch, deferring their enumeration until
                //       we process the corresponding key. Should minimize outstanding pairs that we may
                //       otherwise continually re-sort and scan and such.
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
            //
            //    We do one capability at a time because our output messages can only contain one 
            //    capability. If this restriction were removed, we could skip this loop and simply
            //    extract all updates at times between our capabilities and the input frontier.

            // The following pattern looks a lot like what is done in `arrange`. In fact, we have 
            // stolen some code from there, so if either looks wrong, make sure to check the other
            // as well.

            for index in 0 .. capabilities.len() {

                // Only do all of this if the capability is not present in the input frontier.
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
                    // TODO: This could be much more efficiently done; e.g. radix sorting.
                    // TODO: Perhaps think out whether we could avoid re-sorting sorted elements.
                    // TODO: This is where we might also involve the batches themselves, as all of
                    //       the keys are already sorted therein. This might limit us to one batch
                    //       at a time, without a horrible merge?
                    sort_dedup(&mut interesting);

                    // Segment `interesting` into `exposed` and the next value of interesting.
                    // This is broken out as a separate method mostly for performance profiling.
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
                    let mut output_builder = <T2::Batch as Batch<K,V2,G::Timestamp,R2>>::Builder::new();

                    // We now iterate through exposed keys, for each enumerating through interesting times.
                    // The set of interesting times is initially those in `exposed`, but the set may grow
                    // as computation proceeds, because of joins between new times and pre-existing times.

                    let mut position = 0;
                    while position < exposed.len() {

                        // The reason we are gathered here today.
                        let key = exposed[position].0.clone();

                        // Load `interesting_times` with those times we must reconsider.
                        interesting_times.clear();
                        while position < exposed.len() && exposed[position].0 == key {
                            interesting_times.push(exposed[position].1);
                            position += 1;
                        }

                        // Determine the `meet` of times, useful in restricting updates to capture.
                        let mut meet = interesting_times[0].clone(); 
                        for index in 1 .. interesting_times.len() {
                            meet = meet.meet(&interesting_times[index]);
                        }

                        // Sort and deduplicate interesting times. Helps with accum initialization.
                        interesting_times.sort();
                        interesting_times.dedup();

                        // Our accumulated output updates for the key should start empty.
                        output_yield.clear();

                        // Clear our stashes of input and output updates.
                        input_edits.clear();
                        output_edits.clear();

                        // Clear our staging ground for input and output collections.
                        input_stage.clear();
                        output_stage.clear();

                        yielded_accumulator.clear();

                        // Accumulate into `input_stage` and populate `input_edits`.
                        // TODO: The accumulation starts at `meet` and must be updated to the first 
                        //       element of times. We should probably just start there.
                        source_cursor.seek_key(&key);
                        if source_cursor.key_valid() && source_cursor.key() == &key {
                            while source_cursor.val_valid() {
                                let val: V = source_cursor.val().clone();
                                let mut sum = R::zero();
                                source_cursor.map_times(|t,d| {
                                    if t.le(&meet) { sum = sum + d; }
                                    if !t.le(&meet) {
                                        input_edits.push((val.clone(), t.join(&meet), d));
                                        // input_edits.push((val.clone(), t.clone(), d));
                                    }
                                });
                                if !sum.is_zero() {
                                    input_stage.push((val, sum));
                                }
                                source_cursor.step_val();
                            }
                        }
                        consolidate2(&mut input_edits);

                        // Accumulate into `output_stage` and populate `output_edits`. 
                        // NOTE: No accumulation currently done, as we put user output in `output_stage`.
                        //       Easy-ish to fix.
                        output_cursor.seek_key(&key);
                        if output_cursor.key_valid() && output_cursor.key() == &key {
                            while output_cursor.val_valid() {
                                let val: V2 = output_cursor.val().clone();
                                let mut sum = R2::zero();
                                output_cursor.map_times(|t,d| {
                                    if t.le(&meet) { sum = sum + d; }
                                    if !t.le(&meet) {
                                        output_edits.push((val.clone(), t.join(&meet), d));
                                        // output_edits.push((val.clone(), t.clone(), d));

                                    }
                                });
                                if !sum.is_zero() {
                                    output_stage.push((val, sum));
                                }
                                output_cursor.step_val();
                            }
                        }
                        consolidate2(&mut output_edits);

                        // Determine all interesting times: those that are the join of at least one time
                        // from `interesting_times` and any times from `input_edits`.
                        close_interesting_times(&input_edits[..], &mut interesting_times);

                        // used to help us diff out previous updates.
                        let mut prev_time = meet.clone();

                        // each interesting time must be considered!
                        for this_time in interesting_times.drain(..) {

                            // not all times are ready to be finalized. stash them with the key.
                            if upper.iter().any(|t| t.le(&this_time)) {
                                interesting.push((key.clone(), this_time));
                            }
                            else {

                                // 1. update `input_stage` and `output_stage` to `this_time`.
                                update(&input_edits[..], &mut input_stage, &prev_time, &this_time);
                                update(&output_edits[..], &mut output_stage, &prev_time, &this_time);

                                // 2. apply user logic (only if non-empty input).
                                output_logic.clear();
                                if input_stage.len() > 0 {
                                    logic(&key, &input_stage[..], &mut output_logic);
                                }

                                // println!("key: {:?}, input: {:?}, ouput: {:?} @ {:?}", 
                                //          key, input_stage, output_logic, this_time);

                                // 3. subtract existing output differences.
                                for &(ref val, diff) in &output_stage[..] {
                                    output_logic.push((val.clone(), -diff));
                                }
                                // incorporate uncommitted output updates.
                                yielded_accumulator.update_to(this_time.clone());
                                for &(ref val, diff) in &yielded_accumulator.accum {
                                    let val: &V2 = val;
                                    let val: V2 = val.clone();
                                    output_logic.push((val, -diff));
                                }
                                consolidate_b(&mut output_logic);

                                // 4. send output differences, assemble output layer
                                for (val, diff) in output_logic.drain(..) {
                                    yielded_accumulator.push_edit((val.clone(), this_time.clone(), diff));
                                }

                                // 5. get ready for next iteration!
                                prev_time = this_time;
                            }
                        }

                        // Sort yielded output updates and move into output builder.
                        yielded_accumulator.edits.sort();
                        for (val, time, diff) in yielded_accumulator.edits.drain(..) {
                            output_builder.push((key.clone(), val, time, diff));
                        }
                    }

                    // We have processed all exposed keys and times, and should commit and send the batch.
                    let output_batch = output_builder.done(&[], &[]);     // TODO: fix this nonsense.
                    output.session(&capabilities[index]).give(BatchWrapper { item: output_batch.clone() });
                    let output_borrow: &mut T2 = &mut output_trace.wrapper.borrow_mut().trace;
                    output_borrow.insert(output_batch);
                }
            }

            // 3. Having now processed all capabilities, we must advance them to track the frontier
            //    of times in `interesting`. 
            // 
            // TODO: It would be great if these were just the "newly interesting" times, rather than 
            //       those that live in batches, from which we can directly extract lower bounds.
            // TODO: We should only mint new capabilities for times not already in `capabilities`. 
            let mut new_frontier = Antichain::new();
            for &(_, ref time) in &interesting {
                new_frontier.insert(time.clone());
            }
            let mut new_capabilities = Vec::new();
            for time in new_frontier.elements() {
                if let Some(capability) = capabilities.iter().find(|c| c.time().le(time)) {
                    new_capabilities.push(capability.delayed(time));
                }
            }
            capabilities = new_capabilities;

            // We have processed all updates through the input frontier, and can advance traces.
            source_trace.advance_by(notificator.frontier(0));
            output_trace.advance_by(notificator.frontier(0));
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

#[inline(never)]
fn consolidate_a<T: Ord, R: Ring>(list: &mut Vec<(T, R)>) {
    list.sort_by(|x,y| x.0.cmp(&y.0));
    for index in 1 .. list.len() {
        if list[index].0 == list[index-1].0 {
            list[index].1 = list[index].1 + list[index-1].1;
            list[index-1].1 = R::zero();
        }
    }
    list.retain(|x| !x.1.is_zero());
}

#[inline(never)]
fn consolidate_b<T: Ord, R: Ring>(list: &mut Vec<(T, R)>) {
    list.sort_by(|x,y| x.0.cmp(&y.0));
    for index in 1 .. list.len() {
        if list[index].0 == list[index-1].0 {
            list[index].1 = list[index].1 + list[index-1].1;
            list[index-1].1 = R::zero();
        }
    }
    list.retain(|x| !x.1.is_zero());
}

#[inline(never)]
fn consolidate2<D: Ord, T: Ord, R: Ring>(list: &mut Vec<(D, T, R)>) {
    list.sort_by(|x,y| (&y.0, &y.1).cmp(&(&x.0, &x.1)));
    for index in 1 .. list.len() {
        if list[index].0 == list[index-1].0 && list[index].1 == list[index-1].1 {
            list[index].2 = list[index].2 + list[index-1].2;
            list[index-1].2 = R::zero();
        }
    }
    list.retain(|x| !x.2.is_zero());
}


#[inline(never)]
fn update<D, T, R>(edits: &[(D, T, R)], stage: &mut Vec<(D, R)>, prev_time: &T, this_time: &T) 
where D: Ord+Clone, T: Ord+Lattice, R: Ring {

    // back up to relevant updates.
    let mut start = edits.len();
    while start > 0 && edits[start-1].1.cmp(this_time) != ::std::cmp::Ordering::Greater {
        start -= 1;
    }

    let edits = &edits[start ..];

    for &(ref val, ref time, diff) in edits {
        let le_prev = time.le(&prev_time);
        let le_this = time.le(&this_time);
        if le_prev != le_this {
            if le_prev { stage.push((val.clone(),-diff)); }
            else       { stage.push((val.clone(), diff)); }
        }
    }
    consolidate_a(stage);
}

#[inline(never)]
fn close_interesting_times<D, T: Ord+Lattice+Clone, R>(edits: &[(D, T, R)], times: &mut Vec<T>) {

    // Candidate algorithm: sort list of (time, is_new) pairs describing old and new times. 
    // We aim to do not much worse than processing times in this order. 
    // 
    // We will maintain a few accumulations to help us correctly maintain the set of new times.
    //
    //   Frontier: We track the elements in the frontier, identified in an initial reverse scan.
    //   old_accum: We accumulate old times using the frontier, which I hope is correct.
    //   new_accum: We accumulate new times using the frontier, which I hope is correct.
    //
    // For each time, we check whether the frontier changes, and if so perhaps update our accums.
    // We then do something based on whether it is a new or old time:
    // 
    //   new time: join with each element of old_accum, add results + self to new_accum, re-close, emit.
    //   old time: join with each element of new_accum, re-close, emit.
    //
    // We can either re-close the new_accum set with each element, or repeat the process with new
    // synthetic times. In either case, we need to be careful to not leave a massive amount of work
    // behind (e.g. if a closed new_accum becomes enormous and un-accumulable, bad news for us). It is 
    // probably safe to close in place, as the in-order execution would do this anyhow.

    // // REFERENCE IMPLEMENTATION (LESS CLEVER)
    // let mut reference = times.clone();

    // let mut position = 0;
    // while position < reference.len() {
    //     for &(_, ref time, _) in edits {
    //         if !time.le(&reference[position]) {
    //             let join = time.join(&reference[position]);
    //             reference.push(join);
    //         }
    //     }
    //     position += 1;
    //     reference[position..].sort();
    //     reference.dedup();
    // }

    // CLEVER IMPLEMENTATION
    let mut total = Vec::new();
    let mut old_accum = Vec::new();
    let mut new_accum = Vec::new();

    // 1. populate uniform list of times, and indicate whether they are for new or old times.
    for &(_, ref time, _) in edits { total.push((time.clone(), 0)); }
    for time in times.iter() { total.push((time.clone(), 1)); }
    consolidate_a(&mut total);

    // 2. determine the frontiers by scanning list in reverse order (for each: when it exits frontier).
    let mut entrance = Vec::new();
    let mut frontier = Vec::new();
    let mut position = total.len();
    while position > 0 {
        position -= 1;
        // "add" total[position] and seeing who drops == who is exposed when total[position] removed.
        let mut index = 0;
        while index < frontier.len() {
            if total[position].0.le(&frontier[index]) {
                entrance.push((frontier.swap_remove(index), position));
            }
            else {
                index += 1;
            }
        }
        frontier.push(total[position].0.clone());
    }

    // 3. swing through `total` and apply per-time thinkings.
    let mut old_temp = Vec::new();
    let mut new_temp = Vec::new();
    let mut entrance_cursor = 0;
    for (index, (time, is_new)) in total.drain(..).enumerate() {
        
        if is_new > 0 {

            // new times should be joined against both accumulations. re-closing not needed, as already closed. 
            for new_time in &new_accum {
                let join = time.join(new_time);
                if join != time { new_temp.push(join.clone()); times.push(join); }
            }
            for old_time in &old_accum {
                let join = time.join(old_time);
                if join != time { new_accum.push(join.clone()); times.push(join); }
            }
            new_accum.push(time.clone());

            for t in new_temp.drain(..) { new_accum.push(t); }
        }
        else {

            // old times should be joined against the new accumulation. re-closing not needed, as already closed.
            for new_time in &new_accum {
                let join = time.join(new_time);
                if join != time { new_temp.push(join.clone()); times.push(join); }
            }
            for old_time in &old_accum {
                let join = time.join(old_time);
                if join != time { old_temp.push(join); }
            }
            old_accum.push(time.clone());

            for t in old_temp.drain(..) { old_accum.push(t); }
            for t in new_temp.drain(..) { new_accum.push(t); }
        }

        // update old_accum and new_accum with frontier changes, deduplicating.
        // a. remove time from frontier; it's not there any more.
        frontier.retain(|x| !x.eq(&time));
        // b. add any indicated elements
        while entrance_cursor < entrance.len() && entrance[entrance_cursor].1 == index {
            frontier.push(entrance[entrance_cursor].0.clone());
            entrance_cursor += 1;
        }

        for time in &mut old_accum { time.advance_by(&frontier[..]); }
        old_accum.sort();
        old_accum.dedup();

        for time in &mut new_accum { time.advance_by(&frontier[..]); }
        new_accum.sort();
        new_accum.dedup();
    }

    times.sort();
    times.dedup();

    // // TEST CLEVER OUTPUT vs REFERENCE OUTPUT.
    // assert!(reference == *times);
}

struct Accumulator<D: Ord+Clone, T: Lattice+Ord, R: Ring> {
    pub time: T,
    pub edits: Vec<(D, T, R)>,
    pub chains: Vec<(usize, usize, usize)>,
    pub accum: Vec<(D, R)>,
}

impl<D: Ord+Clone, T: Lattice+Ord, R: Ring> Accumulator<D, T, R> {

    fn new() -> Self {
        Accumulator {
            time: T::min(),
            edits: Vec::new(),
            chains: Vec::new(),
            accum: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.time = T::min();
        self.edits.clear();
        self.chains.clear();
        self.accum.clear();
    }

    /// Introduces edits into a live accumulation.
    ///
    ///
    fn push_edit(&mut self, edit: (D, T, R)) {

        // do we need a new chain?
        if self.edits.len() == 0 || !self.edits[self.edits.len()-1].1.le(&edit.1) {
            let edits = self.edits.len();
            self.chains.push((edits, edits, edits + 1));
            self.edits.push(edit);
        }
        else {
            let chains = self.chains.len();
            self.edits.push(edit);
            self.chains[chains-1].2 = self.edits.len();
        }

        // we may need to advance the finger of the last chain by one.
        let finger = self.chains[self.chains.len()-1].1;
        if self.edits[finger].1.le(&self.time) {
            self.accum.push((self.edits[finger].0.clone(), self.edits[finger].2));
            let chains_len = self.chains.len();
            self.chains[chains_len-1].1 += 1;
        }
    }

    // sorts edits, forms chains, and initializes accumulation.
    fn shackle<F: Fn(&T, &T)->::std::cmp::Ordering>(&mut self, time: T, cmp: F) {

        // arrange edits appropriately; no requirements on `cmp`, as we will segment by `le` anyhow.
        // self.edits.sort_by(cmp);

        let mut lower = 0;
        for upper in 1 .. self.edits.len() {
            if !self.edits[upper-1].1.le(&self.edits[upper].1) {
                self.chains.push((lower, lower, upper));
                lower = upper;
            }
        }

        if self.edits.len() > 0 {
            self.chains.push((lower, lower, self.edits.len()));
        }

        // fold appropriate edits into accumulation.
        self.time = time;
        for chain in 0 .. self.chains.len() {
            let mut finger = self.chains[chain].1;
            while finger < self.chains[chain].2 && self.edits[finger].1.le(&self.time) {
                self.accum.push((self.edits[finger].0.clone(), self.edits[finger].2));
                finger += 1;
            }
            self.chains[chain].1 = finger;
        }
    }

    /// Updates the internal accumulation to correspond to `new_time`.
    ///
    /// This method traverses edits by chains, or totally ordered subsequences. It traverses
    /// each chain at most once over the course of totally ordered updates, as it only moves
    /// forward through each of its own internal chains. Although the method should be correct
    /// for arbitrary sequences of times, the performance could be arbitrarily poor.

    fn update_to(&mut self, new_time: T) -> &[(D, R)] {

        let meet = self.time.meet(&new_time);
        for chain in 0 .. self.chains.len() {

            // finger is the first element *not* `le` this.time.
            let mut finger = self.chains[chain].1;

            // possibly move forward, adding edits while times less than `new_time`.
            while finger < self.chains[chain].2 && self.edits[finger].1.le(&new_time) {
                self.accum.push((self.edits[finger].0.clone(), self.edits[finger].2));
                finger += 1;
            }
            // possibly move backward, subtracting edits with times not less than `new_time`.
            while finger - 1 >= self.chains[chain].0 && !self.edits[finger-1].1.le(&new_time) {
                self.accum.push((self.edits[finger-1].0.clone(), -self.edits[finger-1].2));
                finger -= 1;                    
            }

            let position = ::std::cmp::min(self.chains[chain].1, finger);
            self.chains[chain].1 = finger;

            // from the lower end of updates things are less certain; 
            while position-1 >= self.chains[chain].0 && !self.edits[position-1].1.le(&meet) {

                let le_prev = self.edits[position-1].1.le(&self.time);
                let le_this = self.edits[position-1].1.le(&new_time);
                if le_prev != le_this {
                    if le_prev { self.accum.push((self.edits[position-1].0.clone(),-self.edits[position-1].2)); }
                    else       { self.accum.push((self.edits[position-1].0.clone(), self.edits[position-1].2)); }
                }

            }
        }

        self.time = new_time;
        consolidate_a(&mut self.accum);
        &self.accum[..]
    }
}