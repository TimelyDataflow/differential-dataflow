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

use std::fmt::Debug;
use std::default::Default;

use hashable::{Hashable, UnsignedWrapper};
use ::{Data, Collection, Ring};

// use timely::progress::Antichain;
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
    where G::Timestamp: Lattice+Ord+Debug, <K as Hashable>::Output: Data+Default {
    fn group<L, V2: Data, R2: Ring>(&self, logic: L) -> Collection<G, (K, V2), R2>
        where L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static {
        self.arrange_by_key_hashed_cached()
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

        // Our implementation maintains a list of outstanding `(key, time)` synthetic interesting times, 
        // as well as capabilities for these times (or their lower envelope, at least).
        let mut interesting = Vec::<(K, G::Timestamp)>::new();
        let mut capabilities = Vec::<Capability<G::Timestamp>>::new();

        // buffers and logic for computing per-key interesting times "efficiently".
        let mut interestinator: Interestinator<G::Timestamp> = Default::default();
        let mut interesting_times = Vec::<G::Timestamp>::new();

        // accumulators for input, output, and output produced as we execute.
        let mut input_accumulator = Accumulator::<V, G::Timestamp, R>::new();
        let mut output_accumulator = Accumulator::<V2, G::Timestamp, R2>::new();
        let mut yielded_accumulator = Accumulator::<V2, G::Timestamp, R2>::new();

        // where the user's logic produces output.
        let mut output_logic = Vec::<(V2, R2)>::new();

        // where we stash times as we extract from histories.
        let mut time_buffer1 = Vec::new();
        let mut time_buffer2 = Vec::new();

        // tracks frontiers received from batches, for sanity.
        let mut lower_sanity = vec![<G::Timestamp as Lattice>::min()];
        let mut lower_issued = vec![<G::Timestamp as Lattice>::min()];

        // fabricate a data-parallel operator using the `unary_notify` pattern.
        let stream = self.stream.unary_notify(Pipeline, "Group", Vec::new(), move |input, output, notificator| {

            // The `group` operator receives fully formed batches, which each serve as an indication
            // that the frontier has advanced to the upper bound of their description.
            //
            // Although we could act on each individually, several may have been sent, and it makes 
            // sense to accumulate them first to coordinate their re-evaluation. We will need to pay
            // attention to which times need to be collected under which capability, so that we can
            // assemble output batches correctly. We will maintain several builders concurrently, and
            // place output updates into the appropriate builder.
            //
            // It turns out we must use notificators, as we cannot await empty batches from arrange to 
            // indicate progress, as the arrange may not hold the capability to send such. Instead, we
            // must watch for progress here (and the upper bound of received batches) to tell us how 
            // far we can process work.
            // 
            // We really want to retire all batches we receive, so we want a frontier which reflects 
            // both information from batches as well as progress information. I think this means that 
            // we keep times that are greater than or equal to a time in the other frontier, deduplicated.

            let mut batch_cursors = Vec::new();

            // capture each batch into our stash; update our view of the frontier.
            input.for_each(|capability, batches| {
                for batch in batches.drain(..).map(|x| x.item) {
                    assert!(&lower_sanity[..] == batch.description().lower());
                    lower_sanity = batch.description().upper().to_vec();
                    batch_cursors.push(batch.cursor());                    
                }

                // update capabilities to also cover the capabilities of these batches.
                capabilities.retain(|cap| !capability.time().lt(&cap.time()));
                if !capabilities.iter().any(|cap| cap.time().le(&capability.time())) {
                    capabilities.push(capability);
                }
            });

            // assemble frontier we will use from `upper` and our notificator frontier.
            let mut upper_limit = notificator.frontier(0).to_vec();
            upper_limit.retain(|t1| !lower_sanity.iter().any(|t2| t1.lt(t2)));
            for time in &lower_sanity {
                if !upper_limit.iter().any(|t1| time.le(t1)) {
                    upper_limit.push(time.clone());
                }
            }

            if batch_cursors.len() > 0 || interesting.len() > 0 {

                // Our plan is to retire all updates not at times greater or equal to an element of `frontier`.
                sort_dedup(&mut interesting);
                let mut new_interesting = Vec::new();
                let mut exposed = Vec::new();
                segment(&mut interesting, &mut exposed, &mut new_interesting, |&(_, ref time)| {
                    !upper_limit.iter().any(|t| t.le(&time))
                });
                interesting = new_interesting;

                // Prepare an output buffer and builder for each capability. 
                // It would be better if all updates went into one batch, but timely dataflow prevents this as 
                // long as there is only one capability for each message.
                let mut buffers = Vec::<Vec<(V2, G::Timestamp, R2)>>::new();
                let mut builders = Vec::new();
                for _ in 0 .. capabilities.len() {
                    buffers.push(Vec::new());
                    builders.push(<T2::Batch as Batch<K,V2,G::Timestamp,R2>>::Builder::new());
                }

                // cursors for navigating input and output traces.
                let mut source_cursor: T1::Cursor = source_trace.cursor();
                let mut output_cursor: T2::Cursor = output_trace.cursor();

                let mut synth_position = 0;
                batch_cursors.retain(|cursor| cursor.key_valid());
                while batch_cursors.len() > 0 || synth_position < exposed.len() {

                    // determine the next key we will work on; could be synthetic, could be from a batch.
                    let mut key = None;
                    if synth_position < exposed.len() { key = Some(exposed[synth_position].0.clone()); }
                    for batch_cursor in &batch_cursors {
                        if key == None { key = Some(batch_cursor.key().clone()); }
                        else {
                            key = key.map(|k| ::std::cmp::min(k, batch_cursor.key().clone()));
                        }
                    }

                    debug_assert!(key.is_some());
                    let key = key.unwrap();

                    // Prepare `interesting_times` for this key.
                    interesting_times.clear();

                    // populate `interesting_times` with synthetic interesting times for this key.
                    while synth_position < exposed.len() && exposed[synth_position].0 == key {
                        interesting_times.push(exposed[synth_position].1.clone());
                        synth_position += 1;
                    }

                    // populate `interesting_times` with times from newly accepted updates.
                    for batch_cursor in &mut batch_cursors {
                        if batch_cursor.key_valid() && batch_cursor.key() == &key {
                            while batch_cursor.val_valid() {
                                batch_cursor.map_times(|time,_| interesting_times.push(time.clone()));
                                batch_cursor.step_val();
                            }
                            batch_cursor.step_key();
                        }
                    }
                    batch_cursors.retain(|cursor| cursor.key_valid());

                    // tidy up times, removing redundancy.
                    interesting_times.sort();
                    interesting_times.dedup();

                    // Determine the `meet` of times, useful in restricting updates to capture.
                    let mut meet = interesting_times[0].clone(); 
                    for index in 1 .. interesting_times.len() {
                        meet = meet.meet(&interesting_times[index]);
                    }

                    // clear accumulators (will now repopulate)
                    yielded_accumulator.clear();
                    yielded_accumulator.time = interesting_times[0].clone();

                    // Accumulate into `input_stage` and populate `input_edits`.
                    input_accumulator.clear();
                    input_accumulator.time = interesting_times[0].clone();
                    source_cursor.seek_key(&key);
                    if source_cursor.key_valid() && source_cursor.key() == &key {
                        while source_cursor.val_valid() {
                            let val: V = source_cursor.val().clone();
                            let mut sum = R::zero();

                            source_cursor.map_times(|t,d| {
                                if t.le(&input_accumulator.time) { 
                                    sum = sum + d; 
                                }
                                if !t.le(&meet) {
                                    time_buffer1.push((t.join(&meet), d));
                                }
                            });
                            consolidate(&mut time_buffer1);
                            input_accumulator.edits.extend(time_buffer1.drain(..).map(|(t,d)| (val.clone(), t, d)));
                            if !sum.is_zero() {
                                input_accumulator.accum.push((val, sum));
                            }
                            source_cursor.step_val();
                        }
                    }
                    input_accumulator.shackle();

                    // Accumulate into `output_stage` and populate `output_edits`. 
                    output_accumulator.clear();
                    output_accumulator.time = interesting_times[0].clone();
                    output_cursor.seek_key(&key);
                    if output_cursor.key_valid() && output_cursor.key() == &key {
                        while output_cursor.val_valid() {
                            let val: V2 = output_cursor.val().clone();
                            let mut sum = R2::zero();
                            output_cursor.map_times(|t,d| {
                                if t.le(&output_accumulator.time) {
                                    sum = sum + d;
                                }
                                if !t.le(&meet) {
                                    time_buffer2.push((t.join(&meet), d));
                                }
                            });
                            consolidate(&mut time_buffer2);
                            output_accumulator.edits.extend(time_buffer2.drain(..).map(|(t,d)| (val.clone(), t, d)));
                            if !sum.is_zero() {
                                output_accumulator.accum.push((val, sum));
                            }
                            output_cursor.step_val();
                        }
                    }
                    output_accumulator.shackle();

                    // Determine all interesting times: those that are the join of at least one time
                    // from `interesting_times` and any times from `input_edits`.
                    interestinator.close_interesting_times(&input_accumulator.edits[..], &mut interesting_times);

                    // println!("interesting_times: {:?}", interesting_times.len());

                    // each interesting time must be considered!
                    for this_time in interesting_times.drain(..) {

                        // not all times are ready to be finalized. stash them with the key.
                        if upper_limit.iter().any(|t| t.le(&this_time)) {
                            interesting.push((key.clone(), this_time));
                        }
                        else {

                            // 1. update `input_stage` and `output_stage` to `this_time`.
                            input_accumulator.update_to(this_time.clone());
                            output_accumulator.update_to(this_time.clone());
                            yielded_accumulator.update_to(this_time.clone());

                            // 2. apply user logic (only if non-empty input).
                            output_logic.clear();
                            if input_accumulator.accum.len() > 0 {
                                logic(&key, &input_accumulator.accum[..], &mut output_logic);
                            }

                            // println!("key: {:?}, input: {:?}, ouput: {:?} @ {:?}", 
                            //          key, &input_accumulator.accum[..], output_logic, this_time);

                            // 3. subtract existing output differences.
                            for &(ref val, diff) in &output_accumulator.accum[..] {
                                output_logic.push((val.clone(), -diff));
                            }
                            // incorporate uncommitted output updates.
                            for &(ref val, diff) in &yielded_accumulator.accum {
                                output_logic.push((val.clone(), -diff));
                            }
                            consolidate(&mut output_logic);

                            // 4. stashed produced updates in capability-indexed buffers, and `yielded_accumulator`.
                            let idx = capabilities.iter().rev().position(|cap| cap.le(&this_time));
                            debug_assert!(idx.is_some());
                            let idx = idx.unwrap();
                            debug_assert_eq!(capabilities.len(), buffers.len());
                            debug_assert!(capabilities.len() - idx - 1 < buffers.len());
                            for (val, diff) in output_logic.drain(..) {
                                yielded_accumulator.push_edit((val.clone(), this_time.clone(), diff));
                                buffers[capabilities.len() - idx - 1].push((val, this_time.clone(), diff));
                            }
                        }
                    }
        
                    // move all updates for this key into corresponding builders.
                    for index in 0 .. buffers.len() {
                        buffers[index].sort();
                        for (val, time, diff) in buffers[index].drain(..) {
                            builders[index].push((key.clone(), val, time, diff));
                        }
                    }
                }

                // seal up builders, send along with capabilities, downgrade capabilities to track `interesting` pairs.

                for (index, builder) in builders.drain(..).enumerate() {

                    // this builder captured times not greater or equal to any later capability, nor any element of upper.
                    let mut local_upper = upper_limit.clone();
                    for capability in &capabilities[index + 1 ..] {
                        local_upper.retain(|t| !capability.lt(t));
                        if !local_upper.iter().any(|t| t.lt(&capability.time())) {
                            local_upper.push(capability.time());
                        }
                    }

                    let batch = builder.done(&lower_issued[..], &local_upper[..]);
                    lower_issued = local_upper;

                    output.session(&capabilities[index]).give(BatchWrapper { item: batch.clone() });
                    output_trace.wrapper.borrow_mut().trace.insert(batch);

                }

                assert!(lower_issued == upper_limit);

                // update capabilities to reflect `interesting` pairs.
                let mut frontier = Vec::new();
                for &(_, ref time) in &interesting {
                    frontier.retain(|t| !time.lt(t));
                    if !frontier.iter().any(|t| t.le(time)) {
                        frontier.push(time.clone());
                    }
                }

                // for time in &frontier {
                //     debug_assert!(upper_limit.iter().any(|t| t.le(time)));
                // }

                // update capabilities (readable?)
                let mut new_capabilities = Vec::new();
                for time in frontier.drain(..) {
                    if let Some(cap) = capabilities.iter().find(|c| c.time().le(&time)) {
                        new_capabilities.push(cap.delayed(&time));
                    }
                    else {
                        println!("failed to find capability less than new frontier time:");
                        println!("  time: {:?}", time);
                        println!("  caps: {:?}", capabilities);
                    }
                }
                capabilities = new_capabilities;
            }

            // We have processed all updates through `frontier`, and can advance input and output traces.
            source_trace.advance_by(&upper_limit[..]);
            output_trace.advance_by(&upper_limit[..]);

        });

        Arranged { stream: stream, trace: result_trace }
    }
}

// Several methods are broken out here to help understand performance profiling.

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

// #[inline(never)]
fn consolidate<T: Ord, R: Ring>(list: &mut Vec<(T, R)>) {
    list.sort_by(|x,y| x.0.cmp(&y.0));
    for index in 1 .. list.len() {
        if list[index].0 == list[index-1].0 {
            list[index].1 = list[index].1 + list[index-1].1;
            list[index-1].1 = R::zero();
        }
    }
    list.retain(|x| !x.1.is_zero());
}

// // #[inline(never)]
// fn consolidate2<D: Ord, T: Ord, R: Ring>(list: &mut Vec<(D, T, R)>) {
//     list.sort_by(|x,y| (&y.0, &y.1).cmp(&(&x.0, &x.1)));
//     for index in 1 .. list.len() {
//         if list[index].0 == list[index-1].0 && list[index].1 == list[index-1].1 {
//             list[index].2 = list[index].2 + list[index-1].2;
//             list[index-1].2 = R::zero();
//         }
//     }
//     list.retain(|x| !x.2.is_zero());
// }

/// Allocated state and temporary buffers for closing interesting time under join.
#[derive(Default)]
struct Interestinator<T: Ord+Lattice+Clone> {

    total: Vec<(T, isize)>,     // holds all times; a non-zero value indicates a new time.

    old_accum: Vec<T>,          // accumulated old times, compacted via self.frontier.
    new_accum: Vec<T>,          // accumulated new times, compacted via self.frontier.

    entrance: Vec<(T, usize)>,  // times and self.total indices at which they enter self.frontier.
    frontier: Vec<T>,           // allocation to hold the evolving frontier.

    old_temp: Vec<T>,           // temporary storage to avoid 
    new_temp: Vec<T>,
}

impl<T: Ord+Lattice+Clone+::std::fmt::Debug> Interestinator<T> {
    /// Extends `times` with the join of subsets of `edits` and `times` with at least one element from `times`.
    ///
    /// This method has a somewhat non-standard implementation in the aim of being "more linear", which makes it
    /// a bit more complicated that you might think, and with possibly surprising performance. If this method shows
    /// up on performance profiling, it may be worth asking for more information, as it is a work in progress.
    #[inline(never)]
    fn close_interesting_times<D, R>(&mut self, edits: &[(D, T, R)], times: &mut Vec<T>) {

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

        if edits.len() + times.len() < 10 { 
            self._close_interesting_times_reference(edits, times);
        }
        else {

            // CLEVER IMPLEMENTATION
            // 1. populate uniform list of times, and indicate whether they are for new or old times.
            assert!(self.total.len() == 0);
            self.total.reserve(edits.len() + times.len());
            for &(_, ref time, _) in edits { self.total.push((time.clone(), 1)); }
            for time in times.iter() { self.total.push((time.clone(), edits.len() as isize + 1)); }

            consolidate(&mut self.total);

            // 2. determine the frontiers by scanning list in reverse order (for each: when it exits frontier).
            self.frontier.clear();
            self.entrance.clear();
            self.entrance.reserve(self.total.len());
            let mut position = self.total.len();
            while position > 0 {
                position -= 1;
                // "add" total[position] and seeing who drops == who is exposed when total[position] removed.
                let mut index = 0;
                while index < self.frontier.len() {
                    if self.total[position].0.le(&self.frontier[index]) {
                        self.entrance.push((self.frontier.swap_remove(index), position));
                    }
                    else {
                        index += 1;
                    }
                }
                self.frontier.push(self.total[position].0.clone());
            }

            // 3. swing through `total` and apply per-time thinkings.
            self.old_temp.clear();
            self.new_temp.clear();
            self.old_accum.clear();
            self.new_accum.clear();

            for (index, (time, is_new)) in self.total.drain(..).enumerate() {
                
                if is_new > edits.len() as isize {
                    for new_time in &self.new_accum {
                        let join = time.join(new_time);
                        if join != time { 
                            self.new_temp.push(join.clone()); 
                            times.push(join); 
                        }
                    }
                    for t in self.new_temp.drain(..) { self.new_accum.push(t); }

                    for old_time in &self.old_accum {
                        let join = time.join(old_time);
                        if join != time { 
                            self.new_accum.push(join.clone()); 
                            times.push(join); 
                        }
                    }
                    self.new_accum.push(time.clone());
                }
                else {

                    for new_time in &self.new_accum {
                        let join = time.join(new_time);
                        self.new_temp.push(join.clone()); 
                        times.push(join); 
                    }
                    for t in self.new_temp.drain(..) { self.new_accum.push(t); }

                    for old_time in &self.old_accum {
                        let join = time.join(old_time);
                        if join != time { self.old_temp.push(join); }
                    }
                    for t in self.old_temp.drain(..) { self.old_accum.push(t); }
                    self.old_accum.push(time.clone());
                }

        
                // update old_accum and new_accum with frontier changes, deduplicating.

                // TODO: Can we mantain frontiers corresponding to new times and the current `new_accum`, as all 
                //       future additions must be joined with such elements. This could reduce the complexity of 
                //       `old_accum` substantially, removing distinctions between irrelevant prior times.

                // a. remove time from frontier; it's not there any more.
                self.frontier.retain(|x| !x.eq(&time));
                // b. add any indicated elements
                while self.entrance.last().map(|x| x.1) == Some(index) {
                    self.frontier.push(self.entrance.pop().unwrap().0);
                }

                if self.frontier.len() > 0 {
                    // advance times in the old accumulation, sort and deduplicate.
                    for time in &mut self.old_accum { *time = time.advance_by(&self.frontier[..]).unwrap(); }
                    self.old_accum.sort();
                    self.old_accum.dedup();
                    // advance times in the new accumulation, sort and deduplicate.
                    for time in &mut self.new_accum { *time = time.advance_by(&self.frontier[..]).unwrap(); }
                    self.new_accum.sort();
                    self.new_accum.dedup();
                }
            }

            times.sort();
            times.dedup();

            // assert_eq!(*times, reference);
        }
    }

    /// Reference implementation for `close_interesting_times`, using lots more effort than should be needed.
    fn _close_interesting_times_reference<D, R>(&mut self, edits: &[(D, T, R)], times: &mut Vec<T>) {

        // REFERENCE IMPLEMENTATION (LESS CLEVER)
        let times_len = times.len();
        for position in 0 .. times_len {
            for &(_, ref time, _) in edits {
                if !time.le(&times[position]) {
                    let join = time.join(&times[position]);
                    times.push(join);
                }
            }
        }
        
        let mut position = 0;
        while position < times.len() {
            for index in 0 .. position {
                if !times[index].le(&times[position]) {
                    let join = times[index].join(&times[position]);
                    times.push(join);
                }
            }
            position += 1;
            times[position..].sort();
            times.dedup();
        }
    }
}

/// Maintains an accumulation of updates over partially ordered times.
///
/// The `Accumulator` tries to cleverly partition its input edits into "chains": totally ordered contiguous 
/// subsequences. These chains simplify updating of accumulations, as in each chain it is relatively easy to 
/// understand how mnay updates must be re-considered.
///
/// Many of the methods on `Accumulator` require understanding of how they should be used. Perhaps this can
/// be fixed with an `AccumulatorBuilder` helper type, but the intended life-cycle is: the `Accumulator` is 
/// initially valid, and `push_edit` and `update_to` may be called at will. Direct manipulation of the public
/// fields (which we do) requires a call to `shackle` before using the `Accumulator` again (to rebuild the 
/// chains and correct the accumulation).
struct Accumulator<D: Ord+Clone, T: Lattice+Ord, R: Ring> {
    pub time: T,
    pub edits: Vec<(D, T, R)>,
    pub chains: Vec<(usize, usize, usize)>,
    pub accum: Vec<(D, R)>,
}

impl<D: Ord+Clone, T: Lattice+Ord+Debug, R: Ring> Accumulator<D, T, R> {

    /// Allocates a new empty accumulator.
    fn new() -> Self {
        Accumulator {
            time: T::min(),
            edits: Vec::new(),
            chains: Vec::new(),
            accum: Vec::new(),
        }
    }

    /// Clears the allocator and sets the time to `T::min()`.
    fn clear(&mut self) {
        self.time = T::min();
        self.edits.clear();
        self.chains.clear();
        self.accum.clear();
    }

    /// Introduces edits into a live accumulation.
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

    /// Sorts `edits` and forms chains.
    fn shackle(&mut self) {

        // consolidate2(&mut self.edits);
        consolidate(&mut self.accum);

        self.edits.sort_by(|x,y| x.1.cmp(&y.1));

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

        for chain in 0 .. self.chains.len() {
            let mut finger = self.chains[chain].1;
            while finger < self.chains[chain].2 && self.edits[finger].1.le(&self.time) {
                finger += 1;
            }
            self.chains[chain].1 = finger;
        }

        // if self.chains.len() > 5 { println!("chains: {:?}", self.chains.len()); }
    }
    
    /// Updates the internal accumulation to correspond to `new_time`.
    ///
    /// This method traverses edits by chains, or totally ordered subsequences. It traverses
    /// each chain at most once over the course of totally ordered updates, as it only moves
    /// forward through each of its own internal chains. Although the method should be correct
    /// for arbitrary sequences of times, the performance could be arbitrarily poor.

    #[inline(never)]
    fn update_to(&mut self, new_time: T) -> &[(D, R)] {

        // println!("updating from {:?} to {:?}", self.time, new_time);

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
            while finger > self.chains[chain].0 && !self.edits[finger-1].1.le(&new_time) {
                self.accum.push((self.edits[finger-1].0.clone(), -self.edits[finger-1].2));
                finger -= 1;                    
            }

            let position = ::std::cmp::min(self.chains[chain].1, finger);
            self.chains[chain].1 = finger;

            // from the lower end of updates things are less certain; 
            while position > self.chains[chain].0 && !self.edits[position-1].1.le(&meet) {

                let le_prev = self.edits[position-1].1.le(&self.time);
                let le_this = self.edits[position-1].1.le(&new_time);
                if le_prev != le_this {
                    if le_prev { self.accum.push((self.edits[position-1].0.clone(),-self.edits[position-1].2)); }
                    else       { self.accum.push((self.edits[position-1].0.clone(), self.edits[position-1].2)); }
                }

            }
        }

        self.time = new_time;
        consolidate(&mut self.accum);
        &self.accum[..]
    }
}