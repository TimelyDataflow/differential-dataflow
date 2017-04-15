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

use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely_sort::Unsigned;

use operators::arrange::{Arrange, Arranged, ArrangeByKey, ArrangeBySelf, BatchWrapper, TraceHandle};
use lattice::Lattice;
use trace::{Batch, Cursor, Trace, Builder};
// use trace::implementations::hash::HashValSpine as DefaultValTrace;
// use trace::implementations::hash::HashKeySpine as DefaultKeyTrace;
use trace::implementations::ord::OrdValSpine as DefaultValTrace;
use trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;


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
        // self.arrange_by_key_hashed_cached()
        self.arrange_by_key_hashed()
            .group_arranged(move |k,s,t| logic(&k.item,s,t), DefaultValTrace::new())
            .as_collection(|k,v| (k.item.clone(), v.clone()))
    }
    fn group_u<L, V2: Data, R2: Ring>(&self, logic: L) -> Collection<G, (K, V2), R2>
        where L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static, K: Unsigned+Copy {
        self.map(|(k,v)| (UnsignedWrapper::from(k), v))
            .arrange(DefaultValTrace::new())
            .group_arranged(move |k,s,t| logic(&k.item,s,t), DefaultValTrace::new())
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
            .group_arranged(|_k,_s,t| t.push(((), 1)), DefaultKeyTrace::new())
            .as_collection(|k,_| k.item.clone())
    }
    fn distinct_u(&self) -> Collection<G, K, isize> where K: Unsigned+Copy {
        self.map(|k| (UnsignedWrapper::from(k), ()))
            .arrange(DefaultKeyTrace::new())
            .group_arranged(|_k,_s,t| t.push(((), 1)), DefaultKeyTrace::new())
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
            .group_arranged(|_k,s,t| t.push((s[0].1, 1)), DefaultValTrace::new())
            .as_collection(|k,&c| (k.item.clone(), c))
    }
    fn count_u(&self) -> Collection<G, (K, R), isize> where K: Unsigned+Copy {
        self.map(|k| (UnsignedWrapper::from(k), ()))
            .arrange(DefaultKeyTrace::new())
            .group_arranged(|_k,s,t| t.push((s[0].1, 1)), DefaultValTrace::new())
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
        let mut output_trace = TraceHandle::new(empty, &[G::Timestamp::min()], &[G::Timestamp::min()]);
        let result_trace = output_trace.clone();

        let mut thinker1 = interest_accumulator::InterestAccumulator::<V, V2, G::Timestamp, R, R2>::new();
        let mut thinker2 = history_replay::HistoryReplayer::<V, V2, G::Timestamp, R, R2>::new();
        let mut temporary = Vec::<G::Timestamp>::new();

        // Our implementation maintains a list of outstanding `(key, time)` synthetic interesting times, 
        // as well as capabilities for these times (or their lower envelope, at least).
        let mut interesting = Vec::<(K, G::Timestamp)>::new();
        let mut capabilities = Vec::<Capability<G::Timestamp>>::new();

        // buffers and logic for computing per-key interesting times "efficiently".
        let mut interesting_times = Vec::<G::Timestamp>::new();

        // space for assembling the upper bound of times to process.
        let mut upper_limit = Vec::<G::Timestamp>::new();

        // tracks frontiers received from batches, for sanity.
        let mut upper_received = vec![<G::Timestamp as Lattice>::min()];

        // We separately track the frontiers for what we have sent, and what we have sealed. 
        let mut lower_issued = vec![<G::Timestamp as Lattice>::min()];

        let id = self.stream.scope().index();

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

            // Drain the input stream of batches, validating the contiguity of the batch descriptions and
            // capturing a cursor for each of the batches as well as ensuring we hold a capability for the
            // times in the batch.
            input.for_each(|capability, batches| {

                // In principle we could have multiple batches per message (in practice, it would be weird).
                for batch in batches.drain(..).map(|x| x.item) {
                    assert!(&upper_received[..] == batch.description().lower());
                    upper_received = batch.description().upper().to_vec();
                    batch_cursors.push(batch.cursor());                    
                }

                // Ensure that `capabilities` covers the capability of the batch.
                capabilities.retain(|cap| !capability.time().lt(&cap.time()));
                if !capabilities.iter().any(|cap| cap.time().le(&capability.time())) {
                    capabilities.push(capability);
                }
            });

            // The interval of times we can retire is upper bounded by both the most recently received batch
            // upper bound (`upper_received`) and by the input progress frontier (`notificator.frontier(0)`).
            // Any new changes must be at times in advance of *both* of these antichains, as both the batch 
            // and the frontier guarantee no more updates at times not in advance of them.
            //
            // I think the right thing to do is define a new frontier from the joins of elements in the two
            // antichains. Elements we will see in the future must be greater or equal to elements in both
            // antichains, and so much be greater or equal to some pairwise join of the antichain elements.
            // At the same time, any element greater than some pairwise join is greater than either antichain,
            // and so could plausibly be seen in the future (and so is not safe to retire).
            upper_limit.clear();
            for time1 in notificator.frontier(0) {
                for time2 in &upper_received {
                    let join = time1.join(time2);
                    if !upper_limit.iter().any(|t| t.le(&join)) {
                        upper_limit.retain(|t| !join.le(t));
                        upper_limit.push(join);
                    }
                }
            }

            // If we have no capabilities, then we (i) should not produce any outputs and (ii) could not send
            // any produced outputs even if they were (incorrectly) produced. We cannot even send empty batches
            // to indicate forward progress, and must hope that downstream operators look at progress frontiers
            // as well as batch descriptions.
            //
            // We can (and should) advance source and output traces if `upper_limit` indicates this is possible.
            if capabilities.iter().any(|c| !upper_limit.iter().any(|t| t.le(&c.time()))) {

                // println!("upper: {:?}", upper_limit);

                // println!("working in group; advancing from, to:");
                // println!("  capabilities: {:?}", capabilities);
                // println!("  upper_limit:  {:?}", upper_limit);
                // println!("  frontier:     {:?}", notificator.frontier(0));
                // println!("  received:     {:?}", upper_received);

                // `interesting` contains "warnings" about keys and times that may need to be re-considered.
                // We first extract those times from this list that lie in the interval we will process.
                sort_dedup(&mut interesting);
                let mut new_interesting = Vec::new();
                let mut exposed = Vec::new();
                segment(&mut interesting, &mut exposed, &mut new_interesting, |&(_, ref time)| {
                    !upper_limit.iter().any(|t| t.le(&time))
                });
                interesting = new_interesting;

                // Prepare an output buffer and builder for each capability. 
                //
                // We buffer and build separately, as outputs are produced grouped by time, whereas the 
                // builder wants to see outputs grouped by value. While the per-key computation could 
                // do the re-sorting itself, buffering per-key outputs lets us double check the results
                // against other implementations for accuracy.
                //
                // TODO: It would be better if all updates went into one batch, but timely dataflow prevents 
                //       this as long as it requires that there is only one capability for each message.
                let mut buffers = Vec::<(G::Timestamp, Vec<(V2, G::Timestamp, R2)>)>::new();
                let mut builders = Vec::new();
                for i in 0 .. capabilities.len() {
                    buffers.push((capabilities[i].time(), Vec::new()));
                    builders.push(<T2::Batch as Batch<K,V2,G::Timestamp,R2>>::Builder::new());
                }

                // cursors for navigating input and output traces.
                let mut source_cursor: T1::Cursor = source_trace.cursor();
                let mut output_cursor: T2::Cursor = output_trace.cursor();

                let mut compute_counter = 0;
                let mut output_counter = 0;
                let timer = ::std::time::Instant::now();

                // We now march through the keys we must work on, drawing from `batch_cursors` and `exposed`.
                //
                // We only keep valid cursors (those with more data) in `batch_cursors`, and so its length
                // indicates whether more data remain. We move throuh `exposed` using `exposed_position`.
                // There could perhaps be a less provocative variable name.
                let mut exposed_position = 0;
                batch_cursors.retain(|cursor| cursor.key_valid());
                while batch_cursors.len() > 0 || exposed_position < exposed.len() {

                    // Determine the next key we will work on; could be synthetic, could be from a batch.
                    let mut key = None;
                    if exposed_position < exposed.len() { key = Some(exposed[exposed_position].0.clone()); }
                    for batch_cursor in &batch_cursors {
                        if key == None { key = Some(batch_cursor.key().clone()); }
                        else           { key = key.map(|k| ::std::cmp::min(k, batch_cursor.key().clone())); }
                    }
                    debug_assert!(key.is_some());
                    let key = key.unwrap();

                    // `interesting_times` contains those times between `lower_issued` and `upper_limit` 
                    // that we need to re-consider. We now populate it, but perhaps this should be left
                    // to the per-key computation, which may be able to avoid examining the times of some
                    // values (for example, in the case of min/max/topk).
                    interesting_times.clear();

                    // Populate `interesting_times` with synthetic interesting times for this key.
                    while exposed_position < exposed.len() && exposed[exposed_position].0 == key {
                        interesting_times.push(exposed[exposed_position].1.clone());
                        exposed_position += 1;
                    }

                    // Populate `interesting_times` with times from newly accepted updates.
                    // TODO: This work is partially redundant with the traversal of input updates, and 
                    //       in clever implementations (e.g. min/max/topk) we may want to extract these 
                    //       updates lazily.
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

                    // let mut interesting_times2 = interesting_times.clone();
                    // let mut buffers2 = buffers.clone();
                    // let mut temp2 = Vec::new();

                    // do the per-key computation.
                    temporary.clear();
                    let counters = thinker2.compute(
                        &key, 
                        &mut source_cursor, 
                        &mut output_cursor, 
                        &mut batch_cursors[..],
                        &mut interesting_times, 
                        &logic, 
                        &upper_limit[..], 
                        &mut buffers[..], 
                        &mut temporary,
                    );

                    compute_counter += counters.0;
                    output_counter += counters.1;

                    // source_cursor.rewind_vals();
                    // output_cursor.rewind_vals();
                    // // do the per-key computation.
                    // thinker1.compute(
                    //     &key, 
                    //     &mut source_cursor, 
                    //     &mut output_cursor, 
                    //     &mut interesting_times2, 
                    //     &logic, 
                    //     &upper_limit[..], 
                    //     &mut buffers2[..], 
                    //     &mut temp2,
                    // );

                    // if buffers != buffers2 {
                    //     println!("PANIC: Unequal outputs!!! (key: {:?}:", key);
                    //     for index in 0 .. buffers.len() {
                    //         if buffers[index] != buffers2[index] {
                    //             for thing in &buffers[index].1 { println!("  buff1[{}]: {:?}", index, thing); }
                    //             for thing in &buffers2[index].1 { println!("  buff2[{}]: {:?}", index, thing); }
                    //         }
                    //     }
                    // }
                    // assert_eq!(buffers, buffers2);

                    // // if any of temp2 is not present in temporary, we may miss a time.
                    // for time in &temp2 {
                    //     if !temporary.iter().any(|t2| t2.le(&time)) {
                    //         println!("PANIC: may miss time: {:?} for key: {:?}", time, key);
                    //     }
                    // }

                    // assert_eq!(temporary, temp2);

                    // Record future warnings about interesting times (and assert they should be "future").
                    for time in temporary.drain(..) { 
                        assert!(upper_limit.iter().any(|t| t.le(&time)));
                        interesting.push((key.clone(), time)); 
                    }

                    // Sort each buffer by value and move into the corresponding builder.
                    // TODO: This makes assumptions about at least one of (i) the stability of `sort_by`, 
                    //       (ii) that the buffers are time-ordered, and (iii) that the builders accept 
                    //       arbitrarily ordered times.
                    for index in 0 .. buffers.len() {
                        buffers[index].1.sort_by(|x,y| x.0.cmp(&y.0));
                        for (val, time, diff) in buffers[index].1.drain(..) {
                            builders[index].push((key.clone(), val, time, diff));
                        }
                    }
                }

                // build and ship each batch (because only one capability per message).
                for (index, builder) in builders.drain(..).enumerate() {
                    let mut local_upper = upper_limit.clone();
                    for capability in &capabilities[index + 1 ..] {
                        let time = capability.time();
                        if !local_upper.iter().any(|t| t.le(&time)) {
                            local_upper.retain(|t| !time.lt(t));
                            local_upper.push(time);
                        }
                    }

                    if lower_issued != local_upper {
                        let batch = builder.done(&lower_issued[..], &local_upper[..], &lower_issued[..]);
                        lower_issued = local_upper;
                        output.session(&capabilities[index]).give(BatchWrapper { item: batch.clone() });
                        output_trace.wrapper.borrow_mut().trace.insert(batch);
                    }
                }

                // Determine the frontier of our interesting times.
                let mut frontier = Vec::<G::Timestamp>::new();
                for &(_, ref time) in &interesting {                    
                    if !frontier.iter().any(|t| t.le(time)) {
                        frontier.retain(|t| !time.lt(t));
                        frontier.push(time.clone());
                    }
                }

                // Update `capabilities` to reflect interesting pairs described by `frontier`.
                let mut new_capabilities = Vec::new();
                for time in frontier.drain(..) {
                    if let Some(cap) = capabilities.iter().find(|c| c.time().le(&time)) {
                        new_capabilities.push(cap.delayed(&time));
                    }
                    else {
                        println!("{}:\tfailed to find capability less than new frontier time:", id);
                        println!("{}:\t  time: {:?}", id, time);
                        println!("{}:\t  caps: {:?}", id, capabilities);
                        println!("{}:\t  uppr: {:?}", id, upper_limit);
                    }
                }
                capabilities = new_capabilities;

                // let mut ul = upper_limit.clone();
                // ul.sort();

                // let avg_ns = if compute_counter > 0 { (timer.elapsed() / compute_counter as u32).subsec_nanos() } else { 0 };
                // println!("(key, times) pairs:\t{:?}\t{:?}\tavg {:?}ns\t{:?}", compute_counter, output_counter, avg_ns, ul);
            }

            // We have processed all updates through `upper_limit` and will only use times in advance of 
            // this frontier to compare against historical times, so we should allow the trace to start
            // compacting batches by advancing times.
            source_trace.advance_by(&upper_limit[..]);
            output_trace.advance_by(&upper_limit[..]);

            // We no longer need to distinguish between the batches we have received and historical batches,
            // so we should allow the trace to start merging them.
            //
            // Note: We know that there will be no further times through `upper_limit`, but we still use the
            // earlier `upper_received` to avoid antagonizing the trace which may end up with `upper_limit`
            // cutting through a single (largely empty, at least near `upper_limit`) batch. 
            source_trace.distinguish_since(&upper_received[..]);
            output_trace.distinguish_since(&upper_received[..]);
        });

        Arranged { stream: stream, trace: result_trace }
    }
}

// Several methods are broken out here to help understand performance profiling.

#[inline(never)]
fn sort_dedup_a<T: Ord>(list: &mut Vec<T>) {
    list.dedup();
    list.sort();
    list.dedup();
}

#[inline(never)]
fn sort_dedup_b<T: Ord>(list: &mut Vec<T>) {
    list.dedup();
    list.sort();
    list.dedup();
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

/// Scans `vec[off..]` and consolidates differences of adjacent equivalent elements.
// #[inline(never)]
pub fn consolidate_from<T: Ord+Clone, R: Ring>(vec: &mut Vec<(T, R)>, off: usize) {

    // We should do an insertion-sort like initial scan which builds up sorted, consolidated runs.
    // In a world where there are not many results, we may never even need to call in to merge sort.

    vec[off..].sort_by(|x,y| x.0.cmp(&y.0));
    for index in (off + 1) .. vec.len() {
        if vec[index].0 == vec[index - 1].0 {
            vec[index].1 = vec[index].1 + vec[index - 1].1;
            vec[index - 1].1 = R::zero();
        }
    }

    let mut cursor = off;
    for index in off .. vec.len() {
        if !vec[index].1.is_zero() {
            vec[cursor] = vec[index].clone();
            cursor += 1;
        }
    }
    vec.truncate(cursor);
    
}

trait PerKeyCompute<V1, V2, T, R1, R2> 
where
    V1: Ord+Clone,
    V2: Ord+Clone,
    T: Lattice+Ord+Clone,
    R1: Ring,
    R2: Ring,
{
    fn new() -> Self;
    fn compute<K, C1, C2, C3, L>(
        &mut self,
        key: &K, 
        input: &mut C1, 
        output: &mut C2, 
        batches: &mut [C3],
        times: &mut Vec<T>, 
        logic: &L, 
        upper_limit: &[T],
        outputs: &mut [(T, Vec<(V2, T, R2)>)],
        new_interesting: &mut Vec<T>) -> (usize, usize)
    where 
        K: Eq+Clone+Debug,
        C1: Cursor<K, V1, T, R1>, 
        C3: Cursor<K, V1, T, R1>, 
        C2: Cursor<K, V2, T, R2>, 
        L: Fn(&K, &[(V1, R1)], &mut Vec<(V2, R2)>);
}


/// Implementation based on replaying historical and new updates together.
mod history_replay {

    use std::fmt::Debug;
    use std::cmp::Ordering;

    use ::Ring;
    use lattice::Lattice;
    use trace::Cursor;

    use super::{PerKeyCompute, consolidate, sort_dedup_a, sort_dedup_b, consolidate_from};



    /// The `HistoryReplayer` is a compute strategy based on moving through existing inputs, interesting times, etc in 
    /// time order, maintaining consolidated representations of updates with respect to future interesting times.
    pub struct HistoryReplayer2<V1, V2, T, R1, R2> 
    where
        V1: Ord+Clone,
        V2: Ord+Clone,
        T: Lattice+Ord+Clone,
        R1: Ring,
        R2: Ring,
    {
        input_history: CollectionHistory<V1, T, R1>,
        output_history: CollectionHistory<V2, T, R2>,
        input_buffer: Vec<(V1, R1)>,
        output_buffer: Vec<(V2, R2)>,
        output_produced: Vec<((V2, T), R2)>,
        known_times: Vec<T>,
        synth_times: Vec<T>,
        meets: Vec<T>,
        times_current: Vec<T>,
        lower: Vec<T>,
    }

    impl<V1, V2, T, R1, R2> PerKeyCompute<V1, V2, T, R1, R2> for HistoryReplayer2<V1, V2, T, R1, R2> 
    where
        V1: Ord+Clone+Debug,
        V2: Ord+Clone+Debug,
        T: Lattice+Ord+Clone+Debug,
        R1: Ring+Debug,
        R2: Ring+Debug,
    {
        fn new() -> Self {
            HistoryReplayer2 { 
                input_history: CollectionHistory::new(),
                output_history: CollectionHistory::new(),
                input_buffer: Vec::new(),
                output_buffer: Vec::new(),
                output_produced: Vec::new(),
                known_times: Vec::new(),
                synth_times: Vec::new(),
                meets: Vec::new(),
                times_current: Vec::new(),
                lower: Vec::new(),
            }
        }
        #[inline(never)]
        fn compute<K, C1, C2, C3, L>(
            &mut self,
            key: &K, 
            source_cursor: &mut C1, 
            output_cursor: &mut C2, 
            batch_cursors: &mut [C3],
            times: &mut Vec<T>, 
            logic: &L, 
            upper_limit: &[T],
            outputs: &mut [(T, Vec<(V2, T, R2)>)],
            new_interesting: &mut Vec<T>) -> (usize, usize)
        where 
            K: Eq+Clone+Debug,
            C1: Cursor<K, V1, T, R1>, 
            C3: Cursor<K, V1, T, R1>, 
            C2: Cursor<K, V2, T, R2>, 
            L: Fn(&K, &[(V1, R1)], &mut Vec<(V2, R2)>) 
        {
            // We determine the output changes for times not in advance of `upper_limit` by enumerating and 
            // then exploring all times found in `



            // we use meets[0], and this should be true anyhow (otherwise, don't call).
            assert!(times.len() > 0);

            // determine a lower frontier of interesting times.
            self.lower.clear();
            for time in times.drain(..) {
                if !self.lower.iter().any(|t| t.le(&time)) { 
                    self.lower.retain(|t| !time.lt(t));
                    self.lower.push(time);
                }
            }

            // All times we encounter will be at least `meet`, and so long as we just join with and compare 
            // against interesting times, it is safe to advance all historical times by `meet`. 
            //
            // NOTE: This works fine for distributive lattices, but `advance_by` is more precise. This is a 
            //       bit of an experiment to see what the difference might be. We may want to switch back to
            //       `advance_by` in the future to make it more robust.
            let mut meet = self.lower.iter().fold(T::max(), |meet, time| meet.meet(time));

            // We build "histories" of the input and output, with respect to the times we may encounter in this
            // invocation of `compute`. This means we advance times by joining them with `meet`, which can yield
            // somewhat odd updates; we should not take the advanced times as anything other than a time that is
            // safe to use for joining with and `le` testing against times in the interval.
            //
            // These histories act as caches in front of each cursor. They should provide the same functionality,
            // but are capable of compacting their representation as we proceed through the computation.

            self.input_history.reload(key, source_cursor, &meet);
            self.output_history.reload(key, output_cursor, &meet);

            // TODO: We should be able to thin out any updates at times that, advanced, are greater than some
            //       element of `upper_limit`, as we will never incorporate that update. We should take care 
            //       to notice these times, if we need to report them as interesting times, but once we have 
            //       done that we can ditch the updates.
            // NOTE: Doing this removes a certain amount of precision, in that we can no longer see to which
            //       values the times correspond, and perhaps be less interested if the value is not observed.
            //       Similarly, we lose the ability to see that updates cancel at some point; this may be less
            //       common, if the "future" updates we would join with are largely historical, and so not 
            //       changing in this invocation of `compute`.
            // NOTE: Not done at the moment. The precision is reassuring for correctness.

            // TODO: We should be able to restrict our attention down to just those times that are the joins
            //       of times evident in the input and output. This could be useful in cases like `distinct`,
            //       where the input updates collapse down to relatively fewer distinct moments.
            // NOTE: One simple version of this is to just start with the times of the lower bound, and the 
            //       times present in the input or output, ignoring `times` completely. I think this might be
            //       a good "reference implementation", and it could/should check that each element of `times`
            //       is actually considered. The non-emptiness of `times` would still drive whether we evaluate
            //       a key for any given interval, and it is still important to correctly produce it as output.
            // NOTE: We pay no attention to which times are "new" which are required for interesting times. We
            //       could require that each time be interesting, in that it is the join that includes at least
            //       one new update (from `batch_cursors`, which we do not have access to here) or `times`.

            self.synth_times.clear();
            self.times_current.clear();
            self.output_produced.clear();

            {   // Populate `self.known_times` with times from the input, output, and the `times` argument.
                
                self.known_times.clear();

                // Merge the times in input actions and output actions.
                let mut input_slice = &self.input_history.actions[..];
                let mut output_slice = &self.output_history.actions[..];

                while input_slice.len() > 0 || output_slice.len() > 0 {

                    // Determine the next time.
                    let mut next = T::max();
                    if input_slice.len() > 0 && input_slice[0].0.cmp(&next) == Ordering::Less {
                        next = input_slice[0].0.clone();
                    }
                    if output_slice.len() > 0 && output_slice[0].0.cmp(&next) == Ordering::Less {
                        next = output_slice[0].0.clone();
                    }

                    // Advance each slice until we reach a new time.
                    while input_slice.len() > 0 && input_slice[0].0 == next {
                        input_slice = &input_slice[1..];
                    }
                    while output_slice.len() > 0 && output_slice[0].0 == next {
                        output_slice = &output_slice[1..];
                    }

                    self.known_times.push(next);
                }
            }

            {   // Populate `self.meets` with the meets of suffixes of `self.known_times`.

                // As we move through `self.known_times`, we want to collapse down historical updates, even
                // those that were distinct at the beginning of the interval. To do this, we track the `meet`
                // of each suffix of `self.known_times`, and use this to advance times in update histories.

                self.meets.clear();
                self.meets.reserve(self.known_times.len());
                self.meets.extend(self.known_times.iter().cloned());
                for i in (1 .. self.meets.len()).rev() {
                    self.meets[i-1] = self.meets[i-1].meet(&self.meets[i]);
                }
            }

            // we track our position in each of our lists of actions using slices; 
            // as we pull work from the front, we advance the slice. we could have
            // used drain iterators, but we want to peek at the next element and 
            // this seemed like the simplest way to do that.

            let mut known_slice = &self.known_times[..];
            let mut meets_slice = &self.meets[..];

            let mut compute_counter = 0;
            let mut output_counter = 0;

            // Times to process can either by "known" a priori, or "synth"-etic times produced as 
            // the joins of updates and interesting times. As long as we have either, we should continue
            // to do work.
            while known_slice.len() > 0 || self.synth_times.len() > 0 {

                // Determine the next time to process.
                let mut next_time = T::max();
                if known_slice.len() > 0 && next_time.cmp(&known_slice[0]) == Ordering::Greater {
                    next_time = known_slice[0].clone();
                }
                if self.synth_times.len() > 0 && next_time.cmp(&self.synth_times[0]) == Ordering::Greater {
                    next_time = self.synth_times[0].clone();
                }

                // Advance `known_slice` and `synth_times` past `next_time`.
                while known_slice.len() > 0 && known_slice[0] == next_time {
                    known_slice = &known_slice[1..];
                    meets_slice = &meets_slice[1..];
                }
                while self.synth_times.len() > 0 && self.synth_times[0] == next_time {
                    self.synth_times.remove(0); // <-- TODO: this could be a min-heap.
                }

                // Advance each history that we track.
                // TODO: Add batch history somewhere.
                self.input_history.advance_through(&next_time);
                self.output_history.advance_through(&next_time);

                // We should only process times that are not in advance of `upper_limit`. 
                //
                // We have no particular guarantee that known times will not be in advance of `upper_limit`.
                // We may have the guarantee that synthetic times will not be, as we test against the limit
                // before we add the time to `synth_times`.
                if !upper_limit.iter().any(|t| t.le(&next_time)) {

                    compute_counter += 1;

                    // Assemble the input collection at `next_time`. (`self.input_buffer` cleared just after use).
                    debug_assert!(self.input_buffer.is_empty());
                    self.input_history.insert(&next_time, &meet, &mut self.input_buffer);

                    // Apply user logic if non-empty input and see what happens!
                    self.output_buffer.clear();
                    if self.input_buffer.len() > 0 {
                        logic(key, &self.input_buffer[..], &mut self.output_buffer);
                        self.input_buffer.clear();
                    }
            
                    // Subtract relevant output differences to determine the difference between the intended 
                    // output and the actual current output. Note: we have two places where output differences
                    // live: `output_history` and `output_produced`; the former are those output updates from
                    // the output trace, and the latter are output updates we have produced as this invocation
                    // of `compute` has executed.
                    //
                    // TODO: This could perhaps be done better, as `output_history` is grouped by value and we
                    //       also eventually want to group `output_produced` by value. Perhaps we can manage
                    //       each organized by value and then sort only `self.output_buffer` and merge whatever
                    //       results to see output updates. A log-structured merge list would work, but is more
                    //       engineering than I can pinch off right now.

                    self.output_history.remove(&next_time, &meet, &mut self.output_buffer);
                    for &((ref value, ref time), diff) in self.output_produced.iter() {
                        if time.le(&next_time) {
                            self.output_buffer.push((value.clone(), -diff));
                        }
                    }

                    // Having subtracted output updates from user output, consolidate the results to determine
                    // if there is anything worth reporting. Note: this also orders the results by value, so 
                    // that could make the above merging plan even easier. 
                    consolidate(&mut self.output_buffer);

                    // Stash produced updates into both capability-indexed buffers and `output_produced`. 
                    // The two locations are important, in that we will compact `output_produced` as we move 
                    // through times, but we cannot compact the output buffers because we need their actual
                    // times.
                    if self.output_buffer.len() > 0 {

                        output_counter += 1;

                        // any times not greater than `lower` must be empty (and should be skipped, but testing!)
                        assert!(self.lower.iter().any(|t| t.le(&next_time)));

                        // We *should* be able to find a capability for `next_time`. Any thing else would 
                        // indicate a logical error somewhere along the way; either we release a capability 
                        // we should have kept, or we have computed the output incorrectly (or both!)
                        let idx = outputs.iter().rev().position(|&(ref time, _)| time.le(&next_time));
                        let idx = outputs.len() - idx.unwrap() - 1;
                        for (val, diff) in self.output_buffer.drain(..) {
                            self.output_produced.push(((val.clone(), next_time.clone()), diff));
                            outputs[idx].1.push((val, next_time.clone(), diff));
                        }

                        // Advance times in `self.output_produced` and consolidate the representation.
                        // NOTE: We only do this when we add records; it could be that there are situations 
                        //       where we want to consolidate even without changes (because an initially
                        //       large collection can now be collapsed).
                        for entry in &mut self.output_produced {
                            (entry.0).1 = (entry.0).1.join(&meet);
                        }
                        consolidate(&mut self.output_produced);

                    }

                    // Determine synthetic interesting times.
                    //
                    // This implementation makes the pessimistic assumption that the time we consider could 
                    // be joined with any other time we have seen in this invocation, and that the result 
                    // could possibly be interesting. That isn't wrong, but it is very conservative.
                    // 
                    // Other options include:
                    //     1. Accumulate updates from `batch` separately, and only join input and output times
                    //        against times present in the current accumulation of `batch`. Times that cancel
                    //        don't need to be interesting. 
                    //     2. Notice which times are observed by the user logic, specifically those times that
                    //        were considered but discarded by user logic, indicating that something different
                    //        could happen when the update becomes active.
                    //     3. Only emit the lower bound of joined times, ignoring any joined times greater than
                    //        those warned about, on the premise that when the warned time is considered we can
                    //        warn about further times, if they still seem interesting (e.g., their times have
                    //        not yet cancelled). This may require some assumptions about how times warn about
                    //        future times, to make sure we don't miss things (might fight with other opts).

                    for time in &self.times_current {
                        if !time.le(&next_time) {

                            // The joined time may be available to process in this interval, in which case we 
                            // should enqueue it (we may not be able to process it immediately, and there may
                            // be input or output updates at the same time to integrate).
                            // Otherwise, enqueue the joined time in the `new_interesting` list of future warnings.
                            let join = next_time.join(time);
                            if !upper_limit.iter().any(|t| t.le(&join)) {
                                self.synth_times.push(join); 
                            }
                            else {
                                // NOTE: This implementation only warns about the lower bound of observed warnings.
                                //       I'm a bit worried about this, because I'm not really sure the advantages
                                //       (compactness) outweight the downsides (imprecision).
                                // NOTE: We may need to discard warnings that are not in advance of capabilities.
                                //       I guess this could happen if we previously had evidence that no outputs
                                //       would be produced for a time, and then lost track of that evidence. The
                                //       outer `group` logic will panic if it cannot find a capability for elements
                                //       of `new_interesting`, and we should probably suppress such but notice them
                                //       and confirm that they are not logic bugs (e.g., for each suppressed time
                                //       determine when we released its capability, and understand why that was ok).
                                if outputs.iter().any(|&(ref t,_)| t.le(&join)) {
                                    new_interesting.push(join);
                                }
                                else {
                                    // TODO: Should we tell someone?
                                }
                            }
                        }
                    }
                }
                else {  

                    // If the time is not in advance of `upper_limit`, we should not process it but instead enqueue
                    // it for future re-consideration. As above, we may need to discard updates that are not in 
                    // advance of some capability. For example, just because we saw an advanced time for an input
                    // or output update does not mean that we hold the capability to produce outputs at that time.
                    // If we have previously released the capability, we have committed to *not* producing output at
                    // that time, and we should respect that (also, the outer `group` logic will panic).
                    if outputs.iter().any(|&(ref t,_)| t.le(&next_time)) {
                        new_interesting.push(next_time.clone());
                    }
                    else {
                        // TODO: Should we tell someone?
                        // NOTE: Not incorrect to be here, if it is due to advanced input or output update times.
                    }
                }

                // Record `next_time` as a time we considered, for future times to join against.
                self.times_current.push(next_time);

                // Update `meet`, and advance and deduplicate times is `self.times_current`.
                // NOTE: This does not advance input or output collections, which is done when the are accumulated.
                if meets_slice.len() > 0 || self.synth_times.len() > 0 {

                    // Start from `T::max()` and take the meet with each synthetic time. Also meet with the first 
                    // element of `meets_slice` if it exists.
                    meet = self.synth_times.iter().fold(T::max(), |meet, time| meet.meet(time));
                    if meets_slice.len() > 0 { meet = meet.meet(&meets_slice[0]); }

                    // Advance and deduplicate `self.times_current`.
                    for time in &mut self.times_current {
                        *time = time.join(&meet);
                    }
                    sort_dedup_a(&mut self.times_current);
                }

                // Sort and deduplicate `self.synth_times`. This is our poor replacement for a min-heap, but works.
                sort_dedup_b(&mut self.synth_times);
            }

            // Normalize the representation of `new_interesting`, deduplicating and ordering. 
            // NOTE: This only makes sense for as long as we track the lower frontier. Once we track more precise
            //       times, we will have non-trivial deduplication to perform here. The logic will be similar, but
            //       the current `debug_assert!` will not be correct (it asserts "already deduplicated").
            new_interesting.sort();
            new_interesting.dedup();

            (compute_counter, output_counter)
        }
    }



    /// The `HistoryReplayer` is a compute strategy based on moving through existing inputs, interesting times, etc in 
    /// time order, maintaining consolidated representations of updates with respect to future interesting times.
    pub struct HistoryReplayer<V1, V2, T, R1, R2> 
    where
        V1: Ord+Clone,
        V2: Ord+Clone,
        T: Lattice+Ord+Clone,
        R1: Ring,
        R2: Ring,
    {
        input_history: CollectionHistory<V1, T, R1>,
        output_history: CollectionHistory<V2, T, R2>,
        input_buffer: Vec<(V1, R1)>,
        output_buffer: Vec<(V2, R2)>,
        output_produced: Vec<((V2, T), R2)>,
        known_times: Vec<T>,
        synth_times: Vec<T>,
        meets: Vec<T>,
        times_current: Vec<T>,
        lower: Vec<T>,
    }

    impl<V1, V2, T, R1, R2> PerKeyCompute<V1, V2, T, R1, R2> for HistoryReplayer<V1, V2, T, R1, R2> 
    where
        V1: Ord+Clone+Debug,
        V2: Ord+Clone+Debug,
        T: Lattice+Ord+Clone+Debug,
        R1: Ring+Debug,
        R2: Ring+Debug,
    {
        fn new() -> Self {
            HistoryReplayer { 
                input_history: CollectionHistory::new(),
                output_history: CollectionHistory::new(),
                input_buffer: Vec::new(),
                output_buffer: Vec::new(),
                output_produced: Vec::new(),
                known_times: Vec::new(),
                synth_times: Vec::new(),
                meets: Vec::new(),
                times_current: Vec::new(),
                lower: Vec::new(),
            }
        }
        #[inline(never)]
        fn compute<K, C1, C2, C3, L>(
            &mut self,
            key: &K, 
            source_cursor: &mut C1, 
            output_cursor: &mut C2, 
            batch_cursors: &mut [C3], 
            times: &mut Vec<T>, 
            logic: &L, 
            upper_limit: &[T],
            outputs: &mut [(T, Vec<(V2, T, R2)>)],
            new_interesting: &mut Vec<T>) -> (usize, usize)
        where 
            K: Eq+Clone+Debug,
            C1: Cursor<K, V1, T, R1>, 
            C3: Cursor<K, V1, T, R1>, 
            C2: Cursor<K, V2, T, R2>, 
            L: Fn(&K, &[(V1, R1)], &mut Vec<(V2, R2)>) 
        {

            // we use meets[0], and this should be true anyhow (otherwise, don't call).
            assert!(times.len() > 0);

            // determine a lower frontier of interesting times.
            self.lower.clear();
            for time in times.drain(..) {
                if !self.lower.iter().any(|t| t.le(&time)) { 
                    self.lower.retain(|t| !time.lt(t));
                    self.lower.push(time);
                }
            }

            // All times we encounter will be at least `meet`, and so long as we just join with and compare 
            // against interesting times, it is safe to advance all historical times by `meet`. 
            //
            // NOTE: This works fine for distributive lattices, but `advance_by` is more precise. This is a 
            //       bit of an experiment to see what the difference might be. We may want to switch back to
            //       `advance_by` in the future to make it more robust.
            let mut meet = self.lower.iter().fold(T::max(), |meet, time| meet.meet(time));

            // We build "histories" of the input and output, with respect to the times we may encounter in this
            // invocation of `compute`. This means we advance times by joining them with `meet`, which can yield
            // somewhat odd updates; we should not take the advanced times as anything other than a time that is
            // safe to use for joining with and `le` testing against times in the interval.
            //
            // These histories act as caches in front of each cursor. They should provide the same functionality,
            // but are capable of compacting their representation as we proceed through the computation.

            self.input_history.reload(key, source_cursor, &meet);
            self.output_history.reload(key, output_cursor, &meet);

            // TODO: We should be able to thin out any updates at times that, advanced, are greater than some
            //       element of `upper_limit`, as we will never incorporate that update. We should take care 
            //       to notice these times, if we need to report them as interesting times, but once we have 
            //       done that we can ditch the updates.
            // NOTE: Doing this removes a certain amount of precision, in that we can no longer see to which
            //       values the times correspond, and perhaps be less interested if the value is not observed.
            //       Similarly, we lose the ability to see that updates cancel at some point; this may be less
            //       common, if the "future" updates we would join with are largely historical, and so not 
            //       changing in this invocation of `compute`.
            // NOTE: Not done at the moment. The precision is reassuring for correctness.

            // TODO: We should be able to restrict our attention down to just those times that are the joins
            //       of times evident in the input and output. This could be useful in cases like `distinct`,
            //       where the input updates collapse down to relatively fewer distinct moments.
            // NOTE: One simple version of this is to just start with the times of the lower bound, and the 
            //       times present in the input or output, ignoring `times` completely. I think this might be
            //       a good "reference implementation", and it could/should check that each element of `times`
            //       is actually considered. The non-emptiness of `times` would still drive whether we evaluate
            //       a key for any given interval, and it is still important to correctly produce it as output.
            // NOTE: We pay no attention to which times are "new" which are required for interesting times. We
            //       could require that each time be interesting, in that it is the join that includes at least
            //       one new update (from `batch_cursors`, which we do not have access to here) or `times`.

            self.synth_times.clear();
            self.times_current.clear();
            self.output_produced.clear();

            {   // Populate `self.known_times` with times from the input, output, and the `times` argument.
                
                self.known_times.clear();

                // Merge the times in input actions and output actions.
                let mut input_slice = &self.input_history.actions[..];
                let mut output_slice = &self.output_history.actions[..];

                while input_slice.len() > 0 || output_slice.len() > 0 {

                    // Determine the next time.
                    let mut next = T::max();
                    if input_slice.len() > 0 && input_slice[0].0.cmp(&next) == Ordering::Less {
                        next = input_slice[0].0.clone();
                    }
                    if output_slice.len() > 0 && output_slice[0].0.cmp(&next) == Ordering::Less {
                        next = output_slice[0].0.clone();
                    }

                    // Advance each slice until we reach a new time.
                    while input_slice.len() > 0 && input_slice[0].0 == next {
                        input_slice = &input_slice[1..];
                    }
                    while output_slice.len() > 0 && output_slice[0].0 == next {
                        output_slice = &output_slice[1..];
                    }

                    self.known_times.push(next);
                }
            }

            {   // Populate `self.meets` with the meets of suffixes of `self.known_times`.

                // As we move through `self.known_times`, we want to collapse down historical updates, even
                // those that were distinct at the beginning of the interval. To do this, we track the `meet`
                // of each suffix of `self.known_times`, and use this to advance times in update histories.

                self.meets.clear();
                self.meets.reserve(self.known_times.len());
                self.meets.extend(self.known_times.iter().cloned());
                for i in (1 .. self.meets.len()).rev() {
                    self.meets[i-1] = self.meets[i-1].meet(&self.meets[i]);
                }
            }

            // we track our position in each of our lists of actions using slices; 
            // as we pull work from the front, we advance the slice. we could have
            // used drain iterators, but we want to peek at the next element and 
            // this seemed like the simplest way to do that.

            let mut known_slice = &self.known_times[..];
            let mut meets_slice = &self.meets[..];

            let mut compute_counter = 0;
            let mut output_counter = 0;

            // Times to process can either by "known" a priori, or "synth"-etic times produced as 
            // the joins of updates and interesting times. As long as we have either, we should continue
            // to do work.
            while known_slice.len() > 0 || self.synth_times.len() > 0 {

                // Determine the next time to process.
                let mut next_time = T::max();
                if known_slice.len() > 0 && next_time.cmp(&known_slice[0]) == Ordering::Greater {
                    next_time = known_slice[0].clone();
                }
                if self.synth_times.len() > 0 && next_time.cmp(&self.synth_times[0]) == Ordering::Greater {
                    next_time = self.synth_times[0].clone();
                }

                // Advance `known_slice` and `synth_times` past `next_time`.
                while known_slice.len() > 0 && known_slice[0] == next_time {
                    known_slice = &known_slice[1..];
                    meets_slice = &meets_slice[1..];
                }
                while self.synth_times.len() > 0 && self.synth_times[0] == next_time {
                    self.synth_times.remove(0); // <-- TODO: this could be a min-heap.
                }

                // Advance each history that we track.
                // TODO: Add batch history somewhere.
                self.input_history.advance_through(&next_time);
                self.output_history.advance_through(&next_time);

                // We should only process times that are not in advance of `upper_limit`. 
                //
                // We have no particular guarantee that known times will not be in advance of `upper_limit`.
                // We may have the guarantee that synthetic times will not be, as we test against the limit
                // before we add the time to `synth_times`.
                if !upper_limit.iter().any(|t| t.le(&next_time)) {

                    compute_counter += 1;

                    // Assemble the input collection at `next_time`. (`self.input_buffer` cleared just after use).
                    debug_assert!(self.input_buffer.is_empty());
                    self.input_history.insert(&next_time, &meet, &mut self.input_buffer);

                    // Apply user logic if non-empty input and see what happens!
                    self.output_buffer.clear();
                    if self.input_buffer.len() > 0 {
                        logic(key, &self.input_buffer[..], &mut self.output_buffer);
                        self.input_buffer.clear();
                    }
            
                    // Subtract relevant output differences to determine the difference between the intended 
                    // output and the actual current output. Note: we have two places where output differences
                    // live: `output_history` and `output_produced`; the former are those output updates from
                    // the output trace, and the latter are output updates we have produced as this invocation
                    // of `compute` has executed.
                    //
                    // TODO: This could perhaps be done better, as `output_history` is grouped by value and we
                    //       also eventually want to group `output_produced` by value. Perhaps we can manage
                    //       each organized by value and then sort only `self.output_buffer` and merge whatever
                    //       results to see output updates. A log-structured merge list would work, but is more
                    //       engineering than I can pinch off right now.

                    self.output_history.remove(&next_time, &meet, &mut self.output_buffer);
                    for &((ref value, ref time), diff) in self.output_produced.iter() {
                        if time.le(&next_time) {
                            self.output_buffer.push((value.clone(), -diff));
                        }
                    }

                    // Having subtracted output updates from user output, consolidate the results to determine
                    // if there is anything worth reporting. Note: this also orders the results by value, so 
                    // that could make the above merging plan even easier. 
                    consolidate(&mut self.output_buffer);

                    // Stash produced updates into both capability-indexed buffers and `output_produced`. 
                    // The two locations are important, in that we will compact `output_produced` as we move 
                    // through times, but we cannot compact the output buffers because we need their actual
                    // times.
                    if self.output_buffer.len() > 0 {

                        output_counter += 1;

                        // any times not greater than `lower` must be empty (and should be skipped, but testing!)
                        assert!(self.lower.iter().any(|t| t.le(&next_time)));

                        // We *should* be able to find a capability for `next_time`. Any thing else would 
                        // indicate a logical error somewhere along the way; either we release a capability 
                        // we should have kept, or we have computed the output incorrectly (or both!)
                        let idx = outputs.iter().rev().position(|&(ref time, _)| time.le(&next_time));
                        let idx = outputs.len() - idx.unwrap() - 1;
                        for (val, diff) in self.output_buffer.drain(..) {
                            self.output_produced.push(((val.clone(), next_time.clone()), diff));
                            outputs[idx].1.push((val, next_time.clone(), diff));
                        }

                        // Advance times in `self.output_produced` and consolidate the representation.
                        // NOTE: We only do this when we add records; it could be that there are situations 
                        //       where we want to consolidate even without changes (because an initially
                        //       large collection can now be collapsed).
                        for entry in &mut self.output_produced {
                            (entry.0).1 = (entry.0).1.join(&meet);
                        }
                        consolidate(&mut self.output_produced);

                    }

                    // Determine synthetic interesting times.
                    //
                    // This implementation makes the pessimistic assumption that the time we consider could 
                    // be joined with any other time we have seen in this invocation, and that the result 
                    // could possibly be interesting. That isn't wrong, but it is very conservative.
                    // 
                    // Other options include:
                    //     1. Accumulate updates from `batch` separately, and only join input and output times
                    //        against times present in the current accumulation of `batch`. Times that cancel
                    //        don't need to be interesting. 
                    //     2. Notice which times are observed by the user logic, specifically those times that
                    //        were considered but discarded by user logic, indicating that something different
                    //        could happen when the update becomes active.
                    //     3. Only emit the lower bound of joined times, ignoring any joined times greater than
                    //        those warned about, on the premise that when the warned time is considered we can
                    //        warn about further times, if they still seem interesting (e.g., their times have
                    //        not yet cancelled). This may require some assumptions about how times warn about
                    //        future times, to make sure we don't miss things (might fight with other opts).

                    for time in &self.times_current {
                        if !time.le(&next_time) {

                            // The joined time may be available to process in this interval, in which case we 
                            // should enqueue it (we may not be able to process it immediately, and there may
                            // be input or output updates at the same time to integrate).
                            // Otherwise, enqueue the joined time in the `new_interesting` list of future warnings.
                            let join = next_time.join(time);
                            if !upper_limit.iter().any(|t| t.le(&join)) {
                                self.synth_times.push(join); 
                            }
                            else {
                                // NOTE: This implementation only warns about the lower bound of observed warnings.
                                //       I'm a bit worried about this, because I'm not really sure the advantages
                                //       (compactness) outweight the downsides (imprecision).
                                // NOTE: We may need to discard warnings that are not in advance of capabilities.
                                //       I guess this could happen if we previously had evidence that no outputs
                                //       would be produced for a time, and then lost track of that evidence. The
                                //       outer `group` logic will panic if it cannot find a capability for elements
                                //       of `new_interesting`, and we should probably suppress such but notice them
                                //       and confirm that they are not logic bugs (e.g., for each suppressed time
                                //       determine when we released its capability, and understand why that was ok).
                                if outputs.iter().any(|&(ref t,_)| t.le(&join)) {
                                    new_interesting.push(join);
                                }
                                else {
                                    // TODO: Should we tell someone?
                                }
                            }
                        }
                    }
                }
                else {  

                    // If the time is not in advance of `upper_limit`, we should not process it but instead enqueue
                    // it for future re-consideration. As above, we may need to discard updates that are not in 
                    // advance of some capability. For example, just because we saw an advanced time for an input
                    // or output update does not mean that we hold the capability to produce outputs at that time.
                    // If we have previously released the capability, we have committed to *not* producing output at
                    // that time, and we should respect that (also, the outer `group` logic will panic).
                    if outputs.iter().any(|&(ref t,_)| t.le(&next_time)) {
                        new_interesting.push(next_time.clone());
                    }
                    else {
                        // TODO: Should we tell someone?
                        // NOTE: Not incorrect to be here, if it is due to advanced input or output update times.
                    }
                }

                // Record `next_time` as a time we considered, for future times to join against.
                self.times_current.push(next_time);

                // Update `meet`, and advance and deduplicate times is `self.times_current`.
                // NOTE: This does not advance input or output collections, which is done when the are accumulated.
                if meets_slice.len() > 0 || self.synth_times.len() > 0 {

                    // Start from `T::max()` and take the meet with each synthetic time. Also meet with the first 
                    // element of `meets_slice` if it exists.
                    meet = self.synth_times.iter().fold(T::max(), |meet, time| meet.meet(time));
                    if meets_slice.len() > 0 { meet = meet.meet(&meets_slice[0]); }

                    // Advance and deduplicate `self.times_current`.
                    for time in &mut self.times_current {
                        *time = time.join(&meet);
                    }
                    sort_dedup_a(&mut self.times_current);
                }

                // Sort and deduplicate `self.synth_times`. This is our poor replacement for a min-heap, but works.
                sort_dedup_b(&mut self.synth_times);
            }

            // Normalize the representation of `new_interesting`, deduplicating and ordering. 
            // NOTE: This only makes sense for as long as we track the lower frontier. Once we track more precise
            //       times, we will have non-trivial deduplication to perform here. The logic will be similar, but
            //       the current `debug_assert!` will not be correct (it asserts "already deduplicated").
            new_interesting.sort();
            new_interesting.dedup();

            (compute_counter, output_counter)
        }
    }

    // tracks 
    struct ValueHistory<V> {
        value: V,
        lower: usize,
        clean: usize,
        valid: usize,
        upper: usize,
    }

    struct CollectionHistory<V: Clone, T: Lattice+Ord+Clone, R: Ring> {
        pub values: Vec<ValueHistory<V>>,
        pub actions: Vec<(T, usize)>,
        action_cursor: usize,
        pub times: Vec<(T, R)>,
    }

    impl<V: Clone, T: Lattice+Ord+Clone+Debug, R: Ring> CollectionHistory<V, T, R> {
        fn new() -> Self {
            CollectionHistory {
                values: Vec::new(),
                actions: Vec::new(),
                action_cursor: 0,
                times: Vec::new(),
            }
        }

        fn reload<K, C>(&mut self, key: &K, cursor: &mut C, meet: &T) 
        where K: Eq+Clone+Debug, C: Cursor<K, V, T, R> { 

            self.values.clear();
            self.actions.clear();
            self.action_cursor = 0;
            self.times.clear();

            cursor.seek_key(&key);
            if cursor.key_valid() && cursor.key() == key {
                while cursor.val_valid() {
                    // let val: V1 = source_cursor.val().clone();
                    let start = self.times.len();
                    cursor.map_times(|t, d| {
                        // println!("  INPUT: ({:?}, {:?}, {:?}, {:?})", key, val, t, d);
                        self.times.push((t.join(&meet), d));
                    });
                    self.seal_from(cursor.val().clone(), start);
                    cursor.step_val();
                }
            }

            self.build_actions();
        }

        /// Advances the indexed value by one, with the ability to compact times by `meet`.
        fn advance_through(&mut self, time: &T) {
            while self.action_cursor < self.actions.len() && self.actions[self.action_cursor].0.cmp(time) != Ordering::Greater {
                self.values[self.actions[self.action_cursor].1].valid += 1;
                self.action_cursor += 1;
            }
        }

        #[inline(never)]
        fn collapse(&mut self, value_index: usize, meet: &T) {

            let lower = self.values[value_index].lower;
            let valid = self.values[value_index].valid;

            for index in lower .. valid {
                self.times[index].0 = self.times[index].0.join(meet);
            }

            // consolidating updates with equal times (post-join).
            self.times[lower .. valid].sort_by(|x,y| x.0.cmp(&y.0));

            // only need to collapse if there are at least two updates.
            if lower < valid - 1 {

                // now to collapse updates *forward*, to end at `valid`.
                let mut cursor = valid - 1;
                for index in (lower .. valid - 1).rev() {
                    if self.times[index].0 == self.times[cursor].0 {
                        self.times[cursor].1 = self.times[cursor].1 + self.times[index].1;
                        self.times[index].1 = R::zero();
                    }
                    else {
                        if !self.times[cursor].1.is_zero() {
                            cursor -= 1;
                        }
                        self.times.swap(cursor, index);
                    }
                }
                // if the final element accumulated to zero, advance `cursor`.
                if self.times[cursor].1.is_zero() {
                    cursor += 1;
                }
                
                // // should be a range of zeros, .. 
                // debug_assert!(lower <= cursor);
                // for index in lower .. cursor {
                //     debug_assert!(self.times[index].1.is_zero());
                // }
                // // .. followed by a range of non-zeros.
                // debug_assert!(cursor <= valid);
                // for index in cursor .. valid {
                //     debug_assert!(!self.times[index].1.is_zero());
                // }

                self.values[value_index].lower = cursor;
                self.values[value_index].clean = valid;
            }
        }

        fn seal_from(&mut self, value: V, start: usize) {

            // collapse down the updates
            consolidate_from(&mut self.times, start);

            // if non-empty, push range info for value.
            if self.times.len() > start {
                self.values.push(ValueHistory {
                    value: value,
                    lower: start,
                    clean: start,
                    valid: start,
                    upper: self.times.len(),
                });
            }
        }

        // Builds time-sorted list `self.actions` of what happens for each time.
        fn build_actions(&mut self) {

            self.actions.clear();
            self.actions.reserve(self.times.len());
            for (index, history) in self.values.iter().enumerate() {
                for offset in history.lower .. history.upper {
                    self.actions.push((self.times[offset].0.clone(), index));
                }
            }

            self.actions.sort_by(|x,y| x.0.cmp(&y.0));
        }

        #[inline(never)]
        fn insert(&mut self, time: &T, meet: &T, destination: &mut Vec<(V, R)>) {

            for value_index in 0 .. self.values.len() {

                let lower = self.values[value_index].lower;
                let clean = self.values[value_index].clean;
                let valid = self.values[value_index].valid;

                // take the time to collapse if there are changes. 
                // this is not the only reason to collapse, and we may want to be more or less aggressive
                if clean < valid {
                    self.collapse(value_index, meet);
                }

                let mut sum = R::zero();
                for index in lower .. valid {
                    if self.times[index].0.le(time) {
                        sum = sum + self.times[index].1;
                    }
                }
                if !sum.is_zero() {
                    destination.push((self.values[value_index].value.clone(), sum));
                    // return;  // <-- cool optimization for top_k
                }
            }
        }

        #[inline(never)]
        fn remove(&mut self, time: &T, meet: &T, destination: &mut Vec<(V, R)>) {

            for value_index in 0 .. self.values.len() {

                let lower = self.values[value_index].lower;
                let clean = self.values[value_index].clean;
                let valid = self.values[value_index].valid;

                // take the time to collapse if there are changes. 
                // this is not the only reason to collapse, and we may want to be more or less aggressive
                if clean < valid {
                    self.collapse(value_index, meet);
                }

                let mut sum = R::zero();
                for index in lower .. valid {
                    if self.times[index].0.le(time) {
                        sum = sum - self.times[index].1;
                    }
                }
                if !sum.is_zero() {
                    destination.push((self.values[value_index].value.clone(), sum));
                }
            }
        }
    }
}

/// Implementation based on breaking times into chains and being clever.
mod interest_accumulator {

    use std::fmt::Debug;

    use ::Ring;
    use lattice::Lattice;
    use trace::Cursor;

    use super::PerKeyCompute;
    use super::consolidate;

    pub struct InterestAccumulator<V1, V2, T, R1, R2>
    where 
        V1: Ord+Clone,
        V2: Ord+Clone,
        T: Lattice+Ord+Clone,
        R1: Ring,
        R2: Ring,
    {
        interestinator: Interestinator<T>,
        input_accumulator: Accumulator<V1, T, R1>,
        output_accumulator: Accumulator<V2, T, R2>,
        yielded_accumulator: Accumulator<V2, T, R2>,
        time_buffer1: Vec<(T, R1)>,
        time_buffer2: Vec<(T, R2)>,
        output_logic: Vec<(V2, R2)>,
    }

    impl<V1, V2, T, R1, R2> PerKeyCompute<V1, V2, T, R1, R2> for InterestAccumulator<V1, V2, T, R1, R2> 
    where 
        V1: Ord+Clone+Debug,
        V2: Ord+Clone+Debug,
        T: Lattice+Ord+Clone+Debug,
        R1: Ring,
        R2: Ring,
    {
        fn new() -> Self {
            InterestAccumulator {
                interestinator: Interestinator::new(),
                input_accumulator: Accumulator::new(),
                output_accumulator: Accumulator::new(),
                yielded_accumulator: Accumulator::new(),
                time_buffer1: Vec::new(),
                time_buffer2: Vec::new(),
                output_logic: Vec::new(),
            }
        }
        fn compute<K, C1, C2, C3, L>(
            &mut self,
            key: &K, 
            source_cursor: &mut C1, 
            output_cursor: &mut C2, 
            batch_cursors: &mut [C3], 
            interesting_times: &mut Vec<T>, 
            logic: &L,
            upper_limit: &[T],
            outputs: &mut [(T, Vec<(V2, T, R2)>)],
            new_interesting: &mut Vec<T>) -> (usize, usize)
        where 
            K: Eq+Clone+Debug,
            C1: Cursor<K, V1, T, R1>, 
            C3: Cursor<K, V1, T, R1>, 
            C2: Cursor<K, V2, T, R2>, 
            L: Fn(&K, &[(V1, R1)], &mut Vec<(V2, R2)>)
    {
            // Determine the `meet` of times, useful in restricting updates to capture.
            let mut meet = interesting_times[0].clone(); 
            for index in 1 .. interesting_times.len() {
                meet = meet.meet(&interesting_times[index]);
            }

            // clear accumulators (will now repopulate)
            self.yielded_accumulator.clear();
            self.yielded_accumulator.time = interesting_times[0].clone();

            // Accumulate into `input_stage` and populate `input_edits`.
            self.input_accumulator.clear();
            self.input_accumulator.time = interesting_times[0].clone();
            source_cursor.seek_key(&key);
            if source_cursor.key_valid() && source_cursor.key() == key {
                while source_cursor.val_valid() {
                    let val: V1 = source_cursor.val().clone();
                    let mut sum = R1::zero();

                    source_cursor.map_times(|t,d| {

                        // println!("INPUT UPDATE: {:?}, {:?}, {:?}, {:?}", key, val, t, d);

                        if t.le(&self.input_accumulator.time) { 
                            sum = sum + d; 
                        }
                        if !t.le(&meet) {
                            self.time_buffer1.push((t.join(&meet), d));
                        }
                    });
                    consolidate(&mut self.time_buffer1);
                    self.input_accumulator.edits.extend(self.time_buffer1.drain(..).map(|(t,d)| (val.clone(), t, d)));
                    if !sum.is_zero() {
                        self.input_accumulator.accum.push((val, sum));
                    }
                    source_cursor.step_val();
                }
            }
            self.input_accumulator.shackle();

            // Accumulate into `output_stage` and populate `output_edits`. 
            self.output_accumulator.clear();
            self.output_accumulator.time = interesting_times[0].clone();
            output_cursor.seek_key(&key);
            if output_cursor.key_valid() && output_cursor.key() == key {
                while output_cursor.val_valid() {
                    let val: V2 = output_cursor.val().clone();
                    let mut sum = R2::zero();
                    output_cursor.map_times(|t,d| {
                        if t.le(&self.output_accumulator.time) {
                            sum = sum + d;
                        }
                        if !t.le(&meet) {
                            self.time_buffer2.push((t.join(&meet), d));
                        }
                    });
                    consolidate(&mut self.time_buffer2);
                    self.output_accumulator.edits.extend(self.time_buffer2.drain(..).map(|(t,d)| (val.clone(), t, d)));
                    if !sum.is_zero() {
                        self.output_accumulator.accum.push((val, sum));
                    }
                    output_cursor.step_val();
                }
            }
            self.output_accumulator.shackle();

            self.interestinator.close_interesting_times(&self.input_accumulator.edits[..], interesting_times);

            // each interesting time must be considered!
            for this_time in interesting_times.drain(..) {

                // not all times are ready to be finalized. stash them with the key.
                if upper_limit.iter().any(|t| t.le(&this_time)) {
                    new_interesting.push(this_time);
                }
                else {

                    // 1. update `input_stage` and `output_stage` to `this_time`.
                    self.input_accumulator.update_to(this_time.clone());
                    self.output_accumulator.update_to(this_time.clone());
                    self.yielded_accumulator.update_to(this_time.clone());

                    // 2. apply user logic (only if non-empty input).
                    self.output_logic.clear();
                    if self.input_accumulator.accum.len() > 0 {
                        logic(&key, &self.input_accumulator.accum[..], &mut self.output_logic);
                    }

                    // println!("key: {:?}, input: {:?}, ouput: {:?} @ {:?}", 
                    //          key, &self.input_accumulator.accum[..], self.output_logic, this_time);

                    // 3. subtract existing output differences.
                    for &(ref val, diff) in &self.output_accumulator.accum[..] {
                        self.output_logic.push((val.clone(), -diff));
                    }
                    // incorporate uncommitted output updates.
                    for &(ref val, diff) in &self.yielded_accumulator.accum {
                        self.output_logic.push((val.clone(), -diff));
                    }
                    consolidate(&mut self.output_logic);

                    // 4. stashed produced updates in capability-indexed buffers, and `yielded_accumulator`.
                    let idx = outputs.iter().rev().position(|&(ref time, _)| time.le(&this_time));
                    let idx = idx.unwrap();
                    let idx = outputs.len() - idx - 1;
                    for (val, diff) in self.output_logic.drain(..) {
                        self.yielded_accumulator.push_edit((val.clone(), this_time.clone(), diff));
                        outputs[idx].1.push((val, this_time.clone(), diff));
                    }
                }
            }

            (0, 0)
        }
    }

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
        fn new() -> Self {
            Interestinator {
                total: Vec::new(),
                old_accum: Vec::new(),
                new_accum: Vec::new(),
                entrance: Vec::new(),
                frontier: Vec::new(),
                old_temp: Vec::new(),
                new_temp: Vec::new(),
            }
        }

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
                        for time in &mut self.old_accum { *time = time.advance_by(&self.frontier[..]); }
                        self.old_accum.sort();
                        self.old_accum.dedup();
                        // advance times in the new accumulation, sort and deduplicate.
                        for time in &mut self.new_accum { *time = time.advance_by(&self.frontier[..]); }
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
}