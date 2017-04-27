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
//!     output.push(vals.iter().max_by_key(|&(_val, wgt)| wgt).unwrap());
//! })
//! ```

use std::fmt::Debug;
use std::default::Default;

use hashable::{Hashable, UnsignedWrapper};
use ::{Data, Collection, Diff};

use timely::order::PartialOrder;
use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely_sort::Unsigned;

use operators::arrange::{Arrange, Arranged, ArrangeByKey, ArrangeBySelf, BatchWrapper, TraceHandle};
use lattice::Lattice;
use trace::{Batch, Cursor, Trace, Builder};
use trace::cursor::cursor_list::CursorList;
// use trace::implementations::hash::HashValSpine as DefaultValTrace;
// use trace::implementations::hash::HashKeySpine as DefaultKeyTrace;
use trace::implementations::ord::OrdValSpine as DefaultValTrace;
use trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;


/// Extension trait for the `group` differential dataflow method.
pub trait Group<G: Scope, K: Data, V: Data, R: Diff> where G::Timestamp: Lattice+Ord {
    /// Groups records by their first field, and applies reduction logic to the associated values.
    fn group<L, V2: Data, R2: Diff>(&self, logic: L) -> Collection<G, (K, V2), R2>
        where L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static;
    /// Groups records by their first field, and applies reduction logic to the associated values.
    fn group_u<L, V2: Data, R2: Diff>(&self, logic: L) -> Collection<G, (K, V2), R2>
        where L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static, K: Unsigned+Copy;
}

impl<G: Scope, K: Data+Default+Hashable, V: Data, R: Diff> Group<G, K, V, R> for Collection<G, (K, V), R> 
    where G::Timestamp: Lattice+Ord+Debug, <K as Hashable>::Output: Data+Default {
    fn group<L, V2: Data, R2: Diff>(&self, logic: L) -> Collection<G, (K, V2), R2>
        where L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static {
        // self.arrange_by_key_hashed_cached()
        self.arrange_by_key_hashed()
            .group_arranged(move |k,s,t| logic(&k.item,s,t), DefaultValTrace::new())
            .as_collection(|k,v| (k.item.clone(), v.clone()))
    }
    fn group_u<L, V2: Data, R2: Diff>(&self, logic: L) -> Collection<G, (K, V2), R2>
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
pub trait Count<G: Scope, K: Data, R: Diff> where G::Timestamp: Lattice+Ord {
    /// Counts the number of occurrences of each element.
    fn count(&self) -> Collection<G, (K, R), isize>;
    /// Counts the number of occurrences of each element.
    fn count_u(&self) -> Collection<G, (K, R), isize> where K: Unsigned+Copy;
}

impl<G: Scope, K: Data+Default+Hashable, R: Diff> Count<G, K, R> for Collection<G, K, R>
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
pub trait GroupArranged<G: Scope, K: Data, V: Data, R: Diff> where G::Timestamp: Lattice+Ord {
    /// Applies `group` to arranged data, and returns an arrangement of output data.
    fn group_arranged<L, V2, T2, R2>(&self, logic: L, empty: T2) -> Arranged<G, K, V2, R2, T2>
        where
            V2: Data,
            R2: Diff,
            T2: Trace<K, V2, G::Timestamp, R2>+'static,
            L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static
            ; 
}

impl<G: Scope, K: Data, V: Data, T1, R: Diff> GroupArranged<G, K, V, R> for Arranged<G, K, V, R, T1>
where 
    G::Timestamp: Lattice+Ord,
    T1: Trace<K, V, G::Timestamp, R>+'static {
        
    fn group_arranged<L, V2, T2, R2>(&self, logic: L, empty: T2) -> Arranged<G, K, V2, R2, T2>
        where 
            V2: Data,
            R2: Diff,
            T2: Trace<K, V2, G::Timestamp, R2>+'static,
            L: Fn(&K, &[(V, R)], &mut Vec<(V2, R2)>)+'static {

        let mut source_trace = self.new_handle();
        let mut output_trace = TraceHandle::new(empty, &[G::Timestamp::min()], &[G::Timestamp::min()]);
        let result_trace = output_trace.clone();

        // let mut thinker1 = history_replay_prior::HistoryReplayer::<V, V2, G::Timestamp, R, R2>::new();
        let mut thinker = history_replay::HistoryReplayer::<V, V2, G::Timestamp, R, R2>::new();
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
                capabilities.retain(|cap| !capability.time().less_than(&cap.time()));
                if !capabilities.iter().any(|cap| cap.time().less_equal(&capability.time())) {
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
                    if !upper_limit.iter().any(|t| t.less_equal(&join)) {
                        upper_limit.retain(|t| !join.less_equal(t));
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
            if capabilities.iter().any(|c| !upper_limit.iter().any(|t| t.less_equal(&c.time()))) {

                // `interesting` contains "warnings" about keys and times that may need to be re-considered.
                // We first extract those times from this list that lie in the interval we will process.
                sort_dedup(&mut interesting);
                let mut new_interesting = Vec::new();
                let mut exposed = Vec::new();
                segment(&mut interesting, &mut exposed, &mut new_interesting, |&(_, ref time)| {
                    !upper_limit.iter().any(|t| t.less_equal(&time))
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


                // We no longer need to distinguish between the batches we have received and historical batches,
                // so we should allow the trace to start merging them.
                //
                // Note: We know that there will be no further times through `upper_limit`, but we still use the
                // earlier `upper_received` to avoid antagonizing the trace which may end up with `upper_limit`
                // cutting through a single (largely empty, at least near `upper_limit`) batch. 
                source_trace.distinguish_since(&upper_received[..]);
                output_trace.distinguish_since(&upper_received[..]);

                // cursors for navigating input and output traces.
                let mut source_cursor: T1::Cursor = source_trace.cursor_through(&upper_received[..]).unwrap();
                let mut output_cursor: T2::Cursor = output_trace.cursor(); // TODO: this panicked when as above; WHY???
                let mut batch_cursor = CursorList::new(batch_cursors);

                // // The only purpose of `lower_received` was to allow slicing off old input.
                // lower_received = upper_received.clone();

                // let timer = ::std::time::Instant::now();
                // let mut compute_counter = 0;
                // let mut output_counter = 0;
                // let mut key_count = 0;

                // We now march through the keys we must work on, drawing from `batch_cursors` and `exposed`.
                //
                // We only keep valid cursors (those with more data) in `batch_cursors`, and so its length
                // indicates whether more data remain. We move throuh `exposed` using `exposed_position`.
                // There could perhaps be a less provocative variable name.
                let mut exposed_position = 0;
                while batch_cursor.key_valid() || exposed_position < exposed.len() {

                    // Determine the next key we will work on; could be synthetic, could be from a batch.
                    let key1 = if exposed_position < exposed.len() { Some(exposed[exposed_position].0.clone()) } else { None };
                    let key2 = if batch_cursor.key_valid() { Some(batch_cursor.key().clone()) } else { None };
                    let key = match (key1, key2) {
                        (Some(key1), Some(key2)) => ::std::cmp::min(key1, key2),
                        (Some(key1), None)       => key1,
                        (None, Some(key2))       => key2,
                        (None, None)             => unreachable!(),
                    };

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

                    // tidy up times, removing redundancy.
                    interesting_times.sort();
                    interesting_times.dedup();

                    // let mut interesting_times2 = interesting_times.clone();
                    // let mut buffers2 = buffers.clone();
                    // let mut temporary2 = temporary.clone();

                    // do the per-key computation.
                    let _counters = thinker.compute(
                        &key, 
                        &mut source_cursor, 
                        &mut output_cursor, 
                        &mut batch_cursor,
                        &mut interesting_times, 
                        &logic, 
                        &upper_limit[..], 
                        &mut buffers[..], 
                        &mut temporary,
                    );

                    // source_cursor.rewind_vals();
                    // output_cursor.rewind_vals();
                    // batch_cursor.rewind_vals();

                    // // do the per-key computation.
                    // let counters = thinker1.compute(
                    //     &key, 
                    //     &mut source_cursor, 
                    //     &mut output_cursor, 
                    //     &mut batch_cursor,
                    //     &mut interesting_times2, 
                    //     &logic, 
                    //     &upper_limit[..], 
                    //     &mut buffers2[..], 
                    //     &mut temporary2,
                    // );

                    if batch_cursor.key_valid() && batch_cursor.key() == &key {
                        batch_cursor.step_key();
                    }

                    // key_count += 1;
                    // compute_counter += counters.0;
                    // output_counter += counters.1;

                    // assert!(interesting_times2.iter().all(|t| interesting_times.contains(t)));

                    // if buffers != buffers2 {
                    //     println!("PANIC: Output mis-match for key: {:?}", key);
                    //     println!("expected: {:?}", buffers2);
                    //     println!("actually: {:?}", buffers);
                    // }
                    // assert!(buffers == buffers2);

                    // Record future warnings about interesting times (and assert they should be "future").
                    for time in temporary.drain(..) { 
                        assert!(upper_limit.iter().any(|t| t.less_equal(&time)));
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
                        if !local_upper.iter().any(|t| t.less_equal(&time)) {
                            local_upper.retain(|t| !time.less_than(t));
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
                    if !frontier.iter().any(|t| t.less_equal(time)) {
                        frontier.retain(|t| !time.less_than(t));
                        frontier.push(time.clone());
                    }
                }

                // Update `capabilities` to reflect interesting pairs described by `frontier`.
                let mut new_capabilities = Vec::new();
                for time in frontier.drain(..) {
                    if let Some(cap) = capabilities.iter().find(|c| c.time().less_equal(&time)) {
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
    // println!("(key, times) pairs:\t{:?}\t{:?}\t{:?}\tavg {:?}ns\t{:?}", compute_counter, output_counter, key_count, avg_ns, ul);
            }

            // We have processed all updates through `upper_limit` and will only use times in advance of 
            // this frontier to compare against historical times, so we should allow the trace to start
            // compacting batches by advancing times.
            source_trace.advance_by(&upper_limit[..]);
            output_trace.advance_by(&upper_limit[..]);

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
fn consolidate<T: Ord, R: Diff>(list: &mut Vec<(T, R)>) {
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
pub fn consolidate_from<T: Ord+Clone, R: Diff>(vec: &mut Vec<(T, R)>, off: usize) {

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
    R1: Diff,
    R2: Diff,
{
    fn new() -> Self;
    fn compute<K, C1, C2, C3, L>(
        &mut self,
        key: &K, 
        input: &mut C1, 
        output: &mut C2, 
        batch: &mut C3,
        times: &mut Vec<T>, 
        logic: &L, 
        upper_limit: &[T],
        outputs: &mut [(T, Vec<(V2, T, R2)>)],
        new_interesting: &mut Vec<T>) -> (usize, usize)
    where 
        K: Eq+Clone+Debug,
        C1: Cursor<K, V1, T, R1>, 
        C2: Cursor<K, V2, T, R2>, 
        C3: Cursor<K, V1, T, R1>, 
        L: Fn(&K, &[(V1, R1)], &mut Vec<(V2, R2)>);
}


/// Implementation based on replaying historical and new updates together.
mod history_replay {

    use std::fmt::Debug;
    use std::cmp::Ordering;

    // use timely::progress::frontier::Antichain;

    use ::Diff;
    use lattice::Lattice;
    use trace::Cursor;
    use operators::ValueHistory2;

    use super::{PerKeyCompute, consolidate, sort_dedup};

    /// The `HistoryReplayer` is a compute strategy based on moving through existing inputs, interesting times, etc in 
    /// time order, maintaining consolidated representations of updates with respect to future interesting times.
    pub struct HistoryReplayer<V1, V2, T, R1, R2> 
    where
        V1: Ord+Clone,
        V2: Ord+Clone,
        T: Lattice+Ord+Clone,
        R1: Diff,
        R2: Diff,
    {
        batch_history: ValueHistory2<V1, T, R1>,
        input_history: ValueHistory2<V1, T, R1>,
        output_history: ValueHistory2<V2, T, R2>,
        input_buffer: Vec<(V1, R1)>,
        output_buffer: Vec<(V2, R2)>,
        output_produced: Vec<((V2, T), R2)>,
        // known_times: Vec<T>,
        synth_times: Vec<T>,
        meets: Vec<T>,
        times_current: Vec<T>,
        // lower: Vec<T>,
        temporary: Vec<T>,
    }

    impl<V1, V2, T, R1, R2> PerKeyCompute<V1, V2, T, R1, R2> for HistoryReplayer<V1, V2, T, R1, R2> 
    where
        V1: Ord+Clone+Debug,
        V2: Ord+Clone+Debug,
        T: Lattice+Ord+Clone+Debug,
        R1: Diff+Debug,
        R2: Diff+Debug,
    {
        fn new() -> Self {
            HistoryReplayer { 
                batch_history: ValueHistory2::new(),
                input_history: ValueHistory2::new(),
                output_history: ValueHistory2::new(),
                input_buffer: Vec::new(),
                output_buffer: Vec::new(),
                output_produced: Vec::new(),
                // known_times: Vec::new(),
                synth_times: Vec::new(),
                meets: Vec::new(),
                times_current: Vec::new(),
                // lower: Vec::new(),
                temporary: Vec::new(),
            }
        }
        #[inline(never)]
        fn compute<K, C1, C2, C3, L>(
            &mut self,
            key: &K, 
            source_cursor: &mut C1, 
            output_cursor: &mut C2, 
            batch_cursor: &mut C3,
            times: &mut Vec<T>, 
            logic: &L, 
            upper_limit: &[T],
            outputs: &mut [(T, Vec<(V2, T, R2)>)],
            new_interesting: &mut Vec<T>) -> (usize, usize)
        where 
            K: Eq+Clone+Debug,
            C1: Cursor<K, V1, T, R1>, 
            C2: Cursor<K, V2, T, R2>, 
            C3: Cursor<K, V1, T, R1>, 
            L: Fn(&K, &[(V1, R1)], &mut Vec<(V2, R2)>) 
        {
            // The first thing we need to know is which times and values we are worried about.
            // We use `T::min` as the lower bound with which we join everything to avoid changing the times,
            // not knowing any better, but also because all updates should be ahead of the interval's lower
            // bound.

            self.batch_history.clear(); 
            if batch_cursor.key_valid() && batch_cursor.key() == key {
                self.batch_history.load(batch_cursor, |time| time.clone());
            }

            // Tracks the frontier of times we may still consider. Repeatedly re-derived from frontiers of 
            // each of the (currently five) sources of times we consider. Could be simplified to just be the
            // meet of these times, but would lose minimality (not correctness) for non-distributed lattices.

            // Determine a lower frontier of interesting times.
            // I'm not sure what we do with this other than a `debug_assert` and re-scan it to produce `meet`.
            // Perhaps we should just grab the meet here? Or we could leave this here, because as grown-ups we
            // will eventually want to use `advance_by` with a frontier anyhow.
            let mut meet = T::max();
            if let Some(time) = self.batch_history.meet() { meet = meet.meet(&time); }

            self.meets.clear();
            self.meets.extend(times.iter().cloned());
            for index in (1 .. self.meets.len()).rev() {
                self.meets[index-1] = self.meets[index-1].meet(&self.meets[index]);
            }

            if self.meets.len() > 0 { meet = meet.meet(&self.meets[0]); }

            self.input_history.clear(); 
            source_cursor.seek_key(key);
            if source_cursor.key_valid() && source_cursor.key() == key {
                self.input_history.load(source_cursor, |time| time.join(&meet));
            }

            self.output_history.clear();
            output_cursor.seek_key(key);
            if output_cursor.key_valid() && output_cursor.key() == key {
               self.output_history.load(output_cursor, |time| time.join(&meet));
            }

            self.synth_times.clear();
            self.times_current.clear();
            self.output_produced.clear();

            // The frontier of times we may still consider. 
            // Derived from frontiers of our update histories, supplied times, and synthetic times.

            let mut times_slice = &times[..];
            let mut meets_slice = &self.meets[..];

            let mut compute_counter = 0;
            let mut output_counter = 0;

            while !self.input_history.is_done() || !self.output_history.is_done() || 
                  !self.batch_history.is_done() || self.synth_times.len() > 0 || times_slice.len() > 0 {

                // Determine the next time we will process from the available source of times.
                let mut next_time = T::max();
                if let Some(time) = self.input_history.time() { if time.cmp(&next_time) == Ordering::Less { next_time = time.clone(); } }
                if let Some(time) = self.output_history.time() { if time.cmp(&next_time) == Ordering::Less { next_time = time.clone(); } }
                if let Some(time) = self.batch_history.time() { if time.cmp(&next_time) == Ordering::Less { next_time = time.clone(); } }
                if let Some(time) = self.synth_times.first() { if time.cmp(&next_time) == Ordering::Less { next_time = time.clone(); } }
                if let Some(time) = times_slice.first() { if time.cmp(&next_time) == Ordering::Less { next_time = time.clone(); } }
                assert!(next_time != T::max());

                // advance input and output histories.
                self.input_history.step_while_time_is(&next_time);
                self.output_history.step_while_time_is(&next_time);

                // advance batch history, but capture whether an update exists at `next_time`.
                let mut interesting = self.batch_history.step_while_time_is(&next_time);
                if interesting { self.batch_history.advance_buffer_by(&meet); }

                // advance both `synth_times` and `times_slice`, marking the time interesting if in either.
                while self.synth_times.last() == Some(&next_time) {
                    // We don't know enough about `next_time` to avoid putting it in to `times_current`.
                    // TODO: If we knew that the time derived from a canceled batch update, we could remove the time. 
                    self.times_current.push(self.synth_times.pop().unwrap()); // <-- TODO: this could be a min-heap.
                    interesting = true;
                }
                while times_slice.len() > 0 && times_slice[0] == next_time {
                    // We know nothing about why we were warned about `next_time`, and must include it to scare future times.
                    self.times_current.push(times_slice[0].clone());
                    times_slice = &times_slice[1..];
                    meets_slice = &meets_slice[1..];
                    interesting = true;
                }

                // Times could also be interesting if an interesting time is less than them, as they would join
                // and become the time itself. They may not equal the current time because whatever frontier we
                // are tracking may not have advanced far enough.
                // TODO: `batch_history` may or may not be super compact at this point, and so this check might 
                //       yield false positives if not sufficiently compact. Maybe we should into this and see.
                interesting = interesting || self.batch_history.buffer.iter().any(|&((_, ref t),_)| t.less_equal(&next_time));
                interesting = interesting || self.times_current.iter().any(|t| t.less_equal(&next_time));

                // We should only process times that are not in advance of `upper_limit`. 
                //
                // We have no particular guarantee that known times will not be in advance of `upper_limit`.
                // We may have the guarantee that synthetic times will not be, as we test against the limit
                // before we add the time to `synth_times`.
                if !upper_limit.iter().any(|t| t.less_equal(&next_time)) {

                    // We should re-evaluate the computation if this is an interesting time.
                    // If the time is uninteresting (and our logic is sound) it is not possible for there to be 
                    // output produced. This sounds like a good test to have for debug builds!
                    if interesting { 

                        compute_counter += 1;

                        // Assemble the input collection at `next_time`. (`self.input_buffer` cleared just after use).
                        debug_assert!(self.input_buffer.is_empty());
                        self.input_history.advance_buffer_by(&meet);
                        for &((ref value, ref time), diff) in self.input_history.buffer.iter() {
                            if time.less_equal(&next_time) {
                                self.input_buffer.push((value.clone(), diff));
                            }
                            else {
                                self.temporary.push(next_time.join(time));
                            }
                        }
                        consolidate(&mut self.input_buffer);

                        // Apply user logic if non-empty input and see what happens!
                        if self.input_buffer.len() > 0 {
                            logic(key, &self.input_buffer[..], &mut self.output_buffer);
                            self.input_buffer.clear();
                        }            

                        self.output_history.advance_buffer_by(&meet);
                        for &((ref value, ref time), diff) in self.output_history.buffer.iter() {
                            if time.less_equal(&next_time) {
                                self.output_buffer.push((value.clone(), -diff));
                            }
                            else {
                                self.temporary.push(next_time.join(time));
                            }
                        }
                        for &((ref value, ref time), diff) in self.output_produced.iter() {
                            if time.less_equal(&next_time) {
                                self.output_buffer.push((value.clone(), -diff));
                            }
                            else {
                                self.temporary.push(next_time.join(time));
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

                            // We *should* be able to find a capability for `next_time`. Any thing else would 
                            // indicate a logical error somewhere along the way; either we release a capability 
                            // we should have kept, or we have computed the output incorrectly (or both!)
                            let idx = outputs.iter().rev().position(|&(ref time, _)| time.less_equal(&next_time));
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
                    }

                    // Determine synthetic interesting times.
                    //
                    // Synthetic interesting times are produced differently for interesting and uninteresting
                    // times. An uninteresting time must join with an interesting time to become interesting,
                    // which means joins with `self.batch_history` and  `self.times_current`. I think we can
                    // skip `self.synth_times` as we haven't gotten to them yet, but we will and they will be
                    // joined against everything.

                    // Any time, even uninteresting times, must be joined with the current accumulation of 
                    // batch times as well as the current accumulation of `times_current`.
                    for &((_, ref time), _) in self.batch_history.buffer.iter() {
                        if !time.less_equal(&next_time) {
                            self.temporary.push(time.join(&next_time));
                        }
                    }
                    for time in self.times_current.iter() {
                        if !time.less_equal(&next_time) {
                            self.temporary.push(time.join(&next_time));
                        }
                    }

                    sort_dedup(&mut self.temporary);

                    // Introduce synthetic times, and re-organize if we add any.
                    let synth_len = self.synth_times.len();
                    for time in self.temporary.drain(..) {
                        // We can either service `join` now, or must delay for the future.
                        if upper_limit.iter().any(|t| t.less_equal(&time)) {
                            debug_assert!(outputs.iter().any(|&(ref t,_)| t.less_equal(&time)));
                            new_interesting.push(time);
                        }
                        else {
                            self.synth_times.push(time);
                        }
                    }
                    if self.synth_times.len() > synth_len {
                        self.synth_times.sort_by(|x,y| y.cmp(x));
                        self.synth_times.dedup();
                    }
                }
                else {  

                    if interesting {
                        // We cannot process `next_time` now, and must delay it. 
                        //
                        // I think we are probably only here because of an uninteresting time declared interesting,
                        // as initial interesting times are filtered to be in interval, and synthetic times are also
                        // filtered before introducing them to `self.synth_times`.
                        new_interesting.push(next_time.clone());
                        debug_assert!(outputs.iter().any(|&(ref t,_)| t.less_equal(&next_time)))
                    }
                }

                // Update `meet` to track the meet of each source of times.
                meet = T::max();
                if let Some(time) = self.batch_history.meet() { meet = meet.meet(time); }
                if let Some(time) = self.input_history.meet() { meet = meet.meet(time); }
                if let Some(time) = self.output_history.meet() { meet = meet.meet(time); }
                for time in self.synth_times.iter() { meet = meet.meet(time); }
                if let Some(time) = meets_slice.first() { meet = meet.meet(time); }

                // Update `times_current` by the frontier.
                for time in self.times_current.iter_mut() {
                    *time = time.join(&meet);
                }

                sort_dedup(&mut self.times_current);
            }

            // Normalize the representation of `new_interesting`, deduplicating and ordering.
            sort_dedup(new_interesting);

            (compute_counter, output_counter)
        }
    }
}


/// Implementation based on replaying historical and new updates together.
mod history_replay_prior {

    use std::fmt::Debug;
    use std::cmp::Ordering;

    use ::Diff;
    use lattice::Lattice;
    use trace::Cursor;

    use super::{PerKeyCompute, consolidate, sort_dedup, consolidate_from};

    /// The `HistoryReplayer` is a compute strategy based on moving through existing inputs, interesting times, etc in 
    /// time order, maintaining consolidated representations of updates with respect to future interesting times.
    pub struct HistoryReplayer<V1, V2, T, R1, R2> 
    where
        V1: Ord+Clone,
        V2: Ord+Clone,
        T: Lattice+Ord+Clone,
        R1: Diff,
        R2: Diff,
    {
        batch_history: CollectionHistory<V1, T, R1>,
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
        temporary: Vec<T>,
    }

    impl<V1, V2, T, R1, R2> PerKeyCompute<V1, V2, T, R1, R2> for HistoryReplayer<V1, V2, T, R1, R2> 
    where
        V1: Ord+Clone+Debug,
        V2: Ord+Clone+Debug,
        T: Lattice+Ord+Clone+Debug,
        R1: Diff+Debug,
        R2: Diff+Debug,
    {
        fn new() -> Self {
            HistoryReplayer { 
                batch_history: CollectionHistory::new(),
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
                temporary: Vec::new(),
            }
        }
        #[inline(never)]
        fn compute<K, C1, C2, C3, L>(
            &mut self,
            key: &K, 
            source_cursor: &mut C1, 
            output_cursor: &mut C2, 
            batch_cursor: &mut C3,
            times: &mut Vec<T>, 
            logic: &L, 
            upper_limit: &[T],
            outputs: &mut [(T, Vec<(V2, T, R2)>)],
            new_interesting: &mut Vec<T>) -> (usize, usize)
        where 
            K: Eq+Clone+Debug,
            C1: Cursor<K, V1, T, R1>, 
            C2: Cursor<K, V2, T, R2>, 
            C3: Cursor<K, V1, T, R1>, 
            L: Fn(&K, &[(V1, R1)], &mut Vec<(V2, R2)>) 
        {
            // The first thing we need to know is which times and values we are worried about.
            // We use `T::min` as the lower bound with which we join everything to avoid changing the times.
            self.batch_history.reload1(key, batch_cursor, &T::min());

            // Determine a lower frontier of interesting times.
            // I'm not sure what we do with this other than a `debug_assert` and re-scan it to produce `meet`.
            // Perhaps we should just grab the meet here? Or we could leave this here, because as grown-ups we
            // will eventually want to use `advance_by` with a frontier anyhow.
            self.lower.clear();
            for thing in self.batch_history.times.iter() {
                if !self.lower.iter().any(|t| t.less_equal(&thing.0)) { 
                    self.lower.retain(|t| !thing.0.less_than(t));
                    self.lower.push(thing.0.clone());
                }                
            }
            for time in times.iter() {
                if !self.lower.iter().any(|t| t.less_equal(time)) { 
                    self.lower.retain(|t| !time.less_than(t));
                    self.lower.push(time.clone());
                }
            }

            // All times we encounter will be at least `meet`, and so long as we just join with and compare 
            // against interesting times, it is safe to advance all historical times by `meet`. 
            let mut meet = self.lower.iter().fold(T::max(), |meet, time| meet.meet(time));

            // We build "histories" of the input and output, with respect to the times we may encounter in this
            // invocation of `compute`. This means we advance times by joining them with `meet`, which can yield
            // somewhat odd updates; we should not take the advanced times as anything other than a time that is
            // safe to use for joining with and `le` testing against times in the interval.
            //
            // These histories act as caches in front of each cursor. They should provide the same functionality,
            // but are capable of compacting their representation as we proceed through the computation.

            self.input_history.reload2(key, source_cursor, &meet);
            self.output_history.reload3(key, output_cursor, &meet);

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
                //
                // Merge the times in input actions and output actions.
                self.known_times.clear();
                let mut times_slice = &times[..]; 
                let mut batch_slice = &self.batch_history.actions[..];
                let mut input_slice = &self.input_history.actions[..];
                let mut output_slice = &self.output_history.actions[..];
                while times_slice.len() > 0 || batch_slice.len() > 0 || input_slice.len() > 0 || output_slice.len() > 0 {

                    // Determine the next time.
                    let mut next = T::max();
                    if times_slice.len() > 0 && times_slice[0].cmp(&next) == Ordering::Less { next = times_slice[0].clone(); }
                    if batch_slice.len() > 0 && batch_slice[0].0.cmp(&next) == Ordering::Less { next = batch_slice[0].0.clone(); }
                    if input_slice.len() > 0 && input_slice[0].0.cmp(&next) == Ordering::Less { next = input_slice[0].0.clone(); }
                    if output_slice.len() > 0 && output_slice[0].0.cmp(&next) == Ordering::Less { next = output_slice[0].0.clone(); }

                    // Advance each slice until we reach a new time.
                    while times_slice.len() > 0 && times_slice[0] == next { times_slice = &times_slice[1..]; }
                    while batch_slice.len() > 0 && batch_slice[0].0 == next { batch_slice = &batch_slice[1..]; }
                    while input_slice.len() > 0 && input_slice[0].0 == next { input_slice = &input_slice[1..]; }
                    while output_slice.len() > 0 && output_slice[0].0 == next { output_slice = &output_slice[1..]; }

                    self.known_times.push(next);
                }
            }

            // Populate `self.meets` with the meets of suffixes of `self.known_times`.
            //
            // As we move through `self.known_times`, we want to collapse down historical updates, even
            // those that were distinct at the beginning of the interval. To do this, we track the `meet`
            // of each suffix of `self.known_times`, and use this to advance times in update histories.

            self.meets.clear();
            self.meets.reserve(self.known_times.len());
            self.meets.extend(self.known_times.iter().cloned());
            for i in (1 .. self.meets.len()).rev() {
                self.meets[i-1] = self.meets[i-1].meet(&self.meets[i]);
            }

            // we track our position in each of our lists of actions using slices; 
            // as we pull work from the front, we advance the slice. we could have
            // used drain iterators, but we want to peek at the next element and 
            // this seemed like the simplest way to do that.

            let mut times_slice = &times[..];
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

                // Advance each history that we track.
                // TODO: Add batch history somewhere.
                self.input_history.advance_through(&next_time);
                self.output_history.advance_through(&next_time);

                // A time is interesting if it is the join of times, at least one of is from `batch_history` 
                // or `times`. Clearly, times that are present in `batch_history` or `times` are interesting,
                // but so are times in `synth_times` (which result from joining with interesting times) as well
                // as any times that would join with something in one of these sets and end up the same.

                // As we are going to join with each of the active sets in all cases, except when we are beyond
                // the frontier, which we really shouldn't be, perhaps we should do that first and populate 
                // `synth_times`

                // We judge a time interesting if it involves the time of at least one unresolved update.
                // One way this can happen is if it is the time of an update received in this batch.
                // 
                // There are several other ways that a time might be interesting. For example, when we join
                // an apparently uninteresting time with e.g. batch_history and find that the same time comes
                // out, it should be promoted to interesting.
                //
                // Perhaps we should start out with determining the synthetic times for uninteresting times, 
                // and then determine whether to proceed with interesting times. 

                let mut interesting = self.batch_history.advance_through(&next_time);
                if interesting { self.batch_history.collapse_all(&meet); }

                // Advance `known_slice` and `synth_times` past `next_time`.
                while known_slice.len() > 0 && known_slice[0] == next_time {
                    known_slice = &known_slice[1..];
                    meets_slice = &meets_slice[1..];
                }
                while self.synth_times.len() > 0 && self.synth_times[0] == next_time {
                    self.times_current.push(self.synth_times[0].clone());
                    self.synth_times.remove(0); // <-- TODO: this could be a min-heap.
                    interesting = true;
                }
                while times_slice.len() > 0 && times_slice[0] == next_time {
                    self.times_current.push(times_slice[0].clone());
                    times_slice = &times_slice[1..];
                    interesting = true;
                }

                // Times could also be interesting if an interesting time is less than them, as they would join
                // and become the time itself.
                interesting = interesting || self.batch_history.values.iter().any(|h| self.batch_history.times[h.lower .. h.valid]
                                                                                                .iter().any(|t| t.0.less_equal(&next_time)));
                interesting = interesting || self.times_current.iter().any(|t| t.less_equal(&next_time));


                // We should only process times that are not in advance of `upper_limit`. 
                //
                // We have no particular guarantee that known times will not be in advance of `upper_limit`.
                // We may have the guarantee that synthetic times will not be, as we test against the limit
                // before we add the time to `synth_times`.
                if !upper_limit.iter().any(|t| t.less_equal(&next_time)) {

                    // We should re-evaluate the computation if this is an interesting time.
                    // If the time is uninteresting (and our logic is sound) it is not possible for there to be 
                    // output produced. This sounds like a good test to have for debug builds!
                    if interesting { 

                        // any times not greater than `lower` must be empty (and should be skipped, but testing!)

                        if !self.lower.iter().any(|t| t.less_equal(&next_time)) {
                            println!("PANIC: interesting time inversion:");
                            println!("    time:   {:?}", next_time);
                            println!("    lower: {:?}", self.lower);
                            panic!();
                        }

                        compute_counter += 1;

                        // Assemble the input collection at `next_time`. (`self.input_buffer` cleared just after use).
                        debug_assert!(self.input_buffer.is_empty());
                        self.input_history.insert(&next_time, &meet, &mut self.input_buffer);

                        // Apply user logic if non-empty input and see what happens!
                        self.output_buffer.clear();
                        if self.input_buffer.len() > 0 {
                            logic(key, &self.input_buffer[..], &mut self.output_buffer);
                        }
                
                        self.input_buffer.clear();

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
                            if time.less_equal(&next_time) {
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

                            // We *should* be able to find a capability for `next_time`. Any thing else would 
                            // indicate a logical error somewhere along the way; either we release a capability 
                            // we should have kept, or we have computed the output incorrectly (or both!)
                            let idx = outputs.iter().rev().position(|&(ref time, _)| time.less_equal(&next_time));
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
                    }

                    // Determine synthetic interesting times.
                    //
                    // Synthetic interesting times are produced differently for interesting and uninteresting
                    // times. An uninteresting time must join with an interesting time to become interesting,
                    // which means joins with `self.batch_history` and  `self.times_current`. I think we can
                    // skip `self.synth_times` as we haven't gotten to them yet, but we will and they will be
                    // joined against everything.

                    self.batch_history.join_with_into(&next_time, &mut self.temporary);
                    for time in self.times_current.iter() {
                        if !time.less_equal(&next_time) {
                            self.temporary.push(time.join(&next_time));
                        }
                    }

                    // Interesting times should also join with times of updates present in `self.input_history`
                    // and `self.output_history`. 
                    if interesting {
                        self.input_history.join_with_into(&next_time, &mut self.temporary);
                        self.output_history.join_with_into(&next_time, &mut self.temporary);
                        for &((_, ref time), _) in self.output_produced.iter() {
                            if !time.less_equal(&next_time) {
                                self.temporary.push(time.join(&next_time));
                            }
                        }
                    }

                    self.temporary.sort();
                    self.temporary.dedup();

                    for time in self.temporary.drain(..) {
                        // We can either service `join` now, or must delay for the future.
                        if upper_limit.iter().any(|t| t.less_equal(&time)) {
                            debug_assert!(outputs.iter().any(|&(ref t,_)| t.less_equal(&time)));
                            new_interesting.push(time);
                        }
                        else {
                            self.synth_times.push(time);
                        }
                    }
                }
                else {  

                    if interesting {
                        // We cannot process `next_time` now, and must delay it. I'm not sure why we would ever 
                        // be here, actually. I think we have already filtered all new interesting times, and all
                        // other interesting times should be within the interval.
                        new_interesting.push(next_time.clone());
                        debug_assert!(outputs.iter().any(|&(ref t,_)| t.less_equal(&next_time)))
                    }
                }

                // Whether a time is interesting or not (in both cases) we must consider joining it with the times
                // currently present in `self.batch_history` and likely the times in `self.synth_times`. I am less
                // clear on whether we need to track interesting times from `times` and join with those as well. =/
                //
                // It would seem that the join of a historical time with a historically interesting time should have
                // already been warned about, if previous executions were correct. Or is this the point where we have
                // to admit that we didn't produce the full closure under join before? >.<  Shit.

                // Update `meet`, and advance and deduplicate times in `self.times_current`.
                // NOTE: This does not advance input or output collections, which is done when the are accumulated.
                if meets_slice.len() > 0 || self.synth_times.len() > 0 {
                    // Start from `T::max()` and take the meet with each synthetic time. Also meet with the first 
                    // element of `meets_slice` if it exists.
                    meet = self.synth_times.iter().fold(T::max(), |meet, time| meet.meet(time));
                    if meets_slice.len() > 0 { meet = meet.meet(&meets_slice[0]); }

                    // Advance the contents of `self.times_current`.
                    for time in self.times_current.iter_mut() {
                        *time = time.join(&meet);
                    }
                    self.times_current.sort();
                    self.times_current.dedup();
                }

                // Sort and deduplicate `self.synth_times`. This is our poor replacement for a min-heap, but works.
                sort_dedup(&mut self.synth_times);
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

    struct CollectionHistory<V: Clone, T: Lattice+Ord+Clone, R: Diff> {
        pub values: Vec<ValueHistory<V>>,
        pub actions: Vec<(T, usize)>,
        action_cursor: usize,
        pub times: Vec<(T, R)>,
    }

    impl<V: Clone, T: Lattice+Ord+Clone+Debug, R: Diff> CollectionHistory<V, T, R> {
        fn new() -> Self {
            CollectionHistory {
                values: Vec::new(),
                actions: Vec::new(),
                action_cursor: 0,
                times: Vec::new(),
            }
        }

        #[inline(never)]
        fn reload1<K, C>(&mut self, key: &K, cursor: &mut C, meet: &T) 
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
                cursor.step_key();
            }

            self.build_actions();
        }

        #[inline(never)]
        fn reload2<K, C>(&mut self, key: &K, cursor: &mut C, meet: &T) 
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
                cursor.step_key();
            }

            self.build_actions();
        }

        #[inline(never)]
        fn reload3<K, C>(&mut self, key: &K, cursor: &mut C, meet: &T) 
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
                cursor.step_key();
            }

            self.build_actions();
        }

        /// Advances the indexed value by one, with the ability to compact times by `meet`.
        fn advance_through(&mut self, time: &T) -> bool {
            let mut advanced = false;
            while self.action_cursor < self.actions.len() && self.actions[self.action_cursor].0.cmp(time) != Ordering::Greater {
                self.values[self.actions[self.action_cursor].1].valid += 1;
                self.action_cursor += 1;
                advanced = true;
            }
            advanced
        }

        fn collapse_all(&mut self, meet: &T) {

            for value_index in 0 .. self.values.len() {

                let clean = self.values[value_index].clean;
                let valid = self.values[value_index].valid;

                // take the time to collapse if there are changes. 
                // this is not the only reason to collapse, and we may want to be more or less aggressive
                if clean < valid {
                    self.collapse(value_index, meet);
                }
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
                    if self.times[index].0.less_equal(time) {
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
                    if self.times[index].0.less_equal(time) {
                        sum = sum - self.times[index].1;
                    }
                }
                if !sum.is_zero() {
                    destination.push((self.values[value_index].value.clone(), sum));
                }
            }
        }

        // Joins `time` with the time of each active update and adds the result to `destination` if the 
        // result is strictly greater than `time`.
        fn join_with_into(&self, time: &T, destination: &mut Vec<T>) {
            for history in self.values.iter() {
                for &(ref t, _) in &self.times[history.lower .. history.valid] {
                    if !t.less_equal(time) {
                        destination.push(t.join(time));
                    }
                }
            }
        }
    }
}
