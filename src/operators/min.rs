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
use trace::cursor::cursor_list::CursorList;
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

        // let mut thinker1 = interest_accumulator::InterestAccumulator::<V, V2, G::Timestamp, R, R2>::new();
        let mut thinker2 = value_iteration::ValueIterator::<V, V2, G::Timestamp, R, R2>::new();
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

                let mut compute_counter = 0;
                let mut output_counter = 0;
                let timer = ::std::time::Instant::now();

                // We now march through the keys we must work on, drawing from `batch_cursors` and `exposed`.
                //
                // We only keep valid cursors (those with more data) in `batch_cursors`, and so its length
                // indicates whether more data remain. We move throuh `exposed` using `exposed_position`.
                // There could perhaps be a less provocative variable name.

                let mut key_count = 0;

                let mut exposed_position = 0;
                // batch_cursors.retain(|cursor| cursor.key_valid());
                while batch_cursor.key_valid() || exposed_position < exposed.len() {

                    key_count += 1;

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

                    // do the per-key computation.
                    temporary.clear();
                    let counters = thinker2.compute(
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

                    compute_counter += counters.0;
                    output_counter += counters.1;

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

// Several methods are broken out here to help understand performance profiling.

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
mod value_iteration {

    use std::fmt::Debug;
    use std::cmp::Ordering;

    use ::Ring;
    use lattice::Lattice;
    use trace::Cursor;

    use super::{PerKeyCompute, consolidate, sort_dedup_b, consolidate_from};

    /// The `ValueIterator` is a compute strategy based on moving through existing inputs, interesting times, etc in 
    /// time order, maintaining consolidated representations of updates with respect to future interesting times.
    pub struct ValueIterator<V1, V2, T, R1, R2> 
    where
        V1: Ord+Clone,
        V2: Ord+Clone,
        T: Lattice+Ord+Clone,
        R1: Ring,
        R2: Ring,
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

    impl<V1, V2, T, R1, R2> PerKeyCompute<V1, V2, T, R1, R2> for ValueIterator<V1, V2, T, R1, R2> 
    where
        V1: Ord+Clone+Debug,
        V2: Ord+Clone+Debug,
        T: Lattice+Ord+Clone+Debug,
        R1: Ring+Debug,
        R2: Ring+Debug,
    {
        fn new() -> Self {
            ValueIterator { 
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

            // Our plan is to iterate through values, in order, reproducing the output history for each value.
            // We maintain the cumulative output history with values elided, representing the number of records
            // in the output, which should be sufficent for us to determine for any new value where it peeks 
            // through.
            //
            // There are several optimizations to this, for example:
            //   1. We should be able to stop early if we realize that the interval is "full". I'm not exactly 
            //      sure what this means; we need +1 in the output covering the lower bounds of the interval, 
            //      and no negations anywhere, I would guess (or, only at points where two +s collide?). This
            //      seems like it should have a good answer, but I haven't nailed it yet.
            //   2. With information about the smallest values that have changed, stolen from `batch_cursor` 
            //      and a modified `times`, we can skip all values before the smallest and just load the output 
            //      trace and start from there.

            source_cursor.seek_key(key);
            output_cursor.seek_key(key);

            self.lower.clear();
            if batch_cursor.key_valid() && batch_cursor.key() == key {
                while batch_cursor.val_valid() {
                    batch_cursor.map_times(|time,_| {
                        if !self.lower.iter().any(|t| t.le(time)) { 
                            self.lower.retain(|t| !time.lt(t));
                            self.lower.push(time.clone());
                        }                
                    });
                    batch_cursor.step_val();
                }
            }
            for time in times.iter() {
                if !self.lower.iter().any(|t| t.le(time)) { 
                    self.lower.retain(|t| !time.lt(t));
                    self.lower.push(time.clone());
                }
            }

            // All times we encounter will be at least `meet`, and so long as we just join with and compare 
            // against interesting times, it is safe to advance all historical times by `meet`. 
            let mut meet = self.lower.iter().fold(T::max(), |meet, time| meet.meet(time));

            // All free, input, and output updates, no values.
            let mut free_history = Vec::<(T, T, R2)>::new();
            let mut input_history = Vec::<(T, T, R1)>::new();
            let mut output_history = Vec::<(T, R2)>::new();     // <-- we don't need to track the meet.

            // Accumulated accepted free, input, and output updates, no values.
            let mut full_updates = Vec::<(T, R2)>::new();
            let mut input_updates = Vec::<(T, R1)>::new();
            let mut output_updates = Vec::<(T, R2)>::new();

            while source_cursor.val_valid() {

                let val: V1 = source_cursor.val().clone();
                let mut slice = [(val, R1::zero())];

                // extract the history for the current value. use `time.clone()` as initial meet, but we will fix it.
                source_cursor.map_times(|time, diff| source_history.push(((time.join(&meet), time.join(&meet)), diff)));
                consolidate(&mut source_history);

                // We want to walk through the merged source and output histories, accepting updates to each and 
                // reconsidering the relationship between the two at all interesting times. I am not sure if this 
                // is any more complicated than just looking at times in the source and output histories.
                //
                // Some times we consider may be beyond `upper_limit`, in which case they turn in to notifications.
                // What if anything prompts us to return to a computation later, in the absence of external changes?
                // I suppose we revisit all values, including those traditionally at later times (e.g. large distances
                // even at early iterations). Each update (source, output) should probably be joined against the other 
                // accumulation to indicate noteworthy times? 
                // 
                // I think we are producing the join of all times in the source history with all times in the accumulated
                // output history, which should capture any time that is a change in either of them. 

                // Produced the meets of source and output times.
                for i in (1 .. free_history.len()).rev() { free_history[i-1].1 = free_history[i-1].1.meet(&free_history[i].1); }
                for i in (1 .. input_history.len()).rev() { input_history[i-1].1 = input_history[i-1].1.meet(&input_history[i].1); }

                let mut free_index = 0;
                let mut input_index = 0;

                let mut synthetic_times = Vec::<T>::new();

                let mut full_slice = &free_history[..];
                let mut input_slice = &input_history[..];

                // walk through these historic moments
                while full_slice.len() > 0 || input_slice.len() > 0 || synthetic_times.len() > 0 {

                    // determine the next time we work on
                    let mut time = T::max();

                    // Update the time to be the least of the next times.
                    full_slice.first().map(|x| time = min(time, x.0.clone()));
                    input_slice.first().map(|x| time = min(time, x.0.clone()));
                    synthetic_times.first().map(|x| time = min(time, x.clone()));

                    // accept updates for `time` into `full_updates`
                    while full_slice.len() > 0 && full_slice[0].0 == time {
                        full_updates.push((time.clone(), full_slice[0].2));
                        full_slice = &full_slice[1..];
                    }

                    // accept updates for `time` into `input_updates`
                    while input_slice.len() && input_slice[0].0 == time {
                        input_updates.push((time.clone(), input_slice[0].2));
                        input_slice = &input_slice[1..];
                    }

                    // determine the meet of times, so that we can clean up accepted updates.
                    let mut meet = T::max();
                    if full_slice.len() > 0 { meet = meet.meet(&full_slice[0].1); }
                    if input_slice.len() > 0 { meet = meet.meet(&input_slice[0].1); }
                    for time in synthetic_times.iter() { meet = meet.meet(time); }

                    // bring accepted updates forward to `meet`, consolidate update collections.
                    for update in full_updates.iter_mut() { update.0 = update.0.join(&meet); }
                    consolidate(&mut full_updates);
                    for update in input_updates.iter_mut() { update.0 = update.0.join(&meet); }
                    consolidate(&mut input_updates);
                    for update in output_updates.iter_mut() { update.0 = update.0.join(&meet); }
                    consolidate(&mut output_updates);

                    // TODO: We could delay any inspection/accumulation of input/output until this is zero.
                    let mut full_sum = R2::zero;
                    for &(ref t, diff) in full_updates.iter() {
                        if t.le(&time) { full_sum += diff; }
                        else { interesting.push(t.join(&time)); }
                    }

                    let mut input_sum = R1::zero;
                    for &(ref t, diff) in input_updates.iter() {
                        if t.le(&time) { input_sum += diff; }
                        else { interesting.push(t.join(&time)); }
                    }

                    let mut output_sum = R2::zero;
                    for &(ref t, diff) in output_updates.iter() {
                        if t.le(&time) { output_sum += diff; }
                        else { interesting.push(t.join(&time)); }
                    }

                    // if `full_sum` is non-zero, the output is automatically empty. 
                    // otherwise, we call user logic on the singleton set `[(val, input_sum)]`
                    // and subtract `output_sum` from the result.
                    let mut temp_output = Vec::<(V2, R2)>::new();
                    if full_sum.is_zero() && !input_sum.is_zero() {
                        slice.1 = input_sum;
                        logic(key, &slice[..], &mut temp_output);
                    }

                    // subtract `output_sum` from whatever we got
                    if temp_output.len() == 0 { 
                        temp_output.push((val.clone(), -output_sum));
                    }
                    else if temp_output.len() == 1 {
                        assert!(temp_output[0].0 == val);
                        temp_output[0].1 = temp_output[0].1 - output_sum;
                    }
                    else {
                        panic!("too many outputs! {:?}", temp_output);
                    }
                    consolidate(&mut temp_output);

                    // resolve interesting times.
                    interesting.sort();
                    interesting.dedup();
                    for time in interesting.drain(..) {
                        if !upper_limit.iter().any(|t| t.le(&time)) {
                            synthetic_times.push(time);
                        }
                        else {
                            new_interesting.push(time); // e.g. +(1,8) and +(3,0) warn about ?(3,8), even if not there yet.
                        }
                    }
                    synthetic_times.sort();
                    synthetic_times.dedup();

                }

                // For values <= source.val(), merge-subtract with whatever we have in output_cursor.
                // This may include values strictly less than source.val(), which should make their subtraction easy. ;)
                unimplemented!();

                source_cursor.step_val();
            }

            // Subtract histories for all remaining values in output_cursor.
            unimplemented!();
        }
    }
}