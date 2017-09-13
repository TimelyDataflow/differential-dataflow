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

use std::fmt::Debug;

use hashable::Hashable;
use ::{Data, Collection, Diff};

use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::dataflow::*;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;

use operators::arrange::{Arranged, ArrangeByKey, ArrangeBySelf, BatchWrapper, TraceAgent};
use lattice::Lattice;
use trace::{Batch, BatchReader, Cursor, Trace, Builder};
use trace::cursor::cursor_list::CursorList;
// use trace::implementations::hash::HashValSpine as DefaultValTrace;
// use trace::implementations::hash::HashKeySpine as DefaultKeyTrace;
use trace::implementations::ord::OrdValSpine as DefaultValTrace;
use trace::implementations::ord::OrdKeySpine as DefaultKeyTrace;

use trace::TraceReader;

/// Extension trait for the `group` differential dataflow method.
pub trait Group<G: Scope, K: Data, V: Data, R: Diff> where G::Timestamp: Lattice+Ord {
    /// Groups records by their first field, and applies reduction logic to the associated values.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Group;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///         // report the first value for each group
    ///         scope.new_collection_from(1 .. 10).1
    ///              .map(|x| (x / 3, x))
    ///              .group(|_key, src, dst| {
    ///                  dst.push((*src[0].0, 1))
    ///              });
    ///     });
    /// }
    /// ```
    fn group<L, V2: Data, R2: Diff>(&self, logic: L) -> Collection<G, (K, V2), R2>
    where L: Fn(&K, &[(&V, R)], &mut Vec<(V2, R2)>)+'static;
}

impl<G: Scope, K: Data+Hashable, V: Data, R: Diff> Group<G, K, V, R> for Collection<G, (K, V), R>
    where G::Timestamp: Lattice+Ord+Debug, <K as Hashable>::Output: Data {
    fn group<L, V2: Data, R2: Diff>(&self, logic: L) -> Collection<G, (K, V2), R2>
        where L: Fn(&K, &[(&V, R)], &mut Vec<(V2, R2)>)+'static {
        self.arrange_by_key()
            .group_arranged(logic, DefaultValTrace::new())
            .as_collection(|k,v| (k.clone(), v.clone()))
    }
}

/// Extension trait for the `distinct` differential dataflow method.
pub trait Distinct<G: Scope, K: Data> where G::Timestamp: Lattice+Ord {
    /// Reduces the collection to one occurrence of each distinct element.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Distinct;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///         // report at most one of each key.
    ///         scope.new_collection_from(1 .. 10).1
    ///              .map(|x| x / 3)
    ///              .distinct();
    ///     });
    /// }
    /// ```
    fn distinct(&self) -> Collection<G, K, isize>;
}

impl<G: Scope, K: Data+Hashable> Distinct<G, K> for Collection<G, K, isize>
where G::Timestamp: Lattice+Ord+::std::fmt::Debug {
    fn distinct(&self) -> Collection<G, K, isize> {
        self.arrange_by_self()
            .group_arranged(|_k,_s,t| t.push(((), 1)), DefaultKeyTrace::new())
            .as_collection(|k,_| k.clone())
    }
}


/// Extension trait for the `count` differential dataflow method.
pub trait Count<G: Scope, K: Data, R: Diff> where G::Timestamp: Lattice+Ord {
    /// Counts the number of occurrences of each element.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Count;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///         // report the number of occurrences of each key
    ///         scope.new_collection_from(1 .. 10).1
    ///              .map(|x| x / 3)
    ///              .count();
    ///     });
    /// }
    /// ```
    fn count(&self) -> Collection<G, (K, R), isize>;
}

impl<G: Scope, K: Data+Hashable, R: Diff> Count<G, K, R> for Collection<G, K, R>
 where G::Timestamp: Lattice+Ord+::std::fmt::Debug {
    fn count(&self) -> Collection<G, (K, R), isize> {
        self.arrange_by_self()
            .group_arranged(|_k,s,t| t.push((s[0].1, 1)), DefaultValTrace::new())
            .as_collection(|k,&c| (k.clone(), c))
    }
}


/// Extension trait for the `group_arranged` differential dataflow method.
pub trait GroupArranged<G: Scope, K: Data, V: Data, R: Diff> where G::Timestamp: Lattice+Ord {
    /// Applies `group` to arranged data, and returns an arrangement of output data.
    ///
    /// This method is used by the more ergonomic `group`, `distinct`, and `count` methods, although
    /// it can be very useful if one needs to manually attach and re-use existing arranged collections.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::arrange::Arrange;
    /// use differential_dataflow::operators::group::GroupArranged;
    /// use differential_dataflow::trace::Trace;
    /// use differential_dataflow::trace::implementations::ord::OrdValSpine;
    /// use differential_dataflow::hashable::OrdWrapper;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         // wrap and order input, then group manually.
    ///         scope.new_collection_from(1 .. 10u32).1
    ///              .map(|x| (OrdWrapper { item: x / 3 }, x))
    ///              .arrange(OrdValSpine::new())
    ///              .group_arranged(
    ///                  move |_key, src, dst| dst.push((*src[0].0, 1)),
    ///                  OrdValSpine::new()
    ///              );
    ///     });
    /// }
    /// ```
    fn group_arranged<L, V2, T2, R2>(&self, logic: L, empty: T2) -> Arranged<G, K, V2, R2, TraceAgent<K, V2, G::Timestamp, R2, T2>>
        where
            V2: Data,
            R2: Diff,
            T2: Trace<K, V2, G::Timestamp, R2>+'static,
            T2::Batch: Batch<K, V2, G::Timestamp, R2>,
            L: Fn(&K, &[(&V, R)], &mut Vec<(V2, R2)>)+'static
            ; 
}

impl<G, K, V, R> GroupArranged<G, K, V, R> for Collection<G, (K, V), R>
where
    G: Scope,
    G::Timestamp: Lattice+Ord,
    K: Data+Hashable,
    V: Data,
    R: Diff,
{
    fn group_arranged<L, V2, T2, R2>(&self, logic: L, empty: T2) -> Arranged<G, K, V2, R2, TraceAgent<K, V2, G::Timestamp, R2, T2>>
        where
            V2: Data,
            R2: Diff,
            T2: Trace<K, V2, G::Timestamp, R2>+'static,
            T2::Batch: Batch<K, V2, G::Timestamp, R2>,
            L: Fn(&K, &[(&V, R)], &mut Vec<(V2, R2)>)+'static
    {
        self.arrange_by_key()
            .group_arranged(logic, empty)
    }
}

impl<G: Scope, K: Data, V: Data, T1, R: Diff> GroupArranged<G, K, V, R> for Arranged<G, K, V, R, T1>
where 
    G::Timestamp: Lattice+Ord,
    T1: TraceReader<K, V, G::Timestamp, R>+Clone+'static,
    T1::Batch: BatchReader<K, V, G::Timestamp, R> {
        
    fn group_arranged<L, V2, T2, R2>(&self, logic: L, empty: T2) -> Arranged<G, K, V2, R2, TraceAgent<K, V2, G::Timestamp, R2, T2>>
        where 
            V2: Data,
            R2: Diff,
            T2: Trace<K, V2, G::Timestamp, R2>+'static,
            T2::Batch: Batch<K, V2, G::Timestamp, R2>,
            L: Fn(&K, &[(&V, R)], &mut Vec<(V2, R2)>)+'static {

        let mut source_trace = self.trace.clone();

        let (mut output_reader, mut output_writer) = TraceAgent::new(empty);

        // let mut output_trace = TraceRc::make_from(agent).0;
        let result_trace = output_reader.clone();

        // let mut thinker1 = history_replay_prior::HistoryReplayer::<V, V2, G::Timestamp, R, R2>::new();
        // let mut thinker = history_replay::HistoryReplayer::<V, V2, G::Timestamp, R, R2>::new();
        let mut temporary = Vec::<G::Timestamp>::new();

        // Our implementation maintains a list of outstanding `(key, time)` synthetic interesting times, 
        // as well as capabilities for these times (or their lower envelope, at least).
        let mut interesting = Vec::<(K, G::Timestamp)>::new();
        let mut capabilities = Vec::<Capability<G::Timestamp>>::new();

        // buffers and logic for computing per-key interesting times "efficiently".
        let mut interesting_times = Vec::<G::Timestamp>::new();

        // space for assembling the upper bound of times to process.
        let mut upper_limit = Antichain::<G::Timestamp>::new();

        // tracks frontiers received from batches, for sanity.
        let mut upper_received = vec![<G::Timestamp as Lattice>::minimum()];

        // We separately track the frontiers for what we have sent, and what we have sealed. 
        let mut lower_issued = Antichain::from_elem(<G::Timestamp as Lattice>::minimum());

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
            let mut batch_storage = Vec::new();

            // The only purpose of `lower_received` was to allow slicing off old input.
            let lower_received = upper_received.clone();

            // Drain the input stream of batches, validating the contiguity of the batch descriptions and
            // capturing a cursor for each of the batches as well as ensuring we hold a capability for the
            // times in the batch.
            input.for_each(|capability, batches| {

                // In principle we could have multiple batches per message (in practice, it would be weird).
                for batch in batches.drain(..).map(|x| x.item) {
                    assert!(&upper_received[..] == batch.description().lower());
                    upper_received = batch.description().upper().to_vec();
                    let (cursor, store) = batch.cursor();
                    batch_cursors.push(cursor);
                    batch_storage.push(store);
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
                for time2 in upper_received.iter() {
                    upper_limit.insert(time1.join(time2));
                }
            }

            // If we have no capabilities, then we (i) should not produce any outputs and (ii) could not send
            // any produced outputs even if they were (incorrectly) produced. We cannot even send empty batches
            // to indicate forward progress, and must hope that downstream operators look at progress frontiers
            // as well as batch descriptions.
            //
            // We can (and should) advance source and output traces if `upper_limit` indicates this is possible.
            if capabilities.iter().any(|c| !upper_limit.less_equal(c.time())) {

                // `interesting` contains "warnings" about keys and times that may need to be re-considered.
                // We first extract those times from this list that lie in the interval we will process.
                sort_dedup(&mut interesting);
                let mut new_interesting = Vec::new();
                let mut exposed = Vec::new();
                segment(&mut interesting, &mut exposed, &mut new_interesting, |&(_, ref time)| {
                    !upper_limit.less_equal(time)
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
                    buffers.push((capabilities[i].time().clone(), Vec::new()));
                    builders.push(<T2::Batch as Batch<K,V2,G::Timestamp,R2>>::Builder::new());
                }

                // cursors for navigating input and output traces.
                let (mut source_cursor, source_storage): (T1::Cursor, _) = source_trace.cursor_through(&lower_received[..]).unwrap();
                let source_storage = &source_storage;
                let (mut output_cursor, output_storage): (T2::Cursor, _) = output_reader.cursor(); // TODO: this panicked when as above; WHY???
                let output_storage = &output_storage;
                let (mut batch_cursor, batch_storage) = (CursorList::new(batch_cursors, &batch_storage), batch_storage);
                let batch_storage = &batch_storage;

                let mut thinker = history_replay::HistoryReplayer::<V, V2, G::Timestamp, R, R2>::new();

                // We now march through the keys we must work on, drawing from `batch_cursors` and `exposed`.
                //
                // We only keep valid cursors (those with more data) in `batch_cursors`, and so its length
                // indicates whether more data remain. We move throuh `exposed` using `exposed_position`.
                // There could perhaps be a less provocative variable name.
                let mut exposed_position = 0;
                while batch_cursor.key_valid(batch_storage) || exposed_position < exposed.len() {

                    // Determine the next key we will work on; could be synthetic, could be from a batch.
                    let key1 = if exposed_position < exposed.len() { Some(exposed[exposed_position].0.clone()) } else { None };
                    let key2 = if batch_cursor.key_valid(&batch_storage) { Some(batch_cursor.key(batch_storage).clone()) } else { None };
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
                    sort_dedup(&mut interesting_times);

                    // do the per-key computation.
                    let _counters = thinker.compute(
                        &key, 
                        (&mut source_cursor, source_storage),
                        (&mut output_cursor, output_storage),
                        (&mut batch_cursor, batch_storage),
                        &mut interesting_times, 
                        &logic, 
                        upper_limit.elements(), 
                        &mut buffers[..], 
                        &mut temporary,
                    );

                    if batch_cursor.key_valid(batch_storage) && batch_cursor.key(batch_storage) == &key {
                        batch_cursor.step_key(batch_storage);
                    }

                    // Record future warnings about interesting times (and assert they should be "future").
                    for time in temporary.drain(..) { 
                        debug_assert!(upper_limit.less_equal(&time));
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
                        local_upper.insert(capability.time().clone());
                    }

                    if lower_issued.elements() != local_upper.elements() {

                        let batch = builder.done(lower_issued.elements(), local_upper.elements(), lower_issued.elements());

                        // ship batch to the output, and commit to the output trace.
                        output.session(&capabilities[index]).give(BatchWrapper { item: batch.clone() });
                        output_writer.seal(local_upper.elements(), Some((capabilities[index].time().clone(), batch)));

                        lower_issued = local_upper;
                    }
                }

                // Determine the frontier of our interesting times.
                let mut frontier = Antichain::<G::Timestamp>::new();
                for &(_, ref time) in &interesting {
                    frontier.insert(time.clone());
                }

                // Update `capabilities` to reflect interesting pairs described by `frontier`.
                let mut new_capabilities = Vec::new();
                for time in frontier.elements().iter() {
                    if let Some(cap) = capabilities.iter().find(|c| c.time().less_equal(time)) {
                        new_capabilities.push(cap.delayed(time));
                    }
                    else {
                        println!("{}:\tfailed to find capability less than new frontier time:", id);
                        println!("{}:\t  time: {:?}", id, time);
                        println!("{}:\t  caps: {:?}", id, capabilities);
                        println!("{}:\t  uppr: {:?}", id, upper_limit);
                    }
                }
                capabilities = new_capabilities;

                // ensure that observed progres is reflected in the output.
                output_writer.seal(upper_limit.elements(), None);
            }

            // We only anticipate future times in advance of `upper_limit`.
            source_trace.advance_by(upper_limit.elements());
            output_reader.advance_by(upper_limit.elements());

            // We will only slice the data between future batches.
            source_trace.distinguish_since(&upper_received[..]);
            output_reader.distinguish_since(&upper_received[..]);
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

trait PerKeyCompute<'a, V1, V2, T, R1, R2> 
where
    V1: Ord+Clone+'a,
    V2: Ord+Clone+'a,
    T: Lattice+Ord+Clone,
    R1: Diff,
    R2: Diff,
{
    fn new() -> Self;
    fn compute<K, C1, C2, C3, L>(
        &mut self,
        key: &K, 
        source_cursor: (&mut C1, &'a C1::Storage), 
        output_cursor: (&mut C2, &'a C2::Storage),
        batch_cursor: (&mut C3, &'a C3::Storage),
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
        L: Fn(&K, &[(&V1, R1)], &mut Vec<(V2, R2)>);
}


/// Implementation based on replaying historical and new updates together.
mod history_replay {

    use std::fmt::Debug;

    use ::Diff;
    use lattice::Lattice;
    use trace::Cursor;
    use operators::ValueHistory2;

    use super::{PerKeyCompute, consolidate, sort_dedup};

    /// The `HistoryReplayer` is a compute strategy based on moving through existing inputs, interesting times, etc in 
    /// time order, maintaining consolidated representations of updates with respect to future interesting times.
    pub struct HistoryReplayer<'a, V1, V2, T, R1, R2> 
    where
        V1: Ord+Clone+'a,
        V2: Ord+Clone+'a,
        T: Lattice+Ord+Clone,
        R1: Diff,
        R2: Diff,
    {
        batch_history: ValueHistory2<'a, V1, T, R1>,
        input_history: ValueHistory2<'a, V1, T, R1>,
        output_history: ValueHistory2<'a, V2, T, R2>,
        input_buffer: Vec<(&'a V1, R1)>,
        output_buffer: Vec<(V2, R2)>,
        output_produced: Vec<((V2, T), R2)>,
        synth_times: Vec<T>,
        meets: Vec<T>,
        times_current: Vec<T>,
        temporary: Vec<T>,
    }

    impl<'a, V1, V2, T, R1, R2> PerKeyCompute<'a, V1, V2, T, R1, R2> for HistoryReplayer<'a, V1, V2, T, R1, R2> 
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
                synth_times: Vec::new(),
                meets: Vec::new(),
                times_current: Vec::new(),
                temporary: Vec::new(),
            }
        }
        #[inline(never)]
        fn compute<K, C1, C2, C3, L>(
            &mut self,
            key: &K, 
            (source_cursor, source_storage): (&mut C1, &'a C1::Storage), 
            (output_cursor, output_storage): (&mut C2, &'a C2::Storage),
            (batch_cursor, batch_storage): (&mut C3, &'a C3::Storage),
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
            L: Fn(&K, &[(&V1, R1)], &mut Vec<(V2, R2)>) 
        {

            // The work to do is defined principally be the contents of `batch_cursor` and `times`, which
            // together indicate the times at which we should re-evaluate the user logic on the accumulated
            // inputs. Before anything else, we will want to extract this information, as it will allow us 
            // to thin out other inputs as we load them.

            // Load the batch contents.
            self.batch_history.clear(); 
            if batch_cursor.key_valid(batch_storage) && batch_cursor.key(batch_storage) == key {
                self.batch_history.load(batch_cursor, batch_storage, |time| time.clone());
            }

            // We determine the meet of times we must reconsider (those from `batch` and `times`). This meet
            // can be used to advance other historical times, which may consolidate their representation. As
            // a first step, we determine the meets of each *suffix* of `times`, which we will use as we play
            // history forward.

            self.meets.clear();
            self.meets.extend(times.iter().cloned());
            for index in (1 .. self.meets.len()).rev() {
                self.meets[index-1] = self.meets[index-1].meet(&self.meets[index]);
            }

            // Determine the meet of times in `batch` and `times`.
            let mut meet = T::maximum();
            if self.meets.len() > 0 { meet = meet.meet(&self.meets[0]); }
            if let Some(time) = self.batch_history.meet() { meet = meet.meet(&time); }

            // Having determined the meet, we can load the input and output histories, where we 
            // advance all times by joining them with `meet`. The resulting times are more compact
            // and guaranteed to accumulate identically for times greater or equal to `meet`.

            // Load the input history.
            self.input_history.clear(); 
            source_cursor.seek_key(source_storage, key);
            if source_cursor.key_valid(source_storage) && source_cursor.key(source_storage) == key {
                self.input_history.load(source_cursor, source_storage, |time| time.join(&meet));
            }

            // Load the output history.
            self.output_history.clear();
            output_cursor.seek_key(output_storage, key);
            if output_cursor.key_valid(output_storage) && output_cursor.key(output_storage) == key {
               self.output_history.load(output_cursor, output_storage, |time| time.join(&meet));
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

            // We play history forward, continuing as long as we have any outstanding times.

            while !self.input_history.is_done() || !self.output_history.is_done() || 
                  !self.batch_history.is_done() || self.synth_times.len() > 0 || times_slice.len() > 0 {

                // Determine the next time we will process from the available source of times.
                let mut next_time = T::maximum();
                if let Some(time) = self.input_history.time() { if time < &next_time { next_time = time.clone(); } }
                if let Some(time) = self.output_history.time() { if time < &next_time { next_time = time.clone(); } }
                if let Some(time) = self.batch_history.time() { if time < &next_time { next_time = time.clone(); } }
                if let Some(time) = self.synth_times.first() { if time < &next_time { next_time = time.clone(); } }
                if let Some(time) = times_slice.first() { if time < &next_time { next_time = time.clone(); } }
                assert!(next_time != T::maximum());

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
                        for &((value, ref time), diff) in self.input_history.buffer.iter() {
                            if time.less_equal(&next_time) {
                                self.input_buffer.push((value, diff));
                            }
                            else {
                                self.temporary.push(next_time.join(time));
                            }
                        }
                        for &((value, ref time), diff) in self.batch_history.buffer.iter() {
                            if time.less_equal(&next_time) {
                                self.input_buffer.push((value, diff));
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
                                self.output_buffer.push(((*value).clone(), -diff));
                            }
                            else {
                                self.temporary.push(next_time.join(time));
                            }
                        }
                        for &((ref value, ref time), diff) in self.output_produced.iter() {
                            if time.less_equal(&next_time) {
                                self.output_buffer.push(((*value).clone(), -diff));
                            }
                            else {
                                self.temporary.push(next_time.join(&time));
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
                meet = T::maximum();
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