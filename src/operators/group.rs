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
use std::cmp::Ordering;

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
use trace::implementations::hash::HashValSpine as DefaultValTrace;
use trace::implementations::hash::HashKeySpine as DefaultKeyTrace;

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
            .group_arranged(|_k,_s,t| { 
                if _s[0].1 < 1 {
                    panic!("key: {:?}, count: {:?}", _k, _s[0].1);
                    // assert!(_s[0].1 > 0 ); 
                }
                t.push(((), 1)); 
            }, DefaultKeyTrace::new())
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
        let mut output_trace = TraceHandle::new(empty, &[<G::Timestamp as Lattice>::min()], &[<G::Timestamp as Lattice>::min()]);
        let result_trace = output_trace.clone();

        let mut thinker2 = InterestAccumulator::<V, V2, G::Timestamp, R, R2>::new();
        // let mut thinker2 = HistoryReplayer::<V, V2, G::Timestamp, R, R2>::new();
        let mut temporary = Vec::<G::Timestamp>::new();

        // Our implementation maintains a list of outstanding `(key, time)` synthetic interesting times, 
        // as well as capabilities for these times (or their lower envelope, at least).
        let mut interesting = Vec::<(K, G::Timestamp)>::new();
        let mut capabilities = Vec::<Capability<G::Timestamp>>::new();

        // buffers and logic for computing per-key interesting times "efficiently".
        let mut interesting_times = Vec::<G::Timestamp>::new();

        // tracks frontiers received from batches, for sanity.
        let mut lower_sanity = vec![<G::Timestamp as Lattice>::min()];
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
                let mut buffers = Vec::<(G::Timestamp, Vec<(V2, G::Timestamp, R2)>)>::new();
                let mut builders = Vec::new();
                for i in 0 .. capabilities.len() {
                    buffers.push((capabilities[i].time(), Vec::new()));
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
                        if key == None { 
                            key = Some(batch_cursor.key().clone()); 
                        }
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

                    // let mut interesting_times2 = interesting_times.clone();
                    // let mut buffers2 = buffers.clone();

                    // do the per-key computation.
                    temporary.clear();
                    thinker2.compute(
                        &key, 
                        &mut source_cursor, 
                        &mut output_cursor, 
                        &mut interesting_times, 
                        &logic, 
                        &upper_limit[..], 
                        &mut buffers[..], 
                        &mut temporary,
                    );


                    // source_cursor.rewind_vals();
                    // output_cursor.rewind_vals();
                    // // do the per-key computation.
                    // let mut temp2 = Vec::new();
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

                    // assert_eq!(buffers, buffers2);
                    // assert_eq!(temporary, temp2);

                    for time in temporary.drain(..) { 
                        assert!(upper_limit.iter().any(|t| t.le(&time)));
                        interesting.push((key.clone(), time)); 
                    }

                    // move all updates for this key into corresponding builders.
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

                    let batch = builder.done(&lower_issued[..], &local_upper[..], &lower_issued[..]);
                    lower_issued = local_upper;

                    output.session(&capabilities[index]).give(BatchWrapper { item: batch.clone() });
                    output_trace.wrapper.borrow_mut().trace.insert(batch);
                }

                assert!(lower_issued == upper_limit);

                // update capabilities to reflect `interesting` pairs.
                let mut frontier = Vec::<G::Timestamp>::new();
                for &(_, ref time) in &interesting {                    
                    if !frontier.iter().any(|t| t.le(time)) {
                        frontier.retain(|t| !time.lt(t));
                        frontier.push(time.clone());
                    }
                }

                // update capabilities (readable?)
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
fn sort_dedup_c<T: Ord>(list: &mut Vec<T>) {
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
    fn compute<K, C1, C2, L>(
        &mut self,
        key: &K, 
        input: &mut C1, 
        output: &mut C2, 
        times: &mut Vec<T>, 
        logic: &L, 
        upper_limit: &[T],
        outputs: &mut [(T, Vec<(V2, T, R2)>)],
        new_interesting: &mut Vec<T>) 
    where 
        K: Eq+Clone+Debug,
        C1: Cursor<K, V1, T, R1>, 
        C2: Cursor<K, V2, T, R2>, 
        L: Fn(&K, &[(V1, R1)], &mut Vec<(V2, R2)>);
}


/// The `HistoryReplayer` is a compute strategy based on moving through existing inputs, interesting times, etc in 
/// time order, maintaining consolidated representations of updates with respect to future interesting times.
struct HistoryReplayer<V1, V2, T, R1, R2> 
where
    V1: Ord+Clone,
    V2: Ord+Clone,
    T: Lattice+Ord+Clone,
    R1: Ring,
    R2: Ring,
{
    input_history: CollectionHistory<V1, T, R1>,
    input_actions: Vec<(T, usize)>,
    output_history: CollectionHistory<V2, T, R2>,
    output_actions: Vec<(T, usize)>,
    input_buffer: Vec<(V1, R1)>,
    output_buffer: Vec<(V2, R2)>,
    output_produced: Vec<((V2, T), R2)>,
    known_times: Vec<T>,
    synth_times: Vec<T>,
    meets: Vec<T>,
    times_current: Vec<T>,
    lower: Vec<T>,
}

impl<V1, V2, T, R1, R2> HistoryReplayer<V1, V2, T, R1, R2> 
where
    V1: Ord+Clone+Debug,
    V2: Ord+Clone+Debug,
    T: Lattice+Ord+Clone+Debug,
    R1: Ring+Debug,
    R2: Ring+Debug,
{
    #[inline(never)]
    fn build_input_history<K, C1>(&mut self, key: &K, source_cursor: &mut C1, meet: &T, _upper_limit: &[T]) 
    where K: Eq+Clone+Debug, C1: Cursor<K, V1, T, R1>, {

        self.input_history.clear();
        self.input_actions.clear();

        source_cursor.seek_key(&key);
        if source_cursor.key_valid() && source_cursor.key() == key {
            while source_cursor.val_valid() {
                let start = self.input_history.times.len();
                source_cursor.map_times(|t, d| {
                    // if format!("{:?}", key) == "OrdWrapper { item: 0 }".to_owned() {
                    //     println!("key 0 input:\t{:?}, {:?}", t, d);
                    // }

                    let join = t.join(&meet);
                    if _upper_limit.iter().any(|t| t.le(&join)) {
                        if !self.known_times.iter().any(|t| t.le(&join)) {
                            self.known_times.retain(|t| !join.le(t));
                            self.known_times.push(join);
                        }
                    }
                    else {
                        self.input_history.times.push((join, d));
                    }
                });
                self.input_history.seal_from(source_cursor.val().clone(), start);
                source_cursor.step_val();
            }
        }

        self.input_actions.reserve(self.input_history.times.len());
        for (index, history) in self.input_history.values.iter().enumerate() {
            for offset in history.lower .. history.upper {
                // if format!("{:?}", key) == "OrdWrapper { item: 0 }".to_owned() {
                //     println!("key 0 input:\t{:?}, {:?}, {:?}", self.input_history.times[offset].0, self.input_history.values[index].value, self.input_history.times[offset].1);
                // }
                self.input_actions.push((self.input_history.times[offset].0.clone(), index));
            }
        }

        // TODO: this could have been a merge; helpful if few values. (perhaps it is with mergesort!)
        self.sort_input_actions();
    }

    #[inline(never)]
    fn sort_input_actions(&mut self) {
        self.input_actions.sort_by(|x,y| x.0.cmp(&y.0));
    }

    #[inline(never)]
    fn build_output_history<K, C2>(&mut self, key: &K, output_cursor: &mut C2, meet: &T, _upper_limit: &[T]) 
    where K: Eq+Clone+Debug, C2: Cursor<K, V2, T, R2>, {

        self.output_history.clear();
        self.output_actions.clear();

        output_cursor.seek_key(&key);
        if output_cursor.key_valid() && output_cursor.key() == key {
            while output_cursor.val_valid() {
                let start = self.output_history.times.len();
                output_cursor.map_times(|t, d| {

                    // if format!("{:?}", key) == "OrdWrapper { item: 0 }".to_owned() {
                    //     println!("key 0 output:\t{:?}, {:?}", t, d);
                    // }

                    let join = t.join(&meet);
                    if _upper_limit.iter().any(|t| t.le(&join)) {
                        if !self.known_times.iter().any(|t| t.le(&join)) {
                            self.known_times.retain(|t| !join.le(t));
                            self.known_times.push(join);
                        }
                    }
                    else {
                        self.output_history.times.push((join, d));
                    }
                });
                self.output_history.seal_from(output_cursor.val().clone(), start);
                output_cursor.step_val();
            }
        }

        self.output_actions.reserve(self.output_history.times.len());
        for (index, history) in self.output_history.values.iter().enumerate() {
            for offset in history.lower .. history.upper {
                // if format!("{:?}", key) == "OrdWrapper { item: 0 }".to_owned() {
                //     println!("key 0 output:\t{:?}, {:?}, {:?}", self.output_history.times[offset].0, self.output_history.values[index].value, self.output_history.times[offset].1);
                // }
                self.output_actions.push((self.output_history.times[offset].0.clone(), index));
            }
        }

        // TODO: this could have been a merge; helpful if few values. (perhaps it is with mergesort!)
        self.output_actions.sort_by(|x,y| x.0.cmp(&y.0)); 

        // println!("{:?}", key);
        // if format!("{:?}", key) == "OrdWrapper { item: 0 }".to_owned() {
        //     println!("hey: {:?}", meet);
        // }
    }
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
            input_actions: Vec::new(),
            output_history: CollectionHistory::new(),
            output_actions: Vec::new(),
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
    fn compute<K, C1, C2, L>(
        &mut self,
        key: &K, 
        source_cursor: &mut C1, 
        output_cursor: &mut C2, 
        times: &mut Vec<T>, 
        logic: &L, 
        upper_limit: &[T],
        outputs: &mut [(T, Vec<(V2, T, R2)>)],
        new_interesting: &mut Vec<T>) 
    where 
        K: Eq+Clone+Debug,
        C1: Cursor<K, V1, T, R1>, 
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

        // experimenting with meet vs frontiers for advancing (good for distributive lattices).
        // could also just use `lower` for advancing if we find it isn't too painful.
        let mut meet = self.lower.iter().fold(T::max(), |meet, time| meet.meet(time));

        self.known_times.clear();

        // The fast-forwarding we are about to do with input and output can result in very weird looking
        // data. In particular, we can have "old" updates in the future of "new" updates, and even beyond
        // the frontier of work we can do. We should not take their times as indications of anything other
        // than whether they should be included in accumulations.
        //
        // We extract input and output updates into flat value-indexed arrays, each range of which we sort
        // by time so that the history for a value is always a prefix of its range. As we proceed through 
        // time we extend the valid ranges.

        self.build_input_history(key, source_cursor, &meet, upper_limit);
        self.build_output_history(key, output_cursor, &meet, upper_limit);

        // TODO: We should be able to thin out any updates at times that, advanced, are greater than some
        //       element of `upper_limit`, as we will never incorporate that update. We should take care 
        //       to notice these times, if we need to report them as interesting times, but once we have 
        //       done that we can ditch the updates.

        // TODO: We should be able to restrict our attention down to just those times that are the joins
        //       of times evident in the input and output. This could be useful in cases like `distinct`,
        //       where the input updates collapse down to relatively fewer distinct moments.

        self.synth_times.clear();
        self.times_current.clear();
        self.output_produced.clear();

        {   // populate `self.known_times` with times from the input, output, and the `times` argument.
            
            // TODO: This could be a merge (perhaps it is, if we use mergesort).
            let mut input_slice = &self.input_actions[..];
            let mut output_slice = &self.output_actions[..];

            while input_slice.len() > 0 || output_slice.len() > 0 {
                let mut next = T::max();
                if input_slice.len() > 0 && input_slice[0].0.cmp(&next) == Ordering::Less {
                    next = input_slice[0].0.clone();
                }
                if output_slice.len() > 0 && output_slice[0].0.cmp(&next) == Ordering::Less {
                    next = output_slice[0].0.clone();
                }


                while input_slice.len() > 0 && input_slice[0].0 == next {
                    input_slice = &input_slice[1..];
                }
                while output_slice.len() > 0 && output_slice[0].0 == next {
                    output_slice = &output_slice[1..];
                }

                self.known_times.push(next);
            }

            // for &(ref time, _) in &self.input_actions { self.known_times.push(time.clone()); }
            // for &(ref time, _) in &self.output_actions { self.known_times.push(time.clone()); }

            sort_dedup_c(&mut self.known_times);
        }

        {   // populate `self.meets` with the meets of suffixes of `self.known_times`.

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
        let mut input_slice = &self.input_actions[..];
        let mut output_slice = &self.output_actions[..];

        let mut known_slice = &self.known_times[..];
        let mut meets_slice = &self.meets[..];

        while known_slice.len() > 0 || self.synth_times.len() > 0 {

            // determine the next time to process.
            let mut next_time = T::max();
            if known_slice.len() > 0 && next_time.cmp(&known_slice[0]) == Ordering::Greater {
                next_time = known_slice[0].clone();
            }
            if self.synth_times.len() > 0 && next_time.cmp(&self.synth_times[0]) == Ordering::Greater {
                next_time = self.synth_times[0].clone();
            }

            // advance `known_slice` and `synth_times` as appropriate.
            while known_slice.len() > 0 && known_slice[0] == next_time {
                known_slice = &known_slice[1..];
                meets_slice = &meets_slice[1..];
            }
            while self.synth_times.len() > 0 && self.synth_times[0] == next_time {
                self.synth_times.remove(0); // <-- this should really be a min-heap.
            }

            // advance valid ranges of inputs and outputs for this time.
            while input_slice.len() > 0 && input_slice[0].0 == next_time {
                let value_index = input_slice[0].1;
                self.input_history.step(value_index);
                input_slice = &input_slice[1..];
            }
            while output_slice.len() > 0 && output_slice[0].0 == next_time {
                let value_index = output_slice[0].1;
                self.output_history.step(value_index);
                output_slice = &output_slice[1..];
            }

            // we should only process times that are not in the future.
            if !upper_limit.iter().any(|t| t.le(&next_time)) {

                // assemble input collection (`self.input_buffer` cleared just after use).
                debug_assert!(self.input_buffer.is_empty());
                self.input_history.insert(&next_time, &meet, &mut self.input_buffer);

                // apply user logic and see what happens!
                self.output_buffer.clear();
                if self.input_buffer.len() > 0 {
                    logic(key, &self.input_buffer[..], &mut self.output_buffer);
                    self.input_buffer.clear();
                }
        
                // spill output differences into `self.output_buffer`. 
                // this could probably be done much better, but we currently produce new output
                // updates that we cannot (easily) pack into `self.output_history`. so for now we
                // just have a big pile of output updates and hope that it is painful enough to fix.

                // subtracts pre-existing output updates maintained in `self.output_history`.
                self.output_history.remove(&next_time, &meet, &mut self.output_buffer);
                // subtracts newly formed output updates maintained in `self.output_produced`.
                for &((ref value, ref time), diff) in self.output_produced.iter() {
                    if time.le(&next_time) {
                        self.output_buffer.push((value.clone(), -diff));
                    }
                }

                // we can't rely on user code to do this (plus it's presently all a mess anyhow).
                consolidate(&mut self.output_buffer);

                // stash produced updates in capability-indexed buffers, and `output_updates`.
                if self.output_buffer.len() > 0 {

                    // any times not greater than `lower` must be empty (and should be skipped, but testing!)
                    assert!(self.lower.iter().any(|t| t.le(&next_time)));

                    let idx = outputs.iter().rev().position(|&(ref time, _)| time.le(&next_time));
                    let idx = outputs.len() - idx.unwrap() - 1;
                    for (val, diff) in self.output_buffer.drain(..) {
                        self.output_produced.push(((val.clone(), next_time.clone()), diff));
                        outputs[idx].1.push((val, next_time.clone(), diff));
                    }

                    // consolidate `self.output_produced`.
                    for entry in &mut self.output_produced {
                        (entry.0).1 = (entry.0).1.join(&meet);
                    }
                    consolidate(&mut self.output_produced);

                }

                // determine synthetic interesting times!
                // could just join `next_time` with all times in `input_updates` and `output_updates`.
                // could also just retire to some beach and drink a lot. that is not why we are here!

                for time in &self.times_current {
                    let join = next_time.join(time);
                    if join != next_time {
                        // enqueue `join` if not beyond `upper_limit`; else add to `new_interesting` frontier.
                        if !upper_limit.iter().any(|t| t.le(&join)) {
                            self.synth_times.push(join); 
                        }
                        else {
                            if outputs.iter().any(|&(ref t,_)| t.le(&join)) {
                                if !new_interesting.iter().any(|t| t.le(&join)) { 
                                    new_interesting.retain(|t| !join.lt(t));
                                    // if !outputs.iter().any(|&(ref t,_)| t.le(&join)) {
                                    //     panic!("warning about time w/o capability");
                                    // }
                                    new_interesting.push(join);
                                }                        
                            }
                        }
                    }
                }
            }
            else {  
                // otherwise we delay the time for the future (and warn about it)
                if outputs.iter().any(|&(ref t,_)| t.le(&next_time)) {
                    if !new_interesting.iter().any(|t| t.le(&next_time)) { 
                        new_interesting.retain(|t| !next_time.lt(t));
                        new_interesting.push(next_time.clone());
                        // if !outputs.iter().any(|&(ref t,_)| t.le(&next_time)) {
                        //     panic!("warning about time w/o capability");
                        // }
                    }
                }
            }

            self.times_current.push(next_time);

            // update our view of the meet of remaining times. 
            // NOTE: this does not advance collections, which can be done value-by-value as appropriate.
            if meets_slice.len() > 0 || self.synth_times.len() > 0 {
                meet = self.synth_times.iter().fold(T::max(), |meet, time| meet.meet(time));
                if meets_slice.len() > 0 { meet = meet.meet(&meets_slice[0]); }
                // for time in &self.synth_times { meet = meet.meet(time); }

                for time in &mut self.times_current {
                    *time = time.join(&meet);
                }
                sort_dedup_a(&mut self.times_current);
            }

            // again, this should be a min-heap.
            sort_dedup_b(&mut self.synth_times);
        }

        new_interesting.sort();
        for index in 1 .. new_interesting.len() {
            debug_assert!(new_interesting[index-1].cmp(&new_interesting[index]) == Ordering::Less);
        }

        // println!("input_diffs: {:?}, output_diffs: {:?}", self.input_actions.len(), self.output_actions.len());
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
    pub times: Vec<(T, R)>,
}

impl<V: Clone, T: Lattice+Ord+Clone+Debug, R: Ring> CollectionHistory<V, T, R> {
    fn new() -> Self { 
        CollectionHistory {
            values: Vec::new(),
            times: Vec::new(),
        }
    }
    fn clear(&mut self) {
        self.values.clear();
        self.times.clear();
    }
    /// Advances the indexed value by one, with the ability to compact times by `meet`.
    fn step(&mut self, value_index: usize) {
        // we should not have run out of updates to incorporate.
        debug_assert!(self.values[value_index].valid < self.values[value_index].upper);        
        self.values[value_index].valid += 1;
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

















struct InterestAccumulator<V1, V2, T, R1, R2>
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
    fn compute<K, C1, C2, L>(
        &mut self,
        key: &K, 
        source_cursor: &mut C1, 
        output_cursor: &mut C2, 
        interesting_times: &mut Vec<T>, 
        logic: &L,
        upper_limit: &[T],
        outputs: &mut [(T, Vec<(V2, T, R2)>)],
        new_interesting: &mut Vec<T>)
    where 
        K: Eq+Clone+Debug,
        C1: Cursor<K, V1, T, R1>, 
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