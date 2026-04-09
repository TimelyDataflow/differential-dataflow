//! Applies a reduction function on records grouped by key.
//!
//! The `reduce` operator acts on `(key, val)` data.
//! Records with the same key are grouped together, and a user-supplied reduction function is applied
//! to the key and the list of values.
//! The function is expected to populate a list of output values.

use crate::Data;

use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Pipeline;
use timely::scheduling::Scheduler;
use timely::worker::AsWorker;

use crate::operators::arrange::{Arranged, TraceAgent};
use crate::trace::{BatchReader, Cursor, Trace, Builder, ExertionLogic, Description};
use crate::trace::cursor::CursorList;
use crate::trace::implementations::containers::BatchContainer;
use crate::trace::TraceReader;

/// A key-wise reduction of values in an input trace.
///
/// This method exists to provide reduce functionality without opinions about qualifying trace types.
///
/// The `logic` closure is expected to take a key, accumulated input, and tentative accumulated output,
/// and populate its final argument with whatever it feels to be appopriate updates. The behavior and
/// correctness of the implementation rely on this making sense, and e.g. ideally the updates would if
/// applied to the tentative output bring it in line with some function applied to the input.
///
/// The `push` closure is expected to clear its first argument, then populate it with the key and drain
/// the value updates, as appropriate for the container. It is critical that it clear the container as
/// the operator has no ability to do this otherwise, and failing to do so represents a leak from one
/// key's computation to another, and will likely introduce non-determinism.
pub fn reduce_trace<T1, Bu, T2, L, P>(trace: Arranged<T1::Time, T1>, name: &str, mut logic: L, mut push: P) -> Arranged<T1::Time, TraceAgent<T2>>
where
    T1: TraceReader<Time: crate::lattice::Lattice> + Clone + 'static,
    T2: for<'a> Trace<Key<'a>=T1::Key<'a>, ValOwn: Data, Time = T1::Time> + 'static,
    Bu: Builder<Time=T2::Time, Output = T2::Batch, Input: Default>,
    L: FnMut(T1::Key<'_>, &[(T1::Val<'_>, T1::Diff)], &mut Vec<(T2::ValOwn,T2::Diff)>, &mut Vec<(T2::ValOwn, T2::Diff)>)+'static,
    P: FnMut(&mut Bu::Input, T1::Key<'_>, &mut Vec<(T2::ValOwn, T2::Time, T2::Diff)>) + 'static,
{
    let mut result_trace = None;

    // fabricate a data-parallel operator using the `unary_notify` pattern.
    let stream = {

        let result_trace = &mut result_trace;
        let scope = trace.stream.scope();
        trace.stream.unary_frontier(Pipeline, name, move |_capability, operator_info| {

            // Acquire a logger for arrange events.
            let logger = scope.logger_for::<crate::logging::DifferentialEventBuilder>("differential/arrange").map(Into::into);

            let activator = Some(scope.activator_for(operator_info.address.clone()));
            let mut empty = T2::new(operator_info.clone(), logger.clone(), activator);
            // If there is default exert logic set, install it.
            if let Some(exert_logic) = scope.config().get::<ExertionLogic>("differential/default_exert_logic").cloned() {
                empty.set_exert_logic(exert_logic);
            }

            let mut source_trace = trace.trace.clone();

            let (mut output_reader, mut output_writer) = TraceAgent::new(empty, operator_info, logger);

            *result_trace = Some(output_reader.clone());

            let mut new_interesting_times = Vec::<T1::Time>::new();

            // Our implementation maintains a list of outstanding `(key, time)` synthetic interesting times,
            // sorted by (key, time), as well as capabilities for the lower envelope of the times.
            let mut pending_keys = T1::KeyContainer::with_capacity(0);
            let mut pending_time = T1::TimeContainer::with_capacity(0);
            let mut next_pending_keys = T1::KeyContainer::with_capacity(0);
            let mut next_pending_time = T1::TimeContainer::with_capacity(0);
            let mut capabilities = timely::dataflow::operators::CapabilitySet::<T1::Time>::default();

            // buffers and logic for computing per-key interesting times "efficiently".
            let mut interesting_times = Vec::<T1::Time>::new();

            // Upper and lower frontiers for the pending input and output batches to process.
            let mut upper_limit = Antichain::from_elem(<T1::Time as timely::progress::Timestamp>::minimum());
            let mut lower_limit = Antichain::from_elem(<T1::Time as timely::progress::Timestamp>::minimum());

            // Output batches may need to be built piecemeal, and these temp storage help there.
            let mut output_upper = Antichain::from_elem(<T1::Time as timely::progress::Timestamp>::minimum());
            let mut output_lower = Antichain::from_elem(<T1::Time as timely::progress::Timestamp>::minimum());

            move |(input, frontier), output| {

                // The operator receives input batches, which it treats as contiguous and will collect and
                // then process as one batch. It captures the input frontier from the batches, from the upstream
                // trace, and from the input frontier, and retires the work through that interval.
                //
                // Reduce may retain capabilities and need to perform work and produce output at times that
                // may not be seen in its input. The standard example is that updates at `(0, 1)` and `(1, 0)`
                // may result in outputs at `(1, 1)` as well, even with no input at that time.

                let mut batch_cursors = Vec::new();
                let mut batch_storage = Vec::new();

                // Downgrade previous upper limit to be current lower limit.
                lower_limit.clear();
                lower_limit.extend(upper_limit.borrow().iter().cloned());

                // Drain input batches in order, capturing capabilities and the last upper.
                input.for_each(|capability, batches| {
                    capabilities.insert(capability.retain(0));
                    for batch in batches.drain(..) {
                        upper_limit.clone_from(batch.upper());
                        batch_cursors.push(batch.cursor());
                        batch_storage.push(batch);
                    }
                });

                // Pull in any subsequent empty batches we believe to exist.
                source_trace.advance_upper(&mut upper_limit);
                // Incorporate the input frontier guarantees as well.
                let mut joined = Antichain::new();
                crate::lattice::antichain_join_into(&upper_limit.borrow()[..], &frontier.frontier()[..], &mut joined);
                upper_limit = joined;

                // We plan to retire the interval [lower_limit, upper_limit), which should be non-empty to proceed.
                if upper_limit != lower_limit {

                    // If we hold no capabilities in the interval [lower_limit, upper_limit) then we have no compute needs,
                    // and could not transmit the outputs even if they were (incorrectly) non-zero.
                    // We do have maintenance work after this logic, and should not fuse this test with the above test.
                    if capabilities.iter().any(|c| !upper_limit.less_equal(c.time())) {

                        // cursors for navigating input and output traces.
                        let (mut source_cursor, ref source_storage): (T1::Cursor, _) = source_trace.cursor_through(lower_limit.borrow()).expect("failed to acquire source cursor");
                        let (mut output_cursor, ref output_storage): (T2::Cursor, _) = output_reader.cursor_through(lower_limit.borrow()).expect("failed to acquire output cursor");
                        let (mut batch_cursor, ref batch_storage) = (CursorList::new(batch_cursors, &batch_storage), batch_storage);

                        // Prepare an output buffer and builder for each capability.
                        // TODO: It would be better if all updates went into one batch, but timely dataflow prevents
                        //       this as long as it requires that there is only one capability for each message.
                        let mut buffers = Vec::<(T1::Time, Vec<(T2::ValOwn, T1::Time, T2::Diff)>)>::new();
                        let mut builders = Vec::new();
                        for cap in capabilities.iter() {
                            buffers.push((cap.time().clone(), Vec::new()));
                            builders.push(Bu::new());
                        }
                        // Temporary staging for output building.
                        let mut buffer = Bu::Input::default();

                        // Reuseable state for performing the computation.
                        let mut thinker = history_replay::HistoryReplayer::new();

                        // Merge the received batch cursor with our list of interesting (key, time) moments.
                        // The interesting moments need to be in the interval to prompt work.

                        // March through the keys we must work on, merging `batch_cursors` and `exposed`.
                        let mut pending_pos = 0;
                        while batch_cursor.key_valid(batch_storage) || pending_pos < pending_keys.len() {

                            // Determine the next key we will work on; could be synthetic, could be from a batch.
                            let key1 = pending_keys.get(pending_pos);
                            let key2 = batch_cursor.get_key(batch_storage);
                            let key = match (key1, key2) {
                                (Some(key1), Some(key2)) => ::std::cmp::min(key1, key2),
                                (Some(key1), None)       => key1,
                                (None, Some(key2))       => key2,
                                (None, None)             => unreachable!(),
                            };

                            // Populate `interesting_times` with interesting times not beyond `upper_limit`.
                            // TODO: This could just be `pending_time` and indexes within `lower .. upper`.
                            let prior_pos = pending_pos;
                            interesting_times.clear();
                            while pending_keys.get(pending_pos) == Some(key) {
                                let owned_time = T1::owned_time(pending_time.index(pending_pos));
                                if !upper_limit.less_equal(&owned_time) { interesting_times.push(owned_time); }
                                pending_pos += 1;
                            }

                            // tidy up times, removing redundancy.
                            sort_dedup(&mut interesting_times);

                            // If there are new updates, or pending times, we must investigate!
                            if batch_cursor.get_key(batch_storage) == Some(key) || !interesting_times.is_empty() {

                                // do the per-key computation.
                                thinker.compute(
                                    key,
                                    (&mut source_cursor, source_storage),
                                    (&mut output_cursor, output_storage),
                                    (&mut batch_cursor, batch_storage),
                                    &interesting_times,
                                    &mut logic,
                                    &upper_limit,
                                    &mut buffers[..],
                                    &mut new_interesting_times,
                                );

                                // Advance the cursor if this key, so that the loop's validity check registers the work as done.
                                if batch_cursor.get_key(batch_storage) == Some(key) { batch_cursor.step_key(batch_storage); }

                                // Merge novel pending times with any prior pending times we did not process.
                                // TODO: This could be a merge, not a sort_dedup, because both lists should be sorted.
                                for pos in prior_pos .. pending_pos {
                                    let owned_time = T1::owned_time(pending_time.index(pos));
                                    if upper_limit.less_equal(&owned_time) { new_interesting_times.push(owned_time); }
                                }
                                sort_dedup(&mut new_interesting_times);
                                for time in new_interesting_times.drain(..) {
                                    next_pending_keys.push_ref(key);
                                    next_pending_time.push_own(&time);
                                }

                                // Sort each buffer by value and move into the corresponding builder.
                                // TODO: This makes assumptions about at least one of (i) the stability of `sort_by`,
                                //       (ii) that the buffers are time-ordered, and (iii) that the builders accept
                                //       arbitrarily ordered times.
                                for index in 0 .. buffers.len() {
                                    buffers[index].1.sort_by(|x,y| x.0.cmp(&y.0));
                                    push(&mut buffer, key, &mut buffers[index].1);
                                    buffers[index].1.clear();
                                    builders[index].push(&mut buffer);

                                }
                            }
                            else {
                                // copy over the pending key and times.
                                for pos in prior_pos .. pending_pos {
                                    next_pending_keys.push_ref(pending_keys.index(pos));
                                    next_pending_time.push_ref(pending_time.index(pos));
                                }
                            }
                        }
                        // Drop to avoid lifetime issues that would lock `pending_{keys, time}`.
                        drop(thinker);

                        // We start sealing output batches from the lower limit (previous upper limit).
                        // In principle, we could update `lower_limit` itself, and it should arrive at
                        // `upper_limit` by the end of the process.
                        output_lower.clear();
                        output_lower.extend(lower_limit.borrow().iter().cloned());

                        // build and ship each batch (because only one capability per message).
                        for (index, builder) in builders.drain(..).enumerate() {

                            // Form the upper limit of the next batch, which includes all times greater
                            // than the input batch, or the capabilities from i + 1 onward.
                            output_upper.clear();
                            output_upper.extend(upper_limit.borrow().iter().cloned());
                            for capability in &capabilities[index + 1 ..] {
                                output_upper.insert_ref(capability.time());
                            }

                            if output_upper.borrow() != output_lower.borrow() {

                                let description = Description::new(output_lower.clone(), output_upper.clone(), Antichain::from_elem(T1::Time::minimum()));
                                let batch = builder.done(description);

                                // ship batch to the output, and commit to the output trace.
                                output.session(&capabilities[index]).give(batch.clone());
                                output_writer.insert(batch, Some(capabilities[index].time().clone()));

                                output_lower.clear();
                                output_lower.extend(output_upper.borrow().iter().cloned());
                            }
                        }
                        // This should be true, as the final iteration introduces no capabilities, and
                        // uses exactly `upper_limit` to determine the upper bound. Good to check though.
                        assert!(output_upper.borrow() == upper_limit.borrow());

                        // Refresh pending keys and times, then downgrade capabilities to the frontier of times.
                        pending_keys.clear(); std::mem::swap(&mut next_pending_keys, &mut pending_keys);
                        pending_time.clear(); std::mem::swap(&mut next_pending_time, &mut pending_time);

                        // Update `capabilities` to reflect pending times.
                        let mut frontier = Antichain::<T1::Time>::new();
                        let mut owned_time = T1::Time::minimum();
                        for pos in 0 .. pending_time.len() {
                            T1::clone_time_onto(pending_time.index(pos), &mut owned_time);
                            frontier.insert_ref(&owned_time);
                        }
                        capabilities.downgrade(frontier);
                    }

                    // ensure that observed progress is reflected in the output.
                    output_writer.seal(upper_limit.clone());

                    // We only anticipate future times in advance of `upper_limit`.
                    source_trace.set_logical_compaction(upper_limit.borrow());
                    output_reader.set_logical_compaction(upper_limit.borrow());

                    // We will only slice the data between future batches.
                    source_trace.set_physical_compaction(upper_limit.borrow());
                    output_reader.set_physical_compaction(upper_limit.borrow());
                }

                // Exert trace maintenance if we have been so requested.
                output_writer.exert();
            }
        }
    )
    };

    Arranged { stream, trace: result_trace.unwrap() }
}


#[inline(never)]
fn sort_dedup<T: Ord>(list: &mut Vec<T>) {
    list.dedup();
    list.sort();
    list.dedup();
}

/// Implementation based on replaying historical and new updates together.
mod history_replay {

    use timely::progress::Antichain;
    use timely::PartialOrder;

    use crate::lattice::Lattice;
    use crate::trace::Cursor;
    use crate::operators::ValueHistory;

    use super::sort_dedup;

    /// The `HistoryReplayer` is a compute strategy based on moving through existing inputs, interesting times, etc in
    /// time order, maintaining consolidated representations of updates with respect to future interesting times.
    pub struct HistoryReplayer<'a, C1, C2, C3, V>
    where
        C1: Cursor,
        C2: Cursor<Key<'a> = C1::Key<'a>, Time = C1::Time>,
        C3: Cursor<Key<'a> = C1::Key<'a>, Val<'a> = C1::Val<'a>, Time = C1::Time, Diff = C1::Diff>,
        V: Clone + Ord,
    {
        input_history: ValueHistory<'a, C1>,
        output_history: ValueHistory<'a, C2>,
        batch_history: ValueHistory<'a, C3>,
        input_buffer: Vec<(C1::Val<'a>, C1::Diff)>,
        output_buffer: Vec<(V, C2::Diff)>,
        update_buffer: Vec<(V, C2::Diff)>,
        output_produced: Vec<((V, C2::Time), C2::Diff)>,
        synth_times: Vec<C1::Time>,
        meets: Vec<C1::Time>,
        times_current: Vec<C1::Time>,
        temporary: Vec<C1::Time>,
    }

    impl<'a, C1, C2, C3, V> HistoryReplayer<'a, C1, C2, C3, V>
    where
        C1: Cursor,
        C2: for<'b> Cursor<Key<'a> = C1::Key<'a>, ValOwn = V, Time = C1::Time>,
        C3: Cursor<Key<'a> = C1::Key<'a>, Val<'a> = C1::Val<'a>, Time = C1::Time, Diff = C1::Diff>,
        V: Clone + Ord,
    {
        pub fn new() -> Self {
            HistoryReplayer {
                input_history: ValueHistory::new(),
                output_history: ValueHistory::new(),
                batch_history: ValueHistory::new(),
                input_buffer: Vec::new(),
                output_buffer: Vec::new(),
                update_buffer: Vec::new(),
                output_produced: Vec::new(),
                synth_times: Vec::new(),
                meets: Vec::new(),
                times_current: Vec::new(),
                temporary: Vec::new(),
            }
        }
        #[inline(never)]
        pub fn compute<L>(
            &mut self,
            key: C1::Key<'a>,
            (source_cursor, source_storage): (&mut C1, &'a C1::Storage),
            (output_cursor, output_storage): (&mut C2, &'a C2::Storage),
            (batch_cursor, batch_storage): (&mut C3, &'a C3::Storage),
            times: &Vec<C1::Time>,
            logic: &mut L,
            upper_limit: &Antichain<C1::Time>,
            outputs: &mut [(C2::Time, Vec<(V, C2::Time, C2::Diff)>)],
            new_interesting: &mut Vec<C1::Time>)
        where
            L: FnMut(
                C1::Key<'a>,
                &[(C1::Val<'a>, C1::Diff)],
                &mut Vec<(V, C2::Diff)>,
                &mut Vec<(V, C2::Diff)>,
            )
        {

            // The work we need to perform is at times defined principally by the contents of `batch_cursor`
            // and `times`, respectively "new work we just received" and "old times we were warned about".
            //
            // Our first step is to identify these times, so that we can use them to restrict the amount of
            // information we need to recover from `input` and `output`; as all times of interest will have
            // some time from `batch_cursor` or `times`, we can compute their meet and advance all other
            // loaded times by performing the lattice `join` with this value.

            // Load the batch contents.
            let mut batch_replay = self.batch_history.replay_key(batch_cursor, batch_storage, key, |time| C3::owned_time(time));

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
            let mut meet = None;
            update_meet(&mut meet, self.meets.get(0));
            update_meet(&mut meet, batch_replay.meet());

            // Having determined the meet, we can load the input and output histories, where we
            // advance all times by joining them with `meet`. The resulting times are more compact
            // and guaranteed to accumulate identically for times greater or equal to `meet`.

            // Load the input and output histories.
            let mut input_replay =
            self.input_history.replay_key(source_cursor, source_storage, key, |time| {
                let mut time = C1::owned_time(time);
                if let Some(meet) = meet.as_ref() { time.join_assign(meet); }
                time
            });
            let mut output_replay =
            self.output_history.replay_key(output_cursor, output_storage, key, |time| {
                let mut time = C2::owned_time(time);
                if let Some(meet) = meet.as_ref() { time.join_assign(meet); }
                time
            });

            self.synth_times.clear();
            self.times_current.clear();
            self.output_produced.clear();

            // The frontier of times we may still consider.
            // Derived from frontiers of our update histories, supplied times, and synthetic times.

            let mut times_slice = &times[..];
            let mut meets_slice = &self.meets[..];

            // We have candidate times from `batch` and `times`, as well as times identified by either
            // `input` or `output`. Finally, we may have synthetic times produced as the join of times
            // we consider in the course of evaluation. As long as any of these times exist, we need to
            // keep examining times.
            while let Some(next_time) = [   batch_replay.time(),
                                            times_slice.first(),
                                            input_replay.time(),
                                            output_replay.time(),
                                            self.synth_times.last(),
                                        ].into_iter().flatten().min().cloned() {

                // Advance input and output history replayers. This marks applicable updates as active.
                input_replay.step_while_time_is(&next_time);
                output_replay.step_while_time_is(&next_time);

                // One of our goals is to determine if `next_time` is "interesting", meaning whether we
                // have any evidence that we should re-evaluate the user logic at this time. For a time
                // to be "interesting" it would need to be the join of times that include either a time
                // from `batch`, `times`, or `synth`. Neither `input` nor `output` times are sufficient.

                // Advance batch history, and capture whether an update exists at `next_time`.
                let mut interesting = batch_replay.step_while_time_is(&next_time);
                if interesting { if let Some(meet) = meet.as_ref() { batch_replay.advance_buffer_by(meet); } }

                // advance both `synth_times` and `times_slice`, marking this time interesting if in either.
                while self.synth_times.last() == Some(&next_time) {
                    // We don't know enough about `next_time` to avoid putting it in to `times_current`.
                    // TODO: If we knew that the time derived from a canceled batch update, we could remove the time.
                    self.times_current.push(self.synth_times.pop().expect("failed to pop from synth_times")); // <-- TODO: this could be a min-heap.
                    interesting = true;
                }
                while times_slice.first() == Some(&next_time) {
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
                //       yield false positives if not sufficiently compact. Maybe we should look into this and see.
                interesting = interesting || batch_replay.buffer().iter().any(|&((_, ref t),_)| t.less_equal(&next_time));
                interesting = interesting || self.times_current.iter().any(|t| t.less_equal(&next_time));

                // We should only process times that are not in advance of `upper_limit`.
                //
                // We have no particular guarantee that known times will not be in advance of `upper_limit`.
                // We may have the guarantee that synthetic times will not be, as we test against the limit
                // before we add the time to `synth_times`.
                if !upper_limit.less_equal(&next_time) {

                    // We should re-evaluate the computation if this is an interesting time.
                    // If the time is uninteresting (and our logic is sound) it is not possible for there to be
                    // output produced. This sounds like a good test to have for debug builds!
                    if interesting {

                        // Assemble the input collection at `next_time`. (`self.input_buffer` cleared just after use).
                        debug_assert!(self.input_buffer.is_empty());
                        if let Some(meet) = meet.as_ref() { input_replay.advance_buffer_by(meet) };
                        for ((value, time), diff) in input_replay.buffer().iter() {
                            if time.less_equal(&next_time) { self.input_buffer.push((*value, diff.clone())); }
                            else { self.temporary.push(next_time.join(time)); }
                        }
                        for ((value, time), diff) in batch_replay.buffer().iter() {
                            if time.less_equal(&next_time) { self.input_buffer.push((*value, diff.clone())); }
                            else { self.temporary.push(next_time.join(time)); }
                        }
                        crate::consolidation::consolidate(&mut self.input_buffer);

                        // Assemble the output collection at `next_time`. (`self.output_buffer` cleared just after use).
                        if let Some(meet) = meet.as_ref() { output_replay.advance_buffer_by(meet) };
                        for ((value, time), diff) in output_replay.buffer().iter() {
                            if time.less_equal(&next_time) { self.output_buffer.push((C2::owned_val(*value), diff.clone())); }
                            else { self.temporary.push(next_time.join(time)); }
                        }
                        for ((value, time), diff) in self.output_produced.iter() {
                            if time.less_equal(&next_time) { self.output_buffer.push(((*value).to_owned(), diff.clone())); }
                            else { self.temporary.push(next_time.join(time)); }
                        }
                        crate::consolidation::consolidate(&mut self.output_buffer);

                        // Apply user logic if non-empty input or output and see what happens!
                        if !self.input_buffer.is_empty() || !self.output_buffer.is_empty() {
                            logic(key, &self.input_buffer[..], &mut self.output_buffer, &mut self.update_buffer);
                            self.input_buffer.clear();
                            self.output_buffer.clear();

                            // Having subtracted output updates from user output, consolidate the results to determine
                            // if there is anything worth reporting. Note: this also orders the results by value, so
                            // that could make the above merging plan even easier.
                            //
                            // Stash produced updates into both capability-indexed buffers and `output_produced`.
                            // The two locations are important, in that we will compact `output_produced` as we move
                            // through times, but we cannot compact the output buffers because we need their actual
                            // times.
                            crate::consolidation::consolidate(&mut self.update_buffer);
                            if !self.update_buffer.is_empty() {

                                // We *should* be able to find a capability for `next_time`. Any thing else would
                                // indicate a logical error somewhere along the way; either we release a capability
                                // we should have kept, or we have computed the output incorrectly (or both!)
                                let idx = outputs.iter().rev().position(|(time, _)| time.less_equal(&next_time));
                                let idx = outputs.len() - idx.expect("failed to find index") - 1;
                                for (val, diff) in self.update_buffer.drain(..) {
                                    self.output_produced.push(((val.clone(), next_time.clone()), diff.clone()));
                                    outputs[idx].1.push((val, next_time.clone(), diff));
                                }

                                // Advance times in `self.output_produced` and consolidate the representation.
                                // NOTE: We only do this when we add records; it could be that there are situations
                                //       where we want to consolidate even without changes (because an initially
                                //       large collection can now be collapsed).
                                if let Some(meet) = meet.as_ref() { for entry in &mut self.output_produced { (entry.0).1.join_assign(meet); } }
                                crate::consolidation::consolidate(&mut self.output_produced);
                            }
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
                    self.temporary.extend(batch_replay.buffer().iter().map(|((_,time),_)| time).filter(|time| !time.less_equal(&next_time)).map(|time| time.join(&next_time)));
                    self.temporary.extend(self.times_current.iter().filter(|time| !time.less_equal(&next_time)).map(|time| time.join(&next_time)));
                    sort_dedup(&mut self.temporary);

                    // Introduce synthetic times, and re-organize if we add any.
                    let synth_len = self.synth_times.len();
                    for time in self.temporary.drain(..) {
                        // We can either service `join` now, or must delay for the future.
                        if upper_limit.less_equal(&time) {
                            debug_assert!(outputs.iter().any(|(t,_)| t.less_equal(&time)));
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
                else if interesting {
                    // We cannot process `next_time` now, and must delay it.
                    //
                    // I think we are probably only here because of an uninteresting time declared interesting,
                    // as initial interesting times are filtered to be in interval, and synthetic times are also
                    // filtered before introducing them to `self.synth_times`.
                    new_interesting.push(next_time.clone());
                    debug_assert!(outputs.iter().any(|(t,_)| t.less_equal(&next_time)))
                }

                // Update `meet` to track the meet of each source of times.
                meet = None;
                update_meet(&mut meet, batch_replay.meet());
                update_meet(&mut meet, input_replay.meet());
                update_meet(&mut meet, output_replay.meet());
                for time in self.synth_times.iter() { update_meet(&mut meet, Some(time)); }
                update_meet(&mut meet, meets_slice.first());

                // Update `times_current` by the frontier.
                if let Some(meet) = meet.as_ref() {
                    for time in self.times_current.iter_mut() {
                        *time = time.join(meet);
                    }
                }

                sort_dedup(&mut self.times_current);
            }

            // Normalize the representation of `new_interesting`, deduplicating and ordering.
            sort_dedup(new_interesting);
        }
    }

    /// Updates an optional meet by an optional time.
    fn update_meet<T: Lattice+Clone>(meet: &mut Option<T>, other: Option<&T>) {
        if let Some(time) = other {
            if let Some(meet) = meet.as_mut() { meet.meet_assign(time); }
            else { *meet = Some(time.clone()); }
        }
    }
}
