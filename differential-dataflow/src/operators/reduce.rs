//! Applies a reduction function on records grouped by key.
//!
//! The `reduce` operator acts on `(key, val)` data.
//! Records with the same key are grouped together, and a user-supplied reduction function is applied
//! to the key and the list of values.
//! The function is expected to populate a list of output values.

use crate::Data;

use std::marker::PhantomData;

use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::channels::pact::Pipeline;

use crate::operators::arrange::{Arranged, TraceAgent};
use crate::trace::{BatchCursor, BatchDiff, BatchKey, BatchReader, BatchVal, BatchValOwn, Builder, Cursor, Description, ExertionLogic, Navigable, Trace, TraceReader};
use crate::trace::cursor::cursor_list;
use crate::trace::implementations::containers::BatchContainer;

/// Reduce phase-split instrumentation (debug builds only): tightness counters for the two-pass reduce.
///
/// Pass 1 (`discover_times`) determines interesting times; pass 2 (`evaluate_times`) evaluates them.
/// `discovered` counts the times pass 1 enumerates; `logic`/`produced` count how many of those actually
/// invoke user logic and yield a non-empty diff. A test calls [`reset`](audit::reset) then reads
/// [`snapshot`](audit::snapshot). Output correctness is covered by `tests/scc.rs` (differential vs.
/// sequential) and `tests/reduce.rs`. Removed once the split is settled.
#[cfg(debug_assertions)]
pub mod audit {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    static DISCOVERED: AtomicU64 = AtomicU64::new(0);
    static LOGIC: AtomicU64 = AtomicU64::new(0);
    static PRODUCED: AtomicU64 = AtomicU64::new(0);
    pub(super) fn record_discovered(n: u64) { DISCOVERED.fetch_add(n, Relaxed); }
    pub(super) fn record_logic() { LOGIC.fetch_add(1, Relaxed); }
    pub(super) fn record_produced() { PRODUCED.fetch_add(1, Relaxed); }
    /// Reset all counters to zero (process-global, across all worker threads).
    pub fn reset() {
        for c in [&DISCOVERED, &LOGIC, &PRODUCED] { c.store(0, Relaxed); }
    }
    /// Counters across all worker threads.
    pub fn snapshot() -> Snapshot {
        Snapshot {
            discovered: DISCOVERED.load(Relaxed),
            logic: LOGIC.load(Relaxed),
            produced: PRODUCED.load(Relaxed),
        }
    }
    /// A snapshot of the audit counters.
    #[derive(Debug, Clone, Copy)]
    pub struct Snapshot {
        /// In-interval interesting times enumerated by pass 1.
        pub discovered: u64,
        /// User-logic invocations in pass 2.
        pub logic: u64,
        /// Logic invocations that produced a non-empty diff.
        pub produced: u64,
    }
}

/// A type that resolves a key-wise reduction over batches arriving on the input.
///
/// Unlike join, reduce does not suspend: its output is at most linear in its input, so a single
/// `retire` runs the whole `[lower, upper)` interval to completion rather than yielding under a fuel
/// budget.
pub(crate) trait ReduceTactic<B1: BatchReader, B2: BatchReader<Time = B1::Time>> {
    /// Retire the interval `[lower, upper)`, producing the output batches it informs.
    ///
    /// It is presented with the pre-existing input batches and output batches (those before `lower`),
    /// the new input batches, and `held`: the times the operator currently holds capabilities for. It
    /// reasons only about times, returning the output batches to ship — each tagged with the time at
    /// which to ship it — and the new frontier of interesting times for the operator to hold.
    fn retire(
        &mut self,
        source_batches: Vec<B1>,
        output_batches: Vec<B2>,
        input_batches: Vec<B1>,
        lower: &Antichain<B1::Time>,
        upper: &Antichain<B1::Time>,
        held: &Antichain<B1::Time>,
    ) -> (Vec<(B1::Time, B2)>, Antichain<B1::Time>);
}

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
pub fn reduce_trace<'scope, Tr1, Bu, Tr2, L, P>(trace: Arranged<'scope, Tr1>, name: &str, logic: L, push: P) -> Arranged<'scope, TraceAgent<Tr2>>
where
    Tr1: TraceReader<Batch: Navigable> + 'static,
    Tr2: Trace<Batch: Navigable, Time = Tr1::Time> + 'static,
    BatchCursor<Tr1>: Cursor<Time = Tr1::Time>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = BatchKey<'a, Tr1>, ValOwn: Data, Time = Tr2::Time>,
    Bu: Builder<Time=Tr2::Time, Output = Tr2::Batch, Input: Default> + 'static,
    L: FnMut(BatchKey<'_, Tr1>, &[(BatchVal<'_, Tr1>, BatchDiff<Tr1>)], &mut Vec<(BatchValOwn<Tr2>, BatchDiff<Tr2>)>, &mut Vec<(BatchValOwn<Tr2>, BatchDiff<Tr2>)>)+'static,
    P: FnMut(&mut Bu::Input, BatchKey<'_, Tr1>, &mut Vec<(BatchValOwn<Tr2>, Tr2::Time, BatchDiff<Tr2>)>) + 'static,
{
    reduce_with_tactic(trace, name, cursors::CursorTactic::<Tr1::Batch, Tr2::Batch, Bu, L, P>::new(logic, push))
}

/// Drives a key-wise reduction using a supplied [`ReduceTactic`].
///
/// This is the general reduce operator: it does the dataflow plumbing (frontiers, capabilities, output
/// trace maintenance) and routes the per-interval work through the tactic. It requires only
/// `TraceReader` of its input and `Trace` of its output, never `Navigable`: it extracts batches via
/// `batches_through`, and building cursors over them (if that is how the reduce proceeds) is the
/// tactic's concern.
pub(crate) fn reduce_with_tactic<'scope, Tr1, Tr2, T>(trace: Arranged<'scope, Tr1>, name: &str, mut tactic: T) -> Arranged<'scope, TraceAgent<Tr2>>
where
    Tr1: TraceReader + 'static,
    Tr2: Trace<Time = Tr1::Time> + 'static,
    T: ReduceTactic<Tr1::Batch, Tr2::Batch> + 'static,
{
    let mut result_trace = None;

    // fabricate a data-parallel operator using the `unary_notify` pattern.
    let stream = {

        let mut source_trace = trace.trace;
        let result_trace = &mut result_trace;
        let scope = trace.stream.scope();
        trace.stream.unary_frontier(Pipeline, name, move |_capability, operator_info| {

            // Acquire a logger for arrange events.
            let logger = scope.worker().logger_for::<crate::logging::DifferentialEventBuilder>("differential/arrange").map(Into::into);

            let activator = Some(scope.activator_for(std::rc::Rc::clone(&operator_info.address)));
            let mut empty = Tr2::new(operator_info.clone(), logger.clone(), activator);
            // If there is default exert logic set, install it.
            if let Some(exert_logic) = scope.worker().config().get::<ExertionLogic>("differential/default_exert_logic").cloned() {
                empty.set_exert_logic(exert_logic);
            }

            let (mut output_reader, mut output_writer) = TraceAgent::new(empty, operator_info, logger);

            *result_trace = Some(output_reader.clone());

            // Capabilities for the lower envelope of the interesting times the operator holds.
            let mut capabilities = CapabilitySet::<Tr1::Time>::default();

            // Upper and lower frontiers for the pending input and output batches to process.
            let mut upper_limit = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());
            let mut lower_limit = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());

            move |(input, frontier), output| {

                // The operator receives input batches, which it treats as contiguous and will collect and
                // then process as one batch. It captures the input frontier from the batches, from the upstream
                // trace, and from the input frontier, and retires the work through that interval.
                //
                // Reduce may retain capabilities and need to perform work and produce output at times that
                // may not be seen in its input. The standard example is that updates at `(0, 1)` and `(1, 0)`
                // may result in outputs at `(1, 1)` as well, even with no input at that time.

                let mut batch_storage = Vec::new();

                // Downgrade previous upper limit to be current lower limit.
                lower_limit.clear();
                lower_limit.extend(upper_limit.borrow().iter().cloned());

                // Drain input batches in order, capturing capabilities and the last upper.
                input.for_each(|capability, batches| {
                    capabilities.insert(capability.retain(0));
                    for batch in batches.drain(..) {
                        upper_limit.clone_from(batch.upper());
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

                    // Acquire the pre-existing input and output batches preceding the interval. Batch handles
                    // are cheap to clone, so we fetch them whether or not the tactic finds work to do.
                    let source_batches = source_trace.batches_through(lower_limit.borrow()).expect("failed to acquire source batches");
                    let output_batches = output_reader.batches_through(lower_limit.borrow()).expect("failed to acquire output batches");

                    // The times the operator currently holds capabilities for, as an antichain.
                    let held: Antichain<Tr1::Time> = capabilities.iter().map(|c| c.time().clone()).collect();

                    // Retire the interval. The tactic reasons only about times: it returns output batches
                    // each tagged with the time to ship it at, and the new frontier of interesting times.
                    let (produced, new_frontier) = tactic.retire(source_batches, output_batches, batch_storage, &lower_limit, &upper_limit, &held);

                    // Ship each batch at a capability minted from the set at its time, and commit it to the
                    // output trace. The times are elements of `held`, so they stay valid until we downgrade.
                    for (time, batch) in produced {
                        let capability = capabilities.delayed(&time);
                        output.session(&capability).give(batch.clone());
                        output_writer.insert(batch, Some(time));
                    }

                    // Downgrade to the frontier the tactic handed back (a no-op when it found no work).
                    capabilities.downgrade(new_frontier);

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

/// The conventional cursor-based [`ReduceTactic`].
///
/// It builds a [`CursorList`](crate::trace::cursor::CursorList) over the input, output, and new-batch
/// updates and replays them together per key, applying `logic` and shaping output with `push`. It holds
/// the outstanding synthetic interesting `(key, time)` moments across activations, and reasons only
/// about times: capabilities are the driver's concern.
mod cursors {

    use super::*;

    /// The conventional cursor-based [`ReduceTactic`].
    pub struct CursorTactic<B1, B2, Bu, L, P>
    where
        B1: BatchReader + Navigable,
        B2: BatchReader<Time = B1::Time> + Navigable,
        B1::Cursor: Cursor<Time = B1::Time>,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
    {
        logic: L,
        push: P,
        // Outstanding `(key, time)` synthetic interesting moments, sorted by `(key, time)`, and the
        // buffers into which we assemble the next round's moments.
        pending_keys: <B1::Cursor as Cursor>::KeyContainer,
        pending_time: <B1::Cursor as Cursor>::TimeContainer,
        next_pending_keys: <B1::Cursor as Cursor>::KeyContainer,
        next_pending_time: <B1::Cursor as Cursor>::TimeContainer,
        // Buffers reused across activations.
        interesting_times: Vec<B1::Time>,
        new_interesting_times: Vec<B1::Time>,
        // Output batches may need to be built piecemeal, and these temp storage help there.
        output_upper: Antichain<B1::Time>,
        output_lower: Antichain<B1::Time>,
        _marker: PhantomData<(B2, Bu)>,
    }

    impl<B1, B2, Bu, L, P> CursorTactic<B1, B2, Bu, L, P>
    where
        B1: BatchReader + Navigable,
        B2: BatchReader<Time = B1::Time> + Navigable,
        B1::Cursor: Cursor<Time = B1::Time>,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
    {
        /// Construct a tactic that applies `logic` to each key and shapes output with `push`.
        pub fn new(logic: L, push: P) -> Self {
            CursorTactic {
                logic,
                push,
                pending_keys: <B1::Cursor as Cursor>::KeyContainer::with_capacity(0),
                pending_time: <B1::Cursor as Cursor>::TimeContainer::with_capacity(0),
                next_pending_keys: <B1::Cursor as Cursor>::KeyContainer::with_capacity(0),
                next_pending_time: <B1::Cursor as Cursor>::TimeContainer::with_capacity(0),
                interesting_times: Vec::new(),
                new_interesting_times: Vec::new(),
                output_upper: Antichain::from_elem(<B1::Time as Timestamp>::minimum()),
                output_lower: Antichain::from_elem(<B1::Time as Timestamp>::minimum()),
                _marker: PhantomData,
            }
        }
    }

    impl<B1, B2, Bu, L, P> ReduceTactic<B1, B2> for CursorTactic<B1, B2, Bu, L, P>
    where
        B1: BatchReader + Navigable,
        B2: BatchReader<Time = B1::Time> + Navigable,
        B1::Cursor: Cursor<Time = B1::Time>,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
        Bu: Builder<Time = B1::Time, Output = B2, Input: Default>,
        L: FnMut(<B1::Cursor as Cursor>::Key<'_>, &[(<B1::Cursor as Cursor>::Val<'_>, <B1::Cursor as Cursor>::Diff)], &mut Vec<(<B2::Cursor as Cursor>::ValOwn, <B2::Cursor as Cursor>::Diff)>, &mut Vec<(<B2::Cursor as Cursor>::ValOwn, <B2::Cursor as Cursor>::Diff)>),
        P: FnMut(&mut Bu::Input, <B1::Cursor as Cursor>::Key<'_>, &mut Vec<(<B2::Cursor as Cursor>::ValOwn, B1::Time, <B2::Cursor as Cursor>::Diff)>),
    {
        fn retire(
            &mut self,
            source_batches: Vec<B1>,
            output_batches: Vec<B2>,
            input_batches: Vec<B1>,
            lower: &Antichain<B1::Time>,
            upper: &Antichain<B1::Time>,
            held: &Antichain<B1::Time>,
        ) -> (Vec<(B1::Time, B2)>, Antichain<B1::Time>)
        {
            let mut produced = Vec::new();

            // We have compute needs only if we hold a time in the interval [lower, upper); otherwise we
            // could not transmit outputs even if they were (incorrectly) non-zero, and we leave the held
            // times unchanged.
            if held.elements().iter().any(|time| !upper.less_equal(time)) {

                // cursors for navigating input, output, and new-batch updates.
                let (mut source_cursor, ref source_storage) = cursor_list(source_batches);
                let (mut output_cursor, ref output_storage) = cursor_list(output_batches);
                let (mut batch_cursor, ref batch_storage) = cursor_list(input_batches);

                // Prepare an output buffer and builder for each held time.
                // TODO: It would be better if all updates went into one batch, but timely dataflow prevents
                //       this as long as it requires that there is only one capability for each message.
                let mut buffers = Vec::<(B1::Time, Vec<(<B2::Cursor as Cursor>::ValOwn, B1::Time, <B2::Cursor as Cursor>::Diff)>)>::new();
                let mut builders = Vec::new();
                for time in held.elements().iter() {
                    buffers.push((time.clone(), Vec::new()));
                    builders.push(Bu::new());
                }
                // Temporary staging for output building.
                let mut buffer = Bu::Input::default();

                // Reuseable state for performing the computation.
                let mut thinker = history_replay::HistoryReplayer::new();

                // March through the keys we must work on, merging `batch_cursor` and pending keys.
                // The interesting moments need to be in the interval to prompt work.
                let mut pending_pos = 0;
                while batch_cursor.key_valid(batch_storage) || pending_pos < self.pending_keys.len() {

                    // Determine the next key we will work on; could be synthetic, could be from a batch.
                    let key1 = self.pending_keys.get(pending_pos);
                    let key2 = batch_cursor.get_key(batch_storage);
                    let key = match (key1, key2) {
                        (Some(key1), Some(key2)) => ::std::cmp::min(key1, key2),
                        (Some(key1), None)       => key1,
                        (None, Some(key2))       => key2,
                        (None, None)             => unreachable!(),
                    };

                    // Populate `interesting_times` with interesting times not beyond `upper`.
                    // TODO: This could just be `pending_time` and indexes within `lower .. upper`.
                    let prior_pos = pending_pos;
                    self.interesting_times.clear();
                    while self.pending_keys.get(pending_pos) == Some(key) {
                        let owned_time = <B1::Cursor as Cursor>::owned_time(self.pending_time.index(pending_pos));
                        if !upper.less_equal(&owned_time) { self.interesting_times.push(owned_time); }
                        pending_pos += 1;
                    }

                    // tidy up times, removing redundancy.
                    sort_dedup(&mut self.interesting_times);

                    // If there are new updates, or pending times, we must investigate!
                    if batch_cursor.get_key(batch_storage) == Some(key) || !self.interesting_times.is_empty() {

                        // do the per-key computation.
                        thinker.compute(
                            key,
                            (&mut source_cursor, source_storage),
                            (&mut output_cursor, output_storage),
                            (&mut batch_cursor, batch_storage),
                            &self.interesting_times,
                            &mut self.logic,
                            upper,
                            &mut buffers[..],
                            &mut self.new_interesting_times,
                        );

                        // Advance the cursor if this key, so that the loop's validity check registers the work as done.
                        if batch_cursor.get_key(batch_storage) == Some(key) { batch_cursor.step_key(batch_storage); }

                        // Merge novel pending times with any prior pending times we did not process.
                        // TODO: This could be a merge, not a sort_dedup, because both lists should be sorted.
                        for pos in prior_pos .. pending_pos {
                            let owned_time = <B1::Cursor as Cursor>::owned_time(self.pending_time.index(pos));
                            if upper.less_equal(&owned_time) { self.new_interesting_times.push(owned_time); }
                        }
                        sort_dedup(&mut self.new_interesting_times);
                        for time in self.new_interesting_times.drain(..) {
                            self.next_pending_keys.push_ref(key);
                            self.next_pending_time.push_own(&time);
                        }

                        // Sort each buffer by value and move into the corresponding builder.
                        // TODO: This makes assumptions about at least one of (i) the stability of `sort_by`,
                        //       (ii) that the buffers are time-ordered, and (iii) that the builders accept
                        //       arbitrarily ordered times.
                        for index in 0 .. buffers.len() {
                            buffers[index].1.sort_by(|x,y| x.0.cmp(&y.0));
                            (self.push)(&mut buffer, key, &mut buffers[index].1);
                            buffers[index].1.clear();
                            builders[index].push(&mut buffer);

                        }
                    }
                    else {
                        // copy over the pending key and times.
                        for pos in prior_pos .. pending_pos {
                            self.next_pending_keys.push_ref(self.pending_keys.index(pos));
                            self.next_pending_time.push_ref(self.pending_time.index(pos));
                        }
                    }
                }
                // Drop to avoid lifetime issues that would lock `pending_{keys, time}`.
                drop(thinker);

                // We start sealing output batches from the lower limit (previous upper limit).
                // In principle, we could update `lower` itself, and it should arrive at `upper` by the
                // end of the process.
                self.output_lower.clear();
                self.output_lower.extend(lower.borrow().iter().cloned());

                // build each batch (because only one capability per message).
                for (index, builder) in builders.drain(..).enumerate() {

                    // Form the upper limit of the next batch, which includes all times greater
                    // than the input batch, or the held times from i + 1 onward.
                    self.output_upper.clear();
                    self.output_upper.extend(upper.borrow().iter().cloned());
                    for time in &held.elements()[index + 1 ..] {
                        self.output_upper.insert_ref(time);
                    }

                    if self.output_upper.borrow() != self.output_lower.borrow() {

                        let description = Description::new(self.output_lower.clone(), self.output_upper.clone(), Antichain::from_elem(<B1::Time as Timestamp>::minimum()));
                        let batch = builder.done(description);

                        // hand the batch back to the driver to ship and commit, tagged with its time.
                        produced.push((held.elements()[index].clone(), batch));

                        self.output_lower.clear();
                        self.output_lower.extend(self.output_upper.borrow().iter().cloned());
                    }
                }
                // This should be true, as the final iteration introduces no held times, and
                // uses exactly `upper` to determine the upper bound. Good to check though.
                assert!(self.output_upper.borrow() == upper.borrow());

                // Refresh pending keys and times.
                self.pending_keys.clear(); std::mem::swap(&mut self.next_pending_keys, &mut self.pending_keys);
                self.pending_time.clear(); std::mem::swap(&mut self.next_pending_time, &mut self.pending_time);

                // Compute the new frontier of interesting times for the operator to hold.
                let mut frontier = Antichain::<B1::Time>::new();
                let mut owned_time = <B1::Time as Timestamp>::minimum();
                for pos in 0 .. self.pending_time.len() {
                    <B1::Cursor as Cursor>::clone_time_onto(self.pending_time.index(pos), &mut owned_time);
                    frontier.insert_ref(&owned_time);
                }

                (produced, frontier)
            }
            else {
                // No work: leave the held times unchanged, so the driver's downgrade is a no-op.
                (produced, held.clone())
            }
        }
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

        use crate::lattice::Lattice;
        use crate::trace::Cursor;
        use crate::operators::ValueHistory;

        use super::sort_dedup;

        /// The `HistoryReplayer` is a compute strategy based on moving through existing inputs, interesting times, etc in
        /// time order, maintaining consolidated representations of updates with respect to future interesting times.
        pub struct HistoryReplayer<V1, V2, V, T, D1, D2> {
            input_history: ValueHistory<V1, T, D1>,
            output_history: ValueHistory<V2, T, D2>,
            batch_history: ValueHistory<V1, T, D1>,
            input_buffer: Vec<(V1, D1)>,
            output_buffer: Vec<(V, D2)>,
            update_buffer: Vec<(V, D2)>,
            output_produced: Vec<((V, T), D2)>,
            synth_times: Vec<T>,
            meets: Vec<T>,
            times_current: Vec<T>,
            temporary: Vec<T>,
        }

        impl<V1, V2, V, T, D1, D2> HistoryReplayer<V1, V2, V, T, D1, D2>
        where
            V1: Copy + Ord,
            V2: Copy + Ord,
            V: Clone + Ord,
            T: Ord + Clone + Lattice,
            D1: Clone + crate::difference::Semigroup,
            D2: Clone + crate::difference::Semigroup,
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
            pub fn compute<'a, K, C1, C2, C3, L>(
                &mut self,
                key: K,
                (source_cursor, source_storage): (&mut C1, &'a C1::Storage),
                (output_cursor, output_storage): (&mut C2, &'a C2::Storage),
                (batch_cursor, batch_storage): (&mut C3, &'a C3::Storage),
                times: &Vec<T>,
                logic: &mut L,
                upper_limit: &Antichain<T>,
                outputs: &mut [(T, Vec<(V, T, D2)>)],
                new_interesting: &mut Vec<T>)
            where
                C1: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
                C2: Cursor<Key<'a> = K, Val<'a> = V2, ValOwn = V, Time = T, Diff = D2>,
                C3: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
                K: Copy + Ord,
                L: FnMut(K, &[(V1, D1)], &mut Vec<(V, D2)>, &mut Vec<(V, D2)>),
            {
                // Two passes. First determine every interesting time in `[.., upper_limit)` (reasoning only
                // about times); then evaluate those times in order, reforming collections and running `logic`.
                // (Each pass currently loads independently; step 5 will share a single load.)
                let mut active = Vec::new();
                self.discover_times(
                    key,
                    (&mut *source_cursor, source_storage),
                    (&mut *output_cursor, output_storage),
                    (&mut *batch_cursor, batch_storage),
                    times,
                    upper_limit,
                    &mut active,
                    new_interesting,
                );
                #[cfg(debug_assertions)]
                super::audit::record_discovered(active.len() as u64);

                self.evaluate_times(
                    key,
                    (source_cursor, source_storage),
                    (output_cursor, output_storage),
                    (batch_cursor, batch_storage),
                    times,
                    logic,
                    upper_limit,
                    outputs,
                    &active,
                );
            }

            /// Pass 1: determine the interesting times for `key` in `[.., upper_limit)`.
            ///
            /// Reasoning only about times — no value accumulation, no user logic. Loads the histories and
            /// walks batch/warned/input/output/synth times, generating the join-closure. It does not consult
            /// `output_produced`: output can only change where accumulated input changed, so output-change
            /// times lie within the input join-closure and need not separately seed it.
            ///
            /// In-interval interesting times land (sorted) in `active`; deferred times in `new_interesting`.
            fn discover_times<'a, K, C1, C2, C3>(
                &mut self,
                key: K,
                (source_cursor, source_storage): (&mut C1, &'a C1::Storage),
                (output_cursor, output_storage): (&mut C2, &'a C2::Storage),
                (batch_cursor, batch_storage): (&mut C3, &'a C3::Storage),
                times: &Vec<T>,
                upper_limit: &Antichain<T>,
                discovered: &mut Vec<T>,
                new_interesting: &mut Vec<T>)
            where
                C1: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
                C2: Cursor<Key<'a> = K, Val<'a> = V2, ValOwn = V, Time = T, Diff = D2>,
                C3: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
                K: Copy + Ord,
            {
                discovered.clear();

                let mut batch_replay = self.batch_history.replay_key(batch_cursor, batch_storage, key, None);

                self.meets.clear();
                self.meets.extend(times.iter().cloned());
                for index in (1 .. self.meets.len()).rev() {
                    self.meets[index-1] = self.meets[index-1].meet(&self.meets[index]);
                }

                let mut meet = None;
                update_meet(&mut meet, self.meets.get(0));
                update_meet(&mut meet, batch_replay.meet());

                let mut input_replay =
                self.input_history.replay_key(source_cursor, source_storage, key, meet.as_ref());
                let mut output_replay =
                self.output_history.replay_key(output_cursor, output_storage, key, meet.as_ref());

                self.synth_times.clear();
                self.times_current.clear();

                let mut times_slice = &times[..];
                let mut meets_slice = &self.meets[..];

                while let Some(next_time) = [   batch_replay.time(),
                                                times_slice.first(),
                                                input_replay.time(),
                                                output_replay.time(),
                                                self.synth_times.last(),
                                            ].into_iter().flatten().min().cloned() {

                    input_replay.step_while_time_is(&next_time);
                    output_replay.step_while_time_is(&next_time);

                    let mut interesting = batch_replay.step_while_time_is(&next_time);
                    if interesting { if let Some(meet) = meet.as_ref() { batch_replay.advance_buffer_by(meet); } }

                    while self.synth_times.last() == Some(&next_time) {
                        self.times_current.push(self.synth_times.pop().expect("failed to pop from synth_times"));
                        interesting = true;
                    }
                    while times_slice.first() == Some(&next_time) {
                        self.times_current.push(times_slice[0].clone());
                        times_slice = &times_slice[1..];
                        meets_slice = &meets_slice[1..];
                        interesting = true;
                    }

                    interesting = interesting || batch_replay.buffer().iter().any(|&((_, ref t),_)| t.less_equal(&next_time));
                    interesting = interesting || self.times_current.iter().any(|t| t.less_equal(&next_time));

                    if !upper_limit.less_equal(&next_time) {

                        if interesting {
                            discovered.push(next_time.clone());

                            // Mirror the live value-block's synthetic-time generation from the input and
                            // output buffers, but omit the value-dependent `output_produced` joins.
                            if let Some(meet) = meet.as_ref() { input_replay.advance_buffer_by(meet) };
                            for ((_, time), _) in input_replay.buffer().iter() {
                                if !time.less_equal(&next_time) { self.temporary.push(next_time.join(time)); }
                            }
                            if let Some(meet) = meet.as_ref() { output_replay.advance_buffer_by(meet) };
                            for ((_, time), _) in output_replay.buffer().iter() {
                                if !time.less_equal(&next_time) { self.temporary.push(next_time.join(time)); }
                            }
                        }

                        self.temporary.extend(batch_replay.buffer().iter().map(|((_,time),_)| time).filter(|time| !time.less_equal(&next_time)).map(|time| time.join(&next_time)));
                        self.temporary.extend(self.times_current.iter().filter(|time| !time.less_equal(&next_time)).map(|time| time.join(&next_time)));
                        sort_dedup(&mut self.temporary);

                        let synth_len = self.synth_times.len();
                        for time in self.temporary.drain(..) {
                            if upper_limit.less_equal(&time) {
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
                        new_interesting.push(next_time.clone());
                    }

                    meet = None;
                    update_meet(&mut meet, batch_replay.meet());
                    update_meet(&mut meet, input_replay.meet());
                    update_meet(&mut meet, output_replay.meet());
                    for time in self.synth_times.iter() { update_meet(&mut meet, Some(time)); }
                    update_meet(&mut meet, meets_slice.first());

                    if let Some(meet) = meet.as_ref() {
                        for time in self.times_current.iter_mut() {
                            *time = time.join(meet);
                        }
                    }
                    sort_dedup(&mut self.times_current);
                }

                sort_dedup(discovered);
                sort_dedup(new_interesting);
            }

            /// Pass 2: evaluate the already-determined interesting times in `active` (sorted, in-interval).
            ///
            /// Loads the histories (same `meet(batch ∪ warned)` as pass 1), then walks `active` together
            /// with the input/output/batch time streams. At each active time it reforms the input and output
            /// collections, runs `logic`, and emits the resulting diffs into `outputs`. There is no time
            /// discovery here — the set of times is fixed — so the synthetic-time/interestingness machinery
            /// of the original loop is gone. The running `meet` is the glb of the remaining `active` suffix
            /// and the replay frontiers, which stays `<=` every remaining active time, so buffer compaction
            /// is result-preserving.
            #[allow(clippy::too_many_arguments)]
            fn evaluate_times<'a, K, C1, C2, C3, L>(
                &mut self,
                key: K,
                (source_cursor, source_storage): (&mut C1, &'a C1::Storage),
                (output_cursor, output_storage): (&mut C2, &'a C2::Storage),
                (batch_cursor, batch_storage): (&mut C3, &'a C3::Storage),
                times: &Vec<T>,
                logic: &mut L,
                upper_limit: &Antichain<T>,
                outputs: &mut [(T, Vec<(V, T, D2)>)],
                active: &[T])
            where
                C1: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
                C2: Cursor<Key<'a> = K, Val<'a> = V2, ValOwn = V, Time = T, Diff = D2>,
                C3: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
                K: Copy + Ord,
                L: FnMut(K, &[(V1, D1)], &mut Vec<(V, D2)>, &mut Vec<(V, D2)>),
            {
                // Load the histories with the same meet pass 1 used: meet(batch ∪ warned).
                let mut batch_replay = self.batch_history.replay_key(batch_cursor, batch_storage, key, None);
                self.meets.clear();
                self.meets.extend(times.iter().cloned());
                for index in (1 .. self.meets.len()).rev() {
                    self.meets[index-1] = self.meets[index-1].meet(&self.meets[index]);
                }
                let mut load_meet = None;
                update_meet(&mut load_meet, self.meets.get(0));
                update_meet(&mut load_meet, batch_replay.meet());
                let mut input_replay =
                self.input_history.replay_key(source_cursor, source_storage, key, load_meet.as_ref());
                let mut output_replay =
                self.output_history.replay_key(output_cursor, output_storage, key, load_meet.as_ref());

                self.input_buffer.clear();
                self.output_buffer.clear();
                self.output_produced.clear();

                // Suffix meets of the active times, used to advance buffers as we proceed.
                self.meets.clear();
                self.meets.extend(active.iter().cloned());
                for index in (1 .. self.meets.len()).rev() {
                    self.meets[index-1] = self.meets[index-1].meet(&self.meets[index]);
                }

                let mut meet = None;
                update_meet(&mut meet, self.meets.get(0));
                update_meet(&mut meet, batch_replay.meet());
                update_meet(&mut meet, input_replay.meet());
                update_meet(&mut meet, output_replay.meet());

                // Index of the next active time to evaluate.
                let mut idx = 0;

                while let Some(next_time) = [   batch_replay.time(),
                                                input_replay.time(),
                                                output_replay.time(),
                                                active.get(idx),
                                            ].into_iter().flatten().min().cloned() {

                    // Advance the replayers so their buffers hold all updates at or before `next_time`.
                    input_replay.step_while_time_is(&next_time);
                    output_replay.step_while_time_is(&next_time);
                    let batch_step = batch_replay.step_while_time_is(&next_time);
                    if batch_step { if let Some(meet) = meet.as_ref() { batch_replay.advance_buffer_by(meet); } }

                    // We evaluate exactly the active times, in order.
                    let active_now = active.get(idx) == Some(&next_time);
                    if active_now { idx += 1; }

                    if !upper_limit.less_equal(&next_time) && active_now {

                        // Assemble the input collection at `next_time`.
                        debug_assert!(self.input_buffer.is_empty());
                        if let Some(meet) = meet.as_ref() { input_replay.advance_buffer_by(meet) };
                        for ((value, time), diff) in input_replay.buffer().iter() {
                            if time.less_equal(&next_time) { self.input_buffer.push((*value, diff.clone())); }
                        }
                        for ((value, time), diff) in batch_replay.buffer().iter() {
                            if time.less_equal(&next_time) { self.input_buffer.push((*value, diff.clone())); }
                        }
                        crate::consolidation::consolidate(&mut self.input_buffer);

                        // Assemble the output collection at `next_time`.
                        if let Some(meet) = meet.as_ref() { output_replay.advance_buffer_by(meet) };
                        for ((value, time), diff) in output_replay.buffer().iter() {
                            if time.less_equal(&next_time) { self.output_buffer.push((C2::owned_val(*value), diff.clone())); }
                        }
                        for ((value, time), diff) in self.output_produced.iter() {
                            if time.less_equal(&next_time) { self.output_buffer.push(((*value).to_owned(), diff.clone())); }
                        }
                        crate::consolidation::consolidate(&mut self.output_buffer);

                        // Apply user logic if non-empty input or output and see what happens!
                        if !self.input_buffer.is_empty() || !self.output_buffer.is_empty() {
                            #[cfg(debug_assertions)]
                            super::audit::record_logic();
                            logic(key, &self.input_buffer[..], &mut self.output_buffer, &mut self.update_buffer);
                            self.input_buffer.clear();
                            self.output_buffer.clear();

                            crate::consolidation::consolidate(&mut self.update_buffer);
                            if !self.update_buffer.is_empty() {
                                #[cfg(debug_assertions)]
                                super::audit::record_produced();

                                let oidx = outputs.iter().rev().position(|(time, _)| time.less_equal(&next_time));
                                let oidx = outputs.len() - oidx.expect("failed to find index") - 1;
                                for (val, diff) in self.update_buffer.drain(..) {
                                    self.output_produced.push(((val.clone(), next_time.clone()), diff.clone()));
                                    outputs[oidx].1.push((val, next_time.clone(), diff));
                                }

                                if let Some(meet) = meet.as_ref() { for entry in &mut self.output_produced { (entry.0).1.join_assign(meet); } }
                                crate::consolidation::consolidate(&mut self.output_produced);
                            }
                        }
                    }

                    // Update `meet`: glb of the remaining active suffix and the replay frontiers.
                    meet = None;
                    update_meet(&mut meet, batch_replay.meet());
                    update_meet(&mut meet, input_replay.meet());
                    update_meet(&mut meet, output_replay.meet());
                    update_meet(&mut meet, self.meets.get(idx));
                }
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
}
