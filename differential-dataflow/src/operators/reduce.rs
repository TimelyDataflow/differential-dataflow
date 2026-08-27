//! Applies a reduction function on records grouped by key.
//!
//! The `reduce` operator acts on `(key, val)` data.
//! Records with the same key are grouped together, and a user-supplied reduction function is applied
//! to the key and the list of values.
//! The function is expected to populate a list of output values.
//!
//! The output can change at times that are joins of input times, not only at input times themselves,
//! and the operator must determine at which times to re-evaluate the reduction. A machine-checked
//! account of which times suffice lives in `formal/Differential/Coverage.lean`.

use crate::Data;

use std::marker::PhantomData;

use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::channels::pact::Pipeline;

use crate::operators::arrange::{Arranged, TraceAgent};
use crate::trace::{Span, BatchCursor, BatchDiff, BatchKey, BatchVal, BatchValOwn, Builder, Cursor, Description, ExertionLogic, Navigable, Trace, TraceReader};
use crate::trace::cursor::cursor_list;
use crate::trace::implementations::containers::BatchContainer;

/// The time type of the updates' cursor: the time coordinate the tactics work in.
pub(crate) type TimeOf<B> = <<B as Navigable>::Cursor as Cursor>::Time;

/// Sort and deduplicate a list. Shared by the cursor and reference tactics and the proxy tactic (`crate::operators::reduce::sort_dedup`), which
/// each previously carried an identical copy.
#[inline(never)]
pub(crate) fn sort_dedup<T: Ord>(list: &mut Vec<T>) {
    list.dedup();
    list.sort();
    list.dedup();
}

/// A type that resolves a key-wise reduction over batches arriving on the input.
///
/// Unlike join, reduce does not suspend: its output is at most linear in its input, so a single
/// `retire` runs the whole `[lower, upper)` interval to completion rather than yielding under a fuel
/// budget.
pub trait ReduceTactic<T, B1, B2> {
    /// Retire the interval `[lower, upper)`, producing the output batch it informs.
    ///
    /// It is presented with the pre-existing input batches and output batches (those before `lower`),
    /// the new input batches, and `held`: the times the operator currently holds capabilities for. It
    /// reasons only about times, returning the output batch to ship — `None` when the interval holds
    /// no work at all — and the new frontier of interesting times for the operator to hold.
    ///
    /// # Contract
    ///
    /// The driver ([`reduce_with_tactic`]) relies on the following; the first is cheap to check
    /// and is `debug_assert!`ed there.
    ///
    /// * **Spanning output.** The returned batch's description is exactly `[lower, upper)`. The
    ///   driver ships it stamped with the held times not in advance of `upper`, which justify its
    ///   contents: every update time lies at or beyond one of them (a time at or beyond only the
    ///   remaining held times would be in advance of `upper`, and so outside the interval).
    /// * **Frontier bounds withheld work, and collapses to empty when there is none.** The returned
    ///   frontier must be at-or-below every time the tactic defers, so the driver knows what is safe to
    ///   release. In particular, with no work to defer it must be the *empty* antichain. Derive it from
    ///   the actual withheld set rather than constructing it and this holds for free; returning a
    ///   non-empty frontier with nothing pending holds capabilities forever and **deadlocks recursive
    ///   scopes**. (Not driver-checkable — the withheld set is tactic-internal — so tactics self-enforce.)
    fn retire(
        &mut self,
        source_batches: Vec<B1>,
        output_batches: Vec<B2>,
        input_batches: Vec<B1>,
        lower: &Antichain<T>,
        upper: &Antichain<T>,
        held: &Antichain<T>,
    ) -> (Option<Span<T, B2>>, Antichain<T>);
}

pub use crate::operators::cursor::reduce::reduce_trace;

// The model-derived reference tactic and its entry point live in `mod reference`; re-exported here
// (doc-hidden) as the sole public handle for its differential and oracle tests.
#[doc(hidden)]
pub use reference::reduce_trace_reference;

/// Drives a key-wise reduction using a supplied [`ReduceTactic`].
///
/// This is the general reduce operator: it does the dataflow plumbing (frontiers, capabilities, output
/// trace maintenance) and routes the per-interval work through the tactic. It requires only
/// `TraceReader` of its input and `Trace` of its output, never `Navigable`: it extracts batches via
/// `spans_through`, and building cursors over them (if that is how the reduce proceeds) is the
/// tactic's concern.
pub fn reduce_with_tactic<'scope, Tr1, Tr2, T>(trace: Arranged<'scope, Tr1>, name: &str, mut tactic: T) -> Arranged<'scope, TraceAgent<Tr2>>
where
    Tr1: TraceReader + 'static,
    Tr2: Trace<Time = Tr1::Time> + 'static,
    T: ReduceTactic<Tr1::Time, Tr1::Batch, Tr2::Batch> + 'static,
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
                    for capability in capability.retain_stamp(0).iter() {
                        capabilities.insert(capability.clone());
                    }
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

                    // Retire the interval. The tactic reasons only about times: it returns the output
                    // batch to ship, if any, and the new frontier of interesting times.
                    let (produced, new_frontier) = tactic.retire(source_batches, output_batches, batch_storage.into_iter().filter_map(|b| b.inner).collect(), &lower_limit, &upper_limit, &held);

                    // Contract check (see `ReduceTactic::retire`). Cheap, debug-only.
                    debug_assert!(
                        produced.as_ref().is_none_or(|batch| batch.lower() == &lower_limit && batch.upper() == &upper_limit),
                        "ReduceTactic::retire output must span [lower, upper)",
                    );

                    // Ship the batch stamped with the capabilities it retires — those not in advance of
                    // the upper limit — and commit it to the output trace. The times are elements of
                    // `held`, so they stay valid until we downgrade.
                    if let Some(batch) = produced {
                        let retiring = capabilities
                            .iter()
                            .filter(|c| !upper_limit.less_equal(c.time()))
                            .cloned()
                            .collect::<CapabilitySet<_>>();
                        output.session(&retiring).give(batch.clone());
                        let stamp = retiring.iter().map(|c| c.time().clone()).collect::<timely::progress::Stamp<_>>();
                        output_writer.insert(batch, stamp);
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

/// A second [`ReduceTactic`], written directly from the incremental model in
/// `formal/Differential/Model.lean`.
///
/// Per key it runs two phases over one cursor walk. Phase 1 (determination) computes the
/// interesting times as the truncated join-closure over {input, output, seeds} — the model's
/// `Reached` — advancing by meets so the synthetic set stays bounded. Phase 2 (application) walks
/// exactly those times in order, maintaining tight input/output accumulations by meets, and emits
/// the corrections — the model's `emit_correct`. Determination never consults the output produced
/// this round (it finishes first), so this tactic embodies the proven algorithm exactly and is the
/// clean subject for differential testing against [`crate::operators::cursor::reduce::CursorTactic`].
pub(crate) mod reference {

    use super::*;
    use crate::lattice::Lattice;
    use crate::operators::ValueHistory;

    /// Drives a key-wise reduction with the model-derived [`ReferenceTactic`], the analogue of the
    /// default [`super::reduce_trace`]. Same result contract; intended for differential testing of the
    /// two tactics against each other. Re-exported (doc-hidden) from the parent module as the sole
    /// public handle: the reference tactic is a testing and demonstration oracle, not a stable entry
    /// point to build on.
    pub fn reduce_trace_reference<'scope, Tr1, Bu, Tr2, L, P>(trace: Arranged<'scope, Tr1>, name: &str, logic: L, push: P) -> Arranged<'scope, TraceAgent<Tr2>>
    where
        Tr1: TraceReader<Batch: Navigable> + 'static,
        Tr2: Trace<Batch: Navigable, Time = Tr1::Time> + 'static,
        BatchCursor<Tr1>: Cursor<Time = Tr1::Time>,
        for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = BatchKey<'a, Tr1>, ValOwn: Data, Time = Tr2::Time>,
        Bu: Builder<Time=Tr2::Time, Output: Into<Tr2::Batch>, Input: Default> + 'static,
        L: FnMut(BatchKey<'_, Tr1>, &[(BatchVal<'_, Tr1>, BatchDiff<Tr1>)], &mut Vec<(BatchValOwn<Tr2>, BatchDiff<Tr2>)>, &mut Vec<(BatchValOwn<Tr2>, BatchDiff<Tr2>)>)+'static,
        P: FnMut(&mut Bu::Input, BatchKey<'_, Tr1>, &mut Vec<(BatchValOwn<Tr2>, Tr2::Time, BatchDiff<Tr2>)>) + 'static,
    {
        reduce_with_tactic(trace, name, ReferenceTactic::<Tr1::Batch, Tr2::Batch, Bu, L, P>::new(logic, push))
    }


    /// Updates an optional meet by an optional time.
    fn update_meet<T: Lattice+Clone>(meet: &mut Option<T>, other: Option<&T>) {
        if let Some(time) = other {
            if let Some(meet) = meet.as_mut() { meet.meet_assign(time); }
            else { *meet = Some(time.clone()); }
        }
    }

    /// The model-derived [`ReduceTactic`]. Structurally a twin of [`crate::operators::cursor::reduce::CursorTactic`]; only the
    /// per-key engine differs.
    pub struct ReferenceTactic<B1, B2, Bu, L, P>
    where
        B1: Navigable,
        B2: Navigable,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = TimeOf<B1>>,
    {
        logic: L,
        push: P,
        pending_keys: <B1::Cursor as Cursor>::KeyContainer,
        pending_time: <B1::Cursor as Cursor>::TimeContainer,
        next_pending_keys: <B1::Cursor as Cursor>::KeyContainer,
        next_pending_time: <B1::Cursor as Cursor>::TimeContainer,
        interesting_times: Vec<TimeOf<B1>>,
        new_interesting_times: Vec<TimeOf<B1>>,
        _marker: PhantomData<(B2, Bu)>,
    }

    impl<B1, B2, Bu, L, P> ReferenceTactic<B1, B2, Bu, L, P>
    where
        B1: Navigable,
        B2: Navigable,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = TimeOf<B1>>,
    {
        /// Construct a tactic that applies `logic` to each key and shapes output with `push`.
        pub fn new(logic: L, push: P) -> Self {
            ReferenceTactic {
                logic,
                push,
                pending_keys: <B1::Cursor as Cursor>::KeyContainer::with_capacity(0),
                pending_time: <B1::Cursor as Cursor>::TimeContainer::with_capacity(0),
                next_pending_keys: <B1::Cursor as Cursor>::KeyContainer::with_capacity(0),
                next_pending_time: <B1::Cursor as Cursor>::TimeContainer::with_capacity(0),
                interesting_times: Vec::new(),
                new_interesting_times: Vec::new(),
                _marker: PhantomData,
            }
        }
    }

    impl<B1, B2, Bu, L, P> ReduceTactic<TimeOf<B1>, B1, B2> for ReferenceTactic<B1, B2, Bu, L, P>
    where
        B1: Navigable,
        B2: Navigable,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = TimeOf<B1>>,
        Bu: Builder<Time = TimeOf<B1>, Output: Into<B2>, Input: Default>,
        L: FnMut(<B1::Cursor as Cursor>::Key<'_>, &[(<B1::Cursor as Cursor>::Val<'_>, <B1::Cursor as Cursor>::Diff)], &mut Vec<(<B2::Cursor as Cursor>::ValOwn, <B2::Cursor as Cursor>::Diff)>, &mut Vec<(<B2::Cursor as Cursor>::ValOwn, <B2::Cursor as Cursor>::Diff)>),
        P: FnMut(&mut Bu::Input, <B1::Cursor as Cursor>::Key<'_>, &mut Vec<(<B2::Cursor as Cursor>::ValOwn, TimeOf<B1>, <B2::Cursor as Cursor>::Diff)>),
    {
        fn retire(
            &mut self,
            source_batches: Vec<B1>,
            output_batches: Vec<B2>,
            input_batches: Vec<B1>,
            lower: &Antichain<TimeOf<B1>>,
            upper: &Antichain<TimeOf<B1>>,
            held: &Antichain<TimeOf<B1>>,
        ) -> (Option<Span<TimeOf<B1>, B2>>, Antichain<TimeOf<B1>>)
        {
            let mut produced = None;

            if held.elements().iter().any(|time| !upper.less_equal(time)) {

                let (mut source_cursor, ref source_storage) = cursor_list(source_batches);
                let (mut output_cursor, ref output_storage) = cursor_list(output_batches);
                let (mut batch_cursor, ref batch_storage) = cursor_list(input_batches);

                let mut output_updates = Vec::<(<B2::Cursor as Cursor>::ValOwn, TimeOf<B1>, <B2::Cursor as Cursor>::Diff)>::new();
                let mut builder = Bu::default();
                let mut buffer = Bu::Input::default();

                // Reuseable state for performing the computation.
                let mut thinker = ReferenceThinker::new();

                let mut pending_pos = 0;
                while batch_cursor.key_valid(batch_storage) || pending_pos < self.pending_keys.len() {

                    let key1 = self.pending_keys.get(pending_pos);
                    let key2 = batch_cursor.get_key(batch_storage);
                    let key = match (key1, key2) {
                        (Some(key1), Some(key2)) => ::std::cmp::min(key1, key2),
                        (Some(key1), None)       => key1,
                        (None, Some(key2))       => key2,
                        (None, None)             => unreachable!(),
                    };

                    let prior_pos = pending_pos;
                    self.interesting_times.clear();
                    while self.pending_keys.get(pending_pos) == Some(key) {
                        let owned_time = <B1::Cursor as Cursor>::owned_time(self.pending_time.index(pending_pos));
                        if !upper.less_equal(&owned_time) { self.interesting_times.push(owned_time); }
                        pending_pos += 1;
                    }

                    sort_dedup(&mut self.interesting_times);

                    if batch_cursor.get_key(batch_storage) == Some(key) || !self.interesting_times.is_empty() {

                        thinker.compute(
                            key,
                            (&mut source_cursor, source_storage),
                            (&mut output_cursor, output_storage),
                            (&mut batch_cursor, batch_storage),
                            &self.interesting_times,
                            &mut self.logic,
                            upper,
                            &mut output_updates,
                            held.elements(),
                            &mut self.new_interesting_times,
                        );

                        if batch_cursor.get_key(batch_storage) == Some(key) { batch_cursor.step_key(batch_storage); }

                        for pos in prior_pos .. pending_pos {
                            let owned_time = <B1::Cursor as Cursor>::owned_time(self.pending_time.index(pos));
                            if upper.less_equal(&owned_time) { self.new_interesting_times.push(owned_time); }
                        }
                        sort_dedup(&mut self.new_interesting_times);
                        for time in self.new_interesting_times.drain(..) {
                            self.next_pending_keys.push_ref(key);
                            self.next_pending_time.push_own(&time);
                        }

                        output_updates.sort_by(|x,y| x.0.cmp(&y.0));
                        (self.push)(&mut buffer, key, &mut output_updates);
                        output_updates.clear();
                        builder.push(&mut buffer);
                    }
                    else {
                        for pos in prior_pos .. pending_pos {
                            self.next_pending_keys.push_ref(self.pending_keys.index(pos));
                            self.next_pending_time.push_ref(self.pending_time.index(pos));
                        }
                    }
                }
                drop(thinker);

                let description = Description::new(lower.clone(), upper.clone(), Antichain::from_elem(<TimeOf<B1> as Timestamp>::minimum()));
                produced = Some(Span::new(description, builder.done().map(Into::into)));

                self.pending_keys.clear(); std::mem::swap(&mut self.next_pending_keys, &mut self.pending_keys);
                self.pending_time.clear(); std::mem::swap(&mut self.next_pending_time, &mut self.pending_time);

                let mut frontier = Antichain::<TimeOf<B1>>::new();
                let mut owned_time = <TimeOf<B1> as Timestamp>::minimum();
                for pos in 0 .. self.pending_time.len() {
                    <B1::Cursor as Cursor>::clone_time_onto(self.pending_time.index(pos), &mut owned_time);
                    frontier.insert_ref(&owned_time);
                }

                (produced, frontier)
            }
            else {
                (produced, held.clone())
            }
        }
    }

    /// The two-phase per-key engine.
    ///
    /// Phase 1 (determination) reads the input/output/seed *times* and closes them into `active`
    /// (the interesting times) and the pended set — Model.lean's `Reached`, directly. Phase 2
    /// (application) walks the same, still-loaded histories for *values* and emits corrections.
    pub struct ReferenceThinker<V1, V2, V, T, D1, D2> {
        input_history: ValueHistory<V1, T, D1>,
        output_history: ValueHistory<V2, T, D2>,
        batch_history: ValueHistory<V1, T, D1>,
        input_buffer: Vec<(V1, D1)>,
        output_buffer: Vec<(V, D2)>,
        update_buffer: Vec<(V, D2)>,
        output_produced: Vec<((V, T), D2)>,
        // Phase 1 (the compacted closure): synthetic reached times still to visit, the reached times
        // in play as join partners, scratch for the joins, and suffix-meets of the supplied times.
        synth_times: Vec<T>,
        times_current: Vec<T>,
        temporary: Vec<T>,
        meets: Vec<T>,
        // Reusable time-only buffers for phase 1's `TimeReplay` walks. Pooled here (rather than in
        // `ValueHistory`) so the reference tactic pays for them and the standard value walk does not.
        batch_times: Vec<T>,
        input_times: Vec<T>,
        output_times: Vec<T>,
        // The interesting (in-band reached) times, handed from phase 1 to phase 2.
        active: Vec<T>,
    }

    impl<V1, V2, V, T, D1, D2> ReferenceThinker<V1, V2, V, T, D1, D2>
    where
        V1: Copy + Ord,
        V2: Copy + Ord,
        V: Clone + Ord,
        T: Ord + Clone + Lattice + 'static,
        D1: Clone + crate::difference::Semigroup,
        D2: Clone + crate::difference::Semigroup,
    {
        pub fn new() -> Self {
            ReferenceThinker {
                input_history: ValueHistory::new(),
                output_history: ValueHistory::new(),
                batch_history: ValueHistory::new(),
                input_buffer: Vec::new(),
                output_buffer: Vec::new(),
                update_buffer: Vec::new(),
                output_produced: Vec::new(),
                synth_times: Vec::new(),
                times_current: Vec::new(),
                temporary: Vec::new(),
                meets: Vec::new(),
                batch_times: Vec::new(),
                input_times: Vec::new(),
                output_times: Vec::new(),
                active: Vec::new(),
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
            outputs: &mut Vec<(V, T, D2)>,
                held: &[T],
            new_interesting: &mut Vec<T>)
        where
            C1: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
            C2: Cursor<Key<'a> = K, Val<'a> = V2, ValOwn = V, Time = T, Diff = D2>,
            C3: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
            K: Copy + Ord,
            L: FnMut(K, &[(V1, D1)], &mut Vec<(V, D2)>, &mut Vec<(V, D2)>),
        {
            // ================== PHASE 1 — DETERMINATION (`Reached`, compacted) ==================
            // The interesting times are Model.lean's `Reached` — but computed the non-quadratic way.
            // Walk the input/output/seed times in increasing order; a time is *reached* only via a
            // seed (a batch update, a due pending time, or a synthetic join of earlier reached times);
            // a reached in-band time joins against the live partners to spawn more reached times, and
            // a join beyond `upper` is pended. The live partner sets are kept an antichain by
            // `advance_buffer_by(meet)` — coincident times collapse under the running meet — so this is
            // the closure without the all-pairs blow-up. Time-only: `TimeReplay` reads the histories
            // without touching values or stepping the underlying `history`, so phase 2 can walk them.
            {
                // Suffix-meets of the supplied (due pending) times, consumed as we pass them.
                self.meets.clear();
                self.meets.extend(times.iter().cloned());
                for index in (1 .. self.meets.len()).rev() {
                    self.meets[index-1] = self.meets[index-1].meet(&self.meets[index]);
                }

                // Build each history, then read it time-only (leaving it intact for phase 2).
                drop(self.batch_history.replay_key(batch_cursor, batch_storage, key, None));
                drop(self.input_history.replay_key(source_cursor, source_storage, key, None));
                drop(self.output_history.replay_key(output_cursor, output_storage, key, None));
                let mut batch_replay = self.batch_history.replay_times(&mut self.batch_times);
                let mut input_replay = self.input_history.replay_times(&mut self.input_times);
                let mut output_replay = self.output_history.replay_times(&mut self.output_times);

                self.synth_times.clear();
                self.times_current.clear();
                self.temporary.clear();
                self.active.clear();

                let mut times_slice = &times[..];
                let mut meets_slice = &self.meets[..];
                let mut meet: Option<T> = None;

                while let Some(next_time) = [   batch_replay.time(),
                                                times_slice.first(),
                                                input_replay.time(),
                                                output_replay.time(),
                                                self.synth_times.last(),
                                            ].into_iter().flatten().min().cloned() {

                    input_replay.step_while_time_is(&next_time);
                    output_replay.step_while_time_is(&next_time);

                    // Reached via a seed: a batch update, a due pending time, or a synthetic join.
                    // (Input/output times alone are not reached — they are only join partners.)
                    let mut interesting = batch_replay.step_while_time_is(&next_time);
                    if interesting { if let Some(m) = meet.as_ref() { batch_replay.advance_buffer_by(m); } }
                    while self.synth_times.last() == Some(&next_time) {
                        self.times_current.push(self.synth_times.pop().unwrap());
                        interesting = true;
                    }
                    while times_slice.first() == Some(&next_time) {
                        self.times_current.push(next_time.clone());
                        times_slice = &times_slice[1..];
                        meets_slice = &meets_slice[1..];
                        interesting = true;
                    }
                    // Absorb: a time at or above a reached time is itself reached.
                    interesting = interesting
                        || batch_replay.buffer().iter().any(|t| t.less_equal(&next_time))
                        || self.times_current.iter().any(|t| t.less_equal(&next_time));

                    if !upper_limit.less_equal(&next_time) {
                        // A reached in-band time is `active`; join it against the live partners —
                        // input/output (`joinBase`) and reached-so-far (`joinActive`) — for new times.
                        if interesting {
                            self.active.push(next_time.clone());
                            if let Some(m) = meet.as_ref() { input_replay.advance_buffer_by(m); output_replay.advance_buffer_by(m); }
                            self.temporary.extend(input_replay.buffer().iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                            self.temporary.extend(output_replay.buffer().iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                        }
                        self.temporary.extend(batch_replay.buffer().iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                        self.temporary.extend(self.times_current.iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                        sort_dedup(&mut self.temporary);

                        let synth_len = self.synth_times.len();
                        for time in self.temporary.drain(..) {
                            if upper_limit.less_equal(&time) { new_interesting.push(time); }  // pended
                            else { self.synth_times.push(time); }                             // reached, later
                        }
                        if self.synth_times.len() > synth_len {
                            self.synth_times.sort_by(|x,y| y.cmp(x));
                            self.synth_times.dedup();
                        }
                    }
                    else if interesting {
                        new_interesting.push(next_time.clone());
                    }

                    // Running meet (a lower bound on every time still to visit); compact the reached
                    // partners `times_current` with it.
                    meet = None;
                    update_meet(&mut meet, batch_replay.meet());
                    update_meet(&mut meet, input_replay.meet());
                    update_meet(&mut meet, output_replay.meet());
                    for time in self.synth_times.iter() { update_meet(&mut meet, Some(time)); }
                    update_meet(&mut meet, meets_slice.first());
                    if let Some(m) = meet.as_ref() {
                        for time in self.times_current.iter_mut() { *time = time.join(m); }
                    }
                    sort_dedup(&mut self.times_current);
                }

                sort_dedup(&mut self.active);
            }

            // ===================== PHASE 2 — APPLICATION (`emit_correct`) =====================
            // Walk `self.active` in order over the SAME per-key edits (via `replay`, no cursor), keep
            // the input/output accumulations tight by advancing to the meet of the times still to be
            // produced, apply `logic`, and emit corrections.
            {
                self.meets.clear();
                self.meets.extend(self.active.iter().cloned());
                for index in (1 .. self.meets.len()).rev() {
                    self.meets[index-1] = self.meets[index-1].meet(&self.meets[index]);
                }

                // Walk the histories loaded (and left intact) by phase 1 — no cursor re-read, no
                // rebuild, no re-sort; just a fresh walk of the same sorted `history` for values.
                let mut batch_replay = self.batch_history.walk();
                let mut input_replay = self.input_history.walk();
                let mut output_replay = self.output_history.walk();

                self.output_produced.clear();

                for index in 0 .. self.active.len() {
                    let next_time = self.active[index].clone();
                    let meet = self.meets[index].clone();

                    // Phase 2 visits only the active times, so at each we must catch up the histories to
                    // include every edit that will contribute to the accumulation at `next_time` (edits
                    // at non-active times count too). `history` is sorted by the total `Ord` and `step`
                    // pops the least, so we step the `Ord`-prefix `t <= next_time`, NOT the partial
                    // `t.less_equal(next_time)`: the partial order interleaves with the sort, so an
                    // `Ord`-earlier time incomparable to `next_time` would halt a `less_equal` walk early
                    // and strand later edits that *are* `less_equal(next_time)`. Stepping the `Ord`-prefix
                    // takes a superset; the `less_equal` filter below then selects the true `<= next_time`
                    // edits. (Assembling with `less_equal` while stepping with `<=` is what the value-aware
                    // cursor does via its full time-order frontier walk.)
                    while input_replay.time().map_or(false, |t| *t <= next_time) { input_replay.step(); }
                    while batch_replay.time().map_or(false, |t| *t <= next_time) { batch_replay.step(); }
                    while output_replay.time().map_or(false, |t| *t <= next_time) { output_replay.step(); }
                    input_replay.advance_buffer_by(&meet);
                    batch_replay.advance_buffer_by(&meet);
                    output_replay.advance_buffer_by(&meet);

                    debug_assert!(self.input_buffer.is_empty());
                    for ((value, time), diff) in input_replay.buffer().iter() {
                        if time.less_equal(&next_time) { self.input_buffer.push((*value, diff.clone())); }
                    }
                    for ((value, time), diff) in batch_replay.buffer().iter() {
                        if time.less_equal(&next_time) { self.input_buffer.push((*value, diff.clone())); }
                    }
                    crate::consolidation::consolidate(&mut self.input_buffer);

                    for ((value, time), diff) in output_replay.buffer().iter() {
                        if time.less_equal(&next_time) { self.output_buffer.push((C2::owned_val(*value), diff.clone())); }
                    }
                    for ((value, time), diff) in self.output_produced.iter() {
                        if time.less_equal(&next_time) { self.output_buffer.push((value.clone(), diff.clone())); }
                    }
                    crate::consolidation::consolidate(&mut self.output_buffer);

                    if !self.input_buffer.is_empty() || !self.output_buffer.is_empty() {
                        logic(key, &self.input_buffer[..], &mut self.output_buffer, &mut self.update_buffer);
                        self.input_buffer.clear();
                        self.output_buffer.clear();

                        crate::consolidation::consolidate(&mut self.update_buffer);
                        if !self.update_buffer.is_empty() {

                            assert!(held.iter().any(|time| time.less_equal(&next_time)), "failed to find capability");
                            for (val, diff) in self.update_buffer.drain(..) {
                                self.output_produced.push(((val.clone(), next_time.clone()), diff.clone()));
                                outputs.push((val, next_time.clone(), diff));
                            }

                            for entry in &mut self.output_produced { (entry.0).1.join_assign(&meet); }
                            crate::consolidation::consolidate(&mut self.output_produced);
                        }
                    }
                }
            }
        }
    }
}
