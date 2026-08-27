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

use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::channels::pact::Pipeline;

use crate::operators::arrange::{Arranged, TraceAgent};
use crate::trace::{Span, ExertionLogic, Trace, TraceReader};

/// Sort and deduplicate a list. Shared by the cursor and proxy tactics, which
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
