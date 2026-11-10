//! A key-wise reduction over several co-arranged inputs.
//!
//! The driver mirrors [`reduce_with_tactic`](crate::operators::reduce::reduce_with_tactic), with one
//! source trace per input. Batches reach the tactic tagged by their input, `(i, batch)`, so any
//! [`ReduceTactic`] whose backend reads tagged batches runs unchanged.
//!
//! The interval retired is bounded by the meet of the inputs' progress. An input running ahead of
//! the others can hand the tactic batches holding times beyond the interval. The tactic must defer
//! the times it finds there, as it does output times.

use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::channels::pact::Pipeline;

use crate::operators::arrange::{Arranged, TraceAgent};
use crate::operators::reduce::ReduceTactic;
use crate::trace::{ExertionLogic, Trace, TraceReader};

/// Per-input state of the driver.
struct Side<Tr: TraceReader> {
    trace: Tr,
    /// Received batches not yet handed to the tactic, with their capabilities, in order.
    queued: Vec<(CapabilitySet<Tr::Time>, Tr::Batch, Antichain<Tr::Time>)>,
    /// The upper of the last received batch, advanced past empty batches in the trace.
    received: Antichain<Tr::Time>,
    /// The bound through which batches have been handed to the tactic, a batch boundary.
    handed: Antichain<Tr::Time>,
}

impl<Tr: TraceReader> Side<Tr> {
    fn new(trace: Tr) -> Self {
        let min = Antichain::from_elem(<Tr::Time as Timestamp>::minimum());
        Side { trace, queued: Vec::new(), received: min.clone(), handed: min }
    }
}

/// Drives a key-wise reduction over `inputs`, at least one, using a supplied [`ReduceTactic`].
///
/// The tactic sees each batch tagged by the index of the input it came from. Its output at a time
/// `t` must be a function of each key's input accumulations at `t`. Beyond the contract of
/// [`ReduceTactic::retire`], it must defer every time at or beyond `upper` that a new input batch
/// holds, and leave its input unread only when no held time lies below `upper`.
pub fn co_reduce_with_tactic<'scope, Tr1, Tr2, T>(
    inputs: Vec<Arranged<'scope, Tr1>>,
    name: &str,
    mut tactic: T,
) -> Arranged<'scope, TraceAgent<Tr2>>
where
    Tr1: TraceReader + 'static,
    Tr2: Trace<Time = Tr1::Time> + 'static,
    T: ReduceTactic<Tr1::Time, (usize, Tr1::Batch), Tr2::Batch> + 'static,
{
    assert!(!inputs.is_empty(), "co_reduce_with_tactic: no inputs");
    let scope = inputs[0].stream.scope();
    let mut builder = OperatorBuilder::new(name.to_string(), scope);
    let operator_info = builder.operator_info();
    let (output, stream) = builder.new_output();
    let mut output = OutputBuilder::from(output);
    let mut handles = Vec::with_capacity(inputs.len());
    let mut sides = Vec::with_capacity(inputs.len());
    for input in inputs {
        handles.push(builder.new_input(input.stream, Pipeline));
        sides.push(Side::new(input.trace));
    }

    let logger = scope.worker().logger_for::<crate::logging::DifferentialEventBuilder>("differential/arrange").map(Into::into);
    let activator = Some(scope.activator_for(std::rc::Rc::clone(&operator_info.address)));
    let mut empty = Tr2::new(operator_info.clone(), logger.clone(), activator);
    if let Some(exert_logic) = scope.worker().config().get::<ExertionLogic>("differential/default_exert_logic").cloned() {
        empty.set_exert_logic(exert_logic);
    }
    let (mut output_reader, mut output_writer) = TraceAgent::new(empty, operator_info, logger);
    let result_trace = output_reader.clone();

    builder.build(move |_capabilities| {
        // Capabilities for the interesting times the tactic withholds. Queued batches hold their own.
        let mut capabilities = CapabilitySet::<Tr1::Time>::default();
        let mut upper_limit = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());
        let mut lower_limit = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());

        move |frontiers| {
            let mut output = output.activate();

            lower_limit.clone_from(&upper_limit);

            for (side, handle) in sides.iter_mut().zip(handles.iter_mut()) {
                handle.for_each(|capability, batches| {
                    let capability = capability.retain_stamp(0);
                    // A batch keeps its capability until it is handed over. An empty span adds no
                    // interesting time, so only its upper is kept.
                    for batch in batches.drain(..) {
                        side.received.clone_from(batch.upper());
                        if let Some(inner) = batch.inner {
                            side.queued.push((capability.clone(), inner, batch.desc.lower().clone()));
                        }
                    }
                });
            }

            // Each input's progress, as in the unary driver, and the meet of them all.
            let mut limit = Antichain::new();
            for (side, frontier) in sides.iter_mut().zip(frontiers) {
                side.trace.advance_upper(&mut side.received);
                let mut joined = Antichain::new();
                crate::lattice::antichain_join_into(&side.received.borrow()[..], &frontier.frontier()[..], &mut joined);
                for time in joined { limit.insert(time); }
            }
            upper_limit = limit;

            if upper_limit != lower_limit {

                // The history each input had before its newly handed batches.
                let mut source_batches = Vec::new();
                for (index, side) in sides.iter_mut().enumerate() {
                    let batches = side.trace.batches_through(side.handed.borrow()).expect("failed to acquire source batches");
                    source_batches.extend(batches.into_iter().map(|b| (index, b)));
                }
                let output_batches = output_reader.batches_through(lower_limit.borrow()).expect("failed to acquire output batches");

                // Hand over every queued batch, unless no capability, held or queued, lies below
                // `upper_limit`. The tactic then returns before reading its input, which would
                // lose the batches, so they stay queued. Otherwise a handed batch's times at or
                // beyond `upper_limit` are the tactic's to defer, as it does the output times it
                // finds there. Holding back only some batches could strand a later one's times
                // below `upper_limit`, and waiting for a batch boundary could deadlock a loop.
                let below = |caps: &CapabilitySet<Tr1::Time>| caps.iter().any(|c| !upper_limit.less_equal(c.time()));
                let hand = below(&capabilities) || sides.iter().any(|side| side.queued.iter().any(|(caps, _, _)| below(caps)));
                let mut input_batches = Vec::new();
                for (index, side) in sides.iter_mut().enumerate() {
                    let take = if hand { side.queued.len() } else { 0 };
                    for (caps, batch, _) in side.queued.drain(..take) {
                        for capability in caps.iter() { capabilities.insert(capability.clone()); }
                        input_batches.push((index, batch));
                    }
                    side.handed = match side.queued.first() {
                        Some((_, _, lower)) => lower.clone(),
                        None => side.received.clone(),
                    };
                }

                let held: Antichain<Tr1::Time> = capabilities.iter().map(|c| c.time().clone()).collect();
                let (produced, new_frontier) = tactic.retire(source_batches, output_batches, input_batches, &lower_limit, &upper_limit, &held);

                debug_assert!(
                    produced.as_ref().is_none_or(|batch| batch.lower() == &lower_limit && batch.upper() == &upper_limit),
                    "ReduceTactic::retire output must span [lower, upper)",
                );

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

                capabilities.downgrade(new_frontier);
                output_writer.seal(upper_limit.clone());

                for side in sides.iter_mut() {
                    side.trace.set_logical_compaction(upper_limit.borrow());
                    side.trace.set_physical_compaction(side.handed.borrow());
                }
                output_reader.set_logical_compaction(upper_limit.borrow());
                output_reader.set_physical_compaction(upper_limit.borrow());
            }

            output_writer.exert();
        }
    });

    Arranged { stream, trace: result_trace }
}
