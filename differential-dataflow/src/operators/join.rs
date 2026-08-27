//! Match pairs of records based on a key.
//!
//! The various `join` implementations require that the units of each collection can be multiplied, and that
//! the multiplication distributes over addition. That is, we will repeatedly evaluate (a + b) * c as (a * c)
//! + (b * c), and if this is not equal to the former term, little is known about the actual output.
use std::collections::VecDeque;

use timely::Container;
use timely::container::NoopBuilder;
use timely::order::PartialOrder;
use timely::progress::Timestamp;
use timely::dataflow::Stream;
use timely::dataflow::operators::generic::{Operator, OutputBuilderSession};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::CapabilitySet;

use crate::lattice::Lattice;
use crate::operators::arrange::Arranged;
use crate::trace::TraceReader;

/// A type that can manage the joining of lists of batches.
///
/// The trait is parameterized by the output container `C`, not by the builder that assembles it: a tactic
/// yields finished containers, and how it produces them (pushing records into a [`timely::ContainerBuilder`], or
/// otherwise) is its own concern.
pub trait JoinTactic<T, B0, B1, C> {
    /// Prepare the join of two lists of batches into an iterator of output containers.
    ///
    /// The supplied `fresh` and `meet` indicate respectively which input is "novel", and should drive the
    /// join, as well as a lower bound on that input's times, so that the other input can be loaded
    /// compacted.
    fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, fresh: Fresh, meet: T) -> Box<dyn Iterator<Item = C>>;
}

/// Which input contributed the freshly-arrived batch of a deferred join unit.
///
/// The fresh batch's times all lie at or beyond the capability, so its side is not advanced by the
/// capability's meet; the opposing accumulated trace is. The marker also selects which queue a unit
/// joins, so a burst on one input cannot starve the other.
pub enum Fresh {
    /// The first input (`B0`) contributed the fresh batch.
    Input0,
    /// The second input (`B1`) contributed the fresh batch.
    Input1,
}

pub use crate::operators::cursor::join::join_traces;

/// Drives an equijoin of two traces using a supplied [`JoinTactic`].
///
/// This is the general join operator: it does the dataflow plumbing (frontiers, capabilities, trace
/// compaction) and routes the per-batch work through the tactic. It requires only `TraceReader` of its
/// inputs, never `Navigable`: it extracts trace batches via `spans_through`, and building cursors over
/// them (if that is how the join proceeds) is the tactic's concern.
pub fn join_with_tactic<'scope, Tr1, Tr2, T, C>(arranged1: Arranged<'scope, Tr1>, arranged2: Arranged<'scope, Tr2>, name: &str, mut tactic: T) -> Stream<'scope, Tr1::Time, C>
where
    Tr1: TraceReader+'static,
    Tr2: TraceReader<Time = Tr1::Time>+'static,
    T: JoinTactic<Tr1::Time, Tr1::Batch, Tr2::Batch, C>+'static,
    C: Container + 'static,
{
    // Rename traces for symmetry from here on out.
    let mut trace1 = arranged1.trace;
    let mut trace2 = arranged2.trace;

    let scope = arranged1.stream.scope();
    arranged1.stream.binary_frontier(arranged2.stream, Pipeline, Pipeline, name, move |capability, info| {

        // Acquire an activator to reschedule the operator when it has unfinished work.
        use timely::scheduling::Activator;
        let activations = scope.activations();
        let activator = Activator::new(info.address, activations);

        // Our initial invariants are that for each trace, physical compaction is less or equal the trace's upper bound.
        // These invariants ensure that we can reference observed batch frontiers from `_start_upper` onward, as long as
        // we maintain our physical compaction capabilities appropriately. These assertions are tested as we load up the
        // initial work for the two traces, and before the operator is constructed.

        // Acknowledged frontier for each input.
        // These two are used exclusively to track batch boundaries on which we may want/need to call `spans_through`.
        // They will drive our physical compaction of each trace, and we want to maintain at all times that each is beyond
        // the physical compaction frontier of their corresponding trace.
        // Should we ever *drop* a trace, these are 1. much harder to maintain correctly, but 2. no longer used.
        use timely::progress::frontier::Antichain;
        let mut acknowledged1 = Antichain::from_elem(Tr1::Time::minimum());
        let mut acknowledged2 = Antichain::from_elem(Tr1::Time::minimum());

        // Deferred work, as `(capability, iterator)` pairs bucketed by which input carried the fresh
        // batch (so a burst on one input cannot starve the other). The driver owns the capabilities and
        // the fuel budget; each iterator, prepared by the tactic, yields the output containers to ship
        // under its paired capability, and is dropped once it goes dry.
        let mut todo0: VecDeque<(CapabilitySet<Tr1::Time>, Box<dyn Iterator<Item = C>>)> = VecDeque::new();
        let mut todo1: VecDeque<(CapabilitySet<Tr1::Time>, Box<dyn Iterator<Item = C>>)> = VecDeque::new();

        // We'll unload the initial batches here, to put ourselves in a less non-deterministic state to start.
        trace1.map_spans(|batch1| {
            acknowledged1.clone_from(batch1.upper());
            // No `todo1` work here, because we haven't accepted anything into `batches2` yet.
            // It is effectively "empty", because we choose to drain `trace1` before `trace2`.
            // Once we start streaming batches in, we will need to respond to new batches from
            // `input1` with logic that would have otherwise been here. Check out the next loop
            // for the structure.
        });
        // At this point, `ack1` should exactly equal `trace1.read_upper()`, as they are both determined by
        // iterating through batches and capturing the upper bound. This is a great moment to assert that
        // `trace1`'s physical compaction frontier is before the frontier of completed times in `trace1`.
        // TODO: in the case that this does not hold, instead "upgrade" the physical compaction frontier.
        assert!(PartialOrder::less_equal(&trace1.get_physical_compaction(), &acknowledged1.borrow()));

        // We capture batch2's batches first and establish work second to avoid taking a `RefCell` lock
        // on both traces at the same time, as they could be the same trace and this would panic.
        let mut batch2_list = Vec::new();
        trace2.map_spans(|batch2| {
            acknowledged2.clone_from(batch2.upper());
            batch2_list.push(batch2.clone());
        });
        // At this point, `ack2` should exactly equal `trace2.read_upper()`, as they are both determined by
        // iterating through batches and capturing the upper bound. This is a great moment to assert that
        // `trace2`'s physical compaction frontier is before the frontier of completed times in `trace2`.
        // TODO: in the case that this does not hold, instead "upgrade" the physical compaction frontier.
        assert!(PartialOrder::less_equal(&trace2.get_physical_compaction(), &acknowledged2.borrow()));

        // Batches wholly at or before these frontiers were joined by the start-up loading
        // above; batches arriving on the input streams are ignored up to them. Beyond them,
        // every non-empty arriving batch must be joined, even when `acknowledged` has been
        // advanced past it: `advance_upper` consults the shared trace, whose merges may have
        // consolidated an in-flight batch's updates away (e.g. an add/remove pair collapsing
        // once logical compaction equates their times). The trace's emptiness there is valid
        // only for readers at or beyond the compaction frontier, while our consumers may read
        // finer times; the raw batch still owes them its updates. (#801)
        let preload_upper1 = acknowledged1.clone();
        let preload_upper2 = acknowledged2.clone();

        // Load up deferred work joining each captured `trace2` batch against `trace1`.
        for batch2 in batch2_list.into_iter() {
            // Empty batches carry no updates, and have nothing to join.
            let Some(updates2) = batch2.inner else { continue };
            // It is safe to ask for `ack1` because we have confirmed it to be in advance of `distinguish_since`.
            let trace1_storage = trace1.batches_through(acknowledged1.borrow()).unwrap();
            // We could downgrade the capability here, but doing so is a bit complicated mathematically.
            // TODO: downgrade the capability by searching out the one time in `batch2.lower()` and not
            // in `batch2.upper()`. Only necessary for non-empty batches, as empty batches may not have
            // that property.
            let work = tactic.prep(trace1_storage, vec![updates2], Fresh::Input1, capability.time().clone());
            todo1.push_back((CapabilitySet::from_elem(capability.clone()), work));
        }

        // Droppable handles to shared trace data structures.
        let mut trace1_option = Some(trace1);
        let mut trace2_option = Some(trace2);

        move |(input1, frontier1), (input2, frontier2), output| {

            // 1. Consuming input.
            //
            // The join computation repeatedly accepts batches of updates from each of its inputs.
            //
            // For each accepted batch, it prepares a work-item to join the batch against previously "accepted"
            // updates from its other input. It is important to track which updates have been accepted, because
            // we use a shared trace and there may be updates present that are in advance of this accepted bound.
            //
            // Batches are accepted: 1. in bulk at start-up (above), 2. as we observe them in the input stream,
            // and 3. if the trace can confirm a region of empty space directly following our accepted bound.
            // This last case is a consequence of our inability to transmit empty batches, as they may be formed
            // in the absence of timely dataflow capabilities.

            // Drain input 1, prepare work.
            input1.for_each(|capability, data| {
                // This test *should* always pass, as we only drop a trace in response to the other input emptying.
                if let Some(ref mut trace2) = trace2_option {
                    let capability = capability.retain_stamp(0);
                    // The lattice meet of the stamp's elements lower bounds all output
                    // times this batch can produce.
                    let meet = capability.iter().map(|c| c.time().clone()).reduce(|a, b| a.meet(&b));
                    for batch1 in data.drain(..) {
                        // An arriving batch must lie wholly on one side of the preload boundary,
                        // and wholly on one side of `acknowledged1`: both frontiers are drawn from
                        // the lattice of stream batch boundaries (received uppers, and uppers of
                        // trace merges of whole stream batches). A batch spanning the former would
                        // be partially double-processed; one spanning the latter mis-accounted.
                        assert!(
                            PartialOrder::less_equal(batch1.upper(), &preload_upper1) ||
                            PartialOrder::less_equal(&preload_upper1, batch1.lower()),
                            "batch spans the preload boundary",
                        );
                        assert!(
                            PartialOrder::less_equal(&acknowledged1, batch1.lower()) ||
                            PartialOrder::less_equal(batch1.upper(), &acknowledged1),
                            "batch spans the acknowledged frontier",
                        );

                        // Ignore any pre-loaded data, which was joined at start-up. Note that this
                        // is a test against the preload boundary, not against `acknowledged1`: the
                        // latter can be advanced past an in-flight batch by `advance_upper`, when
                        // trace merges consolidate the batch's updates away, and such a batch must
                        // still be joined (its updates remain real at times finer than the trace's
                        // compaction frontier, and no other work item has accounted for them).
                        if !PartialOrder::less_equal(batch1.upper(), &preload_upper1) {
                            if let Some(updates1) = batch1.inner.clone() {
                                // It is safe to ask for `ack2` as we validated that it was at least `get_physical_compaction()`
                                // at start-up, and have held back physical compaction ever since.
                                let trace2_storage = trace2.batches_through(acknowledged2.borrow()).unwrap();
                                let work = tactic.prep(vec![updates1], trace2_storage, Fresh::Input0, meet.clone().expect("non-empty stamp"));
                                todo0.push_back((capability.clone(), work));
                            }

                            // To update `acknowledged1` we might presume that `batch1.lower` should equal it, but we
                            // may have skipped over empty batches. Still, the batches are in-order, and we should be
                            // able to just assume the most recent `batch1.upper`, unless `advance_upper` has already
                            // moved `acknowledged1` past this batch, in which case we keep the further frontier.
                            if PartialOrder::less_equal(&acknowledged1, batch1.lower()) {
                                debug_assert!(PartialOrder::less_equal(&acknowledged1, batch1.upper()));
                                acknowledged1.clone_from(batch1.upper());
                            }
                        }
                    }
                }
                else { panic!("`trace2_option` dropped before `input1` emptied!"); }
            });

            // Drain input 2, prepare work.
            input2.for_each(|capability, data| {
                // This test *should* always pass, as we only drop a trace in response to the other input emptying.
                if let Some(ref mut trace1) = trace1_option {
                    let capability = capability.retain_stamp(0);
                    // The lattice meet of the stamp's elements lower bounds all output
                    // times this batch can produce.
                    let meet = capability.iter().map(|c| c.time().clone()).reduce(|a, b| a.meet(&b));
                    for batch2 in data.drain(..) {
                        // An arriving batch must lie wholly on one side of the preload boundary,
                        // and wholly on one side of `acknowledged2`: both frontiers are drawn from
                        // the lattice of stream batch boundaries (received uppers, and uppers of
                        // trace merges of whole stream batches). A batch spanning the former would
                        // be partially double-processed; one spanning the latter mis-accounted.
                        assert!(
                            PartialOrder::less_equal(batch2.upper(), &preload_upper2) ||
                            PartialOrder::less_equal(&preload_upper2, batch2.lower()),
                            "batch spans the preload boundary",
                        );
                        assert!(
                            PartialOrder::less_equal(&acknowledged2, batch2.lower()) ||
                            PartialOrder::less_equal(batch2.upper(), &acknowledged2),
                            "batch spans the acknowledged frontier",
                        );

                        // Ignore any pre-loaded data, which was joined at start-up. Note that this
                        // is a test against the preload boundary, not against `acknowledged2`: the
                        // latter can be advanced past an in-flight batch by `advance_upper`, when
                        // trace merges consolidate the batch's updates away, and such a batch must
                        // still be joined (its updates remain real at times finer than the trace's
                        // compaction frontier, and no other work item has accounted for them).
                        if !PartialOrder::less_equal(batch2.upper(), &preload_upper2) {
                            if let Some(updates2) = batch2.inner.clone() {
                                // It is safe to ask for `ack1` as we validated that it was at least `get_physical_compaction()`
                                // at start-up, and have held back physical compaction ever since.
                                let trace1_storage = trace1.batches_through(acknowledged1.borrow()).unwrap();
                                let work = tactic.prep(trace1_storage, vec![updates2], Fresh::Input1, meet.clone().expect("non-empty stamp"));
                                todo1.push_back((capability.clone(), work));
                            }

                            // To update `acknowledged2` we might presume that `batch2.lower` should equal it, but we
                            // may have skipped over empty batches. Still, the batches are in-order, and we should be
                            // able to just assume the most recent `batch2.upper`, unless `advance_upper` has already
                            // moved `acknowledged2` past this batch, in which case we keep the further frontier.
                            if PartialOrder::less_equal(&acknowledged2, batch2.lower()) {
                                debug_assert!(PartialOrder::less_equal(&acknowledged2, batch2.upper()));
                                acknowledged2.clone_from(batch2.upper());
                            }
                        }
                    }
                }
                else { panic!("`trace1_option` dropped before `input2` emptied!"); }
            });

            // Advance acknowledged frontiers through any empty regions that we may not receive as batches.
            if let Some(trace1) = trace1_option.as_mut() {
                trace1.advance_upper(&mut acknowledged1);
            }
            if let Some(trace2) = trace2_option.as_mut() {
                trace2.advance_upper(&mut acknowledged2);
            }

            // 2. Join computation.
            //
            // For each of the inputs, we do some amount of work (measured in terms of number
            // of output records produced). This is meant to yield control to allow downstream
            // operators to consume and reduce the output, but it it also means to provide some
            // degree of responsiveness. There is a potential risk here that if we fall behind
            // then the increasing queues hold back physical compaction of the underlying traces
            // which results in unintentionally quadratic processing time (each batch of either
            // input must scan all batches from the other input).

            // Perform some amount of outstanding work by pulling the deferred iterators and shipping the
            // containers they yield. Each direction drains against its own half of the budget, so a burst
            // on one input cannot starve the other. We reschedule the operator whenever any work remains,
            // which is observable directly: an iterator has yet to yield `None`. The budget is split from
            // `2_000_000` to preserve the historical `1_000_000` of progress per input each activation.
            // The driver only ships finished containers (`give_container`), never pushing records, so it
            // pins the operator output to `NoopBuilder<C>` — the builder for exactly this "containers ready
            // to go" case, which is a `ContainerBuilder` for any `C` without further bounds.
            let output: &mut OutputBuilderSession<'_, Tr1::Time, NoopBuilder<C>> = output;
            let mut drain = |queue: &mut VecDeque<(CapabilitySet<Tr1::Time>, Box<dyn Iterator<Item = C>>)>, mut fuel: isize| {
                while fuel >= 0 {
                    let Some((capability, work)) = queue.front_mut() else { break };
                    match work.next() {
                        Some(mut container) => {
                            fuel -= container.record_count() as isize;
                            output.session_with_builder(&*capability).give_container(&mut container);
                        }
                        None => { queue.pop_front(); }
                    }
                }
            };
            let fuel = 2_000_000;
            drain(&mut todo0, fuel / 2);
            drain(&mut todo1, fuel / 2);
            if !todo0.is_empty() || !todo1.is_empty() {
                activator.activate();
            }

            // 3. Trace maintenance.
            //
            // Importantly, we use `input.frontier()` here rather than `acknowledged` to track
            // the progress of an input, because should we ever drop one of the traces we will
            // lose the ability to extract information from anything other than the input.
            // For example, if we dropped `trace2` we would not be able to use `advance_upper`
            // to keep `acknowledged2` up to date wrt empty batches, and would hold back logical
            // compaction of `trace1`.

            // Maintain `trace1`. Drop if `input2` is empty, or advance based on future needs.
            if let Some(trace1) = trace1_option.as_mut() {
                if frontier2.is_empty() { trace1_option = None; }
                else {
                    // Allow `trace1` to compact logically up to the frontier we may yet receive,
                    // in the opposing input (`input2`). All `input2` times will be beyond this
                    // frontier, and joined times only need to be accurate when advanced to it.
                    trace1.set_logical_compaction(frontier2.frontier());
                    // Allow `trace1` to compact physically up to the upper bound of batches we
                    // have received in its input (`input1`). We will not require a cursor that
                    // is not beyond this bound.
                    trace1.set_physical_compaction(acknowledged1.borrow());
                }
            }

            // Maintain `trace2`. Drop if `input1` is empty, or advance based on future needs.
            if let Some(trace2) = trace2_option.as_mut() {
                if frontier1.is_empty() { trace2_option = None;}
                else {
                    // Allow `trace2` to compact logically up to the frontier we may yet receive,
                    // in the opposing input (`input1`). All `input1` times will be beyond this
                    // frontier, and joined times only need to be accurate when advanced to it.
                    trace2.set_logical_compaction(frontier1.frontier());
                    // Allow `trace2` to compact physically up to the upper bound of batches we
                    // have received in its input (`input2`). We will not require a cursor that
                    // is not beyond this bound.
                    trace2.set_physical_compaction(acknowledged2.borrow());
                }
            }
        }
    })
}
