//! Dataflow operator for delta joins over partially ordered timestamps.
//!
//! Given multiple streams of updates `(data, time, diff)` that are each
//! defined over the same partially ordered `time`, we want to form the
//! full cross-join of all relations (we will *later* apply some filters
//! and instead equijoin on keys).
//!
//! The "correct" output is the outer join of these triples, where
//!   1. The `data` entries are just tuple'd up together,
//!   2. The `time` entries are subjected to the lattice `join` operator,
//!   3. The `diff` entries are multiplied.
//!
//! One way to produce the correct output is to form independent dataflow
//! fragments for each input stream, such that each intended output is then
//! produced by exactly one of these input streams.
//!
//! There are several incorrect ways one might do this, but here is one way
//! that I hope is not incorrect:
//!
//! Each input stream of updates is joined with each other input collection,
//! where each input update is matched against each other input update that
//! has a `time` that is less-than the input update's `time`, *UNDER A TOTAL
//! ORDER ON `time`*. The output are the `(data, time, diff)` entries that
//! follow the rules above, except that we additionally preserve the input's
//! initial `time` as well, for use in subsequent joins with the other input
//! collections.
//!
//! There are some caveats about ties, and we should treat each `time` for
//! each input as occuring at distinct times, one after the other (so that
//! ties are resolved by the index of the input). There is also the matter
//! of logical compaction, which should not be done in a way that prevents
//! the correct determination of the total order comparison.

use std::collections::HashMap;

use timely::dataflow::Scope;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;
use timely::progress::Antichain;

use differential_dataflow::{ExchangeData, Collection, AsCollection, Hashable};
use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{Cursor, TraceReader, BatchReader};
use differential_dataflow::consolidation::{consolidate, consolidate_updates};

/// A binary equijoin that responds to updates on only its first input.
///
/// This operator responds to inputs of the form
///
/// ```ignore
/// ((key, val1, time1), initial_time, diff1)
/// ```
///
/// where `initial_time` is less or equal to `time1`, and produces as output
///
/// ```ignore
/// ((output_func(key, val1, val2), lub(time1, time2)), initial_time, diff1 * diff2)
/// ```
///
/// for each `((key, val2), time2, diff2)` present in `arrangement`, where
/// `time2` is less than `initial_time` *UNDER THE TOTAL ORDER ON TIMES*.
/// This last constraint is important to ensure that we correctly produce
/// all pairs of output updates across multiple `half_join` operators.
///
/// Notice that the time is hoisted up into data. The expectation is that
/// once out of the "delta flow region", the updates will be `delay`d to the
/// times specified in the payloads.
pub fn half_join<G, V, Tr, FF, CF, DOut, S>(
    stream: &Collection<G, (Tr::Key, V, G::Timestamp), Tr::R>,
    mut arrangement: Arranged<G, Tr>,
    frontier_func: FF,
    comparison: CF,
    mut output_func: S,
) -> Collection<G, (DOut, G::Timestamp), Tr::R>
where
    G: Scope,
    G::Timestamp: Lattice,
    V: ExchangeData,
    Tr: TraceReader<Time=G::Timestamp>+Clone+'static,
    Tr::Key: Ord+Hashable+ExchangeData,
    Tr::Val: Clone,
    Tr::Batch: BatchReader<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::Cursor: Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::R: Monoid+ExchangeData,
    FF: Fn(&G::Timestamp) -> G::Timestamp + 'static,
    CF: Fn(&G::Timestamp, &G::Timestamp) -> bool + 'static,
    DOut: Clone+'static,
    Tr::R: std::ops::Mul<Tr::R, Output=Tr::R>,
    S: FnMut(&Tr::Key, &V, &Tr::Val)->DOut+'static,
{
    // No need to block physical merging for this operator.
    arrangement.trace.set_physical_compaction(Antichain::new().borrow());
    let mut arrangement_trace = Some(arrangement.trace);
    let arrangement_stream = arrangement.stream;

    let mut stash = HashMap::new();
    let mut buffer = Vec::new();

    let exchange = Exchange::new(move |update: &((Tr::Key, V, G::Timestamp),G::Timestamp,Tr::R)| (update.0).0.hashed().into());

    // Stash for (time, diff) accumulation.
    let mut output_buffer = Vec::new();

    stream.inner.binary_frontier(&arrangement_stream, exchange, Pipeline, "HalfJoin", move |_,_| move |input1, input2, output| {

        // drain the first input, stashing requests.
        input1.for_each(|capability, data| {
            data.swap(&mut buffer);
            stash.entry(capability.retain())
                 .or_insert(Vec::new())
                 .extend(buffer.drain(..))
        });

        // Drain input batches; although we do not observe them, we want access to the input
        // to observe the frontier and to drive scheduling.
        input2.for_each(|_, _| { });

        if let Some(ref mut trace) = arrangement_trace {

            for (capability, proposals) in stash.iter_mut() {

                // defer requests at incomplete times.
                // TODO: Verify this is correct for TOTAL ORDER.
                if !input2.frontier.less_equal(capability.time()) {

                    let mut session = output.session(capability);

                    // Sort requests by key for in-order cursor traversal.
                    consolidate_updates(proposals);

                    let (mut cursor, storage) = trace.cursor();

                    for &mut ((ref key, ref val1, ref time), ref initial, ref mut diff) in proposals.iter_mut() {
                        // Use TOTAL ORDER to allow the release of `time`.
                        if !input2.frontier.frontier().iter().any(|t| comparison(t, initial)) {
                            cursor.seek_key(&storage, &key);
                            if cursor.get_key(&storage) == Some(&key) {
                                while let Some(val2) = cursor.get_val(&storage) {
                                    cursor.map_times(&storage, |t, d| {
                                        if comparison(t, initial) {
                                            output_buffer.push((t.join(time), d.clone()))
                                        }
                                    });
                                    consolidate(&mut output_buffer);
                                    for (time, count) in output_buffer.drain(..) {
                                        let dout = output_func(key, val1, val2);
                                        session.give(((dout, time), initial.clone(), count * diff.clone()));
                                    }
                                    cursor.step_val(&storage);
                                }
                                cursor.rewind_vals(&storage);
                            }
                            *diff = Tr::R::zero();
                        }
                    }

                    proposals.retain(|ptd| !ptd.2.is_zero());
                }
            }
        }

        // drop fully processed capabilities.
        stash.retain(|_,proposals| !proposals.is_empty());

        // The logical merging frontier depends on both input1 and stash.
        let mut frontier = timely::progress::frontier::Antichain::new();
        for time in input1.frontier().frontier().iter() {
            frontier.insert(frontier_func(time));
        }
        for key in stash.keys() {
            frontier.insert(frontier_func(key.time()));
        }
        arrangement_trace.as_mut().map(|trace| trace.set_logical_compaction(frontier.borrow()));

        if input1.frontier().is_empty() && stash.is_empty() {
            arrangement_trace = None;
        }

    }).as_collection()
}
