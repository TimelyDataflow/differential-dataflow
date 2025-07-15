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
//! each input as occurring at distinct times, one after the other (so that
//! ties are resolved by the index of the input). There is also the matter
//! of logical compaction, which should not be done in a way that prevents
//! the correct determination of the total order comparison.

use std::collections::HashMap;
use std::ops::Mul;
use std::time::Instant;

use timely::container::{CapacityContainerBuilder, ContainerBuilder};
use timely::dataflow::{Scope, ScopeParent, StreamCore};
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::pushers::{Counter as PushCounter, Tee};
use timely::dataflow::operators::Operator;
use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;

use differential_dataflow::{ExchangeData, Collection, AsCollection, Hashable};
use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{Cursor, TraceReader};
use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::IntoOwned;

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
pub fn half_join<G, K, V, R, Tr, FF, CF, DOut, S>(
    stream: &Collection<G, (K, V, G::Timestamp), R>,
    arrangement: Arranged<G, Tr>,
    frontier_func: FF,
    comparison: CF,
    mut output_func: S,
) -> Collection<G, (DOut, G::Timestamp), <R as Mul<Tr::Diff>>::Output>
where
    G: Scope<Timestamp = Tr::Time>,
    K: Hashable + ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Monoid,
    Tr: for<'a> TraceReader<Key<'a> : IntoOwned<'a, Owned = K>, TimeGat<'a> : IntoOwned<'a, Owned = Tr::Time>>+Clone+'static,
    R: Mul<Tr::Diff, Output: Semigroup>,
    FF: Fn(&G::Timestamp, &mut Antichain<G::Timestamp>) + 'static,
    CF: Fn(Tr::TimeGat<'_>, &G::Timestamp) -> bool + 'static,
    DOut: Clone+'static,
    S: FnMut(&K, &V, Tr::Val<'_>)->DOut+'static,
{
    let output_func = move |session: &mut SessionFor<G, _>, k: &K, v1: &V, v2: Tr::Val<'_>, initial: &G::Timestamp, diff1: &R, output: &mut Vec<(G::Timestamp, Tr::Diff)>| {
        for (time, diff2) in output.drain(..) {
            let diff = diff1.clone() * diff2.clone();
            let dout = (output_func(k, v1, v2), time.clone());
            session.give((dout, initial.clone(), diff));
        }
    };
    half_join_internal_unsafe::<_, _, _, _, _, _,_,_,_, CapacityContainerBuilder<Vec<_>>>(stream, arrangement, frontier_func, comparison, |_timer, _count| false, output_func)
        .as_collection()
}

/// A session with lifetime `'a` in a scope `G` with a container builder `CB`.
///
/// This is a shorthand primarily for the reson of readability.
type SessionFor<'a, G, CB> =
    Session<'a,
        <G as ScopeParent>::Timestamp,
        CB,
        PushCounter<
            <G as ScopeParent>::Timestamp,
            <CB as ContainerBuilder>::Container,
            Tee<<G as ScopeParent>::Timestamp, <CB as ContainerBuilder>::Container>
        >
    >;

/// An unsafe variant of `half_join` where the `output_func` closure takes
/// additional arguments a vector of `time` and `diff` tuples as input and
/// writes its outputs at a container builder. The container builder
/// can, but isn't required to, accept `(data, time, diff)` triplets.
/// This allows for more flexibility, but is more error-prone.
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
/// output_func(session, key, val1, val2, initial_time, diff1, &[lub(time1, time2), diff2])
/// ```
///
/// for each `((key, val2), time2, diff2)` present in `arrangement`, where
/// `time2` is less than `initial_time` *UNDER THE TOTAL ORDER ON TIMES*.
///
/// The `yield_function` allows the caller to indicate when the operator should
/// yield control, as a function of the elapsed time and the number of matched
/// records. Note this is not the number of *output* records, owing mainly to
/// the number of matched records being easiest to record with low overhead.
pub fn half_join_internal_unsafe<G, K, V, R, Tr, FF, CF, Y, S, CB>(
    stream: &Collection<G, (K, V, G::Timestamp), R>,
    mut arrangement: Arranged<G, Tr>,
    frontier_func: FF,
    comparison: CF,
    yield_function: Y,
    mut output_func: S,
) -> StreamCore<G, CB::Container>
where
    G: Scope<Timestamp = Tr::Time>,
    K: Hashable + ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Monoid,
    Tr: for<'a> TraceReader<Key<'a> : IntoOwned<'a, Owned = K>, TimeGat<'a> : IntoOwned<'a, Owned = Tr::Time>>+Clone+'static,
    FF: Fn(&G::Timestamp, &mut Antichain<G::Timestamp>) + 'static,
    CF: Fn(Tr::TimeGat<'_>, &Tr::Time) -> bool + 'static,
    Y: Fn(std::time::Instant, usize) -> bool + 'static,
    S: FnMut(&mut SessionFor<G, CB>, &K, &V, Tr::Val<'_>, &G::Timestamp, &R, &mut Vec<(G::Timestamp, Tr::Diff)>) + 'static,
    CB: ContainerBuilder,
{
    // No need to block physical merging for this operator.
    arrangement.trace.set_physical_compaction(Antichain::new().borrow());
    let mut arrangement_trace = Some(arrangement.trace);
    let arrangement_stream = arrangement.stream;

    let mut stash = HashMap::new();

    let exchange = Exchange::new(move |update: &((K, V, G::Timestamp),G::Timestamp,R)| (update.0).0.hashed().into());

    // Stash for (time, diff) accumulation.
    let mut output_buffer = Vec::new();

    stream.inner.binary_frontier(&arrangement_stream, exchange, Pipeline, "HalfJoin", move |_,info| {

        // Acquire an activator to reschedule the operator when it has unfinished work.
        let activator = stream.scope().activator_for(info.address);

        move |input1, input2, output| {

            // drain the first input, stashing requests.
            input1.for_each(|capability, data| {
                stash.entry(capability.retain())
                    .or_insert(Vec::new())
                    .append(data)
            });

            // Drain input batches; although we do not observe them, we want access to the input
            // to observe the frontier and to drive scheduling.
            input2.for_each(|_, _| { });

            // Local variables to track if and when we should exit early.
            // The rough logic is that we fully process inputs and set their differences to zero,
            // stopping at any point. We clean up all of the zeros in buffers that did any work,
            // and reactivate at the end if the yield function still says so.
            let mut yielded = false;
            let timer = std::time::Instant::now();
            let mut work = 0;

            // New entries to introduce to the stash after processing.
            let mut stash_additions = HashMap::new();

            if let Some(ref mut trace) = arrangement_trace {

                for (capability, proposals) in stash.iter_mut() {

                    // Avoid computation if we should already yield.
                    // TODO: Verify this is correct for TOTAL ORDER.
                    yielded = yielded || yield_function(timer, work);
                    if !yielded && !input2.frontier.less_equal(capability.time()) {

                        let frontier = input2.frontier.frontier();

                        // Update yielded: We can only go from false to {false, true} as
                        // we're checking that `!yielded` holds before entering this block.
                        yielded = process_proposals::<G, _, _, _, _, _, _, _, _>(
                            &comparison,
                            &yield_function,
                            &mut output_func,
                            &mut output_buffer,
                            timer,
                            &mut work,
                            trace,
                            proposals,
                            output.session_with_builder(capability),
                            frontier
                        );

                        proposals.retain(|ptd| !ptd.2.is_zero());

                        // Determine the lower bound of remaining update times.
                        let mut antichain = Antichain::new();
                        for (_, initial, _) in proposals.iter() {
                            antichain.insert(initial.clone());
                        }
                        // Fast path: there is only one element in the antichain.
                        // All times in `proposals` must be greater or equal to it.
                        if antichain.len() == 1 && !antichain.less_equal(capability.time()) {
                            stash_additions
                                .entry(capability.delayed(&antichain[0]))
                                .or_insert(Vec::new())
                                .append(proposals);
                        }
                        else if antichain.len() > 1 {
                            // Any remaining times should peel off elements from `proposals`.
                            let mut additions = vec![Vec::new(); antichain.len()];
                            for (data, initial, diff) in proposals.drain(..) {
                                use timely::PartialOrder;
                                let position = antichain.iter().position(|t| t.less_equal(&initial)).unwrap();
                                additions[position].push((data, initial, diff));
                            }
                            for (time, addition) in antichain.into_iter().zip(additions) {
                                stash_additions
                                    .entry(capability.delayed(&time))
                                    .or_insert(Vec::new())
                                    .extend(addition);
                            }
                        }
                    }
                }
            }

            // If we yielded, re-activate the operator.
            if yielded {
                activator.activate();
            }

            // drop fully processed capabilities.
            stash.retain(|_,proposals| !proposals.is_empty());

            for (capability, proposals) in stash_additions.into_iter() {
                stash.entry(capability).or_insert(Vec::new()).extend(proposals);
            }

            // The logical merging frontier depends on both input1 and stash.
            let mut frontier = timely::progress::frontier::Antichain::new();
            for time in input1.frontier().frontier().iter() {
                frontier_func(time, &mut frontier);
            }
            for time in stash.keys() {
                frontier_func(time, &mut frontier);
            }
            arrangement_trace.as_mut().map(|trace| trace.set_logical_compaction(frontier.borrow()));

            if input1.frontier().is_empty() && stash.is_empty() {
                arrangement_trace = None;
            }
        }
    })
}

/// Outlined inner loop for `half_join_internal_unsafe` for reasons of performance.
///
/// Gives Rust/LLVM the opportunity to inline the loop body instead of inlining the loop and
/// leaving all calls in the loop body outlined.
///
/// Consumes proposals until the yield function returns `true` or all proposals are processed.
/// Leaves a zero diff in place for all proposals that were processed.
///
/// Returns `true` if the operator should yield.
fn process_proposals<G, Tr, CF, Y, S, CB, K, V, R>(
    comparison: &CF,
    yield_function: &Y,
    output_func: &mut S,
    mut output_buffer: &mut Vec<(<Tr as TraceReader>::Time, <Tr as TraceReader>::Diff)>,
    timer: Instant,
    work: &mut usize,
    trace: &mut Tr,
    proposals: &mut Vec<((K, V, <Tr as TraceReader>::Time), <Tr as TraceReader>::Time, R)>,
    mut session: SessionFor<G, CB>,
    frontier: AntichainRef<<Tr as TraceReader>::Time>
) -> bool
where
    G: Scope<Timestamp = Tr::Time>,
    Tr: for<'a> TraceReader<Key<'a> : IntoOwned<'a, Owned = K>, TimeGat<'a> : IntoOwned<'a, Owned = Tr::Time>>,
    CF: Fn(Tr::TimeGat<'_>, &Tr::Time) -> bool + 'static,
    Y: Fn(Instant, usize) -> bool + 'static,
    S: FnMut(&mut SessionFor<G, CB>, &K, &V, Tr::Val<'_>, &G::Timestamp, &R, &mut Vec<(G::Timestamp, Tr::Diff)>) + 'static,
    CB: ContainerBuilder,
    K: Ord,
    V: Ord,
    R: Monoid,
{
    // Sort requests by key for in-order cursor traversal.
    consolidate_updates(proposals);

    let (mut cursor, storage) = trace.cursor();
    let mut yielded = false;

    // Process proposals one at a time, stopping if we should yield.
    for ((ref key, ref val1, ref time), ref initial, ref mut diff1) in proposals.iter_mut() {
        // Use TOTAL ORDER to allow the release of `time`.
        yielded = yielded || yield_function(timer, *work);
        if !yielded && !frontier.iter().any(|t| comparison(<Tr::TimeGat<'_> as IntoOwned>::borrow_as(t), initial)) {
            use differential_dataflow::IntoOwned;
            cursor.seek_key(&storage, IntoOwned::borrow_as(key));
            if cursor.get_key(&storage) == Some(IntoOwned::borrow_as(key)) {
                while let Some(val2) = cursor.get_val(&storage) {
                    cursor.map_times(&storage, |t, d| {
                        if comparison(t, initial) {
                            let mut t = t.into_owned();
                            t.join_assign(time);
                            output_buffer.push((t, Tr::owned_diff(d)))
                        }
                    });
                    consolidate(&mut output_buffer);
                    *work += output_buffer.len();
                    output_func(&mut session, key, val1, val2, initial, diff1, &mut output_buffer);
                    // Defensive clear; we'd expect `output_func` to clear the buffer.
                    // TODO: Should we assert it is empty?
                    output_buffer.clear();
                    cursor.step_val(&storage);
                }
                cursor.rewind_vals(&storage);
            }
            *diff1 = R::zero();
        }
    }

    yielded
}
