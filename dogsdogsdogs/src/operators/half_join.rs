//! Streaming asymmetric join between updates `(K,V1)` and an arrangement on `(K,V2)`.
//!
//! The asymmetry is that the join only responds to streamed updates, not to changes in the
//! arrangement. Streamed updates join only with matching arranged updates admitted by the
//! update's [`Cut`] — see [`crate::operators::lookup()`], which is the operator underneath and
//! which carries the reasoning about why the cut is a total order.
//!
//! These are thin derivations: everything about time lives in `lookup`, and what remains here
//! is the choice of output shape. The methods carry an auxiliary time next to the value, used
//! to advance the joined times; that is a byproduct of wanting to advance times a la
//! `join_function` without breaking the coupling by total order on the initial time.

use std::ops::Mul;

use timely::ContainerBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::Stream;
use timely::progress::Antichain;

use differential_dataflow::{ExchangeData, VecCollection, AsCollection, Hashable};
use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchVal, Cursor, Navigable, TraceReader};
use differential_dataflow::consolidation::consolidate;
use differential_dataflow::trace::implementations::BatchContainer;

use crate::operators::lookup::{Cut, lookup};

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
/// for each `((key, val2), time2, diff2)` present in `arrangement` whose `time2` the `cut`
/// admits at `initial_time`.
///
/// Notice that the time is hoisted up into data. The expectation is that once out of the
/// "delta flow region", the updates will be `delay`d to the times specified in the payloads.
pub fn half_join<'scope, K, V, R, Tr, FF, DOut, S>(
    stream: VecCollection<'scope, Tr::Time, (K, V, Tr::Time), R>,
    arrangement: Arranged<'scope, Tr>,
    frontier_func: FF,
    cut: Cut,
    mut output_func: S,
) -> VecCollection<'scope, Tr::Time, (DOut, Tr::Time), <R as Mul<BatchDiff<Tr>>>::Output>
where
    K: Hashable + ExchangeData + Default,
    V: ExchangeData,
    R: ExchangeData + Monoid,
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash>+Clone+'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned=K>,
    R: Mul<BatchDiff<Tr>, Output: Semigroup>,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    DOut: Clone+'static,
    S: FnMut(&K, &V, BatchVal<'_, Tr>)->DOut+'static,
{
    let output_func = move |
        builder: &mut CapacityContainerBuilder<Vec<_>>,
        key: &K,
        val1: &V,
        val2: BatchVal<'_, Tr>,
        initial: &Tr::Time,
        diff1: &R,
        admitted: &mut Vec<(Tr::Time, BatchDiff<Tr>)>,
    | {
        for (time, diff2) in admitted.drain(..) {
            let diff = diff1.clone() * diff2.clone();
            let dout = (output_func(key, val1, val2), time);
            use timely::container::PushInto;
            builder.push_into((dout, initial.clone(), diff));
        }
    };
    half_join_internal_unsafe::<_, _, _, _, _, _, _, CapacityContainerBuilder<Vec<_>>>(
        stream, arrangement, frontier_func, cut, |_timer, _count| false, output_func,
    )
    .as_collection()
}

/// An unsafe variant of [`half_join`] whose `output_func` writes to a container builder.
///
/// The container builder can, but isn't required to, accept `(data, time, diff)` triplets.
/// This allows for more flexibility, but is more error-prone.
///
/// This operator responds to inputs of the form
///
/// ```ignore
/// ((key, val1, time1), initial_time, diff1)
/// ```
///
/// where `initial_time` is less or equal to `time1`, and invokes
///
/// ```ignore
/// output_func(builder, key, val1, val2, initial_time, diff1, &[(lub(time1, time2), diff2)])
/// ```
///
/// for each `((key, val2), time2, diff2)` present in `arrangement` whose `time2` the `cut`
/// admits at `initial_time`.
///
/// The admitted updates are presented already advanced by `time1` and re-consolidated. The
/// underlying operator consolidates in *arrangement*-time space, which is all it can do
/// without an opinion about the output; advancing by `time1` can collapse distinct times
/// together, so this wrapper consolidates a second time before handing the buffer over.
///
/// The `yield_function` allows the caller to indicate when the operator should yield control,
/// as a function of the elapsed time and the number of matched records. Note this is not the
/// number of *output* records, owing mainly to the number of matched records being easiest to
/// record with low overhead.
pub fn half_join_internal_unsafe<'scope, K, V, R, Tr, FF, Y, S, CB>(
    stream: VecCollection<'scope, Tr::Time, (K, V, Tr::Time), R>,
    arrangement: Arranged<'scope, Tr>,
    frontier_func: FF,
    cut: Cut,
    yield_function: Y,
    mut output_func: S,
) -> Stream<'scope, Tr::Time, CB::Container>
where
    K: Hashable + ExchangeData + Default,
    V: ExchangeData,
    R: ExchangeData + Monoid,
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash>+Clone+'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned=K>,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    Y: Fn(std::time::Instant, usize) -> bool + 'static,
    S: FnMut(&mut CB, &K, &V, BatchVal<'_, Tr>, &Tr::Time, &R, &mut Vec<(Tr::Time, BatchDiff<Tr>)>) + 'static,
    CB: ContainerBuilder,
{
    lookup(
        stream,
        arrangement,
        cut,
        frontier_func,
        |request: &(K, V, Tr::Time), key: &mut K| { key.clone_from(&request.0); },
        yield_function,
        move |builder: &mut CB, request: &(K, V, Tr::Time), initial: &Tr::Time, diff1: &R, val2, admitted: &mut Vec<(Tr::Time, BatchDiff<Tr>)>| {
            let (key, val1, time1) = request;
            // Advance the admitted arrangement times by the request's carried time, then
            // re-consolidate: `lookup` could only consolidate before this map.
            for (time, _diff) in admitted.iter_mut() { time.join_assign(time1); }
            consolidate(admitted);
            output_func(builder, key, val1, val2, initial, diff1, admitted);
        },
    )
}
