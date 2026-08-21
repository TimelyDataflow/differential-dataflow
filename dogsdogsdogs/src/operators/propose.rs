use std::ops::Mul;

use timely::progress::Antichain;

use differential_dataflow::{ExchangeData, VecCollection, Hashable};
use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchTimeGat, Cursor, Navigable, TraceReader};
use differential_dataflow::trace::implementations::BatchContainer;

/// Proposes extensions to a prefix stream.
///
/// This operator matches streamed updates with arranged updates, and pairs the streamed updates
/// with arranged updates whose times are less or equal under the *total order* on timestamps.
/// This inequality is allowed to either be strict or non-strict, as determined by `strict`.
/// The total order allows the caller to ensure that each pair of updates match exactly once.
/// The streamed updates also carry a time as data, and that time is advanced (by lattice join)
/// by the time of the arranged update. The time of the streamed update cannot be advanced, as
/// it needs to stay put to ensure the total order math works out.
///
/// The arrangement is expected to hold a *set*: see the note on set semantics in [`crate`].
pub fn propose<'scope, Tr, K, F, P, V, R, FF>(
    prefixes: VecCollection<'scope, Tr::Time, (P, Tr::Time), R>,
    arrangement: Arranged<'scope, Tr>,
    key_selector: F,
    frontier_func: FF,
    strict: bool,
) -> VecCollection<'scope, Tr::Time, ((P, V), Tr::Time), <R as Mul<BatchDiff<Tr>>>::Output>
where
    Tr: TraceReader<Payload: Navigable, Time: std::hash::Hash>+Clone+'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time, ValOwn = V>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned=K>,
    K: Hashable + ExchangeData,
    R: ExchangeData + Monoid + Mul<BatchDiff<Tr>, Output: Semigroup>,
    F: Fn(&P)->K+'static,
    P: ExchangeData,
    V: Clone + 'static,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    for<'a, 'b> BatchTimeGat<'a, Tr>: PartialOrd<&'b Tr::Time>,
{
    let requests = prefixes.map(move |(prefix, payload)| (key_selector(&prefix), prefix, payload));
    // `strict` now reaches the join as a value rather than as a comparison closure, so there is
    // nothing left to monomorphize by branching here; the test is made per arrangement time.
    crate::operators::half_join(requests, arrangement, frontier_func, strict,
        |_key, prefix, value| (prefix.clone(), <BatchCursor<Tr> as Cursor>::owned_val(value)))
}
