use std::ops::Mul;

use timely::progress::Antichain;

use differential_dataflow::{ExchangeData, VecCollection};
use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchTimeGat, Cursor, Navigable, TraceReader};
use differential_dataflow::trace::implementations::BatchContainer;

/// Restricts proposed extensions to those the arrangement would also have proposed.
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
pub fn validate<'scope, Tr, K, V, F, P, R, FF>(
    extensions: VecCollection<'scope, Tr::Time, ((P, V), Tr::Time), R>,
    arrangement: Arranged<'scope, Tr>,
    key_selector: F,
    frontier_func: FF,
    strict: bool,
) -> VecCollection<'scope, Tr::Time, ((P, V), Tr::Time), <R as Mul<BatchDiff<Tr>>>::Output>
where
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash>+Clone+'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned=(K,V)>,
    K: ExchangeData + std::hash::Hash,
    V: ExchangeData + std::hash::Hash,
    R: ExchangeData + Monoid + Mul<BatchDiff<Tr>, Output: Semigroup>,
    F: Fn(&P)->K+'static,
    P: ExchangeData,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    for<'a, 'b> BatchTimeGat<'a, Tr>: PartialOrd<&'b Tr::Time>,
{
    let requests = extensions.map(move |((prefix, extension), payload)| {
        ((key_selector(&prefix), extension.clone()), (prefix, extension), payload)
    });
    // Branch once here, so that each comparison monomorphizes rather than testing `strict` at
    // every timestamp. The cost is instantiating `half_join` twice.
    if strict {
        crate::operators::half_join(requests, arrangement, frontier_func, |t1, t2| t1 < t2,
            |_key, extended, _value| extended.clone())
    }
    else {
        crate::operators::half_join(requests, arrangement, frontier_func, |t1, t2| t1 <= t2,
            |_key, extended, _value| extended.clone())
    }
}
