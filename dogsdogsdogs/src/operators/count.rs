use timely::container::CapacityContainerBuilder;
use timely::container::PushInto;
use timely::progress::Antichain;

use differential_dataflow::{AsCollection, ExchangeData, VecCollection, Hashable};
use differential_dataflow::difference::Monoid;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchTimeGat, BatchVal, Cursor, Navigable, TraceReader};
use differential_dataflow::trace::implementations::BatchContainer;

/// Updates a stream of prefix routing judgements based on approximate counts.
///
/// Each prefix observes the changes in distinct values over time, and treats this as a lower
/// bound on the count that will be experienced. When the lower bound improves on the routing
/// judgement's current count, it is overwritten and the `index` argument is substituted in.
///
/// A prefix is dropped only when its key is absent from `arrangement` entirely, which is only
/// expected to happen when there have never been counts (they are meant to be non-negative).
pub fn count<'scope, Tr, K, F, P, R, FF, CF>(
    prefixes: VecCollection<'scope, Tr::Time, ((P, usize, usize), Tr::Time), R>,
    arrangement: Arranged<'scope, Tr>,
    key_selector: F,
    index: usize,
    frontier_func: FF,
    comparison: CF,
) -> VecCollection<'scope, Tr::Time, ((P, usize, usize), Tr::Time), R>
where
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash>+Clone+'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time, Diff = isize>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned=K>,
    K: Hashable + ExchangeData,
    R: ExchangeData + Monoid,
    F: Fn(&P)->K+'static,
    P: ExchangeData,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    CF: Fn(BatchTimeGat<'_, Tr>, &Tr::Time) -> bool + 'static,
{
    // The payload time is carried in the record as well as in the half-join's own payload
    // slot, because the output closure is handed the joined times rather than the payload.
    let requests = prefixes.map(move |(triple, payload)| {
        (key_selector(&triple.0), (triple, payload.clone()), payload)
    });

    type Output<P, T, R> = CapacityContainerBuilder<Vec<(((P, usize, usize), T), T, R)>>;

    let output_func = move |
        builder: &mut Output<P, Tr::Time, R>,
        _key: &K,
        val1: &((P, usize, usize), Tr::Time),
        _val2: BatchVal<'_, Tr>,
        initial: &Tr::Time,
        diff1: &R,
        output: &mut Vec<(Tr::Time, isize)>,
    | {
        // Each diff is a number of distinct values in/out, so sum the absolute values.
        // A zero count does not mean no changes; it could mean swaps of values, so we
        // do not drop prefixes with zero sums. We may inappropriately favor them, but
        // it is a performance issue not a correctness issue.
        let found: usize = output.iter().map(|(_,diff)| diff.abs()).sum::<isize>() as usize;
        let ((prefix, count, best), payload) = val1;
        let triple = if *count < found { (prefix.clone(), *count, *best) }
                     else              { (prefix.clone(), found, index) };
        builder.push_into(((triple, payload.clone()), initial.clone(), diff1.clone()));
    };

    crate::operators::half_join::half_join_internal_unsafe::<_, _, _, _, _, _, _, _, Output<P, Tr::Time, R>>(
        requests,
        arrangement,
        frontier_func,
        comparison,
        |_timer, _count| false,
        output_func,
    )
    .as_collection()
}
