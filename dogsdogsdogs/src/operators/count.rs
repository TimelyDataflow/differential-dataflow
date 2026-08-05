use differential_dataflow::{ExchangeData, VecCollection, Hashable};
use differential_dataflow::difference::{Semigroup, Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchDiffGat, Cursor, Navigable, TraceReader};

use crate::operators::lookup::Cut;

/// Reports a number of extensions to a stream of prefixes.
///
/// # On "worst-case optimal"
///
/// This is the sizing step of a Generic-Join-shaped plan, and that plan is worst-case optimal
/// for *static, set-valued* relations. Three things weaken the connection here, and none is
/// resolved:
///
/// * `validate` is not a semijoin. In the static algorithm each non-proposing atom is a
///   membership test, strictly non-increasing, and that incremental pruning is how the bound
///   is attained. Here it multiplies over `[(time, diff)]` lists, so an intermediate can grow
///   before consolidation shrinks it again.
/// * Output consolidates. The bound counts combinations of updates; what a consumer sees is
///   combinations *after cancellation*, which can be far fewer. The bound still limits the
///   enumeration, but it is not tight against the achievable output, and an algorithm that
///   skipped combinations destined to cancel would beat this one.
/// * The number reported below is not the bound's quantity. The bound is over updates in the
///   admitted interval; this reports distinct values accumulated at a point, which can be
///   zero while the interval holds real output.
///
/// What is true: enumeration is bounded, and the proposer is chosen by a cardinality proxy.
/// Read the name as identifying the plan shape, not as asserting the bound.
///
/// This method takes as input a stream of `(prefix, count, index)` triples.
/// For each triple, it extracts a key using `key_selector`, and finds the
/// associated count in `arrangement`. If the found count is less than `count`,
/// the `count` and `index` fields are overwritten with their new values.
pub fn count<'scope, Tr, K, R, F, FF, P>(
    prefixes: VecCollection<'scope, Tr::Time, ((P, usize, usize), Tr::Time), R>,
    arrangement: Arranged<'scope, Tr>,
    cut: Cut,
    frontier_func: FF,
    key_selector: F,
    index: usize,
) -> VecCollection<'scope, Tr::Time, ((P, usize, usize), Tr::Time), R>
where
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash + differential_dataflow::ExchangeData>+Clone+'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time, Diff=isize>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: differential_dataflow::trace::implementations::BatchContainer<Owned=K>,
    for<'a> BatchDiff<Tr> : Semigroup<BatchDiffGat<'a, Tr>>,
    K: Hashable + Ord + Default + 'static,
    R: Monoid+Multiply<Output = R>+ExchangeData,
    F: Fn(&P)->K+Clone+'static,
    FF: Fn(&Tr::Time, &mut timely::progress::Antichain<Tr::Time>) + 'static,
    P: ExchangeData,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        cut,
        frontier_func,
        move |p: &((P,usize,usize), Tr::Time), k: &mut K| { *k = key_selector(&(p.0).0); },
        // `count` is a routing decision, not a contributing record: it names the atom with
        // the fewest extensions and leaves the carried time untouched. The relation it reads
        // contributes to the output time later, through its own `propose` or `validate`.
        move |((p,c,i), carried), r, _, s| {
            let s = *s as usize;
            if *c < s { (((p.clone(), *c, *i), carried.clone()), r.clone()) }
            else      { (((p.clone(), s, index), carried.clone()), r.clone()) }
        },
    )
}
