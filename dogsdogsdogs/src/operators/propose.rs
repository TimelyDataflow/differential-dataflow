use differential_dataflow::{ExchangeData, VecCollection, Hashable};
use differential_dataflow::difference::{Semigroup, Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchDiffGat, Cursor, Navigable, TraceReader};

use crate::operators::lookup::Cut;
use crate::operators::lookup_join::lookup_join;

/// Proposes extensions to a prefix stream.
///
/// This method takes a collection `prefixes` and an arrangement `arrangement` and for each
/// update in the collection joins with the accumulated arranged records at a time less or
/// equal to that of the update. Note that this is not a join by itself, but can be used to
/// create a join if the `prefixes` collection is also arranged and responds to changes that
/// `arrangement` undergoes. More complicated patterns are also appropriate, as in the case
/// of delta queries.
pub fn propose<'scope, Tr, K, F, FF, P, V>(
    prefixes: VecCollection<'scope, Tr::Time, (P, Tr::Time), BatchDiff<Tr>>,
    arrangement: Arranged<'scope, Tr>,
    cut: Cut,
    frontier_func: FF,
    key_selector: F,
) -> VecCollection<'scope, Tr::Time, ((P, V), Tr::Time), BatchDiff<Tr>>
where
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash>+Clone+'static,
    for<'a> BatchCursor<Tr>: Cursor<
        Time = Tr::Time,
        ValOwn = V,
        Diff: Monoid+Multiply<Output = BatchDiff<Tr>>+ExchangeData+Semigroup<BatchDiffGat<'a, Tr>>,
    >,
    <BatchCursor<Tr> as Cursor>::KeyContainer: differential_dataflow::trace::implementations::BatchContainer<Owned=K>,
    K: Hashable + Default + Ord + 'static,
    F: Fn(&P)->K+Clone+'static,
    FF: Fn(&Tr::Time, &mut timely::progress::Antichain<Tr::Time>) + 'static,
    P: ExchangeData,
    V: Clone + 'static,
{
    lookup_join(
        prefixes,
        arrangement,
        cut,
        frontier_func,
        move |p: &P, k: &mut K | { *k = key_selector(p); },
        move |prefix, diff, value, matched| ((prefix.clone(), <BatchCursor<Tr> as Cursor>::owned_val(value)), diff.clone().multiply(matched)),
    )
}

/// Proposes distinct extensions to a prefix stream.
///
/// Unlike `propose`, this method does not scale the multiplicity of matched
/// prefixes by the number of matches in `arrangement`. This can be useful to
/// avoid the need to prepare an arrangement of distinct extensions.
pub fn propose_distinct<'scope, Tr, K, F, FF, P, V>(
    prefixes: VecCollection<'scope, Tr::Time, (P, Tr::Time), BatchDiff<Tr>>,
    arrangement: Arranged<'scope, Tr>,
    cut: Cut,
    frontier_func: FF,
    key_selector: F,
) -> VecCollection<'scope, Tr::Time, ((P, V), Tr::Time), BatchDiff<Tr>>
where
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash>+Clone+'static,
    for<'a> BatchCursor<Tr>: Cursor<
        Time = Tr::Time,
        ValOwn = V,
        Diff : Semigroup<BatchDiffGat<'a, Tr>>+Monoid+Multiply<Output = BatchDiff<Tr>>+ExchangeData,
    >,
    <BatchCursor<Tr> as Cursor>::KeyContainer: differential_dataflow::trace::implementations::BatchContainer<Owned=K>,
    K: Hashable + Default + Ord + 'static,
    F: Fn(&P)->K+Clone+'static,
    FF: Fn(&Tr::Time, &mut timely::progress::Antichain<Tr::Time>) + 'static,
    P: ExchangeData,
    V: Clone + 'static,
{
    lookup_join(
        prefixes,
        arrangement,
        cut,
        frontier_func,
        move |p: &P, k: &mut K| { *k = key_selector(p); },
        move |prefix, diff, value, _matched| ((prefix.clone(), <BatchCursor<Tr> as Cursor>::owned_val(value)), diff.clone()),
    )
}
