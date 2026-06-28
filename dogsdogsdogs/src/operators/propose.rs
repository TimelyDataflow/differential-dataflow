use differential_dataflow::{ExchangeData, VecCollection, Hashable};
use differential_dataflow::difference::{Semigroup, Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, Navigable, TraceReader};
use differential_dataflow::trace::Cursor;

/// Proposes extensions to a prefix stream.
///
/// This method takes a collection `prefixes` and an arrangement `arrangement` and for each
/// update in the collection joins with the accumulated arranged records at a time less or
/// equal to that of the update. Note that this is not a join by itself, but can be used to
/// create a join if the `prefixes` collection is also arranged and responds to changes that
/// `arrangement` undergoes. More complicated patterns are also appropriate, as in the case
/// of delta queries.
pub fn propose<'scope, Tr, K, F, P, V>(
    prefixes: VecCollection<'scope, Tr::Time, P, <BatchCursor<Tr> as Cursor>::Diff>,
    arrangement: Arranged<'scope, Tr>,
    key_selector: F,
) -> VecCollection<'scope, Tr::Time, (P, V), <BatchCursor<Tr> as Cursor>::Diff>
where
    Tr: TraceReader<Time: std::hash::Hash>+Clone+'static,
    Tr::Batch: Navigable,
    for<'a> BatchCursor<Tr>: Cursor<
        Time = Tr::Time,
        ValOwn = V,
        Diff: Monoid+Multiply<Output = <BatchCursor<Tr> as Cursor>::Diff>+ExchangeData+Semigroup<<BatchCursor<Tr> as Cursor>::DiffGat<'a>>,
    >,
    <BatchCursor<Tr> as Cursor>::KeyContainer: differential_dataflow::trace::implementations::BatchContainer<Owned=K>,
    K: Hashable + Default + Ord + 'static,
    F: Fn(&P)->K+Clone+'static,
    P: ExchangeData,
    V: Clone + 'static,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        move |p: &P, k: &mut K | { *k = key_selector(p); },
        move |prefix, diff, value, sum| ((prefix.clone(), <BatchCursor<Tr> as Cursor>::owned_val(value)), diff.clone().multiply(sum)),
        Default::default(),
        Default::default(),
        Default::default(),
    )
}

/// Proposes distinct extensions to a prefix stream.
///
/// Unlike `propose`, this method does not scale the multiplicity of matched
/// prefixes by the number of matches in `arrangement`. This can be useful to
/// avoid the need to prepare an arrangement of distinct extensions.
pub fn propose_distinct<'scope, Tr, K, F, P, V>(
    prefixes: VecCollection<'scope, Tr::Time, P, <BatchCursor<Tr> as Cursor>::Diff>,
    arrangement: Arranged<'scope, Tr>,
    key_selector: F,
) -> VecCollection<'scope, Tr::Time, (P, V), <BatchCursor<Tr> as Cursor>::Diff>
where
    Tr: TraceReader<Time: std::hash::Hash>+Clone+'static,
    Tr::Batch: Navigable,
    for<'a> BatchCursor<Tr>: Cursor<
        Time = Tr::Time,
        ValOwn = V,
        Diff : Semigroup<<BatchCursor<Tr> as Cursor>::DiffGat<'a>>+Monoid+Multiply<Output = <BatchCursor<Tr> as Cursor>::Diff>+ExchangeData,
    >,
    <BatchCursor<Tr> as Cursor>::KeyContainer: differential_dataflow::trace::implementations::BatchContainer<Owned=K>,
    K: Hashable + Default + Ord + 'static,
    F: Fn(&P)->K+Clone+'static,
    P: ExchangeData,
    V: Clone + 'static,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        move |p: &P, k: &mut K| { *k = key_selector(p); },
        move |prefix, diff, value, _sum| ((prefix.clone(), <BatchCursor<Tr> as Cursor>::owned_val(value)), diff.clone()),
        Default::default(),
        Default::default(),
        Default::default(),
    )
}
