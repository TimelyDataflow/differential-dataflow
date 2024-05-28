use timely::dataflow::Scope;

use differential_dataflow::{ExchangeData, Collection, Hashable};
use differential_dataflow::difference::{Semigroup, Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::cursor::IntoOwned;

/// Proposes extensions to a prefix stream.
///
/// This method takes a collection `prefixes` and an arrangement `arrangement` and for each
/// update in the collection joins with the accumulated arranged records at a time less or
/// equal to that of the update. Note that this is not a join by itself, but can be used to
/// create a join if the `prefixes` collection is also arranged and responds to changes that
/// `arrangement` undergoes. More complicated patterns are also appropriate, as in the case
/// of delta queries.
pub fn propose<G, Tr, K, F, P, V>(
    prefixes: &Collection<G, P, Tr::Diff>,
    arrangement: Arranged<G, Tr>,
    key_selector: F,
) -> Collection<G, (P, V), Tr::Diff>
where
    G: Scope<Timestamp=Tr::Time>,
    Tr: TraceReader+Clone+'static,
    for<'a> Tr::Key<'a> : IntoOwned<'a, Owned = K>,
    K: Hashable + Default + Ord + 'static,
    Tr::Diff: Monoid+Multiply<Output = Tr::Diff>+ExchangeData,
    for<'a> Tr::Diff : Semigroup<Tr::DiffGat<'a>>,
    F: Fn(&P)->K+Clone+'static,
    P: ExchangeData,
    V: Clone + 'static,
    for<'a> Tr::Val<'a> : IntoOwned<'a, Owned = V>,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        move |p: &P, k: &mut K | { *k = key_selector(p); },
        move |prefix, diff, value, sum| ((prefix.clone(), value.into_owned()), diff.clone().multiply(sum)),
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
pub fn propose_distinct<G, Tr, K, F, P, V>(
    prefixes: &Collection<G, P, Tr::Diff>,
    arrangement: Arranged<G, Tr>,
    key_selector: F,
) -> Collection<G, (P, V), Tr::Diff>
where
    G: Scope<Timestamp=Tr::Time>,
    Tr: TraceReader+Clone+'static,
    for<'a> Tr::Key<'a> : IntoOwned<'a, Owned = K>,
    for<'a> Tr::Diff : Semigroup<Tr::DiffGat<'a>>,
    K: Hashable + Default + Ord + 'static,
    Tr::Diff: Monoid+Multiply<Output = Tr::Diff>+ExchangeData,
    F: Fn(&P)->K+Clone+'static,
    P: ExchangeData,
    V: Clone + 'static,
    for<'a> Tr::Val<'a> : IntoOwned<'a, Owned = V>,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        move |p: &P, k: &mut K| { *k = key_selector(p); },
        move |prefix, diff, value, _sum| ((prefix.clone(), value.into_owned()), diff.clone()),
        Default::default(),
        Default::default(),
        Default::default(),
    )
}
