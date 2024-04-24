use timely::dataflow::Scope;

use differential_dataflow::{ExchangeData, Collection, Hashable};
use differential_dataflow::difference::{Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;

/// Proposes extensions to a prefix stream.
///
/// This method takes a collection `prefixes` and an arrangement `arrangement` and for each
/// update in the collection joins with the accumulated arranged records at a time less or
/// equal to that of the update. Note that this is not a join by itself, but can be used to
/// create a join if the `prefixes` collection is also arranged and responds to changes that
/// `arrangement` undergoes. More complicated patterns are also appropriate, as in the case
/// of delta queries.
pub fn propose<G, Tr, F, P, V, VF>(
    prefixes: &Collection<G, P, Tr::Diff>,
    arrangement: Arranged<G, Tr>,
    key_selector: F,
    val_from: VF,
) -> Collection<G, (P, V), Tr::Diff>
where
    G: Scope<Timestamp=Tr::Time>,
    Tr: TraceReader+Clone+'static,
    Tr::KeyOwned: Hashable + Default,
    Tr::Diff: Monoid+Multiply<Output = Tr::Diff>+ExchangeData,
    F: Fn(&P)->Tr::KeyOwned+Clone+'static,
    P: ExchangeData,
    V: Clone + 'static,
    VF: Fn(Tr::Val<'_>) -> V + 'static,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        move |p: &P, k: &mut Tr::KeyOwned | { *k = key_selector(p); },
        move |prefix, diff, value, sum| ((prefix.clone(), val_from(value)), diff.clone().multiply(sum)),
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
pub fn propose_distinct<G, Tr, F, P, V, VF>(
    prefixes: &Collection<G, P, Tr::Diff>,
    arrangement: Arranged<G, Tr>,
    key_selector: F,
    val_from: VF,
) -> Collection<G, (P, V), Tr::Diff>
where
    G: Scope<Timestamp=Tr::Time>,
    Tr: TraceReader+Clone+'static,
    Tr::KeyOwned: Hashable + Default,
    Tr::Diff: Monoid+Multiply<Output = Tr::Diff>+ExchangeData,
    F: Fn(&P)->Tr::KeyOwned+Clone+'static,
    P: ExchangeData,
    V: Clone + 'static,
    VF: Fn(Tr::Val<'_>) -> V + 'static,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        move |p: &P, k: &mut Tr::KeyOwned| { *k = key_selector(p); },
        move |prefix, diff, value, _sum| ((prefix.clone(), val_from(value)), diff.clone()),
        Default::default(),
        Default::default(),
        Default::default(),
    )
}
