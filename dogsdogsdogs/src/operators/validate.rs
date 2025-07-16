use std::hash::Hash;

use timely::dataflow::Scope;

use differential_dataflow::{ExchangeData, Collection};
use differential_dataflow::difference::{Semigroup, Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;

/// Proposes extensions to a stream of prefixes.
///
/// This method takes a stream of prefixes and for each determines a
/// key with `key_selector` and then proposes all pair af the prefix
/// and values associated with the key in `arrangement`.
pub fn validate<G, K, V, Tr, F, P>(
    extensions: &Collection<G, (P, V), Tr::Diff>,
    arrangement: Arranged<G, Tr>,
    key_selector: F,
) -> Collection<G, (P, V), Tr::Diff>
where
    G: Scope<Timestamp=Tr::Time>,
    Tr: for<'a> TraceReader<
        KeyOwn = (K, V),
        Diff : Semigroup<Tr::DiffGat<'a>>+Monoid+Multiply<Output = Tr::Diff>+ExchangeData,
    >+Clone+'static,
    K: Ord+Hash+Clone+Default + 'static,
    V: ExchangeData+Hash+Default,
    F: Fn(&P)->K+Clone+'static,
    P: ExchangeData,
{
    crate::operators::lookup_map(
        extensions,
        arrangement,
        move |(pre,val),key| { *key = (key_selector(pre), val.clone()); },
        |(pre,val),r,_,_| ((pre.clone(), val.clone()), r.clone()),
        Default::default(),
        Default::default(),
        Default::default(),
    )
}
