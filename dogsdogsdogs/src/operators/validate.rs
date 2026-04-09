use std::hash::Hash;

use timely::progress::Timestamp;

use differential_dataflow::{ExchangeData, VecCollection};
use differential_dataflow::difference::{Semigroup, Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;

/// Proposes extensions to a stream of prefixes.
///
/// This method takes a stream of prefixes and for each determines a
/// key with `key_selector` and then proposes all pair af the prefix
/// and values associated with the key in `arrangement`.
pub fn validate<G, K, V, Tr, F, P>(
    extensions: VecCollection<G, (P, V), Tr::Diff>,
    arrangement: Arranged<Tr>,
    key_selector: F,
) -> VecCollection<G, (P, V), Tr::Diff>
where
    G: Timestamp + std::hash::Hash,
    Tr: for<'a> TraceReader<
        Time = G,
        Diff : Semigroup<Tr::DiffGat<'a>>+Monoid+Multiply<Output = Tr::Diff>+ExchangeData,
    >+Clone+'static,
    Tr::KeyContainer: differential_dataflow::trace::implementations::BatchContainer<Owned=(K,V)>,
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
