use std::ops::Mul;

use timely::dataflow::Scope;

use differential_dataflow::{ExchangeData, Collection, Hashable};
use differential_dataflow::difference::Monoid;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{Cursor, TraceReader, BatchReader};

/// Proposes extensions to a prefix stream.
pub fn propose<G, Tr, F, P>(
    prefixes: &Collection<G, P, Tr::R>,
    arrangement: Arranged<G, Tr>,
    key_selector: F,
) -> Collection<G, (P, Tr::Val), Tr::R>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr: TraceReader<Time=G::Timestamp>+Clone+'static,
    Tr::Key: Ord+Hashable+Default,
    Tr::Val: Clone,
    Tr::Batch: BatchReader<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::Cursor: Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::R: Monoid+Mul<Output = Tr::R>+ExchangeData,
    F: Fn(&P)->Tr::Key+Clone+'static,
    P: ExchangeData,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        move |p: &P, k: &mut Tr::Key| { *k = key_selector(p); },
        |prefix, diff, value, sum| ((prefix.clone(), value.clone()), diff.clone() * sum.clone())
    )
}

/// Proposes distinct extensions to a prefix stream.
///
/// Unlike `propose`, this method does not scale the multiplicity of matched
/// prefixes by the number of matches in `arrangement`. This can be useful to
/// avoid the need to prepare an arrangement of distinct extensions.
pub fn propose_distinct<G, Tr, F, P>(
    prefixes: &Collection<G, P, Tr::R>,
    arrangement: Arranged<G, Tr>,
    key_selector: F,
) -> Collection<G, (P, Tr::Val), Tr::R>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr: TraceReader<Time=G::Timestamp>+Clone+'static,
    Tr::Key: Ord+Hashable+Default,
    Tr::Val: Clone,
    Tr::Batch: BatchReader<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::Cursor: Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::R: Monoid+Mul<Output = Tr::R>+ExchangeData,
    F: Fn(&P)->Tr::Key+Clone+'static,
    P: ExchangeData,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        move |p: &P, k: &mut Tr::Key| { *k = key_selector(p); },
        |prefix, diff, value, _sum| ((prefix.clone(), value.clone()), diff.clone())
    )
}