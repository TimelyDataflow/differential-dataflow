use timely::dataflow::Scope;

use differential_dataflow::{ExchangeData, Collection, Hashable};
use differential_dataflow::difference::{Semigroup, Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;

/// Reports a number of extensions to a stream of prefixes.
///
/// This method takes as input a stream of `(prefix, count, index)` triples.
/// For each triple, it extracts a key using `key_selector`, and finds the
/// associated count in `arrangement`. If the found count is less than `count`,
/// the `count` and `index` fields are overwritten with their new values.
pub fn count<G, Tr, K, R, F, P>(
    prefixes: &Collection<G, (P, usize, usize), R>,
    arrangement: Arranged<G, Tr>,
    key_selector: F,
    index: usize,
) -> Collection<G, (P, usize, usize), R>
where
    G: Scope<Timestamp=Tr::Time>,
    Tr: TraceReader<KeyOwn = K, Diff=isize>+Clone+'static,
    for<'a> Tr::Diff : Semigroup<Tr::DiffGat<'a>>,
    K: Hashable + Ord + Default + 'static,
    R: Monoid+Multiply<Output = R>+ExchangeData,
    F: Fn(&P)->K+Clone+'static,
    P: ExchangeData,
{
    crate::operators::lookup_map(
        prefixes,
        arrangement,
        move |p: &(P,usize,usize), k: &mut K| { *k = key_selector(&p.0); },
        move |(p,c,i), r, _, s| {
            let s = *s as usize;
            if *c < s { ((p.clone(), *c, *i), r.clone()) }
            else      { ((p.clone(), s, index), r.clone()) }
        },
        Default::default(),
        Default::default(),
        Default::default(),
    )
}
