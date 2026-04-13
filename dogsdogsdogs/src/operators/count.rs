use differential_dataflow::{ExchangeData, VecCollection, Hashable};
use differential_dataflow::difference::{Semigroup, Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;

/// Reports a number of extensions to a stream of prefixes.
///
/// This method takes as input a stream of `(prefix, count, index)` triples.
/// For each triple, it extracts a key using `key_selector`, and finds the
/// associated count in `arrangement`. If the found count is less than `count`,
/// the `count` and `index` fields are overwritten with their new values.
pub fn count<'scope, Tr, K, R, F, P>(
    prefixes: VecCollection<'scope, Tr::Time, (P, usize, usize), R>,
    arrangement: Arranged<'scope, Tr>,
    key_selector: F,
    index: usize,
) -> VecCollection<'scope, Tr::Time, (P, usize, usize), R>
where
    Tr: TraceReader<Time: std::hash::Hash, Diff=isize>+Clone+'static,
    Tr::KeyContainer: differential_dataflow::trace::implementations::BatchContainer<Owned=K>,
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
