use differential_dataflow::{ExchangeData, VecCollection, Hashable};
use differential_dataflow::difference::{Semigroup, Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchDiffGat, Cursor, Navigable, TraceReader};

use crate::operators::lookup::Cut;

/// Reports a number of extensions to a stream of prefixes.
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
