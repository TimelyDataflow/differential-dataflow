use std::hash::Hash;

use differential_dataflow::{ExchangeData, VecCollection};
use differential_dataflow::difference::{Semigroup, Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchDiffGat, Cursor, Navigable, TraceReader};

use crate::operators::lookup::Cut;
use crate::operators::lookup_join::lookup_join;

/// Proposes extensions to a stream of prefixes.
///
/// This method takes a stream of prefixes and for each determines a
/// key with `key_selector` and then proposes all pair af the prefix
/// and values associated with the key in `arrangement`.
pub fn validate<'scope, K, V, Tr, F, FF, P>(
    extensions: VecCollection<'scope, Tr::Time, ((P, V), Tr::Time), BatchDiff<Tr>>,
    arrangement: Arranged<'scope, Tr>,
    cut: Cut,
    frontier_func: FF,
    key_selector: F,
) -> VecCollection<'scope, Tr::Time, ((P, V), Tr::Time), BatchDiff<Tr>>
where
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash>+Clone+'static,
    for<'a> BatchCursor<Tr>: Cursor<
        Time = Tr::Time,
        Diff : Semigroup<BatchDiffGat<'a, Tr>>+Monoid+Multiply<Output = BatchDiff<Tr>>+ExchangeData,
    >,
    <BatchCursor<Tr> as Cursor>::KeyContainer: differential_dataflow::trace::implementations::BatchContainer<Owned=(K,V)>,
    K: Ord+Hash+Clone+Default + 'static,
    V: ExchangeData+Hash+Default,
    F: Fn(&P)->K+Clone+'static,
    FF: Fn(&Tr::Time, &mut timely::progress::Antichain<Tr::Time>) + 'static,
    P: ExchangeData,
{
    lookup_join(
        extensions,
        arrangement,
        cut,
        frontier_func,
        move |(pre,val),key| { *key = (key_selector(pre), val.clone()); },
        // An atom contributes its diff to the product whether it proposed the extension
        // or merely validated it, so a semijoin multiplies rather than passing `r` through.
        // For set-valued relations the matched diff is one and this is the old behavior.
        |(pre,val),r,_,matched| ((pre.clone(), val.clone()), r.clone().multiply(matched)),
    )
}
