use std::hash::Hash;

use differential_dataflow::{ExchangeData, VecCollection};
use differential_dataflow::difference::{Semigroup, Monoid, Multiply};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, Navigable, TraceReader};
use differential_dataflow::trace::Cursor;

/// Proposes extensions to a stream of prefixes.
///
/// This method takes a stream of prefixes and for each determines a
/// key with `key_selector` and then proposes all pair af the prefix
/// and values associated with the key in `arrangement`.
pub fn validate<'scope, K, V, Tr, F, P>(
    extensions: VecCollection<'scope, Tr::Time, (P, V), <BatchCursor<Tr> as Cursor>::Diff>,
    arrangement: Arranged<'scope, Tr>,
    key_selector: F,
) -> VecCollection<'scope, Tr::Time, (P, V), <BatchCursor<Tr> as Cursor>::Diff>
where
    Tr: TraceReader<Time: std::hash::Hash>+Clone+'static,
    Tr::Batch: Navigable,
    for<'a> BatchCursor<Tr>: Cursor<
        Time = Tr::Time,
        Diff : Semigroup<<BatchCursor<Tr> as Cursor>::DiffGat<'a>>+Monoid+Multiply<Output = <BatchCursor<Tr> as Cursor>::Diff>+ExchangeData,
    >,
    <BatchCursor<Tr> as Cursor>::KeyContainer: differential_dataflow::trace::implementations::BatchContainer<Owned=(K,V)>,
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
