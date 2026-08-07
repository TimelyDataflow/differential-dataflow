//! Join each admitted update's time into a time the request carries.
//!
//! The core of `propose` and `validate`, and a rough analogue of `half_join` until
//! we unify them all up. The distinctions are superficial at the moment, about how
//! we represent keys.

use timely::container::CapacityContainerBuilder;
use timely::progress::Antichain;

use differential_dataflow::{ExchangeData, VecCollection, AsCollection, Hashable};
use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchDiffGat, BatchVal, Cursor, Navigable, TraceReader};
use differential_dataflow::trace::implementations::BatchContainer;

use crate::operators::lookup::{Cut, lookup};

/// Matches requests against updates in an arrangement.
///
/// The requests come as an update stream, where the data include a time itself.
/// The included time is joined with the matched arrangement times, as is needed
/// for a differential join. The streams update time is left unchanged, as it is
/// used to ensure that update matches occur exactly once.
pub fn lookup_join<'scope, P, K, R, Tr, F, FF, DOut, ROut, S>(
    prefixes: VecCollection<'scope, Tr::Time, (P, Tr::Time), R>,
    arrangement: Arranged<'scope, Tr>,
    cut: Cut,
    frontier_func: FF,
    mut key_selector: F,
    mut output_func: S,
) -> VecCollection<'scope, Tr::Time, (DOut, Tr::Time), ROut>
where
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash>+Clone+'static,
    for<'a> BatchCursor<Tr>: Cursor<
        Time = Tr::Time,
        Diff : Semigroup<BatchDiffGat<'a, Tr>>+Monoid+ExchangeData,
    >,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned=K>,
    K: Hashable + Ord + Default + 'static,
    F: FnMut(&P, &mut K)+Clone+'static,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    P: ExchangeData,
    R: ExchangeData+Monoid,
    DOut: Clone+'static,
    ROut: Monoid + 'static,
    S: FnMut(&P, &R, BatchVal<'_, Tr>, &BatchDiff<Tr>)->(DOut, ROut)+'static,
{
    lookup(
        prefixes,
        arrangement,
        "LookupJoin",
        cut,
        frontier_func,
        move |request: &(P, Tr::Time), key: &mut K| key_selector(&request.0, key),
        |_timer, _count| false,
        move |
            builder: &mut CapacityContainerBuilder<Vec<((DOut, Tr::Time), Tr::Time, ROut)>>,
            request: &(P, Tr::Time),
            initial: &Tr::Time,
            diff: &R,
            value: BatchVal<'_, Tr>,
            admitted: &mut Vec<(Tr::Time, BatchDiff<Tr>)>,
        | {
            let (payload, carried) = request;
            for (time, matched) in admitted.drain(..) {
                let (dout, rout) = output_func(payload, diff, value, &matched);
                if !rout.is_zero() {
                    use timely::container::PushInto;
                    builder.push_into(((dout, carried.join(&time)), initial.clone(), rout));
                }
            }
        },
    )
    .as_collection()
}
