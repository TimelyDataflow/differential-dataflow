//! Join each admitted update's time into a time the request carries.
//!
//! The second of the two behaviors over [`crate::operators::lookup`], and the one that
//! contributes records to a join. Where [`lookup_map`](crate::operators::lookup_map) sums the
//! admitted updates into a single diff and emits once, this one visits them *individually*:
//! each admitted `(time, diff)` produces its own output, at its own lifted time.
//!
//! # Why individually
//!
//! An output tuple of a `k`-way join exists at the join of all `k` contributing times. A
//! request arrives carrying the join of the times it has accumulated so far; matching against
//! an update at `time` produces a tuple whose time is `carried ⊔ time`. Different admitted
//! updates therefore land at *different* output times, and collapsing them into one accumulated
//! diff throws that structure away.
//!
//! Collapsing is nonetheless correct when every admitted time is dominated by the request's
//! own — then `carried ⊔ time` is the same value for all of them and the many outputs coincide
//! with the one. That is precisely the totally ordered case, which is why
//! [`lookup_map`](crate::operators::lookup_map) is sound for `count` and was sound for
//! `propose` and `validate` as long as they were only ever used under a total order. It stops
//! being true the moment a cut admits an update incomparable to the request, which is what
//! happens inside a nested scope.
//!
//! # The two times
//!
//! A request is `(payload, carried)` and arrives at its own timestamp, `initial`. These are not
//! interchangeable:
//!
//! * `initial` is the **order** time — the fixed time of the delta that seeded this rule, which
//!   every atom's [`Cut`] compares against, and which stays the update's dataflow timestamp.
//! * `carried` is the **join** time — the accumulating lub, which grows as the request matches
//!   its way along the chain, and which rides in the payload.
//!
//! The order time has to be the timestamp because it is the one progress tracking must
//! communicate: a downstream operator sets its arrangement's compaction bound from a lower
//! bound on the order times still to arrive, and since `initial ⪯ carried`, a frontier over
//! carried times would not provide one. On leaving the delta region the carried time becomes
//! the update's timestamp and the order time is dropped.

use timely::container::CapacityContainerBuilder;
use timely::progress::Antichain;

use differential_dataflow::{ExchangeData, VecCollection, AsCollection, Hashable};
use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchDiffGat, BatchVal, Cursor, Navigable, TraceReader};
use differential_dataflow::trace::implementations::BatchContainer;

use crate::operators::lookup::{Cut, lookup};

/// Matches requests against an arrangement, lifting each match onto its own joined time.
///
/// Requests are `(payload, carried)`; `key_selector` reads the lookup key from the payload.
/// For each request and each admitted update `(time, diff)` under its key, `output_func` shapes
/// an output record from the payload, the request's diff, the matched value and *that update's*
/// diff. The record is emitted with carried time `carried ⊔ time`, at the request's own
/// timestamp.
///
/// `output_func` receives one arrangement update at a time rather than an accumulation, so a
/// diff-producing implementation should multiply by it, not ignore it: the output tuple's diff
/// is the product over all contributing atoms, and an atom contributes whether it proposed the
/// extension or merely validated it.
///
/// See [`lookup`] for the `frontier_func` obligation, which is unchanged here.
pub fn lookup_join<'scope, P, K, R, Tr, F, FF, DOut, ROut, S>(
    prefixes: VecCollection<'scope, Tr::Time, (P, Tr::Time), R>,
    arrangement: Arranged<'scope, Tr>,
    cut: Cut,
    frontier_func: FF,
    mut key_selector: F,
    mut output_func: S,
) -> VecCollection<'scope, Tr::Time, (DOut, Tr::Time), ROut>
where
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash + ExchangeData>+Clone+'static,
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
