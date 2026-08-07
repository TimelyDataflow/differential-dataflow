//! Accumulate a request's admitted arrangement updates into one diff, and emit once.

use timely::container::CapacityContainerBuilder;
use timely::progress::Antichain;

use differential_dataflow::{ExchangeData, VecCollection, AsCollection, Hashable};
use differential_dataflow::difference::{Semigroup, Monoid};
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchDiffGat, BatchVal, Cursor, Navigable, TraceReader};
use differential_dataflow::trace::implementations::BatchContainer;

use crate::operators::lookup::{Cut, lookup};

/// Looks up each request's key and reports the accumulated matching diff per value.
///
/// For each request and each value under its key, the arrangement updates the `cut` admits are
/// summed into a single diff and handed to `output_func`, whose record is emitted at the
/// request's own time.
///
/// The request is skipped only when the cut admits *nothing* — never on the admitted diffs
/// summing to zero. That distinction is load-bearing: admitted updates can carry different
/// join times, so a sum across them is not the count at any one of them, and skipping on it
/// removes the request from the collection outright. In a prefix-extension join that means
/// no atom proposes the prefix and the extension is lost, where a merely inaccurate count
/// would only have chosen a worse proposer.
///
/// # The `frontier_func` obligation
///
/// As in [`lookup`], this bounds how far the arrangement may compact logically. The rule is
/// sharper than it looks, and the two cuts differ:
///
/// * [`Cut::AtOrBefore`] on a totally ordered timestamp can use the identity — inserting the
///   time unchanged. Compaction advances an admitted `t` to `max(t, F)`, and since a held
///   `initial` is at or after the frontier, `t <= initial` and `F <= initial` give
///   `max(t, F) <= initial`. The match survives. (This is why the previous implementation
///   never needed such a hook.)
/// * [`Cut::Before`] cannot. If `F` sits exactly at a held `initial`, an admitted `t < initial`
///   advances to `initial`, and `initial < initial` is false — the match is silently dropped.
///   The bound must be a strict predecessor, which the lattice does not supply, so the caller
///   writes it (`t.saturating_sub(1)`, `Product::new(outer-1, inner-1)`).
pub fn lookup_map<'scope, D, K, R, Tr, F, FF, DOut, ROut, S>(
    prefixes: VecCollection<'scope, Tr::Time, D, R>,
    arrangement: Arranged<'scope, Tr>,
    cut: Cut,
    frontier_func: FF,
    key_selector: F,
    mut output_func: S,
) -> VecCollection<'scope, Tr::Time, DOut, ROut>
where
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash>+Clone+'static,
    for<'a> BatchCursor<Tr>: Cursor<
        Time = Tr::Time,
        Diff : Semigroup<BatchDiffGat<'a, Tr>>+Monoid+ExchangeData,
    >,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned=K>,
    K: Hashable + Ord + Default + 'static,
    F: FnMut(&D, &mut K)+Clone+'static,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    D: ExchangeData,
    R: ExchangeData+Monoid,
    DOut: Clone+'static,
    ROut: Monoid + 'static,
    S: FnMut(&D, &R, BatchVal<'_, Tr>, &BatchDiff<Tr>)->(DOut, ROut)+'static,
{
    lookup(
        prefixes,
        arrangement,
        "LookupMap",
        cut,
        frontier_func,
        key_selector,
        |_timer, _count| false,
        move |
            builder: &mut CapacityContainerBuilder<Vec<(DOut, Tr::Time, ROut)>>,
            request: &D,
            initial: &Tr::Time,
            diff: &R,
            value: BatchVal<'_, Tr>,
            admitted: &mut Vec<(Tr::Time, BatchDiff<Tr>)>,
        | {
            // Prune on *nothing admitted*, not on the diffs summing to zero. The admitted
            // updates can carry different join times, and a sum across them is not the count
            // at any one of them: `+1` at one time and `-1` at a later one sum to zero while
            // the extension genuinely exists in between. Dropping the request on that sum
            // deletes it from the collection entirely, so no atom proposes it and the answer
            // is lost — where an inaccurate *non-zero* count would only have chosen a worse
            // proposer. An empty admitted list does mean no extension at any time, so that
            // prune is sound and keeps the cheap early-out.
            let admitted_any = !admitted.is_empty();
            let mut count = <BatchDiff<Tr> as Monoid>::zero();
            for (_time, d) in admitted.drain(..) { count.plus_equals(&d); }
            if admitted_any {
                let (dout, rout) = output_func(request, diff, value, &count);
                if !rout.is_zero() {
                    use timely::container::PushInto;
                    builder.push_into((dout, initial.clone(), rout));
                }
            }
        },
    )
    .as_collection()
}
