//! Streamed requests looked up against an arrangement, based on timestamp total-order.
//!
//! The operator takes two inputs, a stream of *requests* and an arrangement of *state*.
//! Each request is matched with the state updates at times that are less or equal to the
//! time of the request, optionally strictly, under the *total order* on timestamps.
//!
//! The total ordering is used to ensure that each pair of updates between the stream and
//! the arrangement can be matched exactly once; the corresponding state updates can stream
//! against the rolled up form of the stream, using the alternate strictness.

use std::collections::VecDeque;

use timely::ContainerBuilder;
use timely::PartialOrder;
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::CapabilitySet;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use timely::progress::frontier::MutableAntichain;

use differential_dataflow::{ExchangeData, VecCollection, Hashable};
use differential_dataflow::difference::Monoid;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchVal, Cursor, Navigable, TraceReader};
use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::trace::implementations::BatchContainer;

/// The strictness of the total order inequality comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cut {
    /// `t < initial`: strictly before, ignoring updates concurrent with the request.
    Before,
    /// `t <= initial`: at or before, seeing updates concurrent with the request.
    AtOrBefore,
}

impl Cut {
    /// Whether an arrangement update at `time` falls within the cut a request takes at `initial`.
    #[inline]
    pub fn admits<T: Ord>(&self, time: &T, initial: &T) -> bool {
        match self {
            Cut::Before => time < initial,
            Cut::AtOrBefore => time <= initial,
        }
    }

    /// A helper method that provides a strictness for two positions: a seed and an atom.
    ///
    /// Atoms before the seed use non-strict inequalities, atoms after the seed use strict
    /// inequalities, and it is an error to use the same identifier as the seed and atom.
    pub fn for_positions(seed: usize, atom: usize) -> Cut {
        assert_ne!(seed, atom, "an atom does not read itself; rule {seed} is seeded by it");
        if atom < seed { Cut::AtOrBefore } else { Cut::Before }
    }
}

/// The identity compaction bound: insert the time unchanged.
///
/// Sound for [`Cut::AtOrBefore`] on a totally ordered timestamp, and *only* there. Compaction
/// advances an admitted `t` to `max(t, F)`; a held `initial` is at or after the frontier, so
/// `t <= initial` and `F <= initial` give `max(t, F) <= initial` and the match survives.
///
/// [`Cut::Before`] must not use this. If `F` sits exactly at a held `initial`, an admitted
/// `t < initial` advances to `initial`, and `initial < initial` is false — the match is
/// silently dropped. That cut needs a strict predecessor, which the lattice cannot supply, so
/// the caller writes one.
pub fn identity_frontier<T: Clone + PartialOrder>(time: &T, antichain: &mut Antichain<T>) {
    antichain.insert(time.clone());
}

/// Looks up streamed requests against an arrangement, at the cut each request takes.
///
/// Requests are exchanged by the hash of the key `key_selector` extracts, stashed until the
/// arrangement is complete for their cut, and then probed. `output_func` is invoked once per
/// (request, matching value) with the consolidated admitted `(time, diff)` updates, and pushes
/// whatever it likes into the container builder.
///
/// # Caller obligations
///
/// * **`frontier_func` must hold back logical compaction far enough to preserve the cut.**
///   Specifically, when the cut is *strict*, the logical compaction needs to preserve the
///   difference between a time that may still arrive and times strictly before it. Usually,
///   this means "subtracting one" from each time that might still arrive, and taking these
///   times as a compaction frontier.
/// * **Exactly-once is not checked here.** This operator implements one atom at one cut. That
///   the cuts across a rule's atoms compose into an exactly-once decomposition is the caller's
///   construction, and is not observable from inside a single operator.
///
/// The `yield_function` bounds how much work one activation does, as a function of elapsed time
/// and matched records. It is not merely a responsiveness control: it is what bounds the
/// intermediate state a batch of requests can materialize at once.
pub fn lookup<'scope, D, K, R, Tr, F, FF, Y, S, CB>(
    requests: VecCollection<'scope, Tr::Time, D, R>,
    mut arrangement: Arranged<'scope, Tr>,
    name: &str,
    cut: Cut,
    frontier_func: FF,
    key_selector: F,
    yield_function: Y,
    mut output_func: S,
) -> Stream<'scope, Tr::Time, CB::Container>
where
    D: ExchangeData,
    K: Hashable + Ord + Default + 'static,
    R: ExchangeData + Monoid,
    Tr: TraceReader<Batch: Navigable, Time: std::hash::Hash>+Clone+'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned=K>,
    F: FnMut(&D, &mut K)+Clone+'static,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    Y: Fn(std::time::Instant, usize) -> bool + 'static,
    S: FnMut(&mut CB, &D, &Tr::Time, &R, BatchVal<'_, Tr>, &mut Vec<(Tr::Time, BatchDiff<Tr>)>) + 'static,
    CB: ContainerBuilder,
{
    // No need to block physical merging for this operator.
    arrangement.trace.set_physical_compaction(Antichain::new().borrow());
    let mut arrangement_trace = Some(arrangement.trace);
    let arrangement_stream = arrangement.stream;

    // The exchange and the probe each need a key extractor; they run at different moments and
    // cannot share one borrow, so each takes its own clone and its own scratch key.
    let mut exchange_logic = key_selector.clone();
    let mut exchange_key: K = Default::default();
    let exchange = Exchange::new(move |update: &(D, Tr::Time, R)| {
        exchange_logic(&update.0, &mut exchange_key);
        exchange_key.hashed().into()
    });
    let mut probe_logic = key_selector.clone();
    let mut sort_logic = key_selector;

    // Stash for (time, diff) accumulation, reused across every (request, value) pair.
    let mut output_buffer = Vec::new();

    // Unified blobs: each blob holds data in (T, D, R) order, with a stuck_count
    // tracking how many elements at the back are not yet eligible for processing.
    // The ready prefix is sorted by (D, T, R) for cursor traversal.
    let mut blobs: Vec<Blob<D, Tr::Time, R>> = Vec::new();

    let scope = requests.scope();
    requests.inner.binary_frontier(arrangement_stream, exchange, Pipeline, name, move |_,info| {

        // Acquire an activator to reschedule the operator when it has unfinished work.
        let activator = scope.activator_for(info.address);

        move |(input1, frontier1), (input2, frontier2), output| {

            // Drain all input into a single buffer.
            let mut arriving: Vec<(Tr::Time, D, R)> = Vec::new();
            let mut caps = CapabilitySet::new();
            input1.for_each(|capability, data| {
                caps.insert(capability.retain(0));
                arriving.extend(data.drain(..).map(|(d, t, r)| (t, d, r)));
            });

            // Drain input batches; although we do not observe them, we want access to the input
            // to observe the frontier and to drive scheduling.
            input2.for_each(|_, _| { });

            // Local variables to track if and when we should exit early.
            let mut yielded = false;
            let timer = std::time::Instant::now();
            let mut work = 0;

            if let Some(ref mut trace) = arrangement_trace {

                let frontier = frontier2.frontier();

                // A request is eligible once no update the arrangement may still receive would
                // be admitted by its cut. Every future update `t` has some frontier element
                // `f` with `f ⪯ t`; since `Ord` extends the partial order and is transitive,
                // `f ⪯ t` and `t` admitted imply `f` admitted. So testing the frontier alone
                // is sound, with no obligation on the caller.
                let eligible = |initial: &Tr::Time| -> bool {
                    !frontier.iter().any(|f| cut.admits(f, initial))
                };

                // Form a new blob from arrivals.
                // consolidate_updates sorts by (T, D, R) — the stuck order.
                consolidate_updates(&mut arriving);

                if !arriving.is_empty() {
                    let mut lower = MutableAntichain::new();
                    lower.update_iter(arriving.iter().map(|(t, _, _)| (t.clone(), 1)));
                    let mut blob_caps = CapabilitySet::new();
                    for time in lower.frontier().iter() {
                        blob_caps.insert(caps.delayed(time));
                    }

                    // Determine how many records are stuck (ineligible). Data is sorted by
                    // (T, D, R) using `Ord`, and eligibility is *anti*-monotone in `initial`
                    // under the same `Ord` that the cut uses, so the stuck records form a
                    // suffix. This is why the cut is fixed to `Ord` rather than an arbitrary
                    // caller-supplied comparison: a comparison disagreeing with `Ord` would
                    // break the suffix property here and silently release requests early.
                    let stuck_count = arriving.iter().rev()
                        .take_while(|(t, _, _)| !eligible(t))
                        .count();

                    let mut data: VecDeque<_> = arriving.into();

                    // Sort the ready prefix by the *lookup key*, so the cursor walk below is
                    // monotone. `seek_key` only advances, so a key that goes backwards is
                    // simply not found and its request silently sees an empty match. Sorting by
                    // the payload is not enough: the key is whatever `key_selector` extracts,
                    // and payload order need not agree with key order.
                    let ready_len = data.len() - stuck_count;
                    if ready_len > 0 {
                        // VecDeque slices: make_contiguous then sort the prefix.
                        let slice = data.make_contiguous();
                        slice[..ready_len].sort_by_cached_key(|(_, payload, _)| {
                            let mut key: K = Default::default();
                            sort_logic(payload, &mut key);
                            key
                        });
                    }

                    blobs.push(Blob {
                        caps: blob_caps,
                        lower,
                        data,
                        stuck_count,
                    });
                }

                // Nibble: only when all ready elements have been drained (stuck_count == len),
                // check if stuck records have become eligible and promote them.
                for blob in blobs.iter_mut().filter(|b| b.stuck_count == b.data.len()) {

                    // Count how many stuck records (from the front, which has the
                    // lowest initial times) are now eligible.
                    let newly_ready = blob.data.iter().take_while(|(t, _, _)| eligible(t)).count();

                    if newly_ready > 0 {
                        blob.stuck_count -= newly_ready;

                        // Sort the newly-ready prefix by the lookup key, as above.
                        let slice = blob.data.make_contiguous();
                        slice[..newly_ready].sort_by_cached_key(|(_, payload, _)| {
                            let mut key: K = Default::default();
                            sort_logic(payload, &mut key);
                            key
                        });

                        // Downgrade capabilities.
                        let mut new_lower = MutableAntichain::new();
                        new_lower.update_iter(blob.data.iter().map(|(t, _, _)| (t.clone(), 1)));
                        blob.lower = new_lower;
                        blob.caps.downgrade(&blob.lower.frontier());
                    }
                }

                // Process ready elements from blobs.
                for blob in blobs.iter_mut().filter(|b| b.data.len() > b.stuck_count) {
                    if yielded { break; }

                    let mut builders = (0..blob.caps.len()).map(|_| CB::default()).collect::<Vec<_>>();

                    let (mut cursor, storage) = trace.cursor();
                    let mut key_con = <BatchCursor<Tr> as Cursor>::KeyContainer::with_capacity(1);
                    let mut key: K = Default::default();
                    let mut removals: ChangeBatch<Tr::Time> = ChangeBatch::new();

                    // Process ready elements from the front.
                    while blob.data.len() > blob.stuck_count {
                        yielded = yielded || yield_function(timer, work);
                        if yielded { break; }

                        // Peek at the front element. It's in (T, D, R) storage order,
                        // but the ready prefix has been sorted by (D, T, R).
                        let (ref initial, ref payload, ref diff) = blob.data[0];

                        let builder_idx = blob.caps.iter().position(|c| c.time().less_equal(initial)).unwrap();

                        probe_logic(payload, &mut key);
                        key_con.clear(); key_con.push_own(&key);
                        cursor.seek_key(&storage, key_con.index(0));
                        if cursor.get_key(&storage) == key_con.get(0) {
                            while let Some(value) = cursor.get_val(&storage) {
                                cursor.map_times(&storage, |t, d| {
                                    let t = <BatchCursor<Tr> as Cursor>::owned_time(t);
                                    if cut.admits(&t, initial) {
                                        output_buffer.push((t, <BatchCursor<Tr> as Cursor>::owned_diff(d)))
                                    }
                                });
                                consolidate(&mut output_buffer);
                                work += output_buffer.len();
                                output_func(&mut builders[builder_idx], payload, initial, diff, value, &mut output_buffer);
                                // Defensive clear; we expect `output_func` to drain the buffer.
                                output_buffer.clear();
                                cursor.step_val(&storage);
                            }
                            cursor.rewind_vals(&storage);
                        }

                        while let Some(container) = builders[builder_idx].extract() {
                            output.session(&blob.caps[builder_idx]).give_container(container);
                        }

                        let (initial, _, _) = blob.data.pop_front().unwrap();
                        removals.update(initial, -1);
                    }

                    for builder_idx in 0 .. blob.caps.len() {
                        while let Some(container) = builders[builder_idx].finish() {
                            output.session(&blob.caps[builder_idx]).give_container(container);
                        }
                    }

                    // Apply all removals in bulk and downgrade once.
                    if blob.data.is_empty() {
                        // Eagerly release the blob's resources.
                        blob.lower = MutableAntichain::new();
                        blob.caps = CapabilitySet::new();
                        blob.data = VecDeque::default();
                    } else {
                        blob.lower.update_iter(removals.drain());
                        blob.caps.downgrade(&blob.lower.frontier());
                    }
                }

                // Remove fully-consumed blobs.
                blobs.retain(|blob| !blob.data.is_empty());
            }

            // Re-activate if we have blobs with ready elements to process.
            if blobs.iter().any(|b| b.data.len() > b.stuck_count) {
                activator.activate();
            }

            // The logical merging frontier depends on input1 and all blobs.
            let held: Vec<Tr::Time> = frontier1.frontier().iter().cloned()
                .chain(blobs.iter().flat_map(|blob| blob.caps.iter().map(|cap| cap.time().clone())))
                .collect();
            let mut frontier = Antichain::new();
            for time in held.iter() {
                frontier_func(time, &mut frontier);
            }

            // `frontier_func` owes the following, which is NOT asserted here — see below.
            //
            //   For every held `initial` and every update at `t` the cut admits at `initial`,
            //   the compacted time `t.advance_by(frontier)` must still be admitted at `initial`.
            //
            // Compaction preserves only *partial-order* comparisons against times at or after
            // the frontier, so a cut that is not the partial order does not ride along for free.
            // `Cut::Before` is where it bites: it is strict, and compaction can destroy
            // strictness by advancing an admitted `t < initial` up to exactly `initial`, after
            // which `initial < initial` is false and the match is silently dropped. That is what
            // callers are buying with `t.saturating_sub(1)` and `Product::new(outer-1, inner-1)`.
            //
            // Two attempts to `debug_assert!` this were both wrong, and the reason is worth
            // recording. The obvious pointwise test — every frontier element admitted at every
            // held time — over-fires, because `advance_by` on a multi-element antichain takes
            // the *meet* of the per-element joins, which can leave a time exactly where it was.
            // With held times `{(0,2), (2,0)}` the frontier holds both `(0,1)` and `(1,0)`, and
            // `(1,0)` is not `Ord`-below `(0,2)` even though nothing is actually advanced past
            // any cut. A sound check has to reason about `advance_by` over the antichain rather
            // than element by element, and the weakened form that is easy to state (frontier
            // `⪯`-below every held time) does not catch the error worth catching: passing
            // `identity_frontier` with `Cut::Before` satisfies it and is still wrong.
            arrangement_trace.as_mut().map(|trace| trace.set_logical_compaction(frontier.borrow()));

            if frontier1.is_empty() && blobs.is_empty() {
                arrangement_trace = None;
            }
        }
    })
}

/// A unified blob of requests. Data is stored as `(T, D, R)` tuples in a VecDeque.
/// The last `stuck_count` elements are stuck (not yet eligible for processing),
/// sorted by `(T, D, R)` from consolidation — eligibility is anti-monotone in `T`
/// under the same `Ord` the cut uses, which is what makes the stuck set a suffix.
/// The ready prefix is re-sorted by the *lookup key*, which the cursor walk requires:
/// `seek_key` only advances.
/// Ready elements are consumed from the front via `pop_front`.
struct Blob<D, T: Timestamp, R> {
    caps: CapabilitySet<T>,
    lower: MutableAntichain<T>,
    data: VecDeque<(T, D, R)>,
    /// Number of stuck (ineligible) elements at the back of `data`.
    stuck_count: usize,
}
