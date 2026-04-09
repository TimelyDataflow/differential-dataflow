/// Streaming asymmetric join between updates (K,V1) and an arrangement on (K,V2).
///
/// The asymmetry is that the join only responds to streamed updates, not to changes in the arrangement.
/// Streamed updates join only with matching arranged updates at lesser times *in the total order*, and
/// subject to a predicate supplied by the user (roughly: strictly less, or not).
///
/// This behavior can ensure that any pair of matching updates interact exactly once.
///
/// There are various forms of this operator with tangled closures about how to emit the outputs and
/// wrangle the logical compaction frontier in order to preserve the distinctions around times that are
/// strictly less (conventional compaction logic would collapse unequal times to the frontier, and lose
/// the distiction).
///
/// The methods also carry an auxiliary time next to the value, which is used to advance the joined times.
/// This is .. a byproduct of wanting to allow advancing times a la `join_function`, without breaking the
/// coupling by total order on "initial time".
///
/// The doccomments for individual methods are a bit of a mess. Sorry.

use std::collections::VecDeque;
use std::ops::Mul;

use timely::ContainerBuilder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::Stream;
use timely::scheduling::Scheduler;
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;
use timely::progress::{Antichain, ChangeBatch, Timestamp};
use timely::progress::frontier::MutableAntichain;

use differential_dataflow::{ExchangeData, VecCollection, AsCollection, Hashable};
use differential_dataflow::difference::{Monoid, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{Cursor, TraceReader};
use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::trace::implementations::BatchContainer;

use timely::dataflow::operators::CapabilitySet;

/// A binary equijoin that responds to updates on only its first input.
///
/// This operator responds to inputs of the form
///
/// ```ignore
/// ((key, val1, time1), initial_time, diff1)
/// ```
///
/// where `initial_time` is less or equal to `time1`, and produces as output
///
/// ```ignore
/// ((output_func(key, val1, val2), lub(time1, time2)), initial_time, diff1 * diff2)
/// ```
///
/// for each `((key, val2), time2, diff2)` present in `arrangement`, where
/// `time2` is less than `initial_time` *UNDER THE TOTAL ORDER ON TIMES*.
/// This last constraint is important to ensure that we correctly produce
/// all pairs of output updates across multiple `half_join` operators.
///
/// Notice that the time is hoisted up into data. The expectation is that
/// once out of the "delta flow region", the updates will be `delay`d to the
/// times specified in the payloads.
pub fn half_join<T, K, V, R, Tr, FF, CF, DOut, S>(
    stream: VecCollection<T, (K, V, T), R>,
    arrangement: Arranged<Tr>,
    frontier_func: FF,
    comparison: CF,
    mut output_func: S,
) -> VecCollection<T, (DOut, T), <R as Mul<Tr::Diff>>::Output>
where
    T: Timestamp + Lattice,
    K: Hashable + ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Monoid,
    Tr: TraceReader<Time = T>+Clone+'static,
    T: std::hash::Hash,
    Tr::KeyContainer: BatchContainer<Owned=K>,
    R: Mul<Tr::Diff, Output: Semigroup>,
    FF: Fn(&T, &mut Antichain<T>) + 'static,
    CF: Fn(Tr::TimeGat<'_>, &T) -> bool + 'static,
    DOut: Clone+'static,
    S: FnMut(&K, &V, Tr::Val<'_>)->DOut+'static,
{
    let output_func = move |builder: &mut CapacityContainerBuilder<Vec<_>>, k: &K, v1: &V, v2: Tr::Val<'_>, initial: &T, diff1: &R, output: &mut Vec<(T, Tr::Diff)>| {
        for (time, diff2) in output.drain(..) {
            let diff = diff1.clone() * diff2.clone();
            let dout = (output_func(k, v1, v2), time.clone());
            use timely::container::PushInto;
            builder.push_into((dout, initial.clone(), diff));
        }
    };
    half_join_internal_unsafe::<_, _, _, _, _, _,_,_,_, CapacityContainerBuilder<Vec<_>>>(stream, arrangement, frontier_func, comparison, |_timer, _count| false, output_func)
        .as_collection()
}

/// An unsafe variant of `half_join` where the `output_func` closure takes
/// additional arguments a vector of `time` and `diff` tuples as input and
/// writes its outputs at a container builder. The container builder
/// can, but isn't required to, accept `(data, time, diff)` triplets.
/// This allows for more flexibility, but is more error-prone.
///
/// This operator responds to inputs of the form
///
/// ```ignore
/// ((key, val1, time1), initial_time, diff1)
/// ```
///
/// where `initial_time` is less or equal to `time1`, and produces as output
///
/// ```ignore
/// output_func(session, key, val1, val2, initial_time, diff1, &[lub(time1, time2), diff2])
/// ```
///
/// for each `((key, val2), time2, diff2)` present in `arrangement`, where
/// `time2` is less than `initial_time` *UNDER THE TOTAL ORDER ON TIMES*.
///
/// The `yield_function` allows the caller to indicate when the operator should
/// yield control, as a function of the elapsed time and the number of matched
/// records. Note this is not the number of *output* records, owing mainly to
/// the number of matched records being easiest to record with low overhead.
pub fn half_join_internal_unsafe<T, K, V, R, Tr, FF, CF, Y, S, CB>(
    stream: VecCollection<T, (K, V, T), R>,
    mut arrangement: Arranged<Tr>,
    frontier_func: FF,
    comparison: CF,
    yield_function: Y,
    mut output_func: S,
) -> Stream<T, CB::Container>
where
    T: Timestamp + Lattice,
    K: Hashable + ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Monoid,
    Tr: TraceReader<Time = T>+Clone+'static,
    T: std::hash::Hash,
    Tr::KeyContainer: BatchContainer<Owned=K>,
    FF: Fn(&T, &mut Antichain<T>) + 'static,
    CF: Fn(Tr::TimeGat<'_>, &Tr::Time) -> bool + 'static,
    Y: Fn(std::time::Instant, usize) -> bool + 'static,
    S: FnMut(&mut CB, &K, &V, Tr::Val<'_>, &T, &R, &mut Vec<(T, Tr::Diff)>) + 'static,
    CB: ContainerBuilder,
{
    // No need to block physical merging for this operator.
    arrangement.trace.set_physical_compaction(Antichain::new().borrow());
    let mut arrangement_trace = Some(arrangement.trace);
    let arrangement_stream = arrangement.stream;

    let exchange = Exchange::new(move |update: &((K, V, T),T,R)| (update.0).0.hashed().into());

    // Stash for (time, diff) accumulation.
    let mut output_buffer = Vec::new();

    // Unified blobs: each blob holds data in (T, D, R) order, with a stuck_count
    // tracking how many elements at the back are not yet eligible for processing.
    // The ready prefix is sorted by (D, T, R) for cursor traversal.
    let mut blobs: Vec<Blob<(K, V, T), T, R>> = Vec::new();

    let scope = stream.scope();
    stream.inner.binary_frontier(arrangement_stream, exchange, Pipeline, "HalfJoin", move |_,info| {

        // Acquire an activator to reschedule the operator when it has unfinished work.
        let activator = scope.activator_for(info.address);

        move |(input1, frontier1), (input2, frontier2), output| {

            // Drain all input into a single buffer.
            let mut arriving: Vec<(T, (K, V, T), R)> = Vec::new();
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

                // Determine the total-order minimum of the arrangement frontier,
                // used to partition arrivals into immediately-eligible vs stuck.
                let mut time_con = Tr::TimeContainer::with_capacity(1);
                if let Some(min_time) = frontier.iter().min() {
                    time_con.push_own(min_time);
                }
                let eligible = |initial: &T| -> bool {
                    !(0..time_con.len()).any(|i| comparison(time_con.index(i), initial))
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

                    // Determine how many records are stuck (ineligible).
                    // Data is sorted by (T, D, R) and eligibility is monotone in T,
                    // so stuck records form a suffix.
                    let stuck_count = arriving.iter().rev()
                        .take_while(|(t, _, _)| !eligible(t))
                        .count();

                    let mut data: VecDeque<_> = arriving.into();

                    // Sort the ready prefix by (D, T, R) for cursor traversal.
                    let ready_len = data.len() - stuck_count;
                    if ready_len > 0 {
                        // VecDeque slices: make_contiguous then sort the prefix.
                        let slice = data.make_contiguous();
                        slice[..ready_len].sort_by(|(t1, d1, r1), (t2, d2, r2)| {
                            (d1, t1, r1).cmp(&(d2, t2, r2))
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

                        // Sort the newly-ready prefix by (D, T, R) for cursor traversal.
                        let slice = blob.data.make_contiguous();
                        slice[..newly_ready].sort_by(|(t1, d1, r1), (t2, d2, r2)| {
                            (d1, t1, r1).cmp(&(d2, t2, r2))
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
                    let mut key_con = Tr::KeyContainer::with_capacity(1);
                    let mut removals: ChangeBatch<T> = ChangeBatch::new();

                    // Process ready elements from the front.
                    while blob.data.len() > blob.stuck_count {
                        yielded = yielded || yield_function(timer, work);
                        if yielded { break; }

                        // Peek at the front element. It's in (T, D, R) storage order,
                        // but the ready prefix has been sorted by (D, T, R).
                        let (ref initial, (ref key, ref val1, ref time), ref diff1) = blob.data[0];

                        let builder_idx = blob.caps.iter().position(|c| c.time().less_equal(initial)).unwrap();

                        key_con.clear(); key_con.push_own(&key);
                        cursor.seek_key(&storage, key_con.index(0));
                        if cursor.get_key(&storage) == key_con.get(0) {
                            while let Some(val2) = cursor.get_val(&storage) {
                                cursor.map_times(&storage, |t, d| {
                                    if comparison(t, initial) {
                                        let mut t = Tr::owned_time(t);
                                        t.join_assign(time);
                                        output_buffer.push((t, Tr::owned_diff(d)))
                                    }
                                });
                                consolidate(&mut output_buffer);
                                work += output_buffer.len();
                                output_func(&mut builders[builder_idx], key, val1, val2, initial, diff1, &mut output_buffer);
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
                    } else  {
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
            let mut frontier = Antichain::new();
            for time in frontier1.frontier().iter() {
                frontier_func(time, &mut frontier);
            }
            for blob in blobs.iter() {
                for cap in blob.caps.iter() {
                    frontier_func(cap.time(), &mut frontier);
                }
            }
            arrangement_trace.as_mut().map(|trace| trace.set_logical_compaction(frontier.borrow()));

            if frontier1.is_empty() && blobs.is_empty() {
                arrangement_trace = None;
            }
        }
    })
}

/// A unified blob of updates. Data is stored as `(T, D, R)` tuples in a VecDeque.
/// The last `stuck_count` elements are stuck (not yet eligible for processing),
/// sorted by `(T, D, R)` from consolidation. The ready prefix (everything before
/// the stuck tail) is sorted by `(D, T, R)` for efficient cursor traversal.
/// Ready elements are consumed from the front via `pop_front`.
struct Blob<D, T: Timestamp, R> {
    caps: CapabilitySet<T>,
    lower: MutableAntichain<T>,
    data: VecDeque<(T, D, R)>,
    /// Number of stuck (ineligible) elements at the back of `data`.
    stuck_count: usize,
}
