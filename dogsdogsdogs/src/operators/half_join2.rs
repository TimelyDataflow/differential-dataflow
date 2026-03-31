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
use timely::dataflow::{Scope, Stream};
use timely::dataflow::channels::pact::{Pipeline, Exchange};
use timely::dataflow::operators::Operator;
use timely::PartialOrder;
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
pub fn half_join<G, K, V, R, Tr, FF, CF, DOut, S>(
    stream: VecCollection<G, (K, V, G::Timestamp), R>,
    arrangement: Arranged<G, Tr>,
    frontier_func: FF,
    comparison: CF,
    mut output_func: S,
) -> VecCollection<G, (DOut, G::Timestamp), <R as Mul<Tr::Diff>>::Output>
where
    G: Scope<Timestamp = Tr::Time>,
    K: Hashable + ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Monoid,
    Tr: TraceReader<KeyOwn = K, Time: std::hash::Hash>+Clone+'static,
    R: Mul<Tr::Diff, Output: Semigroup>,
    FF: Fn(&G::Timestamp, &mut Antichain<G::Timestamp>) + 'static,
    CF: Fn(Tr::TimeGat<'_>, &G::Timestamp) -> bool + 'static,
    DOut: Clone+'static,
    S: FnMut(&K, &V, Tr::Val<'_>)->DOut+'static,
{
    let output_func = move |builder: &mut CapacityContainerBuilder<Vec<_>>, k: &K, v1: &V, v2: Tr::Val<'_>, initial: &G::Timestamp, diff1: &R, output: &mut Vec<(G::Timestamp, Tr::Diff)>| {
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
pub fn half_join_internal_unsafe<G, K, V, R, Tr, FF, CF, Y, S, CB>(
    stream: VecCollection<G, (K, V, G::Timestamp), R>,
    mut arrangement: Arranged<G, Tr>,
    frontier_func: FF,
    comparison: CF,
    yield_function: Y,
    mut output_func: S,
) -> Stream<G, CB::Container>
where
    G: Scope<Timestamp = Tr::Time>,
    K: Hashable + ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Monoid,
    Tr: for<'a> TraceReader<KeyOwn = K, Time: std::hash::Hash>+Clone+'static,
    FF: Fn(&G::Timestamp, &mut Antichain<G::Timestamp>) + 'static,
    CF: Fn(Tr::TimeGat<'_>, &Tr::Time) -> bool + 'static,
    Y: Fn(std::time::Instant, usize) -> bool + 'static,
    S: FnMut(&mut CB, &K, &V, Tr::Val<'_>, &G::Timestamp, &R, &mut Vec<(G::Timestamp, Tr::Diff)>) + 'static,
    CB: ContainerBuilder,
{
    // No need to block physical merging for this operator.
    arrangement.trace.set_physical_compaction(Antichain::new().borrow());
    let mut arrangement_trace = Some(arrangement.trace);
    let arrangement_stream = arrangement.stream;

    let exchange = Exchange::new(move |update: &((K, V, G::Timestamp),G::Timestamp,R)| (update.0).0.hashed().into());

    // Stash for (time, diff) accumulation.
    let mut output_buffer = Vec::new();

    // Stage 1: Stuck blobs sorted by (initial, data). Nibbled from the front
    // as the arrangement frontier advances to determine eligible records.
    let mut stuck: Vec<StuckBlob<(K, V, G::Timestamp), G::Timestamp, R>> = Vec::new();
    // Stage 2: Ready blobs sorted by (data, initial). Consumed from the front
    // one record at a time, yield-safe.
    let mut ready: Vec<ReadyBlob<(K, V, G::Timestamp), G::Timestamp, R>> = Vec::new();
    // Buffer for new arrivals, stored as (initial, data, diff) for direct consolidation.
    let mut arriving: Vec<(G::Timestamp, (K, V, G::Timestamp), R)> = Vec::new();

    let scope = stream.scope();
    stream.inner.binary_frontier(arrangement_stream, exchange, Pipeline, "HalfJoin", move |_,info| {

        // Acquire an activator to reschedule the operator when it has unfinished work.
        let activator = scope.activator_for(info.address);

        move |(input1, frontier1), (input2, frontier2), output| {

            // Drain all input for this activation into a single buffer.
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
                let eligible = |initial: &G::Timestamp| -> bool {
                    !(0..time_con.len()).any(|i| comparison(time_con.index(i), initial))
                };

                // Form new blobs from this activation's arrivals, partitioning
                // immediately-eligible records from those that must wait.
                if !arriving.is_empty() {
                    consolidate_updates(&mut arriving);

                    if !arriving.is_empty() {
                        // Partition: eligible records go straight to a ready blob,
                        // the rest go to a stuck blob. `arriving` is sorted by
                        // (initial, data) after consolidation, and eligibility is
                        // monotone in total-order initial time, so we can split
                        // with a linear scan.
                        let split = arriving.iter().position(|&(ref t, _, _)| !eligible(t)).unwrap_or(arriving.len());

                        // Stuck portion (tail).
                        if split < arriving.len() {
                            let stuck_data: Vec<_> = arriving.drain(split..).collect();
                            let mut lower = MutableAntichain::new();
                            lower.update_iter(stuck_data.iter().map(|(t, _, _)| (t.clone(), 1)));
                            let mut stuck_caps = CapabilitySet::new();
                            for time in lower.frontier().iter() {
                                stuck_caps.insert(caps.delayed(time));
                            }
                            stuck.push(StuckBlob {
                                caps: stuck_caps,
                                lower,
                                data: stuck_data.into(),
                            });
                        }

                        // Eligible portion (head) — rearrange to (data, initial, diff)
                        // and consolidate for key-sorted cursor traversal.
                        if !arriving.is_empty() {
                            let mut active_data: Vec<_> = arriving.drain(..)
                                .map(|(t, d, r)| (d, t, r))
                                .collect();
                            consolidate_updates(&mut active_data);

                            if !active_data.is_empty() {
                                let mut pile_lower = MutableAntichain::new();
                                pile_lower.update_iter(active_data.iter().map(|(_, t, _)| (t.clone(), 1)));
                                let mut ready_caps = CapabilitySet::new();
                                for time in pile_lower.frontier().iter() {
                                    ready_caps.insert(caps.delayed(time));
                                }
                                ready.push(ReadyBlob {
                                    caps: ready_caps,
                                    lower: pile_lower,
                                    data: VecDeque::from(active_data),
                                });
                            }
                        }
                    }
                }

                // Stage 1: nibble stuck blobs to produce new ready piles.
                {
                    // Collect all eligible records across all blobs into one pile.
                    let mut eligible_vec = Vec::new();
                    let mut eligible_caps = CapabilitySet::new();

                    for blob in stuck.iter_mut() {
                        // Pop eligible records from the front. The deque is sorted by
                        // initial time in total order, and `comparison` is monotone,
                        // so we stop at the first ineligible record.
                        let before = eligible_vec.len();
                        while let Some((initial, _, _)) = blob.data.front() {
                            if !eligible(initial) {
                                break;
                            }
                            eligible_vec.push(blob.data.pop_front().unwrap());
                        }

                        if eligible_vec.len() > before {
                            // Grab caps for the eligible records before downgrading the blob.
                            let mut frontier = Antichain::new();
                            for (initial, _, _) in eligible_vec[before..].iter() {
                                frontier.insert(initial.clone());
                            }
                            for time in frontier.iter() {
                                eligible_caps.insert(blob.caps.delayed(time));
                            }

                            // Remove eligible times from the blob's antichain and downgrade.
                            blob.lower.update_iter(eligible_vec[before..].iter().map(|(t, _, _)| (t.clone(), -1)));
                            blob.caps.downgrade(&blob.lower.frontier());
                        }
                    }

                    stuck.retain(|blob| !blob.data.is_empty());

                    if !eligible_vec.is_empty() {
                        // Rearrange to (data, initial, diff) and consolidate.
                        // consolidate_updates sorts by (data, initial) which is
                        // the order we want for the active blob.
                        let mut active_data: Vec<_> = eligible_vec.into_iter()
                            .map(|(t, d, r)| (d, t, r))
                            .collect();
                        consolidate_updates(&mut active_data);

                        if !active_data.is_empty() {
                            let mut pile_lower = MutableAntichain::new();
                            pile_lower.update_iter(active_data.iter().map(|(_, t, _)| (t.clone(), 1)));
                            eligible_caps.downgrade(&pile_lower.frontier());

                            ready.push(ReadyBlob {
                                caps: eligible_caps,
                                lower: pile_lower,
                                data: VecDeque::from(active_data),
                            });
                        }
                    }
                }

                // Stage 2: drain ready piles (key-sorted, yield-safe).
                for pile in ready.iter_mut() {
                    if yielded { break; }

                    // For each pile, we'll set up container builders for each distinct capability in the capset.
                    // The closure gets invoked on a container builder, and if there are containers to extract we
                    // ship them with the associated capability. We flush at the end.

                    let mut builders = (0..pile.caps.len()).map(|_| CB::default()).collect::<Vec<_>>();

                    let (mut cursor, storage) = trace.cursor();
                    let mut key_con = Tr::KeyContainer::with_capacity(1);
                    let mut removals: ChangeBatch<G::Timestamp> = ChangeBatch::new();

                    while let Some(((ref key, ref val1, ref time), ref initial, ref diff1)) = pile.data.front() {
                        yielded = yielded || yield_function(timer, work);
                        if yielded { break; }

                        let builder_idx = pile.caps.iter().position(|c| c.time().less_equal(initial)).unwrap();

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
                                // TODO: Worry about how to avoid reconstructing sessions so often.
                                output_func(&mut builders[builder_idx], key, val1, val2, initial, diff1, &mut output_buffer);
                                output_buffer.clear();
                                cursor.step_val(&storage);
                            }
                            cursor.rewind_vals(&storage);
                        }

                        while let Some(container) = builders[builder_idx].extract() {
                            output.session(&pile.caps[builder_idx]).give_container(container);
                        }

                        let (_, initial, _) = pile.data.pop_front().unwrap();
                        removals.update(initial, -1);
                    }

                    for builder_idx in 0 .. pile.caps.len() {
                        while let Some(container) = builders[builder_idx].finish() {
                            output.session(&pile.caps[builder_idx]).give_container(container);
                        }
                    }

                    // Apply all removals in bulk and downgrade once.
                    if !removals.is_empty() {
                        pile.lower.update_iter(removals.drain());
                        pile.caps.downgrade(&pile.lower.frontier());
                    }
                }
                ready.retain(|pile| !pile.data.is_empty());
            }

            // Re-activate if we have ready piles to process.
            if !ready.is_empty() {
                activator.activate();
            }

            // The logical merging frontier depends on input1, stuck, and ready blobs.
            let mut frontier = Antichain::new();
            for time in frontier1.frontier().iter() {
                frontier_func(time, &mut frontier);
            }
            for blob in stuck.iter() {
                for cap in blob.caps.iter() {
                    frontier_func(cap.time(), &mut frontier);
                }
            }
            for blob in ready.iter() {
                for cap in blob.caps.iter() {
                    frontier_func(cap.time(), &mut frontier);
                }
            }
            arrangement_trace.as_mut().map(|trace| trace.set_logical_compaction(frontier.borrow()));

            if frontier1.is_empty() && stuck.is_empty() && ready.is_empty() {
                arrangement_trace = None;
            }
        }
    })
}

/// Stuck work sorted by `(initial, data)`. Nibbled from the front as the
/// arrangement frontier advances to determine eligible records.
struct StuckBlob<D, T: Timestamp, R> {
    caps: CapabilitySet<T>,
    lower: MutableAntichain<T>,
    data: VecDeque<(T, D, R)>,
}

/// Ready work sorted by `(data, initial)`. Consumed from the front one record
/// at a time during trace lookups. Yield-safe: can stop and resume at any point,
/// because the work can be resumed without re-sorting. Strictly speaking we will
/// do a MutableAntichain rebuild, which could be linear time, but fixing that is
/// future work.
// TODO: Fix the thing in the comments.
struct ReadyBlob<D, T: Timestamp, R> {
    caps: CapabilitySet<T>,
    lower: MutableAntichain<T>,
    data: VecDeque<(D, T, R)>,
}
