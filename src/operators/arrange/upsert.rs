//! Support for upsert based collections.

use std::collections::{BinaryHeap, HashMap};

use timely::order::{PartialOrder, TotalOrder};
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::channels::pact::Exchange;
use timely::progress::Timestamp;
use timely::progress::Antichain;
use timely::dataflow::operators::Capability;

use timely_sort::Unsigned;

use ::{ExchangeData, Hashable};
use lattice::Lattice;
use trace::{Trace, TraceReader, Batch, Cursor};

use trace::Builder;

use operators::arrange::arrangement::Arranged;

use super::TraceAgent;

/// Arrange data from a stream of keyed upserts.
///
/// The input should be a stream of timestamped pairs of Key and Option<Val>.
/// The contents of the collection are defined key-by-key, where each optional
/// value in sequence either replaces or removes the existing value, should it
/// exist.
///
/// This method is only implemented for totally ordered times, as we do not yet
/// understand what a "sequence" of upserts would mean for partially ordered
/// timestamps.
pub fn arrange_from_upsert<G, Tr>(stream: &Stream<G, (Tr::Key, Option<Tr::Val>, G::Timestamp)>, name: &str) -> Arranged<G, TraceAgent<Tr>>
where
    G: Scope,
    G::Timestamp: Lattice+Ord+TotalOrder+ExchangeData,
    Tr::Key: ExchangeData+Hashable+std::hash::Hash,
    Tr::Val: ExchangeData,
    Tr: Trace+TraceReader<Time=G::Timestamp,R=isize>+'static,
    Tr::Batch: Batch<Tr::Key, Tr::Val, G::Timestamp, isize>,
    Tr::Cursor: Cursor<Tr::Key, Tr::Val, G::Timestamp, isize>,
{
    let mut reader: Option<TraceAgent<Tr>> = None;

    // fabricate a data-parallel operator using the `unary_notify` pattern.
    let stream = {

        let reader = &mut reader;

        let exchange = Exchange::new(move |update: &(Tr::Key,Option<Tr::Val>,G::Timestamp)| (update.0).hashed().as_u64());

        stream.unary_frontier(exchange, name, move |_capability, info| {

            // Acquire a logger for arrange events.
            let logger = {
                let scope = stream.scope();
                let register = scope.log_register();
                register.get::<::logging::DifferentialEvent>("differential/arrange")
            };

            // Establish compaction effort to apply even without updates.
            let (activator, effort) =
            if let Ok(text) = ::std::env::var("DIFFERENTIAL_EAGER_MERGE") {
                let effort = text.parse::<isize>().expect("DIFFERENTIAL_EAGER_MERGE must be set to an integer");
                (Some(stream.scope().activator_for(&info.address[..])), Some(effort))
            }
            else {
                (None, None)
            };

            // Tracks the lower envelope of times in `priority_queue`.
            let mut capabilities = Antichain::<Capability<G::Timestamp>>::new();
            let mut buffer = Vec::new();
            // Form the trace we will both use internally and publish.
            let empty_trace = Tr::new(info.clone(), logger.clone(), activator);
            let (mut reader_local, mut writer) = TraceAgent::new(empty_trace, info, logger);
            // Capture the reader outside the builder scope.
            *reader = Some(reader_local.clone());

            // Tracks the input frontier, used to populate the lower bound of new batches.
            let mut input_frontier = Antichain::from_elem(<G::Timestamp as Timestamp>::minimum());

            // For stashing input upserts, ordered increasing by time (`BinaryHeap` is a max-heap).
            let mut priority_queue = BinaryHeap::<std::cmp::Reverse<(G::Timestamp, Tr::Key, Option<Tr::Val>)>>::new();

            move |input, output| {

                // Stash capabilities and associated data (ordered by time).
                input.for_each(|cap, data| {
                    capabilities.insert(cap.retain());
                    data.swap(&mut buffer);
                    for (key, val, time) in buffer.drain(..) {
                        priority_queue.push(std::cmp::Reverse((time, key, val)))
                    }
                });

                // Test to see if strict progress has occurred, which happens whenever any element of
                // the old frontier is not greater or equal to the new frontier. It is only in this
                // case that we have any data processing to do.
                let progress = input_frontier.elements().iter().any(|t2| !input.frontier().less_equal(t2));
                if progress {

                    // If there is at least one capability not in advance of the input frontier ...
                    if capabilities.elements().iter().any(|c| !input.frontier().less_equal(c.time())) {

                        let mut upper = Antichain::new();   // re-used allocation for sealing batches.

                        // For each capability not in advance of the input frontier ...
                        for (index, capability) in capabilities.elements().iter().enumerate() {

                            if !input.frontier().less_equal(capability.time()) {

                                // Assemble the upper bound on times we can commit with this capabilities.
                                // We must respect the input frontier, and *subsequent* capabilities, as
                                // we are pretending to retire the capability changes one by one.
                                upper.clear();
                                for time in input.frontier().frontier().iter() {
                                    upper.insert(time.clone());
                                }
                                for other_capability in &capabilities.elements()[(index + 1) .. ] {
                                    upper.insert(other_capability.time().clone());
                                }

                                // Extract upserts available to process as of this `upper`.
                                let mut to_process = HashMap::new();
                                while priority_queue.peek().map(|std::cmp::Reverse((t,_k,_v))| !upper.less_equal(t)).unwrap_or(false) {
                                    let std::cmp::Reverse((time, key, val)) = priority_queue.pop().expect("Priority queue just ensured non-empty");
                                    to_process.entry(key).or_insert(Vec::new()).push((time, val));
                                }

                                let mut builder = <Tr::Batch as Batch<Tr::Key,Tr::Val,G::Timestamp,Tr::R>>::Builder::new();
                                let (mut trace_cursor, trace_storage) = reader_local.cursor();
                                for (key, mut list) in to_process.drain() {

                                    // The prior value associated with the key.
                                    let mut prev_value: Option<Tr::Val> = None;

                                    // Attempt to find the key in the trace.
                                    trace_cursor.seek_key(&trace_storage, &key);
                                    if trace_cursor.get_key(&trace_storage) == Some(&key) {
                                        // Determine the prior value associated with the key.
                                        while let Some(val) = trace_cursor.get_val(&trace_storage) {
                                            let mut count = 0;
                                            trace_cursor.map_times(&trace_storage, |_time, diff| count += *diff);
                                            assert!(count == 0 || count == 1);
                                            if count == 1 {
                                                assert!(prev_value.is_none());
                                                prev_value = Some(val.clone());
                                            }
                                            trace_cursor.step_val(&trace_storage);
                                        }
                                        trace_cursor.step_key(&trace_storage);
                                    }

                                    // Sort the list of upserts to `key` by their time, suppress multiple updates.
                                    list.sort();
                                    list.dedup_by(|(t1,_), (t2,_)| t1 == t2);
                                    // Process distinct times
                                    for (time, next) in list {
                                        if prev_value != next {
                                            if let Some(prev) = prev_value {
                                                builder.push((key.clone(), prev, time.clone(), -1));
                                            }
                                            if let Some(next) = next.as_ref() {
                                                builder.push((key.clone(), next.clone(), time.clone(), 1));
                                            }
                                            prev_value = next;
                                        }
                                    }
                                }
                                let batch = builder.done(input_frontier.elements(), upper.elements(), &[G::Timestamp::minimum()]);
                                input_frontier.clone_from(&upper);

                                // Communicate `batch` to the arrangement and the stream.
                                writer.insert(batch.clone(), Some(capability.time().clone()));
                                output.session(&capabilities.elements()[index]).give(batch);
                            }
                        }

                        // Having extracted and sent batches between each capability and the input frontier,
                        // we should downgrade all capabilities to match the batcher's lower update frontier.
                        // This may involve discarding capabilities, which is fine as any new updates arrive
                        // in messages with new capabilities.

                        let mut new_capabilities = Antichain::new();
                        if let Some(std::cmp::Reverse((time, _, _))) = priority_queue.peek() {
                            if let Some(capability) = capabilities.elements().iter().find(|c| c.time().less_equal(time)) {
                                new_capabilities.insert(capability.delayed(time));
                            }
                            else {
                                panic!("failed to find capability");
                            }
                        }

                        capabilities = new_capabilities;
                    }
                    else {
                        // Announce progress updates, even without data.
                        writer.seal(&input.frontier().frontier()[..]);
                    }

                    // Update our view of the input frontier.
                    input_frontier.clear();
                    input_frontier.extend(input.frontier().frontier().iter().cloned());

                    // Downgrade capabilities for `reader_local`.
                    reader_local.advance_by(input_frontier.elements());
                    reader_local.distinguish_since(input_frontier.elements());
                }

                if let Some(mut fuel) = effort.clone() {
                    writer.exert(&mut fuel);
                }
            }
        })
    };

    Arranged { stream: stream, trace: reader.unwrap() }

}
