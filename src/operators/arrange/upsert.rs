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

            // Capabilities for the lower envelope of updates in `batcher`.
            let mut capabilities = Antichain::<Capability<G::Timestamp>>::new();

            let mut buffer = Vec::new();

            let (activator, effort) =
            if let Ok(text) = ::std::env::var("DIFFERENTIAL_EAGER_MERGE") {
                let effort = text.parse::<isize>().expect("DIFFERENTIAL_EAGER_MERGE must be set to an integer");
                (Some(stream.scope().activator_for(&info.address[..])), Some(effort))
            }
            else {
                (None, None)
            };

            let empty_trace = Tr::new(info.clone(), logger.clone(), activator);
            let (mut reader_local, mut writer) = TraceAgent::new(empty_trace, info, logger);

            *reader = Some(reader_local.clone());

            // Initialize to the minimal input frontier.
            // Tracks the upper bound of minted batches, use to populate the lower bound of new batches.
            let mut input_frontier = Antichain::from_elem(<G::Timestamp as Timestamp>::minimum());

            // For stashing input upserts, ordered primarily by time (then by key, but we'll
            // need to re-sort them anyhow).
            let mut priority_queue = BinaryHeap::<std::cmp::Reverse<(G::Timestamp, Tr::Key, Option<Tr::Val>)>>::new();

            move |input, output| {

                // As we receive data, we need to (i) stash the data and (ii) keep *enough* capabilities.
                // We don't have to keep all capabilities, but we need to be able to form output messages
                // when we realize that time intervals are complete.

                input.for_each(|cap, data| {
                    capabilities.insert(cap.retain());
                    data.swap(&mut buffer);
                    for (key, val, time) in buffer.drain(..) {
                        priority_queue.push(std::cmp::Reverse((time, key, val)))
                    }
                });

                // The frontier may have advanced by multiple elements, which is an issue because
                // timely dataflow currently only allows one capability per message. This means we
                // must pretend to process the frontier advances one element at a time, batching
                // and sending smaller bites than we might have otherwise done.

                // Assert that the frontier never regresses.
                assert!(input.frontier().frontier().iter().all(|t1| input_frontier.elements().iter().any(|t2: &G::Timestamp| t2.less_equal(t1))));

                // Test to see if strict progress has occurred (any of the old frontier less equal
                // to the new frontier).
                let progress = input_frontier.elements().iter().any(|t2| !input.frontier().less_equal(t2));

                if progress {

                    // There are two cases to handle with some care:
                    //
                    // 1. If any held capabilities are not in advance of the new input frontier,
                    //    we must carve out updates now in advance of the new input frontier and
                    //    transmit them as batches, which requires appropriate *single* capabilites;
                    //    Until timely dataflow supports multiple capabilities on messages, at least.
                    //
                    // 2. If there are no held capabilities in advance of the new input frontier,
                    //    then there are no updates not in advance of the new input frontier and
                    //    we can simply create an empty input batch with the new upper frontier
                    //    and feed this to the trace agent (but not along the timely output).

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

                                // START NEW CODE
                                // Extract upserts available to process as of this `upper`.
                                let mut to_process = HashMap::new();
                                while priority_queue.peek().map(|std::cmp::Reverse((t,_k,_v))| !upper.less_equal(t)).unwrap_or(false) {
                                    let std::cmp::Reverse((time, key, val)) = priority_queue.pop().expect("Priority queue just ensured non-empty");
                                    to_process.entry(key).or_insert(Vec::new()).push((time, val));
                                }

                                let mut builder = <Tr::Batch as Batch<Tr::Key,Tr::Val,G::Timestamp,Tr::R>>::Builder::new();
                                let (mut trace_cursor, trace_storage) = reader_local.cursor();
                                for (key, mut list) in to_process.drain() {

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

                                    list.sort();
                                    // Two updates at the exact same time should produce only one actual
                                    // change, ideally to a deterministically chosen value.
                                    let mut cursor = 1;
                                    while cursor < list.len() {
                                        if list[cursor-1].0 == list[cursor].0 {
                                            list.remove(cursor);
                                        }
                                        else {
                                            cursor += 1;
                                        }
                                    }
                                    // Process distinct times
                                    for (time, next) in list {
                                        if let Some(prev) = prev_value {
                                            builder.push((key.clone(), prev, time.clone(), -1));
                                        }
                                        if let Some(next) = next.as_ref() {
                                            builder.push((key.clone(), next.clone(), time.clone(), 1));
                                        }
                                        prev_value = next;
                                    }
                                }
                                let batch = builder.done(input_frontier.elements(), upper.elements(), &[G::Timestamp::minimum()]);
                                input_frontier.clone_from(&upper);

                                // END NEW CODE
                                writer.insert(batch.clone(), Some(capability.time().clone()));

                                // send the batch to downstream consumers, empty or not.
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

                    input_frontier.clear();
                    input_frontier.extend(input.frontier().frontier().iter().cloned());
                }

                if let Some(mut fuel) = effort.clone() {
                    writer.exert(&mut fuel);
                }
            }
        })
    };

    Arranged { stream: stream, trace: reader.unwrap() }

}
