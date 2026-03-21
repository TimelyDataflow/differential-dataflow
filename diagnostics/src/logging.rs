//! Logging dataflows for timely and differential dataflow events.
//!
//! Captures timely and differential logging events, replays them as timely
//! streams, and maintains them as differential dataflow collections with
//! indexed arrangements and a client-driven sink.
//!
//! # Architecture
//!
//! The [`register`] function:
//! 1. Creates `EventLink` pairs for timely and DD event capture.
//! 2. Builds a dataflow that replays events into DD collections and arranges them.
//! 3. Creates a cross-join with a client input collection: when a client appears
//!    at +1, the join naturally replays the full current state as updates.
//! 4. Captures the joined output via `mpsc` for the WebSocket thread.
//! 5. Registers logging callbacks that push events into the links.
//!
//! The WebSocket thread communicates client connect/disconnect via a
//! [`ClientInput`] that pushes through an `mpsc` channel, and reads
//! diagnostic updates from another `mpsc` channel.
//!
//! Timestamps use `Duration` (matching timely's logging infrastructure).
//! Timestamp quantization (rounding to interval boundaries) is done as a DD
//! `map_in_place` operation on the collections, keeping the logging layer simple.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use differential_dataflow::collection::concatenate;
use differential_dataflow::logging::{DifferentialEvent, DifferentialEventBuilder};
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::{KeySpine, ValSpine};
use differential_dataflow::{AsCollection, VecCollection};

use timely::communication::Allocate;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::{Event, EventLink, Replay, Capture};
use timely::dataflow::operators::Exchange;
use timely::dataflow::operators::vec::Map;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::{Scope, Stream};
use timely::logging::{
    BatchLogger, OperatesEvent, StartStop, TimelyEvent, TimelyEventBuilder,
};
use timely::worker::Worker;

use serde::{Serialize, Deserialize};

// ============================================================================
// ClientInput — manages client connect/disconnect events across threads
// ============================================================================

/// Container type for client connection events: `(client_id, time, diff)`.
type ClientContainer = Vec<(usize, Duration, i64)>;

/// Sends client connect/disconnect events to the diagnostics dataflow.
///
/// The WebSocket server thread uses this to announce clients. On drop,
/// sends a capability retraction so the dataflow's client input frontier
/// can advance (important for multi-worker setups where non-server workers
/// must release their frontier).
pub struct ClientInput {
    sender: mpsc::Sender<Event<Duration, ClientContainer>>,
    time: Duration,
}

impl ClientInput {
    /// Announce a client connection.
    pub fn connect(&mut self, client_id: usize, elapsed: Duration) {
        let _ = self
            .sender
            .send(Event::Messages(self.time, vec![(client_id, elapsed, 1)]));
        self.advance(elapsed);
    }

    /// Announce a client disconnection.
    pub fn disconnect(&mut self, client_id: usize, elapsed: Duration) {
        let _ = self
            .sender
            .send(Event::Messages(self.time, vec![(client_id, elapsed, -1)]));
        self.advance(elapsed);
    }

    /// Advance the capability to `elapsed`. Call periodically so the
    /// dataflow's frontier can progress.
    pub fn advance(&mut self, elapsed: Duration) {
        if self.time < elapsed {
            let _ = self
                .sender
                .send(Event::Progress(vec![(elapsed, 1), (self.time, -1)]));
            self.time = elapsed;
        }
    }
}

impl Drop for ClientInput {
    fn drop(&mut self) {
        let _ = self
            .sender
            .send(Event::Progress(vec![(self.time, -1)]));
    }
}

// ============================================================================
// MpscEventIterator — bridges mpsc::Receiver to timely's EventIterator
// ============================================================================

/// Wraps an `mpsc::Receiver` as an `EventIterator` for use with `Replay`.
struct MpscEventIterator<T, C> {
    receiver: mpsc::Receiver<Event<T, C>>,
}

impl<T: Clone, C: Clone> timely::dataflow::operators::capture::event::EventIterator<T, C>
    for MpscEventIterator<T, C>
{
    fn next(&mut self) -> Option<Cow<'_, Event<T, C>>> {
        self.receiver.try_recv().ok().map(Cow::Owned)
    }
}

// ============================================================================
// Diagnostic update types
// ============================================================================

/// Identifies the kind of a key-only statistic (diff carries the value).
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum StatKind {
    Elapsed,
    Messages,
    ArrangementBatches,
    ArrangementRecords,
    Sharing,
    BatcherRecords,
    BatcherSize,
    BatcherCapacity,
    BatcherAllocations,
}

/// A tagged diagnostic update sent to the WebSocket thread.
///
/// Each variant carries enough information for the browser to apply the update.
/// The diff on the containing `(D, time, diff)` triple carries the magnitude.
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum DiagnosticUpdate {
    /// Operator appeared (+1) or disappeared (-1). Diff is ±1.
    Operator { id: usize, name: String, addr: Vec<usize> },
    /// Channel appeared (+1). Diff is ±1.
    Channel { id: usize, scope_addr: Vec<usize>, source: (usize, usize), target: (usize, usize) },
    /// A key-only statistic keyed by operator/channel id. Diff carries the value.
    Stat { kind: StatKind, id: usize },
}

/// The container type for captured diagnostic output.
/// Each element is `((client_id, update), time, diff)`.
pub type DiagnosticContainer = Vec<((usize, DiagnosticUpdate), Duration, i64)>;

/// The event type received by the WebSocket thread.
pub type DiagnosticEvent = Event<Duration, DiagnosticContainer>;

// ============================================================================
// Trace handle types
// ============================================================================

/// A key-value trace: key K, value V, time Duration, diff i64.
type ValTrace<K, V> = TraceAgent<ValSpine<K, V, Duration, i64>>;
/// A key-only trace: key K, time Duration, diff i64.
type KeyTrace<K> = TraceAgent<KeySpine<K, Duration, i64>>;

/// Trace handles for timely logging arrangements.
pub struct TimelyTraces {
    /// Live operators arranged by id → (name, addr).
    pub operators: ValTrace<usize, (String, Vec<usize>)>,
    /// Live channels arranged by id → (scope_addr, source, target).
    pub channels: ValTrace<usize, (Vec<usize>, (usize, usize), (usize, usize))>,
    /// Schedule elapsed per operator (diff = nanoseconds).
    pub elapsed: KeyTrace<usize>,
    /// Message records sent per channel (diff = record count).
    pub messages: KeyTrace<usize>,
}

/// Trace handles for differential dataflow logging arrangements.
pub struct DifferentialTraces {
    pub arrangement_batches: KeyTrace<usize>,
    pub arrangement_records: KeyTrace<usize>,
    pub sharing: KeyTrace<usize>,
    pub batcher_records: KeyTrace<usize>,
    pub batcher_size: KeyTrace<usize>,
    pub batcher_capacity: KeyTrace<usize>,
    pub batcher_allocations: KeyTrace<usize>,
}

// ============================================================================
// SinkHandle — returned to the caller for WebSocket integration
// ============================================================================

/// Handle for the WebSocket thread to interact with the diagnostics dataflow.
///
/// **Important:** The WS thread must call `client_input.advance(elapsed)`
/// periodically (e.g., every 100ms–1s) to advance the client input's frontier.
/// Without this, the cross-join's output frontier won't advance and the capture
/// operator will never emit `Event::Progress` messages.
pub struct SinkHandle {
    /// Input for the WS thread to send client connect/disconnect events.
    pub client_input: ClientInput,
    /// Receiver for diagnostic updates produced by the cross-join.
    ///
    /// Each `Event::Messages(time, vec)` contains `((client_id, update), time, diff)`
    /// triples. The WS thread routes updates to clients by `client_id`.
    pub output_receiver: mpsc::Receiver<DiagnosticEvent>,
    /// The reference instant for computing elapsed durations.
    /// Use `start.elapsed()` when calling `client_input.advance()`.
    pub start: Instant,
}

/// Everything returned by [`register`].
pub struct LoggingState {
    pub traces: LoggingTraces,
    pub sink: SinkHandle,
}

/// All trace handles.
pub struct LoggingTraces {
    pub timely: TimelyTraces,
    pub differential: DifferentialTraces,
}

// ============================================================================
// Timestamp quantization
// ============================================================================

/// Default quantization interval.
const INTERVAL: Duration = Duration::from_secs(1);

/// Round a Duration up to the next multiple of `interval`.
fn quantize(time: Duration, interval: Duration) -> Duration {
    let nanos = time.as_nanos();
    let interval_nanos = interval.as_nanos();
    let rounded = (nanos / interval_nanos + 1) * interval_nanos;
    Duration::from_nanos(rounded as u64)
}

/// Quantize timestamps in a collection's inner stream.
fn quantize_collection<S, D>(
    collection: VecCollection<S, D, i64>,
    interval: Duration,
) -> VecCollection<S, D, i64>
where
    S: Scope<Timestamp = Duration>,
    D: differential_dataflow::Data,
{
    collection
        .inner
        .map_in_place(move |(_, time, _)| *time = quantize(*time, interval))
        .as_collection()
}

// ============================================================================
// Registration
// ============================================================================

/// Register diagnostics logging for a worker.
///
/// Builds a dataflow that:
/// 1. Captures timely and differential logging events into DD collections.
/// 2. Arranges them into indexed traces for persistence.
/// 3. Cross-joins all collections with a client input, so new clients
///    automatically receive the full current state as updates.
/// 4. Captures the output for the WebSocket thread via `mpsc`.
///
/// If `log_logging` is true, the diagnostics dataflow itself will appear in
/// the timely logs.
///
/// Returns a [`LoggingState`] with trace handles and a [`SinkHandle`] for
/// the WebSocket thread.
pub fn register<A: Allocate>(worker: &mut Worker<A>, log_logging: bool) -> LoggingState {
    let start = Instant::now();

    // Event links for logging capture (worker-internal, Rc-based).
    let t_link: Rc<EventLink<Duration, Vec<(Duration, TimelyEvent)>>> = Rc::new(EventLink::new());
    let d_link: Rc<EventLink<Duration, Vec<(Duration, DifferentialEvent)>>> =
        Rc::new(EventLink::new());

    // Cross-thread channels for client input and diagnostic output.
    let (client_tx, client_rx) = mpsc::channel::<Event<Duration, ClientContainer>>();
    let (output_tx, output_rx) = mpsc::channel::<DiagnosticEvent>();

    if log_logging {
        install_loggers(worker, t_link.clone(), d_link.clone());
    }

    let traces = worker.dataflow::<Duration, _, _>(|scope| {
        // Replay logging events into the dataflow.
        let timely_stream = Some(t_link.clone()).replay_into(scope);
        let diff_stream = Some(d_link.clone()).replay_into(scope);

        // Build collections and arrangements.
        let (t_traces, t_collections) = construct_timely(scope, timely_stream);
        let (d_traces, d_collections) = construct_differential(scope, diff_stream);

        // Replay client connection events from the WS thread.
        let client_iter = MpscEventIterator { receiver: client_rx };
        let clients: VecCollection<_, usize, i64> =
            Some(client_iter).replay_into(scope).as_collection();

        // Cross-join: clients × each data collection.
        let clients_keyed = clients.map(|c| ((), c));

        // Tag all collections and cross-join with clients.
        let operators_tagged = t_collections.operators
            .map(|(id, name, addr)| ((), DiagnosticUpdate::Operator { id, name, addr }));
        let channels_tagged = t_collections.channels
            .map(|(id, scope_addr, source, target)| {
                ((), DiagnosticUpdate::Channel { id, scope_addr, source, target })
            });

        // Key-only stats: tag them all and concat.
        let stats = concatenate(scope, vec![
            t_collections.elapsed
                .map(|id| ((), DiagnosticUpdate::Stat { kind: StatKind::Elapsed, id })),
            t_collections.messages
                .map(|id| ((), DiagnosticUpdate::Stat { kind: StatKind::Messages, id })),
            d_collections.arrangement_batches
                .map(|id| ((), DiagnosticUpdate::Stat { kind: StatKind::ArrangementBatches, id })),
            d_collections.arrangement_records
                .map(|id| ((), DiagnosticUpdate::Stat { kind: StatKind::ArrangementRecords, id })),
            d_collections.sharing
                .map(|id| ((), DiagnosticUpdate::Stat { kind: StatKind::Sharing, id })),
            d_collections.batcher_records
                .map(|id| ((), DiagnosticUpdate::Stat { kind: StatKind::BatcherRecords, id })),
            d_collections.batcher_size
                .map(|id| ((), DiagnosticUpdate::Stat { kind: StatKind::BatcherSize, id })),
            d_collections.batcher_capacity
                .map(|id| ((), DiagnosticUpdate::Stat { kind: StatKind::BatcherCapacity, id })),
            d_collections.batcher_allocations
                .map(|id| ((), DiagnosticUpdate::Stat { kind: StatKind::BatcherAllocations, id })),
        ]);

        // Concatenate all tagged collections.
        let all_data = concatenate(scope, vec![
            operators_tagged,
            channels_tagged,
            stats,
        ]);

        let output = clients_keyed
            .join(all_data)
            .map(|((), (client_id, update))| (client_id, update));

        // Route all output to worker 0 before capture, since only worker 0
        // runs the WebSocket server.
        output.inner.exchange(|_| 0).capture_into(output_tx);

        LoggingTraces {
            timely: t_traces,
            differential: d_traces,
        }
    });

    if !log_logging {
        install_loggers(worker, t_link, d_link);
    }

    LoggingState {
        traces,
        sink: SinkHandle {
            client_input: ClientInput {
                sender: client_tx,
                time: Duration::default(),
            },
            output_receiver: output_rx,
            start,
        },
    }
}

fn install_loggers<A: Allocate>(
    worker: &mut Worker<A>,
    t_link: Rc<EventLink<Duration, Vec<(Duration, TimelyEvent)>>>,
    d_link: Rc<EventLink<Duration, Vec<(Duration, DifferentialEvent)>>>,
) {
    let mut registry = worker.log_register().expect("Logging not initialized");

    // Use timely's BatchLogger directly — it handles progress tracking
    // with Duration timestamps, matching the logging framework's epoch.
    let mut t_batch = BatchLogger::new(t_link);
    registry.insert::<TimelyEventBuilder, _>("timely", move |time, data| {
        t_batch.publish_batch(time, data);
    });

    let mut d_batch = BatchLogger::new(d_link);
    registry.insert::<DifferentialEventBuilder, _>("differential/arrange", move |time, data| {
        d_batch.publish_batch(time, data);
    });
}

// ============================================================================
// Timely event demux
// ============================================================================

/// Internal: collections before arrangement, used for the cross-join.
struct TimelyCollections<S: Scope> {
    operators: VecCollection<S, (usize, String, Vec<usize>), i64>,
    channels: VecCollection<S, (usize, Vec<usize>, (usize, usize), (usize, usize)), i64>,
    elapsed: VecCollection<S, usize, i64>,
    messages: VecCollection<S, usize, i64>,
}

#[derive(Default)]
struct TimelyDemuxState {
    operators: BTreeMap<usize, OperatesEvent>,
    schedule_starts: BTreeMap<usize, Duration>,
}

/// Build timely logging collections and arrangements.
fn construct_timely<S: Scope<Timestamp = Duration>>(
    scope: &mut S,
    stream: Stream<S, Vec<(Duration, TimelyEvent)>>,
) -> (TimelyTraces, TimelyCollections<S>) {
    type OpUpdate = ((usize, String, Vec<usize>), Duration, i64);
    type ChUpdate = ((usize, Vec<usize>, (usize, usize), (usize, usize)), Duration, i64);
    type ElUpdate = (usize, Duration, i64);
    type MsgUpdate = (usize, Duration, i64);

    let mut demux = OperatorBuilder::new("Timely Demux".to_string(), scope.clone());
    let mut input = demux.new_input(stream, Pipeline);

    let (op_out, operates) = demux.new_output::<Vec<OpUpdate>>();
    let mut op_out = OutputBuilder::<_, CapacityContainerBuilder<Vec<OpUpdate>>>::from(op_out);
    let (ch_out, channels) = demux.new_output::<Vec<ChUpdate>>();
    let mut ch_out = OutputBuilder::<_, CapacityContainerBuilder<Vec<ChUpdate>>>::from(ch_out);
    let (el_out, elapsed) = demux.new_output::<Vec<ElUpdate>>();
    let mut el_out = OutputBuilder::<_, CapacityContainerBuilder<Vec<ElUpdate>>>::from(el_out);
    let (msg_out, messages) = demux.new_output::<Vec<MsgUpdate>>();
    let mut msg_out = OutputBuilder::<_, CapacityContainerBuilder<Vec<MsgUpdate>>>::from(msg_out);

    demux.build(|_capabilities| {
        let mut state = TimelyDemuxState::default();
        move |_frontiers| {
            let mut op_act = op_out.activate();
            let mut ch_act = ch_out.activate();
            let mut el_act = el_out.activate();
            let mut msg_act = msg_out.activate();

            input.for_each(|cap, data: &mut Vec<(Duration, TimelyEvent)>| {
                let mut ops = op_act.session(&cap);
                let mut chs = ch_act.session(&cap);
                let mut els = el_act.session(&cap);
                let mut msgs = msg_act.session(&cap);
                let ts = *cap.time();

                for (event_time, event) in data.drain(..) {
                    match event {
                        TimelyEvent::Operates(e) => {
                            ops.give(((e.id, e.name.clone(), e.addr.clone()), ts, 1i64));
                            state.operators.insert(e.id, e);
                        }
                        TimelyEvent::Shutdown(e) => {
                            if let Some(op) = state.operators.remove(&e.id) {
                                ops.give(((op.id, op.name, op.addr), ts, -1i64));
                            }
                        }
                        TimelyEvent::Channels(e) => {
                            chs.give((
                                (e.id, e.scope_addr.clone(), e.source, e.target),
                                ts,
                                1i64,
                            ));
                        }
                        TimelyEvent::Schedule(e) => match e.start_stop {
                            StartStop::Start => {
                                state.schedule_starts.insert(e.id, event_time);
                            }
                            StartStop::Stop => {
                                if let Some(start) = state.schedule_starts.remove(&e.id) {
                                    let elapsed_ns =
                                        event_time.saturating_sub(start).as_nanos() as i64;
                                    if elapsed_ns > 0 {
                                        els.give((e.id, ts, elapsed_ns));
                                    }
                                }
                            }
                        },
                        TimelyEvent::Messages(e) => {
                            if e.is_send {
                                msgs.give((e.channel, ts, e.record_count as i64));
                            }
                        }
                        _ => {}
                    }
                }
            });
        }
    });

    // Quantize timestamps to interval boundaries.
    let op_collection = quantize_collection(operates.as_collection(), INTERVAL);
    let ch_collection = quantize_collection(channels.as_collection(), INTERVAL);
    let el_collection = quantize_collection(elapsed.as_collection(), INTERVAL);
    let msg_collection = quantize_collection(messages.as_collection(), INTERVAL);

    // Arrange for persistence.
    let operators = op_collection.clone()
        .map(|(id, name, addr)| (id, (name, addr)))
        .arrange_by_key_named("Arrange Operators");
    let channels = ch_collection.clone()
        .map(|(id, scope_addr, source, target)| (id, (scope_addr, source, target)))
        .arrange_by_key_named("Arrange Channels");
    let elapsed = el_collection.clone()
        .arrange_by_self_named("Arrange Elapsed");
    let messages = msg_collection.clone()
        .arrange_by_self_named("Arrange Messages");

    let traces = TimelyTraces {
        operators: operators.trace,
        channels: channels.trace,
        elapsed: elapsed.trace,
        messages: messages.trace,
    };

    let collections = TimelyCollections {
        operators: op_collection,
        channels: ch_collection,
        elapsed: el_collection,
        messages: msg_collection,
    };

    (traces, collections)
}

// ============================================================================
// Differential event demux
// ============================================================================

/// Internal: collections before arrangement, used for the cross-join.
struct DifferentialCollections<S: Scope> {
    arrangement_batches: VecCollection<S, usize, i64>,
    arrangement_records: VecCollection<S, usize, i64>,
    sharing: VecCollection<S, usize, i64>,
    batcher_records: VecCollection<S, usize, i64>,
    batcher_size: VecCollection<S, usize, i64>,
    batcher_capacity: VecCollection<S, usize, i64>,
    batcher_allocations: VecCollection<S, usize, i64>,
}

/// Build differential logging collections and arrangements.
fn construct_differential<S: Scope<Timestamp = Duration>>(
    scope: &mut S,
    stream: Stream<S, Vec<(Duration, DifferentialEvent)>>,
) -> (DifferentialTraces, DifferentialCollections<S>) {
    type Update = (usize, Duration, i64);

    let mut demux = OperatorBuilder::new("Differential Demux".to_string(), scope.clone());
    let mut input = demux.new_input(stream, Pipeline);

    let (bat_out, batches) = demux.new_output::<Vec<Update>>();
    let mut bat_out = OutputBuilder::<_, CapacityContainerBuilder<Vec<Update>>>::from(bat_out);
    let (rec_out, records) = demux.new_output::<Vec<Update>>();
    let mut rec_out = OutputBuilder::<_, CapacityContainerBuilder<Vec<Update>>>::from(rec_out);
    let (shr_out, sharing) = demux.new_output::<Vec<Update>>();
    let mut shr_out = OutputBuilder::<_, CapacityContainerBuilder<Vec<Update>>>::from(shr_out);
    let (br_out, batcher_records) = demux.new_output::<Vec<Update>>();
    let mut br_out = OutputBuilder::<_, CapacityContainerBuilder<Vec<Update>>>::from(br_out);
    let (bs_out, batcher_size) = demux.new_output::<Vec<Update>>();
    let mut bs_out = OutputBuilder::<_, CapacityContainerBuilder<Vec<Update>>>::from(bs_out);
    let (bc_out, batcher_capacity) = demux.new_output::<Vec<Update>>();
    let mut bc_out = OutputBuilder::<_, CapacityContainerBuilder<Vec<Update>>>::from(bc_out);
    let (ba_out, batcher_allocations) = demux.new_output::<Vec<Update>>();
    let mut ba_out = OutputBuilder::<_, CapacityContainerBuilder<Vec<Update>>>::from(ba_out);

    demux.build(|_capabilities| {
        move |_frontiers| {
            let mut bat_act = bat_out.activate();
            let mut rec_act = rec_out.activate();
            let mut shr_act = shr_out.activate();
            let mut br_act = br_out.activate();
            let mut bs_act = bs_out.activate();
            let mut bc_act = bc_out.activate();
            let mut ba_act = ba_out.activate();

            input.for_each(|cap, data: &mut Vec<(Duration, DifferentialEvent)>| {
                let mut bat = bat_act.session(&cap);
                let mut rec = rec_act.session(&cap);
                let mut shr = shr_act.session(&cap);
                let mut b_rec = br_act.session(&cap);
                let mut b_sz = bs_act.session(&cap);
                let mut b_cap = bc_act.session(&cap);
                let mut b_alloc = ba_act.session(&cap);
                let ts = *cap.time();

                for (_event_time, event) in data.drain(..) {
                    match event {
                        DifferentialEvent::Batch(e) => {
                            bat.give((e.operator, ts, 1i64));
                            rec.give((e.operator, ts, e.length as i64));
                        }
                        DifferentialEvent::Merge(e) => {
                            if let Some(complete) = e.complete {
                                bat.give((e.operator, ts, -1i64));
                                let diff = complete as i64 - (e.length1 + e.length2) as i64;
                                if diff != 0 {
                                    rec.give((e.operator, ts, diff));
                                }
                            }
                        }
                        DifferentialEvent::Drop(e) => {
                            bat.give((e.operator, ts, -1i64));
                            let diff = -(e.length as i64);
                            if diff != 0 {
                                rec.give((e.operator, ts, diff));
                            }
                        }
                        DifferentialEvent::TraceShare(e) => {
                            shr.give((e.operator, ts, e.diff as i64));
                        }
                        DifferentialEvent::Batcher(e) => {
                            b_rec.give((e.operator, ts, e.records_diff as i64));
                            b_sz.give((e.operator, ts, e.size_diff as i64));
                            b_cap.give((e.operator, ts, e.capacity_diff as i64));
                            b_alloc.give((e.operator, ts, e.allocations_diff as i64));
                        }
                        _ => {}
                    }
                }
            });
        }
    });

    // Quantize timestamps to interval boundaries.
    let bat_coll = quantize_collection(batches.as_collection(), INTERVAL);
    let rec_coll = quantize_collection(records.as_collection(), INTERVAL);
    let shr_coll = quantize_collection(sharing.as_collection(), INTERVAL);
    let br_coll = quantize_collection(batcher_records.as_collection(), INTERVAL);
    let bs_coll = quantize_collection(batcher_size.as_collection(), INTERVAL);
    let bc_coll = quantize_collection(batcher_capacity.as_collection(), INTERVAL);
    let ba_coll = quantize_collection(batcher_allocations.as_collection(), INTERVAL);

    let traces = DifferentialTraces {
        arrangement_batches: bat_coll.clone().arrange_by_self_named("Arrange ArrangementBatches").trace,
        arrangement_records: rec_coll.clone().arrange_by_self_named("Arrange ArrangementRecords").trace,
        sharing: shr_coll.clone().arrange_by_self_named("Arrange Sharing").trace,
        batcher_records: br_coll.clone().arrange_by_self_named("Arrange BatcherRecords").trace,
        batcher_size: bs_coll.clone().arrange_by_self_named("Arrange BatcherSize").trace,
        batcher_capacity: bc_coll.clone().arrange_by_self_named("Arrange BatcherCapacity").trace,
        batcher_allocations: ba_coll.clone().arrange_by_self_named("Arrange BatcherAllocations").trace,
    };

    let collections = DifferentialCollections {
        arrangement_batches: bat_coll,
        arrangement_records: rec_coll,
        sharing: shr_coll,
        batcher_records: br_coll,
        batcher_size: bs_coll,
        batcher_capacity: bc_coll,
        batcher_allocations: ba_coll,
    };

    (traces, collections)
}
