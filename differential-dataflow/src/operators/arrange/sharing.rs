//! Sharing arrangements across timely runtimes.
//!
//! An arrangement is normally readable only from the worker that maintains it: its batches are
//! reference counted with `Rc` and its trace handle is `Rc<RefCell<..>>`, both pinned to one
//! thread. This module lets a worker publish an arrangement whose batches are `Arc`'d (and whose
//! contents are `Send + Sync`) through a *publication point*, from which readers on other threads
//! take consistent snapshots or import the arrangement into a second timely runtime.
//!
//! The unit that crosses the thread boundary is not the [`Spine`](super::super::super::trace::implementations::spine_fueled::Spine),
//! which holds thread-local state and has a single writer, but the spine's *contents*: a chain of
//! immutable `Arc`'d batches together with the trace's `since` and `upper` frontiers. Because
//! batches are immutable, a chain plus frontiers is a self-describing, consistent view. When the
//! publishing worker later merges batches, a reader holding an older chain is unaffected: its `Arc`s
//! keep the pre-merge batches alive until it drops them.
//!
//! ## Pieces
//!
//! * [`Arranged::publish`] attaches a publisher to an arrangement on the owning worker and returns a
//!   [`Published`] whose [`Published::handle`] hands out `Clone + Send` [`SharedTraceHandle`]s.
//! * [`SharedTraceHandle`] implements [`TraceReader`], so it drives compaction and cursors like any
//!   trace handle. [`SharedTraceHandle::snapshot_at`] serves a point or full-scan read from any
//!   thread. [`SharedTraceHandle::import`] replays the shared arrangement into another scope.
//!
//! ## Compaction
//!
//! Every reader registers logical and physical holds. The publisher forwards their *meet* (the
//! greatest lower bound, so the trace never compacts past the least reader's hold) to its own
//! `TraceAgent`, which is the sole writer of the trace's compaction frontiers. Publishing itself
//! carries no compaction floor: with no readers the publisher advances its hold to the
//! writer-driven frontier (the meet of the other agents' holds), so the trace compacts and merges
//! as the writer advances. The publisher never forwards the empty frontier, which would
//! irreversibly release the trace.

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Condvar, Mutex};

use timely::dataflow::operators::generic::{source, Operator};
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::Scope;
use timely::progress::frontier::{AntichainRef, MutableAntichain};
use timely::progress::Antichain;
use timely::scheduling::activate::SyncActivator;

use crate::lattice::{antichain_meet, Lattice};
use crate::trace::cursor::{cursor_list, CursorList, Navigable};
use crate::trace::{BatchReader, TraceReader};

use super::{Arranged, TraceAgent, TraceReplayInstruction};

/// The queue and wakeup for one importer registered against a publication point.
struct ImportQueue<Tr: TraceReader> {
    /// Replay instructions the publisher appends and the importer drains, mirroring the local
    /// arrange replay queue. Batches carry a hint time that lower-bounds their updates.
    instructions: VecDeque<TraceReplayInstruction<Tr>>,
    /// Wakes the importer's source operator on the reader's worker when new instructions arrive.
    activator: SyncActivator,
}

/// State shared between a publisher (on the owning worker) and its readers (on any thread).
///
/// The `chain`, `since`, and `upper` are always updated together under the lock, so every reader
/// observes a frontier-consistent view.
struct SharedTraceState<Tr: TraceReader> {
    /// The published chain, sourced from `map_batches`: contiguous descriptions including the
    /// seal-only empty batches that never travel on the arrangement stream. Coverage is at least
    /// `upper` (within a worker step it may briefly run ahead, never behind).
    chain: Vec<Tr::Batch>,
    /// Logical compaction frontier of the published view. Reads at times not beyond `since` are not
    /// accurate. A snapshot must pick a time at or beyond it.
    since: Antichain<Tr::Time>,
    /// Seal frontier: the join of the chain's batch uppers. Batches strictly below `upper` are
    /// complete and readable.
    upper: Antichain<Tr::Time>,
    /// Per-registration logical holds. The publisher forwards their meet, falling back to the
    /// writer-driven frontier when empty (never the destructive empty meet of zero holds).
    logical_holds: BTreeMap<usize, Antichain<Tr::Time>>,
    /// Per-registration physical holds, forwarded independently of the logical holds.
    physical_holds: BTreeMap<usize, Antichain<Tr::Time>>,
    /// Importer queues, keyed by registration id. A handle may back several registrations, so this
    /// is keyed separately from any handle.
    queues: BTreeMap<usize, ImportQueue<Tr>>,
    /// Monotonic source of registration ids for holds and queues.
    next_id: usize,
    /// Set when the publisher drops. A terminal empty frontier is enqueued to each importer, so
    /// readers close only after draining what was already published.
    closed: bool,
}

impl<Tr: TraceReader> SharedTraceState<Tr>
where
    Tr::Time: Lattice + Clone,
{
    /// The meet of `holds`, or `fallback` when there are none.
    ///
    /// Never returns the empty meet of zero holds, which is the empty frontier and would tell the
    /// trace to compact everything, permanently releasing the publisher's capability.
    fn compaction_target(
        holds: &BTreeMap<usize, Antichain<Tr::Time>>,
        fallback: &Antichain<Tr::Time>,
    ) -> Antichain<Tr::Time> {
        let mut iter = holds.values();
        match iter.next() {
            None => fallback.clone(),
            Some(first) => {
                let mut acc = first.clone();
                for hold in iter {
                    acc = antichain_meet(&acc.borrow()[..], &hold.borrow()[..]);
                }
                acc
            }
        }
    }
}

/// A publication point paired with its live [`Condvar`], so peek waiters can block for `upper` to
/// advance without a lost-wakeup race: the publisher signals under the same lock it uses to move
/// `upper`.
struct SharedTrace<Tr: TraceReader> {
    state: Mutex<SharedTraceState<Tr>>,
    upper_changed: Condvar,
    /// Total peer count (workers-per-process times processes) of the scope that published this
    /// arrangement. Set once at publish time and never mutated afterward, so `import` reads it
    /// without taking `state`'s lock. Pairwise import (importer worker `i` reads publisher worker
    /// `i`) is sound only when an importing scope shards keys the same way, which requires this to
    /// match the importing scope's own `peers()`.
    peers: usize,
}

type SharedTraceRef<Tr> = Arc<SharedTrace<Tr>>;

/// The result of publishing an arrangement. Holding it keeps the publication point registered;
/// dropping it does not stop the publisher (the publisher lives with its dataflow), but no further
/// handles can be minted from it.
pub struct Published<Tr: TraceReader> {
    shared: SharedTraceRef<Tr>,
}

impl<Tr: TraceReader> Published<Tr>
where
    Tr::Time: Lattice + Clone,
{
    /// Hands out a `Clone + Send` handle to the published arrangement.
    ///
    /// The handle registers a logical hold at the current published `since`, so the arrangement
    /// will not compact past it until the handle (and all its clones) drop.
    pub fn handle(&self) -> SharedTraceHandle<Tr> {
        SharedTraceHandle::register(Arc::clone(&self.shared))
    }
}

/// A `Clone + Send` reader of a published arrangement.
///
/// Implements [`TraceReader`], so downstream operators drive its compaction and acquire cursors as
/// with any trace handle. Each clone carries an independent registration: cloning mints a fresh id
/// and copies the source's holds, so two consumers of one import cannot release each other's holds.
pub struct SharedTraceHandle<Tr: TraceReader> {
    shared: SharedTraceRef<Tr>,
    /// This handle's hold registration id.
    id: usize,
    /// This handle's own logical frontier, mirrored into `logical_holds[id]`. Kept locally so
    /// `get_logical_compaction` can return a borrow.
    logical: Antichain<Tr::Time>,
    /// This handle's own physical frontier, mirrored into `physical_holds[id]`.
    physical: Antichain<Tr::Time>,
}

impl<Tr: TraceReader> SharedTraceHandle<Tr>
where
    Tr::Time: Lattice + Clone,
{
    /// Registers a fresh hold at the current published `since` and returns a handle for it.
    fn register(shared: SharedTraceRef<Tr>) -> Self {
        let (id, since) = {
            let mut state = shared.state.lock().expect("shared trace poisoned");
            let id = state.next_id;
            state.next_id += 1;
            let since = state.since.clone();
            state.logical_holds.insert(id, since.clone());
            state.physical_holds.insert(id, since.clone());
            (id, since)
        };
        Self {
            shared,
            id,
            logical: since.clone(),
            physical: since,
        }
    }

    /// Takes a consistent snapshot of the published arrangement as of `time`, blocking until `upper`
    /// passes `time`.
    ///
    /// Works from any thread. Returns `None` when the snapshot cannot serve `time`, which is either
    /// the publisher closed before `upper` passed `time`, or compaction has advanced `since` beyond
    /// `time` so the accumulation at `time` is no longer accurate. The gate on `since` mirrors the
    /// single-runtime peek path, which errors when the compaction frontier is beyond the read time
    /// rather than returning coalesced results. A caller that needs to tell the two `None` cases
    /// apart should inspect `since` before the read.
    pub fn snapshot_at(&self, time: &Tr::Time) -> Option<TraceSnapshot<Tr>> {
        let mut state = self.shared.state.lock().expect("shared trace poisoned");
        // `upper` not less-equal `time` means all updates at `time` are sealed.
        while state.upper.less_equal(time) {
            if state.closed {
                return None;
            }
            state = self
                .shared
                .upper_changed
                .wait(state)
                .expect("shared trace poisoned");
        }
        // `since` beyond `time` means times at `time` have been coalesced and a read there would be
        // inaccurate. Fail to `None` rather than serve stale data.
        if !state.since.less_equal(time) {
            return None;
        }
        Some(TraceSnapshot {
            chain: state.chain.clone(),
            since: state.since.clone(),
            upper: state.upper.clone(),
        })
    }

    /// Writes this handle's logical or physical hold into the shared registry.
    fn update_hold(&self, logical: bool) {
        let mut state = self.shared.state.lock().expect("shared trace poisoned");
        if logical {
            state.logical_holds.insert(self.id, self.logical.clone());
        } else {
            state.physical_holds.insert(self.id, self.physical.clone());
        }
    }
}

impl<Tr: TraceReader> Clone for SharedTraceHandle<Tr>
where
    Tr::Time: Lattice + Clone,
{
    fn clone(&self) -> Self {
        // A clone must be an independent hold: `import` returns `Arranged { trace: handle.clone() }`
        // and `Arranged` is itself `Clone`, so distinct downstream operators drive compaction on
        // distinct clones. Sharing one hold slot would let the faster operator release the slower
        // one's hold. This mirrors `TraceAgent::clone`, which registers an independent counted hold.
        let id = {
            let mut state = self.shared.state.lock().expect("shared trace poisoned");
            let id = state.next_id;
            state.next_id += 1;
            state.logical_holds.insert(id, self.logical.clone());
            state.physical_holds.insert(id, self.physical.clone());
            id
        };
        Self {
            shared: Arc::clone(&self.shared),
            id,
            logical: self.logical.clone(),
            physical: self.physical.clone(),
        }
    }
}

impl<Tr: TraceReader> Drop for SharedTraceHandle<Tr> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.logical_holds.remove(&self.id);
            state.physical_holds.remove(&self.id);
        }
    }
}

impl<Tr: TraceReader> TraceReader for SharedTraceHandle<Tr>
where
    Tr::Time: Lattice + Clone,
{
    type Time = Tr::Time;
    type Batch = Tr::Batch;

    fn batches_through(&mut self, upper: AntichainRef<Tr::Time>) -> Option<Vec<Self::Batch>> {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        // A clean cut of the published chain: all non-empty batches whose upper is not beyond
        // `upper`, and none whose lower is beyond `upper`. Empty batches are dropped, as
        // `Spine::batches_through` does.
        let mut out = Vec::new();
        for batch in state.chain.iter() {
            // A batch whose lower is beyond the cut, and everything after it in the totally
            // ordered chain, lies past `upper`. Empty batches never carry updates to read.
            if timely::PartialOrder::less_equal(&upper, &batch.lower().borrow()) {
                break;
            }
            if !batch.is_empty() {
                // Fail-stop on a batch that straddles the cut (`lower < upper < batch.upper()`),
                // matching `Spine::batches_through`. Returning it would hand back updates at times
                // not before `upper`, corrupting a downstream `cursor_through` consumer such as
                // `join`. The published chain is totally ordered by description, so this cut is
                // clean unless a caller requested a frontier that is not batch-aligned.
                assert!(
                    timely::PartialOrder::less_equal(&batch.upper().borrow(), &upper),
                    "batches_through: upper straddles batch"
                );
                out.push(batch.clone());
            }
        }
        Some(out)
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<Tr::Time>) {
        self.logical = frontier.to_owned();
        self.update_hold(true);
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.logical.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, Tr::Time>) {
        self.physical = frontier.to_owned();
        self.update_hold(false);
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, Tr::Time> {
        self.physical.borrow()
    }

    fn map_batches<F: FnMut(&Self::Batch)>(&self, mut f: F) {
        let state = self.shared.state.lock().expect("shared trace poisoned");
        for batch in state.chain.iter() {
            f(batch);
        }
    }
}

/// Smallest time, used only to satisfy the borrow in the cut check for empty lower frontiers.
fn batch_min<Tr: TraceReader>() -> Tr::Time {
    <Tr::Time as timely::progress::Timestamp>::minimum()
}

/// An owned, consistent snapshot of a published arrangement: an immutable chain plus its frontiers.
///
/// Holding it pins the chain's batches, keeping their memory alive even as the publishing worker
/// merges. Dropping it releases them, from whatever thread drops last.
pub struct TraceSnapshot<Tr: TraceReader> {
    chain: Vec<Tr::Batch>,
    since: Antichain<Tr::Time>,
    upper: Antichain<Tr::Time>,
}

impl<Tr: TraceReader> TraceSnapshot<Tr> {
    /// The logical compaction frontier of the snapshot. Times not beyond it are not accurate.
    pub fn since(&self) -> AntichainRef<'_, Tr::Time> {
        self.since.borrow()
    }
    /// The seal frontier of the snapshot. Times strictly below it are complete.
    pub fn upper(&self) -> AntichainRef<'_, Tr::Time> {
        self.upper.borrow()
    }
    /// A cursor merging the snapshot's batch cursors, with the batches as its storage.
    pub fn cursor(&self) -> (CursorList<<Tr::Batch as Navigable>::Cursor>, Vec<Tr::Batch>)
    where
        Tr::Batch: Navigable,
    {
        cursor_list(self.chain.clone())
    }
}

impl<'scope, Tr> Arranged<'scope, TraceAgent<Tr>>
where
    Tr: crate::trace::Trace + 'static,
    Tr::Batch: Send + Sync,
    Tr::Time: Lattice + Clone + Send + Sync,
{
    /// Publishes this arrangement through a publication point on the owning worker.
    ///
    /// Attaches a publisher operator to the arrangement stream. On each activation the publisher
    /// refreshes the published chain, `since`, and `upper` from the trace, appends newly arrived
    /// batches to importer queues, and forwards the meet of reader holds to the trace's compaction.
    /// The returned [`Published`] mints [`SharedTraceHandle`]s that read the arrangement from any
    /// thread.
    pub fn publish(&self) -> Published<Tr> {
        self.publish_named("PublishShared")
    }

    /// [`Arranged::publish`], with a name for the publisher operator.
    pub fn publish_named(&self, name: &str) -> Published<Tr> {
        // The publisher owns a `TraceAgent` clone: its read capability is the aggregate lease for
        // all readers, so the trace cannot compact or drop out from under them.
        let mut agent = self.trace.clone();

        let initial_since = agent.get_logical_compaction().to_owned();
        let shared: SharedTraceRef<Tr> = Arc::new(SharedTrace {
            state: Mutex::new(SharedTraceState {
                chain: Vec::new(),
                since: initial_since,
                // NOTE: `Antichain::from_elem(minimum)`, never `Antichain::new()`. The empty
                // frontier reads as "complete through the end of time", making every snapshot wait
                // vacuously true and returning empty results instead of blocking.
                upper: Antichain::from_elem(batch_min::<Tr>()),
                logical_holds: BTreeMap::new(),
                physical_holds: BTreeMap::new(),
                queues: BTreeMap::new(),
                next_id: 0,
                closed: false,
            }),
            upper_changed: Condvar::new(),
            peers: self.stream.scope().peers(),
        });

        let publisher = Publisher {
            shared: Arc::clone(&shared),
        };

        let sink_shared = Arc::clone(&shared);
        self.stream.clone().sink(
            timely::dataflow::channels::pact::Pipeline,
            name,
            move |(input, _frontier)| {
                // Keep `publisher` alive with the operator, so operator (dataflow) drop closes the
                // publication point.
                let _publisher = &publisher;

                // Batches arriving on the stream, each with a capability time that lower-bounds the
                // batch's updates. Empty seal batches do not travel the stream; they are picked up
                // from the trace below.
                let mut arrived: Vec<(Tr::Batch, Tr::Time)> = Vec::new();
                input.for_each(|cap, data| {
                    let hint = cap.time().clone();
                    for batch in data.drain(..) {
                        arrived.push((batch, hint.clone()));
                    }
                });

                // Refresh the chain and frontiers from the trace, the authoritative source.
                let mut chain = Vec::new();
                let mut upper = Antichain::new();
                agent.map_batches(|batch| {
                    chain.push(batch.clone());
                    upper = batch.upper().to_owned();
                });
                if chain.is_empty() {
                    upper = Antichain::from_elem(batch_min::<Tr>());
                }
                // Contract: publishing carries no independent compaction floor. The trace's writer
                // (and, in Materialize, the controller) drive `since` through their own trace
                // handles. Only a live importer's registered hold may hold the trace back, and it
                // releases on drop. The publisher keeps a holding agent solely so importer holds
                // have somewhere to forward to, so that hold must FOLLOW the writer rather than pin
                // the trace.
                //
                // The writer-driven frontier is the meet of all other agents' holds, i.e. the
                // frontier the trace would compact to if the publisher were not holding it. The
                // `TraceBox` accumulates every agent's hold in a `MutableAntichain`; cloning it and
                // subtracting the publisher's own contribution yields that meet. Advancing the
                // publisher's hold to it lets the trace compact and merge as the writer advances.
                //
                // NOTE: the per-batch `since` from `map_batches` cannot serve as this source. Those
                // frontiers advance only when the Spine actually compacts, which the publisher's own
                // pinning hold prevents, so they stay frozen at the publish-time `since`. Reading
                // the trace-box meet breaks that circularity.
                let publisher_logical = agent.get_logical_compaction().to_owned();
                let publisher_physical = agent.get_physical_compaction().to_owned();
                // The writer-driven frontier for one dimension: the meet of the accumulated holds
                // with the publisher's own contribution removed. An empty result means the
                // publisher is the sole holder. Never forward the empty frontier: it would compact
                // everything and release the publisher's capability, so fall back to the publisher's
                // current hold and keep the current floor.
                let others_meet =
                    |accumulated: &MutableAntichain<Tr::Time>, own: &Antichain<Tr::Time>| {
                        let mut others = accumulated.clone();
                        others.update_iter(own.iter().map(|t| (t.clone(), -1)));
                        if others.frontier().is_empty() {
                            own.clone()
                        } else {
                            others.frontier().to_owned()
                        }
                    };
                let (writer_logical, writer_physical) = {
                    let trace_box = agent.trace_box_unstable();
                    let trace_box = trace_box.borrow();
                    (
                        others_meet(&trace_box.logical_compaction, &publisher_logical),
                        others_meet(&trace_box.physical_compaction, &publisher_physical),
                    )
                };

                let (logical_target, physical_target) = {
                    let mut state = sink_shared.state.lock().expect("shared trace poisoned");

                    for (batch, hint) in arrived.drain(..) {
                        for queue in state.queues.values_mut() {
                            queue.instructions.push_back(TraceReplayInstruction::Batch(
                                batch.clone(),
                                Some(hint.clone()),
                            ));
                        }
                    }

                    let upper_advanced = state.upper != upper;
                    if upper_advanced {
                        for queue in state.queues.values_mut() {
                            queue
                                .instructions
                                .push_back(TraceReplayInstruction::Frontier(upper.clone()));
                        }
                    }

                    // Fall back to the writer-driven frontier (not the publisher's frozen hold) when
                    // there are no reader holds, so with zero readers the target follows the writer.
                    let logical = SharedTraceState::<Tr>::compaction_target(
                        &state.logical_holds,
                        &writer_logical,
                    );
                    let physical = SharedTraceState::<Tr>::compaction_target(
                        &state.physical_holds,
                        &writer_physical,
                    );

                    state.chain = chain;
                    // Publish the trace's real logical compaction after we forward `logical` below.
                    // Agent compaction only advances (joins), so the publisher's hold becomes
                    // `join(publisher_logical, logical)`. The trace's real compaction is the meet of
                    // every agent's hold: the publisher's post-forward hold and the writer-driven
                    // meet of the others. Publishing exactly that keeps the gate in step with the
                    // trace, so a reader hold keeps its own frontier readable rather than being
                    // raced past by the writer. It is never below the real compaction (it equals
                    // it), so a handle registering in this window cannot latch an anti-conservative
                    // `since` that claims accuracy at already-merged times.
                    let publisher_after = publisher_logical.join(&logical);
                    state.since =
                        antichain_meet(&publisher_after.borrow()[..], &writer_logical.borrow()[..]);
                    state.upper = upper;

                    // Wake importers and any peek waiters.
                    for queue in state.queues.values() {
                        let _ = queue.activator.activate();
                    }
                    if upper_advanced {
                        sink_shared.upper_changed.notify_all();
                    }

                    (logical, physical)
                };

                // Apply compaction to the agent OUTSIDE the lock: `set_physical_compaction` can run
                // an unbounded merge synchronously, which must not block concurrent readers.
                agent.set_logical_compaction(logical_target.borrow());
                agent.set_physical_compaction(physical_target.borrow());
            },
        );

        Published { shared }
    }
}

/// Guard that marks the publication point closed when the publisher operator drops, waking readers
/// so they drain and shut down.
struct Publisher<Tr: TraceReader> {
    shared: SharedTraceRef<Tr>,
}

impl<Tr: TraceReader> Drop for Publisher<Tr> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.closed = true;
            let empty = Antichain::new();
            for queue in state.queues.values_mut() {
                queue
                    .instructions
                    .push_back(TraceReplayInstruction::Frontier(empty.clone()));
                let _ = queue.activator.activate();
            }
        }
        self.shared.upper_changed.notify_all();
    }
}

impl<Tr: TraceReader> SharedTraceHandle<Tr>
where
    Tr: 'static,
    Tr::Time: Lattice + Clone,
    Tr::Batch: Navigable,
{
    /// Imports the published arrangement into `scope`, returning an [`Arranged`] backed by this
    /// shared trace.
    ///
    /// Requires `scope`'s total peer count (workers-per-process times processes) to equal the
    /// publisher's, panicking otherwise. Pairwise import (importer worker `i` reads publisher
    /// worker `i`) is sound only when both sides shard by the same `key.hashed() % peers`; a
    /// mismatched peer count would silently read the wrong shard instead of failing loudly.
    ///
    /// The importer registers a replay queue seeded with the current chain and drains it as the
    /// publisher appends. The registration is owned by the source operator, so dropping the import
    /// dataflow deregisters it and releases its holds even while other handle clones and the reader
    /// worker live on.
    pub fn import<'scope>(
        &self,
        scope: Scope<'scope, Tr::Time>,
        name: &str,
    ) -> Arranged<'scope, SharedTraceHandle<Tr>> {
        assert_eq!(
            scope.peers(),
            self.shared.peers,
            "shared-trace import requires equal total peers (workers_per_process * num_processes)"
        );

        let shared = Arc::clone(&self.shared);
        let trace = self.clone();

        let stream = source(scope, name, move |capability, info| {
            let activator = scope.worker().sync_activator_for(info.address.to_vec());

            // Register under one lock acquisition: mint an id, seed the queue with the current
            // chain (hint `minimum`, as the local replay does for historical batches) followed by
            // the current upper, and install the queue. Later batches append; earlier ones are
            // seeded. Nothing is missed or duplicated.
            let reg_id = {
                let mut state = shared.state.lock().expect("shared trace poisoned");
                let reg_id = state.next_id;
                state.next_id += 1;
                let mut instructions = VecDeque::new();
                for batch in state.chain.iter() {
                    instructions.push_back(TraceReplayInstruction::Batch(
                        batch.clone(),
                        Some(batch_min::<Tr>()),
                    ));
                }
                instructions.push_back(TraceReplayInstruction::Frontier(state.upper.clone()));
                // If the publisher already closed, its one-shot terminal frontier has been and gone,
                // so seed our own. Otherwise a late importer would drain the chain and then wait
                // forever for a frontier that never arrives, leaking its capability. Mirrors the
                // `state.closed` guard in `snapshot_at`.
                if state.closed {
                    instructions.push_back(TraceReplayInstruction::Frontier(Antichain::new()));
                }
                state.queues.insert(
                    reg_id,
                    ImportQueue {
                        instructions,
                        activator,
                    },
                );
                reg_id
            };

            let mut capabilities = Some(CapabilitySet::new());
            capabilities.as_mut().unwrap().insert(capability);

            // Deregisters the queue when the source operator (and thus this closure) drops.
            let _guard = QueueGuard {
                shared: Arc::clone(&shared),
                reg_id,
            };

            move |output| {
                let _guard = &_guard;
                let mut drained = Vec::new();
                {
                    let mut state = shared.state.lock().expect("shared trace poisoned");
                    if let Some(queue) = state.queues.get_mut(&reg_id) {
                        drained.extend(queue.instructions.drain(..));
                    }
                }

                if let Some(caps) = capabilities.as_mut() {
                    for instruction in drained {
                        match instruction {
                            TraceReplayInstruction::Frontier(frontier) => {
                                if frontier.is_empty() {
                                    // Terminal frontier: publisher closed, no more capabilities.
                                    capabilities = None;
                                    break;
                                }
                                caps.downgrade(&frontier.borrow()[..]);
                            }
                            TraceReplayInstruction::Batch(batch, hint) => {
                                if let Some(time) = hint {
                                    if !batch.is_empty() {
                                        // Emit under a capability delayed to the batch's hint, a
                                        // lower bound on its times. Never the batch upper: that
                                        // would let the frontier pass updates still in the batch.
                                        let cap = caps.delayed(&time);
                                        output.session(&cap).give(batch);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Arranged { stream, trace }
    }
}

/// Deregisters an importer's replay queue when its source operator drops.
struct QueueGuard<Tr: TraceReader> {
    shared: SharedTraceRef<Tr>,
    reg_id: usize,
}

impl<Tr: TraceReader> Drop for QueueGuard<Tr> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.queues.remove(&self.reg_id);
        }
    }
}
