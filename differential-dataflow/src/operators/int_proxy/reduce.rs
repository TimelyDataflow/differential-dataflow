//! The proxy reduce framework.
//!
//! A conventional differential reduce against `(u64, u64)`, where the backend supplies the
//! implementation of the interpretation of the integers.

use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::{BatchReader, Description};
use super::ProxyBridge;
use crate::operators::reduce::{sort_dedup, ReduceTactic};

/// A unit of proxied reduce work, presented to the backend.
pub struct ReduceInstance<'a, B1: BatchReader, B2: BatchReader<Time = B1::Time>> {
    /// The accumulated input history.
    pub source_batches: &'a [B1],
    /// The freshly arrived input delta.
    pub input_batches: &'a [B1],
    /// The accumulated output history.
    pub output_batches: &'a [B2],
    /// The compaction frontier for loading (the retire's lower bound).
    pub lower: AntichainRef<'a, B1::Time>,
}

/// One window of the key space: the presentations a bounded, hash-contiguous snip needs.
///
/// The two input runs are held apart because the novel and prior data have different roles:
/// the novel data seed interesting times, and the prior data may benefit from historical rollup.
/// Their updates are combined after we are able to see the distinction between the two.
///
/// Owned by the harness and refilled by [`ProxyReduceBackend::next_window`]. The harness assumes
/// ownership of the presented records: it reorders (and advances the times of) each key's records
/// in place as it works the window, so a backend must not expect to read them back.
pub struct ReduceWindow<T, RIn, ROut> {
    /// Accumulated input preceding the retire's interval, sorted & consolidated by
    /// `((key_hash, value_id), time)`.
    pub history: ProxyBridge<T, RIn>,
    /// The retire's novel input delta, same ordering. Its times per key are the interesting-time
    /// seeds, so it must be the batch's own time support — not a view netted against `history`.
    pub novel: ProxyBridge<T, RIn>,
    /// Accumulated output preceding the retire's interval, same ordering.
    pub output: ProxyBridge<T, ROut>,
}

impl<T, RIn, ROut> Default for ReduceWindow<T, RIn, ROut> {
    fn default() -> Self { ReduceWindow { history: Vec::new(), novel: Vec::new(), output: Vec::new() } }
}

impl<T, RIn, ROut> ReduceWindow<T, RIn, ROut> {
    /// Clear the three proxy bridges.
    pub fn clear(&mut self) {
        self.history.clear();
        self.novel.clear();
        self.output.clear();
    }
}

/// The reduce backend: value semantics for a proxy-space reduction, driven by [`ProxyReduceTactic`].
///
/// The protocol for each round of invocation is
/// `begin [ next_window reduce_corrections* emit ]* finish`,
/// where the window loop runs until `next_window` reports the key space exhausted.
pub trait ProxyReduceBackend<B1: BatchReader, B2: BatchReader<Time = B1::Time>> {
    /// Diff type presented for the input.
    type RIn: Semigroup;
    /// Diff type of the output.
    type ROut: Semigroup + 'static;

    /// Initiate a session to create batches for these descriptions, which span `[lower, upper)`.
    ///
    /// It is the backend's job to prepare output batches for each of these descriptions.
    /// The computation proceeds in windows of keys, where only the backend maintains this
    /// work in progress, until `finish()` is called.
    fn begin(&mut self, tiles: &[Description<B1::Time>]);

    /// Present the next window of the key space, and advance `from` past it.
    ///
    /// On entry `from` is the inclusive lower bound on key hashes still to be covered. The backend
    /// chooses the window's exclusive upper bound and writes it back, or writes `None` to report the
    /// key space exhausted. An implementor must advance `from`, as it is guaranteed to be non-`None`.
    ///
    /// The window must present, for every key hash in `[from_before, from_after)` that either
    /// carries an update in the instance's novel batches or appears in `changed`, that key's novel
    /// updates, its accumulated input, and its accumulated output. A key must be reported entirely
    /// within the window that first mentions it: splitting one across windows drops the interaction
    /// between the halves. `changed` is ascending; the harness reads no key outside the window's
    /// range, so a backend that keeps its own key order need not consult the whole space.
    ///
    /// The size of the window is up to the backend: large enough to amortize the crossings, small
    /// enough that the three presentations are affordable, as all are live at once.
    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, B1, B2>,
        changed: &[u64],
        from: &mut Option<u64>,
        window: &mut ReduceWindow<B1::Time, Self::RIn, Self::ROut>,
    );

    /// A wave of input-output reconciliation, in which the backend supplies necessary edits.
    ///
    /// Multiple keys are provided concurrently, for each an accumulated input and tentative output.
    /// The backend should provide for each key the necessary output updates to bring the output in
    /// with its desires. The `usize` integers upper bound the range for the corresponding key.
    fn reduce_corrections(
        &mut self,
        keys: &[u64],
        in_ends: &[usize],
        input: &[(u64, Self::RIn)],
        out_ends: &[usize],
        output: &[(u64, Self::ROut)],
    ) -> (Vec<(u64, Self::ROut)>, Vec<usize>);

    /// Commit to a collection of updates at a specific batch in progress.
    ///
    /// The `tile: usize` indexes the list of descriptions provided to `begin()`, and these updates
    /// are aimed at that batch in progress.
    ///
    /// Within one retire, successive calls for the same tile carry disjoint, ascending key ranges
    /// (the windows partition the key space in ascending order), and each call's records arrive
    /// consolidated. A backend can therefore build its output incrementally — streaming records
    /// into containers it seals as they fill — rather than staging the retire's output as rows.
    fn emit(&mut self, tile: usize, records: &[((u64, u64), B1::Time, Self::ROut)]);

    /// Complete the session matching `begin`. The outputs correspond to the descriptions it was provided.
    fn finish(&mut self) -> Vec<B2>;
}

/// A proxy-space [`ReduceTactic`]: matches input and output records by `key_hash`.
pub struct ProxyReduceTactic<B1: BatchReader, B2: BatchReader<Time = B1::Time>, Bk: ProxyReduceBackend<B1, B2>> {
    backend: Bk,
    /// Pending interesting times beyond the upper frontier, as parallel `(key hash, time)`
    /// columns sorted by `(key, time)` and deduplicated.
    pending_keys: Vec<u64>,
    pending_time: Vec<B1::Time>,
    /// Reusable scratch, held across retires so its capacities persist.
    scratch: Scratch<B1::Time, Bk::RIn, Bk::ROut>,
}

impl<B1: BatchReader, B2: BatchReader<Time = B1::Time>, Bk: ProxyReduceBackend<B1, B2>> ProxyReduceTactic<B1, B2, Bk> {
    /// A tactic deferring all value semantics to `backend`.
    pub fn new(backend: Bk) -> Self {
        ProxyReduceTactic { backend, pending_keys: Vec::new(), pending_time: Vec::new(), scratch: Scratch::new() }
    }
}

/// The working state a retire builds and tears down, pooled across retires so nothing is
/// reallocated: each piece is cleared or refilled at its natural boundary — the retire, the
/// window, the wave, or the tick. Fresh per-key/per-wave `Vec`s were once the dominant cost of
/// the tactic, and per-retire ones the residue of that: the retires of an iterative computation
/// are many and small, and their setup must cost accordingly.
struct Scratch<T, RIn, ROut> {
    /// The window the backend refills per `next_window` call; the harness then owns and reorders it.
    window: ReduceWindow<T, RIn, ROut>,
    /// Per-key sweeps, reused across windows and retires.
    slots: Vec<KeySweep<T, RIn, ROut>>,
    /// The slots (indices) still suspended at a time, i.e. participating in the next wave.
    live: Vec<usize>,
    /// Output updates staged per held time, drained into `emit` per window. Sized to the largest
    /// held set seen; a retire uses the prefix matching its own held times.
    tile_deltas: Vec<Vec<((u64, u64), T, ROut)>>,
    /// One wave's staging: the participating keys, and the concatenated accumulations with their
    /// per-key ends, as `reduce_corrections` receives them.
    batch_keys: Vec<u64>,
    in_ends: Vec<usize>,
    in_all: Vec<(u64, RIn)>,
    out_ends: Vec<usize>,
    out_all: Vec<(u64, ROut)>,
    active: Vec<(usize, T)>,
    in_accum: Vec<(u64, RIn)>,
    cur_out: Vec<(u64, ROut)>,
    /// Scratch for one step's synthesized times, shared by every sweep: nothing in it outlives a
    /// single tick.
    temporary: Vec<T>,
    /// The out-of-interval times one `next_crossing` call reaches, drained into `new_pending`
    /// (with the key attached) as soon as the call returns.
    pended: Vec<T>,
    /// Per-record suffix meets for the window's bridges, aligned by index and scoped per key:
    /// entry `i` is the meet of the times the record's key still has at or after record `i`.
    /// One shared column per run, rather than per-key state.
    novel_meets: Vec<T>,
    input_meets: Vec<T>,
    output_meets: Vec<T>,
    /// The retire's due half of the pending columns, with the suffix meets of each key's times,
    /// and the deduplicated due keys handed to the backend as `changed`.
    due_keys: Vec<u64>,
    due_time: Vec<T>,
    due_meets: Vec<T>,
    changed: Vec<u64>,
    /// The next retire's pending pairs: the carried half plus everything pended, sorted once at
    /// retire end.
    new_pending: Vec<(u64, T)>,
}

impl<T, RIn, ROut> Scratch<T, RIn, ROut> {
    fn new() -> Self {
        Scratch {
            window: ReduceWindow::default(),
            slots: Vec::new(),
            live: Vec::new(),
            tile_deltas: Vec::new(),
            batch_keys: Vec::new(),
            in_ends: Vec::new(),
            in_all: Vec::new(),
            out_ends: Vec::new(),
            out_all: Vec::new(),
            active: Vec::new(),
            in_accum: Vec::new(),
            cur_out: Vec::new(),
            temporary: Vec::new(),
            pended: Vec::new(),
            novel_meets: Vec::new(),
            input_meets: Vec::new(),
            output_meets: Vec::new(),
            due_keys: Vec::new(),
            due_time: Vec::new(),
            due_meets: Vec::new(),
            changed: Vec::new(),
            new_pending: Vec::new(),
        }
    }
}

impl<B1, B2, Bk> ReduceTactic<B1, B2> for ProxyReduceTactic<B1, B2, Bk>
where
    B1: BatchReader,
    B2: BatchReader<Time = B1::Time>,
    Bk: ProxyReduceBackend<B1, B2>,
{
    fn retire(
        &mut self,
        source_batches: Vec<B1>,
        output_batches: Vec<B2>,
        input_batches: Vec<B1>,
        lower: &Antichain<B1::Time>,
        upper: &Antichain<B1::Time>,
        held: &Antichain<B1::Time>,
    ) -> (Vec<(B1::Time, B2)>, Antichain<B1::Time>) {
        if held.elements().iter().all(|t| upper.less_equal(t)) {
            return (Vec::new(), held.clone());
        }

        let instance = ReduceInstance {
            source_batches: &source_batches,
            input_batches: &input_batches,
            output_batches: &output_batches,
            lower: lower.borrow(),
        };

        // Nothing due and nothing novel: no time in the interval can be interesting, so there is no
        // work and no output. Return the frontier bounding the times still withheld — NOT an empty
        // one. This is exactly where a due-only `changed` differs from the whole pending set: times
        // beyond `upper` can remain when nothing is due, and releasing their capabilities would
        // strand them (see the frontier clause of the `ReduceTactic::retire` contract).
        if self.pending_time.iter().all(|t| upper.less_equal(t)) && instance.input_batches.iter().all(|b| b.is_empty()) {
            let mut frontier = Antichain::new();
            for time in self.pending_time.iter() {
                frontier.insert_ref(time);
            }
            return (Vec::new(), frontier);
        }

        // Split the carried interesting times against `upper`.
        // A time below it is DUE: its key must be re-evaluated this retire, so the key is `changed`.
        // A time at or beyond it is carried forward untouched, seeding the next `pending`.
        // Both splits inherit the `(key, time)` order, so the due half is consumed by a single
        // ascending cursor as the windows march the key space.
        let scratch = &mut self.scratch;
        scratch.due_keys.clear();
        scratch.due_time.clear();
        debug_assert!(scratch.new_pending.is_empty(), "drained at the end of the previous retire");
        for (key, time) in self.pending_keys.drain(..).zip(self.pending_time.drain(..)) {
            if upper.less_equal(&time) { scratch.new_pending.push((key, time)); }
            else { scratch.due_keys.push(key); scratch.due_time.push(time); }
        }
        // The keys the harness knows must be revisited. The backend adds those its novel batches
        // touch, which it discovers while reading them; neither side scans the whole key space.
        scratch.changed.clear();
        scratch.changed.extend_from_slice(&scratch.due_keys);
        scratch.changed.dedup();
        // Suffix meets of each key's due times, aligned with the due columns.
        scratch.due_meets.clear();
        scratch.due_meets.extend_from_slice(&scratch.due_time);
        for i in (1..scratch.due_meets.len()).rev() {
            if scratch.due_keys[i - 1] == scratch.due_keys[i] {
                let (init, tail) = scratch.due_meets.split_at_mut(i);
                init[i - 1].meet_assign(&tail[0]);
            }
        }

        // The output tiling (identical to the Abelian tactic): one tile per held time, keeping
        // non-degenerate intervals; `tile_of[i]` maps held time `i` to its tile.
        let held_elems: Vec<B1::Time> = held.elements().to_vec();
        let (tile_descs, tile_held, tile_of) = tile_descriptions(lower, upper, &held_elems);
        self.backend.begin(&tile_descs);

        // Progress through the key space: `Some(h)` for key hashes at or above `h` remaining, `None`
        // once the backend reports the space covered.
        let mut from = Some(0u64);

        // The ascending cursor into the due columns: window ranges ascend and bracket keys ascend
        // within them, so one pass serves the whole retire. A due key it passes without a bracket
        // has no records at all, and its times drop — see the bracket loop's comment.
        let mut due_pos = 0usize;

        while scratch.tile_deltas.len() < held_elems.len() { scratch.tile_deltas.push(Vec::new()); }

        while from.is_some() {
            let before = from;
            scratch.window.clear();
            self.backend.next_window(&instance, &scratch.changed, &mut from, &mut scratch.window);
            let window = &mut scratch.window;
            super::debug_assert_sorted_bridge(&window.history, "next_window.history");
            super::debug_assert_sorted_bridge(&window.novel, "next_window.novel");
            super::debug_assert_sorted_bridge(&window.output, "next_window.output");
            // Without progress the window loop would never retire, so this guards liveness as well
            // as contract; the range check catches a key reported outside the window that owns it,
            // which would silently drop the interaction between its halves.
            debug_assert!(
                from.is_none() || from > before,
                "next_window must either advance `from` or report the key space exhausted",
            );
            debug_assert!(
                {
                    let mut keys = window.history.iter().chain(window.novel.iter()).map(|r| r.0.0)
                        .chain(window.output.iter().map(|r| r.0.0));
                    keys.all(|k| before.is_none_or(|b| b <= k) && from.is_none_or(|f| k < f))
                },
                "next_window must report a key hash entirely within the window that first mentions it",
            );

            for deltas in scratch.tile_deltas.iter_mut() { deltas.clear(); }

            // The window's keys are the hashes its presentations mention: the least of the three
            // heads, each iteration, until all three are drained. A `changed` key that appears in
            // none of them has no records at all, so its reduction has nothing to read and nothing
            // to retract — the time its due moment would raise reaches the evaluation gate with an
            // empty input and an empty output, and produces nothing. Skipping it is exactly what
            // visiting it would do. (`changed` is still the backend's instruction about which keys
            // to present; it is just not a source of keys here.)
            //
            // Each key gets a `Sweep`, which discovers and evaluates in ONE ascending pass,
            // suspending where the conventional reduce would call user logic. Slots are reused
            // across windows, so a key costs no allocation of its own beyond the first window wide
            // enough to need it. Peak state is O(window presentation), bounded by what
            // `next_window` already materialized.
            let mut n_slots = 0usize;
            let (mut is, mut ns, mut os) = (0usize, 0usize, 0usize);
            scratch.live.clear();
            scratch.novel_meets.clear();
            scratch.novel_meets.resize(window.novel.len(), <B1::Time as Timestamp>::minimum());
            scratch.input_meets.clear();
            scratch.input_meets.resize(window.history.len(), <B1::Time as Timestamp>::minimum());
            scratch.output_meets.clear();
            scratch.output_meets.resize(window.output.len(), <B1::Time as Timestamp>::minimum());
            // Mapped to hashes before the min: the three runs differ in their diff type.
            while let Some(key) = [
                window.history.get(is).map(|record| record.0.0),
                window.novel.get(ns).map(|record| record.0.0),
                window.output.get(os).map(|record| record.0.0),
            ].into_iter().flatten().min() {
                let i0 = is;
                while is < window.history.len() && window.history[is].0.0 == key { is += 1; }
                let i1 = is;
                let n0 = ns;
                while ns < window.novel.len() && window.novel[ns].0.0 == key { ns += 1; }
                let n1 = ns;
                let o0 = os;
                while os < window.output.len() && window.output[os].0.0 == key { os += 1; }
                let o1 = os;
                // Only the DUE times seed the sweep; the carried ones are already in `new_pending`.
                while due_pos < scratch.due_keys.len() && scratch.due_keys[due_pos] < key { due_pos += 1; }
                let d0 = due_pos;
                while due_pos < scratch.due_keys.len() && scratch.due_keys[due_pos] == key { due_pos += 1; }
                let d1 = due_pos;

                // Rearrange the key's presentations for the sweep, in place: the sweep visits the
                // key's times in ascending total order, so each bracket is sorted by `(time, id)`
                // and annotated with its suffix meets. The novel run is left unadvanced — its own
                // times are the seeds the schedule is stated over. Only then is the seed meet
                // known, and the join partners advanced (lifted) by it: the lift is what collapses
                // a long-but-quiet history to the few times that still matter, and losing it turns
                // linear sweeps quadratic. Lifting disturbs the total order, so `prepare` sorts
                // after advancing.
                prepare(&mut window.novel[n0..n1], &mut scratch.novel_meets[n0..n1], None);
                let mut meet: Option<B1::Time> = None;
                if n1 > n0 { update_meet(&mut meet, Some(&scratch.novel_meets[n0])); }
                if d1 > d0 { update_meet(&mut meet, Some(&scratch.due_meets[d0])); }
                prepare(&mut window.history[i0..i1], &mut scratch.input_meets[i0..i1], meet.as_ref());
                prepare(&mut window.output[o0..o1], &mut scratch.output_meets[o0..o1], meet.as_ref());

                if n_slots == scratch.slots.len() { scratch.slots.push(KeySweep::empty()); }
                let slot = &mut scratch.slots[n_slots];
                slot.key = key;
                slot.novel = (n0, n1);
                slot.input = (i0, i1);
                slot.output = (o0, o1);
                slot.due = (d0, d1);
                slot.sweep.load(meet);
                let view = KeyView::of(slot, window, &scratch.novel_meets, &scratch.input_meets, &scratch.output_meets, &scratch.due_time, &scratch.due_meets);
                slot.at = slot.sweep.next_crossing(&view, upper, &mut scratch.pended, &mut scratch.temporary);
                scratch.new_pending.extend(scratch.pended.drain(..).map(|t| (key, t)));
                if slot.at.is_some() { scratch.live.push(n_slots); }
                n_slots += 1;
            }

            // Each wave: read every suspended key's accumulations, cross the non-empty ones in one
            // call, hand the corrections back, and step every live key on. A key retires when its
            // sweep runs dry, at which point its pended times are carried forward.
            while !scratch.live.is_empty() {
                scratch.batch_keys.clear();
                scratch.in_ends.clear();
                scratch.in_all.clear();
                scratch.out_ends.clear();
                scratch.out_all.clear();
                scratch.active.clear();

                for &si in scratch.live.iter() {
                    let at = scratch.slots[si].at.clone().expect("live slots are suspended at a time");
                    scratch.in_accum.clear();
                    scratch.cur_out.clear();
                    scratch.slots[si].sweep.input_at(&at, &mut scratch.in_accum);
                    scratch.slots[si].sweep.output_at(&at, &mut scratch.cur_out);
                    // An interesting time can still reach the gate with nothing to read; the
                    // conventional reduce skips user logic there and so do we.
                    if scratch.in_accum.is_empty() && scratch.cur_out.is_empty() { continue; }
                    scratch.batch_keys.push(scratch.slots[si].key);
                    scratch.in_all.append(&mut scratch.in_accum);
                    scratch.in_ends.push(scratch.in_all.len());
                    scratch.out_all.append(&mut scratch.cur_out);
                    scratch.out_ends.push(scratch.out_all.len());
                    scratch.active.push((si, at));
                }

                if !scratch.batch_keys.is_empty() {
                    let (corr, corr_ends) = self.backend.reduce_corrections(&scratch.batch_keys, &scratch.in_ends, &scratch.in_all, &scratch.out_ends, &scratch.out_all);
                    let mut cstart = 0usize;
                    for (bi, (si, at)) in scratch.active.iter().enumerate() {
                        let cend = corr_ends[bi];
                        if cstart != cend {
                            let idx = held_elems.iter().rposition(|h| h.less_equal(at)).expect("no held capability <= active time");
                            for (vid, d) in &corr[cstart..cend] {
                                scratch.tile_deltas[idx].push(((scratch.slots[*si].key, *vid), at.clone(), d.clone()));
                            }
                            scratch.slots[*si].sweep.commit(at, corr[cstart..cend].iter().cloned());
                        }
                        cstart = cend;
                    }
                }

                // Step every live key past the time it was suspended at, and retire the spent ones.
                for &si in scratch.live.iter() {
                    let slot = &mut scratch.slots[si];
                    let view = KeyView::of(slot, window, &scratch.novel_meets, &scratch.input_meets, &scratch.output_meets, &scratch.due_time, &scratch.due_meets);
                    slot.at = slot.sweep.next_crossing(&view, upper, &mut scratch.pended, &mut scratch.temporary);
                    let key = slot.key;
                    scratch.new_pending.extend(scratch.pended.drain(..).map(|t| (key, t)));
                }
                let slots = &scratch.slots;
                scratch.live.retain(|&si| slots[si].at.is_some());
            }

            for (held_index, deltas) in scratch.tile_deltas[..held_elems.len()].iter_mut().enumerate() {
                if deltas.is_empty() {
                    continue;
                }
                if let Some(tile) = tile_of[held_index] {
                    crate::consolidation::consolidate_updates(deltas);
                    self.backend.emit(tile, &deltas[..]);
                }
            }
        }

        // One sort re-establishes the `(key, time)` order the next retire's cursor relies on: the
        // carried half is pushed before any window runs, so it interleaves with the pended times.
        sort_dedup(&mut scratch.new_pending);
        self.pending_keys.extend(scratch.new_pending.iter().map(|(key, _)| *key));
        self.pending_time.extend(scratch.new_pending.drain(..).map(|(_, time)| time));

        let produced: Vec<(B1::Time, B2)> = tile_held.into_iter().zip(self.backend.finish()).collect();
        let mut frontier = Antichain::new();
        for t in self.pending_time.iter() {
            frontier.insert_ref(t);
        }
        (produced, frontier)
    }
}

/// One key's slot in a window: the ranges of its records in the window's presentations, its
/// [`Sweep`], and the time it is suspended at. Slots are reused across windows, so a key costs no
/// allocation of its own beyond the first window wide enough to need it.
struct KeySweep<T, RIn, ROut> {
    key: u64,
    /// The key's `[lower, upper)` record ranges in the window's novel, history, and output runs,
    /// and in the retire's due columns.
    novel: (usize, usize),
    input: (usize, usize),
    output: (usize, usize),
    due: (usize, usize),
    sweep: Sweep<T, RIn, ROut>,
    /// The time the sweep last suspended at, or `None` once it is spent.
    at: Option<T>,
}

impl<T: Timestamp + Lattice, RIn: Semigroup + Clone, ROut: Semigroup + Clone> KeySweep<T, RIn, ROut> {
    fn empty() -> Self {
        KeySweep {
            key: 0,
            novel: (0, 0), input: (0, 0), output: (0, 0), due: (0, 0),
            sweep: Sweep::new(), at: None,
        }
    }
}

/// One key's presentation, resliced from the window and the shared meet columns each time the
/// key's sweep runs: the sweep itself holds only positions, so nothing here is per-key state.
struct KeyView<'a, T, RIn, ROut> {
    /// The key's three record runs, each sorted by `(time, id)` (history and output lifted by the
    /// seed meet), and the aligned suffix meets of each.
    novel: &'a [((u64, u64), T, RIn)],
    novel_meets: &'a [T],
    input: &'a [((u64, u64), T, RIn)],
    input_meets: &'a [T],
    output: &'a [((u64, u64), T, ROut)],
    output_meets: &'a [T],
    /// The key's due interesting times, ascending, with their suffix meets.
    due: &'a [T],
    due_meets: &'a [T],
}

impl<'a, T, RIn, ROut> KeyView<'a, T, RIn, ROut> {
    /// The view for `slot`, sliced from the window it was loaded against.
    fn of(
        slot: &KeySweep<T, RIn, ROut>,
        window: &'a ReduceWindow<T, RIn, ROut>,
        novel_meets: &'a [T],
        input_meets: &'a [T],
        output_meets: &'a [T],
        due_time: &'a [T],
        due_meets: &'a [T],
    ) -> Self {
        KeyView {
            novel: &window.novel[slot.novel.0..slot.novel.1],
            novel_meets: &novel_meets[slot.novel.0..slot.novel.1],
            input: &window.history[slot.input.0..slot.input.1],
            input_meets: &input_meets[slot.input.0..slot.input.1],
            output: &window.output[slot.output.0..slot.output.1],
            output_meets: &output_meets[slot.output.0..slot.output.1],
            due: &due_time[slot.due.0..slot.due.1],
            due_meets: &due_meets[slot.due.0..slot.due.1],
        }
    }
}

/// Prepare one key's record run for its sweep: advance each time by `advance_by` if supplied,
/// sort the run by `(time, id)`, and fill `meets` (of equal length) with the suffix meets —
/// `meets[i]` is the meet of the times of `records[i..]`.
fn prepare<K: Ord, T: Ord + Lattice + Clone, R>(records: &mut [(K, T, R)], meets: &mut [T], advance_by: Option<&T>) {
    if let Some(m) = advance_by {
        for record in records.iter_mut() { record.1.join_assign(m); }
    }
    records.sort_by(|x, y| (&x.1, &x.0).cmp(&(&y.1, &y.0)));
    if let Some(last) = records.len().checked_sub(1) {
        meets[last] = records[last].1.clone();
        for i in (0..last).rev() {
            meets[i] = records[i].1.clone();
            let (init, tail) = meets.split_at_mut(i + 1);
            init[i].meet_assign(&tail[0]);
        }
    }
}

/// Cuts the interval `[lower, upper)` into consecutive batch descriptions along `held`, which
/// must be sorted: the `i`-th cut point is the frontier formed by inserting `held[i+1..]` into
/// `upper`, so description `i` covers the part of the interval not greater-or-equal any held
/// time after `held[i]` (and not covered by an earlier description). Descriptions whose
/// interval is empty are skipped. Returns the descriptions, the held time associated with
/// each, and, per held index, the index of its description (`None` if skipped). A batch built
/// to description `i` can be committed at the capability `held[i]`.
fn tile_descriptions<T: Timestamp + Lattice>(
    lower: &Antichain<T>,
    upper: &Antichain<T>,
    held: &[T],
) -> (Vec<Description<T>>, Vec<T>, Vec<Option<usize>>) {
    let mut tile_descs: Vec<Description<T>> = Vec::new();
    let mut tile_held: Vec<T> = Vec::new();
    let mut tile_of: Vec<Option<usize>> = vec![None; held.len()];
    let mut out_lower = lower.clone();
    for index in 0..held.len() {
        let mut out_upper = upper.clone();
        for t in &held[index + 1..] {
            out_upper.insert(t.clone());
        }
        if out_upper != out_lower {
            tile_of[index] = Some(tile_descs.len());
            tile_descs.push(Description::new(out_lower.clone(), out_upper.clone(), Antichain::from_elem(T::minimum())));
            tile_held.push(held[index].clone());
            out_lower = out_upper;
        }
    }
    (tile_descs, tile_held, tile_of)
}

/// Updates an optional meet by an optional time.
fn update_meet<T: Lattice + Clone>(meet: &mut Option<T>, other: Option<&T>) {
    if let Some(time) = other {
        match meet.as_mut() {
            Some(m) => m.meet_assign(time),
            None => *meet = Some(time.clone()),
        }
    }
}

/// Advance the buffered times by `meet` and consolidate — the collapse that keeps a long replay
/// linear rather than quadratic.
fn advance_buffer_by<V: Copy + Ord, T: Ord + Clone + Lattice, R: Semigroup>(buffer: &mut Vec<((V, T), R)>, meet: &T) {
    for element in buffer.iter_mut() { (element.0).1.join_assign(meet); }
    crate::consolidation::consolidate(buffer);
}

/// Step `run`'s records into `buffer` while the record at `pos` sits at `at`; true iff any did.
fn step_while_time_is<T: Ord + Clone, R: Clone>(
    run: &[((u64, u64), T, R)],
    pos: &mut usize,
    at: &T,
    buffer: &mut Vec<((u64, T), R)>,
) -> bool {
    let mut found = false;
    while let Some(record) = run.get(*pos) {
        if &record.1 != at { break; }
        buffer.push(((record.0.1, record.1.clone()), record.2.clone()));
        *pos += 1;
        found = true;
    }
    found
}

/// A resumable, fused determination-and-evaluation sweep over one key's times.
///
/// A determination pass would enumerate a key's interesting times up front, and the caller would
/// then walk them again to evaluate. The conventional reduce does not: it runs ONE ascending pass
/// and evaluates as it discovers. It can, because discovery never looks backwards — every
/// synthesized time is
/// `next_time.join(t)` for some `t` NOT at or below `next_time`, so it is strictly greater, and new
/// work only ever lands ahead of the sweep.
///
/// This is that pass, cut at the point where the conventional operator would call user logic. Each
/// [`next_crossing`](Self::next_crossing) returns the next in-interval time that needs evaluating,
/// with the buffers positioned to read the accumulations; the caller evaluates and hands the
/// corrections back through [`commit`](Self::commit); the next call resumes. Many keys can be run
/// to their next crossing and evaluated together, which is what a batched backend wants, without
/// any of them enumerating their times first.
///
/// The sweep owns no record data: it walks the presentation a [`KeyView`] reslices for it, and
/// holds only its positions, its accumulations, and the schedule state below.
///
/// The schedule is `formal/Differential/RoundCoverage.lean`'s `round_coverage`: a time carrying an
/// output change lies in the join-closure of `prior ∪ novel` AND is at or above some novel time.
/// The two clauses appear here as the `interesting` test and the split synthesis — see the comments
/// at each.
struct Sweep<T, RIn, ROut> {
    /// Positions into the key view's runs: the un-replayed suffix of each starts here.
    novel_pos: usize,
    input_pos: usize,
    output_pos: usize,
    due_pos: usize,
    /// The stepped-in accumulations: edits whose times the sweep has passed, compacted under the
    /// running meet where they are read. The novel run's times enter `reached` as they step in —
    /// they are the seeds — and its values are held apart from the input rather than merged,
    /// because consolidating the two can cancel a novel update against a compacted history record
    /// and lose its interesting time.
    novel_buf: Vec<((u64, T), RIn)>,
    input_buf: Vec<((u64, T), RIn)>,
    output_buf: Vec<((u64, T), ROut)>,
    /// Synthesized times not yet visited, sorted DESCENDING so `last()` is the least.
    synth: Vec<T>,
    /// The seed times reached so far, compacted by the running meet. They are the witnesses the
    /// absorption test looks for, and the partners a close joins against; keeping them collapsed is
    /// what stops a key with many reached times rescanning all of them.
    reached: Vec<T>,
    /// Corrections emitted so far this sweep, meet-collapsed; both a join partner and part of the
    /// output accumulation.
    produced: Vec<((u64, T), ROut)>,
    /// The meet of every time still to come.
    meet: Option<T>,
    /// Whether the last `next_crossing` returned a time whose step is not yet settled.
    suspended: bool,
}

/// What one [`tick`](Sweep::tick) decided about the time it visited.
enum Tick<T> {
    /// No seed reaches this time; the sweep moved past it.
    Passed,
    /// Reached, but at or beyond `upper`: carried to a later round rather than evaluated.
    Pended,
    /// Reached and in the interval. The caller must evaluate here before the sweep goes on.
    Crossing(T),
    /// Every source is drained.
    Done,
}

impl<T: Timestamp + Lattice, RIn: Semigroup + Clone, ROut: Semigroup + Clone> Sweep<T, RIn, ROut> {
    /// An empty sweep, to be `load`ed. Reuse one per key rather than allocating per key.
    fn new() -> Self {
        Sweep {
            novel_pos: 0, input_pos: 0, output_pos: 0, due_pos: 0,
            novel_buf: Vec::new(), input_buf: Vec::new(), output_buf: Vec::new(),
            synth: Vec::new(), reached: Vec::new(),
            produced: Vec::new(), meet: None, suspended: false,
        }
    }

    /// Position the sweep at the start of one key, whose seed meet (of the novel and due times,
    /// the value the view's join partners were lifted by) is `meet`.
    fn load(&mut self, meet: Option<T>) {
        self.novel_pos = 0;
        self.input_pos = 0;
        self.output_pos = 0;
        self.due_pos = 0;
        self.novel_buf.clear();
        self.input_buf.clear();
        self.output_buf.clear();
        self.synth.clear();
        self.reached.clear();
        self.produced.clear();
        self.meet = meet;
        self.suspended = false;
    }

    /// Advance to the next in-interval time that needs evaluating, or `None` once the key is spent.
    ///
    /// Times at or beyond `upper` that the schedule reaches are appended to `pended` for the caller
    /// to carry into a later round.
    fn next_crossing(
        &mut self,
        view: &KeyView<'_, T, RIn, ROut>,
        upper: &Antichain<T>,
        pended: &mut Vec<T>,
        temporary: &mut Vec<T>,
    ) -> Option<T> {
        loop {
            // A crossing leaves its step half-finished, because `settle` must see the corrections
            // the caller commits. Finishing it is the first thing the next call does.
            if self.suspended {
                self.suspended = false;
                self.settle(view);
            }
            match self.tick(view, upper, pended, temporary) {
                Tick::Done => return None,
                Tick::Crossing(at) => {
                    self.suspended = true;
                    return Some(at);
                }
                Tick::Passed | Tick::Pended => {}
            }
        }
    }

    /// Visit one time: find it, decide whether it is reached, close it forward, and report.
    fn tick(
        &mut self,
        view: &KeyView<'_, T, RIn, ROut>,
        upper: &Antichain<T>,
        pended: &mut Vec<T>,
        temporary: &mut Vec<T>,
    ) -> Tick<T> {
        let Some(at) = self.frontier(view) else { return Tick::Done };
        let reached = self.absorb(view, &at);
        if upper.less_equal(&at) {
            // Out of the interval: nothing can be emitted here, so there is nothing to close
            // against either — a join with `at` is at or beyond `at`, hence also out of interval,
            // and will be rediscovered from `at` in the round that admits it.
            self.settle(view);
            if reached { pended.push(at); return Tick::Pended; }
            return Tick::Passed;
        }
        self.close(&at, reached, upper, pended, temporary);
        if reached { return Tick::Crossing(at); }
        self.settle(view);
        Tick::Passed
    }

    /// The sweep's position: the least time any source still offers.
    ///
    /// The TOTAL order, not the partial one. Every time `close` produces is strictly greater than
    /// the position that produced it, so new work only ever lands ahead of here and the sweep never
    /// revisits.
    fn frontier(&self, view: &KeyView<'_, T, RIn, ROut>) -> Option<T> {
        [
            view.novel.get(self.novel_pos).map(|record| &record.1),
            view.due.get(self.due_pos),
            view.input.get(self.input_pos).map(|record| &record.1),
            view.output.get(self.output_pos).map(|record| &record.1),
            self.synth.last(),
        ].into_iter().flatten().min().cloned()
    }

    /// Step every source sitting at `at`, and decide whether `at` is REACHED.
    ///
    /// Reached is clause two of `round_coverage` — `∃ nu ∈ novel, nu ≤ at` — evaluated
    /// incrementally: either a seed lands exactly here, or one already stepped in lies below.
    ///
    /// Input and output are stepped whether or not `at` is reached, and that is forced rather than
    /// eager: they are sources of the frontier, so leaving them would stall the sweep, and their
    /// edits must reach the buffers or they are lost to every later accumulation. Stepping only
    /// moves an edit across; it consolidates nothing. The expensive part — the buffer advance,
    /// which joins every buffered time and re-consolidates — is deferred to `close`, and happens
    /// only where the buffers are actually read.
    fn absorb(&mut self, view: &KeyView<'_, T, RIn, ROut>, at: &T) -> bool {
        step_while_time_is(view.input, &mut self.input_pos, at, &mut self.input_buf);
        step_while_time_is(view.output, &mut self.output_pos, at, &mut self.output_buf);

        // A novel update here is a seed, as is a synthetic join scheduled for here, or a due time
        // carried in. All enter the ONE reached set, where they are the witnesses the absorption
        // test looks for and the partners a close joins against — the single `novel` set of
        // `round_coverage`, passed so far and meet-compacted.
        let mut reached = step_while_time_is(view.novel, &mut self.novel_pos, at, &mut self.novel_buf);
        if reached {
            self.reached.push(at.clone());
            if let Some(meet) = self.meet.as_ref() { advance_buffer_by(&mut self.novel_buf, meet); }
        }
        while self.synth.last() == Some(at) {
            self.reached.push(self.synth.pop().expect("nonempty"));
            reached = true;
        }
        while view.due.get(self.due_pos) == Some(at) {
            self.reached.push(at.clone());
            self.due_pos += 1;
            reached = true;
        }
        // Absorption: a time at or above a seed already stepped in is itself reached, because
        // joining that seed with it yields it back. Checked against the passed prefix only.
        reached || self.reached.iter().any(|t| t.less_equal(at))
    }

    /// Close `at` forward under joins — clause one of `round_coverage`, the join-closure.
    ///
    /// Against the REACHED (novel) times always, reached or not: an unreached time joined with a
    /// novel time lands at or above that novel time, so it carries a witness and is on the schedule.
    ///
    /// Against the PRIOR times only when `at` is itself reached, because the join then inherits
    /// `at`'s witness. A join of two prior times carries none and is deliberately never produced —
    /// that asymmetry is the whole of why an incremental operator does less work than the closure
    /// of everything.
    ///
    /// `produced` counts as prior. A correction emitted at `p` changes the accumulated output at
    /// every time at or above `p`, so `p ∨ at` has to be visited; nothing else covers it, since
    /// this round's corrections are not in the output history and `at` was not yet stepped in when
    /// the sweep passed `p`.
    fn close(&mut self, at: &T, reached: bool, upper: &Antichain<T>, pended: &mut Vec<T>, temporary: &mut Vec<T>) {
        temporary.extend(self.reached.iter()
            .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
        if reached {
            if let Some(meet) = self.meet.as_ref() {
                advance_buffer_by(&mut self.input_buf, meet);
                advance_buffer_by(&mut self.output_buf, meet);
            }
            temporary.extend(self.input_buf.iter().map(|((_, t), _)| t)
                .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
            temporary.extend(self.output_buf.iter().map(|((_, t), _)| t)
                .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
            temporary.extend(self.produced.iter().map(|((_, t), _)| t)
                .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
        }
        sort_dedup(temporary);
        let before = self.synth.len();
        for time in temporary.drain(..) {
            if upper.less_equal(&time) { pended.push(time); } else { self.synth.push(time); }
        }
        if self.synth.len() > before {
            self.synth.sort_by(|x, y| y.cmp(x));
            self.synth.dedup();
        }
    }

    /// The input accumulation at the suspended time: both input runs, meeting only here.
    fn input_at(&self, at: &T, into: &mut Vec<(u64, RIn)>) {
        for ((id, time), diff) in self.input_buf.iter().chain(self.novel_buf.iter()) {
            if time.less_equal(at) { into.push((*id, diff.clone())); }
        }
        crate::consolidation::consolidate(into);
    }

    /// The tentative output accumulation at the suspended time, including this sweep's corrections.
    fn output_at(&self, at: &T, into: &mut Vec<(u64, ROut)>) {
        for ((id, time), diff) in self.output_buf.iter().chain(self.produced.iter()) {
            if time.less_equal(at) { into.push((*id, diff.clone())); }
        }
        crate::consolidation::consolidate(into);
    }

    /// Record the corrections evaluated at the suspended time, and collapse them by the meet.
    fn commit(&mut self, at: &T, corrections: impl Iterator<Item = (u64, ROut)>) {
        let before = self.produced.len();
        for (id, diff) in corrections { self.produced.push(((id, at.clone()), diff)); }
        if self.produced.len() > before {
            if let Some(meet) = self.meet.as_ref() {
                for entry in self.produced.iter_mut() { (entry.0).1.join_assign(meet); }
            }
            crate::consolidation::consolidate(&mut self.produced);
        }
    }

    /// Close a step: recompute the meet of everything still to come, and compact the reached set by
    /// it. This is what keeps a key with a long history linear rather than quadratic.
    fn settle(&mut self, view: &KeyView<'_, T, RIn, ROut>) {
        let mut meet: Option<T> = None;
        update_meet(&mut meet, view.novel_meets.get(self.novel_pos));
        update_meet(&mut meet, view.input_meets.get(self.input_pos));
        update_meet(&mut meet, view.output_meets.get(self.output_pos));
        for time in self.synth.iter() { update_meet(&mut meet, Some(time)); }
        update_meet(&mut meet, view.due_meets.get(self.due_pos));
        if let Some(m) = meet.as_ref() {
            for time in self.reached.iter_mut() { *time = time.join(m); }
        }
        sort_dedup(&mut self.reached);
        self.meet = meet;
    }
}
