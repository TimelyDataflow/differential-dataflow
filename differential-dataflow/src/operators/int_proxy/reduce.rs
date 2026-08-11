//! The proxy reduce framework.
//!
//! A conventional differential reduce against `(u64, u64)`, where the backend supplies the
//! implementation of the interpretation of the integers.

use std::collections::BTreeMap;

use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::{BatchReader, Description};
use super::ProxyBridge;
use crate::operators::reduce::{sort_dedup, ReduceTactic};
use crate::operators::ValueHistory;

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
/// Owned by the harness and refilled by [`ProxyReduceBackend::next_window`].
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
    fn emit(&mut self, tile: usize, records: &[((u64, u64), B1::Time, Self::ROut)]);

    /// Complete the session matching `begin`. The outputs correspond to the descriptions it was provided.
    fn finish(&mut self) -> Vec<B2>;
}

/// A proxy-space [`ReduceTactic`]: matches input and output records by `key_hash`.
pub struct ProxyReduceTactic<T, Bk> {
    backend: Bk,
    /// Pending interesting times beyond the upper frontier, keyed by key hash.
    pending: BTreeMap<u64, Vec<T>>,
}

impl<T, Bk> ProxyReduceTactic<T, Bk> {
    /// A tactic deferring all value semantics to `backend`.
    pub fn new(backend: Bk) -> Self {
        ProxyReduceTactic { backend, pending: BTreeMap::new() }
    }
}

impl<B1, B2, Bk> ReduceTactic<B1, B2> for ProxyReduceTactic<B1::Time, Bk>
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

        // Split the carried interesting times against `upper`.
        // A time below it is DUE: its key must be re-evaluated this retire, so the key is `changed`.
        // A time at or beyond it is carried forward untouched, seeding `new_pending`.
        let mut due: BTreeMap<u64, Vec<B1::Time>> = BTreeMap::new();
        let mut new_pending: BTreeMap<u64, Vec<B1::Time>> = BTreeMap::new();
        for (key, times) in self.pending.iter() {
            let (carried, ready): (Vec<_>, Vec<_>) = times.iter().cloned().partition(|t| upper.less_equal(t));
            if !ready.is_empty() { due.insert(*key, ready); }
            if !carried.is_empty() { new_pending.insert(*key, carried); }
        }
        // The keys the harness knows must be revisited. The backend adds those its novel batches
        // touch, which it discovers while reading them; neither side scans the whole key space.
        let changed: Vec<u64> = due.keys().copied().collect();

        // Nothing due and nothing novel: no time in the interval can be interesting, so there is no
        // work and no output. Return the frontier bounding the times still withheld — NOT an empty
        // one. This is exactly where a due-only `changed` differs from the whole pending set: times
        // beyond `upper` can remain when nothing is due, and releasing their capabilities would
        // strand them (see the frontier clause of the `ReduceTactic::retire` contract).
        if changed.is_empty() && instance.input_batches.iter().all(|b| b.is_empty()) {
            let mut frontier = Antichain::new();
            for times in self.pending.values() {
                for time in times {
                    frontier.insert_ref(time);
                }
            }
            return (Vec::new(), frontier);
        }

        // The output tiling (identical to the Abelian tactic): one tile per held time, keeping
        // non-degenerate intervals; `tile_of[i]` maps held time `i` to its tile.
        let held_elems: Vec<B1::Time> = held.elements().to_vec();
        let (tile_descs, tile_held, tile_of) = tile_descriptions(lower, upper, &held_elems);
        self.backend.begin(&tile_descs);

        // Progress through the key space: `Some(h)` for key hashes at or above `h` remaining, `None`
        // once the backend reports the space covered.
        let mut from = Some(0u64);
        let mut window: ReduceWindow<B1::Time, Bk::RIn, Bk::ROut> = ReduceWindow::default();

        // Retire-wide reusable scratch: cleared per window or wave, never reallocated. Fresh
        // per-key/per-wave `Vec`s were once the dominant cost here, which is why the slots and the
        // staging buffers are held across the whole retire rather than built where they are used.
        let mut slots: Vec<KeySweep<B1::Time, Bk::RIn, Bk::ROut>> = Vec::new();
        let mut live: Vec<usize> = Vec::new();
        let mut tile_deltas: Vec<Vec<((u64, u64), B1::Time, Bk::ROut)>> = (0..held_elems.len()).map(|_| Vec::new()).collect();
        let mut batch_keys: Vec<u64> = Vec::new();
        let mut in_ends: Vec<usize> = Vec::new();
        let mut in_all: Vec<(u64, Bk::RIn)> = Vec::new();
        let mut out_ends: Vec<usize> = Vec::new();
        let mut out_all: Vec<(u64, Bk::ROut)> = Vec::new();
        let mut active: Vec<(usize, B1::Time)> = Vec::new();
        let mut in_accum: Vec<(u64, Bk::RIn)> = Vec::new();
        let mut cur_out: Vec<(u64, Bk::ROut)> = Vec::new();

        while from.is_some() {
            let before = from;
            window.clear();
            self.backend.next_window(&instance, &changed, &mut from, &mut window);
            let p_in = &window.history;
            let p_nv = &window.novel;
            let p_out = &window.output;
            super::debug_assert_sorted_bridge(p_in, "next_window.history");
            super::debug_assert_sorted_bridge(p_nv, "next_window.novel");
            super::debug_assert_sorted_bridge(p_out, "next_window.output");
            // Without progress the window loop would never retire, so this guards liveness as well
            // as contract; the range check catches a key reported outside the window that owns it,
            // which would silently drop the interaction between its halves.
            debug_assert!(
                from.is_none() || from > before,
                "next_window must either advance `from` or report the key space exhausted",
            );
            debug_assert!(
                {
                    let mut keys = p_in.iter().chain(p_nv.iter()).map(|r| r.0.0).chain(p_out.iter().map(|r| r.0.0));
                    keys.all(|k| before.is_none_or(|b| b <= k) && from.is_none_or(|f| k < f))
                },
                "next_window must report a key hash entirely within the window that first mentions it",
            );

            for deltas in tile_deltas.iter_mut() { deltas.clear(); }

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
            live.clear();
            // Mapped to hashes before the min: the three runs differ in their diff type.
            while let Some(key) = [
                p_in.get(is).map(|record| record.0.0),
                p_nv.get(ns).map(|record| record.0.0),
                p_out.get(os).map(|record| record.0.0),
            ].into_iter().flatten().min() {
                let i0 = is;
                while is < p_in.len() && p_in[is].0.0 == key { is += 1; }
                let i1 = is;
                let n0 = ns;
                while ns < p_nv.len() && p_nv[ns].0.0 == key { ns += 1; }
                let n1 = ns;
                let o0 = os;
                while os < p_out.len() && p_out[os].0.0 == key { os += 1; }
                let o1 = os;

                if n_slots == slots.len() { slots.push(KeySweep::empty()); }
                let slot = &mut slots[n_slots];
                slot.key = key;
                slot.pended.clear();
                // Only the DUE times seed the sweep; the carried ones are already in `new_pending`.
                let owed = due.get(&key).map(|p| &p[..]).unwrap_or(&[]);
                slot.sweep.load(
                    (n0..n1).map(|n| (p_nv[n].0.1, p_nv[n].1.clone(), p_nv[n].2.clone())),
                    (i0..i1).map(|i| (p_in[i].0.1, p_in[i].1.clone(), p_in[i].2.clone())),
                    (o0..o1).map(|o| (p_out[o].0.1, p_out[o].1.clone(), p_out[o].2.clone())),
                    owed,
                );
                slot.at = slot.sweep.next_crossing(upper, &mut slot.pended);
                if slot.at.is_some() { live.push(n_slots); }
                else if !slot.pended.is_empty() {
                    new_pending.entry(key).or_default().append(&mut slot.pended);
                }
                n_slots += 1;
            }

            // Each wave: read every suspended key's accumulations, cross the non-empty ones in one
            // call, hand the corrections back, and step every live key on. A key retires when its
            // sweep runs dry, at which point its pended times are carried forward.
            while !live.is_empty() {
                batch_keys.clear();
                in_ends.clear();
                in_all.clear();
                out_ends.clear();
                out_all.clear();
                active.clear();

                for &si in live.iter() {
                    let at = slots[si].at.clone().expect("live slots are suspended at a time");
                    in_accum.clear();
                    cur_out.clear();
                    slots[si].sweep.input_at(&at, &mut in_accum);
                    slots[si].sweep.output_at(&at, &mut cur_out);
                    // An interesting time can still reach the gate with nothing to read; the
                    // conventional reduce skips user logic there and so do we.
                    if in_accum.is_empty() && cur_out.is_empty() { continue; }
                    batch_keys.push(slots[si].key);
                    in_all.append(&mut in_accum);
                    in_ends.push(in_all.len());
                    out_all.append(&mut cur_out);
                    out_ends.push(out_all.len());
                    active.push((si, at));
                }

                if !batch_keys.is_empty() {
                    let (corr, corr_ends) = self.backend.reduce_corrections(&batch_keys, &in_ends, &in_all, &out_ends, &out_all);
                    let mut cstart = 0usize;
                    for (bi, (si, at)) in active.iter().enumerate() {
                        let cend = corr_ends[bi];
                        if cstart != cend {
                            let idx = held_elems.iter().rposition(|h| h.less_equal(at)).expect("no held capability <= active time");
                            for (vid, d) in &corr[cstart..cend] {
                                tile_deltas[idx].push(((slots[*si].key, *vid), at.clone(), d.clone()));
                            }
                            slots[*si].sweep.commit(at, corr[cstart..cend].iter().cloned());
                        }
                        cstart = cend;
                    }
                }

                // Step every live key past the time it was suspended at, and retire the spent ones.
                for &si in live.iter() {
                    let slot = &mut slots[si];
                    slot.at = slot.sweep.next_crossing(upper, &mut slot.pended);
                    if slot.at.is_none() && !slot.pended.is_empty() {
                        let entry = new_pending.entry(slot.key).or_default();
                        entry.append(&mut slot.pended);
                        crate::operators::reduce::sort_dedup(entry);
                    }
                }
                live.retain(|&si| slots[si].at.is_some());
            }

            for (held_index, deltas) in tile_deltas.iter_mut().enumerate() {
                if deltas.is_empty() {
                    continue;
                }
                if let Some(tile) = tile_of[held_index] {
                    crate::consolidation::consolidate_updates(deltas);
                    self.backend.emit(tile, &deltas[..]);
                }
            }
        }

        self.pending = new_pending;
        let produced: Vec<(B1::Time, B2)> = tile_held.into_iter().zip(self.backend.finish()).collect();
        let mut frontier = Antichain::new();
        for times in self.pending.values() {
            for t in times {
                frontier.insert_ref(t);
            }
        }
        (produced, frontier)
    }
}

/// One key's slot in a window: its [`Sweep`], the time it is suspended at, and the times it has
/// pended so far. Slots are reused across windows, so a key costs no allocation of its own beyond
/// the first window wide enough to need it.
struct KeySweep<T, RIn, ROut> {
    key: u64,
    sweep: Sweep<T, RIn, ROut>,
    /// Times at or beyond `upper` the sweep has reached; carried forward when the slot retires.
    pended: Vec<T>,
    /// The time the sweep last suspended at, or `None` once it is spent.
    at: Option<T>,
}

impl<T: Timestamp + Lattice, RIn: Semigroup + Clone, ROut: Semigroup + Clone> KeySweep<T, RIn, ROut> {
    fn empty() -> Self {
        KeySweep { key: 0, sweep: Sweep::new(), pended: Vec::new(), at: None }
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
/// The schedule is `formal/Differential/RoundCoverage.lean`'s `round_coverage`: a time carrying an
/// output change lies in the join-closure of `prior ∪ novel` AND is at or above some novel time.
/// The two clauses appear here as the `interesting` test and the split synthesis — see the comments
/// at each.
struct Sweep<T, RIn, ROut> {
    /// The novel run: the only source that SEEDS interest, replayed unadvanced.
    novel: ValueHistory<u64, T, RIn>,
    /// The accumulated input and output: join partners, and the accumulations to evaluate over.
    input: ValueHistory<u64, T, RIn>,
    output: ValueHistory<u64, T, ROut>,
    /// The key's due interesting times, ascending, with their suffix meets; `due_pos` consumes them.
    due: Vec<T>,
    due_meets: Vec<T>,
    due_pos: usize,
    /// Synthesized times not yet visited, sorted DESCENDING so `last()` is the least.
    synth: Vec<T>,
    /// The seed times reached so far, compacted by the running meet. They are the witnesses the
    /// absorption test looks for, and the partners a close joins against; keeping them collapsed is
    /// what stops a key with many reached times rescanning all of them.
    reached: Vec<T>,
    /// Scratch for one step's synthesized times.
    temporary: Vec<T>,
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
            novel: ValueHistory::new(), input: ValueHistory::new(), output: ValueHistory::new(),
            due: Vec::new(), due_meets: Vec::new(), due_pos: 0,
            synth: Vec::new(), reached: Vec::new(), temporary: Vec::new(),
            produced: Vec::new(), meet: None, suspended: false,
        }
    }

    /// Position the sweep at the start of one key.
    ///
    /// `novel` must be the batch's own `(id, time, diff)` support for the key and `input` the
    /// accumulated input WITHOUT it: the two are never merged, because consolidating them can cancel
    /// a novel update against a compacted history record and lose its interesting time.
    fn load(
        &mut self,
        novel: impl Iterator<Item = (u64, T, RIn)>,
        input: impl Iterator<Item = (u64, T, RIn)>,
        output: impl Iterator<Item = (u64, T, ROut)>,
        due: &[T],
    ) {
        self.due.clear();
        self.due.extend(due.iter().cloned());
        self.due_meets.clear();
        self.due_meets.extend(due.iter().cloned());
        for i in (1..self.due_meets.len()).rev() {
            let (init, tail) = self.due_meets.split_at_mut(i);
            init[i - 1].meet_assign(&tail[0]);
        }
        self.due_pos = 0;
        self.synth.clear();
        self.reached.clear();
        self.temporary.clear();
        self.produced.clear();
        self.suspended = false;

        // The novel run is loaded UNADVANCED: it is the seed, and its own times are what the
        // schedule is stated over. Only then is the meet known, and the join partners advanced by it.
        self.novel.load_iter(novel, None);
        let mut meet: Option<T> = None;
        update_meet(&mut meet, self.due_meets.first());
        update_meet(&mut meet, self.novel.meet());
        self.input.load_iter(input, meet.as_ref());
        self.output.load_iter(output, meet.as_ref());
        self.meet = meet;
    }

    /// Advance to the next in-interval time that needs evaluating, or `None` once the key is spent.
    ///
    /// Times at or beyond `upper` that the schedule reaches are appended to `pended` for the caller
    /// to carry into a later round.
    fn next_crossing(&mut self, upper: &Antichain<T>, pended: &mut Vec<T>) -> Option<T> {
        loop {
            // A crossing leaves its step half-finished, because `settle` must see the corrections
            // the caller commits. Finishing it is the first thing the next call does.
            if self.suspended {
                self.suspended = false;
                self.settle();
            }
            match self.tick(upper, pended) {
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
    fn tick(&mut self, upper: &Antichain<T>, pended: &mut Vec<T>) -> Tick<T> {
        let Some(at) = self.frontier() else { return Tick::Done };
        let reached = self.absorb(&at);
        if upper.less_equal(&at) {
            // Out of the interval: nothing can be emitted here, so there is nothing to close
            // against either — a join with `at` is at or beyond `at`, hence also out of interval,
            // and will be rediscovered from `at` in the round that admits it.
            self.settle();
            if reached { pended.push(at); return Tick::Pended; }
            return Tick::Passed;
        }
        self.close(&at, reached, upper, pended);
        if reached { return Tick::Crossing(at); }
        self.settle();
        Tick::Passed
    }

    /// The sweep's position: the least time any source still offers.
    ///
    /// The TOTAL order, not the partial one. Every time `close` produces is strictly greater than
    /// the position that produced it, so new work only ever lands ahead of here and the sweep never
    /// revisits.
    fn frontier(&self) -> Option<T> {
        [
            self.novel.time(), self.due.get(self.due_pos), self.input.time(),
            self.output.time(), self.synth.last(),
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
    /// moves an edit across; it consolidates nothing. The expensive part — `advance_buffer_by`,
    /// which joins every buffered time and re-consolidates — is deferred to `close`, and happens
    /// only where the buffers are actually read.
    fn absorb(&mut self, at: &T) -> bool {
        self.input.step_while_time_is(at);
        self.output.step_while_time_is(at);

        // A novel update here is a seed.
        let mut reached = self.novel.step_while_time_is(at);
        if reached {
            if let Some(meet) = self.meet.as_ref() { self.novel.advance_buffer_by(meet); }
        }
        // So is a synthetic join scheduled for here, or a due time carried in. Both move into the
        // reached set, where they become witnesses and join partners for later times.
        while self.synth.last() == Some(at) {
            self.reached.push(self.synth.pop().expect("nonempty"));
            reached = true;
        }
        while self.due.get(self.due_pos) == Some(at) {
            self.reached.push(at.clone());
            self.due_pos += 1;
            reached = true;
        }
        // Absorption: a time at or above a seed already stepped in is itself reached, because
        // joining that seed with it yields it back. Checked against the stepped prefixes only.
        reached
            || self.novel.buffer().iter().any(|((_, t), _)| t.less_equal(at))
            || self.reached.iter().any(|t| t.less_equal(at))
    }

    /// Close `at` forward under joins — clause one of `round_coverage`, the join-closure.
    ///
    /// Against the NOVEL times always, reached or not: an unreached time joined with a novel time
    /// lands at or above that novel time, so it carries a witness and is on the schedule.
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
    fn close(&mut self, at: &T, reached: bool, upper: &Antichain<T>, pended: &mut Vec<T>) {
        self.temporary.extend(self.novel.buffer().iter().map(|((_, t), _)| t)
            .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
        self.temporary.extend(self.reached.iter()
            .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
        if reached {
            if let Some(meet) = self.meet.as_ref() {
                self.input.advance_buffer_by(meet);
                self.output.advance_buffer_by(meet);
            }
            self.temporary.extend(self.input.buffer().iter().map(|((_, t), _)| t)
                .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
            self.temporary.extend(self.output.buffer().iter().map(|((_, t), _)| t)
                .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
            self.temporary.extend(self.produced.iter().map(|((_, t), _)| t)
                .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
        }
        sort_dedup(&mut self.temporary);
        let before = self.synth.len();
        for time in self.temporary.drain(..) {
            if upper.less_equal(&time) { pended.push(time); } else { self.synth.push(time); }
        }
        if self.synth.len() > before {
            self.synth.sort_by(|x, y| y.cmp(x));
            self.synth.dedup();
        }
    }

    /// The input accumulation at the suspended time: both input runs, meeting only here.
    fn input_at(&self, at: &T, into: &mut Vec<(u64, RIn)>) {
        for ((id, time), diff) in self.input.buffer().iter().chain(self.novel.buffer().iter()) {
            if time.less_equal(at) { into.push((*id, diff.clone())); }
        }
        crate::consolidation::consolidate(into);
    }

    /// The tentative output accumulation at the suspended time, including this sweep's corrections.
    fn output_at(&self, at: &T, into: &mut Vec<(u64, ROut)>) {
        for ((id, time), diff) in self.output.buffer().iter().chain(self.produced.iter()) {
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
    fn settle(&mut self) {
        let mut meet: Option<T> = None;
        update_meet(&mut meet, self.novel.meet());
        update_meet(&mut meet, self.input.meet());
        update_meet(&mut meet, self.output.meet());
        for time in self.synth.iter() { update_meet(&mut meet, Some(time)); }
        update_meet(&mut meet, self.due_meets.get(self.due_pos));
        if let Some(m) = meet.as_ref() {
            for time in self.reached.iter_mut() { *time = time.join(m); }
        }
        sort_dedup(&mut self.reached);
        self.meet = meet;
    }
}
