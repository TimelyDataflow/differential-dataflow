//! The proxy reduce framework.
//!
//! A conventional differential reduce against `(u64, u64)`, where the backend supplies the
//! implementation of the interpretation of the integers. Times are rows of the backend's
//! [`TimeColumn`]: the tactic reads, compares, joins and meets them by index and never holds
//! one per record.

use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::{Span, Description};
use super::times::{Bridge, Carried, Seeds, TimeColumn};
use super::history::{consolidate_buffer, ColumnHistory};
use crate::operators::reduce::ReduceTactic;

/// A unit of proxied reduce work, presented to the backend.
pub struct ReduceInstance<'a, T, B1, B2> {
    /// The accumulated input history.
    pub source_batches: &'a [B1],
    /// The freshly arrived input delta.
    pub input_batches: &'a [B1],
    /// The accumulated output history.
    pub output_batches: &'a [B2],
    /// The compaction frontier for loading (the retire's lower bound).
    pub lower: AntichainRef<'a, T>,
}

/// One window of the key space: the presentations a bounded, hash-contiguous snip needs.
///
/// Seeds travel as times; records travel netted. The novel data's two roles are carried by two
/// different channels: its TIME SUPPORT seeds interesting times and rides `seeds`, raw; its
/// RECORDS are mere accumulants and join partners, so they ride `input` merged with the prior
/// history, where they may net against it and be advanced like anything else. Consolidation can
/// only cancel equal-`((key, id), time)` pairs, and such a time is necessarily in `seeds`, so no
/// interesting time is lost to netting — the invariant that once forced the runs apart.
///
/// Owned by the harness and refilled by [`ProxyReduceBackend::next_window`].
pub struct ReduceWindow<C, RIn, ROut> {
    /// The key's full input — novel and prior merged, netted — sorted & consolidated by
    /// `((key_hash, value_id), time)`. May be advanced to the compaction frontier.
    pub input: Bridge<C, RIn>,
    /// The RAW novel time support: `(key_hash, time)` pairs sorted by `(key_hash, time)` and
    /// deduplicated, recorded from the novel batches BEFORE any consolidation or advancement —
    /// a netted-away record's time must still appear here.
    pub seeds: Seeds<C>,
    /// Accumulated output preceding the retire's interval, same ordering as `input`.
    pub output: Bridge<C, ROut>,
}

impl<C: Default, RIn, ROut> Default for ReduceWindow<C, RIn, ROut> {
    fn default() -> Self { ReduceWindow { input: Bridge::default(), seeds: Seeds::default(), output: Bridge::default() } }
}

impl<C: TimeColumn, RIn, ROut> ReduceWindow<C, RIn, ROut> {
    /// Clear the presentations, keeping their allocations.
    pub fn clear(&mut self) {
        self.input.clear();
        self.seeds.clear();
        self.output.clear();
    }
}

/// The reduce backend: value semantics for a proxy-space reduction, driven by [`ProxyReduceTactic`].
///
/// The protocol for each round of invocation is
/// `begin [ next_window reduce_corrections* emit ]* finish`,
/// where the window loop runs until `next_window` reports the key space exhausted.
pub trait ProxyReduceBackend<T, B1, B2> {
    /// Diff type presented for the input.
    type RIn: Semigroup;
    /// Diff type of the output.
    type ROut: Semigroup + 'static;
    /// The time column the bridges carry.
    type Times: TimeColumn<Time = T>;

    /// Initiate a session to create batches for these descriptions, which span `[lower, upper)`.
    ///
    /// It is the backend's job to prepare output batches for each of these descriptions.
    /// The computation proceeds in windows of keys, where only the backend maintains this
    /// work in progress, until `finish()` is called.
    fn begin(&mut self, description: Description<T>);

    /// Present the next window of the key space, and advance `from` past it.
    ///
    /// On entry `from` is the inclusive lower bound on key hashes still to be covered. The backend
    /// chooses the window's exclusive upper bound and writes it back, or writes `None` to report the
    /// key space exhausted. An implementor must advance `from`, as it is guaranteed to be non-`None`.
    ///
    /// The window must present, for every key hash in `[from_before, from_after)` that either
    /// carries an update in the instance's novel batches or appears in `changed`: that key's merged
    /// input (novel and prior together, netted), its raw novel time support in `seeds`, and its
    /// accumulated output. A key must be reported entirely within the window that first mentions
    /// it: splitting one across windows drops the interaction between the halves. `changed` is
    /// ascending; the harness reads no key outside the window's range, so a backend that keeps its
    /// own key order need not consult the whole space.
    ///
    /// `seeds` must be recorded from the novel batches before any consolidation or advancement:
    /// a novel record that nets to zero against compacted history vanishes from `input`, but its
    /// time must still seed — that cancellation is exactly the case that loses updates otherwise.
    ///
    /// The size of the window is up to the backend: large enough to amortize the crossings, small
    /// enough that the presentations are affordable, as all are live at once.
    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, T, B1, B2>,
        changed: &[u64],
        from: &mut Option<u64>,
        window: &mut ReduceWindow<Self::Times, Self::RIn, Self::ROut>,
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

    /// Commit a consolidated bridge of updates to the batch in progress.
    fn emit(&mut self, records: &Bridge<Self::Times, Self::ROut>);

    /// Complete the session matching `begin`, yielding the batch it described,
    /// or `None` when the span it described carries no updates.
    fn finish(&mut self) -> Option<B2>;
}

/// A proxy-space [`ReduceTactic`]: matches input and output records by `key_hash`.
pub struct ProxyReduceTactic<T, B1, B2, Bk: ProxyReduceBackend<T, B1, B2>> {
    backend: Bk,
    /// Maximum number of key hashes with live sweep state at once.
    key_batch_size: usize,
    /// Interesting times beyond the upper frontier, held as one column with `(key, row)`
    /// entries rather than a timestamp apiece.
    pending: Carried<Bk::Times>,
    /// The per-retire working state, kept so that its columns and vectors keep their capacity
    /// from one retire to the next.
    scratch: ReduceScratch<Bk::Times, Bk::RIn, Bk::ROut>,
    _marker: std::marker::PhantomData<(B1, B2)>,
}

/// A retire's working state: the window, the sweep slots, and the staging buffers. Cleared per
/// group, window or wave, retaining capacity. Fresh per-key/per-wave `Vec`s were once the
/// dominant cost here, and a retire that starts from empty columns grows them by doubling.
struct ReduceScratch<C, RIn, ROut> {
    window: ReduceWindow<C, RIn, ROut>,
    slots: Vec<KeySweep<C, RIn, ROut>>,
    live: Vec<usize>,
    deltas: Bridge<C, ROut>,
    batch_keys: Vec<u64>,
    in_ends: Vec<usize>,
    in_all: Vec<(u64, RIn)>,
    out_ends: Vec<usize>,
    out_all: Vec<(u64, ROut)>,
    active: Vec<(usize, usize)>,
    in_accum: Vec<(u64, RIn)>,
    cur_out: Vec<(u64, ROut)>,
    /// The retire's partition of the held times: those DUE now (`due`, a key's times a run of
    /// rows delimited by `due_ends`), those still carried, the frontier of the carried ones, and
    /// `upper`'s own elements — all as rows of columns.
    due: Carried<C>,
    due_keys: Vec<u64>,
    due_ends: Vec<usize>,
    carried: Carried<C>,
    front: C,
    upper_rows: C,
}

impl<C: Default, RIn, ROut> Default for ReduceScratch<C, RIn, ROut> {
    fn default() -> Self {
        ReduceScratch {
            window: ReduceWindow::default(), slots: Vec::new(), live: Vec::new(), deltas: Bridge::default(),
            batch_keys: Vec::new(), in_ends: Vec::new(), in_all: Vec::new(), out_ends: Vec::new(), out_all: Vec::new(),
            active: Vec::new(), in_accum: Vec::new(), cur_out: Vec::new(),
            due: Carried::default(), due_keys: Vec::new(), due_ends: Vec::new(),
            carried: Carried::default(), front: C::default(), upper_rows: C::default(),
        }
    }
}

impl<T, B1, B2, Bk: ProxyReduceBackend<T, B1, B2>> ProxyReduceTactic<T, B1, B2, Bk> {
    /// A tactic deferring all value semantics to `backend`.
    pub fn new(backend: Bk) -> Self {
        ProxyReduceTactic { backend, key_batch_size: usize::MAX, pending: Carried::default(), scratch: ReduceScratch::default(), _marker: std::marker::PhantomData }
    }

    /// Limit simultaneous sweeps independently of the backend's presentation window.
    ///
    /// Complete key hashes stay together, including all real keys sharing a hash.
    /// Sweep scratch is reused between groups within a retire. The bound does not
    /// limit a single key's size, the presentation, or the output batch. Corrections
    /// remain batched, and emission still happens once per backend window.
    /// By default, all keys in the presentation can have live sweeps at once.
    pub fn with_key_batch_size(mut self, key_batch_size: usize) -> Self {
        assert!(key_batch_size > 0, "key batch size must be positive");
        self.key_batch_size = key_batch_size;
        self
    }
}

/// A column read as an antichain, as the timestamps the harness speaks.
fn antichain_of<C: TimeColumn>(front: &C) -> Antichain<C::Time>
where
    C::Time: PartialOrder + Clone,
{
    let mut out = Antichain::new();
    for r in 0..front.len() { out.insert(front.get(r)); }
    out
}

fn debug_assert_pending_frontier<C: TimeColumn>(pending: &Carried<C>, maintained: &Antichain<C::Time>)
where
    C::Time: PartialOrder + Clone,
{
    debug_assert!({
        let mut expected = Antichain::new();
        for &(_, row) in pending.entries.iter() { expected.insert(pending.times.get(row)); }
        expected.elements().iter().all(|t| maintained.less_equal(t))
            && maintained.elements().iter().all(|t| expected.less_equal(t))
    }, "maintained pending frontier differs from the times held");
}

impl<T, B1, B2, Bk> ReduceTactic<T, B1, B2> for ProxyReduceTactic<T, B1, B2, Bk>
where
    T: Timestamp + Lattice,
    Bk: ProxyReduceBackend<T, B1, B2>,
{
    fn retire(
        &mut self,
        source_batches: Vec<B1>,
        output_batches: Vec<B2>,
        input_batches: Vec<B1>,
        lower: &Antichain<T>,
        upper: &Antichain<T>,
        held: &Antichain<T>,
    ) -> (Option<Span<T, B2>>, Antichain<T>) {
        if held.elements().iter().all(|t| upper.less_equal(t)) {
            debug_assert!(
                self.pending.entries.iter().all(|&(_, row)| held.less_equal(&self.pending.times.get(row))),
                "held capabilities do not cover pending times",
            );
            return (None, held.clone());
        }

        let instance = ReduceInstance {
            source_batches: &source_batches,
            input_batches: &input_batches,
            output_batches: &output_batches,
            lower: lower.borrow(),
        };

        // Split the held interesting times against `upper`, key by key in one pass over the
        // column: a time below it is DUE — its key must be re-evaluated this retire, so the key
        // is `changed` — and a time at or beyond it stays held. Both sides are built as rows of
        // fresh columns, which compacts the held column to exactly what survives; the frontier
        // of the survivors is built as an antichain column, so a timestamp is materialized once
        // per frontier element rather than once per held time.
        let scratch = &mut self.scratch;
        scratch.due.clear();
        scratch.due_keys.clear();
        scratch.due_ends.clear();
        scratch.carried.clear();
        scratch.front.clear();
        scratch.upper_rows.clear();
        for time in upper.elements() { scratch.upper_rows.push(time); }
        self.pending.order();
        for &(key, row) in self.pending.entries.iter() {
            // `upper.less_equal(time)`: some element of the frontier at or below the time.
            let beyond = (0..scratch.upper_rows.len()).any(|u| scratch.upper_rows.less_equal_cross(u, &self.pending.times, row));
            if beyond {
                scratch.carried.push_from(key, &self.pending.times, row);
                scratch.front.insert_antichain(&self.pending.times, row);
            } else {
                if scratch.due_keys.last() != Some(&key) {
                    if !scratch.due_keys.is_empty() { scratch.due_ends.push(scratch.due.len()); }
                    scratch.due_keys.push(key);
                }
                scratch.due.push_from(key, &self.pending.times, row);
            }
        }
        if !scratch.due_keys.is_empty() { scratch.due_ends.push(scratch.due.len()); }
        std::mem::swap(&mut self.pending, &mut scratch.carried);
        // The keys the harness knows must be revisited. The backend adds those its novel batches
        // touch, which it discovers while reading them; neither side scans the whole key space.
        let changed: Vec<u64> = scratch.due_keys.clone();

        // Nothing due and nothing novel: no time in the interval can be interesting, so there is no
        // work and no output. Return the frontier bounding the times still withheld — NOT an empty
        // one. This is exactly where a due-only `changed` differs from the whole pending set: times
        // beyond `upper` can remain when nothing is due, and releasing their capabilities would
        // strand them (see the frontier clause of the `ReduceTactic::retire` contract).
        if changed.is_empty() && instance.input_batches.is_empty() {
            let frontier = antichain_of(&self.scratch.front);
            debug_assert_pending_frontier(&self.pending, &frontier);
            return (None, frontier);
        }

        // The single output batch spans the retired interval.
        let description = Description::new(lower.clone(), upper.clone(), Antichain::from_elem(T::minimum()));
        self.backend.begin(description.clone());

        // Progress through the key space: `Some(h)` for key hashes at or above `h` remaining, `None`
        // once the backend reports the space covered.
        let mut from = Some(0u64);

        // The working state, kept across retires for its capacity.
        let ReduceScratch { window, slots, live, deltas, batch_keys, in_ends, in_all, out_ends, out_all, active, in_accum, cur_out, due, due_keys, due_ends, front, .. } = &mut self.scratch;
        let (window, slots, live, deltas) = (window, slots, live, deltas);
        let (batch_keys, in_ends, in_all, out_ends, out_all, active, in_accum, cur_out) = (batch_keys, in_ends, in_all, out_ends, out_all, active, in_accum, cur_out);
        let (due, due_keys, due_ends, front) = (&*due, &*due_keys, &*due_ends, front);
        let pending = &mut self.pending;
        let backend = &mut self.backend;
        let key_batch_size = self.key_batch_size;

        while from.is_some() {
            let before = from;
            window.clear();
            backend.next_window(&instance, &changed, &mut from, window);
            let p_in = &window.input;
            let seeds = &window.seeds;
            let p_out = &window.output;
            p_in.debug_assert_sorted("next_window.input");
            p_out.debug_assert_sorted("next_window.output");
            seeds.debug_assert_sorted("next_window.seeds");
            // Without progress the window loop would never retire, so this guards liveness as well
            // as contract; the range check catches a key reported outside the window that owns it,
            // which would silently drop the interaction between its halves.
            debug_assert!(
                from.is_none() || from > before,
                "next_window must either advance `from` or report the key space exhausted",
            );
            debug_assert!(
                {
                    let mut keys = p_in.ids.iter().map(|r| r.0).chain(seeds.keys.iter().copied()).chain(p_out.ids.iter().map(|r| r.0));
                    keys.all(|k| before.is_none_or(|b| b <= k) && from.is_none_or(|f| k < f))
                },
                "next_window must report a key hash entirely within the window that first mentions it",
            );

            deltas.clear();

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
            // across bounded groups as well as windows. A backend can present a large
            // window without allocating sweep state for every key simultaneously.
            let (mut is, mut ns, mut os) = (0usize, 0usize, 0usize);
            while is < p_in.len() || ns < seeds.len() || os < p_out.len() {
                let mut n_slots = 0usize;
                live.clear();
                // Mapped to hashes before the min: the sources differ in shape.
                while let Some(key) = [
                    p_in.ids.get(is).map(|record| record.0),
                    seeds.keys.get(ns).copied(),
                    p_out.ids.get(os).map(|record| record.0),
                ].into_iter().flatten().min() {
                    let i0 = is;
                    while is < p_in.len() && p_in.ids[is].0 == key { is += 1; }
                    let i1 = is;
                    let n0 = ns;
                    while ns < seeds.len() && seeds.keys[ns] == key { ns += 1; }
                    let n1 = ns;
                    let o0 = os;
                    while os < p_out.len() && p_out.ids[os].0 == key { os += 1; }
                    let o1 = os;

                    if n_slots == slots.len() { slots.push(KeySweep::empty()); }
                    let slot = &mut slots[n_slots];
                    slot.key = key;
                    slot.pended.clear();
                    // Only the DUE times seed the sweep; the rest stay held for a later retire.
                    let owed = due_keys.binary_search(&key).map_or(0..0, |k| {
                        let start = if k == 0 { 0 } else { due_ends[k - 1] };
                        start..due_ends[k]
                    });
                    slot.sweep.load(upper, &due.times, owed, seeds, n0..n1, p_in, i0..i1, p_out, o0..o1);
                    slot.at = slot.sweep.next_crossing(&mut slot.pended);
                    if slot.at.is_some() { live.push(n_slots); }
                    else if !slot.pended.is_empty() {
                        slot.retire_pended(pending, front);
                    }
                    n_slots += 1;
                    if n_slots == key_batch_size { break; }
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
                        let at = slots[si].at.expect("live slots are suspended at a time");
                        in_accum.clear();
                        cur_out.clear();
                        slots[si].sweep.input_at(at, in_accum);
                        slots[si].sweep.output_at(at, cur_out);
                        // An interesting time can still reach the gate with nothing to read; the
                        // conventional reduce skips user logic there and so do we.
                        if in_accum.is_empty() && cur_out.is_empty() { continue; }
                        batch_keys.push(slots[si].key);
                        in_all.append(in_accum);
                        in_ends.push(in_all.len());
                        out_all.append(cur_out);
                        out_ends.push(out_all.len());
                        active.push((si, at));
                    }

                    if !batch_keys.is_empty() {
                        let (corr, corr_ends) = backend.reduce_corrections(batch_keys, in_ends, in_all, out_ends, out_all);
                        let mut cstart = 0usize;
                        for (bi, &(si, at)) in active.iter().enumerate() {
                            let cend = corr_ends[bi];
                            if cstart != cend {
                                debug_assert!(
                                    held.elements().iter().any(|h| h.less_equal(&slots[si].sweep.pool.get(at))),
                                    "no held capability <= active time",
                                );
                                for (vid, d) in &corr[cstart..cend] {
                                    deltas.push_from((slots[si].key, *vid), &slots[si].sweep.pool, at, d.clone());
                                }
                                slots[si].sweep.commit(at, corr[cstart..cend].iter().cloned());
                            }
                            cstart = cend;
                        }
                    }

                    // Step every live key past the time it was suspended at, and retire the spent ones.
                    for &si in live.iter() {
                        let slot = &mut slots[si];
                        slot.at = slot.sweep.next_crossing(&mut slot.pended);
                        if slot.at.is_none() && !slot.pended.is_empty() {
                            slot.retire_pended(pending, front);
                        }
                    }
                    live.retain(|&si| slots[si].at.is_some());
                }
            }

            if !deltas.is_empty() {
                deltas.consolidate();
                backend.emit(deltas);
            }
        }

        let produced = Some(Span::new(description, backend.finish()));
        // The frontier of everything still held, read off the antichain column once: the times
        // the harness must keep capabilities for.
        let frontier = antichain_of(front);
        debug_assert_pending_frontier(pending, &frontier);
        (produced, frontier)
    }
}

/// One key's slot in a window: its [`Sweep`], the time it is suspended at, and the times it has
/// pended so far. Slots and their scratch capacity are reused across groups and windows
/// within a retire, then dropped when the retire completes.
struct KeySweep<C, RIn, ROut> {
    key: u64,
    sweep: Sweep<C, RIn, ROut>,
    /// Times at or beyond `upper` the sweep has reached (rows of its pool); carried forward when
    /// the slot retires.
    pended: Vec<usize>,
    /// The time the sweep last suspended at (a row of its pool), or `None` once it is spent.
    at: Option<usize>,
}

impl<C: TimeColumn, RIn: Semigroup + Clone, ROut: Semigroup + Clone> KeySweep<C, RIn, ROut>
where
    C::Time: Timestamp + Lattice,
{
    fn empty() -> Self {
        KeySweep { key: 0, sweep: Sweep::new(), pended: Vec::new(), at: None }
    }

    /// Hold the pended times for a later retire: their rows are copied out of the sweep's pool
    /// into the held column, and into the frontier column read as an antichain. No timestamp is
    /// built here; the retire reads the frontier off that column once, at its return.
    fn retire_pended(&mut self, pending: &mut Carried<C>, front: &mut C) {
        for &row in &self.pended {
            pending.push_from(self.key, &self.sweep.pool, row);
            front.insert_antichain(&self.sweep.pool, row);
        }
        self.pended.clear();
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
/// The novel times arrive as the SEED LIST (the harness's warned times merged with the window's raw
/// novel time support); the records themselves travel merged with the prior history and carry no
/// witness duty, which is what lets them net and advance. Coverage is invariant under that move:
/// the witness clause reads only times, and consolidation cancels only equal-time pairs whose time
/// the seed list retains.
///
/// Every time the sweep holds is a row of `pool`, its own time column: the frontier's elements,
/// the seeds and their suffix meets, the histories' times, the synthesized and reached times, the
/// produced corrections' times, the running meet. Rows are appended as joins produce them and
/// the pool is compacted when the dead rows outnumber the live ones.
struct Sweep<C, RIn, ROut> {
    /// The time column every row below refers to.
    pool: C,
    /// The retire's upper frontier, as rows.
    upper: Vec<usize>,
    /// The accumulated input (novel and prior, merged and netted) and output: join partners, and
    /// the accumulations to evaluate over. Both may be advanced freely — witness duty lives in
    /// `seeds`, not in any record.
    input: ColumnHistory<u64, RIn>,
    output: ColumnHistory<u64, ROut>,
    /// The key's seed times — the harness's due (warned) times merged with the raw novel time
    /// support — ascending and deduplicated, with their suffix meets; `seed_pos` consumes them.
    /// These are the ONLY source of interest: the schedule is stated over them, so they are held
    /// raw, never advanced.
    seeds: Vec<usize>,
    seed_meets: Vec<usize>,
    seed_pos: usize,
    /// Synthesized times not yet visited, sorted DESCENDING so `last()` is the least.
    synth: Vec<usize>,
    /// The seed times reached so far, compacted by the running meet. They are the witnesses the
    /// absorption test looks for, and the partners a close joins against; keeping them collapsed is
    /// what stops a key with many reached times rescanning all of them.
    reached: Vec<usize>,
    /// Scratch for one step's synthesized times.
    temporary: Vec<usize>,
    /// Corrections emitted so far this sweep, meet-collapsed; both a join partner and part of the
    /// output accumulation.
    produced: Vec<((u64, usize), ROut)>,
    /// The meet of every time still to come.
    meet: Option<usize>,
    /// Whether the last `next_crossing` returned a time whose step is not yet settled.
    suspended: bool,
    /// Scratch for a pool compaction: the live rows, and the old-to-new row map.
    live_rows: Vec<usize>,
    row_map: Vec<usize>,
}

/// What one [`tick`](Sweep::tick) decided about the time it visited.
enum Tick {
    /// No seed reaches this time; the sweep moved past it.
    Passed,
    /// Reached, but at or beyond `upper`: carried to a later round rather than evaluated.
    Pended,
    /// Reached and in the interval. The caller must evaluate here before the sweep goes on.
    Crossing(usize),
    /// Every source is drained.
    Done,
}

impl<C: TimeColumn, RIn: Semigroup + Clone, ROut: Semigroup + Clone> Sweep<C, RIn, ROut>
where
    C::Time: Timestamp + Lattice,
{
    /// An empty sweep, to be `load`ed and reused for successive keys.
    fn new() -> Self {
        Sweep {
            pool: C::default(), upper: Vec::new(),
            input: ColumnHistory::new(), output: ColumnHistory::new(),
            seeds: Vec::new(), seed_meets: Vec::new(), seed_pos: 0,
            synth: Vec::new(), reached: Vec::new(), temporary: Vec::new(),
            produced: Vec::new(), meet: None, suspended: false,
            live_rows: Vec::new(), row_map: Vec::new(),
        }
    }

    /// Position the sweep at the start of one key.
    ///
    /// `owed_times[owed]` is the harness's due (warned) times and `seeds[novel]` the window's raw
    /// novel time support, both ascending; they merge into the seed list, held raw — the schedule is
    /// stated over these times, so they are never advanced. The records in `input` are merged and
    /// netted (novel and prior together): a cancelled record's time survives in the seed list, so
    /// netting loses nothing, and every record is a mere partner/accumulant that the meet may
    /// advance freely.
    #[allow(clippy::too_many_arguments)]
    fn load(
        &mut self,
        upper: &Antichain<C::Time>,
        owed_times: &C,
        owed: std::ops::Range<usize>,
        seeds: &Seeds<C>,
        novel: std::ops::Range<usize>,
        input: &Bridge<C, RIn>,
        input_rows: std::ops::Range<usize>,
        output: &Bridge<C, ROut>,
        output_rows: std::ops::Range<usize>,
    ) {
        let pool = &mut self.pool;
        pool.clear();
        self.upper.clear();
        self.upper.extend(upper.elements().iter().map(|t| pool.push(t)));

        // Merge the two ascending seed sources, deduplicated, as rows.
        self.seeds.clear();
        let (mut oi, mut ni) = (owed.start, novel.start);
        loop {
            let owed_row = (oi < owed.end).then(|| pool.push_from(owed_times, oi));
            let novel_row = (ni < novel.end).then(|| pool.push_from(&seeds.times, ni));
            let take_owed = match (owed_row, novel_row) {
                (Some(a), Some(b)) => pool.cmp(a, b) != std::cmp::Ordering::Greater,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let row = if take_owed {
                oi += 1;
                if let Some(b) = novel_row { debug_assert_eq!(b, pool.len() - 1); pool.truncate(b); }
                owed_row.expect("taken")
            } else {
                ni += 1;
                novel_row.expect("taken")
            };
            if self.seeds.last().is_none_or(|&l| pool.cmp(l, row) != std::cmp::Ordering::Equal) {
                self.seeds.push(row);
            }
        }
        self.seed_meets.clear();
        for &s in &self.seeds { self.seed_meets.push(pool.push_copy(s)); }
        for i in (1..self.seed_meets.len()).rev() {
            let (prev, next) = (self.seed_meets[i - 1], self.seed_meets[i]);
            pool.meet_assign(prev, next);
        }
        self.seed_pos = 0;
        self.synth.clear();
        self.reached.clear();
        self.temporary.clear();
        self.produced.clear();
        self.suspended = false;

        // The meet of every seed bounds every time the sweep will visit, so the record buffers can
        // be advanced by it at load.
        let meet = self.seed_meets.first().copied();
        self.input.load(pool, &input.times, input_rows.map(|i| (input.ids[i].1, i, input.diffs[i].clone())), meet);
        self.output.load(pool, &output.times, output_rows.map(|i| (output.ids[i].1, i, output.diffs[i].clone())), meet);
        self.meet = meet;
    }

    /// Advance to the next in-interval time that needs evaluating, or `None` once the key is spent.
    ///
    /// Times at or beyond `upper` that the schedule reaches are appended to `pended` for the caller
    /// to carry into a later round.
    fn next_crossing(&mut self, pended: &mut Vec<usize>) -> Option<usize> {
        loop {
            // A crossing leaves its step half-finished, because `settle` must see the corrections
            // the caller commits. Finishing it is the first thing the next call does.
            if self.suspended {
                self.suspended = false;
                self.settle();
            }
            match self.tick(pended) {
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
    fn tick(&mut self, pended: &mut Vec<usize>) -> Tick {
        let Some(at) = self.frontier() else { return Tick::Done };
        let reached = self.absorb(at);
        if self.beyond_upper(at) {
            // Out of the interval: nothing can be emitted here, so there is nothing to close
            // against either — a join with `at` is at or beyond `at`, hence also out of interval,
            // and will be rediscovered from `at` in the round that admits it.
            self.settle();
            if reached { pended.push(at); return Tick::Pended; }
            return Tick::Passed;
        }
        self.close(at, reached, pended);
        if reached { return Tick::Crossing(at); }
        self.settle();
        Tick::Passed
    }

    /// Whether row `at` is at or beyond the retire's upper frontier.
    fn beyond_upper(&self, at: usize) -> bool {
        self.upper.iter().any(|&u| self.pool.less_equal(u, at))
    }

    /// The sweep's position: the least time any source still offers, as a fresh row (so the
    /// sources may be advanced while it is held).
    ///
    /// The TOTAL order, not the partial one. Every time `close` produces is strictly greater than
    /// the position that produced it, so new work only ever lands ahead of here and the sweep never
    /// revisits.
    fn frontier(&mut self) -> Option<usize> {
        let pool = &mut self.pool;
        let mut least: Option<usize> = None;
        for cand in [self.seeds.get(self.seed_pos).copied(), self.input.time(), self.output.time(), self.synth.last().copied()].into_iter().flatten() {
            if least.is_none_or(|l| pool.cmp(cand, l) == std::cmp::Ordering::Less) { least = Some(cand); }
        }
        least.map(|l| pool.push_copy(l))
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
    fn absorb(&mut self, at: usize) -> bool {
        self.input.step_while_time_is(&self.pool, at);
        self.output.step_while_time_is(&self.pool, at);

        // A seed here — a due time or a novel-support time — is consumed into the reached set,
        // where it becomes a witness and a join partner for every later time. So is a synthetic
        // join scheduled for here.
        let mut reached = false;
        while self.synth.last().is_some_and(|&s| self.pool.cmp(s, at) == std::cmp::Ordering::Equal) {
            self.reached.push(self.synth.pop().expect("nonempty"));
            reached = true;
        }
        while self.seeds.get(self.seed_pos).is_some_and(|&s| self.pool.cmp(s, at) == std::cmp::Ordering::Equal) {
            let copy = self.pool.push_copy(at);
            self.reached.push(copy);
            self.seed_pos += 1;
            reached = true;
        }
        // Absorption: a time at or above a seed already consumed is itself reached, because
        // joining that seed with it yields it back.
        reached || self.reached.iter().any(|&t| self.pool.less_equal(t, at))
    }

    /// Close `at` forward under joins — clause one of `round_coverage`, the join-closure.
    ///
    /// Against the REACHED (seed-derived) times always, reached or not: an unreached time joined
    /// with a seed lands at or above that seed, so it carries a witness and is on the schedule.
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
    fn close(&mut self, at: usize, reached: bool, pended: &mut Vec<usize>) {
        let pool = &mut self.pool;
        for &t in &self.reached {
            if !pool.less_equal(t, at) { self.temporary.push(pool.push_join(t, at)); }
        }
        if reached {
            if let Some(meet) = self.meet {
                self.input.advance_buffer_by(pool, meet);
                self.output.advance_buffer_by(pool, meet);
            }
            for ((_, t), _) in self.input.buffer() {
                if !pool.less_equal(*t, at) { self.temporary.push(pool.push_join(*t, at)); }
            }
            for ((_, t), _) in self.output.buffer() {
                if !pool.less_equal(*t, at) { self.temporary.push(pool.push_join(*t, at)); }
            }
            for ((_, t), _) in &self.produced {
                if !pool.less_equal(*t, at) { self.temporary.push(pool.push_join(*t, at)); }
            }
        }
        self.temporary.sort_by(|&x, &y| pool.cmp(x, y));
        self.temporary.dedup_by(|&mut x, &mut y| pool.cmp(x, y) == std::cmp::Ordering::Equal);
        let before = self.synth.len();
        for time in self.temporary.drain(..) {
            if self.upper.iter().any(|&u| pool.less_equal(u, time)) { pended.push(time); } else { self.synth.push(time); }
        }
        if self.synth.len() > before {
            self.synth.sort_by(|&x, &y| pool.cmp(y, x));
            self.synth.dedup_by(|&mut x, &mut y| pool.cmp(x, y) == std::cmp::Ordering::Equal);
        }
    }

    /// The input accumulation at the suspended time.
    fn input_at(&self, at: usize, into: &mut Vec<(u64, RIn)>) {
        for ((id, time), diff) in self.input.buffer().iter() {
            if self.pool.less_equal(*time, at) { into.push((*id, diff.clone())); }
        }
        crate::consolidation::consolidate(into);
    }

    /// The tentative output accumulation at the suspended time, including this sweep's corrections.
    fn output_at(&self, at: usize, into: &mut Vec<(u64, ROut)>) {
        for ((id, time), diff) in self.output.buffer().iter().chain(self.produced.iter()) {
            if self.pool.less_equal(*time, at) { into.push((*id, diff.clone())); }
        }
        crate::consolidation::consolidate(into);
    }

    /// Record the corrections evaluated at the suspended time, and collapse them by the meet.
    fn commit(&mut self, at: usize, corrections: impl Iterator<Item = (u64, ROut)>) {
        let before = self.produced.len();
        for (id, diff) in corrections {
            let row = self.pool.push_copy(at);
            self.produced.push(((id, row), diff));
        }
        if self.produced.len() > before {
            if let Some(meet) = self.meet {
                for entry in self.produced.iter_mut() { self.pool.join_assign((entry.0).1, meet); }
            }
            consolidate_buffer(&self.pool, &mut self.produced);
        }
    }

    /// Close a step: recompute the meet of everything still to come, and compact the reached set by
    /// it. This is what keeps a key with a long history linear rather than quadratic.
    fn settle(&mut self) {
        let pool = &mut self.pool;
        let mut meet: Option<usize> = None;
        let mut update = |meet: &mut Option<usize>, other: Option<usize>| {
            if let Some(t) = other {
                match *meet {
                    Some(m) => pool.meet_assign(m, t),
                    None => *meet = Some(pool.push_copy(t)),
                }
            }
        };
        update(&mut meet, self.input.meet());
        update(&mut meet, self.output.meet());
        for &time in &self.synth { update(&mut meet, Some(time)); }
        update(&mut meet, self.seed_meets.get(self.seed_pos).copied());
        if let Some(m) = meet {
            for &time in &self.reached { pool.join_assign(time, m); }
        }
        self.reached.sort_by(|&x, &y| pool.cmp(x, y));
        self.reached.dedup_by(|&mut x, &mut y| pool.cmp(x, y) == std::cmp::Ordering::Equal);
        self.meet = meet;
        self.compact_if_large();
    }

    /// Rebuild the pool from its live rows once the dead ones dominate: every join appends a row,
    /// and a key with a long history would otherwise keep every time it ever synthesized.
    fn compact_if_large(&mut self) {
        // An upper bound on the live rows, cheap to compute, so that a key with a long history is
        // not scanned for its live rows at every step.
        let live_estimate = self.upper.len() + self.seeds.len() + self.seed_meets.len() + self.synth.len()
            + self.reached.len() + self.produced.len() + self.input.rows_len() + self.output.rows_len() + 2;
        if self.pool.len() < 4 * live_estimate + 256 { return; }
        let live = &mut self.live_rows;
        live.clear();
        live.extend(&self.upper);
        live.extend(&self.seeds);
        live.extend(&self.seed_meets);
        live.extend(&self.synth);
        live.extend(&self.reached);
        live.extend(self.produced.iter().map(|p| (p.0).1));
        live.extend(self.meet);
        self.input.rows(live);
        self.output.rows(live);
        live.sort_unstable();
        live.dedup();
        if self.pool.len() < 4 * live.len() + 256 { return; }
        let map = &mut self.row_map;
        map.clear();
        map.resize(self.pool.len(), usize::MAX);
        let mut fresh = C::default();
        for &row in live.iter() { map[row] = fresh.push_from(&self.pool, row); }
        self.pool = fresh;
        for r in self.upper.iter_mut() { *r = map[*r]; }
        for r in self.seeds.iter_mut() { *r = map[*r]; }
        for r in self.seed_meets.iter_mut() { *r = map[*r]; }
        for r in self.synth.iter_mut() { *r = map[*r]; }
        for r in self.reached.iter_mut() { *r = map[*r]; }
        for p in self.produced.iter_mut() { (p.0).1 = map[(p.0).1]; }
        if let Some(m) = self.meet.as_mut() { *m = map[*m]; }
        self.input.remap(map);
        self.output.remap(map);
    }
}
