//! The proxy reduce framework.
//!
//! A conventional differential reduce against `(u64, u64)`, where the backend supplies the
//! implementation of the interpretation of the integers.

use super::pending::Pending;

use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use super::diffs::{Consolidation, DiffContainer, Records};
use crate::lattice::Lattice;
use crate::trace::{Span, Description};
use super::history::DiffHistory;
use crate::operators::reduce::{sort_dedup, ReduceTactic};

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
pub struct ReduceWindow<T, RIn, ROut> {
    /// The key's full input — novel and prior merged, netted — sorted & consolidated by
    /// `((key_hash, value_id), time)`. May be advanced to the compaction frontier.
    pub input: Records<((u64, u64), T), RIn>,
    /// The RAW novel time support: `(key_hash, time)` pairs sorted by `(key_hash, time)` and
    /// deduplicated, recorded from the novel batches BEFORE any consolidation or advancement —
    /// a netted-away record's time must still appear here.
    pub seeds: Vec<(u64, T)>,
    /// Accumulated output preceding the retire's interval, same ordering as `input`.
    pub output: Records<((u64, u64), T), ROut>,
}

impl<T, RIn: DiffContainer, ROut: DiffContainer> ReduceWindow<T, RIn, ROut> {
    /// Empty presentations with backend-supplied difference storage.
    pub fn new(input: RIn, output: ROut) -> Self {
        Self { input: Records::new(input), seeds: Vec::new(), output: Records::new(output) }
    }

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
/// `begin new_diffs [ next_window reduce_corrections* emit ]* finish`,
/// where the window loop runs until `next_window` reports the key space exhausted.
pub trait ProxyReduceBackend<T, B1, B2> {
    /// Difference storage presented for the input.
    type RIn: DiffContainer;
    /// Difference storage for the output.
    type ROut: DiffContainer;

    /// Initiate a session to create batches for these descriptions, which span `[lower, upper)`.
    ///
    /// It is the backend's job to prepare output batches for each of these descriptions.
    /// The computation proceeds in windows of keys, where only the backend maintains this
    /// work in progress, until `finish()` is called.
    fn begin(&mut self, description: Description<T>);

    /// Empty input and output storage with this session's schemas and accumulation semantics.
    /// Called after `begin`; all presentations and corrections must use these schemas.
    fn new_diffs(&self) -> (Self::RIn, Self::ROut);

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
        window: &mut ReduceWindow<T, Self::RIn, Self::ROut>,
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
        input: &Records<u64, Self::RIn>,
        out_ends: &[usize],
        output: &Records<u64, Self::ROut>,
    ) -> (Records<u64, Self::ROut>, Vec<usize>);

    /// Commit a collection of updates to the batch in progress.
    fn emit(&mut self, records: &Records<((u64, u64), T), Self::ROut>);

    /// Complete the session matching `begin`, yielding the batch it described,
    /// or `None` when the span it described carries no updates.
    fn finish(&mut self) -> Option<B2>;
}

/// A proxy-space [`ReduceTactic`]: matches input and output records by `key_hash`.
pub struct ProxyReduceTactic<T, Bk> {
    backend: Bk,
    /// Maximum number of key hashes with live sweep state at once.
    key_batch_size: usize,
    /// Pending interesting times, shared across flat key ranges.
    pending: Pending<T>,
}

impl<T, Bk> ProxyReduceTactic<T, Bk> {
    /// A tactic deferring all value semantics to `backend`.
    pub fn new(backend: Bk) -> Self {
        ProxyReduceTactic { backend, key_batch_size: usize::MAX, pending: Pending::default() }
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

impl<T, B1, B2, Bk> ReduceTactic<T, B1, B2> for ProxyReduceTactic<T, Bk>
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
                self.pending.frontier().iter().all(|time| held.less_equal(time)),
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

        // Split the carried interesting times against `upper`.
        // A time below it is DUE: its key must be re-evaluated this retire, so the key is `changed`.
        // A time at or beyond it remains carried in `self.pending`.
        // Activate time groups before visiting their keys. Due rows index a shared time table.
        let due = self.pending.activate(upper.borrow());
        // The keys the harness knows must be revisited. The backend adds those its novel batches
        // touch, which it discovers while reading them; neither side scans the whole key space.
        let mut changed: Vec<u64> = due.rows.iter().map(|r| r.0).collect();
        changed.dedup();
        let mut deferred = Vec::new();
        let mut due_pos = 0;

        // Nothing due and nothing novel: no time in the interval can be interesting, so there is no
        // work and no output. Return the frontier bounding the times still withheld — NOT an empty
        // one. This is exactly where a due-only `changed` differs from the whole pending set: times
        // beyond `upper` can remain when nothing is due, and releasing their capabilities would
        // strand them (see the frontier clause of the `ReduceTactic::retire` contract).
        if changed.is_empty() && instance.input_batches.is_empty() {
            return (None, self.pending.frontier());
        }

        // The single output batch spans the retired interval.
        let description = Description::new(lower.clone(), upper.clone(), Antichain::from_elem(T::minimum()));
        self.backend.begin(description.clone());

        // Progress through the key space: `Some(h)` for key hashes at or above `h` remaining, `None`
        // once the backend reports the space covered.
        let mut from = Some(0u64);
        let (input_diffs, output_diffs) = self.backend.new_diffs();
        let mut window = ReduceWindow::new(input_diffs, output_diffs);

        // Retire-wide reusable scratch: cleared per group, window or wave, retaining capacity. Fresh
        // per-key/per-wave `Vec`s were once the dominant cost here, which is why the slots and the
        // staging buffers are held across the whole retire rather than built where they are used.
        let mut slots: Vec<KeySweep<T, Bk::RIn, Bk::ROut>> = Vec::new();
        let mut live: Vec<usize> = Vec::new();
        let mut deltas = Records::new(window.output.diffs.empty());
        let mut delta_scratch = Consolidation::new(&window.output.diffs);
        let mut batch_keys: Vec<u64> = Vec::new();
        let mut in_ends: Vec<usize> = Vec::new();
        let mut in_all = Records::new(window.input.diffs.empty());
        let mut out_ends: Vec<usize> = Vec::new();
        let mut out_all = Records::new(window.output.diffs.empty());
        let mut active: Vec<(usize, T)> = Vec::new();
        let mut in_accum = Records::new(window.input.diffs.empty());
        let mut cur_out = Records::new(window.output.diffs.empty());

        let mut in_scratch = Consolidation::new(&window.input.diffs);
        let mut out_scratch = Consolidation::new(&window.output.diffs);

        while from.is_some() {
            let before = from;
            window.clear();
            self.backend.next_window(&instance, &changed, &mut from, &mut window);
            let p_in = &window.input;
            let seeds = &window.seeds;
            let p_out = &window.output;
            debug_assert_eq!(p_in.len(), p_in.diffs.len());
            debug_assert!(p_in.data.windows(2).all(|w| w[0] < w[1]), "next_window.input must be sorted and consolidated");
            debug_assert_eq!(p_out.len(), p_out.diffs.len());
            debug_assert!(p_out.data.windows(2).all(|w| w[0] < w[1]), "next_window.output must be sorted and consolidated");
            debug_assert!(
                seeds.windows(2).all(|w| w[0] < w[1]),
                "next_window.seeds must be sorted by (key_hash, time) and deduplicated",
            );
            // Without progress the window loop would never retire, so this guards liveness as well
            // as contract; the range check catches a key reported outside the window that owns it,
            // which would silently drop the interaction between its halves.
            debug_assert!(
                from.is_none() || from > before,
                "next_window must either advance `from` or report the key space exhausted",
            );
            debug_assert!(
                {
                    let mut keys = p_in.data.iter().map(|r| r.0.0).chain(seeds.iter().map(|s| s.0)).chain(p_out.data.iter().map(|r| r.0.0));
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
                    p_in.data.get(is).map(|record| record.0.0),
                    seeds.get(ns).map(|seed| seed.0),
                    p_out.data.get(os).map(|record| record.0.0),
                ].into_iter().flatten().min() {
                    let i0 = is;
                    while is < p_in.len() && p_in.data[is].0.0 == key { is += 1; }
                    let i1 = is;
                    let n0 = ns;
                    while ns < seeds.len() && seeds[ns].0 == key { ns += 1; }
                    let n1 = ns;
                    let o0 = os;
                    while os < p_out.len() && p_out.data[os].0.0 == key { os += 1; }
                    let o1 = os;

                    if n_slots == slots.len() { slots.push(KeySweep::empty(&p_in.diffs, &p_out.diffs)); }
                    let slot = &mut slots[n_slots];
                    slot.key = key;
                    slot.pended.clear();
                    // Only the DUE times seed the sweep; the carried ones remain in `self.pending`.
                    while due_pos < due.rows.len() && due.rows[due_pos].0 < key { due_pos += 1; }
                    let start = due_pos;
                    while due_pos < due.rows.len() && due.rows[due_pos].0 == key { due_pos += 1; }
                    let owed = &due.rows[start..due_pos];
                    let novel = &seeds[n0..n1];
                    // One seed dominating every record has exactly one possible evaluation.
                    let single = owed.first().map(|r| &due.times[r.1]).or_else(|| novel.first().map(|r| &r.1))
                        .filter(|at| owed.len() <= 1 && novel.len() <= 1
                            && novel.first().is_none_or(|r| &r.1 == *at)
                            && p_in.data[i0..i1].iter().all(|r| r.1.less_equal(at))
                            && p_out.data[o0..o1].iter().all(|r| r.1.less_equal(at)));
                    slot.direct = single.map(|_| (i0..i1, o0..o1));
                    slot.at = if let Some(at) = single {
                        if upper.less_equal(at) { slot.pended.push(at.clone()); None }
                        else { Some(at.clone()) }
                    } else {
                        slot.sweep.load(
                            owed.iter().map(|&(_, row)| due.times[row].clone()),
                            novel.iter().map(|r| r.1.clone()),
                            p_in, i0..i1,
                            p_out, o0..o1,
                        );
                        slot.sweep.next_crossing(upper, &mut slot.pended)
                    };
                    if slot.at.is_some() { live.push(n_slots); }
                    else if !slot.pended.is_empty() {
                        deferred.extend(slot.pended.drain(..).map(|time| (time, key)));
                    }
                    n_slots += 1;
                    if n_slots == self.key_batch_size { break; }
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
                        if let Some((ir, or)) = &slots[si].direct {
                            in_accum.extend(p_in, ir.clone(), |r| r.0.1);
                            cur_out.extend(p_out, or.clone(), |r| r.0.1);
                        } else {
                            slots[si].sweep.input_at(&at, &mut in_accum);
                            slots[si].sweep.output_at(&at, &mut cur_out);
                        }
                        in_scratch.consolidate(&mut in_accum.data, &mut in_accum.diffs);
                        out_scratch.consolidate(&mut cur_out.data, &mut cur_out.diffs);
                        // An interesting time can still reach the gate with nothing to read; the
                        // conventional reduce skips user logic there and so do we.
                        if in_accum.is_empty() && cur_out.is_empty() { continue; }
                        batch_keys.push(slots[si].key);
                        in_all.extend(&in_accum, 0..in_accum.len(), |id| *id);
                        in_ends.push(in_all.len());
                        out_all.extend(&cur_out, 0..cur_out.len(), |id| *id);
                        out_ends.push(out_all.len());
                        active.push((si, at));
                    }

                    if !batch_keys.is_empty() {
                        let (corr, corr_ends) = self.backend.reduce_corrections(&batch_keys, &in_ends, &in_all, &out_ends, &out_all);
                        let mut cstart = 0usize;
                        for (bi, (si, at)) in active.iter().enumerate() {
                            let cend = corr_ends[bi];
                            if cstart != cend {
                                debug_assert!(held.elements().iter().any(|h| h.less_equal(at)), "no held capability <= active time");
                                deltas.extend(&corr, cstart..cend, |vid| ((slots[*si].key, *vid), at.clone()));
                                if slots[*si].direct.is_none() {
                                    slots[*si].sweep.commit(at, &corr, cstart..cend);
                                }
                            }
                            cstart = cend;
                        }
                    }

                    // Step every live key past the time it was suspended at, and retire the spent ones.
                    for &si in live.iter() {
                        let slot = &mut slots[si];
                        slot.at = if slot.direct.is_some() { None }
                            else { slot.sweep.next_crossing(upper, &mut slot.pended) };
                        if slot.at.is_none() && !slot.pended.is_empty() {
                            deferred.extend(slot.pended.drain(..).map(|time| (time, slot.key)));
                        }
                    }
                    live.retain(|&si| slots[si].at.is_some());
                }
                // Bound staging by the same key groups as the sweep scratch.
                self.pending.insert(std::mem::take(&mut deferred));
            }

            if !deltas.is_empty() {
                delta_scratch.consolidate(&mut deltas.data, &mut deltas.diffs);
                self.backend.emit(&deltas);
            }
        }

        let produced = Some(Span::new(description, self.backend.finish()));
        (produced, self.pending.frontier())
    }
}

/// One key's slot in a window: its [`Sweep`], the time it is suspended at, and the times it has
/// pended so far. Slots and their scratch capacity are reused across groups and windows
/// within a retire, then dropped when the retire completes.
struct KeySweep<T, RIn, ROut> {
    key: u64,
    sweep: Sweep<T, RIn, ROut>,
    direct: Option<(std::ops::Range<usize>, std::ops::Range<usize>)>,
    /// Times at or beyond `upper` the sweep has reached; carried forward when the slot retires.
    pended: Vec<T>,
    /// The time the sweep last suspended at, or `None` once it is spent.
    at: Option<T>,
}

impl<T: Timestamp + Lattice, RIn: DiffContainer, ROut: DiffContainer> KeySweep<T, RIn, ROut> {
    fn empty(input: &RIn, output: &ROut) -> Self {
        KeySweep { key: 0, sweep: Sweep::new(input, output), direct: None, pended: Vec::new(), at: None }
    }
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
/// The novel times arrive as the SEED LIST (the harness's warned times merged with the window's raw
/// novel time support); the records themselves travel merged with the prior history and carry no
/// witness duty, which is what lets them net and advance. Coverage is invariant under that move:
/// the witness clause reads only times, and consolidation cancels only equal-time pairs whose time
/// the seed list retains.
struct Sweep<T, RIn, ROut> {
    /// The accumulated input (novel and prior, merged and netted) and output: join partners, and
    /// the accumulations to evaluate over. Both may be advanced freely — witness duty lives in
    /// `seeds`, not in any record.
    input: DiffHistory<T, RIn>,
    output: DiffHistory<T, ROut>,
    /// The key's seed times — the harness's due (warned) times merged with the raw novel time
    /// support — ascending and deduplicated, with their suffix meets; `seed_pos` consumes them.
    /// These are the ONLY source of interest: the schedule is stated over them, so they are held
    /// raw, never advanced.
    seeds: Vec<T>,
    seed_meets: Vec<T>,
    seed_pos: usize,
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
    produced: Records<(u64, T), ROut>,
    produced_scratch: Consolidation<(u64, T), ROut>,
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

impl<T: Timestamp + Lattice, RIn: DiffContainer, ROut: DiffContainer> Sweep<T, RIn, ROut> {
    /// An empty sweep, to be `load`ed and reused for successive keys.
    fn new(input: &RIn, output: &ROut) -> Self {
        Sweep {
            input: DiffHistory::new(input), output: DiffHistory::new(output),
            seeds: Vec::new(), seed_meets: Vec::new(), seed_pos: 0,
            synth: Vec::new(), reached: Vec::new(), temporary: Vec::new(),
            produced: Records::new(output.empty()), produced_scratch: Consolidation::new(output),
            meet: None, suspended: false,
        }
    }

    /// Position the sweep at the start of one key.
    ///
    /// `owed` is the harness's due (warned) times and `novel_times` the window's raw novel time
    /// support, both ascending; they merge into the seed list, which is held raw — the schedule is
    /// stated over these times, so they are never advanced. The records in `input` are merged and
    /// netted (novel and prior together): a cancelled record's time survives in the seed list, so
    /// netting loses nothing, and every record is a mere partner/accumulant that the meet may
    /// advance freely.
    fn load(
        &mut self,
        owed: impl Iterator<Item = T>,
        novel_times: impl Iterator<Item = T>,
        input: &Records<((u64, u64), T), RIn>,
        in_rows: std::ops::Range<usize>,
        output: &Records<((u64, u64), T), ROut>,
        out_rows: std::ops::Range<usize>,
    ) {
        // Merge the two ascending seed sources, deduplicated.
        self.seeds.clear();
        let mut owed = owed.peekable();
        let mut novel = novel_times.peekable();
        loop {
            let take_owed = match (owed.peek(), novel.peek()) {
                (Some(a), Some(b)) => a <= b,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (None, None) => break,
            };
            let time = if take_owed { owed.next() } else { novel.next() }.expect("peeked");
            if self.seeds.last() != Some(&time) {
                self.seeds.push(time);
            }
        }
        self.seed_meets.clear();
        self.seed_meets.extend(self.seeds.iter().cloned());
        for i in (1..self.seed_meets.len()).rev() {
            let (init, tail) = self.seed_meets.split_at_mut(i);
            init[i - 1].meet_assign(&tail[0]);
        }
        self.seed_pos = 0;
        self.synth.clear();
        self.reached.clear();
        self.temporary.clear();
        self.produced.clear();
        self.suspended = false;

        // The meet of every seed bounds every time the sweep will visit, so the record buffers can
        // be advanced by it at load.
        let mut meet: Option<T> = None;
        update_meet(&mut meet, self.seed_meets.first());
        self.input.load(input, in_rows, meet.as_ref());
        self.output.load(output, out_rows, meet.as_ref());
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
            self.seeds.get(self.seed_pos), self.input.time(),
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

        // A seed here — a due time or a novel-support time — is consumed into the reached set,
        // where it becomes a witness and a join partner for every later time. So is a synthetic
        // join scheduled for here.
        let mut reached = false;
        while self.synth.last() == Some(at) {
            self.reached.push(self.synth.pop().expect("nonempty"));
            reached = true;
        }
        while self.seeds.get(self.seed_pos) == Some(at) {
            self.reached.push(at.clone());
            self.seed_pos += 1;
            reached = true;
        }
        // Absorption: a time at or above a seed already consumed is itself reached, because
        // joining that seed with it yields it back.
        reached || self.reached.iter().any(|t| t.less_equal(at))
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
    fn close(&mut self, at: &T, reached: bool, upper: &Antichain<T>, pended: &mut Vec<T>) {
        self.temporary.extend(self.reached.iter()
            .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
        if reached {
            if let Some(meet) = self.meet.as_ref() {
                self.input.advance_buffer_by(meet);
                self.output.advance_buffer_by(meet);
            }
            self.temporary.extend(self.input.buffer.data.iter().map(|(_, t)| t)
                .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
            self.temporary.extend(self.output.buffer.data.iter().map(|(_, t)| t)
                .filter(|t| !t.less_equal(at)).map(|t| t.join(at)));
            self.temporary.extend(self.produced.data.iter().map(|(_, t)| t)
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

    /// The input accumulation at the suspended time, to be consolidated by the caller.
    fn input_at(&self, at: &T, into: &mut Records<u64, RIn>) {
        let buffer = &self.input.buffer;
        into.extend(buffer, (0..buffer.len()).filter(|&row| buffer.data[row].1.less_equal(at)), |r| r.0);
    }

    /// The tentative output accumulation at the suspended time, including this sweep's corrections.
    /// The caller consolidates the combined selection.
    fn output_at(&self, at: &T, into: &mut Records<u64, ROut>) {
        for buffer in [&self.output.buffer, &self.produced] {
            into.extend(buffer, (0..buffer.len()).filter(|&row| buffer.data[row].1.less_equal(at)), |r| r.0);
        }
    }

    /// Record the corrections evaluated at the suspended time, and collapse them by the meet.
    fn commit(&mut self, at: &T, corrections: &Records<u64, ROut>, rows: std::ops::Range<usize>) {
        if !rows.is_empty() {
            self.produced.extend(corrections, rows, |id| (*id, at.clone()));
            if let Some(meet) = self.meet.as_ref() {
                for (_, time) in &mut self.produced.data { time.join_assign(meet); }
            }
            self.produced_scratch.consolidate(&mut self.produced.data, &mut self.produced.diffs);
        }
    }

    /// Close a step: recompute the meet of everything still to come, and compact the reached set by
    /// it. This is what keeps a key with a long history linear rather than quadratic.
    fn settle(&mut self) {
        let mut meet: Option<T> = None;
        update_meet(&mut meet, self.input.meet());
        update_meet(&mut meet, self.output.meet());
        for time in self.synth.iter() { update_meet(&mut meet, Some(time)); }
        update_meet(&mut meet, self.seed_meets.get(self.seed_pos));
        if let Some(m) = meet.as_ref() {
            for time in self.reached.iter_mut() { *time = time.join(m); }
        }
        sort_dedup(&mut self.reached);
        self.meet = meet;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::diffs::consolidate;
    use timely::order::Product;

    /// Two independent columns; neither storage nor a scalar row implements Semigroup here.
    struct Pair { left: Vec<i64>, right: Vec<i64> }

    impl DiffContainer for Pair {
        fn len(&self) -> usize { self.left.len() }
        fn empty(&self) -> Self { Self { left: vec![], right: vec![] } }
        fn clear(&mut self) { self.left.clear(); self.right.clear(); }
        fn copy_from(&mut self, source: &Self, rows: &[usize]) {
            DiffContainer::copy_from(&mut self.left, &source.left, rows);
            DiffContainer::copy_from(&mut self.right, &source.right, rows);
        }
        fn sum_from(&mut self, source: &Self, rows: &[usize], ends: &[usize]) {
            self.left.sum_from(&source.left, rows, ends);
            self.right.sum_from(&source.right, rows, ends);
        }
        fn nonzero(&self, into: &mut Vec<usize>) {
            into.extend(self.left.iter().zip(&self.right).enumerate()
                .filter(|(_, (a, b))| **a != 0 || **b != 0).map(|(row, _)| row));
        }
    }

    #[test]
    fn columnar_diffs_survive_consolidation_and_history_replay() {
        let mut data = vec!["a", "b", "a", "b", "c", "c"];
        let mut diffs = Pair { left: vec![1, 0, -1, 0, 1, -1], right: vec![0, 1, 2, -1, 0, 0] };
        consolidate(&mut data, &mut diffs);
        assert_eq!(data, vec!["a"]);
        assert_eq!((diffs.left, diffs.right), (vec![0], vec![2]));

        let schema = Pair { left: vec![], right: vec![] };
        let mut input = Records::new(schema.empty());
        input.data = vec![((0, 7), Product::new(0u64, 1u64)), ((0, 7), Product::new(1, 0)), ((0, 7), Product::new(2, 2))];
        input.diffs = Pair { left: vec![1, -1, 3], right: vec![0, 2, -2] };
        let output = Records::new(schema.empty());
        let mut sweep = Sweep::new(&schema, &schema);
        sweep.load(std::iter::empty(), input.data.iter().map(|r| r.1), &input, 0..3, &output, 0..0);
        let mut pended = Vec::new();
        let mut accum = Records::new(schema.empty());
        let mut current = Records::new(schema.empty());
        // Identity reduction: corrections from earlier crossings must enter later output sums.
        for (time, expected_input, expected_output) in [
            (Product::new(0, 1), (1, 0), None),
            (Product::new(1, 0), (-1, 2), None),
            (Product::new(1, 1), (0, 2), Some((0, 2))),
            (Product::new(2, 2), (3, 0), Some((0, 2))),
        ] {
            assert_eq!(sweep.next_crossing(&Antichain::new(), &mut pended), Some(time));
            accum.clear();
            current.clear();
            sweep.input_at(&time, &mut accum);
            sweep.output_at(&time, &mut current);
            consolidate(&mut accum.data, &mut accum.diffs);
            consolidate(&mut current.data, &mut current.diffs);
            assert_eq!(accum.data, [7]);
            assert_eq!((accum.diffs.left[0], accum.diffs.right[0]), expected_input);
            if let Some(expected) = expected_output {
                assert_eq!(current.data, [7]);
                assert_eq!((current.diffs.left[0], current.diffs.right[0]), expected);
            } else {
                assert!(current.is_empty());
            }
            let previous = expected_output.unwrap_or((0, 0));
            accum.diffs.left[0] -= previous.0;
            accum.diffs.right[0] -= previous.1;
            consolidate(&mut accum.data, &mut accum.diffs);
            sweep.commit(&time, &accum, 0..accum.len());
        }
        assert_eq!(sweep.next_crossing(&Antichain::new(), &mut pended), None);
        assert!(pended.is_empty());
    }

}
