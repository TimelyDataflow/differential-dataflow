//! Types and methods generally useful for differential computation.
//!
//! The contents operate on `(id, time, diff)` update streams and lattice operations, with key
//! and value representation left to the caller: replaying histories in time order with
//! meet-advanced buffers ([`ValueHistory`], [`TimeHistory`]), determining the times at which a
//! key's reduction must be re-evaluated ([`discover_times`]), producing the join of two
//! histories in time order ([`bilinear_wave`]), and cutting an output interval into batch
//! descriptions along held capabilities ([`tile_descriptions`]).
//!
//! Each item's correctness is a property of its own inputs and outputs, so they can be used
//! and tested in isolation; sequencing obligations that span calls (for example, where
//! interesting-time seeds must come from) are stated at the item that imposes them and are
//! the caller's responsibility.

use timely::progress::{Antichain, Timestamp};

use crate::difference::{Multiply, Semigroup};
use crate::lattice::Lattice;
use crate::trace::Description;
use crate::operators::reduce::sort_dedup;

pub use crate::operators::ValueHistory;

/// Replays a set of times in ascending order, maintaining the meet of the times not yet
/// replayed and a deduplicated buffer of replayed times advanced by that meet. A cheaper
/// [`ValueHistory`] for callers that need time structure only (no values, no cancellation).
pub struct TimeHistory<T> {
    /// Un-replayed `(time, meet)`, sorted descending by time so popping replays ascending;
    /// `meet` is the meet of this time with all times later in the replay.
    history: Vec<(T, T)>,
    /// Stepped-in times, advanced and deduplicated, sorted ascending.
    buffer: Vec<T>,
}

impl<T> Default for TimeHistory<T> {
    fn default() -> Self { TimeHistory { history: Vec::new(), buffer: Vec::new() } }
}

impl<T: Lattice + Clone + Ord> TimeHistory<T> {
    /// An empty history, to be `load`ed.
    pub fn new() -> Self { TimeHistory { history: Vec::new(), buffer: Vec::new() } }

    /// Load `times`, advancing each by `advance_by` if supplied, and organize the replay
    /// (sort + suffix meets).
    pub fn load(&mut self, times: impl Iterator<Item = T>, advance_by: Option<&T>) {
        self.history.clear();
        self.buffer.clear();
        for mut time in times {
            if let Some(m) = advance_by {
                time = time.join(m);
            }
            self.history.push((time.clone(), time));
        }
        self.history.sort_by(|x, y| y.0.cmp(&x.0));
        self.history.iter_mut().reduce(|prev, cur| {
            cur.1.meet_assign(&prev.1);
            cur
        });
    }

    /// The next (least) un-replayed time.
    pub fn time(&self) -> Option<&T> {
        self.history.last().map(|x| &x.0)
    }
    /// The meet of all un-replayed times.
    pub fn meet(&self) -> Option<&T> {
        self.history.last().map(|x| &x.1)
    }

    /// Step times while the next equals `time`; true iff any did.
    pub fn step_while_time_is(&mut self, time: &T) -> bool {
        let mut found = false;
        while self.time() == Some(time) {
            found = true;
            let (t, _) = self.history.pop().unwrap();
            self.buffer.push(t);
        }
        found
    }

    /// Advance buffered times by `meet` and deduplicate — the collapse that keeps replay
    /// linear.
    pub fn advance_buffer_by(&mut self, meet: &T) {
        for time in self.buffer.iter_mut() {
            *time = time.join(meet);
        }
        self.buffer.sort();
        self.buffer.dedup();
    }

    /// The buffered (stepped-in, advanced) times.
    pub fn buffer(&self) -> &[T] {
        &self.buffer
    }
}

/// Produces the join of two histories: every pair of edits, diffs multiplied and times
/// joined, visited in time order. Repeatedly steps the history with the earlier un-replayed
/// edit and multiplies it against the other's buffer, which is consolidated under the meet of
/// its remaining times as the wave advances — so work is bounded by the netted accumulation
/// sizes rather than the raw history lengths.
///
/// `emit` receives every produced `(id0, id1, joined time, multiplied diff)`. Both histories
/// must be pre-loaded (`load`/`load_iter`) and are fully drained. For small histories a plain
/// cross product is cheaper; callers should gate on size.
pub fn bilinear_wave<V, T, R0, R1, RO>(
    h0: &mut ValueHistory<V, T, R0>,
    h1: &mut ValueHistory<V, T, R1>,
    mut emit: impl FnMut(V, V, T, RO),
) where
    V: Copy + Ord,
    T: Ord + Clone + Lattice,
    R0: Semigroup + Multiply<R1, Output = RO> + Clone,
    R1: Semigroup + Clone,
{
    while h0.time().is_some() && h1.time().is_some() {
        if h0.time().unwrap() < h1.time().unwrap() {
            h1.advance_buffer_by(h0.meet().unwrap());
            let (v0, t0, d0) = h0.edit().unwrap();
            for ((v1, t1), d1) in h1.buffer() {
                emit(v0, *v1, t0.join(t1), d0.clone().multiply(d1));
            }
            h0.step();
        } else {
            h0.advance_buffer_by(h1.meet().unwrap());
            let (v1, t1, d1) = h1.edit().unwrap();
            for ((v0, t0), d0) in h0.buffer() {
                emit(*v0, v1, t0.join(t1), d0.clone().multiply(d1));
            }
            h1.step();
        }
    }
    while h0.time().is_some() {
        h1.advance_buffer_by(h0.meet().unwrap());
        let (v0, t0, d0) = h0.edit().unwrap();
        for ((v1, t1), d1) in h1.buffer() {
            emit(v0, *v1, t0.join(t1), d0.clone().multiply(d1));
        }
        h0.step();
    }
    while h1.time().is_some() {
        h0.advance_buffer_by(h1.meet().unwrap());
        let (v1, t1, d1) = h1.edit().unwrap();
        for ((v0, t0), d0) in h0.buffer() {
            emit(*v0, v1, t0.join(t1), d0.clone().multiply(d1));
        }
        h1.step();
    }
}

/// Cuts the interval `[lower, upper)` into consecutive batch descriptions along `held`, which
/// must be sorted: the `i`-th cut point is the frontier formed by inserting `held[i+1..]` into
/// `upper`, so description `i` covers the part of the interval not greater-or-equal any held
/// time after `held[i]` (and not covered by an earlier description). Descriptions whose
/// interval is empty are skipped. Returns the descriptions, the held time associated with
/// each, and, per held index, the index of its description (`None` if skipped). A batch built
/// to description `i` can be committed at the capability `held[i]`.
pub fn tile_descriptions<T: Timestamp + Lattice>(
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

/// A one-key view into the ACCUMULATED input presentation: the read-only arguments
/// [`discover_times`] needs about a single key — its slice `[i0, i1)` of the `(id, time, diff)` run
/// `p_in` and the carried `pending` times. The novel run is not here; it arrives as `seed_times`,
/// and the two are deliberately never merged (see the note on `seed_times` below).
pub struct KeyView<'a, T, RIn> {
    /// The presented `((key_hash, value_id), time, diff)` run the key's records live in.
    pub p_in: &'a [((u64, u64), T, RIn)],
    /// The key's first record.
    pub i0: usize,
    /// One past the key's last record.
    pub i1: usize,
    /// Interesting times pended for this key by earlier retires.
    pub pending: &'a [T],
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

/// Reusable per-key scratch for [`discover_times`]: held once and threaded through every key,
/// so replays and time buffers are cleared and refilled rather than reallocated per key.
pub struct DiscoverScratch<T, RIn> {
    batch_replay: TimeHistory<T>,
    input_replay: ValueHistory<u64, T, RIn>,
    output_replay: TimeHistory<T>,
    synth: Vec<T>,
    times_current: Vec<T>,
    temporary: Vec<T>,
    meets: Vec<T>,
}

impl<T: Timestamp + Lattice, RIn: Semigroup + Clone> DiscoverScratch<T, RIn> {
    /// Fresh scratch; hold one per retire and thread it through every key.
    pub fn new() -> Self {
        DiscoverScratch {
            batch_replay: TimeHistory::new(),
            input_replay: ValueHistory::new(),
            output_replay: TimeHistory::new(),
            synth: Vec::new(),
            times_current: Vec::new(),
            temporary: Vec::new(),
            meets: Vec::new(),
        }
    }
}

impl<T: Timestamp + Lattice, RIn: Semigroup + Clone> Default for DiscoverScratch<T, RIn> {
    fn default() -> Self { Self::new() }
}

/// Determines the times in `[lower, upper)` at which a key's reduction must be re-evaluated
/// (`moments`), and the times at or beyond `upper` to carry into the next invocation
/// (`pended`). Replays the key's `seed_times` (the novel run) and `pending` times in ascending
/// order, marking
/// those that carry updates and closing the set under joins with the input and output
/// histories' times and with each other. No input collection is materialized, so peak memory
/// is O(times); buffers are advanced by the meet of the times still to come, keeping a key
/// with many distinct times linear rather than quadratic.
///
/// `seed_times` must be the novel batch's own time support for this key, and `key.p_in` the
/// accumulated input WITHOUT it. Seeding from a view of the two consolidated together is unsound:
/// compaction may advance a history record onto a novel time, where consolidation cancels the novel
/// update and its interesting time is missed. This is why the caller keeps the two runs apart and
/// combines them only when accumulating.
#[allow(clippy::too_many_arguments)]
pub fn discover_times<T, RIn>(
    key: KeyView<'_, T, RIn>,
    seed_times: impl Iterator<Item = T>,
    out_times: impl Iterator<Item = T>,
    upper: &Antichain<T>,
    scratch: &mut DiscoverScratch<T, RIn>,
    moments: &mut Vec<T>,
    pended: &mut Vec<T>,
) where
    T: Timestamp + Lattice,
    RIn: Semigroup + Clone,
{
    // Reuse the retire's scratch: `load`/`load_iter` reset the replays (keeping capacity); the plain
    // buffers are cleared here. `meets_slice` reborrows `meets` immutably; the rest stay disjoint.
    let DiscoverScratch { batch_replay, input_replay, output_replay, synth, times_current, temporary, meets } = scratch;
    synth.clear();
    times_current.clear();
    temporary.clear();

    batch_replay.load(seed_times, None);

    meets.clear();
    meets.extend(key.pending.iter().cloned());
    for i in (1..meets.len()).rev() {
        let m = meets[i].clone();
        meets[i - 1].meet_assign(&m);
    }

    let mut meet: Option<T> = None;
    update_meet(&mut meet, meets.first());
    update_meet(&mut meet, batch_replay.meet());

    // The merged (history ⊎ novel) run — replayed for its TIMES only (join base), never
    // accumulated. Output times likewise: base joins, never seeds.
    input_replay.load_iter(
        (key.i0..key.i1).map(|i| (key.p_in[i].0.1, key.p_in[i].1.clone(), key.p_in[i].2.clone())),
        meet.as_ref(),
    );
    output_replay.load(out_times, meet.as_ref());

    let mut times_slice = key.pending;
    let mut meets_slice = &meets[..];

    while let Some(next_time) = [batch_replay.time(), times_slice.first(), input_replay.time(), output_replay.time(), synth.last()]
        .into_iter()
        .flatten()
        .min()
        .cloned()
    {
        input_replay.step_while_time_is(&next_time);
        output_replay.step_while_time_is(&next_time);
        let mut interesting = batch_replay.step_while_time_is(&next_time);
        if interesting {
            if let Some(m) = meet.as_ref() {
                batch_replay.advance_buffer_by(m);
            }
        }
        while synth.last() == Some(&next_time) {
            times_current.push(synth.pop().expect("nonempty"));
            interesting = true;
        }
        while times_slice.first() == Some(&next_time) {
            times_current.push(times_slice[0].clone());
            times_slice = &times_slice[1..];
            meets_slice = &meets_slice[1..];
            interesting = true;
        }
        interesting = interesting || batch_replay.buffer().iter().any(|t| t.less_equal(&next_time));
        interesting = interesting || times_current.iter().any(|t| t.less_equal(&next_time));

        if !upper.less_equal(&next_time) {
            if interesting {
                // Synthesize joins against the input/output histories (times only — no
                // accumulation), then record `next_time` as an interesting moment.
                if let Some(m) = meet.as_ref() {
                    input_replay.advance_buffer_by(m);
                }
                for ((_, t), _) in input_replay.buffer().iter() {
                    if !t.less_equal(&next_time) {
                        temporary.push(next_time.join(t));
                    }
                }
                if let Some(m) = meet.as_ref() {
                    output_replay.advance_buffer_by(m);
                }
                for t in output_replay.buffer().iter() {
                    if !t.less_equal(&next_time) {
                        temporary.push(next_time.join(t));
                    }
                }
                moments.push(next_time.clone());
            }
            temporary.extend(batch_replay.buffer().iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
            temporary.extend(times_current.iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
            sort_dedup(temporary);
            let synth_len = synth.len();
            for time in temporary.drain(..) {
                if upper.less_equal(&time) {
                    pended.push(time);
                } else {
                    synth.push(time);
                }
            }
            if synth.len() > synth_len {
                synth.sort_by(|x, y| y.cmp(x));
                synth.dedup();
            }
        } else if interesting {
            pended.push(next_time.clone());
        }

        meet = None;
        update_meet(&mut meet, batch_replay.meet());
        update_meet(&mut meet, input_replay.meet());
        update_meet(&mut meet, output_replay.meet());
        for t in synth.iter() {
            update_meet(&mut meet, Some(t));
        }
        update_meet(&mut meet, meets_slice.first());
        if let Some(m) = meet.as_ref() {
            for t in times_current.iter_mut() {
                *t = t.join(m);
            }
        }
        sort_dedup(times_current);
    }
    sort_dedup(pended);
}

/// A resumable, fused determination-and-evaluation sweep over one key's times.
///
/// [`discover_times`] enumerates a key's interesting times up front, and the caller then walks them
/// again to evaluate. The conventional reduce does not: it runs ONE ascending pass and evaluates as
/// it discovers. It can, because discovery never looks backwards — every synthesized time is
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
pub struct Sweep<T, RIn, ROut> {
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
    pub fn new() -> Self {
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
    pub fn load(
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
    pub fn next_crossing(&mut self, upper: &Antichain<T>, pended: &mut Vec<T>) -> Option<T> {
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
    pub fn input_at(&self, at: &T, into: &mut Vec<(u64, RIn)>) {
        for ((id, time), diff) in self.input.buffer().iter().chain(self.novel.buffer().iter()) {
            if time.less_equal(at) { into.push((*id, diff.clone())); }
        }
        crate::consolidation::consolidate(into);
    }

    /// The tentative output accumulation at the suspended time, including this sweep's corrections.
    pub fn output_at(&self, at: &T, into: &mut Vec<(u64, ROut)>) {
        for ((id, time), diff) in self.output.buffer().iter().chain(self.produced.iter()) {
            if time.less_equal(at) { into.push((*id, diff.clone())); }
        }
        crate::consolidation::consolidate(into);
    }

    /// Record the corrections evaluated at the suspended time, and collapse them by the meet.
    pub fn commit(&mut self, at: &T, corrections: impl Iterator<Item = (u64, ROut)>) {
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

#[cfg(test)]
mod sweep_tests {

    use timely::order::Product;
    use timely::progress::Antichain;

    use super::{discover_times, DiscoverScratch, KeyView, Sweep};

    type Time = Product<u64, u64>;

    /// A deterministic LCG: enough variety without a dependency.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }
    }

    fn time(r: &mut Rng, span: u64) -> Time { Product::new(r.next(span), r.next(span)) }

    /// `Sweep` must never reach a time `discover_times` does not: determination is the schedule,
    /// and a crossing outside it would be work the coverage argument does not cover.
    ///
    /// The converse is NOT asserted, and must not be. The fused pass may legitimately explore FEWER
    /// times, because it reaches each one carrying live accumulations: a time whose updates have
    /// consolidated away by the time the sweep arrives needs no evaluation, where determination —
    /// which reads times only — has no way to know that. Pinning equality here would forbid the
    /// pruning that fusing the two passes exists to enable. Output equality against the conventional
    /// reduce is the real check, and lives in the dataflow tests.
    ///
    /// As it happens the two coincide today, since both consolidate their buffers identically and
    /// nothing is committed here; the containment is what is guaranteed.
    #[test]
    fn sweep_within_discover_times() {
        for seed in 0..400u64 {
            let mut rng = Rng(seed.wrapping_mul(2654435761).wrapping_add(12345));
            let span = 2 + rng.next(4);

            let novel: Vec<(u64, Time, i64)> =
                (0..rng.next(6)).map(|_| (rng.next(3), time(&mut rng, span), 1i64)).collect();
            let input: Vec<(u64, Time, i64)> =
                (0..rng.next(6)).map(|_| (rng.next(3), time(&mut rng, span), 1i64)).collect();
            let output: Vec<(u64, Time, i64)> =
                (0..rng.next(4)).map(|_| (rng.next(3), time(&mut rng, span), 1i64)).collect();
            let mut due: Vec<Time> = (0..rng.next(3)).map(|_| time(&mut rng, span)).collect();
            due.sort();
            due.dedup();
            let upper = Antichain::from_elem(Product::new(span, span));

            // Determination: `p_in` is the accumulated input, the seeds are the novel times.
            let p_in: Vec<((u64, u64), Time, i64)> =
                input.iter().map(|(v, t, d)| ((0u64, *v), *t, *d)).collect();
            let mut scratch: DiscoverScratch<Time, i64> = DiscoverScratch::new();
            let (mut moments, mut pended) = (Vec::new(), Vec::new());
            discover_times(
                KeyView { p_in: &p_in[..], i0: 0, i1: p_in.len(), pending: &due[..] },
                novel.iter().map(|(_, t, _)| *t),
                output.iter().map(|(_, t, _)| *t),
                &upper,
                &mut scratch,
                &mut moments,
                &mut pended,
            );

            // The fused sweep, committing nothing.
            let mut sweep: Sweep<Time, i64, i64> = Sweep::new();
            sweep.load(novel.iter().cloned(), input.iter().cloned(), output.iter().cloned(), &due[..]);
            let (mut crossings, mut sweep_pended) = (Vec::new(), Vec::new());
            while let Some(at) = sweep.next_crossing(&upper, &mut sweep_pended) {
                crossings.push(at);
            }
            crate::operators::reduce::sort_dedup(&mut sweep_pended);

            assert!(crossings.iter().all(|t| moments.contains(t)),
                    "seed {seed}: sweep reached a time outside the schedule: {crossings:?} vs {moments:?}");
            assert!(crossings.windows(2).all(|w| w[0] <= w[1]), "seed {seed}: crossings must ascend");
            assert!(sweep_pended.iter().all(|t| pended.contains(t)),
                    "seed {seed}: sweep pended a time outside the schedule");
        }
    }
}

/// One side's presentation for a whole window, flat, with per-key windows into it.
///
/// The records live in `((time, value_id), diff)` order per key, which is the order a sweep
/// consumes them, so the presentation IS the replay: there is no `EditList`, no permutation from
/// value order into time order, and no second copy of every time.
///
/// Each key owns a contiguous slice, split by two cursors:
///
/// ```text
///   [start ...... free) [free ...... head) [head ...... end)
///        reclaimed          accumulation        unreplayed
/// ```
///
/// Stepping a record in is `head += 1` — the record is already adjacent to the accumulation, so
/// nothing moves. Advancing the accumulation joins its times by the meet and consolidates it into
/// its own SUFFIX (`consolidate_slice_suffix`), which can only shrink it, so `free` moves forward
/// and `head` stays put. That is why the consolidation compacts backwards: compacting forwards
/// would leave the freed space between the survivors and the next record to step in.
///
/// `meets[i]` is the meet of the times at or after `i` within the key. It is read only at `head`,
/// on the unreplayed side, which is never reordered — so consolidating the accumulation is free to
/// scramble the meets it passes over.
pub struct Replay<T, R> {
    runs: Vec<((T, u64), R)>,
    meets: Vec<T>,
    free: Vec<usize>,
    head: Vec<usize>,
    end: Vec<usize>,
}

impl<T: Timestamp + Lattice, R: Semigroup> Replay<T, R> {
    /// An empty presentation, to be filled a key at a time.
    pub fn new() -> Self {
        Replay { runs: Vec::new(), meets: Vec::new(), free: Vec::new(), head: Vec::new(), end: Vec::new() }
    }

    /// Discard every key, keeping the allocations.
    pub fn clear(&mut self) {
        self.runs.clear();
        self.meets.clear();
        self.free.clear();
        self.head.clear();
        self.end.clear();
    }

    /// The number of keys loaded.
    pub fn keys(&self) -> usize { self.end.len() }

    /// Append one key's records, advancing each time by `advance_by` if supplied.
    ///
    /// The records are put into `(time, value_id)` order and netted; advancing can make distinct
    /// times coincide, so the netting is not redundant even on an already-consolidated presentation.
    /// The suffix meets are then a single backward pass.
    pub fn push_key(&mut self, records: impl Iterator<Item = (u64, T, R)>, advance_by: Option<&T>) {
        let start = self.runs.len();
        for (vid, mut time, diff) in records {
            if let Some(m) = advance_by { time.join_assign(m); }
            self.runs.push(((time, vid), diff));
        }
        let kept = crate::consolidation::consolidate_slice(&mut self.runs[start..]);
        self.runs.truncate(start + kept);

        self.meets.resize_with(self.runs.len(), || T::minimum());
        for i in start..self.runs.len() {
            self.meets[i].clone_from(&self.runs[i].0.0);
        }
        for i in (start + 1..self.runs.len()).rev() {
            let (init, tail) = self.meets.split_at_mut(i);
            init[i - 1].meet_assign(&tail[0]);
        }

        self.free.push(start);
        self.head.push(start);
        self.end.push(self.runs.len());
    }

    /// The next unreplayed time for key `k`, or `None` once it is drained.
    pub fn time(&self, k: usize) -> Option<&T> {
        (self.head[k] < self.end[k]).then(|| &self.runs[self.head[k]].0.0)
    }

    /// The meet of every unreplayed time for key `k`.
    pub fn meet(&self, k: usize) -> Option<&T> {
        (self.head[k] < self.end[k]).then(|| &self.meets[self.head[k]])
    }

    /// Step key `k`'s records while the next time equals `at`; true iff any did.
    ///
    /// Purely an index bump: the records are already where the accumulation wants them.
    pub fn step_while_time_is(&mut self, k: usize, at: &T) -> bool {
        let before = self.head[k];
        while self.head[k] < self.end[k] && &self.runs[self.head[k]].0.0 == at {
            self.head[k] += 1;
        }
        self.head[k] > before
    }

    /// Advance key `k`'s accumulation by `meet` and consolidate it, reclaiming what cancels.
    pub fn advance_active(&mut self, k: usize, meet: &T) {
        let (free, head) = (self.free[k], self.head[k]);
        if free == head { return; }
        for record in self.runs[free..head].iter_mut() { record.0.0.join_assign(meet); }
        let kept = crate::consolidation::consolidate_slice_suffix(&mut self.runs[free..head]);
        self.free[k] = free + kept;
    }

    /// Key `k`'s accumulation, as `(value_id, time, diff)`.
    pub fn active(&self, k: usize) -> impl Iterator<Item = (u64, &T, &R)> {
        self.runs[self.free[k]..self.head[k]].iter().map(|((t, v), d)| (*v, t, d))
    }
}

#[cfg(test)]
mod replay_tests {

    use timely::order::Product;

    use super::{Replay, ValueHistory};

    type Time = Product<u64, u64>;

    struct Rng(u64);
    impl Rng {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }
    }

    /// `Replay` must behave as `ValueHistory` does, which is the implementation it replaces: the
    /// same replay times and meets, and after each advance the same accumulation. Compared as
    /// multisets, since the two hold their accumulations in different orders — `Replay` by
    /// `(time, id)`, `ValueHistory` by `(id, time)` — and neither order is observable to a caller
    /// that consolidates what it reads.
    #[test]
    fn replay_matches_value_history() {
        let mut rng = Rng(0xfeed);
        for case in 0..500u64 {
            let span = 2 + rng.next(4);
            // As the bridge contract delivers it: consolidated, and grouped by value id. Feeding
            // unsorted records instead exposes a real difference — `Replay` nets the whole key,
            // while `load_iter` only nets within CONSECUTIVE value groups, so it retains records
            // `Replay` cancels away and reports replay times for them. `Replay` is the more compact
            // of the two there, but the two are only comparable on input both consolidate alike.
            let mut records: Vec<((u64, Time), i64)> = (0..rng.next(9))
                .map(|_| ((rng.next(3), Product::new(rng.next(span), rng.next(span))), rng.next(5) as i64 - 2))
                .collect();
            crate::consolidation::consolidate(&mut records);
            let records: Vec<(u64, Time, i64)> =
                records.into_iter().map(|((v, t), d)| (v, t, d)).collect();
            let meet = Product::new(rng.next(2), rng.next(2));

            let mut flat: Replay<Time, i64> = Replay::new();
            flat.push_key(records.iter().cloned(), Some(&meet));
            let mut history: ValueHistory<u64, Time, i64> = ValueHistory::new();
            history.load_iter(records.iter().cloned(), Some(&meet));

            let mut steps = 0;
            loop {
                let (a, b) = (flat.time(0).cloned(), history.time().cloned());
                assert_eq!(a, b, "case {case} step {steps}: next time");
                assert_eq!(flat.meet(0).cloned(), history.meet().cloned(), "case {case} step {steps}: meet");
                let Some(at) = a else { break };

                assert_eq!(
                    flat.step_while_time_is(0, &at),
                    history.step_while_time_is(&at),
                    "case {case} step {steps}: stepped",
                );
                let collapse = Product::new(rng.next(span), rng.next(span));
                flat.advance_active(0, &collapse);
                history.advance_buffer_by(&collapse);

                let mut got: Vec<(u64, Time, i64)> =
                    flat.active(0).map(|(v, t, d)| (v, *t, *d)).collect();
                let mut want: Vec<(u64, Time, i64)> =
                    history.buffer().iter().map(|((v, t), d)| (*v, *t, *d)).collect();
                got.sort();
                want.sort();
                assert_eq!(got, want, "case {case} step {steps}: accumulation");
                steps += 1;
            }
        }
    }
}
