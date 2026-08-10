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
    /// Seeds stepped in so far, compacted by the running meet: the `∃ nu ∈ novel, nu ≤ x` witnesses.
    times_current: Vec<T>,
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

impl<T: Timestamp + Lattice, RIn: Semigroup + Clone, ROut: Semigroup + Clone> Sweep<T, RIn, ROut> {
    /// An empty sweep, to be `load`ed. Reuse one per key rather than allocating per key.
    pub fn new() -> Self {
        Sweep {
            novel: ValueHistory::new(), input: ValueHistory::new(), output: ValueHistory::new(),
            due: Vec::new(), due_meets: Vec::new(), due_pos: 0,
            synth: Vec::new(), times_current: Vec::new(), temporary: Vec::new(),
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
        self.times_current.clear();
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
            if self.suspended {
                self.suspended = false;
                self.settle();
            }
            let next_time = [
                self.novel.time(), self.due.get(self.due_pos), self.input.time(),
                self.output.time(), self.synth.last(),
            ].into_iter().flatten().min().cloned()?;

            self.input.step_while_time_is(&next_time);
            self.output.step_while_time_is(&next_time);

            // CLAUSE TWO of `round_coverage`, first half: a time is reached only via a seed — a
            // novel update, a due time, or a synthetic join of earlier reached times.
            let mut interesting = self.novel.step_while_time_is(&next_time);
            if interesting {
                if let Some(meet) = self.meet.as_ref() { self.novel.advance_buffer_by(meet); }
            }
            while self.synth.last() == Some(&next_time) {
                self.times_current.push(self.synth.pop().expect("nonempty"));
                interesting = true;
            }
            while self.due.get(self.due_pos) == Some(&next_time) {
                self.times_current.push(next_time.clone());
                self.due_pos += 1;
                interesting = true;
            }
            // CLAUSE TWO, second half: absorption. A time at or above a seed already stepped in is
            // itself reached, because joining that seed with it yields it back.
            interesting = interesting
                || self.novel.buffer().iter().any(|((_, t), _)| t.less_equal(&next_time))
                || self.times_current.iter().any(|t| t.less_equal(&next_time));

            if !upper.less_equal(&next_time) {
                // CLAUSE ONE: close under joins. Against the NOVEL times unconditionally — even an
                // unreached time joined with a novel time lands above a novel time, so it is on the
                // schedule. Against the PRIOR times only when this time is itself reached, since it
                // then carries the novel witness; a join of two prior times carries none and is
                // deliberately never produced.
                self.temporary.extend(self.novel.buffer().iter().map(|((_, t), _)| t)
                    .filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                self.temporary.extend(self.times_current.iter()
                    .filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                if interesting {
                    if let Some(meet) = self.meet.as_ref() {
                        self.input.advance_buffer_by(meet);
                        self.output.advance_buffer_by(meet);
                    }
                    self.temporary.extend(self.input.buffer().iter().map(|((_, t), _)| t)
                        .filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                    self.temporary.extend(self.output.buffer().iter().map(|((_, t), _)| t)
                        .filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                    self.temporary.extend(self.produced.iter().map(|((_, t), _)| t)
                        .filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                }
                sort_dedup(&mut self.temporary);
                let synth_len = self.synth.len();
                for time in self.temporary.drain(..) {
                    if upper.less_equal(&time) { pended.push(time); } else { self.synth.push(time); }
                }
                if self.synth.len() > synth_len {
                    self.synth.sort_by(|x, y| y.cmp(x));
                    self.synth.dedup();
                }

                if interesting {
                    // The suspension point: exactly where the conventional operator calls logic.
                    self.suspended = true;
                    return Some(next_time);
                }
            }
            else if interesting {
                pended.push(next_time.clone());
            }
            self.settle();
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

    /// Close a step: recompute the meet of everything still to come, and compact the reached seeds
    /// by it. This is what keeps a key with a long history linear rather than quadratic.
    fn settle(&mut self) {
        let mut meet: Option<T> = None;
        update_meet(&mut meet, self.novel.meet());
        update_meet(&mut meet, self.input.meet());
        update_meet(&mut meet, self.output.meet());
        for time in self.synth.iter() { update_meet(&mut meet, Some(time)); }
        update_meet(&mut meet, self.due_meets.get(self.due_pos));
        if let Some(m) = meet.as_ref() {
            for time in self.times_current.iter_mut() { *time = time.join(m); }
        }
        sort_dedup(&mut self.times_current);
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
