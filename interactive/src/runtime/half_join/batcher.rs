//! The half join's batcher: arrivals wait in a merge ladder of chains in time order, and what
//! the frontier unblocks leaves the fronts of the chains as runs in identifier order.

use std::borrow::Cow;
use std::cmp::Ordering;

use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;

use differential_dataflow::batcher::Batcher;
use differential_dataflow::batcher::merge::{MergeLadder, Merger};

use corgi::arrange::{gather, gather_lanes, survey_groups, GroupRun};
use corgi::Value as CValue;

use crate::runtime::chunk::concat_blocks;
use crate::runtime::col_times::{ColTimes, Lanes};
use crate::ir::{Diff, Time};

use super::{consolidate_rows, gather_rows, Part, PartTimes, Run, CC};

/// The most rows in one [`Run`]. Small under test, so that units hold many runs.
const RUN: usize = if cfg!(test) { 4 } else { 1 << 16 };

/// The most rows a merge puts in one [`TimeChunk`]. Releasing part of a chunk recomputes its
/// lower bound, which costs its length. Small under test, so that chains hold many chunks.
const TIME_TARGET: usize = if cfg!(test) { 5 } else { 1 << 16 };

/// Updates in the total order on their times: rows `(t, key, val)`, where `t` is the update's own
/// time as a `Prod` of `Int` lanes, sorted in corgi's structural order and consolidated. The
/// structural order on the lanes is lexicographic, which is the total order on times.
pub(super) struct TimeChunk {
    /// `Prod([t, key, val])`.
    rows: CValue,
    diffs: Vec<Diff>,
    /// Rows before `start` have been released.
    start: usize,
    /// The minimal times among the rows from `start` on, in the partial order.
    lower: Antichain<Time>,
}

impl Default for TimeChunk {
    fn default() -> Self { TimeChunk { rows: CValue::Unit(0), diffs: Vec::new(), start: 0, lower: Antichain::new() } }
}

impl TimeChunk {
    fn new(rows: CValue, diffs: Vec<Diff>) -> Self {
        let mut chunk = TimeChunk { rows, diffs, start: 0, lower: Antichain::new() };
        chunk.set_lower();
        chunk
    }

    /// Sorts and consolidates rows `(t, key, val)`.
    fn form(rows: CValue, diffs: Vec<Diff>) -> Option<Self> {
        let (keep, kept) = consolidate_rows(&rows, &diffs);
        if kept.is_empty() { return None; }
        Some(TimeChunk::new(gather(&rows, &keep), kept))
    }

    /// The number of rows not yet released.
    fn len(&self) -> usize { self.diffs.len() - self.start }

    /// The time lanes, as integers.
    fn lanes(&self) -> Vec<Cow<'_, [i64]>> { time_lanes(&self.rows) }

    /// Recompute `lower` from the rows not yet released. Equal times are adjacent, so each distinct
    /// time is offered once.
    fn set_lower(&mut self) {
        let lanes = self.lanes();
        let mut lower = Antichain::new();
        for i in self.start..self.diffs.len() {
            if i > self.start && lanes.iter().all(|lane| lane[i - 1] == lane[i]) { continue; }
            lower.insert(Time::from_coordinates(lanes.iter().map(|lane| lane[i] as u64)));
        }
        drop(lanes);
        self.lower = lower;
    }

    /// The end of the rows `bound` unblocks: those before the bound in the total order, and those
    /// equal to it when `strict`. No bound unblocks every row.
    fn unblocked(&self, bound: Option<&[i64]>, strict: bool) -> usize {
        let Some(bound) = bound else { return self.diffs.len() };
        let lanes = self.lanes();
        let (mut lo, mut hi) = (self.start, self.diffs.len());
        while lo < hi {
            let mid = (lo + hi) / 2;
            if unblocked_at(&lanes, mid, bound, strict) { lo = mid + 1 } else { hi = mid }
        }
        lo
    }

    /// Release the rows up to `end`, which no longer count towards `lower`.
    fn release_to(&mut self, end: usize) -> TimeChunk {
        let rows: Vec<usize> = (self.start..end).collect();
        let released = TimeChunk { rows: gather(&self.rows, &rows), diffs: self.diffs[self.start..end].to_vec(), start: 0, lower: Antichain::new() };
        self.start = end;
        self.set_lower();
        released
    }
}

/// Staged updates as rows `(t, key, val)`, their times written as `width` lanes, and their diffs.
fn stage_rows(stage: Vec<CC>, width: usize) -> Option<(CValue, Vec<Diff>)> {
    if stage.is_empty() { return None; }
    assert!(stage.iter().all(|c| c.times.width() <= width), "half_join: a time wider than its scope");
    let keys = concat_blocks(&stage.iter().map(|c| c.keys.clone()).collect::<Vec<_>>());
    let vals = concat_blocks(&stage.iter().map(|c| c.vals.clone()).collect::<Vec<_>>());
    let lanes = (0..width).map(|k| CValue::i64(stage.iter().flat_map(|c| c.times.lane(k)).map(|x| x as i64).collect())).collect();
    let diffs: Vec<Diff> = stage.iter().flat_map(|c| c.diffs.iter().copied()).collect();
    Some((CValue::Prod(vec![CValue::Prod(lanes), keys, vals]), diffs))
}

/// The time lanes of rows `(t, key, val)`, as integers.
fn time_lanes(rows: &CValue) -> Vec<Cow<'_, [i64]>> {
    let CValue::Prod(fields) = rows else { panic!("half_join: rows that are not (t, key, val)") };
    let CValue::Prod(lanes) = &fields[0] else { panic!("half_join: a time that is not a tuple") };
    lanes.iter().map(|lane| lane.as_i64("half_join time lane").unwrap()).collect()
}

/// Whether row `i`'s time, as `lanes`, is unblocked by `bound`: before it in the total order, or
/// equal to it when `strict`.
fn unblocked_at(lanes: &[Cow<'_, [i64]>], i: usize, bound: &[i64], strict: bool) -> bool {
    match lanes.iter().zip(bound).map(|(lane, b)| lane[i].cmp(b)).find(|o| o.is_ne()).unwrap_or(Ordering::Equal) {
        Ordering::Less => true,
        Ordering::Equal => strict,
        Ordering::Greater => false,
    }
}

/// Rows `start..` of each part `(rows, start, diffs)`, in order, as one column and their diffs.
fn concat_parts(parts: &[(&CValue, usize, &[Diff])]) -> (CValue, Vec<Diff>) {
    if let [(rows, 0, diffs)] = parts { return ((*rows).clone(), diffs.to_vec()); }
    let sources: Vec<Option<&CValue>> = parts.iter().map(|p| Some(p.0)).collect();
    let (mut tags, mut offs, mut diffs) = (Vec::new(), Vec::new(), Vec::new());
    for (tag, (_, start, part)) in parts.iter().enumerate() {
        tags.resize(tags.len() + (part.len() - start), tag);
        offs.extend(*start..part.len());
        diffs.extend_from_slice(&part[*start..]);
    }
    (gather_lanes(&sources, &tags, &offs), diffs)
}

/// The unreleased rows of `chunks`, in order, as one column and their diffs.
fn live_rows(chunks: &[TimeChunk]) -> (CValue, Vec<Diff>) {
    concat_parts(&chunks.iter().map(|c| (&c.rows, c.start, &c.diffs[..])).collect::<Vec<_>>())
}

/// Merges chains of [`TimeChunk`]s, consolidating equal rows.
#[derive(Default)]
pub(super) struct TimeMerger;

impl Merger for TimeMerger {
    type Chunk = TimeChunk;

    fn merge(&mut self, list1: Vec<TimeChunk>, list2: Vec<TimeChunk>, output: &mut Vec<TimeChunk>, _stash: &mut Vec<TimeChunk>) {
        let (a, ad) = live_rows(&list1);
        let (b, bd) = live_rows(&list2);
        let (mut tags, mut offs, mut diffs) = (Vec::new(), Vec::new(), Vec::new());
        for run in survey_groups(&a, &b) {
            match run {
                GroupRun::A(lo, hi) => {
                    tags.resize(tags.len() + (hi - lo), 0);
                    offs.extend(lo..hi);
                    diffs.extend_from_slice(&ad[lo..hi]);
                }
                GroupRun::B(lo, hi) => {
                    tags.resize(tags.len() + (hi - lo), 1);
                    offs.extend(lo..hi);
                    diffs.extend_from_slice(&bd[lo..hi]);
                }
                GroupRun::Both(alo, ahi, blo, bhi) => {
                    let d: Diff = ad[alo..ahi].iter().chain(&bd[blo..bhi]).sum();
                    if d != 0 {
                        tags.push(0);
                        offs.push(alo);
                        diffs.push(d);
                    }
                }
            }
        }
        for lo in (0..diffs.len()).step_by(TIME_TARGET) {
            let hi = (lo + TIME_TARGET).min(diffs.len());
            output.push(TimeChunk::new(gather_lanes(&[Some(&a), Some(&b)], &tags[lo..hi], &offs[lo..hi]), diffs[lo..hi].to_vec()));
        }
    }

    fn len(chunk: &TimeChunk) -> usize { chunk.len() }
}

/// Stages a lifted stream's updates and releases those the arrangement's frontier has unblocked.
///
/// An update at `t` is unblocked once no arrangement time that precedes it in the total order can
/// still arrive: the frontier's least element `b` bounds every time yet to come, so `t ≤ b` suffices
/// when only strictly earlier times meet `t`, and `t < b` when equal ones do too. An empty frontier
/// unblocks everything.
///
/// Updates wait in a merge ladder of chains in time order, so what a frontier releases from each
/// chain is a prefix: whole chunks from its front, and part of at most one more. Arrivals the
/// frontier already unblocks skip the ladder. What one extraction releases is put in identifier
/// order, ready to meet the arrangement in its order, and consolidated within each window it is
/// released in (see [`Run`]); nothing is held back to consolidate more.
pub(crate) struct HalfJoinBatcher {
    strict: bool,
    /// The number of lanes in a time: the scope depth, plus one.
    width: usize,
    /// Arrivals not yet sorted.
    stage: Vec<CC>,
    ladder: MergeLadder<TimeMerger>,
    /// The minimal times among retained updates.
    lower: Antichain<Time>,
}

impl HalfJoinBatcher {
    pub(crate) fn new(strict: bool, width: usize) -> Self {
        HalfJoinBatcher { strict, width, stage: Vec::new(), ladder: MergeLadder::new(None, 0), lower: Antichain::new() }
    }
}

impl Batcher<CC> for HalfJoinBatcher {
    type Time = Time;
    type Output = Vec<Run>;

    fn insert(&mut self, container: &mut CC) {
        if !container.times.is_empty() {
            self.stage.push(std::mem::take(container));
        }
    }

    fn extract<'a>(&'a mut self, frontier: AntichainRef<'_, Time>) -> (Option<Vec<Run>>, AntichainRef<'a, Time>) {
        let width = self.width;
        let least = frontier.iter().min();
        let bound: Option<Vec<i64>> = least.map(|b| {
            let mut lanes: Vec<i64> = b.coordinates().map(|x| x as i64).collect();
            assert!(lanes.len() <= width, "half_join: a frontier wider than its scope");
            lanes.resize(width, 0);
            lanes
        });
        let bound_col: Option<ColTimes<Time>> = least.map(|b| std::iter::once(b.clone()).collect());
        let strict = self.strict;

        // Arrivals the bound already unblocks are released without entering the ladder.
        let (mut staged, mut blocked) = (Vec::new(), Vec::new());
        for c in std::mem::take(&mut self.stage) {
            let Some(b) = &bound_col else { staged.push(c); continue };
            let (now, later): (Vec<usize>, Vec<usize>) = (0..c.times.len()).partition(|&i| match c.times.cmp_cross(i, b, 0) {
                Ordering::Less => true,
                Ordering::Equal => strict,
                Ordering::Greater => false,
            });
            if later.is_empty() { staged.push(c); }
            else if now.is_empty() { blocked.push(c); }
            else {
                staged.push(gather_rows(&c, &now));
                blocked.push(gather_rows(&c, &later));
            }
        }
        if let Some(chunk) = stage_rows(blocked, width).and_then(|(rows, diffs)| TimeChunk::form(rows, diffs)) {
            self.ladder.insert_chain(vec![chunk]);
        }

        let mut released = Vec::new();
        self.ladder.retain_chains(|chain| {
            let whole = chain.iter().take_while(|c| c.unblocked(bound.as_deref(), strict) == c.diffs.len()).count();
            released.extend(chain.drain(..whole));
            if let Some(front) = chain.first_mut() {
                let end = front.unblocked(bound.as_deref(), strict);
                if end > front.start { released.push(front.release_to(end)); }
            }
        });
        self.lower = Antichain::new();
        for chunk in self.ladder.chains().flatten() {
            for t in chunk.lower.elements() { self.lower.insert(t.clone()); }
        }
        let released = (!released.is_empty() || !staged.is_empty()).then(|| into_runs(released, staged, width));
        (released, self.lower.borrow())
    }
}

/// Released rows, from chunks and from arrivals released on arrival, as runs in identifier order.
/// Each source is dropped once its runs are formed.
fn into_runs(released: Vec<TimeChunk>, staged: Vec<CC>, width: usize) -> Vec<Run> {
    let mut runs = Vec::new();
    for chunk in released {
        let CValue::Prod(fields) = &chunk.rows else { panic!("half_join: a time chunk that is not (t, key, val)") };
        Part { keys: fields[1].clone(), vals: &fields[2], times: PartTimes::Lanes(chunk.lanes()), diffs: &chunk.diffs, rows: chunk.start..chunk.diffs.len() }.runs(RUN, width, &mut runs);
    }
    for c in staged {
        Part { keys: c.keys.clone(), vals: &c.vals, times: PartTimes::Col(&c.times), diffs: &c.diffs, rows: 0..c.diffs.len() }.runs(RUN, width, &mut runs);
    }
    runs
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::test_util::*;
    use crate::runtime::chunk::{ident, recover_key};
    use crate::runtime::container::CorgiContainer;

    /// The batcher releases what the frontier's least element unblocks, and bounds what it keeps.
    #[test]
    fn releases_by_the_least_frontier_element() {
        let released = |strict: bool, frontier: &[Time]| -> (Vec<Time>, Vec<Time>) {
            let mut batcher = HalfJoinBatcher::new(strict, 2);
            let times = [t(1, 0), t(0, 5), t(0, 2)];
            batcher.insert(&mut container(&[1, 2, 3], &[0, 0, 0], &[(0, 0); 3], &times, &[1, 1, 1]));
            let frontier: Antichain<Time> = frontier.iter().cloned().collect();
            let (out, lower) = batcher.extract(frontier.borrow());
            let mut out: Vec<Time> = out.unwrap_or_default().iter().flat_map(|c| c.times().to_vec()).collect();
            out.sort();
            let mut lower = lower.to_vec();
            lower.sort();
            (out, lower)
        };
        assert_eq!(released(true, &[t(1, 0)]), (vec![t(0, 2), t(0, 5), t(1, 0)], vec![]));
        assert_eq!(released(false, &[t(1, 0)]), (vec![t(0, 2), t(0, 5)], vec![t(1, 0)]));
        // (0, [5]) and (1, [0]) are incomparable, so both bound what is kept.
        assert_eq!(released(false, &[t(0, 3), t(2, 0)]), (vec![t(0, 2)], vec![t(0, 5), t(1, 0)]));
        assert_eq!(released(true, &[]), (vec![t(0, 2), t(0, 5), t(1, 0)], vec![]));
    }

    /// Arrivals the frontier already unblocks leave in the extraction they arrive for, nothing held
    /// back, and each release comes out consolidated within itself: equal rows merged, rows that
    /// cancel gone. Keys are tuples, so they are identified by a hash; consolidation compares the
    /// real keys, so distinct keys stay apart.
    #[test]
    fn a_release_is_consolidated_within_itself() {
        let keys = |ks: &[(i64, i64)]| CValue::Prod(vec![ints(&ks.iter().map(|k| k.0).collect::<Vec<_>>()), ints(&ks.iter().map(|k| k.1).collect::<Vec<_>>())]);
        let arrival = |ks: &[(i64, i64)], vals: &[i64], times: &[Time], diffs: &[Diff]| CorgiContainer {
            keys: keys(ks),
            vals: CValue::Prod(vec![ints(vals), lifted(&vec![(0, 0); vals.len()])]),
            times: times.iter().cloned().collect(),
            diffs: diffs.to_vec(),
        };
        let row = |k: (i64, i64), v: i64| (tup(vec![int(k.0), int(k.1)]), tup(vec![int(v), tup(vec![int(0), int(0)])]));
        let updates = |runs: &[Run]| -> Vec<_> {
            runs.iter().flat_map(|run| CorgiContainer { keys: recover_key(run.keys()), vals: run.vals().clone(), times: run.times().clone(), diffs: run.diffs().to_vec() }.into_updates()).collect()
        };
        for strict in [false, true] {
            let mut batcher = HalfJoinBatcher::new(strict, 2);
            // One arrival, all of it unblocked and within one window of `RUN` rows: (1, 1) repeats,
            // (2, 1) cancels.
            batcher.insert(&mut arrival(&[(1, 1), (2, 1), (1, 1), (2, 1)], &[5, 7, 5, 7], &vec![t(0, 1); 4], &[1, 1, 2, -1]));
            let (out, lower) = batcher.extract(Antichain::from_elem(t(1, 0)).borrow());
            assert!(lower.is_empty(), "strict {strict}: nothing unblocked is held back");
            let got = updates(&out.expect("an unblocked arrival is released at once"));
            assert_eq!(got, vec![(row((1, 1), 5), t(0, 1), 3)], "strict {strict}: one row per (key, val, time), cancelling rows gone");

            // A later arrival repeating a row is released in its own extraction, not held to merge.
            batcher.insert(&mut arrival(&[(1, 1)], &[5], &[t(0, 1)], &[1]));
            let (later, later_lower) = batcher.extract(Antichain::from_elem(t(1, 0)).borrow());
            assert!(later_lower.is_empty());
            assert_eq!(updates(&later.unwrap()), vec![(row((1, 1), 5), t(0, 1), 1)]);
        }
    }

    /// Against a model that holds each update until the frontier unblocks it: over rounds of random
    /// arrivals, some already unblocked, and an advancing frontier, each extraction releases exactly
    /// the unblocked updates, in identifier order, and reports a bound that covers every update it
    /// keeps and holds only times it keeps.
    #[test]
    fn releases_what_the_model_releases() {
        use std::collections::BTreeMap;
        for strict in [false, true] {
            for seed in 0..40 {
                let mut rng = Rng(seed);
                let mut batcher = HalfJoinBatcher::new(strict, 2);
                let mut pending: BTreeMap<(Time, i64, i64), Diff> = BTreeMap::new();
                for round in 0..14u64 {
                    for _ in 0..rng.below(4) {
                        let n = rng.below(9) as usize;
                        let keys: Vec<i64> = (0..n).map(|_| rng.below(4) as i64).collect();
                        let vals: Vec<i64> = (0..n).map(|_| rng.below(3) as i64).collect();
                        let times: Vec<Time> = (0..n).map(|_| t(round / 3 + rng.below(3), rng.below(3))).collect();
                        let diffs: Vec<Diff> = (0..n).map(|_| rng.diff()).collect();
                        for i in 0..n { *pending.entry((times[i].clone(), keys[i], vals[i])).or_default() += diffs[i]; }
                        batcher.insert(&mut container(&keys, &vals, &vec![(0, 0); n], &times, &diffs));
                    }
                    let frontier: Antichain<Time> = if round == 13 { Antichain::new() } else { [t(round / 3, round % 3), t(round / 3 + 1, 0)].into_iter().collect() };
                    let bound = frontier.iter().min().cloned();
                    let unblocked = |time: &Time| bound.as_ref().is_none_or(|b| time < b || (strict && time == b));

                    let (out, lower) = batcher.extract(frontier.borrow());
                    let lower = lower.to_owned();
                    let mut got = Vec::new();
                    for chunk in out.unwrap_or_default() {
                        assert!(ident(chunk.keys()).windows(2).all(|w| w[0] <= w[1]), "released updates are in identifier order");
                        let c = CorgiContainer { keys: recover_key(chunk.keys()), vals: chunk.vals().clone(), times: chunk.times().clone(), diffs: chunk.diffs().to_vec() };
                        got.extend(c.into_updates());
                    }
                    let released: Vec<_> = pending.keys().filter(|(time, _, _)| unblocked(time)).cloned().collect();
                    let mut expect = Vec::new();
                    for entry in released {
                        let d = pending.remove(&entry).unwrap();
                        let (time, k, v) = entry;
                        if d != 0 { expect.push(((int(k), tup(vec![int(v), tup(vec![int(0), int(0)])])), time, d)); }
                    }
                    assert_eq!(consolidated(got), consolidated(expect), "seed {seed}, round {round}, strict {strict}: the released updates");
                    for ((time, _, _), d) in pending.iter() {
                        assert!(*d == 0 || lower.less_equal(time), "seed {seed}, round {round}: the bound covers a kept update at {time:?}");
                    }
                    for b in lower.elements() {
                        assert!(pending.keys().any(|(time, _, _)| time == b), "seed {seed}, round {round}: the bound holds {b:?}, which no kept update has");
                    }
                }
                assert!(pending.values().all(|&d| d == 0), "an empty frontier releases everything");
            }
        }
    }
}
