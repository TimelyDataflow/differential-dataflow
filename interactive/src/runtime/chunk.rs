//! `CorgiChunk`: a [`Chunk`](differential_dataflow::trace::chunk::Chunk) whose key/val payload is a
//! pair of corgi columns, with per-tuple times held in adaptive [`ColTimes`] (no per-row `PointStamp`);
//! diffs use constants, byte dictionaries, or dense values too.
//! It is a **`Chunk` but NOT `NavigableChunk`**:
//! it exposes no `Ord` key, no cursor, no sorted-trie layout — the merge/advance/settle transducers
//! drive everything through corgi's own structural order (`compare_at`) and gather primitives
//! (`gather`, `gather_lanes`). Consumption is the tactics' job (they read the columns in bulk), which
//! is exactly why cursor-less `Chunk` suffices here.
//!
//! This is a faithful port of the reference [`VecChunk`](differential_dataflow::trace::chunk::vec):
//! same resumable merge→advance→settle pipeline and grade-at-yield invariant, with the flat
//! `Rc<Vec<row>>` swapped for corgi columns. Adopting the `Chunk` framework gives us the fueled,
//! graded `ChunkBatchMerger` for free.
//!
//! Order: `(key, val)` by corgi structural order (`compare_at` over `Prod([keys, vals])`), then `time`
//! by `Ord`. Any consistent total order is fine — correctness compares multisets, not DDIR's `Ord`.
//!
//! Simplification vs `VecChunk`: `merge` processes only the two front chunks per call (no mid-merge
//! refill), so `gather_lanes` source indices stay valid for the whole call. The `Chunk` contract
//! permits this — "consume at least one input; the harness may re-invoke."

use std::collections::VecDeque;
use std::rc::Rc;

use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::trace::chunk::{pack, Chunk, ChunkBatch};

use corgi::arrange::{equal_idx, gather, gather_lanes, group_bounds, sort_blocks, survey_groups, GroupRun};
use corgi::Value as CValue;

use crate::runtime::col_times::{ColTime, ColTimes};
use crate::runtime::col_diffs::ColDiffs;

use std::cmp::Ordering;

/// The chunk size the merge and the advance emit, and the unit the fueled merger yields on.
/// Larger than `VecChunk`'s 8192 to amortize corgi's per-chunk columnar set-up (each chunk
/// boundary costs a `gather` materialization). Swept at 1M nodes with `INGEST` at 2^24: 2^18 to
/// 2^20 takes ast's initial epoch 6.98 to 5.46 s and kcore's 4.27 to 3.05 with churn epochs flat;
/// 2^22 is within noise of that; 2^24 costs a merge-heavy churn epoch 11% (ast 2.42 -> 2.69 s).
const TARGET: usize = 1 << 20;

/// How many rows the chunker accumulates before sorting them into one chunk: the ingest bundle.
/// A separate knob from `TARGET`, since a radix sort of a big bundle is cheaper than merging its
/// pieces while the merger's granularity is `TARGET`'s to set. On its own, 2^18 to 2^22 at 1M
/// nodes takes kcore's initial epoch 4.27 to 3.42 s and scc's 22.99 to 22.16 with churn flat;
/// 2^24 sorts the whole of a large epoch's input at once.
const INGEST: usize = 1 << 24;

/// Shared, immutable chunk contents. `Clone` of a `CorgiChunk` is an `Rc` bump.
///
/// Same payload as [`CorgiContainer`](crate::runtime::container::CorgiContainer), and the two
/// should eventually be ONE type: both hold times as a `ColTimes`, and they differ only in
/// invariants (sorted+consolidated+shared here, raw+owned there).
struct Inner<T, R> {
    /// Key column (corgi), aligned with `vals`/`times`/`diffs`, sorted by `(key, val, time)`.
    keys: CValue,
    /// Val column (corgi).
    vals: CValue,
    /// Per-update times, as constant, indexed, or dense integer lanes (the lattice algebra lives here; corgi never sees time).
    times: ColTimes<T>,
    /// Per-update diffs.
    diffs: ColDiffs<R>,
}

/// A sorted, consolidated run of `((key, val), time, diff)` with corgi-columnar key/val, shared via `Rc`.
pub struct CorgiChunk<T, R>(Rc<Inner<T, R>>);

impl<T, R> Clone for CorgiChunk<T, R> {
    fn clone(&self) -> Self { CorgiChunk(Rc::clone(&self.0)) }
}

impl<T, R> Default for CorgiChunk<T, R> {
    fn default() -> Self {
        CorgiChunk(Rc::new(Inner { keys: CValue::Unit(0), vals: CValue::Unit(0), times: ColTimes::default(), diffs: ColDiffs::new() }))
    }
}

/// Split a `Prod([keys, vals])` corgi value into its two columns.
fn split_kv(kv: CValue) -> (CValue, CValue) {
    let mut cols = kv.into_prod("corgi chunk kv").unwrap();
    let vals = cols.pop().unwrap();
    let keys = cols.pop().unwrap();
    (keys, vals)
}

impl<T, R: Clone + PartialEq> CorgiChunk<T, R> {
    fn from_parts(keys: CValue, vals: CValue, mut times: ColTimes<T>, mut diffs: ColDiffs<R>) -> Self {
        assert_eq!(times.len(), diffs.len());
        times.seal();
        diffs.compact();
        CorgiChunk(Rc::new(Inner { keys, vals, times, diffs }))
    }
    /// The `(key, val)` sort payload as one corgi `Prod` column (cheap `Arc` bumps).
    fn kv(&self) -> CValue { CValue::Prod(vec![self.0.keys.clone(), self.0.vals.clone()]) }
    fn from_kv(kv: CValue, times: ColTimes<T>, diffs: ColDiffs<R>) -> Self {
        let (keys, vals) = split_kv(kv);
        Self::from_parts(keys, vals, times, diffs)
    }
    pub(crate) fn keys(&self) -> &CValue { &self.0.keys }
    pub(crate) fn vals(&self) -> &CValue { &self.0.vals }
    pub fn times(&self) -> &ColTimes<T> { &self.0.times }
    pub(crate) fn same_storage(&self, other: &Self) -> bool { Rc::ptr_eq(&self.0, &other.0) }
    pub(crate) fn diffs(&self) -> &ColDiffs<R> { &self.0.diffs }
}

impl<T, R> CorgiChunk<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + PartialEq + 'static,
{
    /// Materialize `[start, end)` of accumulated `(tag → src, off, time, diff)` into `TARGET`-sized
    /// output chunks. `srcs` are the (stable) source kv columns the tags/offs index into.
    fn emit(
        srcs: &[Option<&CValue>],
        tags: &[usize],
        offs: &[usize],
        times: ColTimes<T>,
        diffs: ColDiffs<R>,
        out: &mut VecDeque<Self>,
    ) {
        let n = times.len();
        if n <= TARGET {
            if n != 0 {
                let kv = gather_lanes(srcs, tags, offs);
                out.push_back(Self::from_kv(kv, times, diffs));
            }
            return;
        }
        let mut s = 0;
        while s < n {
            let e = (s + TARGET).min(n);
            let kv = gather_lanes(srcs, &tags[s..e], &offs[s..e]);
            let mut t = ColTimes::new();
            t.push_range(&times, s, e);
            out.push_back(Self::from_kv(kv, t, diffs.slice(s, e)));
            s = e;
        }
    }

    /// Concatenate a run of (globally-sorted) chunks into one combined `(kv, times, diffs)`.
    fn concat(chunks: &[Self]) -> (CValue, ColTimes<T>, ColDiffs<R>) {
        if let [chunk] = chunks { return (chunk.kv(), chunk.times().clone(), chunk.diffs().clone()); }
        let kvs: Vec<CValue> = chunks.iter().map(Self::kv).collect();
        let srcs: Vec<Option<&CValue>> = kvs.iter().map(Some).collect();
        let total: usize = chunks.iter().map(Self::len_).sum();
        let (mut tags, mut offs) = (Vec::with_capacity(total), Vec::with_capacity(total));
        let (mut times, mut diffs) = (ColTimes::new(), ColDiffs::new());
        for (ti, ch) in chunks.iter().enumerate() {
            tags.resize(tags.len() + ch.len_(), ti);
            offs.extend(0..ch.len_());
            times.push_range(ch.times(), 0, ch.len_());
            diffs.push_range(ch.diffs(), 0, ch.len_());
        }
        let kv = if total == 0 { CValue::Unit(0) } else { gather_lanes(&srcs, &tags, &offs) };
        (kv, times, diffs)
    }

    fn emit_columns(kv: CValue, times: ColTimes<T>, diffs: ColDiffs<R>, out: &mut VecDeque<Self>) {
        let n = times.len();
        if n == kv.len() && n <= TARGET {
            if n != 0 { out.push_back(Self::from_kv(kv, times, diffs)); }
        } else {
            for start in (0..n).step_by(TARGET) {
                let stop = (start + TARGET).min(n);
                let rows: Vec<_> = (start..stop).collect();
                out.push_back(Self::from_kv(gather(&kv, &rows), times.slice(start, stop), diffs.slice(start, stop)));
            }
        }
    }

    fn len_(&self) -> usize { self.0.times.len() }
}

impl<T, R> Chunk for CorgiChunk<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + PartialEq + 'static,
{
    type Time = T;
    const TARGET: usize = TARGET;

    fn len(&self) -> usize { self.0.times.len() }

    /// Merge of the two front chunks through their shared horizon, FULLY consolidating equal
    /// `(key, val, time)` triples and pushing back the survivor's suffix (the fueled-merger
    /// contract). Batched: corgi's `survey_groups` reports the interleaving of the two `(key, val)`
    /// columns as maximal ranges exclusive to one side and equal classes as their ranges on BOTH
    /// sides, so rows only one side holds are copied by range, and only the classes both sides
    /// hold — where consolidation can happen — are merged row by row, on their times, which corgi
    /// does not own. The shape is walked once per level per chunk, never once per row.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let c1 = in1.pop_front().unwrap();
        let c2 = in2.pop_front().unwrap();
        let (kv1, kv2) = (c1.kv(), c2.kv());
        let (n1, n2) = (c1.len_(), c2.len_());
        let (t1, d1) = (c1.times(), c1.diffs());
        let (t2, d2) = (c2.times(), c2.diffs());

        let runs = survey_groups(&kv1, &kv2);
        let (mut tags, mut offs) = (Vec::with_capacity(n1 + n2), Vec::with_capacity(n1 + n2));
        let mut diffs = Vec::with_capacity(n1 + n2);
        // Where the survivor's pushed-back suffix starts: the last run, if it is exclusive.
        let (mut p1, mut p2) = (n1, n2);
        let copy = |tags: &mut Vec<usize>, offs: &mut Vec<usize>, diffs: &mut Vec<R>, side: usize, lo: usize, hi: usize| {
            let d = if side == 0 { d1 } else { d2 };
            tags.resize(tags.len() + (hi - lo), side);
            offs.extend(lo..hi);
            d.extend_dense(lo, hi, diffs);
        };
        let common_order = (t1.is_constant() && t2.is_constant()).then(|| t1.cmp_cross(0, t2, 0));
        for (r, run) in runs.iter().enumerate() {
            let last = r + 1 == runs.len();
            match *run {
                GroupRun::A(lo, hi) => {
                    if last { p1 = lo; } else { copy(&mut tags, &mut offs, &mut diffs, 0, lo, hi); }
                }
                GroupRun::B(lo, hi) => {
                    if last { p2 = lo; } else { copy(&mut tags, &mut offs, &mut diffs, 1, lo, hi); }
                }
                GroupRun::Both(a_lo, a_hi, b_lo, b_hi) => {
                    // Both classes hold one `(key, val)`, sorted by time: merge on time, summing the
                    // diffs of equal times, which is the consolidation.
                    let (mut i, mut j) = (a_lo, b_lo);
                    while i < a_hi && j < b_hi {
                        match common_order.unwrap_or_else(|| t1.cmp_cross(i, t2, j)) {
                            Ordering::Less => { copy(&mut tags, &mut offs, &mut diffs, 0, i, i + 1); i += 1; }
                            Ordering::Greater => { copy(&mut tags, &mut offs, &mut diffs, 1, j, j + 1); j += 1; }
                            Ordering::Equal => {
                                let mut d = d1[i].clone();
                                d.plus_equals(&d2[j]);
                                if !d.is_zero() { tags.push(0); offs.push(i); diffs.push(d); }
                                i += 1;
                                j += 1;
                            }
                        }
                    }
                    // Equal (key, val) classes can continue in the next chunk. Once either
                    // whole input chunk is spent, its last timestamp is the shared horizon:
                    // retain the other side's suffix, including the rest of this class.
                    if i == n1 || j == n2 {
                        p1 = i;
                        p2 = j;
                        break;
                    }
                    if i < a_hi { copy(&mut tags, &mut offs, &mut diffs, 0, i, a_hi); }
                    if j < b_hi { copy(&mut tags, &mut offs, &mut diffs, 1, j, b_hi); }
                }
            }
        }

        // The survey can be large. Its last use precedes materializing the
        // output columns, so do not retain it across that allocation peak.
        drop(runs);
        let times = ColTimes::gather_from(&[t1, t2], &tags, &offs);
        let srcs = [Some(&kv1), Some(&kv2)];
        Self::emit(&srcs, &tags, &offs, times, ColDiffs::from_vec(diffs), out);
        drop(tags);
        drop(offs);

        // Push back the survivor's unconsumed suffix (all `>` the horizon), ahead of its deque.
        if p1 < n1 {
            let idx: Vec<usize> = (p1..n1).collect();
            let mut t = ColTimes::new();
            t.push_range(t1, p1, n1);
            in1.push_front(Self::from_kv(gather(&kv1, &idx), t, d1.slice(p1, n1)));
        }
        if p2 < n2 {
            let idx: Vec<usize> = (p2..n2).collect();
            let mut t = ColTimes::new();
            t.push_range(t2, p2, n2);
            in2.push_front(Self::from_kv(gather(&kv2, &idx), t, d2.slice(p2, n2)));
        }
    }

    fn extract(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        residual: &mut Antichain<T>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        // Move whole chunks; only a split needs to gather its two payload pieces.
        let Some(chunk) = input.pop_front() else { return };
        let times = chunk.times();
        if times.is_empty() { return; }
        let beyond = times.beyond(frontier);
        if beyond.iter().all(|b| !b) {
            ship.push_back(chunk);
            return;
        }
        if beyond.iter().all(|b| *b) {
            let covered = times.beyond(residual.borrow());
            let fresh: Vec<usize> = (0..times.len()).filter(|&i| !covered[i]).collect();
            for i in times.minimal(&fresh) { residual.insert(times.get(i)); }
            keep.push_back(chunk);
            return;
        }
        let kv = chunk.kv();
        let diffs = chunk.diffs();
        let (mut ki, mut si) = (Vec::new(), Vec::new());
        for (i, is_beyond) in beyond.into_iter().enumerate() {
            if is_beyond { ki.push(i); } else { si.push(i); }
        }
        if !ki.is_empty() {
            // Only kept rows the residual does not already cover can change it; timestamps are
            // built for the minimal ones among those.
            let covered = times.beyond(residual.borrow());
            let fresh: Vec<usize> = ki.iter().copied().filter(|&i| !covered[i]).collect();
            for i in times.minimal(&fresh) { residual.insert(times.get(i)); }
            let d = diffs.gather(&ki);
            keep.push_back(Self::from_kv(gather(&kv, &ki), times.gather(&ki), d));
        }
        if !si.is_empty() {
            let d = diffs.gather(&si);
            ship.push_back(Self::from_kv(gather(&kv, &si), times.gather(&si), d));
        }
    }

    fn advance(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) {
        // Concatenate the pushed-back carry with the newly-arrived chunks, then advance/consolidate
        // each *complete* `(key, val)` group; withhold the last group as the carry unless `done`.
        if input.is_empty() { return; }
        let (ckv, mut ctimes, cdiffs) = if input.len() == 1 {
            // Merge output normally arrives uniquely owned. Move its columns
            // into advancement instead of copying the whole chunk first.
            let chunk = input.pop_front().unwrap();
            match Rc::try_unwrap(chunk.0) {
                Ok(inner) => (CValue::Prod(vec![inner.keys, inner.vals]), inner.times, inner.diffs),
                Err(inner) => Self::concat(&[Self(inner)]),
            }
        } else {
            Self::concat(&input.drain(..).collect::<Vec<_>>())
        };
        let n = ctimes.len();
        if n == 0 { return; }

        // Sorted, consolidated chunks sharing one time have distinct (key, val)s,
        // including across chunk boundaries. There is no per-row time or grouping work.
        if ctimes.is_constant() {
            let end = if done { n } else { n - 1 };
            if end < n {
                input.push_front(Self::from_kv(gather(&ckv, &[end]), ctimes.slice(end, n), cdiffs.slice(end, n)));
            }
            if end > 0 {
                ctimes.truncate_rows(end);
                ctimes.advance_by(frontier, end);
                Self::emit_columns(ckv, ctimes, if end == n { cdiffs } else { cdiffs.slice(0, end) }, out);
            }
            return;
        }

        // Group boundaries in ONE pass (corgi `group_bounds`) instead of a per-row `compare_at` scan.
        let bounds = group_bounds(&ckv); // exclusive group ends, ascending, `bounds.last() == n`

        // Giant-key case: a single group spanning the whole buffer → no group provably complete.
        if !done && bounds.len() == 1 {
            input.push_front(Self::from_kv(ckv, ctimes, cdiffs));
            return;
        }

        // Withhold the trailing group as the carry unless `done` (its start = the 2nd-to-last end).
        let end = if done { n } else { bounds[bounds.len() - 2] };
        if end < n {
            let idx: Vec<usize> = (end..n).collect();
            let mut ct = ColTimes::new();
            ct.push_range(&ctimes, end, n);
            input.push_front(Self::from_kv(gather(&ckv, &idx), ct, cdiffs.slice(end, n)));
        }

        // Advance + consolidate each complete group; emit `TARGET`-sized chunks. All rows of a group
        // share `(key, val)`, so one representative offset materializes each output row's kv. Times
        // advance in the dictionary or dense lanes; advancing can reorder a group's times,
        // so each group's row indices are re-sorted by time and equal times sum. Surviving rows are gathered.
        ctimes.truncate_rows(end);
        let strict = ctimes.advance_by(frontier, end);
        // Distinct (key, val) pairs cannot collide or reorder when their times advance.
        // Keep the columns intact when possible, and only slice at output boundaries.
        if strict || bounds.len() == n {
            Self::emit_columns(ckv, ctimes, if end == n { cdiffs } else { cdiffs.slice(0, end) }, out);
            return;
        }
        let srcs = [Some(&ckv)];
        let (mut tags, mut offs, mut rows) = (Vec::new(), Vec::new(), Vec::new());
        let mut odiffs = Vec::new();
        let mut order: Vec<usize> = Vec::new();
        let mut i = 0;
        for &g_end in &bounds {
            if g_end > end { break; }
            order.clear();
            order.extend(i..g_end);
            if !ctimes.is_constant() { order.sort_by(|&a, &b| ctimes.cmp(a, b)); }
            let mut k = 0;
            while k < order.len() {
                let rep = order[k];
                let mut d = cdiffs[rep].clone();
                k += 1;
                while k < order.len() && ctimes.cmp(order[k], rep) == Ordering::Equal {
                    d.plus_equals(&cdiffs[order[k]]);
                    k += 1;
                }
                if !d.is_zero() {
                    tags.push(0); offs.push(i); rows.push(rep); odiffs.push(d);
                    if rows.len() >= TARGET {
                        Self::emit(&srcs, &tags, &offs, ctimes.gather(&rows), ColDiffs::from_vec(std::mem::take(&mut odiffs)), out);
                        tags.clear(); offs.clear(); rows.clear();
                    }
                }
            }
            i = g_end;
        }
        if !rows.is_empty() { Self::emit(&srcs, &tags, &offs, ctimes.gather(&rows), ColDiffs::from_vec(odiffs), out); }
    }

    /// Maximal packing via the harness [`pack`]: coalesce by concatenating columns (`gather_lanes`),
    /// split with `gather`, seal as a no-op (corgi chunks are never paged here).
    fn settle(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
        pack(
            input,
            done,
            out,
            |run| {
                let (kv, times, diffs) = Self::concat(&run);
                Self::from_kv(kv, times, diffs)
            },
            |chunk, m| {
                let kv = chunk.kv();
                let n = chunk.len_();
                let left: Vec<usize> = (0..m).collect();
                let right: Vec<usize> = (m..n).collect();
                let (mut lt, mut rt) = (ColTimes::new(), ColTimes::new());
                lt.push_range(chunk.times(), 0, m);
                rt.push_range(chunk.times(), m, n);
                let l = Self::from_kv(gather(&kv, &left), lt, chunk.diffs().slice(0, m));
                let r = Self::from_kv(gather(&kv, &right), rt, chunk.diffs().slice(m, n));
                (l, r)
            },
            |chunk| chunk,
        );
    }
}

/// Sort parallel columns by `(key, val, time)` and consolidate exact `(key, val, time)` triples
/// (summing diffs, dropping zeros). Returns a sorted+consolidated `(keys, vals, times, diffs)`.
///
/// Multi-record: one columnar `sort_blocks` (discrimination sort) orders by `(key, val)` and
/// identifies equal runs. Only the small per-run *time* tiebreak is a Rust sort (time is not a
/// corgi type). Gather the payload once, after consolidation, from the surviving source rows.
fn sort_consolidate<T, R>(keys: CValue, vals: CValue, times: ColTimes<T>, diffs: Vec<R>) -> (CValue, CValue, ColTimes<T>, ColDiffs<R>)
where
    T: ColTime,
    R: Semigroup + Clone + PartialEq,
{
    let n = times.len();
    if n == 0 {
        return (keys, vals, times, ColDiffs::new());
    }
    let kv = CValue::Prod(vec![keys, vals]);
    // Order the payload; retain time/diff source coordinates until final consolidation.
    let (perm, labels) = sort_blocks(&[], &kv);
    // Walk maximal equal-`(key,val)` runs; within each, order by time and consolidate equal times.
    let (mut time_rows, mut od) = (Vec::new(), Vec::new());
    let mut run = Vec::new();
    let mut i = 0;
    while i < n {
        let mut j = i + 1;
        while j < n && labels[j] == labels[i] {
            j += 1;
        }
        if times.is_constant() {
            let rep = perm[i];
            let mut d = diffs[rep].clone();
            for &row in &perm[i + 1..j] { d.plus_equals(&diffs[row]); }
            if !d.is_zero() { time_rows.push(rep); od.push(d); }
            i = j;
            continue;
        }
        run.clear();
        run.extend_from_slice(&perm[i..j]);
        if !times.is_constant() { run.sort_by(|&a, &b| times.cmp(a, b)); }
        let mut k = 0;
        while k < run.len() {
            let rep = run[k];
            let mut d = diffs[rep].clone();
            k += 1;
            while k < run.len() && times.cmp(run[k], rep) == Ordering::Equal {
                d.plus_equals(&diffs[run[k]]);
                k += 1;
            }
            if !d.is_zero() {
                time_rows.push(rep);
                od.push(d);
            }
        }
        i = j;
    }
    let (keys, vals) = split_kv(gather(&kv, &time_rows));
    (keys, vals, times.gather(&time_rows), ColDiffs::from_vec(od))
}

impl<T, R> CorgiChunk<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + PartialEq + 'static,
{
    /// One sorted+consolidated chunk from columns already in corgi form (the column-native arrange
    /// ingest — no transcode).
    pub fn from_columns(keys: CValue, vals: CValue, times: ColTimes<T>, diffs: Vec<R>) -> Self {
        // An integer value is held as `i64`s, as an integer key is, so that readers may borrow it
        // whichever chunk of the arrangement it is in.
        let vals = hold_wide(vals);
        let (keys, vals, times, diffs) = sort_consolidate(keys, vals, times, diffs);
        debug_assert!({
            let lane = int_lane(key_lane(&keys));
            lane.is_some_and(|ids| ids.windows(2).all(|pair| pair[0] <= pair[1]))
        }, "arrangement key must lead with a sorted i64 identifier lane");
        Self::from_parts(keys, vals, times, diffs)
    }

}

/// Build a `ChunkBatch<CorgiChunk>` from corgi key/val COLUMNS directly (no transcode): sort +
/// consolidate into one chunk, then `settle`. The column-native egress the reduce backend seals its
/// output with (it resolves proxy ids to real columns by `gather` and hands them here).
pub(crate) fn columns_to_batch<T, R>(keys: CValue, vals: CValue, times: ColTimes<T>, diffs: Vec<R>) -> ChunkBatch<CorgiChunk<T, R>>
where
    T: ColTime,
    R: Semigroup + Clone + PartialEq + 'static,
{
    let chunk = CorgiChunk::from_columns(keys, vals, times, diffs);
    settle_one(chunk)
}

/// Grade one chunk into a `ChunkBatch`.
fn settle_one<T, R>(chunk: CorgiChunk<T, R>) -> ChunkBatch<CorgiChunk<T, R>>
where
    T: ColTime,
    R: Semigroup + Clone + PartialEq + 'static,
{
    let mut input = VecDeque::new();
    if chunk.len_() > 0 { input.push_back(chunk); }
    let mut output = VecDeque::new();
    CorgiChunk::settle(&mut input, true, &mut output);
    ChunkBatch::new(output.into())
}

/// A column-native arrange **chunker**: turns input `CorgiContainer`s into sorted+consolidated
/// `CorgiChunk`s WITHOUT `ContainerChunker`'s drain-to-rows. Paired with the
/// standard `ChunkBatcher`/`ChunkBuilder`, the arrange ingest stays column-native — no
/// columns→rows→columns round-trip at the arrangement boundary.
///
/// Crucially it **accumulates to `INGEST`** before consolidating (like `ContainerChunker`), so it
/// emits few large chunks rather than one tiny chunk per input container — otherwise the columnar
/// per-chunk set-up (`gather`/`sort_perm`) dominates when input arrives as many small batches.
pub(crate) struct CorgiChunker<T, R> {
    /// Un-consolidated key/val blocks, adaptive times, and donated dense ingest diffs.
    k_blocks: Vec<CValue>,
    v_blocks: Vec<CValue>,
    times: ColTimes<T>,
    diffs: Vec<R>,
    ready: VecDeque<CorgiChunk<T, R>>,
    current: Option<CorgiChunk<T, R>>,
}

impl<T, R> Default for CorgiChunker<T, R> {
    fn default() -> Self {
        CorgiChunker { k_blocks: Vec::new(), v_blocks: Vec::new(), times: ColTimes::default(), diffs: Vec::new(), ready: VecDeque::new(), current: None }
    }
}

/// An arrangement's key column, in the form every consumer of a `CorgiChunk` can rely on:
/// **it leads with an integer, and the chunk is sorted by that integer.**
///
/// A key that is already an integer (an `Int` leaf, or the 1-field `Prod` that
/// [`corgi::arrange::leaf_slice`] also reads through) is used as it stands — the value IS the
/// identifier, injectively, and a hash lane would cost 8 bytes a row to say the same thing.
/// Any other key shape — multi-field `Prod`, `List`, `Sum`, `Unit` — is hashed and the hash is
/// PREPENDED, so the key becomes `Prod([hash, key])`. `CorgiChunk::from_columns` then sorts
/// lexicographically over lanes, which is hash order with the real key as tie-break.
///
/// The original key stays in the column, which is what makes the hash safe: colliding keys land
/// adjacent and sub-sorted, so they are told apart by comparison rather than by luck, and reads
/// recover the real key by [`recover_key`]. The two forms are distinguishable after the fact
/// (`leaf_slice` succeeds on exactly the un-prepended one) because no compound key reaches an
/// arrangement un-prepended.
///
/// The identifier lane is held as `i64`s, so readers borrow it ([`ident`]). Corgi chooses how to
/// hold an integer column (a mask or a sum tag may be held as bytes); a key held narrower is
/// widened here, once.
///
/// The hash is computed ONCE here, at ingest, and thereafter moves as data: `merge`, `advance`
/// and `settle` permute key columns with `gather_lanes`, so no transducer recomputes it.
pub(crate) fn present_key(keys: CValue) -> CValue {
    match corgi::arrange::leaf_slice(&keys) {
        Some(_) => hold_wide(keys),
        None => {
            let hashes = corgi::hash(&keys).into_iter().map(|h| h as i64).collect();
            CValue::Prod(vec![CValue::i64(hashes), keys])
        }
    }
}

/// An integer column (an `Int` leaf, or a 1-field `Prod` of one) held as `i64`s; any other
/// column as it is. Corgi chooses how to hold an integer column, and may hold one narrower.
fn hold_wide(col: CValue) -> CValue {
    match corgi::arrange::leaf_slice(&col) {
        Some(std::borrow::Cow::Owned(ids)) => rewrap(&col, ids),
        _ => col,
    }
}

/// An integer column's values held as `i64`s, wrapped in `template`'s single-field products.
pub(crate) fn rewrap(template: &CValue, ids: Vec<i64>) -> CValue {
    match template {
        CValue::Prod(fields) => CValue::Prod(vec![rewrap(&fields[0], ids)]),
        _ => CValue::i64(ids),
    }
}

/// An integer column's values, borrowed: `Some` exactly when the column is an `Int` leaf (or a
/// 1-field `Prod` of one) held as `i64`s. An integer column held narrower is `None`, and a
/// reader that can take one falls back to its structural path.
pub(crate) fn int_lane(col: &CValue) -> Option<&[i64]> {
    match col {
        CValue::Prod(fields) if fields.len() == 1 => int_lane(&fields[0]),
        _ => corgi::arrange::borrowed_i64(col),
    }
}

/// The identifier lane of a [`present_key`] column, borrowed: the key's own values when it is an
/// integer, and the prepended hash lane otherwise. `present_key` holds it as `i64`s.
pub(crate) fn ident(keys: &CValue) -> &[i64] {
    int_lane(key_lane(keys)).expect("an arrangement's identifier lane is held as i64s")
}

/// The integer identifier of each row of a [`present_key`] column: the key's own values when it is
/// an integer, and the prepended hash lane otherwise. Never re-hashes.
pub(crate) fn key_ids(keys: &CValue) -> Vec<i64> {
    ident(keys).to_vec()
}

/// The single column an arrangement is sorted by, for seeking: the key itself when it is an
/// integer, else the prepended hash lane. Always an `i64` leaf, so `find_ranges` over it takes
/// corgi's integer fast path whatever the underlying key shape.
///
/// One rule covers both forms, because [`present_key`] leaves exactly three possibilities: a bare
/// `Int`, the 1-field `Prod` that also counts as an integer, or a prepended `Prod([hash, key])`.
/// The leading field is the identifier in all three.
fn key_lane(keys: &CValue) -> &CValue {
    match keys {
        CValue::Prod(cols) => &cols[0],
        _ => keys,
    }
}

/// Whether [`present_key`] prepended a hash to this key — i.e. whether rows sharing an identifier
/// may hold DIFFERENT keys. False for primitive-integer keys, whose identifier is injective, so
/// readers can skip the checks that guard against collisions entirely.
pub(crate) fn key_is_hashed(keys: &CValue) -> bool {
    corgi::arrange::leaf_slice(keys).is_none()
}

/// Undo [`present_key`]: the key as the rest of the system knows it.
/// Cloning the column shares its primitive buffers.
pub(crate) fn recover_key(keys: &CValue) -> CValue {
    recover_key_ref(keys).clone()
}

/// Borrow the declared key without its arrangement-only identifier lane.
/// Use before gathering a projection input to avoid copying a discarded hash column.
pub(crate) fn recover_key_ref(keys: &CValue) -> &CValue {
    match keys {
        CValue::Prod(cols) if corgi::arrange::leaf_slice(keys).is_none() => &cols[1],
        _ => keys,
    }
}

/// Validate the matched identifier runs for a block, batching by column pair. Runs are sub-sorted by real
/// key, so matching each run's endpoints to a representative checks every row without visiting it.
/// Each `(key, column, start, end)` names a non-empty identifier run, with `end` exclusive.
/// Coordinates name borrowed real-key columns; self checks and duplicate singleton endpoints vanish.
pub(super) fn colliding_keys(
    keys: usize,
    columns: &[&CValue],
    runs: impl Iterator<Item = (usize, usize, usize, usize)>,
) -> Vec<bool> {
    let mut first = vec![None; keys];
    let mut pairs = Vec::new();
    for (key, column, start, end) in runs {
        let reference = *first[key].get_or_insert((column, start));
        for row in std::iter::once(start).chain((end - start > 1).then_some(end - 1)) {
            if (column, row) != reference {
                pairs.push((column, reference.0, row, reference.1, key));
            }
        }
    }
    let mut colliding = vec![false; keys];
    pairs.sort_unstable_by_key(|p| (p.0, p.1, p.2));
    let (mut left, mut right) = (Vec::new(), Vec::new());
    for group in pairs.chunk_by(|a, b| (a.0, a.1) == (b.0, b.1)) {
        left.clear(); right.clear();
        left.extend(group.iter().map(|p| p.2));
        right.extend(group.iter().map(|p| p.3));
        let signs = equal_idx(columns[group[0].0], columns[group[0].1], &left, &right);
        for (pair, sign) in group.iter().zip(signs) { colliding[pair.4] |= sign != 0; }
    }
    colliding
}

/// Concatenate column blocks into one column (multi-source `gather_lanes`, no sort).
pub(crate) fn concat_blocks(blocks: &[CValue]) -> CValue {
    if blocks.len() == 1 {
        return blocks[0].clone();
    }
    let srcs: Vec<Option<&CValue>> = blocks.iter().map(Some).collect();
    let total: usize = blocks.iter().map(CValue::len).sum();
    let (mut tags, mut offs) = (Vec::with_capacity(total), Vec::with_capacity(total));
    for (ti, b) in blocks.iter().enumerate() {
        for o in 0..b.len() { tags.push(ti); offs.push(o); }
    }
    gather_lanes(&srcs, &tags, &offs)
}

impl<T, R> CorgiChunker<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + PartialEq + 'static,
{
    /// Consolidate the accumulated blocks into one graded chunk (concat columns, then sort+consolidate).
    fn flush(&mut self) {
        if self.times.is_empty() {
            return;
        }
        let keys = present_key(concat_blocks(&self.k_blocks));
        let vals = concat_blocks(&self.v_blocks);
        self.k_blocks.clear();
        self.v_blocks.clear();
        let times = std::mem::take(&mut self.times);
        let diffs = std::mem::take(&mut self.diffs);
        let chunk = CorgiChunk::from_columns(keys, vals, times, diffs);
        if chunk.len_() > 0 {
            self.ready.push_back(chunk);
        }
    }
}

impl<T, R> timely::container::PushInto<&mut crate::runtime::container::CorgiContainer<T, R>> for CorgiChunker<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + PartialEq + 'static,
{
    fn push_into(&mut self, c: &mut crate::runtime::container::CorgiContainer<T, R>) {
        if c.times.is_empty() {
            return;
        }
        self.k_blocks.push(std::mem::replace(&mut c.keys, CValue::Unit(0)));
        self.v_blocks.push(std::mem::replace(&mut c.vals, CValue::Unit(0)));
        if self.times.is_empty() {
            // The first block can donate its owned lanes. Return our empty
            // buffers to the sender; later blocks append to the donated ones.
            std::mem::swap(&mut self.times, &mut c.times);
            std::mem::swap(&mut self.diffs, &mut c.diffs);
        } else {
            self.times.push_range(&c.times, 0, c.times.len());
            c.times.clear();
            self.diffs.append(&mut c.diffs);
        }
        if self.times.len() >= INGEST {
            self.flush();
        }
    }
}

impl<T, R> timely::container::ContainerBuilder for CorgiChunker<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + PartialEq + 'static,
{
    type Container = CorgiChunk<T, R>;
    // `extract` ships ready chunks, leaving the sub-`INGEST` remainder to accumulate further.
    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.current = self.ready.pop_front();
        self.current.as_mut()
    }
    // `finish` also flushes the remainder (called until it returns None).
    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.flush();
        self.extract()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use corgi::arrange::compare_at;
    use differential_dataflow::trace::chunk::{ChunkBatchMerger, is_graded};
        use differential_dataflow::trace::implementations::spine_fueled::Merger;
    use std::collections::BTreeMap;

    fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

    /// An integer column of these values.
    fn ints(xs: Vec<u64>) -> CValue { CValue::i64(xs.into_iter().map(|x| x as i64).collect()) }

    /// An integer column's values.
    fn lane(col: &CValue) -> Vec<u64> { col.as_i64("lane").unwrap().iter().map(|&x| x as u64).collect() }

    #[test]
    fn ingest_nested_payload_matches_scalar_consolidation() {
        type T = crate::ir::Time;
        // Lists compare by length before content; their elements are products.
        let ends: Vec<usize> = (0..32).scan(0, |end, i| { *end += i % 5; Some(*end) }).collect();
        let elements = *ends.last().unwrap();
        let values = CValue::List(ends.into(), Box::new(CValue::Prod(vec![
            ints((0..elements).map(|i| (i % 7) as u64).collect()),
            ints((0..elements).map(|i| (i % 3) as u64).collect()),
        ])));
        for n in [0, 1, 64, 1024] {
            for cancel_all in [false, true] {
                let mut seed = 719;
                let rows: Vec<_> = (0..n).map(|_| xorshift(&mut seed) as usize % 32).collect();
                let keys = ints(rows.iter().map(|&r| (r % 4) as u64).collect());
                let vals = gather(&values, &rows);
                let kv = CValue::Prod(vec![keys.clone(), vals.clone()]);
                let times: ColTimes<T> = rows.iter().map(|&r| T::new([(r % 2) as u64, (r % 3) as u64, (r % 5) as u64].into_iter().collect())).collect();
                let diffs: Vec<i64> = (0..n).map(|_| (xorshift(&mut seed) % 5) as i64 - 2).collect();
                // The paired case cancels every triple, including on unordered input.
                let input: Vec<usize> = if cancel_all { (0..n).chain(0..n).collect() } else { (0..n).collect() };
                let diffs = if cancel_all { diffs.iter().copied().chain(diffs.iter().map(|d| -d)).collect() } else { diffs };
                let keys = gather(&keys, &input);
                let vals = gather(&vals, &input);
                let times = times.gather(&input);
                let mut order: Vec<_> = (0..input.len()).collect();
                order.sort_by(|&a, &b| compare_at(&kv, input[a], &kv, input[b]).then_with(|| times.cmp(a, b)));
                let mut expected: Vec<(usize, i64)> = Vec::new();
                for row in order {
                    if let Some((last, diff)) = expected.last_mut() {
                        if compare_at(&kv, input[*last], &kv, input[row]) == Ordering::Equal
                            && times.cmp(*last, row) == Ordering::Equal {
                            *diff += diffs[row];
                            continue;
                        }
                    }
                    expected.push((row, diffs[row]));
                }
                expected.retain(|(_, d)| *d != 0);
                let result = CorgiChunk::from_columns(keys, vals, times.clone(), diffs);
                assert_eq!(result.len_(), expected.len(), "n={n}, cancel={cancel_all}");
                let actual = result.kv();
                for (i, &(row, diff)) in expected.iter().enumerate() {
                    assert_eq!(compare_at(&actual, i, &kv, input[row]), Ordering::Equal);
                    assert_eq!(result.times().get(i), times.get(row));
                    assert_eq!(result.diffs()[i], diff);
                }
            }
        }
    }

    #[test]
    fn chunker_owned_lanes_match_scalar_across_flushes() {
        use timely::container::{ContainerBuilder, PushInto};
        use crate::runtime::container::CorgiContainer;
        let mut builder = CorgiChunker::<crate::ir::Time, i64>::default();
        let mut source = CorgiContainer::default();
        let mut seed = 731;
        for blocks in [1, 3, 1, 5] {
            let mut expected = BTreeMap::new();
            // Empty containers do not consume the first-block opportunity.
            builder.push_into(&mut source);
            for block in 0..blocks {
                let mut keys = Vec::new();
                for row in 0..64 {
                    let key = xorshift(&mut seed) % 8;
                    let time = crate::ir::Time::new((0..(block + row) % 4)
                        .map(|_| xorshift(&mut seed) % 3).collect());
                    let diff = (xorshift(&mut seed) % 5) as i64 - 2;
                    keys.push(key);
                    source.times.push(&time);
                    source.diffs.push(diff);
                    *expected.entry((key, time)).or_insert(0i64) += diff;
                }
                source.keys = ints(keys);
                source.vals = CValue::Unit(64);
                let donated = source.diffs.as_ptr();
                builder.push_into(&mut source);
                if block == 0 { assert_eq!(builder.diffs.as_ptr(), donated); }
                assert!(source.times.is_empty() && source.diffs.is_empty());
            }
            expected.retain(|_, diff| *diff != 0);
            let result = builder.finish().unwrap();
            let keys = lane(result.keys());
            let actual: BTreeMap<_, _> = (0..result.len_()).map(|i|
                ((keys[i], result.times().get(i)), result.diffs()[i])).collect();
            assert_eq!(actual.len(), result.len_(), "duplicate output triples");
            assert_eq!(actual, expected);
            assert!(builder.finish().is_none());
        }
    }

    #[test]
    fn int_lane_borrows_through_single_field_products() {
        let leaf = CValue::i64(vec![-3, 0, 5, 9]);
        let pointer = int_lane(&leaf).unwrap().as_ptr();
        for col in [leaf.clone(), CValue::Prod(vec![leaf.clone()]),
                    CValue::Prod(vec![CValue::Prod(vec![leaf])])] {
            let lane = int_lane(&col).unwrap();
            assert_eq!(lane, &[-3, 0, 5, 9]);
            assert_eq!(lane.as_ptr(), pointer);
        }
    }

    /// A key corgi holds as bytes (here a comparison mask) is an integer key like any other: it is
    /// its own identifier, held as `i64`s, and not hashed.
    #[test]
    fn byte_held_integer_keys_are_presented_as_i64() {
        let xs = CValue::i64(vec![-3, 0, 5, 9]);
        let mask = corgi::eval_graph(&{
            let mut b = corgi::Builder::<corgi::NumOp>::default();
            let input = b.input();
            let out = b.add(corgi::CmpOp::RelImm(corgi::Pred::Gt, corgi::Scalar::Int(0)), vec![input]);
            b.finish(out)
        }, xs);
        assert_eq!(int_lane(&mask), None, "corgi holds a mask as bytes");
        for keys in [mask.clone(), CValue::Prod(vec![mask])] {
            let presented = present_key(keys);
            assert!(!key_is_hashed(&presented));
            assert_eq!(ident(&presented), &[0, 0, 1, 1]);
        }
    }

    #[test]
    fn cancelled_merge_does_not_retain_input_sized_diff_storage() {
        let mut retained_capacity = None;
        for rows in [16, 256, 4096] {
            let make = |diffs| CorgiChunk::from_columns(
                ints((0..rows).collect()), CValue::Unit(rows as usize),
                (0..rows).map(|_| 0u64).collect(), diffs,
            );
            let mut retractions = vec![-1i64; rows as usize];
            *retractions.last_mut().unwrap() = -2;
            let mut left = VecDeque::from([make(vec![1i64; rows as usize])]);
            let mut right = VecDeque::from([make(retractions)]);
            let mut output = VecDeque::new();
            CorgiChunk::merge(&mut left, &mut right, &mut output);
            assert!(left.is_empty() && right.is_empty());
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].diffs().to_vec(), vec![-1]);
            assert_eq!(lane(output[0].keys()), &[rows - 1]);
            let capacity = output[0].0.diffs.storage_bytes().1;
            assert_eq!(*retained_capacity.get_or_insert(capacity), capacity,
                "one surviving row should not retain storage proportional to cancelled input");
        }
    }

    #[test]
    fn advance_owned_and_shared_nested_times_matches_reference() {
        use differential_dataflow::lattice::Lattice;
        type T = crate::ir::Time;
        let time = |outer, coords: &[u64]| T::new(std::iter::once(outer).chain(coords.iter().copied()).collect());
        let times = [time(0, &[]), time(1, &[2, 3]), time(2, &[1, 4]), time(2, &[3, 1]), time(3, &[2])];
        let rows: Vec<_> = (0..5).flat_map(|k| (0..2).flat_map(move |v| (0..5).map(move |t| (k, v, t))))
            .map(|(k, v, i)| ((k, v), times[i].clone(), if i % 2 == 0 { 1i64 } else { -1 }))
            .collect();
        for frontier in [Antichain::new(), Antichain::from_elem(time(3, &[2, 2])),
                         Antichain::from(vec![time(1, &[4, 1]), time(3, &[1, 2])])] {
            let mut expected = BTreeMap::new();
            for (kv, t, d) in &rows {
                let mut t = t.clone();
                t.advance_by(frontier.borrow());
                *expected.entry((*kv, t)).or_insert(0i64) += d;
            }
            expected.retain(|_, d| *d != 0);
            for size in [1, 3, rows.len()] {
                for shared in [false, true] {
                    let chunks: Vec<_> = rows.chunks(size).map(|rows| CorgiChunk::from_columns(
                        ints(rows.iter().map(|r| r.0.0).collect()),
                        ints(rows.iter().map(|r| r.0.1).collect()),
                        rows.iter().map(|r| r.1.clone()).collect(), rows.iter().map(|r| r.2).collect(),
                    )).collect();
                    let retained = if shared { chunks.clone() } else { Vec::new() };
                    let (mut input, mut output) = (VecDeque::new(), VecDeque::new());
                    for chunk in chunks {
                        input.push_back(chunk);
                        CorgiChunk::advance(&mut input, frontier.borrow(), false, &mut output);
                    }
                    CorgiChunk::advance(&mut input, frontier.borrow(), true, &mut output);
                    assert!(input.is_empty());
                    let mut actual = BTreeMap::new();
                    let mut previous = None;
                    for chunk in output {
                        let keys = lane(chunk.keys());
                        let vals = lane(chunk.vals());
                        for i in 0..chunk.len_() {
                            let key = ((keys[i], vals[i]), chunk.times().get(i));
                            assert!(previous.as_ref().is_none_or(|p| p < &key));
                            previous = Some(key.clone());
                            assert!(actual.insert(key, chunk.diffs()[i]).is_none());
                        }
                    }
                    assert_eq!(actual, expected, "size={size}, shared={shared}, frontier={frontier:?}");
                    drop(retained);
                }
            }
        }
    }

    #[test]
    fn extract_moves_whole_structured_chunks() {
        let source = CorgiChunk::from_columns(ints(vec![0, 0, 1, 1]),
            CValue::List(corgi::Bounds::Stride(1, 4), Box::new(ints(vec![7, 7, 8, 8]))),
            vec![0u64, 1, 2, 3].into_iter().collect(), vec![1i64, -1, 2, -3]);
        for frontier in [0, 2, 4] {
            for initial in [0, 5] {
                let mut input = VecDeque::from([source.clone()]);
                let (mut keep, mut ship) = (VecDeque::new(), VecDeque::new());
                let mut residual = Antichain::from_elem(initial);
                let mut expected = residual.clone();
                for row in 0..source.len_() {
                    if source.times().get(row) >= frontier { expected.insert(source.times().get(row)); }
                }
                CorgiChunk::extract(&mut input, AntichainRef::new(&[frontier]),
                    &mut residual, &mut keep, &mut ship);
                assert!(input.is_empty());
                assert_eq!(residual, expected);
                for (output, kept) in [(&keep, true), (&ship, false)] {
                    let selected: Vec<_> = (0..source.len_())
                        .filter(|&row| (source.times().get(row) >= frontier) == kept).collect();
                    assert_eq!(output.len(), usize::from(!selected.is_empty()));
                    if let Some(chunk) = output.front() {
                        assert_eq!(chunk.len_(), selected.len());
                        for (row, &original) in selected.iter().enumerate() {
                            assert_eq!(compare_at(&chunk.kv(), row, &source.kv(), original), Ordering::Equal);
                            assert_eq!(chunk.times().get(row), source.times().get(original));
                            assert_eq!(chunk.diffs()[row], source.diffs()[original]);
                        }
                        if selected.len() == source.len_() {
                            assert!(Rc::ptr_eq(&source.0, &chunk.0));
                        } else {
                            assert!(!Rc::ptr_eq(&source.0, &chunk.0));
                        }
                    }
                }
            }
        }
        let mut input = VecDeque::from([CorgiChunk::<u64, i64>::default()]);
        let (mut keep, mut ship) = (VecDeque::new(), VecDeque::new());
        let mut residual = Antichain::from_elem(5);
        CorgiChunk::extract(&mut input, AntichainRef::new(&[0]), &mut residual, &mut keep, &mut ship);
        assert!(input.is_empty() && keep.is_empty() && ship.is_empty());
        assert_eq!(residual, Antichain::from_elem(5));
    }

    /// `extract` must split rows at the frontier and extend the residual with exactly the kept
    /// times, whether a chunk is kept whole, shipped whole, or split.
    #[test]
    fn extract_matches_owned_times_for_whole_and_split_chunks() {
        type T = crate::ir::Time;
        let time = |outer, coords: &[u64]| T::new(std::iter::once(outer).chain(coords.iter().copied()).collect());
        let times = [time(0, &[]), time(1, &[2]), time(2, &[0, 3]), time(1, &[4, 1]), time(3, &[1])];
        let frontiers = [Antichain::new(), Antichain::from_elem(time(0, &[])), Antichain::from_elem(time(1, &[1])),
                         Antichain::from(vec![time(1, &[3]), time(2, &[0, 1])]), Antichain::from_elem(time(9, &[]))];
        for frontier in &frontiers {
            for size in [1, 2, times.len()] {
                let chunks: Vec<_> = (0..times.len()).collect::<Vec<_>>().chunks(size).map(|rows| CorgiChunk::from_columns(
                    ints(rows.iter().map(|&r| r as u64).collect()), ints(vec![0; rows.len()]),
                    rows.iter().map(|&r| times[r].clone()).collect(), vec![1i64; rows.len()],
                )).collect();
                let mut residual = Antichain::from_elem(time(5, &[5]));
                let mut expected = residual.clone();
                for t in times.iter().filter(|t| frontier.less_equal(t)) { expected.insert_ref(t); }
                let (mut input, mut keep, mut ship) = (VecDeque::from(chunks), VecDeque::new(), VecDeque::new());
                while !input.is_empty() {
                    CorgiChunk::extract(&mut input, frontier.borrow(), &mut residual, &mut keep, &mut ship);
                }
                assert_eq!(residual, expected, "frontier={frontier:?}, size={size}");
                for (chunks, beyond) in [(keep, true), (ship, false)] {
                    let mut actual: Vec<T> = chunks.iter().flat_map(|c| c.times().to_vec()).collect();
                    let mut expected: Vec<T> = times.iter().filter(|t| frontier.less_equal(t) == beyond).cloned().collect();
                    actual.sort(); expected.sort();
                    assert_eq!(actual, expected, "frontier={frontier:?}, size={size}, beyond={beyond}");
                }
            }
        }
    }

    /// Build a single sorted+consolidated CorgiChunk from u64 (key,val,time,diff) rows.
    fn chunk(rows: &[((u64, u64), u64, i64)]) -> CorgiChunk<u64, i64> {
        // Sort + consolidate by ((k,v),t) so a chunk is a legal sorted run.
        let mut m: BTreeMap<((u64, u64), u64), i64> = BTreeMap::new();
        for &(kv, t, d) in rows { *m.entry((kv, t)).or_insert(0) += d; }
        m.retain(|_, d| *d != 0);
        let keys = ints(m.keys().map(|((k, _), _)| *k).collect());
        let vals = ints(m.keys().map(|((_, v), _)| *v).collect());
        let times: ColTimes<u64> = m.keys().map(|(_, t)| *t).collect();
        let diffs = m.values().copied().collect();
        CorgiChunk::from_parts(keys, vals, times, diffs)
    }

    fn read_batch(b: &ChunkBatch<CorgiChunk<u64, i64>>) -> BTreeMap<((u64, u64), u64), i64> {
        let mut m = BTreeMap::new();
        for ch in &b.chunks {
            let ks = lane(ch.keys());
            let vs = lane(ch.vals());
            for i in 0..ch.len_() { *m.entry(((ks[i], vs[i]), ch.times().get(i))).or_insert(0) += ch.diffs()[i]; }
        }
        m.retain(|_, d| *d != 0);
        m
    }

    fn reference(u1: &[((u64, u64), u64, i64)], u2: &[((u64, u64), u64, i64)], f: u64) -> BTreeMap<((u64, u64), u64), i64> {
        let mut m = BTreeMap::new();
        for u in u1.iter().chain(u2) { *m.entry((u.0, u.1.max(f))).or_insert(0) += u.2; } // advance_by on u64 = max
        m.retain(|_, d| *d != 0);
        m
    }

    /// Cut a consolidated set into a batch of small chunks (globally sorted; groups straddle).
    fn batch(rows: &[((u64, u64), u64, i64)], sz: usize) -> ChunkBatch<CorgiChunk<u64, i64>> {
        let mut m: BTreeMap<((u64, u64), u64), i64> = BTreeMap::new();
        for &(kv, t, d) in rows { *m.entry((kv, t)).or_insert(0) += d; }
        m.retain(|_, d| *d != 0);
        let all: Vec<((u64, u64), u64, i64)> = m.into_iter().map(|((kv, t), d)| (kv, t, d)).collect();
        let chunks: Vec<_> = all.chunks(sz.max(1)).map(chunk).collect();
        ChunkBatch::new(chunks)
    }

    #[test]
    fn batch_merger_resumable_matches_reference() {
        let mut seed = 0x9E3779B97F4A7C15u64;
        for _ in 0..200 {
            let gen = |seed: &mut u64| -> Vec<((u64, u64), u64, i64)> {
                let n = (xorshift(seed) % 40) as usize + 1;
                (0..n).map(|_| {
                    let k = xorshift(seed) % 10; let v = xorshift(seed) % 3; let t = xorshift(seed) % 6;
                    let d = if xorshift(seed) % 4 == 0 { -1 } else { 1 };
                    ((k, v), t, d)
                }).collect()
            };
            let u1 = gen(&mut seed);
            let u2 = gen(&mut seed);
            let sz = (xorshift(&mut seed) % 4) as usize + 1;
            let f = xorshift(&mut seed) % 6;
            let (s1, s2) = (batch(&u1, sz), batch(&u2, sz));
            let frontier = Antichain::from_elem(f);

            let mut merger = ChunkBatchMerger::new(&s1, &s2, frontier.borrow());
            loop {
                let mut fuel = 1isize; // tiny → many yields, each settling
                merger.work(&s1, &s2, &mut fuel);
                if fuel > 0 { break; }
            }
            let result = merger.done();
            let chunks: &[CorgiChunk<u64, i64>] = result.as_ref().map_or(&[], |b| &b.chunks[..]);
            assert!(is_graded(chunks), "ungraded: {:?}", chunks.iter().map(Chunk::len).collect::<Vec<_>>());
            let want = reference(&u1, &u2, f);
            // A merge that cancels to nothing reports an absent payload.
            assert_eq!(result.is_none(), want.is_empty(), "absence must track emptiness\nu1={u1:?}\nu2={u2:?}\nf={f}");
            assert_eq!(result.as_ref().map_or_else(BTreeMap::new, read_batch), want, "u1={u1:?}\nu2={u2:?}\nf={f}");
        }
    }
}
