//! `CorgiChunk`: a [`Chunk`](differential_dataflow::trace::chunk::Chunk) whose key/val payload is a
//! pair of corgi columns, with per-tuple times held columnar in a [`ColTimes`] (SoA over
//! `<T as Columnar>::Container`, killing the per-row `PointStamp` allocation); diffs stay a `Vec`.
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

use corgi::arrange::{compare_adjacent, compare_at, gather, gather_lanes, group_bounds, sort_perm, survey, Run};
use corgi::Value as CValue;

use columnar::Columnar;

use crate::corgi::col_times::{ColTime, ColTimes};

use std::cmp::Ordering;

/// The grading target (also the merge/advance emit-chunk size). Larger than `VecChunk`'s 8192 to
/// amortize corgi's per-chunk columnar set-up (each chunk boundary costs a `gather` materialization);
/// bigger chunks mean fewer boundaries. Bounded so the fueled merger still yields.
const TARGET: usize = 1 << 18;

/// Shared, immutable chunk contents. `Clone` of a `CorgiChunk` is an `Rc` bump.
///
/// Same payload as [`CorgiContainer`](crate::corgi::container::CorgiContainer), and the two
/// should eventually be ONE type: today they differ only in time storage (`ColTimes` here —
/// bulk-read, never mutated — vs `Vec<T>` there, because feedback/enter mutate times row-wise)
/// and in invariants (sorted+consolidated+shared here, raw+owned there). A time container with
/// bulk mutation verbs (apply one summary across a range) removes the last real difference.
struct Inner<T: Columnar, R> {
    /// Key column (corgi), aligned with `vals`/`times`/`diffs`, sorted by `(key, val, time)`.
    keys: CValue,
    /// Val column (corgi).
    vals: CValue,
    /// Per-update times, SoA-columnar (the lattice algebra lives here; corgi never sees time).
    times: ColTimes<T>,
    /// Per-update diffs.
    diffs: Vec<R>,
}

/// A sorted, consolidated run of `((key, val), time, diff)` with corgi-columnar key/val, shared via `Rc`.
pub struct CorgiChunk<T: Columnar, R>(Rc<Inner<T, R>>);

impl<T: Columnar, R> Clone for CorgiChunk<T, R> {
    fn clone(&self) -> Self { CorgiChunk(Rc::clone(&self.0)) }
}

impl<T: Columnar, R> Default for CorgiChunk<T, R> {
    fn default() -> Self {
        CorgiChunk(Rc::new(Inner { keys: CValue::Unit(0), vals: CValue::Unit(0), times: ColTimes::new(), diffs: Vec::new() }))
    }
}

/// Split a `Prod([keys, vals])` corgi value into its two columns.
fn split_kv(kv: CValue) -> (CValue, CValue) {
    let mut cols = kv.into_prod("corgi chunk kv").unwrap();
    let vals = cols.pop().unwrap();
    let keys = cols.pop().unwrap();
    (keys, vals)
}

impl<T: Columnar + Clone, R: Clone> CorgiChunk<T, R> {
    fn from_parts(keys: CValue, vals: CValue, times: ColTimes<T>, diffs: Vec<R>) -> Self {
        CorgiChunk(Rc::new(Inner { keys, vals, times, diffs }))
    }
    /// The `(key, val)` sort payload as one corgi `Prod` column (cheap `Arc` bumps).
    fn kv(&self) -> CValue { CValue::Prod(vec![self.0.keys.clone(), self.0.vals.clone()]) }
    fn from_kv(kv: CValue, times: ColTimes<T>, diffs: Vec<R>) -> Self {
        let (keys, vals) = split_kv(kv);
        Self::from_parts(keys, vals, times, diffs)
    }
    pub fn keys(&self) -> &CValue { &self.0.keys }
    pub fn vals(&self) -> &CValue { &self.0.vals }
    pub fn times(&self) -> &ColTimes<T> { &self.0.times }
    pub fn diffs(&self) -> &[R] { &self.0.diffs }
}

impl<T, R> CorgiChunk<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + 'static,
{
    /// Materialize `[start, end)` of accumulated `(tag → src, off, time, diff)` into `TARGET`-sized
    /// output chunks. `srcs` are the (stable) source kv columns the tags/offs index into.
    fn emit(
        srcs: &[Option<&CValue>],
        tags: &[usize],
        offs: &[usize],
        times: &ColTimes<T>,
        diffs: &[R],
        out: &mut VecDeque<Self>,
    ) {
        let n = times.len();
        let mut s = 0;
        while s < n {
            let e = (s + TARGET).min(n);
            let kv = gather_lanes(srcs, &tags[s..e], &offs[s..e]);
            let mut t = ColTimes::new();
            t.push_range(times, s, e);
            out.push_back(Self::from_kv(kv, t, diffs[s..e].to_vec()));
            s = e;
        }
    }

    /// Concatenate a run of (globally-sorted) chunks into one combined `(kv, times, diffs)`.
    fn concat(chunks: &[Self]) -> (CValue, ColTimes<T>, Vec<R>) {
        let kvs: Vec<CValue> = chunks.iter().map(Self::kv).collect();
        let srcs: Vec<Option<&CValue>> = kvs.iter().map(Some).collect();
        let total: usize = chunks.iter().map(Self::len_).sum();
        let (mut tags, mut offs) = (Vec::with_capacity(total), Vec::with_capacity(total));
        let (mut times, mut diffs) = (ColTimes::new(), Vec::with_capacity(total));
        for (ti, ch) in chunks.iter().enumerate() {
            for o in 0..ch.len_() { tags.push(ti); offs.push(o); }
            times.push_range(ch.times(), 0, ch.len_());
            diffs.extend_from_slice(ch.diffs());
        }
        let kv = if total == 0 { CValue::Unit(0) } else { gather_lanes(&srcs, &tags, &offs) };
        (kv, times, diffs)
    }

    fn len_(&self) -> usize { self.0.times.len() }
}

/// The `u64` leaf lanes of a column that is nothing but nested products of `u64` leaves and units,
/// in structural order — the `(key, val)` column of an integer-keyed arrangement — or `None`.
/// Structural order on such a column is lexicographic over these lanes.
fn u64_lanes(v: &CValue) -> Option<Vec<&[u64]>> {
    fn walk<'a>(v: &'a CValue, out: &mut Vec<&'a [u64]>) -> bool {
        match v {
            CValue::Prod(fields) => fields.iter().all(|f| walk(f, out)),
            CValue::Unit(_) => true,
            leaf => match corgi::arrange::leaf_slice(leaf) {
                Some(xs) => { out.push(xs); true }
                None => false,
            },
        }
    }
    let mut out = Vec::new();
    walk(v, &mut out).then_some(out)
}

impl<T, R> Chunk for CorgiChunk<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + 'static,
{
    type Time = T;
    const TARGET: usize = TARGET;

    fn len(&self) -> usize { self.0.times.len() }

    /// Merge of the two front chunks through their shared horizon, FULLY consolidating equal
    /// `(key, val, time)` triples and pushing back the survivor's suffix (the fueled-merger
    /// contract). Batched: corgi's `survey` reports the interleaving of the two `(key, val)`
    /// columns as maximal ranges exclusive to one side and matched pairs, so rows only one side
    /// holds are copied by range, and only the `(key, val)` groups both sides hold — where
    /// consolidation can happen — are merged row by row, on their times, which corgi does not
    /// own. A matched pair names its two groups (`group_bounds` on each side); the groups'
    /// remaining rows surface in later exclusive runs and are skipped there, having been emitted
    /// with their pair. The shape is walked a few times per chunk, never once per row.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let c1 = in1.pop_front().unwrap();
        let c2 = in2.pop_front().unwrap();
        let (kv1, kv2) = (c1.kv(), c2.kv());
        let (n1, n2) = (c1.len_(), c2.len_());
        let (t1, d1) = (c1.times(), c1.diffs());
        let (t2, d2) = (c2.times(), c2.diffs());

        let runs = survey(&kv1, &kv2);
        // A matched pair names the heads of two equal-`(key, val)` groups; each group extends
        // forward while its rows stay equal to the head — a scan over the matched rows only, on
        // the `u64` lanes directly where the column is a product of them, else structurally.
        let (l1, l2) = (u64_lanes(&kv1), u64_lanes(&kv2));
        let group_end = |kv: &CValue, lanes: &Option<Vec<&[u64]>>, n: usize, head: usize| -> usize {
            let mut end = head + 1;
            match lanes {
                Some(lanes) => while end < n && lanes.iter().all(|lane| lane[end] == lane[head]) { end += 1; },
                None => while end < n && compare_at(kv, head, kv, end) == Ordering::Equal { end += 1; },
            }
            end
        };

        let (mut tags, mut offs) = (Vec::with_capacity(n1 + n2), Vec::with_capacity(n1 + n2));
        let (mut times, mut diffs): (ColTimes<T>, Vec<R>) = (ColTimes::new(), Vec::with_capacity(n1 + n2));
        // Everything below these marks was emitted with a matched group pair; an exclusive run
        // that starts under a mark is that pair's tail and skips to it.
        let (mut done1, mut done2) = (0usize, 0usize);
        // Where the survivor's pushed-back suffix starts: the last run, if it is exclusive.
        let (mut back1, mut back2) = (n1, n2);
        let copy = |tags: &mut Vec<usize>, offs: &mut Vec<usize>, times: &mut ColTimes<T>, diffs: &mut Vec<R>, side: usize, lo: usize, hi: usize| {
            let (t, d) = if side == 0 { (t1, d1) } else { (t2, d2) };
            tags.resize(tags.len() + (hi - lo), side);
            offs.extend(lo..hi);
            times.push_range(t, lo, hi);
            diffs.extend_from_slice(&d[lo..hi]);
        };
        for (r, run) in runs.iter().enumerate() {
            let last = r + 1 == runs.len();
            match *run {
                Run::A(lo, hi) => {
                    let lo = lo.max(done1);
                    if lo >= hi { continue; }
                    if last { back1 = lo; } else { copy(&mut tags, &mut offs, &mut times, &mut diffs, 0, lo, hi); }
                }
                Run::B(lo, hi) => {
                    let lo = lo.max(done2);
                    if lo >= hi { continue; }
                    if last { back2 = lo; } else { copy(&mut tags, &mut offs, &mut times, &mut diffs, 1, lo, hi); }
                }
                Run::Both(ia, ib) => {
                    if ia < done1 { continue; } // a later pair of a group pair already merged
                    debug_assert!(ib >= done2, "survey paired a row of a group already merged");
                    let ((a_lo, a_hi), (b_lo, b_hi)) = ((ia, group_end(&kv1, &l1, n1, ia)), (ib, group_end(&kv2, &l2, n2, ib)));
                    // Both groups hold one `(key, val)`, sorted by time: merge on time, summing the
                    // diffs of equal times, which is the consolidation.
                    let (mut i, mut j) = (a_lo, b_lo);
                    while i < a_hi && j < b_hi {
                        match t1.cmp_cross(i, t2, j) {
                            Ordering::Less => { copy(&mut tags, &mut offs, &mut times, &mut diffs, 0, i, i + 1); i += 1; }
                            Ordering::Greater => { copy(&mut tags, &mut offs, &mut times, &mut diffs, 1, j, j + 1); j += 1; }
                            Ordering::Equal => {
                                let mut d = d1[i].clone();
                                d.plus_equals(&d2[j]);
                                if !d.is_zero() { tags.push(0); offs.push(i); times.push_ref(t1, i); diffs.push(d); }
                                i += 1;
                                j += 1;
                            }
                        }
                    }
                    if i < a_hi { copy(&mut tags, &mut offs, &mut times, &mut diffs, 0, i, a_hi); }
                    if j < b_hi { copy(&mut tags, &mut offs, &mut times, &mut diffs, 1, j, b_hi); }
                    done1 = a_hi;
                    done2 = b_hi;
                }
            }
        }

        let srcs = [Some(&kv1), Some(&kv2)];
        Self::emit(&srcs, &tags, &offs, &times, &diffs, out);

        // Push back the survivor's unconsumed suffix (all beyond the horizon), ahead of its deque.
        if back1 < n1 {
            let idx: Vec<usize> = (back1..n1).collect();
            let mut t = ColTimes::new();
            t.push_range(t1, back1, n1);
            in1.push_front(Self::from_kv(gather(&kv1, &idx), t, d1[back1..].to_vec()));
        }
        if back2 < n2 {
            let idx: Vec<usize> = (back2..n2).collect();
            let mut t = ColTimes::new();
            t.push_range(t2, back2, n2);
            in2.push_front(Self::from_kv(gather(&kv2, &idx), t, d2[back2..].to_vec()));
        }
    }

    fn extract(
        input: &mut VecDeque<Self>,
        frontier: AntichainRef<T>,
        residual: &mut Antichain<T>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        // One input chunk per call: partition into keep (`>= frontier`) and ship pieces via `gather`.
        let Some(chunk) = input.pop_front() else { return };
        let kv = chunk.kv();
        let (times, diffs) = (chunk.times(), chunk.diffs());
        let (mut ki, mut si) = (Vec::new(), Vec::new());
        for i in 0..chunk.len_() {
            let ti = times.get(i);
            if frontier.less_equal(&ti) { residual.insert_ref(&ti); ki.push(i); } else { si.push(i); }
        }
        if !ki.is_empty() {
            let mut t = ColTimes::new();
            for &i in &ki { t.push_ref(times, i); }
            let d: Vec<R> = ki.iter().map(|&i| diffs[i].clone()).collect();
            keep.push_back(Self::from_kv(gather(&kv, &ki), t, d));
        }
        if !si.is_empty() {
            let mut t = ColTimes::new();
            for &i in &si { t.push_ref(times, i); }
            let d: Vec<R> = si.iter().map(|&i| diffs[i].clone()).collect();
            ship.push_back(Self::from_kv(gather(&kv, &si), t, d));
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
        let chunks: Vec<Self> = input.drain(..).collect();
        let (ckv, ctimes, cdiffs) = Self::concat(&chunks);
        let n = ctimes.len();
        if n == 0 { return; }

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
            input.push_front(Self::from_kv(gather(&ckv, &idx), ct, cdiffs[end..].to_vec()));
        }

        // Advance + consolidate each complete group; emit `TARGET`-sized chunks. All rows of a group
        // share `(key, val)`, so one representative offset materializes each output row's kv. Times are
        // materialized here (owned `T`) because `advance_by` mutates and the tiebreak re-sort is a Rust
        // sort — the compaction path, not the merge hot path.
        let srcs = [Some(&ckv)];
        let (mut tags, mut offs) = (Vec::new(), Vec::new());
        let (mut otimes, mut odiffs): (ColTimes<T>, Vec<R>) = (ColTimes::new(), Vec::new());
        // A batch holds few distinct times (one epoch, a handful of iterations), so each is
        // advanced ONCE. `seen` keeps the distinct source times as refs, `order` a permutation of
        // them sorted for binary search, `advanced[j]` the advanced form of `seen[j]`. Rows carry
        // an index into `advanced`: no `T` is materialized, joined, or cloned per row, and the
        // `PointStamp` allocations that go with each of those happen once per distinct time.
        let mut seen: ColTimes<T> = ColTimes::new();
        let mut order: Vec<usize> = Vec::new();
        let mut advanced: Vec<T> = Vec::new();
        let mut last = usize::MAX;
        // One scratch buffer for every group: most groups are a row or two, and a fresh `Vec`
        // per group was an allocation per row.
        let mut pairs: Vec<(usize, R)> = Vec::new();
        let mut i = 0;
        for &g_end in &bounds {
            if g_end > end { break; }
            pairs.clear();
            for k in i..g_end {
                // Sorted input repeats the previous row's time far more often than not.
                let j = if last != usize::MAX && ctimes.cmp_cross(k, &seen, last) == Ordering::Equal {
                    last
                } else {
                    match order.binary_search_by(|&j| seen.cmp_cross(j, &ctimes, k)) {
                        Ok(pos) => order[pos],
                        Err(pos) => {
                            let mut t = ctimes.get(k);
                            t.advance_by(frontier);
                            let j = advanced.len();
                            seen.push_ref(&ctimes, k);
                            advanced.push(t);
                            order.insert(pos, j);
                            j
                        }
                    }
                };
                last = j;
                pairs.push((j, cdiffs[k].clone()));
            }
            // Distinct source times may advance to one time; coalesce by the advanced value.
            pairs.sort_by(|a, b| advanced[a.0].cmp(&advanced[b.0]));
            let mut k = 0;
            while k < pairs.len() {
                let t = &advanced[pairs[k].0];
                let mut d = pairs[k].1.clone();
                k += 1;
                while k < pairs.len() && advanced[pairs[k].0] == *t { d.plus_equals(&pairs[k].1); k += 1; }
                if !d.is_zero() {
                    tags.push(0); offs.push(i); otimes.push(t); odiffs.push(d);
                    if otimes.len() >= TARGET {
                        Self::emit(&srcs, &tags, &offs, &otimes, &odiffs, out);
                        tags.clear(); offs.clear(); otimes.clear(); odiffs.clear();
                    }
                }
            }
            i = g_end;
        }
        if !otimes.is_empty() { Self::emit(&srcs, &tags, &offs, &otimes, &odiffs, out); }
    }

    /// Maximal packing via the harness [`pack`]: coalesce by concatenating columns (`gather_lanes`),
    /// split with `gather`, seal as a no-op (corgi chunks are never paged here).
    fn settle(input: &mut VecDeque<Self>, done: bool, out: &mut VecDeque<Self>) {
        pack(
            input,
            done,
            out,
            |acc, next| {
                let (na, nb) = (acc.len_(), next.len_());
                let kvs = [acc.kv(), next.kv()];
                let srcs = [Some(&kvs[0]), Some(&kvs[1])];
                let mut tags = Vec::with_capacity(na + nb);
                let mut offs = Vec::with_capacity(na + nb);
                for o in 0..na { tags.push(0); offs.push(o); }
                for o in 0..nb { tags.push(1); offs.push(o); }
                let kv = gather_lanes(&srcs, &tags, &offs);
                let mut times = ColTimes::new();
                times.push_range(acc.times(), 0, na);
                times.push_range(next.times(), 0, nb);
                let mut diffs = acc.diffs().to_vec();
                diffs.extend_from_slice(next.diffs());
                *acc = Self::from_kv(kv, times, diffs);
            },
            |chunk, m| {
                let kv = chunk.kv();
                let n = chunk.len_();
                let left: Vec<usize> = (0..m).collect();
                let right: Vec<usize> = (m..n).collect();
                let (mut lt, mut rt) = (ColTimes::new(), ColTimes::new());
                lt.push_range(chunk.times(), 0, m);
                rt.push_range(chunk.times(), m, n);
                let l = Self::from_kv(gather(&kv, &left), lt, chunk.diffs()[..m].to_vec());
                let r = Self::from_kv(gather(&kv, &right), rt, chunk.diffs()[m..].to_vec());
                (l, r)
            },
            |chunk| chunk,
        );
    }
}

/// Sort parallel columns by `(key, val, time)` and consolidate exact `(key, val, time)` triples
/// (summing diffs, dropping zeros). Returns a sorted+consolidated `(keys, vals, times, diffs)`.
///
/// Multi-record: one columnar `sort_perm` (discrimination sort) orders by `(key, val)`, one batched
/// `compare_adjacent` flags adjacent-equal runs; only the small per-run *time* tiebreak is a Rust sort
/// (time is not a corgi type). No per-pair `compare_at`.
fn sort_consolidate<T, R>(keys: CValue, vals: CValue, times: Vec<T>, diffs: Vec<R>) -> (CValue, CValue, Vec<T>, Vec<R>)
where
    T: Ord + Clone + Columnar,
    R: Semigroup + Clone,
{
    let n = times.len();
    if n == 0 {
        return (keys, vals, times, diffs);
    }
    let kv = CValue::Prod(vec![keys, vals]);
    // Batched argsort by (key, val); reorder the parallel Rust columns by the same permutation.
    let perm = sort_perm(&kv);
    let kv_s = gather(&kv, &perm);
    let times_s: Vec<T> = perm.iter().map(|&i| times[i].clone()).collect();
    let diffs_s: Vec<R> = perm.iter().map(|&i| diffs[i].clone()).collect();
    // Batched adjacent-equality over the kv-sorted column: `adj[m] == 0` iff `kv_s[m] == kv_s[m+1]`.
    // Naming the pattern rather than writing out the two index columns: corgi reads both sides
    // densely, and the `i`/`i+1` index vectors this used to build are not built at all.
    let adj: Vec<i8> = compare_adjacent(&kv_s);

    // Walk maximal equal-`(key,val)` runs; within each, order by time and consolidate equal times.
    let (mut keep, mut ot, mut od) = (Vec::new(), Vec::new(), Vec::new());
    let mut i = 0;
    while i < n {
        let mut j = i + 1;
        while j < n && adj[j - 1] == 0 {
            j += 1;
        }
        let mut run: Vec<usize> = (i..j).collect();
        run.sort_by(|&a, &b| times_s[a].cmp(&times_s[b]));
        let mut k = 0;
        while k < run.len() {
            let rep = run[k];
            let t = times_s[rep].clone();
            let mut d = diffs_s[rep].clone();
            k += 1;
            while k < run.len() && times_s[run[k]] == t {
                d.plus_equals(&diffs_s[run[k]]);
                k += 1;
            }
            if !d.is_zero() {
                keep.push(rep);
                ot.push(t);
                od.push(d);
            }
        }
        i = j;
    }
    let (keys, vals) = split_kv(gather(&kv_s, &keep));
    (keys, vals, ot, od)
}

impl<T, R> CorgiChunk<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + 'static,
{
    /// One sorted+consolidated chunk from columns already in corgi form (the column-native arrange
    /// ingest — no transcode).
    pub fn from_columns(keys: CValue, vals: CValue, times: Vec<T>, diffs: Vec<R>) -> Self {
        let (keys, vals, times, diffs) = sort_consolidate(keys, vals, times, diffs);
        debug_assert!({
            let lane = corgi::arrange::leaf_slice(key_lane(&keys));
            lane.is_some_and(|ids| ids.windows(2).all(|pair| pair[0] <= pair[1]))
        }, "arrangement key must lead with a sorted u64 identifier lane");
        Self::from_parts(keys, vals, ColTimes::from_iter(times), diffs)
    }

}

/// Concatenate chunks' columns into flat `(keys, vals, times, diffs)` with **no transcode** — for
/// reading an arrangement back column-natively (e.g. `Backend::as_collection` straight into a
/// Build a `ChunkBatch<CorgiChunk>` from corgi key/val COLUMNS directly (no transcode): sort +
/// consolidate into one chunk, then `settle`. The column-native egress the reduce backend seals its
/// output with (it resolves proxy ids to real columns by `gather` and hands them here).
pub fn columns_to_batch<T, R>(keys: CValue, vals: CValue, times: Vec<T>, diffs: Vec<R>) -> ChunkBatch<CorgiChunk<T, R>>
where
    T: ColTime,
    R: Semigroup + Clone + 'static,
{
    let chunk = CorgiChunk::from_columns(keys, vals, times, diffs);
    settle_one(chunk)
}

/// Grade one chunk into a `ChunkBatch` (shared tail of `rows_to_batch`/`columns_to_batch`).
fn settle_one<T, R>(chunk: CorgiChunk<T, R>) -> ChunkBatch<CorgiChunk<T, R>>
where
    T: ColTime,
    R: Semigroup + Clone + 'static,
{
    let mut input = VecDeque::new();
    if chunk.len_() > 0 { input.push_back(chunk); }
    let mut output = VecDeque::new();
    CorgiChunk::settle(&mut input, true, &mut output);
    ChunkBatch::new(output.into())
}

/// A column-native arrange **chunker**: turns input `CorgiContainer`s into sorted+consolidated
/// `CorgiChunk`s WITHOUT `ContainerChunker`'s drain-to-rows (which untranscodes). Paired with the
/// standard `ChunkBatcher`/`ChunkBuilder`, the arrange ingest stays column-native — no
/// columns→rows→columns round-trip at the arrangement boundary.
///
/// Crucially it **accumulates to `TARGET`** before consolidating (like `ContainerChunker`), so it
/// emits few large chunks rather than one tiny chunk per input container — otherwise the columnar
/// per-chunk set-up (`gather`/`sort_perm`) dominates when input arrives as many small batches.
pub struct CorgiChunker<T: Columnar, R> {
    /// Un-consolidated key/val column blocks (one per absorbed container), flat time/diff.
    k_blocks: Vec<CValue>,
    v_blocks: Vec<CValue>,
    times: Vec<T>,
    diffs: Vec<R>,
    ready: VecDeque<CorgiChunk<T, R>>,
    current: Option<CorgiChunk<T, R>>,
}

impl<T: Columnar, R> Default for CorgiChunker<T, R> {
    fn default() -> Self {
        CorgiChunker { k_blocks: Vec::new(), v_blocks: Vec::new(), times: Vec::new(), diffs: Vec::new(), ready: VecDeque::new(), current: None }
    }
}

/// An arrangement's key column, in the form every consumer of a `CorgiChunk` can rely on:
/// **it leads with an integer, and the chunk is sorted by that integer.**
///
/// A key that is already a primitive integer (a bare 64-bit `Prim`, or the 1-field `Prod` that
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
/// The hash is computed ONCE here, at ingest, and thereafter moves as data: `merge`, `advance`
/// and `settle` permute key columns with `gather_lanes`, so no transducer recomputes it.
pub fn present_key(keys: CValue) -> CValue {
    if corgi::arrange::leaf_slice(&keys).is_some() {
        return keys;
    }
    let hashes = corgi::hash(&keys);
    CValue::Prod(vec![CValue::u64(hashes), keys])
}

/// The integer identifier of each row of a [`present_key`] column: the key's own values when it is
/// a primitive integer, and the prepended hash lane otherwise. Never re-hashes.
pub fn key_ids(keys: &CValue) -> Vec<u64> {
    if let Some(sl) = corgi::arrange::leaf_slice(keys) {
        return sl.to_vec();
    }
    corgi::arrange::leaf_slice(key_lane(keys)).expect("a prepended hash lane is a u64 leaf").to_vec()
}

/// The single column an arrangement is sorted by, for seeking: the key itself when it is a
/// primitive integer, else the prepended hash lane. Always a bare `u64` leaf, so `find_ranges`
/// over it takes corgi's `u64` fast path whatever the underlying key shape.
///
/// One rule covers both forms, because [`present_key`] leaves exactly three possibilities: a bare
/// `Prim`, the 1-field `Prod` that also counts as primitive, or a prepended `Prod([hash, key])`.
/// The leading field is the identifier in all three.
pub fn key_lane(keys: &CValue) -> &CValue {
    match keys {
        CValue::Prod(cols) => &cols[0],
        _ => keys,
    }
}

/// Whether [`present_key`] prepended a hash to this key — i.e. whether rows sharing an identifier
/// may hold DIFFERENT keys. False for primitive-integer keys, whose identifier is injective, so
/// readers can skip the checks that guard against collisions entirely.
pub fn key_is_hashed(keys: &CValue) -> bool {
    corgi::arrange::leaf_slice(keys).is_none()
}

/// Undo [`present_key`]: the key as the rest of the system knows it. A corgi clone is an `Arc`
/// bump, so dropping the hash lane costs nothing.
pub fn recover_key(keys: &CValue) -> CValue {
    match keys {
        CValue::Prod(cols) if corgi::arrange::leaf_slice(keys).is_none() => cols[1].clone(),
        _ => keys.clone(),
    }
}

/// Concatenate column blocks into one column (multi-source `gather_lanes`, no sort).
fn concat_blocks(blocks: &[CValue]) -> CValue {
    if blocks.len() == 1 {
        return blocks[0].clone();
    }
    let srcs: Vec<Option<&CValue>> = blocks.iter().map(Some).collect();
    let (mut tags, mut offs) = (Vec::new(), Vec::new());
    for (ti, b) in blocks.iter().enumerate() {
        for o in 0..b.len() { tags.push(ti); offs.push(o); }
    }
    gather_lanes(&srcs, &tags, &offs)
}

impl<T, R> CorgiChunker<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + 'static,
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

impl<T, R> timely::container::PushInto<&mut crate::corgi::container::CorgiContainer<T, R>> for CorgiChunker<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + 'static,
{
    fn push_into(&mut self, c: &mut crate::corgi::container::CorgiContainer<T, R>) {
        if c.times.is_empty() {
            return;
        }
        self.k_blocks.push(std::mem::replace(&mut c.keys, CValue::Unit(0)));
        self.v_blocks.push(std::mem::replace(&mut c.vals, CValue::Unit(0)));
        self.times.append(&mut c.times);
        self.diffs.append(&mut c.diffs);
        if self.times.len() >= TARGET {
            self.flush();
        }
    }
}

impl<T, R> timely::container::ContainerBuilder for CorgiChunker<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + 'static,
{
    type Container = CorgiChunk<T, R>;
    // `extract` ships ready chunks, leaving the sub-TARGET remainder to accumulate further.
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
    use differential_dataflow::trace::chunk::{ChunkBatchMerger, is_graded};
        use differential_dataflow::trace::implementations::spine_fueled::Merger;
    use std::collections::BTreeMap;

    fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

    /// Build a single sorted+consolidated CorgiChunk from u64 (key,val,time,diff) rows.
    fn chunk(rows: &[((u64, u64), u64, i64)]) -> CorgiChunk<u64, i64> {
        // Sort + consolidate by ((k,v),t) so a chunk is a legal sorted run.
        let mut m: BTreeMap<((u64, u64), u64), i64> = BTreeMap::new();
        for &(kv, t, d) in rows { *m.entry((kv, t)).or_insert(0) += d; }
        m.retain(|_, d| *d != 0);
        let keys = CValue::u64(m.keys().map(|((k, _), _)| *k).collect());
        let vals = CValue::u64(m.keys().map(|((_, v), _)| *v).collect());
        let times: ColTimes<u64> = m.keys().map(|(_, t)| *t).collect();
        let diffs = m.values().copied().collect();
        CorgiChunk::from_parts(keys, vals, times, diffs)
    }

    fn read_batch(b: &ChunkBatch<CorgiChunk<u64, i64>>) -> BTreeMap<((u64, u64), u64), i64> {
        let mut m = BTreeMap::new();
        for ch in &b.chunks {
            let ks = ch.keys().clone().into_u64("k").unwrap();
            let vs = ch.vals().clone().into_u64("v").unwrap();
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

#[cfg(test)]
mod merge_tests {
    //! The batched merge against the definition: the emitted rows are sorted and consolidated,
    //! the pushed-back suffix lies beyond them on one side only, and together they are the
    //! consolidated union of the two inputs. Small key and value universes so that groups collide
    //! across sides, several times per group, and cancellations.
    use super::*;
    use std::collections::BTreeMap;
    use differential_dataflow::dynamic::pointstamp::PointStamp;
    use timely::order::Product;

    type T = Product<u64, PointStamp<u64>>;
    type Row = (u64, u64, T, i64);

    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }
    }

    fn time(outer: u64, inner: u64) -> T {
        Product::new(outer, PointStamp::new([inner].into_iter().collect()))
    }

    fn chunk(rows: &[Row]) -> CorgiChunk<T, i64> {
        CorgiChunk::from_columns(
            CValue::u64(rows.iter().map(|r| r.0).collect()),
            CValue::u64(rows.iter().map(|r| r.1).collect()),
            rows.iter().map(|r| r.2.clone()).collect(),
            rows.iter().map(|r| r.3).collect(),
        )
    }

    fn rows_of(c: &CorgiChunk<T, i64>) -> Vec<Row> {
        let keys = corgi::arrange::leaf_slice(c.keys()).expect("u64 keys");
        let vals = corgi::arrange::leaf_slice(c.vals()).expect("u64 vals");
        (0..c.len_()).map(|i| (keys[i], vals[i], c.times().get(i), c.diffs()[i])).collect()
    }

    fn consolidated(rows: impl Iterator<Item = Row>) -> BTreeMap<(u64, u64, T), i64> {
        let mut out = BTreeMap::new();
        for (k, v, t, d) in rows {
            *out.entry((k, v, t)).or_insert(0) += d;
        }
        out.retain(|_, d| *d != 0);
        out
    }

    #[test]
    fn merge_is_the_consolidated_union_with_a_pushed_back_suffix() {
        let mut g = Lcg(5);
        for _ in 0..2000 {
            let mut make = |g: &mut Lcg| -> Vec<Row> {
                (0..g.next(14)).map(|_| (g.next(4), g.next(3), time(g.next(2), g.next(3)), if g.next(4) == 0 { -1 } else { 1 })).collect()
            };
            let (a, b) = (chunk(&make(&mut g)), chunk(&make(&mut g)));
            let expect = consolidated(rows_of(&a).into_iter().chain(rows_of(&b)));
            let (mut in1, mut in2, mut out) = (VecDeque::from([a]), VecDeque::from([b]), VecDeque::new());
            <CorgiChunk<T, i64> as Chunk>::merge(&mut in1, &mut in2, &mut out);
            let emitted: Vec<Row> = out.iter().flat_map(rows_of).collect();
            for w in emitted.windows(2) {
                assert!((w[0].0, w[0].1, &w[0].2) < (w[1].0, w[1].1, &w[1].2), "emitted rows out of order or repeated: {:?} then {:?}", w[0], w[1]);
            }
            assert!(emitted.iter().all(|r| r.3 != 0), "a zero diff was emitted");
            assert!(in1.is_empty() || in2.is_empty(), "both sides pushed back");
            let back: Vec<Row> = in1.iter().chain(in2.iter()).flat_map(rows_of).collect();
            if let Some(last) = emitted.last() {
                for r in &back {
                    assert!((r.0, r.1, &r.2) > (last.0, last.1, &last.2), "pushed-back row {:?} is not beyond the last emitted {:?}", r, last);
                }
            }
            assert_eq!(consolidated(emitted.into_iter().chain(back)), expect);
        }
    }
}
