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
use differential_dataflow::trace::Description;

use corgi::arrange::{compare_at, compare_idx, gather, gather_lanes, group_bounds, sort_perm, survey_groups, GroupRun};
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
    let mut cols = kv.into_prod("corgi chunk kv");
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
        let mut kvs: Vec<CValue> = chunks.iter().map(Self::kv).collect();
        unify_all(&mut kvs);
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

impl<T, R> Chunk for CorgiChunk<T, R>
where
    T: ColTime,
    R: Semigroup + Clone + 'static,
{
    type Time = T;
    const TARGET: usize = TARGET;

    fn len(&self) -> usize { self.0.times.len() }

    /// Two-pointer merge of the two front chunks through their shared horizon, FULLY consolidating
    /// equal `(key, val, time)` triples and pushing back the survivor's suffix (the fueled-merger
    /// contract). NB a `survey`-based merge cannot be substituted as-is: survey aligns only
    /// `(key, val)` (corgi owns no time), so its positional `Both` under-consolidates cross-side
    /// times, and consuming both chunks with no push-back un-grades the chain; it would need a
    /// group-RANGE `Both` (both sides' full equal-`(key,val)` group) to time-merge correctly.
    ///
    /// TODO: newer corgi revs export exactly that (`survey_groups` -> `GroupRun::{A, B, Both}`,
    /// with `Both` carrying both sides' group ranges). Once the pin moves past it, rewrite this
    /// merge batched: bulk-copy `A`/`B` runs (range gather + `push_range` + `extend_from_slice`),
    /// row-merge times only within `Both` classes. This row-at-a-time loop is the largest
    /// `compare_at`-in-a-hot-path residue in the backend (the ingest batcher's merge).
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let c1 = in1.pop_front().unwrap();
        let c2 = in2.pop_front().unwrap();
        let (mut kv1, mut kv2) = (c1.kv(), c2.kv());
        // Chunks are sealed by separate flushes, so their inferred arities can differ; the
        // compare below is undefined on a tag with no lane.
        unify_sum_arity(&mut kv1, &mut kv2);
        let (n1, n2) = (c1.len_(), c2.len_());
        let (t1, d1) = (c1.times(), c1.diffs());
        let (t2, d2) = (c2.times(), c2.diffs());

        let (mut tags, mut offs) = (Vec::new(), Vec::new());
        let (mut times, mut diffs) = (ColTimes::new(), Vec::new());

        // Survey the interleaving at EQUAL-CLASS granularity: corgi does the structural compares
        // in bulk (one boundary crossing per range, not per row-pair) and hands back each matched
        // `(key, val)` class as both sides' ranges, which is what a time-carrying merge needs to
        // consolidate across sides.
        let runs = survey_groups(&kv1, &kv2);
        // A trailing exclusive run is the survivor's suffix past the shared horizon (once one side
        // is exhausted the rest of the other is one maximal run). Push it back rather than emit it,
        // so the chain stays graded — same contract as the two-pointer's `p < n` push-back.
        let split = match runs.last() {
            Some(GroupRun::A(..)) | Some(GroupRun::B(..)) => runs.len() - 1,
            _ => runs.len(),
        };
        for run in &runs[..split] {
            match *run {
                GroupRun::A(lo, hi) => for p in lo..hi {
                    tags.push(0); offs.push(p); times.push_ref(t1, p); diffs.push(d1[p].clone());
                },
                GroupRun::B(lo, hi) => for p in lo..hi {
                    tags.push(1); offs.push(p); times.push_ref(t2, p); diffs.push(d2[p].clone());
                },
                GroupRun::Both(alo, ahi, blo, bhi) => {
                    // One equal-`(key, val)` class: merge it by time, consolidating across sides.
                    let (mut p1, mut p2) = (alo, blo);
                    while p1 < ahi && p2 < bhi {
                        match t1.cmp_cross(p1, t2, p2) {
                            Ordering::Less => { tags.push(0); offs.push(p1); times.push_ref(t1, p1); diffs.push(d1[p1].clone()); p1 += 1; }
                            Ordering::Greater => { tags.push(1); offs.push(p2); times.push_ref(t2, p2); diffs.push(d2[p2].clone()); p2 += 1; }
                            Ordering::Equal => {
                                let mut d = d1[p1].clone();
                                d.plus_equals(&d2[p2]);
                                if !d.is_zero() { tags.push(0); offs.push(p1); times.push_ref(t1, p1); diffs.push(d); }
                                p1 += 1; p2 += 1;
                            }
                        }
                    }
                    for p in p1..ahi { tags.push(0); offs.push(p); times.push_ref(t1, p); diffs.push(d1[p].clone()); }
                    for p in p2..bhi { tags.push(1); offs.push(p); times.push_ref(t2, p); diffs.push(d2[p].clone()); }
                }
            }
        }

        let srcs = [Some(&kv1), Some(&kv2)];
        Self::emit(&srcs, &tags, &offs, &times, &diffs, out);

        match runs.get(split) {
            Some(&GroupRun::A(lo, hi)) => {
                let idx: Vec<usize> = (lo..hi).collect();
                let mut t = ColTimes::new();
                t.push_range(t1, lo, hi);
                in1.push_front(Self::from_kv(gather(&kv1, &idx), t, d1[lo..hi].to_vec()));
            }
            Some(&GroupRun::B(lo, hi)) => {
                let idx: Vec<usize> = (lo..hi).collect();
                let mut t = ColTimes::new();
                t.push_range(t2, lo, hi);
                in2.push_front(Self::from_kv(gather(&kv2, &idx), t, d2[lo..hi].to_vec()));
            }
            _ => {}
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
        let mut i = 0;
        for &g_end in &bounds {
            if g_end > end { break; }
            let mut pairs: Vec<(T, R)> = (i..g_end)
                .map(|k| { let mut t = ctimes.get(k); t.advance_by(frontier); (t, cdiffs[k].clone()) })
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let mut k = 0;
            while k < pairs.len() {
                let t = pairs[k].0.clone();
                let mut d = pairs[k].1.clone();
                k += 1;
                while k < pairs.len() && pairs[k].0 == t { d.plus_equals(&pairs[k].1); k += 1; }
                if !d.is_zero() {
                    tags.push(0); offs.push(i); otimes.push(&t); odiffs.push(d);
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
                let mut kvs = [acc.kv(), next.kv()];
                unify_all(&mut kvs);
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
/// `compare_idx` flags adjacent-equal runs; only the small per-run *time* tiebreak is a Rust sort
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
    let adj: Vec<i8> = if n > 1 {
        let left: Vec<usize> = (0..n - 1).collect();
        let right: Vec<usize> = (1..n).collect();
        compare_idx(&kv_s, &kv_s, &left, &right)
    } else {
        Vec::new()
    };

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
        Self::from_parts(keys, vals, ColTimes::from_iter(times), diffs)
    }

}

/// Concatenate chunks' columns into flat `(keys, vals, times, diffs)` with **no transcode** — for
/// reading an arrangement back column-natively (e.g. `Backend::as_collection` straight into a
/// Build a `ChunkBatch<CorgiChunk>` from corgi key/val COLUMNS directly (no transcode): sort +
/// consolidate into one chunk, then `settle`. The column-native egress the reduce backend seals its
/// output with (it resolves proxy ids to real columns by `gather` and hands them here).
pub fn columns_to_batch<T, R>(keys: CValue, vals: CValue, times: Vec<T>, diffs: Vec<R>, description: Description<T>) -> ChunkBatch<CorgiChunk<T, R>>
where
    T: ColTime,
    R: Semigroup + Clone + 'static,
{
    let chunk = CorgiChunk::from_columns(keys, vals, times, diffs);
    settle_one(chunk, description)
}

/// Grade one chunk into a `ChunkBatch` (shared tail of `rows_to_batch`/`columns_to_batch`).
fn settle_one<T, R>(chunk: CorgiChunk<T, R>, description: Description<T>) -> ChunkBatch<CorgiChunk<T, R>>
where
    T: ColTime,
    R: Semigroup + Clone + 'static,
{
    let mut input = VecDeque::new();
    if chunk.len_() > 0 { input.push_back(chunk); }
    let mut output = VecDeque::new();
    CorgiChunk::settle(&mut input, true, &mut output);
    ChunkBatch::new(output.into(), description)
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

/// Pad two columns' `Sum` lane vectors to their common arity, `None` (`⊥`) filling.
///
/// Shapes here are inferred per batch by `infer_shape_cols`, which commits only the arms it
/// actually SEES — so two batches of one DDIR type can disagree on arity (a batch of only
/// `Rare(_)` gives `Sum([Some])`, one of only `Common(_)` gives `Sum([None, Some])`). corgi
/// reads that as two unrelated types (`shape::join` calls a differing arity the genuine type
/// error), and `gather_lanes` then indexes a lane vector shorter than the tags it is given.
/// Widening with `⊥` is sound by corgi's own rule that an uncommitted lane holds no rows, and
/// leaves the within-variant offsets untouched (they depend only on tags and per-tag cursors).
fn unify_sum_arity(a: &mut CValue, b: &mut CValue) {
    match (a, b) {
        (CValue::Prod(xs), CValue::Prod(ys)) => {
            for (x, y) in xs.iter_mut().zip(ys.iter_mut()) { unify_sum_arity(x, y); }
        }
        (CValue::List(_, x), CValue::List(_, y)) => unify_sum_arity(x, y),
        (CValue::Sum(_, _, xs), CValue::Sum(_, _, ys)) => {
            let k = xs.len().max(ys.len());
            xs.resize(k, None);
            ys.resize(k, None);
            for (x, y) in xs.iter_mut().zip(ys.iter_mut()) {
                if let (Some(x), Some(y)) = (x, y) { unify_sum_arity(x, y); }
            }
        }
        _ => {}
    }
}

/// Bring a set of columns to a common `Sum` arity. Pairwise against column 0 twice: `resize`
/// only grows, so the first pass leaves column 0 at the running max and the second lifts the
/// rest to it.
///
/// Every site that reads two independently-inferred columns together must call this first —
/// `compare_at` as much as `gather_lanes`, since a tag with no lane has no defined order.
fn unify_all(cols: &mut [CValue]) {
    if cols.len() < 2 { return; }
    for _ in 0..2 {
        let (head, tail) = cols.split_at_mut(1);
        for c in tail.iter_mut() { unify_sum_arity(&mut head[0], c); }
    }
}

/// Concatenate column blocks into one column (multi-source `gather_lanes`, no sort).
fn concat_blocks(blocks: &mut [CValue]) -> CValue {
    if blocks.len() == 1 {
        return blocks[0].clone();
    }
    unify_all(blocks);
    let srcs: Vec<Option<&CValue>> = blocks.iter().map(|b| Some(&*b)).collect();
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
        let keys = concat_blocks(&mut self.k_blocks);
        let vals = concat_blocks(&mut self.v_blocks);
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
    use differential_dataflow::trace::{Description, Merger};
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
            let ks = ch.keys().clone().into_u64("k");
            let vs = ch.vals().clone().into_u64("v");
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
        let desc = Description::new(Antichain::from_elem(0u64), Antichain::from_elem(10u64), Antichain::from_elem(0u64));
        ChunkBatch::new(chunks, desc)
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
            assert!(is_graded(&result.chunks), "ungraded: {:?}", result.chunks.iter().map(Chunk::len).collect::<Vec<_>>());
            assert_eq!(read_batch(&result), reference(&u1, &u2, f), "u1={u1:?}\nu2={u2:?}\nf={f}");
        }
    }
}
