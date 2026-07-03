//! `CorgiChunk`: a [`Chunk`](differential_dataflow::trace::chunk::Chunk) whose key/val payload is a
//! pair of corgi columns (time/diff stay Rust `Vec`s). It is a **`Chunk` but NOT `NavigableChunk`**:
//! it exposes no `Ord` key, no cursor, no sorted-trie layout — the merge/advance/settle transducers
//! drive everything through corgi's own structural order (`compare_at`) and gather primitives
//! (`gather`, `gather_lanes`). Consumption is the tactics' job (they read the columns in bulk), which
//! is exactly why cursor-less `Chunk` suffices here.
//!
//! This is a faithful port of the reference [`VecChunk`](differential_dataflow::trace::chunk::vec):
//! same resumable merge→advance→settle pipeline and grade-at-yield invariant, with the flat
//! `Rc<Vec<row>>` swapped for corgi columns. Adopting the `Chunk` framework gives us the fueled,
//! graded `ChunkBatchMerger` for free — replacing the eager whole-trace `CorgiMerger`.
//!
//! Order: `(key, val)` by corgi structural order (`compare_at` over `Prod([keys, vals])`), then `time`
//! by `Ord`. Any consistent total order is fine — correctness compares multisets, not DDIR's `Ord`.
//!
//! Simplification vs `VecChunk`: `merge` processes only the two front chunks per call (no mid-merge
//! refill), so `gather_lanes` source indices stay valid for the whole call. The `Chunk` contract
//! permits this — "consume at least one input; the harness may re-invoke."

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::AntichainRef;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::chunk::{merge_chains, pack, Chunk, ChunkBatch};
use differential_dataflow::trace::{Builder as DdBuilder, Description};

use corgi::arrange::{compare_at, compare_idx, find_ranges, gather, gather_lanes, sort_perm};
use corgi::Value as CValue;

use crate::corgi_logic::{infer_shape_cols, transcode, untranscode};
use crate::ir::Value as DValue;

use std::cmp::Ordering;

/// A DDIR row update: `((key, val), time, diff)`.
pub type Upd<T, R> = ((DValue, DValue), T, R);

/// The grading target (also the merge/advance emit-chunk size). Larger than `VecChunk`'s 8192 to
/// amortize corgi's per-chunk columnar set-up (each chunk boundary costs a `gather` materialization);
/// bigger chunks mean fewer boundaries. Bounded so the fueled merger still yields.
const TARGET: usize = 1 << 18;

/// Shared, immutable chunk contents. `Clone` of a `CorgiChunk` is an `Rc` bump.
struct Inner<T, R> {
    /// Key column (corgi), aligned with `vals`/`times`/`diffs`, sorted by `(key, val, time)`.
    keys: CValue,
    /// Val column (corgi).
    vals: CValue,
    /// Per-update times (the lattice algebra lives here; corgi never sees time).
    times: Vec<T>,
    /// Per-update diffs.
    diffs: Vec<R>,
}

/// A sorted, consolidated run of `((key, val), time, diff)` with corgi-columnar key/val, shared via `Rc`.
pub struct CorgiChunk<T, R>(Rc<Inner<T, R>>);

impl<T, R> Clone for CorgiChunk<T, R> {
    fn clone(&self) -> Self { CorgiChunk(Rc::clone(&self.0)) }
}

impl<T, R> Default for CorgiChunk<T, R> {
    fn default() -> Self {
        CorgiChunk(Rc::new(Inner { keys: CValue::Unit(0), vals: CValue::Unit(0), times: Vec::new(), diffs: Vec::new() }))
    }
}

/// Split a `Prod([keys, vals])` corgi value into its two columns.
fn split_kv(kv: CValue) -> (CValue, CValue) {
    let mut cols = kv.into_prod("corgi chunk kv");
    let vals = cols.pop().unwrap();
    let keys = cols.pop().unwrap();
    (keys, vals)
}

impl<T: Clone, R: Clone> CorgiChunk<T, R> {
    fn from_parts(keys: CValue, vals: CValue, times: Vec<T>, diffs: Vec<R>) -> Self {
        CorgiChunk(Rc::new(Inner { keys, vals, times, diffs }))
    }
    /// The `(key, val)` sort payload as one corgi `Prod` column (cheap `Arc` bumps).
    fn kv(&self) -> CValue { CValue::Prod(vec![self.0.keys.clone(), self.0.vals.clone()]) }
    fn from_kv(kv: CValue, times: Vec<T>, diffs: Vec<R>) -> Self {
        let (keys, vals) = split_kv(kv);
        Self::from_parts(keys, vals, times, diffs)
    }
    pub fn keys(&self) -> &CValue { &self.0.keys }
    pub fn vals(&self) -> &CValue { &self.0.vals }
    pub fn times(&self) -> &[T] { &self.0.times }
    pub fn diffs(&self) -> &[R] { &self.0.diffs }
}

/// Total order used throughout: `(key, val)` structurally, then `time`.
#[inline]
fn cmp_at<T: Ord>(a_kv: &CValue, ai: usize, at: &T, b_kv: &CValue, bi: usize, bt: &T) -> Ordering {
    compare_at(a_kv, ai, b_kv, bi).then_with(|| at.cmp(bt))
}

impl<T, R> CorgiChunk<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    /// Materialize `[start, end)` of accumulated `(tag → src, off, time, diff)` into `TARGET`-sized
    /// output chunks. `srcs` are the (stable) source kv columns the tags/offs index into.
    fn emit(
        srcs: &[Option<&CValue>],
        tags: &[usize],
        offs: &[usize],
        times: &[T],
        diffs: &[R],
        out: &mut VecDeque<Self>,
    ) {
        let n = times.len();
        let mut s = 0;
        while s < n {
            let e = (s + TARGET).min(n);
            let kv = gather_lanes(srcs, &tags[s..e], &offs[s..e]);
            out.push_back(Self::from_kv(kv, times[s..e].to_vec(), diffs[s..e].to_vec()));
            s = e;
        }
    }

    /// Concatenate a run of (globally-sorted) chunks into one combined `(kv, times, diffs)`.
    fn concat(chunks: &[Self]) -> (CValue, Vec<T>, Vec<R>) {
        let kvs: Vec<CValue> = chunks.iter().map(Self::kv).collect();
        let srcs: Vec<Option<&CValue>> = kvs.iter().map(Some).collect();
        let total: usize = chunks.iter().map(Self::len_).sum();
        let (mut tags, mut offs) = (Vec::with_capacity(total), Vec::with_capacity(total));
        let (mut times, mut diffs) = (Vec::with_capacity(total), Vec::with_capacity(total));
        for (ti, ch) in chunks.iter().enumerate() {
            for o in 0..ch.len_() { tags.push(ti); offs.push(o); }
            times.extend_from_slice(ch.times());
            diffs.extend_from_slice(ch.diffs());
        }
        let kv = if total == 0 { CValue::Unit(0) } else { gather_lanes(&srcs, &tags, &offs) };
        (kv, times, diffs)
    }

    fn len_(&self) -> usize { self.0.times.len() }
}

impl<T, R> Chunk for CorgiChunk<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    type Time = T;
    const TARGET: usize = TARGET;

    fn len(&self) -> usize { self.0.times.len() }

    /// Two-pointer merge of the two front chunks through their shared horizon, consolidating equal
    /// `(key, val, time)` triples. Accumulates `(tag, off)` against the two stable source kv columns
    /// and materializes output via `gather_lanes`; the survivor's suffix is pushed back once.
    fn merge(in1: &mut VecDeque<Self>, in2: &mut VecDeque<Self>, out: &mut VecDeque<Self>) {
        let c1 = in1.pop_front().unwrap();
        let c2 = in2.pop_front().unwrap();
        let (kv1, kv2) = (c1.kv(), c2.kv());
        let (n1, n2) = (c1.len_(), c2.len_());
        let (t1, d1) = (c1.times(), c1.diffs());
        let (t2, d2) = (c2.times(), c2.diffs());

        let (mut tags, mut offs) = (Vec::new(), Vec::new());
        let (mut times, mut diffs) = (Vec::new(), Vec::new());
        let (mut p1, mut p2) = (0usize, 0usize);
        while p1 < n1 && p2 < n2 {
            match cmp_at(&kv1, p1, &t1[p1], &kv2, p2, &t2[p2]) {
                Ordering::Less => { tags.push(0); offs.push(p1); times.push(t1[p1].clone()); diffs.push(d1[p1].clone()); p1 += 1; }
                Ordering::Greater => { tags.push(1); offs.push(p2); times.push(t2[p2].clone()); diffs.push(d2[p2].clone()); p2 += 1; }
                Ordering::Equal => {
                    let mut d = d1[p1].clone();
                    d.plus_equals(&d2[p2]);
                    if !d.is_zero() { tags.push(0); offs.push(p1); times.push(t1[p1].clone()); diffs.push(d); }
                    p1 += 1; p2 += 1;
                }
            }
        }

        let srcs = [Some(&kv1), Some(&kv2)];
        Self::emit(&srcs, &tags, &offs, &times, &diffs, out);

        // Push back the survivor's unconsumed suffix (all `>` the horizon), ahead of its deque.
        if p1 < n1 {
            let idx: Vec<usize> = (p1..n1).collect();
            in1.push_front(Self::from_kv(gather(&kv1, &idx), t1[p1..].to_vec(), d1[p1..].to_vec()));
        }
        if p2 < n2 {
            let idx: Vec<usize> = (p2..n2).collect();
            in2.push_front(Self::from_kv(gather(&kv2, &idx), t2[p2..].to_vec(), d2[p2..].to_vec()));
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
            if frontier.less_equal(&times[i]) { residual.insert_ref(&times[i]); ki.push(i); } else { si.push(i); }
        }
        if !ki.is_empty() {
            let t: Vec<T> = ki.iter().map(|&i| times[i].clone()).collect();
            let d: Vec<R> = ki.iter().map(|&i| diffs[i].clone()).collect();
            keep.push_back(Self::from_kv(gather(&kv, &ki), t, d));
        }
        if !si.is_empty() {
            let t: Vec<T> = si.iter().map(|&i| times[i].clone()).collect();
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

        // Giant-key case: whole buffer is one `(key, val)` → no group provably complete.
        if !done && compare_at(&ckv, 0, &ckv, n - 1) == Ordering::Equal {
            input.push_front(Self::from_kv(ckv, ctimes, cdiffs));
            return;
        }

        // Withhold the trailing group as the carry unless `done`.
        let end = if done {
            n
        } else {
            let mut start = n;
            while start > 0 && compare_at(&ckv, start - 1, &ckv, n - 1) == Ordering::Equal { start -= 1; }
            start
        };
        if end < n {
            let idx: Vec<usize> = (end..n).collect();
            input.push_front(Self::from_kv(gather(&ckv, &idx), ctimes[end..].to_vec(), cdiffs[end..].to_vec()));
        }

        // Advance + consolidate each complete group; emit `TARGET`-sized chunks. All rows of a group
        // share `(key, val)`, so one representative offset materializes each output row's kv.
        let srcs = [Some(&ckv)];
        let (mut tags, mut offs) = (Vec::new(), Vec::new());
        let (mut otimes, mut odiffs) = (Vec::new(), Vec::new());
        let mut i = 0;
        while i < end {
            let mut j = i;
            while j < end && compare_at(&ckv, i, &ckv, j) == Ordering::Equal { j += 1; }
            let mut pairs: Vec<(T, R)> = (i..j)
                .map(|k| { let mut t = ctimes[k].clone(); t.advance_by(frontier); (t, cdiffs[k].clone()) })
                .collect();
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            let mut k = 0;
            while k < pairs.len() {
                let t = pairs[k].0.clone();
                let mut d = pairs[k].1.clone();
                k += 1;
                while k < pairs.len() && pairs[k].0 == t { d.plus_equals(&pairs[k].1); k += 1; }
                if !d.is_zero() {
                    tags.push(0); offs.push(i); otimes.push(t); odiffs.push(d);
                    if otimes.len() >= TARGET {
                        Self::emit(&srcs, &tags, &offs, &otimes, &odiffs, out);
                        tags.clear(); offs.clear(); otimes.clear(); odiffs.clear();
                    }
                }
            }
            i = j;
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
                let mut times = acc.times().to_vec();
                times.extend_from_slice(next.times());
                let mut diffs = acc.diffs().to_vec();
                diffs.extend_from_slice(next.diffs());
                *acc = Self::from_kv(kv, times, diffs);
            },
            |chunk, m| {
                let kv = chunk.kv();
                let left: Vec<usize> = (0..m).collect();
                let right: Vec<usize> = (m..chunk.len_()).collect();
                let l = Self::from_kv(gather(&kv, &left), chunk.times()[..m].to_vec(), chunk.diffs()[..m].to_vec());
                let r = Self::from_kv(gather(&kv, &right), chunk.times()[m..].to_vec(), chunk.diffs()[m..].to_vec());
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
    T: Ord + Clone,
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
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    /// One sorted+consolidated chunk from DDIR rows (the arrange-ingest transcode boundary). Shapes
    /// are inferred by whole-column scan ([`infer_shape_cols`]) so variant arms resolve correctly.
    pub fn from_rows(rows: &[Upd<T, R>]) -> Self {
        if rows.is_empty() { return Self::default(); }
        let keys_rows: Vec<DValue> = rows.iter().map(|u| u.0 .0.clone()).collect();
        let vals_rows: Vec<DValue> = rows.iter().map(|u| u.0 .1.clone()).collect();
        let keys = transcode(&keys_rows, &infer_shape_cols(&keys_rows));
        let vals = transcode(&vals_rows, &infer_shape_cols(&vals_rows));
        let times = rows.iter().map(|u| u.1.clone()).collect();
        let diffs = rows.iter().map(|u| u.2.clone()).collect();
        let (keys, vals, times, diffs) = sort_consolidate(keys, vals, times, diffs);
        Self::from_parts(keys, vals, times, diffs)
    }

    /// One sorted+consolidated chunk from columns already in corgi form (the column-native arrange
    /// ingest — no transcode, unlike `from_rows`).
    pub fn from_columns(keys: CValue, vals: CValue, times: Vec<T>, diffs: Vec<R>) -> Self {
        let (keys, vals, times, diffs) = sort_consolidate(keys, vals, times, diffs);
        Self::from_parts(keys, vals, times, diffs)
    }

    /// Untranscode this chunk back to DDIR rows (egress / history read).
    pub fn to_rows(&self) -> Vec<Upd<T, R>> {
        if self.len_() == 0 { return Vec::new(); }
        let keys = untranscode(self.0.keys.clone(), &corgi::shape_of_value(&self.0.keys));
        let vals = untranscode(self.0.vals.clone(), &corgi::shape_of_value(&self.0.vals));
        keys.into_iter()
            .zip(vals)
            .zip(&self.0.times)
            .zip(&self.0.diffs)
            .map(|(((k, v), t), d)| ((k, v), t.clone(), d.clone()))
            .collect()
    }
}

/// Read a whole `ChunkBatch<CorgiChunk>` back to DDIR rows (across all chunks) — the cursor-less
/// history read used by `as_collection` and the reduce tactic.
pub fn batch_to_rows<T, R>(b: &ChunkBatch<CorgiChunk<T, R>>) -> Vec<Upd<T, R>>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    let mut out = Vec::new();
    for ch in &b.chunks { out.extend(ch.to_rows()); }
    out
}

/// Concatenate chunks' columns into flat `(keys, vals, times, diffs)` with **no transcode** — for
/// reading an arrangement back column-natively (e.g. `Backend::as_collection` straight into a
/// `CorgiContainer`), instead of untranscoding to rows and re-transcoding.
pub fn chunks_to_columns<T, R>(chunks: &[CorgiChunk<T, R>]) -> (CValue, CValue, Vec<T>, Vec<R>)
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    if chunks.iter().all(|c| c.len_() == 0) {
        return (CValue::Unit(0), CValue::Unit(0), Vec::new(), Vec::new());
    }
    let (kv, times, diffs) = CorgiChunk::concat(chunks);
    let (keys, vals) = split_kv(kv);
    (keys, vals, times, diffs)
}

/// Build a `ChunkBatch<CorgiChunk>` from DDIR rows: transcode+sort+consolidate into one chunk, then
/// `settle` it into a graded chunk sequence.
fn rows_to_batch<T, R>(rows: Vec<Upd<T, R>>, description: Description<T>) -> ChunkBatch<CorgiChunk<T, R>>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    let chunk = CorgiChunk::from_rows(&rows);
    settle_one(chunk, description)
}

/// Build a `ChunkBatch<CorgiChunk>` from corgi key/val COLUMNS directly (no transcode): sort +
/// consolidate into one chunk, then `settle`. The column-native egress the reduce backend seals its
/// output with (it resolves proxy ids to real columns by `gather` and hands them here).
pub fn columns_to_batch<T, R>(keys: CValue, vals: CValue, times: Vec<T>, diffs: Vec<R>, description: Description<T>) -> ChunkBatch<CorgiChunk<T, R>>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    let chunk = CorgiChunk::from_columns(keys, vals, times, diffs);
    settle_one(chunk, description)
}

/// Grade one chunk into a `ChunkBatch` (shared tail of `rows_to_batch`/`columns_to_batch`).
fn settle_one<T, R>(chunk: CorgiChunk<T, R>, description: Description<T>) -> ChunkBatch<CorgiChunk<T, R>>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    let mut input = VecDeque::new();
    if chunk.len_() > 0 { input.push_back(chunk); }
    let mut output = VecDeque::new();
    CorgiChunk::settle(&mut input, true, &mut output);
    ChunkBatch::new(output.into(), description)
}

/// DD [`Builder`](DdBuilder) that consumes sorted/consolidated row chains (the vec batcher's `Output`)
/// and builds a `ChunkBatch<CorgiChunk>`. `Input = Vec<Upd>` so the existing `MergeBatcher` over vec
/// rows can stay the arrange batcher; only the builder + trace become chunk-native. `RcBuilder` wraps
/// it → `Rc<ChunkBatch<CorgiChunk>>`.
pub struct CorgiChunkBuilder<T, R> {
    rows: Vec<Upd<T, R>>,
}

impl<T, R> DdBuilder for CorgiChunkBuilder<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    type Input = Vec<Upd<T, R>>;
    type Time = T;
    type Output = ChunkBatch<CorgiChunk<T, R>>;

    fn with_capacity(_keys: usize, _vals: usize, upds: usize) -> Self {
        CorgiChunkBuilder { rows: Vec::with_capacity(upds) }
    }
    fn push(&mut self, chunk: &mut Self::Input) { self.rows.append(chunk); }
    fn done(self, description: Description<T>) -> Self::Output { rows_to_batch(self.rows, description) }
    fn seal(chain: &mut Vec<Self::Input>, description: Description<T>) -> Self::Output {
        let mut all = Vec::new();
        for c in chain.iter_mut() { all.append(c); }
        rows_to_batch(all, description)
    }
}

/// A column-native arrange **chunker**: turns input `CorgiContainer`s into sorted+consolidated
/// `CorgiChunk`s WITHOUT `ContainerChunker`'s drain-to-rows (which untranscodes). Paired with the
/// standard `ChunkBatcher`/`ChunkBuilder`, the arrange ingest stays column-native — no
/// columns→rows→columns round-trip at the arrangement boundary.
///
/// Crucially it **accumulates to `TARGET`** before consolidating (like `ContainerChunker`), so it
/// emits few large chunks rather than one tiny chunk per input container — otherwise the columnar
/// per-chunk set-up (`gather`/`sort_perm`) dominates when input arrives as many small batches.
pub struct CorgiChunker<T, R> {
    /// Un-consolidated key/val column blocks (one per absorbed container), flat time/diff.
    k_blocks: Vec<CValue>,
    v_blocks: Vec<CValue>,
    times: Vec<T>,
    diffs: Vec<R>,
    ready: VecDeque<CorgiChunk<T, R>>,
    current: Option<CorgiChunk<T, R>>,
}

impl<T, R> Default for CorgiChunker<T, R> {
    fn default() -> Self {
        CorgiChunker { k_blocks: Vec::new(), v_blocks: Vec::new(), times: Vec::new(), diffs: Vec::new(), ready: VecDeque::new(), current: None }
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
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    /// Consolidate the accumulated blocks into one graded chunk (concat columns, then sort+consolidate).
    fn flush(&mut self) {
        if self.times.is_empty() {
            return;
        }
        let keys = concat_blocks(&self.k_blocks);
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

impl<T, R> timely::container::PushInto<&mut crate::corgi_backend::CorgiContainer<T, R>> for CorgiChunker<T, R>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    fn push_into(&mut self, c: &mut crate::corgi_backend::CorgiContainer<T, R>) {
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
    T: Timestamp + Lattice,
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

/// A single sorted+consolidated run over corgi columns — the join tactic's per-side input, produced
/// by merging a batch list [`flatten_batches`]. Separate `keys`/`vals` columns so the merge-join can
/// compare by key (`compare_at`) and `gather` matched runs.
pub struct SortedRun<T, R> {
    pub keys: CValue,
    pub vals: CValue,
    pub times: Vec<T>,
    pub diffs: Vec<R>,
}

/// Flatten a list of batches into one sorted+consolidated run over columns (no row round-trip, no
/// re-sort). Each batch's `.chunks` is already a sorted, consolidated chain; we **merge** those chains
/// (reusing their order via `merge_chains` → `CorgiChunk::merge`) into one, then concatenate. `None`
/// if empty. This replaces an earlier concat+full-sort that dominated recursive (reach) cost.
pub fn flatten_batches<T, R>(batches: &[Rc<ChunkBatch<CorgiChunk<T, R>>>]) -> Option<SortedRun<T, R>>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    let mut merged: VecDeque<CorgiChunk<T, R>> = VecDeque::new();
    for b in batches {
        if b.chunks.is_empty() { continue; }
        if merged.is_empty() {
            merged.extend(b.chunks.iter().cloned());
        } else {
            let chain: Vec<CorgiChunk<T, R>> = merged.drain(..).collect();
            merge_chains(chain, b.chunks.clone(), &mut merged);
        }
    }
    if merged.is_empty() { return None; }
    // The merged chain is globally sorted+consolidated: concatenate (no sort) into one flat run.
    let chunks: Vec<CorgiChunk<T, R>> = merged.into();
    let (kv, times, diffs) = CorgiChunk::concat(&chunks);
    let (keys, vals) = split_kv(kv);
    Some(SortedRun { keys, vals, times, diffs })
}

/// Group a key column by identity, returning integers only — the Rust↔corgi boundary primitive.
/// `perm` lists row indices in group order; `ends[g]` is the exclusive end of group `g` within `perm`
/// (so group `g`'s member rows are `perm[ends[g-1]..ends[g]]`, with `ends[-1] = 0`). DD orchestrates
/// per group by index — it never sees a key `Value`; keys and payloads stay columnar in corgi. Built
/// on the columnar discrimination sort + one batched adjacent-compare (no per-pair `compare_at`).
pub fn group_offsets(col: &CValue) -> (Vec<usize>, Vec<usize>) {
    let n = col.len();
    if n == 0 {
        return (Vec::new(), Vec::new());
    }
    let perm = sort_perm(col);
    let sorted = gather(col, &perm);
    let mut ends = Vec::new();
    if n > 1 {
        let left: Vec<usize> = (0..n - 1).collect();
        let right: Vec<usize> = (1..n).collect();
        // adj[i] == 0 iff sorted[i] == sorted[i+1] — a group boundary is where adj[i] != 0.
        let adj = compare_idx(&sorted, &sorted, &left, &right);
        for (i, &a) in adj.iter().enumerate() {
            if a != 0 {
                ends.push(i + 1);
            }
        }
    }
    ends.push(n);
    (perm, ends)
}

/// Semijoin: for each key in `needle_keys` (a set of DDIR keys), gather its `(val, time, diff)`
/// history from `batches` via a batched `find` probe over the flattened, sorted source — untranscoding
/// only the matched rows, never the whole trace. Returns a `key -> history` map (only matched keys
/// present), the drop-in replacement for grouping untranscode-all rows by key. `O(|needle|·log|source|)`
/// probe + `O(matched)` untranscode, vs the old `O(|source|)` untranscode + hash-group.
pub fn semijoin_history<T, R>(
    batches: &[Rc<ChunkBatch<CorgiChunk<T, R>>>],
    needle_keys: &[DValue],
) -> HashMap<DValue, Vec<(DValue, T, R)>>
where
    T: Timestamp + Lattice,
    R: Semigroup + Clone + 'static,
{
    let mut out: HashMap<DValue, Vec<(DValue, T, R)>> = HashMap::new();
    if needle_keys.is_empty() {
        return out;
    }
    // Each chunk already holds a sorted key column, so probe each in place — NO flatten/merge (which
    // would re-materialize the whole accumulated trace every round). Transcode the needle once with
    // the arrangement's key shape (taken from the first non-empty chunk; the schema is uniform).
    let shape = batches
        .iter()
        .flat_map(|b| b.chunks.iter())
        .find(|c| c.len_() > 0)
        .map(|c| corgi::shape_of_value(c.keys()));
    let Some(shape) = shape else { return out };
    let needle_col = transcode(needle_keys, &shape);

    for b in batches {
        for chunk in &b.chunks {
            if chunk.len_() == 0 {
                continue;
            }
            let (lo, hi) = find_ranges(&needle_col, chunk.keys());
            let mut matched: Vec<usize> = Vec::new();
            for i in 0..needle_keys.len() {
                matched.extend(lo[i]..hi[i]);
            }
            if matched.is_empty() {
                continue;
            }
            let vcol = gather(chunk.vals(), &matched);
            let vals = untranscode(vcol.clone(), &corgi::shape_of_value(&vcol));
            let mut c = 0;
            for i in 0..needle_keys.len() {
                let len = hi[i] - lo[i];
                if len == 0 {
                    continue;
                }
                let entry = out.entry(needle_keys[i].clone()).or_default();
                for k in 0..len {
                    let r = matched[c + k];
                    entry.push((vals[c + k].clone(), chunk.times()[r].clone(), chunk.diffs()[r].clone()));
                }
                c += len;
            }
        }
    }
    out
}

#[cfg(test)]
mod test {
    use super::*;
    use differential_dataflow::trace::chunk::{ChunkBatchMerger, is_graded};
    use differential_dataflow::trace::{Description, Merger};
    use std::collections::BTreeMap;

    fn xorshift(s: &mut u64) -> u64 { *s ^= *s << 13; *s ^= *s >> 7; *s ^= *s << 17; *s }

    #[test]
    fn group_offsets_partitions_by_key() {
        // keys 3,1,1,2,3 → groups key1={rows 1,2}, key2={row 3}, key3={rows 0,4}.
        let col = CValue::u64(vec![3, 1, 1, 2, 3]);
        let (perm, ends) = group_offsets(&col);
        let mut groups: Vec<Vec<usize>> = Vec::new();
        let mut start = 0;
        for &e in &ends {
            let mut g = perm[start..e].to_vec();
            g.sort();
            groups.push(g);
            start = e;
        }
        assert_eq!(groups, vec![vec![1, 2], vec![3], vec![0, 4]]);
    }

    /// Build a single sorted+consolidated CorgiChunk from u64 (key,val,time,diff) rows.
    fn chunk(rows: &[((u64, u64), u64, i64)]) -> CorgiChunk<u64, i64> {
        // Sort + consolidate by ((k,v),t) so a chunk is a legal sorted run.
        let mut m: BTreeMap<((u64, u64), u64), i64> = BTreeMap::new();
        for &(kv, t, d) in rows { *m.entry((kv, t)).or_insert(0) += d; }
        m.retain(|_, d| *d != 0);
        let keys = CValue::u64(m.keys().map(|((k, _), _)| *k).collect());
        let vals = CValue::u64(m.keys().map(|((_, v), _)| *v).collect());
        let times = m.keys().map(|(_, t)| *t).collect();
        let diffs = m.values().copied().collect();
        CorgiChunk::from_parts(keys, vals, times, diffs)
    }

    fn read_batch(b: &ChunkBatch<CorgiChunk<u64, i64>>) -> BTreeMap<((u64, u64), u64), i64> {
        let mut m = BTreeMap::new();
        for ch in &b.chunks {
            let ks = ch.keys().clone().into_u64("k");
            let vs = ch.vals().clone().into_u64("v");
            for i in 0..ch.len_() { *m.entry(((ks[i], vs[i]), ch.times()[i])).or_insert(0) += ch.diffs()[i]; }
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
