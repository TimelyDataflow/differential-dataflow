//! The corgi JOIN backend for the `int_proxy` proxy-join seam.
//!
//! [`CorgiJoinBackend`] implements [`ProxyJoinBackend`]: `advance` draws blocks of the two
//! inputs' key intersection as `((group, coord), time, diff)` bridges, and `cross` redeems
//! matched coordinates directly against the instance's chunks (`gather_lanes`), runs the
//! compiled projection, and cuts `TARGET_OUT`-sized [`CorgiContainer`]s. This replaces the
//! bespoke whole-unit tactic (`run_unit`), whose single giant output container was an OOM
//! for large snapshots; peak state is now one block's bridges plus one container.
//!
//! # Tokens
//!
//! *   The **group token** is the key's own `u64` when the key column is a leaf (DDIR `Int`
//!     keys transcode to a `U64` leaf, and chunk order IS `u64` order), which makes `from`
//!     seekable (`find_ranges` with a one-row needle) and blocks resumable. Structured keys
//!     have no order-preserving `u64` embedding, so they take a fallback: the whole
//!     intersection in ONE block, tokens an ordinal counter (block-scoped, both sides
//!     assigned by the same walk). Containers are still cut at `TARGET_OUT` either way —
//!     the fallback forgoes only the bounded-bridge property, not bounded output.
//!
//! *   The **value token** is a *canonical coordinate*: `(chunk << 48) | row` of the value's
//!     first occurrence among its side's chunks. Coordinates redeem against the instance
//!     alone (no backend state, no reliance on call adjacency), and value columns are never
//!     copied into the presentation — they are gathered once, straight from chunk storage
//!     into the projection input, and only for rows that matched. Assigning *equal* values
//!     (found by the per-key merge across chunks) the *same* canonical coordinate is what
//!     lets bridge consolidation cancel cross-batch churn: tokens are the unit of
//!     cancellation, and location-distinct tokens for equal data would silently opt out.
//!
//! Times are advanced by `instance.lower` on presentation (consolidate on load), and a key
//! whose side nets to zero after consolidation is suppressed entirely (the harness requires
//! keys common to both bridges).

use std::cmp::Ordering;
use std::marker::PhantomData;
use std::rc::Rc;

use differential_dataflow::operators::int_proxy::{JoinInstance, ProxyBridge, ProxyJoinBackend};
use differential_dataflow::operators::int_proxy::join::JoinMatches;
use differential_dataflow::trace::chunk::{Chunk, ChunkBatch};

use corgi::arrange::{compare_at, find_ranges, gather, gather_lanes};
use corgi::{shape_of_value, Shape, Value as CValue};

use crate::corgi::chunk::CorgiChunk;
use crate::corgi::col_times::ColTime;
use crate::corgi::container::CorgiContainer;
use crate::corgi::logic::compile_join_projection;
use crate::ir::Diff;
use crate::parse::Term;

type CBatch<T> = Rc<ChunkBatch<CorgiChunk<T, Diff>>>;

/// Matches per output container cut by `cross` (mirrors the chunk grading `TARGET`).
const TARGET_OUT: usize = 1 << 18;
/// Coordinate packing: chunk index in the high bits, row in the low `COORD_BITS`.
const COORD_BITS: u32 = 48;

/// The corgi [`ProxyJoinBackend`]: holds the projection `Term`s (`Var(0)=key`, `Var(1)=val0`,
/// `Var(2)=val1`), compiled per container against the matched columns' shapes.
pub struct CorgiJoinBackend<T: ColTime> {
    key: Term,
    val: Term,
    _t: PhantomData<T>,
}

impl<T: ColTime> CorgiJoinBackend<T> {
    pub fn new(key: Term, val: Term) -> Self {
        CorgiJoinBackend { key, val, _t: PhantomData }
    }
}

impl<T: ColTime> ProxyJoinBackend<CBatch<T>, CBatch<T>> for CorgiJoinBackend<T> {
    type R0 = Diff;
    type R1 = Diff;
    type ROut = Diff;
    type Output = CorgiContainer<T, Diff>;

    fn advance(
        &mut self,
        instance: &JoinInstance<CBatch<T>, CBatch<T>>,
        from: &mut Option<u64>,
        bridge0: &mut ProxyBridge<T, Diff>,
        bridge1: &mut ProxyBridge<T, Diff>,
    ) {
        let chunks0 = side_chunks(&instance.batches0);
        let chunks1 = side_chunks(&instance.batches1);
        if chunks0.iter().all(|c| c.len() == 0) || chunks1.iter().all(|c| c.len() == 0) {
            *from = None;
            return;
        }
        if single_lane_keyed(&chunks0) && single_lane_keyed(&chunks1) {
            advance_leaf(&chunks0, &chunks1, &instance.lower, from, bridge0, bridge1);
        } else {
            advance_structured(&chunks0, &chunks1, &instance.lower, from, bridge0, bridge1);
        }
    }

    fn cross(
        &mut self,
        instance: &JoinInstance<CBatch<T>, CBatch<T>>,
        matches: &mut JoinMatches<T, Diff>,
        output: &mut Vec<CorgiContainer<T, Diff>>,
    ) {
        let chunks0 = side_chunks(&instance.batches0);
        let chunks1 = side_chunks(&instance.batches1);
        let keys0: Vec<Option<&CValue>> = chunks0.iter().map(|c| Some(c.keys())).collect();
        let vals0: Vec<Option<&CValue>> = chunks0.iter().map(|c| Some(c.vals())).collect();
        let vals1: Vec<Option<&CValue>> = chunks1.iter().map(|c| Some(c.vals())).collect();

        let n = matches.ids.len();
        let (mut tag0, mut off0) = (Vec::new(), Vec::new());
        let (mut tag1, mut off1) = (Vec::new(), Vec::new());
        let mut start = 0usize;
        while start < n {
            let end = (start + TARGET_OUT).min(n);
            tag0.clear(); off0.clear(); tag1.clear(); off1.clear();
            for (_, (c0, c1)) in &matches.ids[start..end] {
                tag0.push((c0 >> COORD_BITS) as usize);
                off0.push((c0 & ((1 << COORD_BITS) - 1)) as usize);
                tag1.push((c1 >> COORD_BITS) as usize);
                off1.push((c1 & ((1 << COORD_BITS) - 1)) as usize);
            }
            let kc = gather_lanes(&keys0, &tag0, &off0);
            let v0 = gather_lanes(&vals0, &tag0, &off0);
            let v1 = gather_lanes(&vals1, &tag1, &off1);
            let proj = compile_join_projection(&self.key, &self.val, &shape_of_value(&kc), &shape_of_value(&v0), &shape_of_value(&v1));
            let projected = corgi::eval_graph(&proj, CValue::Prod(vec![kc, v0, v1]));
            let mut cols = projected.into_prod("corgi join projection");
            let nv = cols.pop().unwrap();
            let nk = cols.pop().unwrap();
            output.push(CorgiContainer {
                keys: nk,
                vals: nv,
                times: matches.times[start..end].to_vec(),
                diffs: matches.diffs[start..end].to_vec(),
            });
            start = end;
        }
    }
}

/// The instance's chunks on one side, in the deterministic order coordinates index
/// (batches in order, chunks in order) — `advance` and `cross` must agree on it.
fn side_chunks<T: ColTime>(batches: &[CBatch<T>]) -> Vec<&CorgiChunk<T, Diff>> {
    let chunks: Vec<&CorgiChunk<T, Diff>> = batches.iter().flat_map(|b| b.chunks.iter()).collect();
    assert!(chunks.len() < (1 << (64 - COORD_BITS)), "too many chunks for coordinate packing");
    chunks
}

/// The column flattened to `u64` leaf lanes: `Some(lanes)` when it is a (possibly nested)
/// product of 64-bit leaves — the shape DDIR tuples transcode to — so row order is the
/// lexicographic order of the lane tuples. `Sum`/`List`/narrow leaves give `None`
/// (structural compares).
fn leaf_lanes(col: &CValue) -> Option<Vec<&CValue>> {
    fn walk<'a>(col: &'a CValue, out: &mut Vec<&'a CValue>) -> bool {
        match col {
            CValue::Prim(_) => { out.push(col); matches!(shape_of_value(col), Shape::Prim(64)) }
            CValue::Prod(fields) => fields.iter().all(|f| walk(f, out)),
            CValue::Unit(_) => true,
            _ => false,
        }
    }
    let mut out = Vec::new();
    if walk(col, &mut out) { Some(out) } else { None }
}

/// Whether every nonempty chunk's key column is a single `u64` leaf lane, making the key's
/// own value the group token (chunk order IS `u64` order for a one-lane lexicographic key).
fn single_lane_keyed<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>]) -> bool {
    chunks.iter().filter(|c| c.len() > 0).all(|c| leaf_lanes(c.keys()).is_some_and(|l| l.len() == 1))
}

/// Whether every nonempty chunk's val column flattens to `u64` leaf lanes.
fn leaf_valued<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>]) -> bool {
    chunks.iter().filter(|c| c.len() > 0).all(|c| leaf_lanes(c.vals()).is_some())
}

/// A needle column of `keys`, shaped like `col` (a single-leaf-lane key column).
fn needle_like(col: &CValue, keys: &[u64]) -> CValue {
    match col {
        CValue::Prim(_) => CValue::u64(keys.to_vec()),
        CValue::Prod(fields) => CValue::Prod(fields.iter().map(|f| needle_like(f, keys)).collect()),
        CValue::Unit(_) => CValue::Unit(keys.len()),
        _ => unreachable!("needle_like: single-leaf key columns only"),
    }
}

/// Pull rows `idx` of the column's leaf lanes as `u64` buffers.
fn pull_lanes(col: &CValue, idx: &[usize]) -> Vec<Vec<u64>> {
    leaf_lanes(col).expect("pull_lanes: leaf-laned column")
        .into_iter()
        .map(|lane| gather(lane, idx).into_u64("corgi join lane pull"))
        .collect()
}

/// One past the end of the key run beginning at `start` (gallop + binary search, same-column
/// structural compares).
fn run_end(keys: &CValue, start: usize, len: usize) -> usize {
    let same = |i: usize| compare_at(keys, i, keys, start) == Ordering::Equal;
    let mut step = 1usize;
    let mut lo = start; // last known-equal
    while lo + step < len && same(lo + step) {
        lo += step;
        step <<= 1;
    }
    let mut hi = (lo + step).min(len); // first known-beyond (or len)
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        if same(mid) { lo = mid; } else { hi = mid; }
    }
    hi
}

/// One key's records in one chunk: absolute rows `[s, e)`. When the vals are leaf-laned,
/// `vals = (lanes, pos)` gives row `r`'s tuple as `lanes[.][pos + r - s]`; `None` falls
/// back to structural compares against the chunk itself.
struct RunRef<'a, T: ColTime> {
    chunk: &'a CorgiChunk<T, Diff>,
    cid: usize,
    s: usize,
    e: usize,
    vals: Option<(&'a [Vec<u64>], usize)>,
}

impl<'a, T: ColTime> RunRef<'a, T> {
    fn val_less(&self, row: usize, other: &Self, orow: usize) -> bool {
        match (self.vals, other.vals) {
            (Some((a, ap)), Some((b, bp))) => {
                let (i, j) = (ap + row - self.s, bp + orow - other.s);
                a.iter().zip(b).map(|(la, lb)| (la[i], lb[j])).find(|(x, y)| x != y).is_some_and(|(x, y)| x < y)
            }
            _ => compare_at(self.chunk.vals(), row, other.chunk.vals(), orow) == Ordering::Less,
        }
    }
    fn val_eq(&self, row: usize, other: &Self, orow: usize) -> bool {
        match (self.vals, other.vals) {
            (Some((a, ap)), Some((b, bp))) => {
                let (i, j) = (ap + row - self.s, bp + orow - other.s);
                a.iter().zip(b).all(|(la, lb)| la[i] == lb[j])
            }
            _ => compare_at(self.chunk.vals(), row, other.chunk.vals(), orow) == Ordering::Equal,
        }
    }
    /// One past the end of the value run starting at `row`.
    fn val_run_end(&self, row: usize) -> usize {
        let mut r = row + 1;
        while r < self.e && self.val_eq(r, self, row) { r += 1; }
        r
    }
}

/// Per-key staging for one side: consolidated `(coord, time, diff)` entries, built in scratch
/// so a side that nets to zero suppresses the key before anything reaches the bridges.
struct SideScratch<T> {
    entries: Vec<(u64, T, Diff)>,
    /// `(time, diff)` scratch for one value's cross-chunk merge.
    tds: Vec<(T, Diff)>,
}

impl<T: ColTime> SideScratch<T> {
    fn new() -> Self {
        SideScratch { entries: Vec::new(), tds: Vec::new() }
    }

    fn push(&mut self, coord: u64, time: T, diff: Diff) {
        match self.entries.last_mut() {
            Some((lc, lt, ld)) if *lc == coord && *lt == time => *ld += diff,
            _ => self.entries.push((coord, time, diff)),
        }
    }

    /// Stage one key's records from `runs` (its equal-key row ranges, one per chunk holding it):
    /// values merged across chunks by content, equal values sharing the canonical coordinate of
    /// their least occurrence, times advanced by `lower`, consolidated, zeros dropped. Entries
    /// end sorted by `(coord, time)`.
    fn stage_runs(&mut self, runs: &[RunRef<'_, T>], lower: &T) {
        self.entries.clear();
        if let [run] = runs {
            // Single-chunk run: values grouped, times ascending within a value; advanced times
            // stay ascending (join is monotone), so consolidation is adjacent. Coordinates of
            // successive value runs ascend, so entries are born sorted.
            let coord_hi = (run.cid as u64) << COORD_BITS;
            let mut v_start = run.s;
            for row in run.s..run.e {
                if row > v_start && !run.val_eq(row, run, v_start) {
                    v_start = row;
                }
                self.push(coord_hi | v_start as u64, run.chunk.times().get(row).join(lower), run.chunk.diffs()[row]);
            }
        } else {
            // Cross-chunk merge by value content: heads are (run index, row); each step takes
            // the least value among heads, drains every chunk's sub-run of it into `tds`,
            // and consolidates. The canonical coordinate is the least contributing (chunk, row).
            let mut heads: Vec<(usize, usize)> = runs.iter().enumerate().map(|(i, r)| (i, r.s)).collect();
            while !heads.is_empty() {
                let mut min = 0usize;
                for h in 1..heads.len() {
                    if runs[heads[h].0].val_less(heads[h].1, &runs[heads[min].0], heads[min].1) {
                        min = h;
                    }
                }
                let (ri_min, row_min) = heads[min];
                self.tds.clear();
                let mut coord = u64::MAX;
                let mut h = 0usize;
                while h < heads.len() {
                    let (ri, row) = heads[h];
                    let run = &runs[ri];
                    if run.val_eq(row, &runs[ri_min], row_min) {
                        let r_end = run.val_run_end(row);
                        for r in row..r_end {
                            self.tds.push((run.chunk.times().get(r).join(lower), run.chunk.diffs()[r]));
                        }
                        coord = coord.min(((run.cid as u64) << COORD_BITS) | row as u64);
                        if r_end < run.e { heads[h] = (ri, r_end); h += 1; } else { heads.swap_remove(h); }
                    } else {
                        h += 1;
                    }
                }
                self.tds.sort_by(|a, b| a.0.cmp(&b.0));
                let tds = std::mem::take(&mut self.tds);
                for (time, diff) in tds.iter() {
                    self.push(coord, time.clone(), *diff);
                }
                self.tds = tds;
                self.tds.clear();
            }
            // Canonical coordinates interleave chunks; restore bridge order.
            self.entries.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        }
        self.entries.retain(|(_, _, d)| *d != 0);
    }

    /// Move the staged entries into `bridge` under group token `k`.
    fn emit(&mut self, k: u64, bridge: &mut ProxyBridge<T, Diff>) -> usize {
        let n = self.entries.len();
        bridge.extend(self.entries.drain(..).map(|(coord, t, d)| ((k, coord), t, d)));
        n
    }
}

/// The equal-key runs at the current positions: for each chunk whose next key equals the
/// candidate at `(cand_chunk, cand_row)`, its `(chunk, start, end)` — advancing positions
/// past every run taken.
fn take_runs<T: ColTime>(
    chunks: &[&CorgiChunk<T, Diff>],
    pos: &mut [usize],
    cand: (&CorgiChunk<T, Diff>, usize),
    runs: &mut Vec<(usize, usize, usize)>,
) {
    runs.clear();
    for (c, chunk) in chunks.iter().enumerate() {
        let p = pos[c];
        if p < chunk.len() && compare_at(chunk.keys(), p, cand.0.keys(), cand.1) == Ordering::Equal {
            let e = run_end(chunk.keys(), p, chunk.len());
            runs.push((c, p, e));
            pos[c] = e;
        }
    }
}

/// The chunk holding the least current key across both sides, or `None` when exhausted.
fn min_key<'a, T: ColTime>(
    chunks0: &[&'a CorgiChunk<T, Diff>],
    pos0: &[usize],
    chunks1: &[&'a CorgiChunk<T, Diff>],
    pos1: &[usize],
) -> Option<(&'a CorgiChunk<T, Diff>, usize)> {
    let mut best: Option<(&CorgiChunk<T, Diff>, usize)> = None;
    for (&chunk, &p) in chunks0.iter().zip(pos0.iter()).chain(chunks1.iter().zip(pos1.iter())) {
        if p < chunk.len() {
            match best {
                None => best = Some((chunk, p)),
                Some((bc, bp)) => {
                    if compare_at(chunk.keys(), p, bc.keys(), bp) == Ordering::Less {
                        best = Some((chunk, p));
                    }
                }
            }
        }
    }
    best
}

/// Pulled-buffer view over one leaf-keyed chunk of the DRIVER side: the block's key rows
/// extracted once per `advance` call as `u64` buffers, so the key merge runs on slices.
struct LeafView<'a, T: ColTime> {
    chunk: &'a CorgiChunk<T, Diff>,
    cid: usize,
    /// Absolute row of `keys[0]`.
    base: usize,
    keys: Vec<u64>,
    /// Leaf-laned vals over the same rows; `None` when vals are structured.
    vals: Option<Vec<Vec<u64>>>,
    /// Cursor within `keys`.
    cur: usize,
}

/// Rows pulled per driver chunk per `advance` call. Unprocessed pulled rows are re-pulled
/// by the next call (bounded per-call waste, in exchange for pull-once simplicity).
const PULL: usize = 1 << 14;

impl<'a, T: ColTime> LeafView<'a, T> {
    fn new(chunk: &'a CorgiChunk<T, Diff>, cid: usize, start: usize, leaf_vals: bool) -> Self {
        let mut view = LeafView { chunk, cid, base: start, keys: Vec::new(), vals: if leaf_vals { Some(Vec::new()) } else { None }, cur: 0 };
        view.extend_pull(PULL);
        view
    }
    fn abs_end(&self) -> usize {
        self.base + self.keys.len()
    }
    fn fully_pulled(&self) -> bool {
        self.abs_end() == self.chunk.len()
    }
    /// Append up to `more` further rows to the buffers.
    fn extend_pull(&mut self, more: usize) {
        let s = self.abs_end();
        let e = (s + more).min(self.chunk.len());
        if s == e { return; }
        let idx: Vec<usize> = (s..e).collect();
        let key_lane = leaf_lanes(self.chunk.keys()).expect("single-lane keys")[0];
        self.keys.extend(gather(key_lane, &idx).into_u64("corgi join key pull"));
        if let Some(vals) = self.vals.as_mut() {
            let pulled = pull_lanes(self.chunk.vals(), &idx);
            if vals.is_empty() {
                *vals = pulled;
            } else {
                for (lane, more) in vals.iter_mut().zip(pulled) { lane.extend(more); }
            }
        }
    }
    /// The key under the cursor, if any remains in the buffer.
    fn cur_key(&self) -> Option<u64> {
        self.keys.get(self.cur).copied()
    }
    /// Take the run of `k` at the cursor, if that is the cursor's key: absolute `(start, end)`.
    fn take_run(&mut self, k: u64) -> Option<(usize, usize)> {
        if self.cur_key() != Some(k) { return None; }
        let s = self.cur;
        let e = s + self.keys[s..].partition_point(|&x| x == k);
        self.cur = e;
        Some((self.base + s, self.base + e))
    }
    /// The run `[s, e)` as a [`RunRef`].
    fn run_ref(&self, s: usize, e: usize) -> RunRef<'_, T> {
        RunRef {
            chunk: self.chunk,
            cid: self.cid,
            s,
            e,
            vals: self.vals.as_ref().map(|lanes| (&lanes[..], s - self.base)),
        }
    }
}

/// The batched probe of one PROBEE-side chunk: per driver key, its equal-range in the
/// chunk (`find_ranges`), with the matched rows' vals gathered once as a `u64` buffer
/// when leaf-shaped (`off` gives each key's slice within it).
struct Probe<'a, T: ColTime> {
    chunk: &'a CorgiChunk<T, Diff>,
    cid: usize,
    lo: Vec<usize>,
    hi: Vec<usize>,
    vals: Option<Vec<Vec<u64>>>,
    off: Vec<usize>,
}

impl<'a, T: ColTime> Probe<'a, T> {
    fn new(chunk: &'a CorgiChunk<T, Diff>, cid: usize, needles: &CValue, leaf_vals: bool) -> Self {
        let (lo, hi) = find_ranges(needles, chunk.keys());
        let mut off = Vec::with_capacity(lo.len() + 1);
        let mut idx: Vec<usize> = Vec::new();
        off.push(0);
        for i in 0..lo.len() {
            idx.extend(lo[i]..hi[i]);
            off.push(idx.len());
        }
        let vals = if leaf_vals && !idx.is_empty() {
            Some(pull_lanes(chunk.vals(), &idx))
        } else {
            None
        };
        Probe { chunk, cid, lo, hi, vals, off }
    }
    /// The run of driver key `j` in this chunk, if any.
    fn run_ref(&self, j: usize) -> Option<RunRef<'_, T>> {
        let (s, e) = (self.lo[j], self.hi[j]);
        if s == e { return None; }
        Some(RunRef {
            chunk: self.chunk,
            cid: self.cid,
            s,
            e,
            vals: self.vals.as_ref().map(|lanes| (&lanes[..], self.off[j])),
        })
    }
}

/// Blockwise `advance` for leaf-keyed inputs: group token = the key's own `u64` (chunk order
/// IS `u64` order), so blocks resume by seeking `from` and end at key boundaries.
///
/// Two regimes, mirroring the bespoke tactic's probe heuristic: when one side is much
/// smaller (the fresh delta against an accumulated trace), the small side DRIVES and the
/// large side is presented only at the driver's keys (batched `find_ranges` — cost tracks
/// the driver plus matches). When the sides are comparable, probing costs `n log n` against
/// a merge's `n`, so both sides are pulled and merged symmetrically instead.
fn advance_leaf<T: ColTime>(
    chunks0: &[&CorgiChunk<T, Diff>],
    chunks1: &[&CorgiChunk<T, Diff>],
    lower: &T,
    from: &mut Option<u64>,
    bridge0: &mut ProxyBridge<T, Diff>,
    bridge1: &mut ProxyBridge<T, Diff>,
) {
    let start = from.expect("advance called on an exhausted unit");
    let seek = |chunks: &[&CorgiChunk<T, Diff>]| -> Vec<usize> {
        chunks.iter().map(|c| if c.len() == 0 { 0 } else { find_ranges(&needle_like(c.keys(), &[start]), c.keys()).0[0] }).collect()
    };
    let start0 = seek(chunks0);
    let start1 = seek(chunks1);
    let remaining = |chunks: &[&CorgiChunk<T, Diff>], starts: &[usize]| -> usize {
        chunks.iter().zip(starts).map(|(c, &s)| c.len() - s).sum()
    };
    let (r0, r1) = (remaining(chunks0, &start0), remaining(chunks1, &start1));
    fn views<'a, T: ColTime>(chunks: &[&'a CorgiChunk<T, Diff>], starts: &[usize]) -> Vec<LeafView<'a, T>> {
        let leaf_vals = leaf_valued(chunks);
        chunks.iter().enumerate()
            .filter(|(_, c)| c.len() > 0)
            .map(|(cid, c)| LeafView::new(c, cid, starts[cid], leaf_vals))
            .collect()
    }
    if r0.max(r1) >= 2 * r0.min(r1) {
        let drive0 = r0 <= r1;
        let (dviews, pchunks) = if drive0 { (views(chunks0, &start0), chunks1) } else { (views(chunks1, &start1), chunks0) };
        let (bd, bp) = if drive0 { (bridge0, bridge1) } else { (bridge1, bridge0) };
        leaf_probe(dviews, pchunks, lower, start, from, bd, bp);
    } else {
        leaf_merge(views(chunks0, &start0), views(chunks1, &start1), lower, start, from, bridge0, bridge1);
    }
}

/// The pull horizon: keys at or beyond the least last-pulled key of a partially-pulled
/// view may have rows outside its buffer, so the block stops there. Extended pulls push
/// it out when it would deny all progress (a run longer than the pull).
fn pull_horizon<'a, T: ColTime>(views0: &mut [LeafView<'a, T>], views1: &mut [LeafView<'a, T>], start: u64) -> Option<u64> {
    let horizon = |a: &[LeafView<'a, T>], b: &[LeafView<'a, T>]| -> Option<u64> {
        a.iter().chain(b).filter(|v| !v.fully_pulled()).map(|v| *v.keys.last().unwrap()).min()
    };
    let mut h = horizon(views0, views1);
    while h == Some(start) {
        for v in views0.iter_mut().chain(views1.iter_mut()) {
            if !v.fully_pulled() && *v.keys.last().unwrap() == start {
                v.extend_pull(v.keys.len());
            }
        }
        h = horizon(views0, views1);
    }
    h
}

/// Lopsided regime: the driver views are walked; the probee is presented only at the
/// driver's keys, one batched `find_ranges` per probee chunk per block.
fn leaf_probe<'a, T: ColTime>(
    mut dviews: Vec<LeafView<'a, T>>,
    pchunks: &[&CorgiChunk<T, Diff>],
    lower: &T,
    start: u64,
    from: &mut Option<u64>,
    bridge_d: &mut ProxyBridge<T, Diff>,
    bridge_p: &mut ProxyBridge<T, Diff>,
) {
    let h = pull_horizon(&mut dviews, &mut [], start);

    // Merge the driver block's keys (strictly below the horizon) into the distinct key
    // list and each key's runs.
    let mut keyset: Vec<u64> = Vec::new();
    let mut druns: Vec<(usize, usize, usize, usize)> = Vec::new(); // (key idx, view idx, s, e)
    loop {
        let k = dviews.iter().filter_map(LeafView::cur_key).min();
        let Some(k) = k.filter(|k| h.map_or(true, |h| *k < h)) else { break };
        let j = keyset.len();
        keyset.push(k);
        for (vi, v) in dviews.iter_mut().enumerate() {
            if let Some((s, e)) = v.take_run(k) {
                druns.push((j, vi, s, e));
            }
        }
    }
    if keyset.is_empty() {
        // Nothing below the horizon: the driver is spent (or the block was empty).
        *from = h;
        return;
    }

    // One batched probe of every probee chunk at the driver's keys.
    let pvleaf = leaf_valued(pchunks);
    let probes: Vec<Probe<T>> = pchunks.iter().enumerate()
        .filter(|(_, c)| c.len() > 0)
        .map(|(cid, c)| Probe::new(c, cid, &needle_like(c.keys(), &keyset), pvleaf))
        .collect();

    // Walk the keys in order, staging both sides and emitting the survivors.
    let (mut sd, mut sp) = (SideScratch::new(), SideScratch::new());
    let mut drun_at = 0usize;
    let mut refs: Vec<RunRef<T>> = Vec::new();
    for (j, &k) in keyset.iter().enumerate() {
        refs.clear();
        while drun_at < druns.len() && druns[drun_at].0 == j {
            let (_, vi, s, e) = druns[drun_at];
            refs.push(dviews[vi].run_ref(s, e));
            drun_at += 1;
        }
        let dref_count = refs.len();
        refs.extend(probes.iter().filter_map(|p| p.run_ref(j)));
        if refs.len() == dref_count {
            continue; // key absent from the probee: no matches
        }
        let (drefs, prefs) = refs.split_at(dref_count);
        sd.stage_runs(drefs, lower);
        sp.stage_runs(prefs, lower);
        if sd.entries.is_empty() || sp.entries.is_empty() {
            continue; // a side net-cancelled: the harness requires keys common to both bridges
        }
        sd.emit(k, bridge_d);
        sp.emit(k, bridge_p);
    }
    // The block ran to the horizon; resume there (`None` = the driver is exhausted, and
    // with it the intersection).
    *from = h;
}

/// Comparable-sides regime: both sides pulled and merged symmetrically on the `u64`
/// buffers — a probe here would cost `n log n` against this merge's `n`.
fn leaf_merge<'a, T: ColTime>(
    mut views0: Vec<LeafView<'a, T>>,
    mut views1: Vec<LeafView<'a, T>>,
    lower: &T,
    start: u64,
    from: &mut Option<u64>,
    bridge0: &mut ProxyBridge<T, Diff>,
    bridge1: &mut ProxyBridge<T, Diff>,
) {
    let h = pull_horizon(&mut views0, &mut views1, start);
    let (mut s0, mut s1) = (SideScratch::new(), SideScratch::new());
    let (mut refs0, mut refs1): (Vec<(usize, usize, usize)>, Vec<(usize, usize, usize)>) = (Vec::new(), Vec::new());
    loop {
        let k = views0.iter().chain(&views1).filter_map(LeafView::cur_key).min();
        let Some(k) = k.filter(|k| h.map_or(true, |h| *k < h)) else {
            *from = h;
            return;
        };
        refs0.clear();
        refs0.extend(views0.iter_mut().enumerate().filter_map(|(vi, v)| v.take_run(k).map(|(s, e)| (vi, s, e))));
        refs1.clear();
        refs1.extend(views1.iter_mut().enumerate().filter_map(|(vi, v)| v.take_run(k).map(|(s, e)| (vi, s, e))));
        if refs0.is_empty() || refs1.is_empty() {
            continue;
        }
        let r0: Vec<RunRef<T>> = refs0.iter().map(|&(vi, s, e)| views0[vi].run_ref(s, e)).collect();
        let r1: Vec<RunRef<T>> = refs1.iter().map(|&(vi, s, e)| views1[vi].run_ref(s, e)).collect();
        s0.stage_runs(&r0, lower);
        s1.stage_runs(&r1, lower);
        if s0.entries.is_empty() || s1.entries.is_empty() {
            continue;
        }
        s0.emit(k, bridge0);
        s1.emit(k, bridge1);
    }
}

/// Fallback `advance` for structured keys, which have no order-preserving `u64` embedding to
/// resume by: the whole intersection in one block, group tokens an ordinal counter (block-
/// scoped; both sides named by this single walk). Output is still cut at `TARGET_OUT` by
/// `cross`; only the bounded-bridge property is forgone.
fn advance_structured<T: ColTime>(
    chunks0: &[&CorgiChunk<T, Diff>],
    chunks1: &[&CorgiChunk<T, Diff>],
    lower: &T,
    from: &mut Option<u64>,
    bridge0: &mut ProxyBridge<T, Diff>,
    bridge1: &mut ProxyBridge<T, Diff>,
) {
    let mut pos0 = vec![0usize; chunks0.len()];
    let mut pos1 = vec![0usize; chunks1.len()];
    let (mut s0, mut s1) = (SideScratch::new(), SideScratch::new());
    let (mut runs0, mut runs1) = (Vec::new(), Vec::new());
    let mut token = 0u64;
    while let Some(cand) = min_key(chunks0, &pos0, chunks1, &pos1) {
        take_runs(chunks0, &mut pos0, cand, &mut runs0);
        take_runs(chunks1, &mut pos1, cand, &mut runs1);
        if runs0.is_empty() || runs1.is_empty() {
            continue;
        }
        fn refs<'a, T: ColTime>(chunks: &[&'a CorgiChunk<T, Diff>], runs: &[(usize, usize, usize)]) -> Vec<RunRef<'a, T>> {
            runs.iter().map(|&(c, s, e)| RunRef { chunk: chunks[c], cid: c, s, e, vals: None }).collect()
        }
        s0.stage_runs(&refs(chunks0, &runs0), lower);
        s1.stage_runs(&refs(chunks1, &runs1), lower);
        if s0.entries.is_empty() || s1.entries.is_empty() {
            continue;
        }
        s0.emit(token, bridge0);
        s1.emit(token, bridge1);
        token += 1;
    }
    *from = None;
}
