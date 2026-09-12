//! The corgi JOIN backend for the `int_proxy` proxy-join seam.
//!
//! [`CorgiJoinBackend`] implements [`ProxyJoinBackend`]: `advance` draws blocks of the two
//! inputs' key intersection as `((group, coord), time, diff)` bridges, and `cross` redeems
//! matched coordinates directly against the instance's chunks (`gather_lanes`), runs the
//! compiled projection, and cuts `TARGET_OUT`-sized [`CorgiContainer`]s. Peak state is one
//! block's bridges plus one container, however large the unit.
//!
//! # Tokens
//!
//! *   The **group token** is the arrangement's leading `u64` identifier: the key itself for
//!     primitive integer keys, or the carried content hash for structured keys. Chunk order is
//!     identifier order, which makes `from` seekable and every key shape resumably blockable.
//!     A hash collision remains under that token through proxy matching; only then does `cross`
//!     compare the recorded real-key coordinates and discard unequal pairs.
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

use differential_dataflow::operators::int_proxy::{Bridge, JoinInstance, ProxyJoinBackend, TimeColumn};
use differential_dataflow::operators::int_proxy::join::JoinMatches;
use differential_dataflow::trace::chunk::{Chunk, ChunkBatch};

use corgi::arrange::{compare_at, gather, gather_lanes};
use crate::corgi::search::MatchingRanges;
use corgi::{shape_of_value, Shape, Value as CValue};

use crate::corgi::chunk::{key_is_hashed, key_lane, recover_key, recover_val, CorgiChunk};
use crate::corgi::col_times::{ColTime, ColTimes};
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
    /// Identifier tokens in the current block that cover more than one real key. `advance` writes
    /// this and the immediately following `cross` reads it before the next `advance`; only matches
    /// under these astronomically rare tokens need a real-key comparison.
    colliding: Vec<u64>,
    _t: PhantomData<T>,
}

impl<T: ColTime> CorgiJoinBackend<T> {
    pub fn new(key: Term, val: Term) -> Self {
        CorgiJoinBackend { key, val, colliding: Vec::new(), _t: PhantomData }
    }
}

impl<T: ColTime> ProxyJoinBackend<T, CBatch<T>, CBatch<T>> for CorgiJoinBackend<T> {
    type R0 = Diff;
    type R1 = Diff;
    type ROut = Diff;
    type Output = CorgiContainer<T, Diff>;
    type Times = ColTimes<T>;

    fn advance(
        &mut self,
        instance: &JoinInstance<T, CBatch<T>, CBatch<T>>,
        from: &mut Option<u64>,
        bridge0: &mut Bridge<ColTimes<T>, Diff>,
        bridge1: &mut Bridge<ColTimes<T>, Diff>,
    ) {
        self.colliding.clear();
        let chunks0 = side_chunks(&instance.batches0);
        let chunks1 = side_chunks(&instance.batches1);
        if chunks0.is_empty() || chunks1.is_empty() {
            *from = None;
            return;
        }
        debug_assert_eq!(
            key_is_hashed(chunks0[0].keys()),
            key_is_hashed(chunks1[0].keys()),
            "join sides disagree on whether the arrangement key carries a hash lane",
        );
        debug_assert!(
            chunks0.iter().all(|chunk| key_is_hashed(chunk.keys()) == key_is_hashed(chunks0[0].keys()))
                && chunks1.iter().all(|chunk| key_is_hashed(chunk.keys()) == key_is_hashed(chunks1[0].keys())),
            "chunks of one join key disagree on whether they carry a hash lane",
        );
        // Every arrangement key leads with its identifier and is sorted by it, so one resumable
        // leaf walk applies to every key shape. A hash collision is retained in proxy space and
        // recorded here; `cross` then filters only that token's unequal real-key pairs. Restarting
        // through a whole-key walk is not valid after an earlier block has already retired.
        // The instance's lower bound becomes one row every staged time is joined with.
        let mut lower = ColTimes::new();
        lower.push(&instance.lower);
        advance_leaf(
            &chunks0,
            &chunks1,
            &lower,
            from,
            bridge0,
            bridge1,
            &mut self.colliding,
        );
    }

    fn cross(
        &mut self,
        instance: &JoinInstance<T, CBatch<T>, CBatch<T>>,
        matches: &mut JoinMatches<ColTimes<T>, Diff>,
        output: &mut Vec<CorgiContainer<T, Diff>>,
    ) {
        let chunks0 = side_chunks(&instance.batches0);
        let chunks1 = side_chunks(&instance.batches1);
        let keys0: Vec<Option<&CValue>> = chunks0.iter().map(|c| Some(c.keys())).collect();
        // The projection reads the values the program wrote: a structured value is stored under
        // its hash lane, dropped here (an `Arc` bump) so the gather moves only the value.
        let rec0: Vec<CValue> = chunks0.iter().map(|c| recover_val(c.vals())).collect();
        let rec1: Vec<CValue> = chunks1.iter().map(|c| recover_val(c.vals())).collect();
        let vals0: Vec<Option<&CValue>> = rec0.iter().map(Some).collect();
        let vals1: Vec<Option<&CValue>> = rec1.iter().map(Some).collect();

        let n = matches.ids.len();
        let (mut tag0, mut off0) = (Vec::new(), Vec::new());
        let (mut tag1, mut off1) = (Vec::new(), Vec::new());
        let mut start = 0usize;
        while start < n {
            let end = (start + TARGET_OUT).min(n);
            tag0.clear(); off0.clear(); tag1.clear(); off1.clear();
            let kept = if self.colliding.is_empty() {
                // The universal hot path: preserve the old allocation-free slice copies.
                for (_, (c0, c1)) in &matches.ids[start..end] {
                    tag0.push((c0 >> COORD_BITS) as usize);
                    off0.push((c0 & ((1 << COORD_BITS) - 1)) as usize);
                    tag1.push((c1 >> COORD_BITS) as usize);
                    off1.push((c1 & ((1 << COORD_BITS) - 1)) as usize);
                }
                None
            } else {
                let mut kept = Vec::with_capacity(end - start);
                for (index, (token, (c0, c1))) in matches.ids[start..end].iter().enumerate() {
                    let c0_chunk = (c0 >> COORD_BITS) as usize;
                    let c0_row = (c0 & ((1 << COORD_BITS) - 1)) as usize;
                    let c1_chunk = (c1 >> COORD_BITS) as usize;
                    let c1_row = (c1 & ((1 << COORD_BITS) - 1)) as usize;
                    if self.colliding.binary_search(token).is_ok()
                        && compare_at(chunks0[c0_chunk].keys(), c0_row, chunks1[c1_chunk].keys(), c1_row)
                            != Ordering::Equal
                    {
                        continue;
                    }
                    kept.push(start + index);
                    tag0.push(c0_chunk);
                    off0.push(c0_row);
                    tag1.push(c1_chunk);
                    off1.push(c1_row);
                }
                Some(kept)
            };
            if kept.as_ref().is_some_and(Vec::is_empty) {
                start = end;
                continue;
            }
            // The join's projection is written against the key the program declared, so drop
            // the arrangement's leading identifier lane before evaluating it. The output goes to
            // an arrange, which re-derives the identifier for the new key.
            let kc = recover_key(&gather_lanes(&keys0, &tag0, &off0));
            let v0 = gather_lanes(&vals0, &tag0, &off0);
            let v1 = gather_lanes(&vals1, &tag1, &off1);
            let proj = compile_join_projection(&self.key, &self.val, &shape_of_value(&kc), &shape_of_value(&v0), &shape_of_value(&v1))
                .unwrap_or_else(|e| panic!("join projection: type error: {e}"));
            let projected = corgi::eval_graph(&proj, CValue::Prod(vec![kc, v0, v1]));
            let mut cols = projected.into_prod("corgi join projection").unwrap();
            let nv = cols.pop().unwrap();
            let nk = cols.pop().unwrap();
            output.push(CorgiContainer {
                keys: nk,
                vals: nv,
                times: kept.as_ref().map_or_else(
                    || { let mut t = ColTimes::new(); t.push_range(&matches.times, start, end); t },
                    |kept| matches.times.gather(kept),
                ),
                diffs: kept.as_ref().map_or_else(
                    || matches.diffs[start..end].to_vec(),
                    |kept| kept.iter().map(|&index| matches.diffs[index]).collect(),
                ),
            });
            start = end;
        }
    }
}

/// The instance's chunks on one side, in the deterministic order coordinates index
/// (batches in order, chunks in order) — `advance` and `cross` must agree on it.
fn side_chunks<T: ColTime>(batches: &[CBatch<T>]) -> Vec<&CorgiChunk<T, Diff>> {
    // Coordinates use this same filtered ordering in `advance` and `cross`. Excluding empties here
    // makes the convention explicit and keeps `ident` from inspecting a shape-less `Unit(0)` key.
    let chunks: Vec<&CorgiChunk<T, Diff>> = batches
        .iter()
        .flat_map(|batch| batch.chunks.iter())
        .filter(|chunk| chunk.len() > 0)
        .collect();
    assert!(chunks.len() < (1 << (64 - COORD_BITS)), "too many chunks for coordinate packing");
    chunks
}

/// The column flattened to `u64` leaf lanes: `Some(lanes)` when it is a (possibly nested)
/// product of 64-bit leaves — the shape DDIR tuples transcode to — so row order is the
/// lexicographic order of the lane tuples. `Sum`/`List`/narrow leaves give `None`
/// (structural compares over the stored form, whose leading hash lane decides most of them).
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

/// Whether every nonempty chunk's val column flattens to `u64` leaf lanes.
fn leaf_valued<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>]) -> bool {
    chunks.iter().filter(|c| c.len() > 0).all(|c| leaf_lanes(c.vals()).is_some())
}

/// Pull rows `idx` of the column's leaf lanes as `u64` buffers.
fn pull_lanes(col: &CValue, idx: &[usize]) -> Vec<Vec<u64>> {
    leaf_lanes(col).expect("pull_lanes: leaf-laned column")
        .into_iter()
        .map(|lane| gather(lane, idx).into_u64("corgi join lane pull").unwrap())
        .collect()
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
/// so a side that nets to zero suppresses the key before anything reaches the bridges. Times
/// are rows of a lane column, each the join of a chunk's row with the instance's lower bound.
struct SideScratch<T> {
    coords: Vec<u64>,
    times: ColTimes<T>,
    diffs: Vec<Diff>,
    /// `(time, diff)` scratch for one value's cross-chunk merge.
    tds: (ColTimes<T>, Vec<Diff>),
    /// Index scratch for the sorts.
    order: Vec<usize>,
}

impl<T: ColTime> SideScratch<T> {
    fn new() -> Self {
        SideScratch { coords: Vec::new(), times: ColTimes::new(), diffs: Vec::new(), tds: (ColTimes::new(), Vec::new()), order: Vec::new() }
    }

    fn is_empty(&self) -> bool { self.coords.is_empty() }

    fn clear(&mut self) {
        self.coords.clear();
        self.times.clear();
        self.diffs.clear();
    }

    /// Append `(coord, join(times[row], lower), diff)`, merged with the last entry when it has
    /// the same coordinate and time.
    fn push(&mut self, coord: u64, times: &ColTimes<T>, row: usize, lower: &ColTimes<T>, diff: Diff) {
        let n = self.times.push_join_cross(times, row, lower, 0);
        if n > 0 && self.coords[n - 1] == coord && self.times.cmp(n - 1, n) == Ordering::Equal {
            self.diffs[n - 1] += diff;
            TimeColumn::truncate(&mut self.times, n);
        } else {
            self.coords.push(coord);
            self.diffs.push(diff);
        }
    }

    /// Reorder the entries by `(coord, time)` and drop zero diffs.
    fn sort_and_retain(&mut self, sort: bool) {
        let n = self.coords.len();
        self.order.clear();
        self.order.extend(0..n);
        if sort {
            let (coords, times) = (&self.coords, &self.times);
            self.order.sort_by(|&a, &b| coords[a].cmp(&coords[b]).then_with(|| times.cmp(a, b)));
        }
        let mut coords = Vec::with_capacity(n);
        let mut times = ColTimes::new();
        let mut diffs = Vec::with_capacity(n);
        for &i in &self.order {
            if self.diffs[i] != 0 {
                coords.push(self.coords[i]);
                times.push_ref(&self.times, i);
                diffs.push(self.diffs[i]);
            }
        }
        self.coords = coords;
        self.times = times;
        self.diffs = diffs;
    }

    /// Stage one key's records from `runs` (its equal-key row ranges, one per chunk holding it):
    /// values merged across chunks by content, equal values sharing the canonical coordinate of
    /// their least occurrence, times advanced by `lower`, consolidated, zeros dropped. Entries
    /// end sorted by `(coord, time)`.
    fn stage_runs(&mut self, runs: &[RunRef<'_, T>], lower: &ColTimes<T>) {
        self.clear();
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
                self.push(coord_hi | v_start as u64, run.chunk.times(), row, lower, run.chunk.diffs()[row]);
            }
            self.sort_and_retain(false);
        } else {
            // Cross-chunk merge by value content: heads are (run index, row); each step takes
            // the least value among heads, drains every chunk's sub-run of it into `tds`,
            // and consolidates. The canonical coordinate is the least contributing (chunk, row).
            let mut heads: Vec<(usize, usize)> = runs.iter().enumerate().map(|(i, r)| (i, r.s)).collect();
            let mut tds = std::mem::take(&mut self.tds);
            while !heads.is_empty() {
                let mut min = 0usize;
                for h in 1..heads.len() {
                    if runs[heads[h].0].val_less(heads[h].1, &runs[heads[min].0], heads[min].1) {
                        min = h;
                    }
                }
                let (ri_min, row_min) = heads[min];
                tds.0.clear();
                tds.1.clear();
                let mut coord = u64::MAX;
                let mut h = 0usize;
                while h < heads.len() {
                    let (ri, row) = heads[h];
                    let run = &runs[ri];
                    if run.val_eq(row, &runs[ri_min], row_min) {
                        let r_end = run.val_run_end(row);
                        for r in row..r_end {
                            tds.0.push_join_cross(run.chunk.times(), r, lower, 0);
                            tds.1.push(run.chunk.diffs()[r]);
                        }
                        coord = coord.min(((run.cid as u64) << COORD_BITS) | row as u64);
                        if r_end < run.e { heads[h] = (ri, r_end); h += 1; } else { heads.swap_remove(h); }
                    } else {
                        h += 1;
                    }
                }
                // In time order, merged with the last entry where the time repeats.
                self.order.clear();
                self.order.extend(0..tds.1.len());
                let times = &tds.0;
                self.order.sort_by(|&a, &b| times.cmp(a, b));
                for &i in &self.order {
                    let n = self.times.push_ref(&tds.0, i);
                    if n > 0 && self.coords[n - 1] == coord && self.times.cmp(n - 1, n) == Ordering::Equal {
                        self.diffs[n - 1] += tds.1[i];
                        TimeColumn::truncate(&mut self.times, n);
                    } else {
                        self.coords.push(coord);
                        self.diffs.push(tds.1[i]);
                    }
                }
            }
            self.tds = tds;
            // Canonical coordinates interleave chunks; restore bridge order.
            self.sort_and_retain(true);
        }
    }

    /// Move the staged entries into `bridge` under group token `k`.
    fn emit(&mut self, k: u64, bridge: &mut Bridge<ColTimes<T>, Diff>) -> usize {
        let n = self.coords.len();
        for i in 0..n {
            bridge.push_from((k, self.coords[i]), &self.times, i, self.diffs[i]);
        }
        self.clear();
        n
    }
}

/// The identifier column of a chunk's keys, as a sorted `u64` slice. Every arrangement key
/// leads with its identifier ([`present_key`](crate::corgi::chunk::present_key)), so this is
/// always available and always ordered — no gather, no copy.
fn ident<'a, T: ColTime>(chunk: &'a CorgiChunk<T, Diff>) -> &'a [u64] {
    corgi::arrange::leaf_slice(key_lane(chunk.keys())).expect("the identifier lane is a u64 leaf")
}

/// The block's exclusive identifier bound: for each chunk with more than `budget` rows left,
/// take the identifier `budget` rows in and bump past its whole run, then choose the least of
/// those bounds. A chunk may therefore contribute `budget + run length` rows rather than obeying
/// a hard cap; the expansion is what keeps one identifier wholly within one block. `None` when no
/// chunk is over budget — the block runs to the end and the walk is exhausted.
///
/// This is the whole point of a totally ordered identifier: the block's extent is decided by
/// reading ONE value per chunk, before anything is read in bulk, so each chunk can then be
/// read exactly once over exactly the rows the block needs. Deciding it the other way round —
/// read a fixed number of rows, then discover how far the block can reach — leaves whatever
/// was read past the bound to be discarded and read again next block, and the further apart
/// the chunks' key densities are, the more that is.
fn block_horizon<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>], starts: &[usize], budget: usize) -> Option<u64> {
    let mut horizon: Option<u64> = None;
    for (c, &s) in chunks.iter().zip(starts) {
        let lane = ident(c);
        if lane.len() - s <= budget {
            continue;
        }
        // Past the whole run of the identifier at the budget, so the block ends at a key
        // boundary. `None` on overflow means nothing can exceed it: run to the end.
        let Some(bound) = lane[s + budget].checked_add(1) else { continue };
        horizon = Some(horizon.map_or(bound, |h: u64| h.min(bound)));
    }
    horizon
}

/// Where each chunk's block ends: the first row at or past `horizon`, or its end.
fn block_ends<T: ColTime>(chunks: &[&CorgiChunk<T, Diff>], horizon: Option<u64>) -> Vec<usize> {
    chunks.iter().map(|c| match horizon {
        Some(h) => ident(c).partition_point(|&x| x < h),
        None => c.len(),
    }).collect()
}

/// View over one leaf-keyed chunk's rows for THIS block, `[base, end)`. The identifiers are
/// borrowed from the chunk's own lane — the block reads them, it does not copy them — and the
/// vals are gathered once, over exactly those rows.
struct LeafView<'a, T: ColTime> {
    chunk: &'a CorgiChunk<T, Diff>,
    cid: usize,
    /// Absolute row of `keys[0]`.
    base: usize,
    keys: &'a [u64],
    /// Leaf-laned vals over the same rows; `None` when vals are structured.
    vals: Option<Vec<Vec<u64>>>,
    /// Cursor within `keys`.
    cur: usize,
}

/// Rows per chunk per block. Bounds the block's working set; nothing outside it is read.
const PULL: usize = 1 << 14;

impl<'a, T: ColTime> LeafView<'a, T> {
    fn new(chunk: &'a CorgiChunk<T, Diff>, cid: usize, start: usize, end: usize, leaf_vals: bool) -> Self {
        let vals = (leaf_vals && end > start).then(|| pull_lanes(chunk.vals(), &(start..end).collect::<Vec<_>>()));
        LeafView { chunk, cid, base: start, keys: &ident(chunk)[start..end], vals, cur: 0 }
    }
    /// The key under the cursor, if any remains in this block.
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
/// chunk, with the matched rows' vals gathered once as a `u64` buffer
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
    fn new(chunk: &'a CorgiChunk<T, Diff>, cid: usize, needles: &[u64], leaf_vals: bool) -> Self {
        let keys = corgi::arrange::leaf_slice(key_lane(chunk.keys())).expect("identifier lane is a u64 leaf");
        let (mut lo, mut hi) = (vec![0; needles.len()], vec![0; needles.len()]);
        for (j, range) in MatchingRanges::new(needles, keys) {
            lo[j] = range.start;
            hi[j] = range.end;
        }
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

/// Append every record of `from` to `into`.
fn append_bridge<T: ColTime>(into: &mut Bridge<ColTimes<T>, Diff>, from: &Bridge<ColTimes<T>, Diff>) {
    for i in 0..from.len() {
        into.push_from(from.ids[i], &from.times, i, from.diffs[i]);
    }
}

/// Whether every row covered by `refs` carries the same KEY, not merely the same identifier.
///
/// The identifier is injective for a primitive-integer key, but a hashed key can in principle put
/// two distinct keys under one identifier, and the leaf path would then cross-product them. Runs
/// are contiguous and sub-sorted by the real key ([`present_key`](crate::corgi::chunk::present_key)
/// keeps the key in the column, after the hash), so a run holds one key exactly when its first and
/// last rows agree — a handful of `compare_at` per matched key, never per row. `refs` spans both
/// sides, so one pass also confirms the two sides matched on the key and not just the hash.
fn one_key<T: ColTime>(a: &[RunRef<'_, T>], b: &[RunRef<'_, T>]) -> bool {
    let Some(first) = a.first().or_else(|| b.first()) else { return true };
    let (rk, ri) = (first.chunk.keys(), first.s);
    a.iter().chain(b).all(|r| {
        compare_at(r.chunk.keys(), r.s, rk, ri) == Ordering::Equal
            && compare_at(r.chunk.keys(), r.e - 1, rk, ri) == Ordering::Equal
    })
}

/// Stage a colliding identifier without allowing equal values from different real keys to
/// consolidate together. Each real key is staged independently, but all retain the identifier as
/// their proxy token; `cross` uses the coordinates to discard unequal-key pairs afterward.
fn stage_collision<T: ColTime>(
    runs: &[RunRef<'_, T>],
    lower: &ColTimes<T>,
    token: u64,
    bridge: &mut Bridge<ColTimes<T>, Diff>,
) {
    let mut positions: Vec<usize> = runs.iter().map(|run| run.s).collect();
    let mut scratch = SideScratch::new();
    let mut staged: Bridge<ColTimes<T>, Diff> = Bridge::default();
    loop {
        let Some(min) = positions
            .iter()
            .enumerate()
            .filter(|(index, position)| **position < runs[*index].e)
            .min_by(|(ai, ap), (bi, bp)| {
                compare_at(runs[*ai].chunk.keys(), **ap, runs[*bi].chunk.keys(), **bp)
            })
            .map(|(index, _)| index)
        else {
            break;
        };
        let (reference, row) = (runs[min].chunk.keys(), positions[min]);
        let mut equal_runs = Vec::new();
        for (index, run) in runs.iter().enumerate() {
            let start = positions[index];
            if start == run.e
                || compare_at(run.chunk.keys(), start, reference, row) != Ordering::Equal
            {
                continue;
            }
            let end = start
                + (start..run.e)
                    .position(|candidate| {
                        compare_at(run.chunk.keys(), candidate, reference, row) != Ordering::Equal
                    })
                    .unwrap_or(run.e - start);
            let vals = run
                .vals
                .map(|(lanes, offset)| (lanes, offset + start - run.s));
            equal_runs.push(RunRef { chunk: run.chunk, cid: run.cid, s: start, e: end, vals });
            positions[index] = end;
        }
        scratch.stage_runs(&equal_runs, lower);
        scratch.emit(token, &mut staged);
    }
    staged.consolidate();
    for i in 0..staged.len() {
        bridge.push_from(staged.ids[i], &staged.times, i, staged.diffs[i]);
    }
}

/// Blockwise `advance` over every arrangement key shape: group token = the leading identifier
/// lane (the key itself or its carried hash), so blocks resume by seeking `from` and end at
/// identifier boundaries.
///
/// Two regimes: when one side is much smaller (the fresh delta against an accumulated
/// trace), the small side DRIVES and the large side is presented only at the driver's keys
/// (sorted probes — cost tracks the driver plus matches). When the sides are
/// comparable, probing costs `n log n` against a merge's `n`, so both sides are pulled and
/// merged symmetrically instead.
fn advance_leaf<T: ColTime>(
    chunks0: &[&CorgiChunk<T, Diff>],
    chunks1: &[&CorgiChunk<T, Diff>],
    lower: &ColTimes<T>,
    from: &mut Option<u64>,
    bridge0: &mut Bridge<ColTimes<T>, Diff>,
    bridge1: &mut Bridge<ColTimes<T>, Diff>,
    colliding: &mut Vec<u64>,
) {
    let start = from.expect("advance called on an exhausted unit");
    let hashed = key_is_hashed(chunks0[0].keys());
    // Resume: the first row of each chunk at or past `start`, by binary search on its identifier
    // lane. The lane is a sorted `u64` slice, so this is a slice operation, not a column probe.
    let seek = |chunks: &[&CorgiChunk<T, Diff>]| -> Vec<usize> {
        chunks.iter().map(|c| ident(c).partition_point(|&x| x < start)).collect()
    };
    let start0 = seek(chunks0);
    let start1 = seek(chunks1);
    let remaining = |chunks: &[&CorgiChunk<T, Diff>], starts: &[usize]| -> usize {
        chunks.iter().zip(starts).map(|(c, &s)| c.len() - s).sum()
    };
    let (r0, r1) = (remaining(chunks0, &start0), remaining(chunks1, &start1));
    fn views<'a, T: ColTime>(chunks: &[&'a CorgiChunk<T, Diff>], starts: &[usize], ends: &[usize]) -> Vec<LeafView<'a, T>> {
        let leaf_vals = leaf_valued(chunks);
        chunks.iter().enumerate()
            .filter(|(cid, _)| ends[*cid] > starts[*cid])
            .map(|(cid, c)| LeafView::new(c, cid, starts[cid], ends[cid], leaf_vals))
            .collect()
    }
    if r0.max(r1) >= 2 * r0.min(r1) {
        // Lopsided: only the driver is read in bulk, so only the driver bounds the block.
        let drive0 = r0 <= r1;
        let (dchunks, dstarts, pchunks) = if drive0 { (chunks0, &start0, chunks1) } else { (chunks1, &start1, chunks0) };
        let h = block_horizon(dchunks, dstarts, PULL);
        let dviews = views(dchunks, dstarts, &block_ends(dchunks, h));
        let (bd, bp) = if drive0 { (bridge0, bridge1) } else { (bridge1, bridge0) };
        leaf_probe(dviews, pchunks, lower, h, from, bd, bp, hashed, colliding)
    } else {
        // Symmetric: both sides are read, so both bound the block.
        let h = block_horizon(chunks0, &start0, PULL).into_iter()
            .chain(block_horizon(chunks1, &start1, PULL))
            .min();
        let views0 = views(chunks0, &start0, &block_ends(chunks0, h));
        let views1 = views(chunks1, &start1, &block_ends(chunks1, h));
        leaf_merge(views0, views1, lower, h, from, bridge0, bridge1, hashed, colliding)
    }
}

/// Lopsided regime: the driver views are walked; the probee is presented only at the
/// driver's sorted keys, one monotone search per probee chunk per block.
fn leaf_probe<'a, T: ColTime>(
    mut dviews: Vec<LeafView<'a, T>>,
    pchunks: &[&CorgiChunk<T, Diff>],
    lower: &ColTimes<T>,
    h: Option<u64>,
    from: &mut Option<u64>,
    bridge_d: &mut Bridge<ColTimes<T>, Diff>,
    bridge_p: &mut Bridge<ColTimes<T>, Diff>,
    hashed: bool,
    colliding: &mut Vec<u64>,
) {
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
        .map(|(cid, c)| Probe::new(c, cid, &keyset, pvleaf))
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
        let collision = hashed && !one_key(&refs, &[]);
        if collision {
            colliding.push(k);
        }
        let (drefs, prefs) = refs.split_at(dref_count);
        if collision {
            let (mut staged_d, mut staged_p): (Bridge<ColTimes<T>, Diff>, Bridge<ColTimes<T>, Diff>) = (Bridge::default(), Bridge::default());
            stage_collision(drefs, lower, k, &mut staged_d);
            stage_collision(prefs, lower, k, &mut staged_p);
            if !staged_d.is_empty() && !staged_p.is_empty() {
                append_bridge(bridge_d, &staged_d);
                append_bridge(bridge_p, &staged_p);
            }
            continue;
        }
        sd.stage_runs(drefs, lower);
        sp.stage_runs(prefs, lower);
        if sd.is_empty() || sp.is_empty() {
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
    lower: &ColTimes<T>,
    h: Option<u64>,
    from: &mut Option<u64>,
    bridge0: &mut Bridge<ColTimes<T>, Diff>,
    bridge1: &mut Bridge<ColTimes<T>, Diff>,
    hashed: bool,
    colliding: &mut Vec<u64>,
) {
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
        let collision = hashed && !one_key(&r0, &r1);
        if collision {
            colliding.push(k);
        }
        if collision {
            let (mut staged0, mut staged1): (Bridge<ColTimes<T>, Diff>, Bridge<ColTimes<T>, Diff>) = (Bridge::default(), Bridge::default());
            stage_collision(&r0, lower, k, &mut staged0);
            stage_collision(&r1, lower, k, &mut staged1);
            if !staged0.is_empty() && !staged1.is_empty() {
                append_bridge(bridge0, &staged0);
                append_bridge(bridge1, &staged1);
            }
            continue;
        }
        s0.stage_runs(&r0, lower);
        s1.stage_runs(&r1, lower);
        if s0.is_empty() || s1.is_empty() {
            continue;
        }
        s0.emit(k, bridge0);
        s1.emit(k, bridge1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn batch(rows: &[(u64, u64, u64)]) -> CBatch<u64> {
        let keys = CValue::Prod(vec![
            CValue::u64(rows.iter().map(|row| row.0).collect()),
            CValue::Prod(vec![
                CValue::u64(rows.iter().map(|row| row.1).collect()),
                CValue::u64(rows.iter().map(|row| row.2).collect()),
            ]),
        ]);
        // Deliberately equal across different real keys: collision staging must not consolidate
        // these together before `cross` has a chance to compare their keys.
        let vals = CValue::u64(vec![0; rows.len()]);
        let chunk = CorgiChunk::from_columns(keys, vals, vec![0; rows.len()].into_iter().collect(), vec![1; rows.len()]);
        Rc::new(ChunkBatch::new(vec![chunk]))
    }

    fn backend() -> CorgiJoinBackend<u64> {
        CorgiJoinBackend::new(
            Term::Var(0),
            Term::Tuple(vec![Term::Var(1), Term::Var(2)]),
        )
    }

    fn cross_bridges(
        backend: &mut CorgiJoinBackend<u64>,
        instance: &JoinInstance<u64, CBatch<u64>, CBatch<u64>>,
        left: &Bridge<ColTimes<u64>, Diff>,
        right: &Bridge<ColTimes<u64>, Diff>,
    ) -> usize {
        let mut matches: JoinMatches<ColTimes<u64>, Diff> = JoinMatches::default();
        for a in 0..left.len() {
            for b in 0..right.len() {
                if left.ids[a].0 == right.ids[b].0 {
                    matches.ids.push((left.ids[a].0, (left.ids[a].1, right.ids[b].1)));
                    matches.times.push_join_cross(&left.times, a, &right.times, b);
                    matches.diffs.push(left.diffs[a] * right.diffs[b]);
                }
            }
        }
        let mut output = Vec::new();
        backend.cross(instance, &mut matches, &mut output);
        output.iter().map(|container| container.diffs.len()).sum()
    }

    #[test]
    fn collision_after_completed_block_filters_real_keys_without_restarting() {
        let collision = PULL as u64 + 1;
        let mut rows: Vec<_> = (0..collision).map(|id| (id, id, 0)).collect();
        rows.extend([(collision, 7, 0), (collision, 8, 0)]);
        let instance = JoinInstance {
            batches0: vec![batch(&rows)],
            batches1: vec![batch(&rows)],
            lower: 0,
        };
        let mut backend = backend();
        let mut from = Some(0);
        let (mut left, mut right): (Bridge<ColTimes<u64>, Diff>, Bridge<ColTimes<u64>, Diff>) = (Bridge::default(), Bridge::default());

        backend.advance(&instance, &mut from, &mut left, &mut right);
        assert!(from.is_some(), "the first block must leave work for the collision block");
        assert!(backend.colliding.is_empty());

        left.clear();
        right.clear();
        backend.advance(&instance, &mut from, &mut left, &mut right);
        assert_eq!(backend.colliding, vec![collision]);
        assert_eq!(cross_bridges(&mut backend, &instance, &left, &right), 2);
    }

    #[test]
    fn lopsided_collision_filters_real_keys() {
        let collision = 42;
        let left_rows = [(collision, 7, 0), (collision, 8, 0)];
        let mut right_rows: Vec<_> = (0..8).map(|id| (id, id, 0)).collect();
        right_rows.extend([(collision, 7, 0), (collision, 8, 0)]);
        let instance = JoinInstance {
            batches0: vec![batch(&left_rows)],
            batches1: vec![batch(&right_rows)],
            lower: 0,
        };
        let mut backend = backend();
        let mut from = Some(0);
        let (mut left, mut right): (Bridge<ColTimes<u64>, Diff>, Bridge<ColTimes<u64>, Diff>) = (Bridge::default(), Bridge::default());

        backend.advance(&instance, &mut from, &mut left, &mut right);
        assert_eq!(backend.colliding, vec![collision]);
        assert_eq!(cross_bridges(&mut backend, &instance, &left, &right), 2);
    }
}
