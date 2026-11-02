//! The join's staging on lanes: each side's rows of a key are copied from the chunks by ranges,
//! joined with `lower` lane by lane, and consolidated by sorting positions, so that no owned time
//! is built per staged row. The half join keeps the row staging of [`SideScratch`].

use timely::progress::frontier::AntichainRef;

use super::*;
use differential_dataflow::operators::int_proxy::join::JoinBridge;
use crate::runtime::col_times::ColTimes;

/// The join's bridge, as DDIR fills it: `i64` group tokens, `u64` value tokens, times as lanes.
pub(super) type LaneBridge<T> = JoinBridge<ColTimes<T>, Diff, i64>;

/// Per-key staging for one side: consolidated `(token, time, diff)` columns, built in scratch so a
/// side that nets to zero suppresses the key before anything reaches the bridges.
pub(super) struct LaneScratch<T> {
    tokens: Vec<u64>,
    times: ColTimes<T>,
    diffs: Vec<Diff>,
    /// Row positions in `(token, time)` order, reused across keys.
    order: Vec<usize>,
    /// Cross-chunk value-merge heads, reused across keys.
    heads: Vec<(usize, usize)>,
    /// Row indices sorted by projected value, reused across keys.
    idx: Vec<(u32, u32)>,
}

impl<T: ColTime> LaneScratch<T> {
    pub(super) fn new() -> Self {
        LaneScratch { tokens: Vec::new(), times: ColTimes::new(), diffs: Vec::new(), order: Vec::new(), heads: Vec::new(), idx: Vec::new() }
    }

    pub(super) fn is_empty(&self) -> bool { self.tokens.is_empty() }

    fn clear(&mut self) {
        self.tokens.clear();
        self.times.clear();
        self.diffs.clear();
    }

    /// Rows `[s, e)` of `run`, under one token.
    fn range(&mut self, run: &RunRef<'_, T>, s: usize, e: usize, token: u64) {
        self.tokens.resize(self.tokens.len() + (e - s), token);
        self.times.push_range(run.chunk.times(), s, e);
        self.diffs.extend_from_slice(&run.chunk.diffs()[s..e]);
    }

    /// Stage one key's records from `runs` (its equal-key row ranges, one per chunk holding it),
    /// with the tokens [`SideScratch::stage_runs`] mints: primitive values identify themselves;
    /// other values share their least occurrence's coordinate across chunks. Times are joined with
    /// `lower`, consolidated, zeros dropped. Rows end sorted by `(token, time)`.
    pub(super) fn stage_runs(&mut self, runs: &[RunRef<'_, T>], lower: &T) {
        self.clear();
        let Some(first) = runs.first() else { return };
        if primitive_values(first.chunk) {
            for run in runs {
                // An unread primitive value is one token (`cross` never reads it back).
                if run.kind == ProjKind::Collapse {
                    self.range(run, run.s, run.e, 0);
                    continue;
                }
                let ids = int_lane(run.chunk.vals()).unwrap();
                self.tokens.extend(ids[run.s..run.e].iter().map(|&v| v as u64));
                self.times.push_range(run.chunk.times(), run.s, run.e);
                self.diffs.extend_from_slice(&run.chunk.diffs()[run.s..run.e]);
            }
        } else if first.kind == ProjKind::Ungrouped && (runs.len() != 1 || first.e - first.s > 1) {
            // The stored order does not group equal projected values: sort the key's rows by
            // projected value. The canonical coordinate is the least contributing (chunk, row).
            let mut idx = std::mem::take(&mut self.idx);
            idx.clear();
            for (i, run) in runs.iter().enumerate() { idx.extend((run.s..run.e).map(|row| (i as u32, row as u32))); }
            idx.sort_unstable_by(|a, b| runs[a.0 as usize].val_cmp(a.1 as usize, &runs[b.0 as usize], b.1 as usize));
            let mut start = 0;
            while start < idx.len() {
                let (ri, row) = (idx[start].0 as usize, idx[start].1 as usize);
                let mut end = start + 1;
                while end < idx.len() && runs[idx[end].0 as usize].val_eq(idx[end].1 as usize, &runs[ri], row) { end += 1; }
                let coord = idx[start..end].iter().map(|&(i, r)| ((runs[i as usize].cid as u64) << COORD_BITS) | r as u64).min().unwrap();
                for &(i, r) in &idx[start..end] {
                    self.range(&runs[i as usize], r as usize, r as usize + 1, coord);
                }
                start = end;
            }
            self.idx = idx;
        } else if let [run] = runs {
            // Single-chunk run: values are grouped, and each value run's token is its first row.
            let coord_hi = (run.cid as u64) << COORD_BITS;
            let mut start = run.s;
            while start < run.e {
                let end = run.val_run_end(start);
                self.range(run, start, end, coord_hi | start as u64);
                start = end;
            }
        } else {
            // Cross-chunk merge by value content: heads are (run index, row); each step takes the
            // least value among heads and copies every chunk's sub-run of it. The canonical
            // coordinate is the least contributing (chunk, row).
            let mut heads = std::mem::take(&mut self.heads);
            heads.clear();
            heads.extend(runs.iter().enumerate().map(|(i, r)| (i, r.s)));
            while !heads.is_empty() {
                let mut min = 0usize;
                for h in 1..heads.len() {
                    if runs[heads[h].0].val_less(heads[h].1, &runs[heads[min].0], heads[min].1) {
                        min = h;
                    }
                }
                let (ri_min, row_min) = heads[min];
                let coord = heads.iter()
                    .filter(|&&(ri, row)| runs[ri].val_eq(row, &runs[ri_min], row_min))
                    .map(|&(ri, row)| ((runs[ri].cid as u64) << COORD_BITS) | row as u64)
                    .min().unwrap();
                let mut h = 0usize;
                while h < heads.len() {
                    let (ri, row) = heads[h];
                    let run = &runs[ri];
                    if run.val_eq(row, &runs[ri_min], row_min) {
                        let end = run.val_run_end(row);
                        self.range(run, row, end, coord);
                        if end < run.e { heads[h] = (ri, end); h += 1; } else { heads.swap_remove(h); }
                    } else {
                        h += 1;
                    }
                }
            }
            self.heads = heads;
        }
        self.times.advance_by(AntichainRef::new(std::slice::from_ref(lower)), self.times.len());
        self.consolidate();
    }

    /// Sort the rows by `(token, time)`, sum equal rows and drop zeros. Rows already in order
    /// with no zero need nothing; rows in order need no sort.
    fn consolidate(&mut self) {
        let n = self.tokens.len();
        let mut order = std::mem::take(&mut self.order);
        let kept = {
            let (tokens, times) = (&self.tokens, &self.times);
            let cmp = |a: usize, b: usize| tokens[a].cmp(&tokens[b]).then_with(|| times.cmp(a, b));
            if self.diffs.iter().all(|d| *d != 0) && (1..n).all(|i| cmp(i - 1, i).is_lt()) {
                self.order = order;
                return;
            }
            order.clear();
            order.extend(0..n);
            if (1..n).any(|i| cmp(i - 1, i).is_gt()) { order.sort_unstable_by(|&a, &b| cmp(a, b)); }
            let mut kept = Vec::with_capacity(n);
            let mut diffs = Vec::with_capacity(n);
            for group in order.chunk_by(|&a, &b| cmp(a, b).is_eq()) {
                let diff: Diff = group.iter().map(|&i| self.diffs[i]).sum();
                if diff != 0 { kept.push(group[0]); diffs.push(diff); }
            }
            self.diffs = diffs;
            kept
        };
        self.tokens = kept.iter().map(|&i| self.tokens[i]).collect();
        self.times = self.times.gather(&kept);
        self.order = order;
    }

    /// Append `other`'s rows.
    fn append(&mut self, other: &Self) {
        self.tokens.extend_from_slice(&other.tokens);
        self.times.push_range(&other.times, 0, other.times.len());
        self.diffs.extend_from_slice(&other.diffs);
    }

    /// Append the staged rows to `bridge` under group token `k`.
    pub(super) fn emit(&self, k: i64, bridge: &mut LaneBridge<T>) {
        let n = self.tokens.len();
        bridge.keys.resize(bridge.keys.len() + n, k);
        bridge.vals.extend_from_slice(&self.tokens);
        bridge.times.push_range(&self.times, 0, n);
        bridge.diffs.extend_from_slice(&self.diffs);
    }
}

/// Stage a non-colliding key, emitting it only when both sides survive consolidation.
pub(super) fn stage_pair<T: ColTime>(
    token: i64,
    runs: (&[RunRef<'_, T>], &[RunRef<'_, T>]),
    lower: &T,
    scratch: (&mut LaneScratch<T>, &mut LaneScratch<T>),
    bridges: (&mut LaneBridge<T>, &mut LaneBridge<T>),
) {
    scratch.0.stage_runs(runs.0, lower);
    scratch.1.stage_runs(runs.1, lower);
    if !scratch.0.is_empty() && !scratch.1.is_empty() {
        scratch.0.emit(token, bridges.0);
        scratch.1.emit(token, bridges.1);
    }
}

/// Stage a colliding identifier, as [`super::stage_collision`] does on rows: each real key is
/// staged on its own, so equal values from different real keys never consolidate together, and
/// all keep the identifier as their token; `cross` discards unequal-key pairs afterward.
pub(super) fn stage_collision<T: ColTime>(runs: &[RunRef<'_, T>], lower: &T) -> LaneScratch<T> {
    let mut positions: Vec<usize> = runs.iter().map(|run| run.s).collect();
    let mut scratch = LaneScratch::new();
    let mut staged = LaneScratch::new();
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
            equal_runs.push(RunRef { chunk: run.chunk, cid: run.cid, s: start, e: end, vals: run.vals, pcol: run.pcol, kind: run.kind });
            positions[index] = end;
        }
        scratch.stage_runs(&equal_runs, lower);
        staged.append(&scratch);
    }
    // Coordinates differ between real keys, so this only orders the rows by `(token, time)`.
    staged.consolidate();
    staged
}
