//! The join's staging on lanes: each side's rows of a key are copied from the chunks by ranges
//! straight into the bridge, joined with `lower` as they are copied, so that no owned time is built
//! per staged row. Each side of a key is then consolidated in place, sorting a row record of its
//! token and first two time coordinates when the rows are out of order, and a key with an empty
//! side is truncated off both bridges. The half join keeps the row staging of [`SideScratch`].

use super::*;
use differential_dataflow::operators::int_proxy::join::JoinBridge;
use crate::runtime::col_times::ColTimes;

/// The join's bridge, as DDIR fills it: `i64` group tokens, `u64` value tokens, times as lanes.
pub(super) type LaneBridge<T> = JoinBridge<ColTimes<T>, Diff, i64>;

/// Truncate `bridge` to its first `len` rows.
fn truncate<T>(bridge: &mut LaneBridge<T>, len: usize) {
    bridge.keys.truncate(len);
    bridge.vals.truncate(len);
    bridge.times.truncate_rows(len);
    bridge.diffs.truncate(len);
}

/// A staged row as a sort record: its token, its first two time coordinates, and its position.
type Record = (u64, u64, u64, usize);

/// Scratch for staging keys, reused across them.
pub(super) struct Staging {
    /// Cross-chunk value-merge heads.
    heads: Vec<(usize, usize)>,
    /// Row indices sorted by projected value.
    idx: Vec<(u32, u32)>,
    /// One key side's rows as records, to consolidate.
    records: Vec<Record>,
    /// The unit's lower bound, as coordinates.
    lower: smallvec::SmallVec<[u64; 4]>,
}

impl Staging {
    pub(super) fn new<T: ColTime>(lower: &T) -> Self {
        Staging { heads: Vec::new(), idx: Vec::new(), records: Vec::new(), lower: lower.coordinates().collect() }
    }

    /// Sort the rows from `start` on by `(token, time)`, sum equal rows and drop zeros, in place.
    /// Rows already in order with no zero need nothing; rows in order need no sort. Returns whether
    /// any row remains.
    fn settle<T: ColTime>(&mut self, bridge: &mut LaneBridge<T>, start: usize) -> bool {
        let n = bridge.len();
        let (kept, diffs) = {
            let times = &bridge.times;
            let lanes = times.raw_lanes();
            let wide = lanes.len() > 2;
            let lane = |j: usize, i: usize| lanes.get(j).map_or(0, |l| l[i]);
            // Times beyond two coordinates break ties by comparing lanes.
            let cmp = |a: &Record, b: &Record| (a.0, a.1, a.2).cmp(&(b.0, b.1, b.2))
                .then_with(|| if wide { times.cmp(a.3, b.3) } else { Ordering::Equal });
            let vals = &bridge.vals;
            let in_order = (start + 1..n).all(|i| vals[i - 1].cmp(&vals[i]).then_with(|| times.cmp(i - 1, i)).is_lt());
            if in_order && bridge.diffs[start..].iter().all(|d| *d != 0) { return n > start; }
            let records = &mut self.records;
            records.clear();
            records.extend((start..n).map(|i| (vals[i], lane(0, i), lane(1, i), i)));
            if !in_order { records.sort_unstable_by(cmp); }
            let (mut kept, mut diffs) = (Vec::with_capacity(n - start), Vec::with_capacity(n - start));
            for group in records.chunk_by(|a, b| cmp(a, b).is_eq()) {
                let diff: Diff = group.iter().map(|r| bridge.diffs[r.3]).sum();
                if diff != 0 { kept.push(group[0].3); diffs.push(diff); }
            }
            (kept, diffs)
        };
        // Rewrite the key side's rows as their consolidation.
        let k = bridge.keys[start];
        let vals: Vec<u64> = kept.iter().map(|&i| bridge.vals[i]).collect();
        let times = bridge.times.gather(&kept);
        truncate(bridge, start);
        bridge.keys.resize(start + kept.len(), k);
        bridge.vals.extend(vals);
        bridge.times.push_range(&times, 0, times.len());
        bridge.diffs.extend(diffs);
        !kept.is_empty()
    }

    /// Append one key's records from `runs` (its equal-key row ranges, one per chunk holding it) to
    /// `bridge` under group token `k`, with the tokens [`SideScratch::stage_runs`] mints: primitive
    /// values identify themselves; other values share their least occurrence's coordinate across
    /// chunks. Times are joined with `lower` as they are copied.
    fn append_runs<T: ColTime>(&mut self, bridge: &mut LaneBridge<T>, runs: &[RunRef<'_, T>], k: i64) {
        let Some(first) = runs.first() else { return };
        let lower = &self.lower[..];
        let range = |bridge: &mut LaneBridge<T>, run: &RunRef<'_, T>, s: usize, e: usize, token: u64| {
            bridge.vals.resize(bridge.vals.len() + (e - s), token);
            bridge.times.push_range_joined(run.chunk.times(), s, e, lower);
            bridge.diffs.extend_from_slice(&run.chunk.diffs()[s..e]);
        };
        if primitive_values(first.chunk) {
            for run in runs {
                // An unread primitive value is one token (`cross` never reads it back).
                if run.kind == ProjKind::Collapse {
                    range(bridge, run, run.s, run.e, 0);
                    continue;
                }
                let ids = int_lane(run.chunk.vals()).unwrap();
                bridge.vals.extend(ids[run.s..run.e].iter().map(|&v| v as u64));
                bridge.times.push_range_joined(run.chunk.times(), run.s, run.e, lower);
                bridge.diffs.extend_from_slice(&run.chunk.diffs()[run.s..run.e]);
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
                    range(bridge, &runs[i as usize], r as usize, r as usize + 1, coord);
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
                bridge.vals.resize(bridge.vals.len() + (end - start), coord_hi | start as u64);
                start = end;
            }
            bridge.times.push_range_joined(run.chunk.times(), run.s, run.e, lower);
            bridge.diffs.extend_from_slice(&run.chunk.diffs()[run.s..run.e]);
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
                let first = bridge.vals.len();
                let mut coord = u64::MAX;
                let mut h = 0usize;
                while h < heads.len() {
                    let (ri, row) = heads[h];
                    let run = &runs[ri];
                    if run.val_eq(row, &runs[ri_min], row_min) {
                        let end = run.val_run_end(row);
                        coord = coord.min(((run.cid as u64) << COORD_BITS) | row as u64);
                        range(bridge, run, row, end, 0);
                        if end < run.e { heads[h] = (ri, end); h += 1; } else { heads.swap_remove(h); }
                    } else {
                        h += 1;
                    }
                }
                bridge.vals[first..].fill(coord);
            }
            self.heads = heads;
        }
        bridge.keys.resize(bridge.vals.len(), k);
    }

    /// Append a colliding identifier, as [`super::stage_collision`] does on rows: each real key is
    /// appended on its own, so equal values from different real keys never share a token (their
    /// coordinates differ), and all keep the identifier as their group token; `cross` discards
    /// unequal-key pairs afterward.
    fn append_collision<T: ColTime>(&mut self, bridge: &mut LaneBridge<T>, runs: &[RunRef<'_, T>], k: i64) {
        let mut positions: Vec<usize> = runs.iter().map(|run| run.s).collect();
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
            self.append_runs(bridge, &equal_runs, k);
        }
    }
}

/// Stage one key on both sides, straight into the bridges, consolidating each side in place, and
/// truncate it away again if either side nets to zero. A colliding key appends each real key apart.
pub(super) fn stage_pair<T: ColTime>(
    token: i64,
    runs: (&[RunRef<'_, T>], &[RunRef<'_, T>]),
    staging: &mut Staging,
    bridges: (&mut LaneBridge<T>, &mut LaneBridge<T>),
    collision: bool,
) {
    let (s0, s1) = (bridges.0.len(), bridges.1.len());
    if collision {
        staging.append_collision(bridges.0, runs.0, token);
        staging.append_collision(bridges.1, runs.1, token);
    } else {
        staging.append_runs(bridges.0, runs.0, token);
        staging.append_runs(bridges.1, runs.1, token);
    }
    // A side's rows run from where it began, so each settles apart.
    let a = staging.settle(bridges.0, s0);
    let b = staging.settle(bridges.1, s1);
    if !(a && b) {
        truncate(bridges.0, s0);
        truncate(bridges.1, s1);
    }
}

#[cfg(test)]
/// The lane staging of `runs` alone, as `(token, time, diff)` rows.
pub(super) fn staged<T: ColTime>(runs: &[RunRef<'_, T>], lower: &T) -> Vec<(u64, T, Diff)> {
    let mut bridge = LaneBridge::default();
    let mut staging = Staging::new(lower);
    staging.append_runs(&mut bridge, runs, 0);
    staging.settle(&mut bridge, 0);
    (0..bridge.len()).map(|i| (bridge.vals[i], bridge.times.get(i), bridge.diffs[i])).collect()
}
