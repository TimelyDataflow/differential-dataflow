//! The half join's backend: a unit's runs, read by identifier a block at a time, and the
//! arrangement's chunks, presented to dogs3's proxy half-join tactic, which decides which pairs meet
//! and states the output's times; the matched pairs are gathered into columns and projected, and
//! their data times evaluated lane by lane over the presented times.
//!
//! The arranged side is staged as the join backend stages its sides: one key's rows across
//! chunks are merged by projected value, equal projections share one token (the value itself for read integer
//! values under integer keys, else the coordinate of its first occurrence), and the records are
//! consolidated. Identifiers that hold more than one real key are recorded, and only pairs under
//! those have their real keys compared.

use std::borrow::Cow;

use timely::progress::Timestamp;

use differential_dataflow::operators::int_proxy::{KeyPosition, ProxyBridge};
use differential_dogs3::operators::half_join::proxy::{data_time, HalfJoinInstance, HalfJoinMatches, HalfJoinStream, Lifted, ProxyHalfJoinBackend};
use differential_dataflow::trace::chunk::Chunk;

use corgi::arrange::{compare_at, equal_idx, gather_lanes};
use corgi::{shape_of_value, Demand, Graph, NumOp, Shape, Value as CValue};

use crate::runtime::chunk::{colliding_keys, ident, key_is_hashed, recover_key_ref, rewrap, CorgiChunk};
use crate::runtime::col_times::{ColTimes, Lanes};
use crate::runtime::container::CorgiContainer;
use crate::runtime::join::{demanded, primitive_values, stage_collision, Probe, Reads, RunRef, SideProj, SideScratch, COORD_BITS};
use crate::lambda::compile_projection;
use crate::ir::{Diff, Lambda, Time};

use super::{NativeBatch, Run, CC};

/// Released updates are met with the arrangement this many at a time, so that the driver can yield
/// between blocks. Small under test, so that tests span many blocks.
const BLOCK: usize = if cfg!(test) { 3 } else { 1 << 14 };

/// The corgi [`ProxyHalfJoinBackend`]: released runs and arranged chunks, both in identifier
/// order, presented a block of identifiers at a time, and the matched pairs projected. The
/// projection is compiled once, at the shapes it reads, narrowed to the fields it reads, and
/// shared by every unit of work.
pub(crate) struct CorgiHalfJoinBackend {
    /// The number of lanes in a time as data: the scope depth, plus one.
    width: usize,
    /// The projection, narrowed to take its input projected by `reads.argument`.
    graph: Graph<NumOp>,
    /// What the graph reads of the key, the update's value, and the arranged value.
    reads: Reads,
    /// Whether the stream value loses fields, and can coalesce previously distinct rows.
    project_stream: bool,
    /// Identifiers in the current block that cover more than one real key, ascending. `advance`
    /// writes this and the immediately following `cross` reads it; only matches under these
    /// identifiers need a real-key comparison.
    colliding: Vec<i64>,
}

impl CorgiHalfJoinBackend {
    /// The projection over a key, an update's value, and an arranged value of these shapes. A
    /// type error is a panic with corgi's message: install type-checked the program, so it is not
    /// reached.
    pub(crate) fn new(projection: &Lambda, width: usize, [k, v0, v1]: [Shape; 3]) -> Self {
        let g = compile_projection(projection, &[k.clone(), v0.clone(), v1.clone()])
            .unwrap_or_else(|e| panic!("half_join: type error at shapes ({k}, {v0}, {v1}): {e}"));
        let input = Shape::Prod(vec![k, v0, v1]);
        let narrowed = corgi::compile(g, &input).and_then(|compiled| corgi::narrow(compiled, &Demand::All))
            .unwrap_or_else(|e| panic!("half_join: narrowing a compiled projection at {input}: {e}"));
        let reads = Reads::new(narrowed.input, &input);
        let Shape::Prod(parts) = &input else { unreachable!() };
        let project_stream = reads.columns[1].projected_shape(&parts[1]).expect("the stream value's demand") != parts[1];
        CorgiHalfJoinBackend { width, graph: narrowed.graph, reads, project_stream, colliding: Vec::new() }
    }
}

/// The unit's non-empty arranged chunks, in the order the arranged coordinates index.
fn arranged_chunks(batches: &[NativeBatch]) -> Vec<&CorgiChunk<Time, Diff>> {
    let chunks: Vec<_> = batches.iter().flat_map(|b| b.chunks.iter().filter(|c| c.len() > 0)).collect();
    assert!(chunks.len() < (1 << (64 - COORD_BITS)), "too many chunks for coordinate packing");
    chunks
}

/// A run's values as its fields `[val, time as data]`.
fn run_fields(run: &Run) -> &[CValue] {
    match run.vals() {
        CValue::Prod(fields) => &fields[..],
        _ => panic!("half_join: a stream value that is not (val, time)"),
    }
}

/// A run's times as data, as `width` integer lanes.
fn data_lanes(run: &Run, width: usize) -> Vec<Cow<'_, [i64]>> {
    let CValue::Prod(lanes) = &run_fields(run)[1] else { panic!("half_join: a time as data that is not a tuple") };
    assert_eq!(lanes.len(), width, "half_join: a time as data of the wrong width");
    lanes.iter().map(|lane| lane.as_i64("half_join time lane").unwrap()).collect()
}

/// Where each run's next block ends, and the identifier the block after it starts at: below the
/// least identifier `budget` rows into a run with more than that left, past that identifier's
/// whole run of rows, so that no identifier is split between blocks. Every run with no more than
/// `budget` rows left runs to its end; no horizon means every run does.
fn block_ends(runs: &[Run], starts: &[usize], budget: usize) -> (Vec<usize>, Option<i64>) {
    let mut horizon: Option<i64> = None;
    for (run, &s) in runs.iter().zip(starts) {
        let ids = ident(run.keys());
        if ids.len() - s <= budget { continue; }
        // `None` on overflow means nothing can exceed it: run to the end.
        let Some(bound) = ids[s + budget].checked_add(1) else { continue };
        horizon = Some(horizon.map_or(bound, |h| h.min(bound)));
    }
    let ends = runs.iter().map(|run| match horizon {
        Some(h) => ident(run.keys()).partition_point(|&x| x < h),
        None => run.len(),
    }).collect();
    (ends, horizon)
}

impl ProxyHalfJoinBackend<Time, NativeBatch, Run> for CorgiHalfJoinBackend {
    type Key = i64;
    type V0 = u64;
    type V1 = u64;
    type R0 = Diff;
    type R1 = Diff;
    type ROut = Diff;
    type Output = CC;
    type Stream = Vec<Run>;

    /// The batcher releases runs already in identifier order.
    fn stage(&mut self, runs: Vec<Run>) -> Vec<Run> {
        debug_assert!(runs.iter().all(|r| ident(r.keys()).windows(2).all(|w| w[0] <= w[1])), "half_join: a run out of identifier order");
        runs
    }

    /// The rows of every run below one identifier horizon, merged by identifier, and the arranged
    /// rows of those identifiers from every chunk, staged by value. Each run and chunk is entered
    /// by a binary search from the block's first identifier, so a unit's position is `from` alone.
    fn advance(
        &mut self,
        instance: &HalfJoinInstance<NativeBatch, Vec<Run>>,
        from: &mut KeyPosition<i64>,
        stream: &mut HalfJoinStream<Time, Diff, i64, u64>,
        arranged: &mut ProxyBridge<Time, Diff, i64, u64>,
    ) {
        let next = match *from {
            KeyPosition::Start => i64::MIN,
            KeyPosition::At(key) => key,
            KeyPosition::End => return,
        };
        self.colliding.clear();
        let runs = &instance.stream;
        let starts: Vec<usize> = runs.iter().map(|run| ident(run.keys()).partition_point(|&x| x < next)).collect();
        let live = runs.iter().zip(&starts).filter(|(r, &s)| s < r.len()).count();
        if live == 0 {
            *from = KeyPosition::End;
            return;
        }
        let (ends, horizon) = block_ends(runs, &starts, (BLOCK / live).max(1));
        *from = horizon.map_or(KeyPosition::End, KeyPosition::At);

        // The block's rows across runs, in identifier order.
        let mut rows: Vec<(i64, u32, u32)> = Vec::new();
        for (r, run) in runs.iter().enumerate() {
            let ids = ident(run.keys());
            rows.extend((starts[r]..ends[r]).map(|u| (ids[u], r as u32, u as u32)));
        }
        let lanes: Vec<Vec<Cow<'_, [i64]>>> = runs.iter().map(|run| data_lanes(run, self.width)).collect();
        let mut push = |(id, r, u): (i64, u32, u32), diff| {
            if diff == 0 { return; }
            let (r, u) = (r as usize, u as usize);
            let lifted = Lifted {
                time: runs[r].times().get(u),
                data: Time::from_coordinates(lanes[r].iter().map(|lane| lane[u] as u64)),
            };
            stream.push(((id, ((r as u64) << COORD_BITS) | u as u64), lifted, diff));
        };
        if self.project_stream {
            // The released runs are only in identifier order. Sort on the projected value,
            // retaining both times and the real key: only rows that agree on all can cancel.
            // A representative coordinate still names the original columns for `cross`.
            let vals = demanded(&self.reads.columns[1], runs.iter().map(|r| &run_fields(r)[0]));
            let hashed = key_is_hashed(runs[0].keys());
            let cmp = |&(id0, r0, u0): &(i64, u32, u32), &(id1, r1, u1): &(i64, u32, u32)| {
                let (r0, u0, r1, u1) = (r0 as usize, u0 as usize, r1 as usize, u1 as usize);
                id0.cmp(&id1)
                    .then_with(|| if hashed { compare_at(runs[r0].keys(), u0, runs[r1].keys(), u1) } else { std::cmp::Ordering::Equal })
                    .then_with(|| if vals.is_empty() { std::cmp::Ordering::Equal } else { compare_at(&vals[r0], u0, &vals[r1], u1) })
                    .then_with(|| runs[r0].times().cmp_cross(u0, runs[r1].times(), u1))
                    .then_with(|| lanes[r0].iter().map(|l| l[u0]).cmp(lanes[r1].iter().map(|l| l[u1])))
            };
            rows.sort_unstable_by(cmp);
            for group in rows.chunk_by(|a, b| cmp(a, b).is_eq()) {
                let diff = group.iter().map(|&(_, r, u)| runs[r as usize].diffs()[u as usize]).sum();
                push(group[0], diff);
            }
        } else {
            rows.sort_unstable();
            for &row in &rows { push(row, runs[row.1 as usize].diffs()[row.2 as usize]); }
        }
        if stream.is_empty() { return; }

        // The arranged rows of the block's identifiers: one batched probe of each chunk that
        // can hold any of them.
        let mut needles: Vec<i64> = rows.iter().map(|row| row.0).collect();
        needles.dedup();
        let (first, last) = (needles[0], needles[needles.len() - 1]);
        let chunks = arranged_chunks(&instance.batches);
        // An empty arrangement meets nothing; the stream rows are presented and skipped.
        if chunks.is_empty() { return; }
        // Values equal on the read fields share a token before matching.
        let proj = SideProj::new(&chunks, &self.reads.columns[2]);
        let probes: Vec<Probe<'_, Time>> = chunks.iter().enumerate()
            .filter(|(_, c)| { let ids = ident(c.keys()); ids[0] <= last && ids[ids.len() - 1] >= first })
            .map(|(cid, c)| Probe::new(c, cid, &needles, &proj))
            .collect();
        let matched: Vec<bool> = (0..needles.len()).map(|n| probes.iter().any(|p| p.lo[n] < p.hi[n])).collect();

        // Under a hashed identifier, which of the block's matched identifiers hold more than one
        // real key, found by comparing run endpoints against a representative. Arranged runs are
        // sub-sorted by real key; a released run is in identifier order only, so its rows are
        // offered one by one.
        let hashed = key_is_hashed(runs[rows[0].1 as usize].keys());
        let collisions = if hashed {
            let columns: Vec<&CValue> = runs.iter().map(|run| recover_key_ref(run.keys()))
                .chain(probes.iter().map(|p| recover_key_ref(p.chunk.keys()))).collect();
            let mut cursor = 0usize;
            let released = rows.iter().filter_map(|&(id, r, u)| {
                while needles[cursor] < id { cursor += 1; }
                matched[cursor].then_some((cursor, r as usize, u as usize, u as usize + 1))
            });
            let offset = runs.len();
            let probed = probes.iter().enumerate().flat_map(|(pi, p)| {
                p.lo.iter().zip(&p.hi).enumerate().filter(|(_, (lo, hi))| lo < hi)
                    .map(move |(n, (&s, &e))| (n, offset + pi, s, e))
            });
            colliding_keys(needles.len(), &columns, released.chain(probed))
        } else { Vec::new() };

        // Stage each identifier's arranged rows as the join stages a side: merged by value across
        // chunks, consolidated, equal values under one token. Nothing is advanced: the bound the
        // join's staging joins times with is the minimum here, which leaves them as stored.
        let mut scratch = SideScratch::new();
        let mut refs: Vec<RunRef<'_, Time>> = Vec::new();
        for (n, &id) in needles.iter().enumerate() {
            if !matched[n] { continue; }
            refs.clear();
            refs.extend(probes.iter().filter_map(|p| p.run_ref(n)));
            if hashed && collisions[n] {
                self.colliding.push(id);
                stage_collision(&refs, &Time::minimum(), id, arranged);
            } else {
                scratch.stage_runs(&refs, &Time::minimum());
                scratch.emit(id, arranged);
            }
        }
    }

    /// The matched pairs' keys, values and arranged values gathered into columns and projected;
    /// pairs under a colliding identifier whose real keys differ are dropped first. The data times
    /// are the tactic's rule, evaluated lane by lane: the presented times of the block's rows are
    /// read into lanes once, and each match takes the lane-wise max.
    fn cross(
        &mut self,
        instance: &HalfJoinInstance<NativeBatch, Vec<Run>>,
        stream: &HalfJoinStream<Time, Diff, i64, u64>,
        arranged: &ProxyBridge<Time, Diff, i64, u64>,
        matches: &mut HalfJoinMatches<Diff>,
        output: &mut Vec<CC>,
    ) {
        let runs = &instance.stream;
        let chunks = arranged_chunks(&instance.batches);
        let primitive = primitive_values(chunks[0]);
        let n = matches.pairs.len();
        let mask = (1u64 << COORD_BITS) - 1;
        let mut rs: Vec<usize> = Vec::with_capacity(n);
        let mut us: Vec<usize> = Vec::with_capacity(n);
        // Arranged tokens: the value itself when primitive, else `(chunk, row)` coordinates.
        let mut tokens: Vec<i64> = Vec::new();
        let mut cs: Vec<usize> = Vec::new();
        let mut js: Vec<usize> = Vec::new();
        for &(a, b) in matches.pairs.iter() {
            let (c0, c1) = (stream[a].0.1, arranged[b].0.1);
            rs.push((c0 >> COORD_BITS) as usize);
            us.push((c0 & mask) as usize);
            if primitive { tokens.push(c1 as i64); } else {
                cs.push((c1 >> COORD_BITS) as usize);
                js.push((c1 & mask) as usize);
            }
        }
        let keys: Vec<&CValue> = runs.iter().map(|run| recover_key_ref(run.keys())).collect();

        // Only pairs under a colliding identifier compare real keys, one batched comparison per
        // run and chunk. Colliding identifiers are hashed, so their tokens are coordinates.
        let kept: Option<Vec<usize>> = if self.colliding.is_empty() { None } else {
            let mut suspect: Vec<usize> = (0..n).filter(|&i| self.colliding.binary_search(&stream[matches.pairs[i].0].0.0).is_ok()).collect();
            suspect.sort_unstable_by_key(|&i| (rs[i], cs[i]));
            let mut keep = vec![true; n];
            let (mut left, mut right) = (Vec::new(), Vec::new());
            for group in suspect.chunk_by(|&a, &b| (rs[a], cs[a]) == (rs[b], cs[b])) {
                left.clear(); right.clear();
                left.extend(group.iter().map(|&i| us[i]));
                right.extend(group.iter().map(|&i| js[i]));
                let (r, c) = (rs[group[0]], cs[group[0]]);
                let signs = equal_idx(keys[r], recover_key_ref(chunks[c].keys()), &left, &right);
                for (&i, sign) in group.iter().zip(signs) { keep[i] = sign == 0; }
            }
            let kept: Vec<usize> = (0..n).filter(|&i| keep[i]).collect();
            if kept.len() == n { None } else { Some(kept) }
        };
        if let Some(kept) = &kept {
            if kept.is_empty() { return; }
            rs = kept.iter().map(|&i| rs[i]).collect();
            us = kept.iter().map(|&i| us[i]).collect();
            if primitive { tokens = kept.iter().map(|&i| tokens[i]).collect(); } else {
                cs = kept.iter().map(|&i| cs[i]).collect();
                js = kept.iter().map(|&i| js[i]).collect();
            }
        }
        let pair = |i: usize| matches.pairs[kept.as_ref().map_or(i, |kept| kept[i])];

        // The projection over (key, val, arranged val), each projected to what it reads before
        // gathering; the collision checks above compared the original keys. The time as data
        // (the value's second field) is not the projection's to read: `advance` presented it.
        let rows = rs.len();
        let [dk, d0, d1] = &self.reads.columns;
        let fields: Vec<&[CValue]> = runs.iter().map(run_fields).collect();
        let gathered = |cols: &[Cow<'_, CValue>], rs: &[usize], us: &[usize]| {
            gather_lanes(&cols.iter().map(|c| Some(&**c)).collect::<Vec<_>>(), rs, us)
        };
        let out_keys = if *dk == Demand::None { CValue::Unit(rows) } else { gathered(&demanded(dk, keys.iter().copied()), &rs, &us) };
        let vals = if *d0 == Demand::None { CValue::Unit(rows) } else { gathered(&demanded(d0, fields.iter().map(|f| &f[0])), &rs, &us) };
        let arranged_vals = if *d1 == Demand::None { CValue::Unit(rows) } else if primitive {
            d1.project(rewrap(chunks[0].vals(), tokens)).expect("the arranged value's demand")
        } else { gathered(&demanded(d1, chunks.iter().map(|c| c.vals())), &cs, &js) };
        let argument = self.reads.argument.project(CValue::Prod(vec![out_keys, vals, arranged_vals])).expect("the half join's input has the shape its demand is on");
        let shape = shape_of_value(&argument);
        assert!(shape == self.reads.shape, "half_join: a block read as {shape} reached a projection narrowed to {}", self.reads.shape);
        let mut out = corgi::eval_graph(&self.graph, argument).into_prod("half_join projection").unwrap();
        let out_val = out.pop().unwrap();
        let out_key = out.pop().unwrap();

        // The presented times as lanes, once per block: a time is as wide as its last nonzero
        // coordinate, and the rest are zero.
        let width = self.width;
        let to_lanes = |times: &mut dyn Iterator<Item = &Time>, rows: usize| -> Vec<Vec<i64>> {
            let mut lanes = vec![vec![0i64; rows]; width];
            for (i, t) in times.enumerate() {
                for (l, c) in t.coordinates().enumerate() {
                    assert!(l < width, "half_join: a time wider than its scope");
                    lanes[l][i] = c as i64;
                }
            }
            lanes
        };
        let data = to_lanes(&mut stream.iter().map(|row| &row.1.data), stream.len());
        let time2 = to_lanes(&mut arranged.iter().map(|row| &row.1), arranged.len());
        // The data time of each match: the lane-wise max is the join in a product of chains.
        let joined: Vec<CValue> = (0..width).map(|l| {
            CValue::i64((0..rows).map(|i| { let (a, b) = pair(i); data[l][a].max(time2[l][b]) }).collect())
        }).collect();
        debug_assert!((0..rows).all(|i| {
            let (a, b) = pair(i);
            let t = data_time(&stream[a].1, &arranged[b].1);
            let coords: Vec<i64> = t.coordinates().map(|c| c as i64).collect();
            coords.iter().enumerate().all(|(l, &c)| joined[l].as_i64("lane").unwrap()[i] == c)
        }), "half_join: the lane-wise data time must agree with the tactic's rule");
        // Each row keeps its update's own time, which the unit's capabilities cover: the driver
        // ships the block stamped with all of them.
        let mut times = ColTimes::new();
        for (&r, &u) in rs.iter().zip(&us) { times.push_ref(runs[r].times(), u); }
        let diffs = match &kept {
            None => std::mem::take(&mut matches.diffs),
            Some(kept) => kept.iter().map(|&i| matches.diffs[i]).collect(),
        };
        output.push(CorgiContainer {
            keys: out_key,
            vals: CValue::Prod(vec![out_val, CValue::Prod(joined.into_iter().collect())]),
            times,
            diffs,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use timely::progress::Antichain;
    use differential_dogs3::operators::half_join::HalfJoinTactic;
    use differential_dogs3::operators::half_join::proxy::ProxyHalfJoinTactic;
    use crate::runtime::half_join::test_util::Rng;

    use super::*;
    use super::super::test_util::*;
    use crate::runtime::chunk::{columns_to_batch, present_key};

    /// The projection `(k, (v0, v1))`.
    fn pairing() -> Lambda { Lambda::new("let (k, v0, v1) = input in (k, (v0, v1))".into(), &corgi::Scope::default()) }

    /// A tactic over the backend at these shapes.
    fn tactic(strict: bool, shapes: [Shape; 3]) -> ProxyHalfJoinTactic<NativeBatch, Run, CorgiHalfJoinBackend> {
        ProxyHalfJoinTactic::new(CorgiHalfJoinBackend::new(&pairing(), 2, shapes), strict)
    }

    /// Projection cancels unread churn, but neither of a lifted update's times may be merged.
    #[test]
    fn projected_churn_keeps_both_stream_times() {
        for change_data in [false, true] {
            let arranged = Rc::new(columns_to_batch(
                present_key(ints(&[1, 1])), CValue::Prod(vec![ints(&[8, 8]), ints(&[20, 21])]),
                [t(0, 0), t(0, 0)].into_iter().collect(), vec![1, -1],
            ));
            let run = Run::new(ints(&[1; 4]), CValue::Prod(vec![
                CValue::Prod(vec![ints(&[7; 4]), ints(&[10, 11, 12, 13])]),
                lifted(&[(3, 0), (3, 0), (4, 0), if change_data { (5, 0) } else { (4, 0) }]),
            ]), [t(2, 0), t(2, 0), t(2, 0), if change_data { t(2, 0) } else { t(3, 0) }].into_iter().collect(), vec![1, -1, 1, -1]);
            let pair = Shape::Prod(vec![Shape::Int, Shape::Int]);
            let lambda = Lambda::new("let (k, (a, _), (b, _)) = input in (k, (a, b))".into(), &corgi::Scope::default());
            let mut backend = CorgiHalfJoinBackend::new(&lambda, 2, [Shape::Int, pair.clone(), pair]);
            let instance = HalfJoinInstance { stream: vec![run], batches: vec![arranged] };
            let (mut stream, mut arranged) = (Vec::new(), Vec::new());
            backend.advance(&instance, &mut KeyPosition::Start, &mut stream, &mut arranged);
            assert_eq!(stream.len(), 2);
            assert!(arranged.is_empty());
        }
    }

    /// An update meets the arranged rows before it in the total order on times, strictly or not:
    /// never a later one, even one beyond it in a later coordinate alone. Its data time joins the
    /// arranged row's, coordinate by coordinate, and every output row is covered by `lower`.
    #[test]
    fn meets_what_precedes_in_the_total_order() {
        let arranged = Rc::new(columns_to_batch(
            present_key(ints(&[1, 1, 1, 1, 1])),
            ints(&[50, 100, 200, 300, 400]),
            [t(0, 1), t(0, 5), t(1, 0), t(2, 0), t(1, 2)].into_iter().collect(),
            vec![1, 1, 1, 1, 1],
        ));
        // Updates of key 1 at (1, [0]) and at (0, [3]), each with its own time as data.
        let updates = container(&[1, 1], &[7, 8], &[(1, 0), (0, 3)], &[t(1, 0), t(0, 3)], &[2, 1]);
        let updates = Run::new(updates.keys, updates.vals, updates.times, updates.diffs);
        let lower: Antichain<Time> = [t(0, 3), t(1, 0)].into_iter().collect();
        let rows = |strict: bool| -> Vec<((String, String), Time, Diff)> {
            let mut tactic = tactic(strict, [Shape::Int, Shape::Int, Shape::Int]);
            let mut rows: Vec<_> = tactic.prep(vec![updates.clone()], vec![Rc::clone(&arranged)], lower.clone())
                .flat_map(|c| c.into_updates())
                .collect();
            assert!(rows.iter().all(|row| lower.less_equal(&row.1)), "every output row is covered by lower");
            rows.sort();
            rows
        };
        let row = |x: i64, y: i64, data: (i64, i64), time: Time, diff: Diff| {
            ((int(1), tup(vec![tup(vec![int(x), int(y)]), tup(vec![int(data.0), int(data.1)])])), time, diff)
        };
        let mut strict = vec![
            row(7, 50, (1, 1), t(1, 0), 2),
            row(7, 100, (1, 5), t(1, 0), 2),
            row(8, 50, (0, 3), t(0, 3), 1),
        ];
        strict.sort();
        assert_eq!(rows(true), strict);
        // Not strict, the update at (1, [0]) also meets the arranged row at (1, [0]).
        let mut loose = strict.clone();
        loose.push(row(7, 200, (1, 0), t(1, 0), 2));
        loose.sort();
        assert_eq!(rows(false), loose);
    }

    /// A shared hash does not make distinct real keys equal: time filtering precedes the key
    /// checks, and strictness, signed diffs and candidate order survive them.
    #[test]
    fn colliding_keys_are_checked_after_time_filtering() {
        let keys = |ks: &[i64]| CValue::Prod(vec![
            ints(&vec![42; ks.len()]),
            CValue::Prod(vec![ints(ks), ints(&ks.iter().map(|k| k * 7).collect::<Vec<_>>())]),
        ]);
        let arranged_keys: Vec<i64> = (0..4).flat_map(|k| [k; 3]).collect();
        let arranged_vals: Vec<i64> = (0..12).collect();
        let arranged = Rc::new(differential_dataflow::trace::chunk::ChunkBatch::new(vec![CorgiChunk::from_columns(
            keys(&arranged_keys), ints(&arranged_vals),
            (0..4).flat_map(|_| [t(0, 0), t(1, 0), t(2, 0)]).collect(),
            (0..4).flat_map(|_| [1, -1, 2]).collect(),
        )]));
        let update_keys: Vec<i64> = (0..6).collect();
        let updates = Run {
            keys: keys(&update_keys),
            vals: CValue::Prod(vec![ints(&update_keys), lifted(&[(1, 0); 6])]),
            times: (0..6).map(|_| t(1, 0)).collect(),
            diffs: vec![2, -1, 2, -1, 2, -1],
        };
        for strict in [false, true] {
            let pair = Shape::Prod(vec![Shape::Int, Shape::Int]);
            let mut tactic = tactic(strict, [pair, Shape::Int, Shape::Int]);
            let got: Vec<_> = tactic.prep(vec![updates.clone()], vec![Rc::clone(&arranged)], Antichain::from_elem(t(1, 0)))
                .flat_map(|c| c.into_updates()).collect();
            let mut expected = Vec::new();
            for k in 0..4 {
                for epoch in 0..if strict { 1 } else { 2 } {
                    expected.push((
                        (tup(vec![int(k), int(k * 7)]),
                         tup(vec![tup(vec![int(k), int(k * 3 + epoch)]), tup(vec![int(1), int(0)])])),
                        t(1, 0), if k % 2 == 0 { 2 } else { -1 } * if epoch == 0 { 1 } else { -1 },
                    ));
                }
            }
            assert_eq!(consolidated(got), consolidated(expected), "strict {strict}");
        }
    }

    /// Among hashed identifiers, only those holding several real keys are recorded, and only
    /// pairs under them have their keys compared: the recorded set is exactly the colliding
    /// identifiers, and the output is the definition's whether an identifier collides or not.
    #[test]
    fn only_colliding_identifiers_are_recorded_and_checked() {
        // Real keys 0 and 1 share identifier 0; every other key is its own identifier.
        let hash = |k: i64| if k < 2 { 0 } else { k };
        let keys = |ks: &[i64]| CValue::Prod(vec![
            ints(&ks.iter().map(|&k| hash(k)).collect::<Vec<_>>()),
            CValue::Prod(vec![ints(ks), ints(&ks.iter().map(|k| k * 7).collect::<Vec<_>>())]),
        ]);
        let arranged_keys: Vec<i64> = (0..4).flat_map(|k| [k; 2]).collect();
        let arranged_vals: Vec<i64> = (0..8).collect();
        let arranged: Vec<NativeBatch> = (0..2).map(|half| Rc::new(differential_dataflow::trace::chunk::ChunkBatch::new(vec![CorgiChunk::from_columns(
            keys(&arranged_keys), ints(&arranged_vals.iter().map(|v| v + 100 * half).collect::<Vec<_>>()),
            (0..8).map(|_| t(0, 0)).collect(), vec![1; 8],
        )]))).collect();
        let update_keys: Vec<i64> = (0..5).collect();
        let updates = Run {
            keys: keys(&update_keys),
            vals: CValue::Prod(vec![ints(&update_keys), lifted(&[(1, 0); 5])]),
            times: (0..5).map(|_| t(1, 0)).collect(),
            diffs: vec![1; 5],
        };
        let pair = Shape::Prod(vec![Shape::Int, Shape::Int]);
        let mut backend = CorgiHalfJoinBackend::new(&pairing(), 2, [pair.clone(), Shape::Int, Shape::Int]);
        let instance = HalfJoinInstance { stream: backend.stage(vec![updates.clone()]), batches: arranged.clone() };
        let (mut from, mut stream, mut arr) = (KeyPosition::Start, Vec::new(), Vec::new());
        backend.advance(&instance, &mut from, &mut stream, &mut arr);
        assert_eq!(backend.colliding, vec![0], "the one identifier holding two keys");
        assert!(arr.windows(2).all(|w| (w[0].0, &w[0].1) < (w[1].0, &w[1].1)), "the arranged side is a consolidated bridge");

        let mut tactic = tactic(true, [pair, Shape::Int, Shape::Int]);
        let got: Vec<_> = tactic.prep(vec![updates], arranged, Antichain::from_elem(t(1, 0)))
            .flat_map(|c| c.into_updates()).collect();
        let mut expected = Vec::new();
        for k in 0..4i64 {
            for v in [2 * k, 2 * k + 1] {
                for half in 0..2 {
                    expected.push(((tup(vec![int(k), int(k * 7)]), tup(vec![tup(vec![int(k), int(v + 100 * half)]), tup(vec![int(1), int(0)])])), t(1, 0), 1));
                }
            }
        }
        assert_eq!(consolidated(got), consolidated(expected));
    }

    /// Against the definition, over several arranged chunks and released updates in several runs
    /// spanning many blocks, with keys that are integers and keys that are hashed: each update
    /// meets exactly the arranged rows of its key that precede it, and is covered by `lower`.
    #[test]
    fn meets_what_the_definition_meets() {
        for hashed in [false, true] {
            for strict in [false, true] {
                for seed in 0..40 {
                    let mut rng = Rng(seed);
                    let key_col = |ks: &[i64]| if hashed { CValue::Prod(vec![ints(ks), ints(&ks.iter().map(|k| k * 7).collect::<Vec<_>>())]) } else { ints(ks) };
                    let key_val = |k: i64| if hashed { tup(vec![int(k), int(k * 7)]) } else { int(k) };
                    let mut arranged_rows = Vec::new();
                    let batches: Vec<NativeBatch> = (0..3).map(|_| {
                        let n = 1 + rng.below(20) as usize;
                        let ks: Vec<i64> = (0..n).map(|_| rng.below(6) as i64).collect();
                        let vs: Vec<i64> = (0..n).map(|_| rng.below(100) as i64).collect();
                        let ts: Vec<Time> = (0..n).map(|_| t(rng.below(4), rng.below(3))).collect();
                        let ds: Vec<Diff> = (0..n).map(|_| rng.diff()).collect();
                        for i in 0..n { arranged_rows.push((ks[i], vs[i], ts[i].clone(), ds[i])); }
                        Rc::new(columns_to_batch(present_key(key_col(&ks)), ints(&vs), ts.into_iter().collect(), ds))
                    }).collect();
                    let n = 1 + rng.below(40) as usize;
                    let ks: Vec<i64> = (0..n).map(|_| rng.below(8) as i64).collect();
                    let vs: Vec<i64> = (0..n).map(|_| rng.below(10) as i64).collect();
                    let ss: Vec<(i64, i64)> = (0..n).map(|_| (rng.below(4) as i64, rng.below(3) as i64)).collect();
                    let ts: Vec<Time> = (0..n).map(|_| t(rng.below(4), rng.below(3))).collect();
                    // A time as data is at or beyond the update's own time.
                    let ss: Vec<(i64, i64)> = ss.iter().zip(&ts).map(|(s, t)| (s.0.max(t.first().copied().unwrap_or(0) as i64), s.1.max(t.get(1).copied().unwrap_or(0) as i64))).collect();
                    let ds: Vec<Diff> = (0..n).map(|_| rng.diff()).collect();
                    // The updates as several runs, of random lengths, as one extraction releases them.
                    let mut runs = Vec::new();
                    let mut lo = 0;
                    while lo < n {
                        let hi = (lo + 1 + rng.below(6) as usize).min(n);
                        runs.push(Run::new(key_col(&ks[lo..hi]), CValue::Prod(vec![ints(&vs[lo..hi]), lifted(&ss[lo..hi])]), ts[lo..hi].iter().cloned().collect(), ds[lo..hi].to_vec()));
                        lo = hi;
                    }
                    let lower: Antichain<Time> = ts.iter().cloned().collect();

                    let mut expect = Vec::new();
                    for i in 0..n {
                        for (k2, v1, t2, d2) in arranged_rows.iter() {
                            if *k2 != ks[i] || !(*t2 < ts[i] || (!strict && *t2 == ts[i])) { continue; }
                            let s = tup(vec![int(ss[i].0.max(t2.first().copied().unwrap_or(0) as i64)), int(ss[i].1.max(t2.get(1).copied().unwrap_or(0) as i64))]);
                            expect.push(((key_val(ks[i]), tup(vec![tup(vec![int(vs[i]), int(*v1)]), s])), ts[i].clone(), ds[i] * d2));
                        }
                    }
                    let key = if hashed { Shape::Prod(vec![Shape::Int, Shape::Int]) } else { Shape::Int };
                    let mut tactic = tactic(strict, [key, Shape::Int, Shape::Int]);
                    let mut got = Vec::new();
                    for c in tactic.prep(runs, batches, lower.clone()) {
                        for row in c.into_updates() {
                            assert!(lower.less_equal(&row.1), "every output row is covered by lower");
                            got.push(row);
                        }
                    }
                    assert_eq!(consolidated(got), consolidated(expect), "seed {seed}, strict {strict}, hashed {hashed}");
                }
            }
        }
    }
    /// Narrowed to what its projection reads, a half join gives the unnarrowed half join's rows:
    /// over wide update and arranged values, keys read or not, integer and hashed keys, and a
    /// projection that reads nothing.
    #[test]
    fn narrowed_projection_matches_the_unnarrowed_half_join() {
        for hashed in [false, true] {
            let key_col = |ks: &[i64]| if hashed { CValue::Prod(vec![ints(ks), ints(&ks.iter().map(|k| k * 7).collect::<Vec<_>>())]) } else { ints(ks) };
            let wide = |xs: &[i64]| CValue::Prod((0..4).map(|f| ints(&xs.iter().map(|x| 10 * f + x).collect::<Vec<_>>())).collect());
            let mut rng = Rng(7);
            let batches: Vec<NativeBatch> = (0..2).map(|_| {
                let n = 1 + rng.below(20) as usize;
                let ks: Vec<i64> = (0..n).map(|_| rng.below(6) as i64).collect();
                let vs: Vec<i64> = (0..n).map(|_| rng.below(100) as i64).collect();
                let ts: Vec<Time> = (0..n).map(|_| t(rng.below(3), rng.below(3))).collect();
                let ds: Vec<Diff> = (0..n).map(|_| rng.diff()).collect();
                Rc::new(columns_to_batch(present_key(key_col(&ks)), wide(&vs), ts.into_iter().collect(), ds))
            }).collect();
            let n = 30;
            let ks: Vec<i64> = (0..n).map(|_| rng.below(8) as i64).collect();
            let vs: Vec<i64> = (0..n).map(|_| rng.below(10) as i64).collect();
            let ss: Vec<(i64, i64)> = (0..n).map(|_| (2 + rng.below(2) as i64, 2 + rng.below(2) as i64)).collect();
            let ts: Vec<Time> = (0..n).map(|_| t(2, rng.below(3))).collect();
            let ds: Vec<Diff> = (0..n).map(|_| rng.diff()).collect();
            let run = Run::new(key_col(&ks), CValue::Prod(vec![wide(&vs), lifted(&ss)]), ts.iter().cloned().collect(), ds);
            let lower: Antichain<Time> = ts.iter().cloned().collect();
            let key = if hashed { Shape::Prod(vec![Shape::Int, Shape::Int]) } else { Shape::Int };
            let shapes = [key, Shape::Prod(vec![Shape::Int; 4]), Shape::Prod(vec![Shape::Int; 4])];
            let bodies = ["(k, (a, e))", "((), (d, h))", "(k, ())", "((), ())", "(k, (v0, v1))", if hashed { "let (x, _) = k in (x, g)" } else { "(k, (b, f))" }];
            for body in bodies {
                let lambda = Lambda::new(format!("let (k, v0, v1) = input in let (a, b, c, d) = v0 in let (e, f, g, h) = v1 in {body}"), &corgi::Scope::default());
                let narrowed = CorgiHalfJoinBackend::new(&lambda, 2, shapes.clone());
                let original = CorgiHalfJoinBackend {
                    width: 2, project_stream: false, graph: compile_projection(&lambda, &shapes).unwrap(),
                    reads: Reads::new(Demand::All, &Shape::Prod(shapes.to_vec())), colliding: Vec::new(),
                };
                let rows = |backend: CorgiHalfJoinBackend| {
                    let mut tactic = ProxyHalfJoinTactic::new(backend, true);
                    consolidated(tactic.prep(vec![run.clone()], batches.clone(), lower.clone()).flat_map(|c| c.into_updates()).collect())
                };
                let got = rows(narrowed);
                assert!(!got.is_empty(), "the case meets something");
                assert_eq!(got, rows(original), "hashed {hashed}, body {body}");
            }
        }
    }
}
