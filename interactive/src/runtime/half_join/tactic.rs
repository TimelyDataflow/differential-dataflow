//! The half join's tactic: a unit's runs, merged by identifier a block at a time, meet the
//! arrangement's chunks, each read forward from where the last block left it.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::rc::Rc;

use timely::progress::Antichain;

use differential_dataflow::trace::chunk::Chunk;
use differential_dogs3::operators::half_join::HalfJoinTactic;

use corgi::arrange::{equal_idx, gather_lanes};
use corgi::{Graph, NumOp, Shape, Value as CValue};

use crate::runtime::chunk::{ident, key_is_hashed, recover_key_ref, CorgiChunk};
use crate::runtime::col_times::ColTimes;
use crate::runtime::container::CorgiContainer;
use crate::lambda::compile_projection;
use crate::runtime::search::matching_ranges;
use crate::ir::{Diff, Lambda, Time};

use super::{NativeBatch, Run, CC};

/// Released updates are met with the arrangement this many at a time, so that the driver can yield
/// between blocks. Small under test, so that tests span many blocks.
const BLOCK: usize = if cfg!(test) { 3 } else { 1 << 14 };

/// Meets released updates with an arrangement's chunks. The projection is compiled once, at the
/// shapes it reads, and shared by every unit of work.
pub(crate) struct CorgiHalfJoin {
    strict: bool,
    /// The number of lanes in a time as data: the scope depth, plus one.
    width: usize,
    /// The projection, at the shapes of the key, the update's value, and the arranged value.
    projection: Rc<([Shape; 3], Graph<NumOp>)>,
}

impl CorgiHalfJoin {
    /// The projection over a key, an update's value, and an arranged value of these shapes. A
    /// type error is a panic with corgi's message: install type-checked the program, so it is not
    /// reached.
    pub(crate) fn new(projection: &Lambda, strict: bool, width: usize, [k, v0, v1]: [Shape; 3]) -> Self {
        let g = compile_projection(projection, &[k.clone(), v0.clone(), v1.clone()])
            .unwrap_or_else(|e| panic!("half_join: type error at shapes ({k}, {v0}, {v1}): {e}"));
        CorgiHalfJoin { strict, width, projection: Rc::new(([k, v0, v1], g)) }
    }
}

impl HalfJoinTactic<Time, NativeBatch, Run, CC> for CorgiHalfJoin {
    fn prep(&mut self, runs: Vec<Run>, batches: Vec<NativeBatch>, _lower: Antichain<Time>) -> Box<dyn Iterator<Item = CC>> {
        let arranged: Vec<CorgiChunk<Time, Diff>> = batches.iter().flat_map(|b| b.chunks.iter().filter(|c| c.len() > 0).cloned()).collect();
        debug_assert!(runs.iter().all(|r| ident(r.keys()).windows(2).all(|w| w[0] <= w[1])), "half_join: a run out of identifier order");
        Box::new(Work {
            starts: vec![0; runs.len()],
            runs,
            seek: vec![0; arranged.len()],
            arranged,
            strict: self.strict,
            width: self.width,
            projection: Rc::clone(&self.projection),
            ready: VecDeque::new(),
        })
    }
}

/// One unit of half-join work: released runs, merged and met a block at a time.
///
/// Runs and arranged chunks are all in identifier order. Each block takes the rows of every run
/// below one identifier horizon, so the runs are read front to back, and the blocks meet each
/// arranged chunk at positions that only move forward: `seek` records where each arranged chunk's
/// next search starts, and each is walked at most once per unit.
struct Work {
    runs: Vec<Run>,
    /// For each run, the first row not yet met.
    starts: Vec<usize>,
    arranged: Vec<CorgiChunk<Time, Diff>>,
    /// For each arranged chunk, the first row later blocks could meet.
    seek: Vec<usize>,
    strict: bool,
    width: usize,
    projection: Rc<([Shape; 3], Graph<NumOp>)>,
    /// Output met but not yet handed over.
    ready: VecDeque<CC>,
}

impl Iterator for Work {
    type Item = CC;

    fn next(&mut self) -> Option<CC> {
        while self.ready.is_empty() {
            let live = self.runs.iter().zip(&self.starts).filter(|(r, &s)| s < r.len()).count();
            if live == 0 { return None; }
            let ends = self.block_ends((BLOCK / live).max(1));
            self.meet(&ends);
            self.starts = ends;
        }
        self.ready.pop_front()
    }
}

impl Work {
    /// Where each run's next block ends: below the least identifier `budget` rows into a run with
    /// more than that left, past that identifier's whole run of rows, so that no identifier is
    /// split between blocks. Every run with no more than `budget` rows left runs to its end.
    fn block_ends(&self, budget: usize) -> Vec<usize> {
        let mut horizon: Option<i64> = None;
        for (run, &s) in self.runs.iter().zip(&self.starts) {
            let ids = ident(run.keys());
            if ids.len() - s <= budget { continue; }
            // `None` on overflow means nothing can exceed it: run to the end.
            let Some(bound) = ids[s + budget].checked_add(1) else { continue };
            horizon = Some(horizon.map_or(bound, |h| h.min(bound)));
        }
        self.runs.iter().map(|run| match horizon {
            Some(h) => ident(run.keys()).partition_point(|&x| x < h),
            None => run.len(),
        }).collect()
    }

    /// Meet each run's rows from its start to `ends` with the arrangement, queueing the output.
    fn meet(&mut self, ends: &[usize]) {
        // Each run's rows from its start to `ends` are already in identifier order, so each run is
        // met on its own, with its distinct identifiers as the needles; the block is never sorted.
        let live = || (0..self.runs.len()).filter(|&r| self.starts[r] < ends[r]);
        let Some(r0) = live().next() else { return };
        let last = live().map(|r| ident(self.runs[r].keys())[ends[r] - 1]).max().unwrap();
        let hashed = key_is_hashed(self.runs[r0].keys());
        let keys: Vec<&CValue> = self.runs.iter().map(|run| recover_key_ref(run.keys())).collect();

        // The pairs that meet: (run, update row, arranged chunk, arranged row).
        let mut pairs = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let (mut needles, mut groups) = (Vec::new(), Vec::new());
        let (mut scratch, mut found) = (Vec::new(), Vec::new());
        for (r, run) in self.runs.iter().enumerate() {
            let (lo, hi) = (self.starts[r], ends[r]);
            if lo == hi { continue; }
            let ids = ident(run.keys());
            needles.clear();
            groups.clear();
            for u in lo..hi {
                if needles.last() != Some(&ids[u]) {
                    needles.push(ids[u]);
                    groups.push(u..u);
                }
                groups.last_mut().unwrap().end = u + 1;
            }
            let (first, run_last) = (needles[0], needles[needles.len() - 1]);
            for (cid, chunk) in self.arranged.iter().enumerate() {
                let base = self.seek[cid];
                let hay = &ident(chunk.keys())[base..];
                if hay.first().is_none_or(|&x| x > run_last) || hay.last().is_some_and(|&x| x < first) { continue; }
                matching_ranges(&needles, hay, &mut scratch, &mut found);
                let chunk_key = recover_key_ref(chunk.keys());
                // A hashed identifier can hold several real keys, so this run's and chunk's
                // time-eligible pairs have their keys compared, in batches of at most `BLOCK`.
                let mut unchecked = pairs.1.len();
                for (needle, range) in found.iter() {
                    for u in groups[*needle].clone() {
                        for j in range.start + base..range.end + base {
                            let precedes = match chunk.times().cmp_cross(j, run.times(), u) {
                                Ordering::Less => true,
                                Ordering::Equal => !self.strict,
                                Ordering::Greater => false,
                            };
                            if precedes {
                                pairs.0.push(r);
                                pairs.1.push(u);
                                pairs.2.push(cid);
                                pairs.3.push(j);
                                if hashed && pairs.1.len() - unchecked == BLOCK {
                                    keep_equal_keys(keys[r], chunk_key, unchecked, &mut pairs);
                                    unchecked = pairs.1.len();
                                }
                            }
                        }
                    }
                }
                if hashed && pairs.1.len() > unchecked { keep_equal_keys(keys[r], chunk_key, unchecked, &mut pairs); }
            }
        }
        // Later blocks hold identifiers beyond `last`, so they need nothing before it.
        for (cid, chunk) in self.arranged.iter().enumerate() {
            self.seek[cid] += ident(chunk.keys())[self.seek[cid]..].partition_point(|&x| x < last);
        }
        let (rs, us, cs, js) = pairs;
        if us.is_empty() { return; }

        // The projection over (key, val, arranged val).
        let fields: Vec<&[CValue]> = self.runs.iter().map(|run| match run.vals() {
            CValue::Prod(fields) => &fields[..],
            _ => panic!("half_join: a stream value that is not (val, time)"),
        }).collect();
        let keys = gather_lanes(&keys.iter().map(|&k| Some(k)).collect::<Vec<_>>(), &rs, &us);
        let vals = gather_lanes(&fields.iter().map(|f| Some(&f[0])).collect::<Vec<_>>(), &rs, &us);
        let sources: Vec<Option<&CValue>> = self.arranged.iter().map(|c| Some(c.vals())).collect();
        let arranged_vals = gather_lanes(&sources, &cs, &js);
        let shapes = [corgi::shape_of_value(&keys), corgi::shape_of_value(&vals), corgi::shape_of_value(&arranged_vals)];
        let (pinned, graph) = &*self.projection;
        assert!(*pinned == shapes, "half_join: a block of shape {shapes:?} reached a projection compiled at {pinned:?}");
        let mut out = corgi::eval_graph(graph, CValue::Prod(vec![keys, vals, arranged_vals])).into_prod("half_join projection").unwrap();
        let out_val = out.pop().unwrap();
        let out_key = out.pop().unwrap();

        // The update's time as data, joined lane by lane with the arranged row's time.
        let data_times: Vec<Vec<Cow<'_, [i64]>>> = fields.iter().map(|f| {
            let CValue::Prod(lanes) = &f[1] else { panic!("half_join: a time as data that is not a tuple") };
            assert_eq!(lanes.len(), self.width, "half_join: a time as data of the wrong width");
            lanes.iter().map(|lane| lane.as_i64("half_join time lane").unwrap()).collect()
        }).collect();
        let joined: Vec<CValue> = (0..self.width).map(|l| {
            CValue::i64(rs.iter().zip(&us).zip(cs.iter().zip(&js)).map(|((&r, &u), (&c, &j))| {
                let t2 = self.arranged[c].times().raw_lanes().get(l).map_or(0, |lane| lane[j]) as i64;
                data_times[r][l][u].max(t2)
            }).collect())
        }).collect();
        let mut times = ColTimes::new();
        for (&r, &u) in rs.iter().zip(&us) { times.push_ref(self.runs[r].times(), u); }
        // Each row keeps its update's own time, which the unit's capabilities cover: the driver
        // ships the block stamped with all of them.
        self.ready.push_back(CorgiContainer {
            keys: out_key,
            vals: CValue::Prod(vec![out_val, CValue::Prod(joined)]),
            times,
            diffs: rs.iter().zip(&us).zip(cs.iter().zip(&js)).map(|((&r, &u), (&c, &j))| self.runs[r].diffs()[u] * self.arranged[c].diffs()[j]).collect(),
        });
    }
}

/// Keep the pairs from `start` on whose update key, a row of `key`, equals the arranged key, a row
/// of `chunk_key`: one batched comparison for the pairs of one run and one arranged chunk.
fn keep_equal_keys(key: &CValue, chunk_key: &CValue, start: usize, pairs: &mut (Vec<usize>, Vec<usize>, Vec<usize>, Vec<usize>)) {
    let equal = equal_idx(key, chunk_key, &pairs.1[start..], &pairs.3[start..]);
    let mut keep = start;
    for (row, sign) in (start..pairs.1.len()).zip(equal) {
        if sign == 0 {
            pairs.0[keep] = pairs.0[row];
            pairs.1[keep] = pairs.1[row];
            pairs.2[keep] = pairs.2[row];
            pairs.3[keep] = pairs.3[row];
            keep += 1;
        }
    }
    pairs.0.truncate(keep);
    pairs.1.truncate(keep);
    pairs.2.truncate(keep);
    pairs.3.truncate(keep);
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::test_util::*;
    use crate::runtime::chunk::{columns_to_batch, present_key};

    /// The projection `(k, (v0, v1))`.
    fn pairing() -> Lambda { Lambda::new("let (k, v0, v1) = input in (k, (v0, v1))".into(), &corgi::Scope::default()) }

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
            let mut tactic = CorgiHalfJoin::new(&pairing(), strict, 2, [Shape::Int, Shape::Int, Shape::Int]);
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

    /// A shared hash does not make distinct real keys equal. Time filtering precedes the bounded
    /// key checks; strictness, signed diffs and candidate order survive several comparison batches.
    #[test]
    fn colliding_keys_are_checked_in_batches_after_time_filtering() {
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
            let mut tactic = CorgiHalfJoin::new(&pairing(), strict, 2, [pair, Shape::Int, Shape::Int]);
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
                            let s = tup(vec![int(ss[i].0.max(t2.outer as i64)), int(ss[i].1.max(t2.inner.first().copied().unwrap_or(0) as i64))]);
                            expect.push(((key_val(ks[i]), tup(vec![tup(vec![int(vs[i]), int(*v1)]), s])), ts[i].clone(), ds[i] * d2));
                        }
                    }
                    let key = if hashed { Shape::Prod(vec![Shape::Int, Shape::Int]) } else { Shape::Int };
                    let mut tactic = CorgiHalfJoin::new(&pairing(), strict, 2, [key, Shape::Int, Shape::Int]);
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
}
