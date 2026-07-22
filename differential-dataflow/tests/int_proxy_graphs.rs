//! Behavioral tests for the proxy tactics against a reference "graph" backend.
//!
//! The value-token instantiation from the seam design: data is edges `(src, dst)`,
//! grouped by source. `Group = u32` is the source itself (exact — no hashing, no
//! collisions), and the value token is the whole edge (self-redeeming: `cross` and
//! `finish` build outputs straight from tokens without consulting storage). Batches
//! are plain sorted update lists; window sizes are kept tiny to force many windows
//! and exercise the streaming protocols.

use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;

use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::operators::int_proxy::{
    JoinInstance, JoinWindow, ProxyJoinBackend, ProxyJoinTactic, ProxyReduceBackend,
    ProxyReduceTactic, ReduceInstance, ReduceWindow,
};
use differential_dataflow::operators::join::{Fresh, JoinTactic};
use differential_dataflow::operators::reduce::ReduceTactic;
use differential_dataflow::trace::{BatchReader, Description};

type Edge = (u32, u32);
type Time = u64;
type Diff = isize;

/// A batch of edge updates, sorted by `((src, dst), time)` and consolidated.
#[derive(Clone)]
struct GraphBatch {
    updates: Vec<(Edge, Time, Diff)>,
    description: Description<Time>,
}

impl BatchReader for GraphBatch {
    type Time = Time;
    fn len(&self) -> usize { self.updates.len() }
    fn description(&self) -> &Description<Time> { &self.description }
}

/// Sort, consolidate, and wrap updates as a batch describing `[lower, upper)`.
fn batch(mut updates: Vec<(Edge, Time, Diff)>, lower: Time, upper: Time) -> GraphBatch {
    consolidate_updates(&mut updates);
    GraphBatch {
        updates,
        description: Description::new(
            Antichain::from_elem(lower),
            Antichain::from_elem(upper),
            Antichain::from_elem(0),
        ),
    }
}

/// Advance a time to the compaction frontier (total order: max against its element).
fn advance(t: Time, lower: AntichainRef<'_, Time>) -> Time {
    lower.iter().fold(t, |t, l| std::cmp::max(t, *l))
}

/// The next group at or after the per-batch positions `pos` into `batches`.
fn next_group(batches: &[GraphBatch], pos: &[usize]) -> Option<u32> {
    batches
        .iter()
        .zip(pos.iter())
        .filter_map(|(b, &p)| b.updates.get(p).map(|u| u.0 .0))
        .min()
}

/// Drain every update of group `g` from `batches` (advancing `pos`) into `logic`.
fn drain_group(
    batches: &[GraphBatch],
    pos: &mut [usize],
    g: u32,
    mut logic: impl FnMut(Edge, Time, Diff),
) {
    for (b, p) in batches.iter().zip(pos.iter_mut()) {
        while *p < b.updates.len() && b.updates[*p].0 .0 == g {
            let (edge, t, r) = b.updates[*p];
            logic(edge, t, r);
            *p += 1;
        }
    }
}

// ---------------------------------------------------------------- join backend

/// Per-unit resumption: a position per batch per side. This is the state a single
/// `usize` could not carry — the finding that motivated `type Cursor`.
#[derive(Default)]
struct GraphJoinCursor {
    started: bool,
    pos0: Vec<usize>,
    pos1: Vec<usize>,
}

struct GraphJoinBackend {
    /// Groups per window; tiny in tests to force many windows.
    window: usize,
}

impl ProxyJoinBackend<GraphBatch, GraphBatch> for GraphJoinBackend {
    type Group = u32;
    type Token0 = Edge;
    type Token1 = Edge;
    type R0 = Diff;
    type R1 = Diff;
    type ROut = Diff;
    /// Matched edge pairs with their joined time and multiplied diff.
    type Output = Vec<(Edge, Edge, Time, Diff)>;
    type Cursor = GraphJoinCursor;

    fn next_window(
        &mut self,
        instance: &JoinInstance<'_, GraphBatch, GraphBatch>,
        _fresh: Fresh,
        cursor: &mut GraphJoinCursor,
    ) -> Option<JoinWindow<u32, Edge, Edge, Time, Diff, Diff>> {
        if !cursor.started {
            cursor.started = true;
            cursor.pos0 = vec![0; instance.batches0.len()];
            cursor.pos1 = vec![0; instance.batches1.len()];
        }
        let mut input0 = Vec::new();
        let mut input1 = Vec::new();
        let mut groups = 0;
        while groups < self.window {
            let g0 = next_group(instance.batches0, &cursor.pos0);
            let g1 = next_group(instance.batches1, &cursor.pos1);
            let g = match (g0, g1) {
                (None, None) => break,
                (a, b) => a.into_iter().chain(b).min().unwrap(),
            };
            groups += 1;
            drain_group(instance.batches0, &mut cursor.pos0, g, |e, t, r| {
                input0.push(((g, e), advance(t, instance.lower), r));
            });
            drain_group(instance.batches1, &mut cursor.pos1, g, |e, t, r| {
                input1.push(((g, e), advance(t, instance.lower), r));
            });
        }
        if groups == 0 {
            return None;
        }
        consolidate_updates(&mut input0);
        consolidate_updates(&mut input1);
        Some(JoinWindow { input0, input1 })
    }

    fn cross(
        &mut self,
        _instance: &JoinInstance<'_, GraphBatch, GraphBatch>,
        left: &[(u32, Edge)],
        right: &[(u32, Edge)],
        times: Vec<Time>,
        diffs: Vec<Diff>,
    ) -> Self::Output {
        // Self-redeeming tokens: the output is built from the tokens alone.
        left.iter()
            .zip(right)
            .zip(times)
            .zip(diffs)
            .map(|(((l, r), t), d)| (l.1, r.1, t, d))
            .collect()
    }
}

/// Reference join: all pairs with equal source, times joined, diffs multiplied.
fn naive_join(
    a: &[(Edge, Time, Diff)],
    b: &[(Edge, Time, Diff)],
) -> Vec<((Edge, Edge), Time, Diff)> {
    let mut out = Vec::new();
    for (e0, t0, r0) in a {
        for (e1, t1, r1) in b {
            if e0.0 == e1.0 {
                out.push(((*e0, *e1), std::cmp::max(*t0, *t1), r0 * r1));
            }
        }
    }
    consolidate_updates(&mut out);
    out
}

#[test]
fn join_matches_naive() {
    // Edges spread over several groups, several batches per side, including a
    // retraction; window size 1 forces one group per window.
    let a0 = batch(vec![((0, 1), 0, 1), ((0, 2), 1, 1), ((1, 5), 0, 1), ((3, 9), 2, 1)], 0, 3);
    let a1 = batch(vec![((0, 2), 2, -1), ((2, 4), 2, 1)], 0, 3);
    let b0 = batch(vec![((0, 7), 1, 1), ((1, 8), 2, 1), ((2, 6), 0, 1)], 0, 3);
    let b1 = batch(vec![((1, 8), 2, 1), ((4, 4), 0, 1)], 0, 3);

    let all_a: Vec<_> = a0.updates.iter().chain(&a1.updates).cloned().collect();
    let all_b: Vec<_> = b0.updates.iter().chain(&b1.updates).cloned().collect();
    let expected = naive_join(&all_a, &all_b);

    for window in [1, 2, 100] {
        for fresh in [Fresh::Input0, Fresh::Input1] {
            let mut tactic = ProxyJoinTactic::new(GraphJoinBackend { window });
            let work = tactic.prep(
                vec![a0.clone(), a1.clone()],
                vec![b0.clone(), b1.clone()],
                fresh,
                0,
            );
            let mut got: Vec<((Edge, Edge), Time, Diff)> = work
                .flatten()
                .map(|(l, r, t, d)| ((l, r), t, d))
                .collect();
            consolidate_updates(&mut got);
            assert_eq!(got, expected, "window={window}");
        }
    }
}

// -------------------------------------------------------------- reduce backend

/// Counts edges per source: the output value for source `s` is `(s, count)`.
struct GraphReduceBackend {
    /// Groups per window; tiny in tests to force many windows.
    window: usize,
    // Session state, bracketed by `begin`/`finish`.
    tiles: Vec<Description<Time>>,
    emitted: Vec<Vec<(Edge, Time, Diff)>>,
    started: bool,
    pos_source: Vec<usize>,
    pos_input: Vec<usize>,
    pos_output: Vec<usize>,
    pend_idx: usize,
}

impl GraphReduceBackend {
    fn new(window: usize) -> Self {
        GraphReduceBackend {
            window,
            tiles: Vec::new(),
            emitted: Vec::new(),
            started: false,
            pos_source: Vec::new(),
            pos_input: Vec::new(),
            pos_output: Vec::new(),
            pend_idx: 0,
        }
    }
}

impl ProxyReduceBackend<GraphBatch, GraphBatch> for GraphReduceBackend {
    type Group = u32;
    type Token = Edge;
    type RIn = Diff;
    type ROut = Diff;

    fn begin(&mut self, tiles: &[Description<Time>]) {
        self.tiles = tiles.to_vec();
        self.emitted = vec![Vec::new(); tiles.len()];
        self.started = false;
        self.pend_idx = 0;
    }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, GraphBatch, GraphBatch>,
        pending: &[u32],
    ) -> Option<ReduceWindow<u32, Edge, Time, Diff, Diff>> {
        if !self.started {
            self.started = true;
            self.pos_source = vec![0; instance.source_batches.len()];
            self.pos_input = vec![0; instance.input_batches.len()];
            self.pos_output = vec![0; instance.output_batches.len()];
        }
        let mut keys = Vec::new();
        let mut seeds = Vec::new();
        let mut input = Vec::new();
        let mut output = Vec::new();
        while keys.len() < self.window {
            let g = [
                next_group(instance.source_batches, &self.pos_source),
                next_group(instance.input_batches, &self.pos_input),
                next_group(instance.output_batches, &self.pos_output),
                pending.get(self.pend_idx).copied(),
            ]
            .into_iter()
            .flatten()
            .min();
            let Some(g) = g else { break };
            keys.push(g);
            if pending.get(self.pend_idx) == Some(&g) {
                self.pend_idx += 1;
            }
            // Input presentation: accumulated history plus the novel delta, advanced.
            drain_group(instance.source_batches, &mut self.pos_source, g, |e, t, r| {
                input.push(((g, e), advance(t, instance.lower), r));
            });
            drain_group(instance.input_batches, &mut self.pos_input, g, |e, t, r| {
                // Novel batches also seed interesting times, with their own (raw) times.
                seeds.push((g, t));
                input.push(((g, e), advance(t, instance.lower), r));
            });
            drain_group(instance.output_batches, &mut self.pos_output, g, |e, t, r| {
                output.push(((g, e), advance(t, instance.lower), r));
            });
        }
        if keys.is_empty() {
            return None;
        }
        consolidate_updates(&mut input);
        consolidate_updates(&mut output);
        seeds.sort();
        seeds.dedup();
        Some(ReduceWindow { keys, seeds, input, output })
    }

    fn reduce_corrections(
        &mut self,
        keys: &[u32],
        in_ends: &[usize],
        input: &[(Edge, Diff)],
        out_ends: &[usize],
        output: &[(Edge, Diff)],
    ) -> (Vec<(Edge, Diff)>, Vec<usize>) {
        let mut corr = Vec::new();
        let mut ends = Vec::new();
        let (mut i0, mut o0) = (0, 0);
        for (k, (&i1, &o1)) in keys.iter().zip(in_ends.iter().zip(out_ends)) {
            let count: Diff = input[i0..i1].iter().map(|(_, d)| d).sum();
            assert!(count >= 0, "graph count went negative");
            // Desired output, minus the tentative output, consolidated.
            let mut delta: Vec<(Edge, Diff)> = Vec::new();
            if count > 0 {
                delta.push(((*k, count as u32), 1));
            }
            for (v, d) in &output[o0..o1] {
                delta.push((*v, -d));
            }
            consolidate(&mut delta);
            corr.extend(delta);
            ends.push(corr.len());
            i0 = i1;
            o0 = o1;
        }
        (corr, ends)
    }

    fn emit(&mut self, tile: usize, records: &[((u32, Edge), Time, Diff)]) {
        self.emitted[tile].extend(records.iter().map(|((_g, e), t, r)| (*e, *t, *r)));
    }

    fn finish(&mut self) -> Vec<GraphBatch> {
        self.tiles
            .drain(..)
            .zip(self.emitted.drain(..))
            .map(|(desc, mut updates)| {
                consolidate_updates(&mut updates);
                GraphBatch { updates, description: desc }
            })
            .collect()
    }
}

/// Accumulate `updates` at times `<= t`.
fn accumulate(updates: impl Iterator<Item = (Edge, Time, Diff)>, t: Time) -> Vec<(Edge, Diff)> {
    let mut acc: Vec<(Edge, Diff)> = updates.filter(|(_, ut, _)| *ut <= t).map(|(e, _, r)| (e, r)).collect();
    consolidate(&mut acc);
    acc
}

#[test]
fn reduce_counts_match_naive() {
    // Rounds of edge updates, including deletions that drop a source's count to zero
    // (full retraction) and change another's (correction).
    let rounds: Vec<Vec<(Edge, Time, Diff)>> = vec![
        vec![((0, 1), 0, 1), ((0, 2), 0, 1), ((1, 5), 0, 1)],
        vec![((0, 3), 1, 1), ((2, 7), 1, 1), ((1, 5), 1, -1)],
        vec![((2, 8), 2, 1), ((0, 2), 2, -1), ((3, 4), 2, 1)],
    ];

    for window in [1, 2, 100] {
        let mut tactic = ProxyReduceTactic::new(GraphReduceBackend::new(window));
        let mut source: Vec<GraphBatch> = Vec::new();
        let mut outputs: Vec<GraphBatch> = Vec::new();

        for (r, updates) in rounds.iter().enumerate() {
            let r = r as Time;
            let input = batch(updates.clone(), r, r + 1);
            let lower = Antichain::from_elem(r);
            let upper = Antichain::from_elem(r + 1);
            let held = Antichain::from_elem(r);
            let (produced, frontier) = tactic.retire(
                source.clone(),
                outputs.clone(),
                vec![input.clone()],
                &lower,
                &upper,
                &held,
            );
            assert!(frontier.is_empty(), "total order should defer nothing");
            for (time, b) in produced {
                assert!(held.elements().contains(&time));
                outputs.push(b);
            }
            source.push(input);
        }

        // Oracle: at each round boundary, the accumulated output is exactly the
        // per-source counts of the accumulated input.
        for t in 0..rounds.len() as Time {
            let input_acc = accumulate(source.iter().flat_map(|b| b.updates.iter().cloned()), t);
            let mut expected: Vec<(Edge, Diff)> = Vec::new();
            let mut counts: std::collections::BTreeMap<u32, Diff> = std::collections::BTreeMap::new();
            for ((src, _dst), d) in &input_acc {
                *counts.entry(*src).or_insert(0) += d;
            }
            for (src, c) in counts {
                assert!(c >= 0);
                if c > 0 {
                    expected.push(((src, c as u32), 1));
                }
            }
            let got = accumulate(outputs.iter().flat_map(|b| b.updates.iter().cloned()), t);
            assert_eq!(got, expected, "window={window} at time={t}");
        }
    }
}
