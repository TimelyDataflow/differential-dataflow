//! Property tests for the proxy tactics, against a value-token edge backend.
//!
//! Every test here checks a general property against an oracle, not current
//! behavior: join against a naive nested-loop join; reduce against exact
//! per-time counting (including a randomized differential test); the pending
//! contract against a partial-order grid; and unit isolation under interleaved
//! draining (which pinned a real bug). The scaffold is the value-token
//! instantiation from the seam design: `Group = src` exact, token = the edge.

use std::collections::BTreeMap;

use timely::progress::Antichain;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::operators::int_proxy::{ProxyJoinTactic, ProxyReduceTactic};
use differential_dataflow::operators::join::{Fresh, JoinTactic};
use differential_dataflow::operators::reduce::ReduceTactic;

use pair::Pair;

type Time = u64;
type GraphBatch = EdgeBatch<Time>;

// ------------------------------------------------------------------- scaffold


use timely::progress::frontier::AntichainRef;
use timely::progress::Timestamp;

use differential_dataflow::consolidation::consolidate;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::int_proxy::{
    JoinInstance, JoinWindow, ProxyJoinBackend, ProxyReduceBackend, ReduceInstance,
    ReduceWindow,
};
use differential_dataflow::trace::{BatchReader, Description};

pub type Edge = (u32, u32);
pub type Diff = isize;

/// A batch of edge updates, sorted and consolidated, `G = src` exact.
#[derive(Clone)]
pub struct EdgeBatch<T> {
    pub updates: Vec<(Edge, T, Diff)>,
    pub description: Description<T>,
}

impl<T: Timestamp + Lattice> BatchReader for EdgeBatch<T> {
    type Time = T;
    fn len(&self) -> usize { self.updates.len() }
    fn description(&self) -> &Description<T> { &self.description }
}

/// Sort, consolidate, and wrap updates as a batch describing `[lower, upper)`.
pub fn batch<T: Timestamp + Lattice + Ord>(updates: Vec<(Edge, T, Diff)>, lower: T, upper: T) -> EdgeBatch<T> {
    batch_frontiers(updates, Antichain::from_elem(lower), Antichain::from_elem(upper))
}

/// As [`batch`], with antichain frontiers for partially ordered intervals.
pub fn batch_frontiers<T: Timestamp + Lattice + Ord>(
    mut updates: Vec<(Edge, T, Diff)>,
    lower: Antichain<T>,
    upper: Antichain<T>,
) -> EdgeBatch<T> {
    consolidate_updates(&mut updates);
    EdgeBatch {
        updates,
        description: Description::new(lower, upper, Antichain::from_elem(T::minimum())),
    }
}

/// Advance a time to the compaction frontier (join against its elements).
pub fn advance<T: Lattice + Clone>(t: T, lower: AntichainRef<'_, T>) -> T {
    lower.iter().fold(t, |t, l| t.join(l))
}

/// The next group at or after the per-batch positions `pos` into `batches`.
///
/// Test-grade: rescans every batch head per group. A real backend should k-way merge
/// with a heap over batch cursors.
pub fn next_group<T>(batches: &[EdgeBatch<T>], pos: &[usize]) -> Option<u32> {
    batches
        .iter()
        .zip(pos.iter())
        .filter_map(|(b, &p)| b.updates.get(p).map(|u| u.0 .0))
        .min()
}

/// Drain every update of group `g` from `batches` (advancing `pos`) into `logic`.
pub fn drain_group<T: Clone>(
    batches: &[EdgeBatch<T>],
    pos: &mut [usize],
    g: u32,
    mut logic: impl FnMut(Edge, T, Diff),
) {
    for (b, p) in batches.iter().zip(pos.iter_mut()) {
        while *p < b.updates.len() && b.updates[*p].0 .0 == g {
            let (edge, ref t, r) = b.updates[*p];
            logic(edge, t.clone(), r);
            *p += 1;
        }
    }
}

// ---------------------------------------------------------------- join backend

/// Per-unit resumption: a position per batch per side. This is the state a single
/// `usize` could not carry — the finding that motivated `type Cursor`.
#[derive(Default)]
pub struct EdgeJoinCursor {
    pub pos0: Vec<usize>,
    pub pos1: Vec<usize>,
}

pub struct EdgeJoinBackend {
    /// Groups per window; tiny in tests to force many windows.
    pub window: usize,
    /// Matches per output container; tiny in tests to force many containers.
    pub target: usize,
}

impl EdgeJoinBackend {
    pub fn new(window: usize) -> Self {
        EdgeJoinBackend { window, target: 8 }
    }
}

impl<T: Timestamp + Lattice + Ord> ProxyJoinBackend<EdgeBatch<T>, EdgeBatch<T>> for EdgeJoinBackend {
    type Group = u32;
    type Token0 = Edge;
    type Token1 = Edge;
    type R0 = Diff;
    type R1 = Diff;
    type ROut = Diff;
    /// Matched edge pairs with their joined time and multiplied diff.
    type Output = Vec<(Edge, Edge, T, Diff)>;
    type Cursor = EdgeJoinCursor;
    type Sink = Vec<(Edge, Edge, T, Diff)>;

    fn next_window(
        &mut self,
        instance: &JoinInstance<'_, EdgeBatch<T>, EdgeBatch<T>>,
        fresh: Fresh,
        cursor: &mut EdgeJoinCursor,
        reuse: Option<JoinWindow<u32, Edge, Edge, T, Diff, Diff>>,
    ) -> Option<JoinWindow<u32, Edge, Edge, T, Diff, Diff>> {
        cursor.pos0.resize(instance.batches0.len(), 0);
        cursor.pos1.resize(instance.batches1.len(), 0);
        // Drive by the fresh side's groups: the accumulated side's other groups are
        // skipped, never presented (O(fresh) presentations; empty fresh side is free).
        let (fresh_b, other_b) = match fresh {
            Fresh::Input0 => (instance.batches0, instance.batches1),
            Fresh::Input1 => (instance.batches1, instance.batches0),
        };
        // Reclaim the spent window's bridge capacity.
        // Route each reclaimed vec back to the side it served, so capacities stay
        // side-stable rather than alternating duties across round trips.
        let (mut fresh_run, mut other_run) = match (reuse, fresh) {
            (Some(JoinWindow { input0, input1 }), Fresh::Input0) => (input0, input1),
            (Some(JoinWindow { input0, input1 }), Fresh::Input1) => (input1, input0),
            (None, _) => (Vec::new(), Vec::new()),
        };
        loop {
            fresh_run.clear();
            other_run.clear();
            let mut groups = 0;
            while groups < self.window {
                let (fresh_pos, other_pos) = match fresh {
                    Fresh::Input0 => (&mut cursor.pos0, &mut cursor.pos1),
                    Fresh::Input1 => (&mut cursor.pos1, &mut cursor.pos0),
                };
                let Some(g) = next_group(fresh_b, fresh_pos) else { break };
                groups += 1;
                drain_group(fresh_b, fresh_pos, g, |e, t, r| {
                    fresh_run.push(((g, e), advance(t, instance.lower), r));
                });
                // Skip the accumulated side below `g`, then drain its matching group.
                for (b, p) in other_b.iter().zip(other_pos.iter_mut()) {
                    while *p < b.updates.len() && b.updates[*p].0 .0 < g {
                        *p += 1;
                    }
                }
                drain_group(other_b, other_pos, g, |e, t, r| {
                    other_run.push(((g, e), advance(t, instance.lower), r));
                });
            }
            if groups == 0 {
                return None;
            }
            consolidate_updates(&mut fresh_run);
            consolidate_updates(&mut other_run);
            // A fully-cancelled quota presents nothing; keep going rather than emit an
            // empty window (the harness's progress guard forbids those).
            if fresh_run.is_empty() && other_run.is_empty() {
                continue;
            }
            let (input0, input1) = match fresh {
                Fresh::Input0 => (fresh_run, other_run),
                Fresh::Input1 => (other_run, fresh_run),
            };
            return Some(JoinWindow { input0, input1 });
        }
    }

    fn absorb(
        &mut self,
        _instance: &JoinInstance<'_, EdgeBatch<T>, EdgeBatch<T>>,
        sink: &mut Self::Sink,
        left: (u32, Edge),
        right: (u32, Edge),
        time: T,
        diff: Diff,
    ) -> Option<Self::Output> {
        // Self-redeeming tokens: the output is built from the tokens alone.
        sink.push((left.1, right.1, time, diff));
        if sink.len() >= self.target {
            Some(std::mem::take(sink))
        } else {
            None
        }
    }

    fn flush(&mut self, _instance: &JoinInstance<'_, EdgeBatch<T>, EdgeBatch<T>>, sink: &mut Self::Sink) -> Option<Self::Output> {
        if sink.is_empty() {
            None
        } else {
            Some(std::mem::take(sink))
        }
    }
}

/// Reference join: all pairs with equal source, times joined, diffs multiplied.
pub fn naive_join<T: Timestamp + Lattice + Ord>(
    a: &[(Edge, T, Diff)],
    b: &[(Edge, T, Diff)],
) -> Vec<((Edge, Edge), T, Diff)> {
    let mut out = Vec::new();
    for (e0, t0, r0) in a {
        for (e1, t1, r1) in b {
            if e0.0 == e1.0 {
                out.push(((*e0, *e1), t0.join(t1), r0 * r1));
            }
        }
    }
    consolidate_updates(&mut out);
    out
}

// -------------------------------------------------------------- reduce backend

/// Counts edges per source: the output value for source `s` is `(s, count)`.
pub struct EdgeReduceBackend<T> {
    /// Groups per window; tiny in tests to force many windows.
    pub window: usize,
    // Session state, bracketed by `begin`/`finish`.
    tiles: Vec<Description<T>>,
    emitted: Vec<Vec<(Edge, T, Diff)>>,
    pos_source: Vec<usize>,
    pos_input: Vec<usize>,
    pos_output: Vec<usize>,
    pend_idx: usize,
}

impl<T> EdgeReduceBackend<T> {
    pub fn new(window: usize) -> Self {
        EdgeReduceBackend {
            window,
            tiles: Vec::new(),
            emitted: Vec::new(),
            pos_source: Vec::new(),
            pos_input: Vec::new(),
            pos_output: Vec::new(),
            pend_idx: 0,
        }
    }
}

impl<T: Timestamp + Lattice + Ord> ProxyReduceBackend<EdgeBatch<T>, EdgeBatch<T>> for EdgeReduceBackend<T> {
    type Group = u32;
    type Token = Edge;
    type RIn = Diff;
    type ROut = Diff;

    fn begin(&mut self, tiles: &[Description<T>]) {
        self.tiles = tiles.to_vec();
        self.emitted = vec![Vec::new(); tiles.len()];
        self.pos_source.clear();
        self.pos_input.clear();
        self.pos_output.clear();
        self.pend_idx = 0;
    }

    fn next_window(
        &mut self,
        instance: &ReduceInstance<'_, EdgeBatch<T>, EdgeBatch<T>>,
        pending: &[u32],
        reuse: Option<ReduceWindow<u32, Edge, T, Diff, Diff>>,
    ) -> Option<ReduceWindow<u32, Edge, T, Diff, Diff>> {
        self.pos_source.resize(instance.source_batches.len(), 0);
        self.pos_input.resize(instance.input_batches.len(), 0);
        self.pos_output.resize(instance.output_batches.len(), 0);
        // Reclaim the spent window's buffer capacity.
        let (mut keys, mut seeds, mut input, mut output) = match reuse {
            Some(ReduceWindow { mut keys, mut seeds, mut input, mut output }) => {
                keys.clear();
                seeds.clear();
                input.clear();
                output.clear();
                (keys, seeds, input, output)
            }
            None => (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
        };
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
            drain_group(instance.source_batches, &mut self.pos_source, g, |e, t, r| {
                input.push(((g, e), advance(t, instance.lower), r));
            });
            drain_group(instance.input_batches, &mut self.pos_input, g, |e, t, r| {
                // Novel batches also seed interesting times, with their own (raw) times.
                seeds.push((g, t.clone()));
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
            assert!(count >= 0, "edge count went negative");
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

    fn emit(&mut self, tile: usize, records: &[((u32, Edge), T, Diff)]) {
        self.emitted[tile].extend(records.iter().map(|((_g, e), t, r)| (*e, t.clone(), *r)));
    }

    fn finish(&mut self) -> Vec<EdgeBatch<T>> {
        self.tiles
            .drain(..)
            .zip(self.emitted.drain(..))
            .map(|(desc, mut updates)| {
                consolidate_updates(&mut updates);
                EdgeBatch { updates, description: desc }
            })
            .collect()
    }
}

/// Accumulate `updates` at times less-or-equal `t` (the partial order's `less_equal`).
pub fn accumulate<T: Timestamp + Lattice>(
    updates: impl Iterator<Item = (Edge, T, Diff)>,
    t: &T,
) -> Vec<(Edge, Diff)> {
    let mut acc: Vec<(Edge, Diff)> = updates
        .filter(|(_, ut, _)| ut.less_equal(t))
        .map(|(e, _, r)| (e, r))
        .collect();
    consolidate(&mut acc);
    acc
}

/// The `Pair` partially ordered timestamp (product order on two coordinates).
pub mod pair {

    #[derive(Hash, Default, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
    pub struct Pair<S, T> { pub first: S, pub second: T }

    impl<S, T> Pair<S, T> {
        pub fn new(first: S, second: T) -> Self { Pair { first, second } }
    }

    use timely::order::PartialOrder;
    impl<S: PartialOrder, T: PartialOrder> PartialOrder for Pair<S, T> {
        fn less_equal(&self, other: &Self) -> bool {
            self.first.less_equal(&other.first) && self.second.less_equal(&other.second)
        }
    }

    use timely::progress::timestamp::Refines;
    impl<S: Timestamp, T: Timestamp> Refines<()> for Pair<S, T> {
        fn to_inner(_outer: ()) -> Self { Self::minimum() }
        fn to_outer(self) {}
        fn summarize(_summary: <Self>::Summary) {}
    }

    use timely::progress::PathSummary;
    impl<S: Timestamp, T: Timestamp> PathSummary<Pair<S, T>> for () {
        fn results_in(&self, timestamp: &Pair<S, T>) -> Option<Pair<S, T>> { Some(timestamp.clone()) }
        fn followed_by(&self, other: &Self) -> Option<Self> { Some(*other) }
    }

    use timely::progress::Timestamp;
    impl<S: Timestamp, T: Timestamp> Timestamp for Pair<S, T> {
        fn minimum() -> Self { Pair { first: S::minimum(), second: T::minimum() } }
        type Summary = ();
    }

    use differential_dataflow::lattice::Lattice;
    impl<S: Lattice, T: Lattice> Lattice for Pair<S, T> {
        fn join(&self, other: &Self) -> Self {
            Pair { first: self.first.join(&other.first), second: self.second.join(&other.second) }
        }
        fn meet(&self, other: &Self) -> Self {
            Pair { first: self.first.meet(&other.first), second: self.second.meet(&other.second) }
        }
    }

    use serde::{Deserialize, Serialize};
    use std::fmt::{Debug, Error, Formatter};
    impl<TOuter: Debug, TInner: Debug> Debug for Pair<TOuter, TInner> {
        fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
            f.write_str(&format!("({:?}, {:?})", self.first, self.second))
        }
    }
}

// ----------------------------------------------------------------- the tests

#[test]
fn join_wave_path_matches_naive() {
    // `join_key` switches implementations at 16 records per side: below, a nested-loop
    // cross product; at or above (on BOTH sides), the `bilinear_wave` time-ordered
    // compacting replay. Every other key in this suite is small, so this test exists to
    // run the wave path at all: one deliberately fat key (20 presented records per
    // side — insertions at spread times plus non-cancelling retractions at later
    // times, so consolidation keeps all of them), alongside a small key so the two
    // paths mix within one unit, checked against the naive oracle.
    let mut a_upds: Vec<(Edge, Time, Diff)> = Vec::new();
    let mut b_upds: Vec<(Edge, Time, Diff)> = Vec::new();
    for v in 0..12u32 {
        a_upds.push(((0, v), (v % 4) as Time, 1));
        b_upds.push(((0, 100 + v), (v % 3) as Time, 1));
    }
    for v in 0..8u32 {
        // Retractions at times distinct from the insertions: two bridge records each,
        // netting to zero late — cancellation the wave must navigate, not lose.
        a_upds.push(((0, v), 4 + (v % 2) as Time, -1));
        b_upds.push(((0, 100 + v), 4 + (v % 3) as Time, -1));
    }
    // A small key too, so nested-loop and wave paths run in the same unit.
    a_upds.push(((1, 7), 0, 1));
    b_upds.push(((1, 9), 2, 1));

    let a = batch(a_upds.clone(), 0, 8);
    let b = batch(b_upds.clone(), 0, 8);
    assert!(a.updates.iter().filter(|u| u.0 .0 == 0).count() >= 16, "fat key must reach the wave threshold");
    assert!(b.updates.iter().filter(|u| u.0 .0 == 0).count() >= 16, "fat key must reach the wave threshold");
    let expected = naive_join(&a.updates, &b.updates);

    for window in [1, 100] {
        for fresh in [Fresh::Input0, Fresh::Input1] {
            let mut tactic = ProxyJoinTactic::new(EdgeJoinBackend::new(window));
            let work = tactic.prep(vec![a.clone()], vec![b.clone()], fresh, 0);
            let mut got: Vec<((Edge, Edge), Time, Diff)> =
                work.flatten().map(|(l, r, t, d)| ((l, r), t, d)).collect();
            consolidate_updates(&mut got);
            assert_eq!(got, expected, "window={window}");
        }
    }
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
            let mut tactic = ProxyJoinTactic::new(EdgeJoinBackend::new(window));
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

#[test]
fn interleaved_units_share_backend() {
    // Two units prepped from one tactic and drained alternately: the scenario that
    // motivated per-unit `Cursor` state and the shared `Rc<RefCell>` backend — the
    // driver holds half-drained units from both queues and polls them under fuel.
    let a = vec![
        batch(vec![((0, 1), 0, 1), ((0, 2), 0, 1), ((0, 3), 1, 1), ((1, 5), 0, 1), ((2, 2), 1, 1)], 0, 2),
        batch(vec![((3, 3), 1, 1)], 0, 2),
    ];
    let b = vec![batch(vec![((0, 7), 1, 1), ((2, 6), 0, 1), ((3, 8), 1, 1)], 0, 2)];
    let c = vec![batch(vec![((5, 1), 0, 1), ((6, 2), 0, 1)], 0, 2)];
    let d = vec![
        batch(vec![((5, 9), 1, 1), ((6, 4), 0, 1)], 0, 2),
        batch(vec![((5, 9), 1, 1)], 0, 2),
    ];

    let all = |bs: &[GraphBatch]| bs.iter().flat_map(|b| b.updates.iter().cloned()).collect::<Vec<_>>();
    let expected1 = naive_join(&all(&a), &all(&b));
    let expected2 = naive_join(&all(&c), &all(&d));

    // target = 2 with a 3-match key (key 0): unit 1 yields a full container and PAUSES
    // holding one staged match, then unit 2 absorbs against the same shared backend.
    // Staging must be unit-owned (`Sink`): with a shared buffer — the bug shape this
    // test pins — unit 1's held match ships inside unit 2's next container, under
    // unit 2's capability. target = 1 would NOT catch this (staging empty at every
    // yield); the straddling partial container is the point.
    let mut backend = EdgeJoinBackend::new(1);
    backend.target = 2;
    let mut tactic = ProxyJoinTactic::new(backend);
    let mut u1 = tactic.prep(a, b, Fresh::Input0, 0);
    let mut u2 = tactic.prep(c, d, Fresh::Input1, 0);
    let mut got1: Vec<((Edge, Edge), Time, Diff)> = Vec::new();
    let mut got2: Vec<((Edge, Edge), Time, Diff)> = Vec::new();
    let (mut done1, mut done2) = (false, false);
    while !done1 || !done2 {
        if !done1 {
            match u1.next() {
                Some(out) => got1.extend(out.into_iter().map(|(l, r, t, d)| ((l, r), t, d))),
                None => done1 = true,
            }
        }
        if !done2 {
            match u2.next() {
                Some(out) => got2.extend(out.into_iter().map(|(l, r, t, d)| ((l, r), t, d))),
                None => done2 = true,
            }
        }
    }
    consolidate_updates(&mut got1);
    consolidate_updates(&mut got2);
    assert_eq!(got1, expected1);
    assert_eq!(got2, expected2);
}

#[test]
fn reduce_fuzz_matches_oracle() {
    // Deterministic randomized differential test: random updates over a small key
    // space, random times within each round's interval (so rounds mix fast-path
    // keys — one distinct time — with slow-path keys), random retractions of live
    // edges, several window sizes. The oracle is exact per-time counting.
    let mut state = 0x853c49e6748fea9bu64;
    let mut rng = move || {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (state >> 33) as u32
    };

    for window in [1, 3, 100] {
        let mut tactic = ProxyReduceTactic::new(EdgeReduceBackend::new(window));
        let mut source: Vec<GraphBatch> = Vec::new();
        let mut outputs: Vec<GraphBatch> = Vec::new();
        let mut live: Vec<Edge> = Vec::new();
        const ROUNDS: u64 = 8;
        const WIDTH: u64 = 3;
        for round in 0..ROUNDS {
            let lo = round * WIDTH;
            let hi = lo + WIDTH;
            let mut updates: Vec<(Edge, Time, Diff)> = Vec::new();
            // Retract only edges from PRIOR rounds (any time this round is at or after
            // their insertion, keeping every prefix accumulation non-negative); this
            // round's insertions join `live` only at round end.
            let mut fresh: Vec<Edge> = Vec::new();
            for _ in 0..40 {
                let t = lo + (rng() as u64) % WIDTH;
                if !live.is_empty() && rng() % 4 == 0 {
                    let e = live[(rng() as usize) % live.len()];
                    live.retain(|x| *x != e);
                    updates.push((e, t, -1));
                } else {
                    let e = (rng() % 16, rng() % 64);
                    fresh.push(e);
                    updates.push((e, t, 1));
                }
            }
            live.append(&mut fresh);
            let input = batch(updates, lo, hi);
            let lower = Antichain::from_elem(lo);
            let upper = Antichain::from_elem(hi);
            let held = Antichain::from_elem(lo);
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
            for t in lo..hi {
                assert_counts(&source, &outputs, t, &format!("fuzz window={window} round={round}"));
            }
        }
    }
}

/// Per-time count oracle shared by the reduce tests.
fn assert_counts(source: &[GraphBatch], outputs: &[GraphBatch], t: Time, label: &str) {
    let input_acc = accumulate(source.iter().flat_map(|b| b.updates.iter().cloned()), &t);
    let mut counts: BTreeMap<u32, Diff> = BTreeMap::new();
    for ((src, _dst), d) in &input_acc {
        *counts.entry(*src).or_insert(0) += d;
    }
    let mut expected: Vec<(Edge, Diff)> = Vec::new();
    for (src, c) in counts {
        assert!(c >= 0);
        if c > 0 {
            expected.push(((src, c as u32), 1));
        }
    }
    let got = accumulate(outputs.iter().flat_map(|b| b.updates.iter().cloned()), &t);
    assert_eq!(got, expected, "{label} at time={t}");
}

#[test]
fn reduce_multitime_rounds() {
    // Intervals of width two, two distinct times per batch: under total order this
    // exercises the slow (discovery) path even with the single-moment fast path in
    // place — a key with two distinct seed times cannot take it — and mixes fast
    // and slow keys within one window.
    let rounds: Vec<(Vec<(Edge, Time, Diff)>, Time, Time)> = vec![
        (vec![((0, 1), 0, 1), ((0, 2), 1, 1), ((1, 5), 0, 1), ((1, 6), 1, 1)], 0, 2),
        (vec![((0, 3), 2, 1), ((1, 5), 3, -1), ((2, 7), 2, 1), ((2, 8), 3, 1)], 2, 4),
    ];
    for window in [1, 100] {
        let mut tactic = ProxyReduceTactic::new(EdgeReduceBackend::new(window));
        let mut source: Vec<GraphBatch> = Vec::new();
        let mut outputs: Vec<GraphBatch> = Vec::new();
        for (updates, lo, hi) in rounds.iter() {
            let input = batch(updates.clone(), *lo, *hi);
            let lower = Antichain::from_elem(*lo);
            let upper = Antichain::from_elem(*hi);
            let held = Antichain::from_elem(*lo);
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
        for t in 0..4u64 {
            assert_counts(&source, &outputs, t, &format!("multitime window={window}"));
        }
    }
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
        let mut tactic = ProxyReduceTactic::new(EdgeReduceBackend::new(window));
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
            let input_acc = accumulate(source.iter().flat_map(|b| b.updates.iter().cloned()), &t);
            let mut expected: Vec<(Edge, Diff)> = Vec::new();
            let mut counts: BTreeMap<u32, Diff> = BTreeMap::new();
            for ((src, _dst), d) in &input_acc {
                *counts.entry(*src).or_insert(0) += d;
            }
            for (src, c) in counts {
                assert!(c >= 0);
                if c > 0 {
                    expected.push(((src, c as u32), 1));
                }
            }
            let got = accumulate(outputs.iter().flat_map(|b| b.updates.iter().cloned()), &t);
            assert_eq!(got, expected, "window={window} at time={t}");
        }
    }
}

// ---------------------------------------------------- partial order (pending)

type PTime = Pair<u64, u64>;

fn t(a: u64, b: u64) -> PTime {
    Pair::new(a, b)
}

#[test]
fn pending_times_carry_across_retires() {
    // Key 7 receives updates at incomparable times in round A; key 3 receives novel
    // input only in round B, so round B's windows must merge a pending-only group
    // (7) with a novel-only group (3), in group order, at window size 1.
    for window in [1, 2, 100] {
        let mut tactic = ProxyReduceTactic::new(EdgeReduceBackend::new(window));

        // Round A: interval [ {(0,0)}, {(1,1)} ), capability held at (0,0).
        let input_a = batch_frontiers(
            vec![((7, 100), t(0, 1), 1), ((7, 200), t(1, 0), 1)],
            Antichain::from_elem(t(0, 0)),
            Antichain::from_elem(t(1, 1)),
        );
        let lower = Antichain::from_elem(t(0, 0));
        let upper = Antichain::from_elem(t(1, 1));
        let held = Antichain::from_elem(t(0, 0));
        let (produced_a, frontier_a) = tactic.retire(
            Vec::new(),
            Vec::new(),
            vec![input_a.clone()],
            &lower,
            &upper,
            &held,
        );
        // The joint interesting time (1,1) lies beyond upper: it must be deferred,
        // and the returned frontier must hold it.
        assert_eq!(frontier_a, Antichain::from_elem(t(1, 1)), "the join of incomparable updates must be pended");

        let mut outputs: Vec<EdgeBatch<PTime>> = produced_a.into_iter().map(|(_, b)| b).collect();

        // Round B: interval [ {(1,1)}, {(2,2)} ), capability held at (1,1). Novel
        // input only for key 3; key 7 participates as a pending-only group.
        let input_b = batch_frontiers(
            vec![((3, 5), t(1, 1), 1)],
            Antichain::from_elem(t(1, 1)),
            Antichain::from_elem(t(2, 2)),
        );
        let lower = Antichain::from_elem(t(1, 1));
        let upper = Antichain::from_elem(t(2, 2));
        let held = Antichain::from_elem(t(1, 1));
        let (produced_b, frontier_b) = tactic.retire(
            vec![input_a.clone()],
            outputs.clone(),
            vec![input_b.clone()],
            &lower,
            &upper,
            &held,
        );
        assert!(frontier_b.is_empty(), "nothing further to defer");
        outputs.extend(produced_b.into_iter().map(|(_, b)| b));

        // Oracle over a grid of times, using the partial order's less_equal: the
        // accumulated output at t must be exactly the per-source counts of the
        // accumulated input at t. At (1,1) this requires round B's correction to
        // have collapsed the two partial counts of key 7 into one count of 2.
        let inputs: Vec<(Edge, PTime, Diff)> = input_a
            .updates
            .iter()
            .chain(&input_b.updates)
            .cloned()
            .collect();
        for probe in [t(0, 0), t(0, 1), t(1, 0), t(1, 1), t(2, 1), t(2, 2)] {
            let input_acc = accumulate(inputs.iter().cloned(), &probe);
            let mut counts: BTreeMap<u32, Diff> = BTreeMap::new();
            for ((src, _dst), d) in &input_acc {
                *counts.entry(*src).or_insert(0) += d;
            }
            let mut expected: Vec<(Edge, Diff)> = Vec::new();
            for (src, c) in counts {
                if c > 0 {
                    expected.push(((src, c as u32), 1));
                }
            }
            let got = accumulate(outputs.iter().flat_map(|b| b.updates.iter().cloned()), &probe);
            assert_eq!(got, expected, "window={window} at probe={probe:?}");
        }
    }
}
