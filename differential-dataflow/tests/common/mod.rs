//! Shared scaffolding for the proxy-tactic reference-backend tests: an edge-update
//! batch and value-token backends generic over the timestamp, so the total-order
//! (`u64`) and partial-order (`Pair`) tests drive one implementation. The byte-rows
//! test keeps its own scaffold deliberately — its columnar layout is the point.
#![allow(dead_code)]

use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};

use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::int_proxy::{
    JoinInstance, JoinWindow, ProxyJoinBackend, ProxyReduceBackend, ReduceInstance,
    ReduceWindow,
};
use differential_dataflow::operators::join::Fresh;
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
