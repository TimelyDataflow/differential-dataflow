//! SCC at `(system, a, b)` points, including corrections to earlier `(a, b)` points.
//! Run on demand with:
//! `cargo test --offline --release -p interactive --test tritemporal -- --ignored --nocapture`
//!
//! Like `bitemporal.rs`, every worker advances and reads, worker 0 feeds and compares, and
//! the oracle accumulates only the updates <= the requested point. Here the oracle uses
//! Kosaraju's two DFS passes, independently of the program's nested label propagation.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use interactive::server::{HostTime, Server};
use interactive::{lower, parse};
use timely::order::PartialOrder;

// The SCC example, exporting the actual surviving edges and their outgoing degrees.
// An allow list published at width 1 controls both seeds and propagated labels inside
// each inner loop. The result is SCC on the subgraph induced by the allowed nodes.
// `count` depends on the outer feedback, so it cannot be hoisted out of the loop.
const PROGRAM: &str = r#"
    let edges = input 0 : ((int, int), ()) | map(((src, dst), ()) -> (src, dst));
    let trans = edges | map((src, dst) -> (dst, src));
    let allowed = import "allowed";
    outer: {
        let scc = edges + trim;
        let degrees = scc | map((src, _) -> (src, ())) | count;
        fwd: {
            let nodes = edges | map((_, dst) -> (dst, dst))
                | join(allowed, (dst, label, ()) -> (dst, label))
                | delay((_, n) -> (0, 0, 0, 0, (256, n call bit_length) mul));
            let labels = proposals + nodes | min;
            set proposals = labels | join(scc, (_, label, dst) -> (dst, label))
                | join(allowed, (dst, label, ()) -> (dst, label));
        }
        let trim_fwd = edges
            | join(fwd::labels, (src, dst, label) -> (dst, (src, label)))
            | join(fwd::labels, (dst, (src, label), other) -> (dst, (src, label, other)))
            | filter((_, (_, a, b)) -> (a, b) eq)
            | map((dst, (src, _, _)) -> (dst, src));
        bwd: {
            let nodes = trans | map((_, dst) -> (dst, dst))
                | join(allowed, (dst, label, ()) -> (dst, label))
                | delay((_, n) -> (0, 0, 0, 0, (256, n call bit_length) mul));
            let labels = proposals + nodes | min;
            set proposals = labels | join(trim_fwd, (_, label, dst) -> (dst, label))
                | join(allowed, (dst, label, ()) -> (dst, label));
        }
        let trim_bwd = trans
            | join(bwd::labels, (src, dst, label) -> (dst, (src, label)))
            | join(bwd::labels, (dst, (src, label), other) -> (dst, (src, label, other)))
            | filter((_, (_, a, b)) -> (a, b) eq)
            | map((dst, (src, _, _)) -> (dst, src));
        set trim = trim_bwd - edges;
    }
    export "scc" = outer::scc;
    export "degrees" = outer::degrees;
"#;

const EXPORTS: [&str; 2] = ["scc", "degrees"];
type Point = [u64; 3];
type Edge = (usize, usize);
type Rows = Vec<(String, String, i64)>;

fn at(point: Point) -> HostTime { HostTime::new(point.to_vec().into()) }
fn visible(time: Point, point: Point) -> bool { (0..3).all(|i| time[i] <= point[i]) }
fn sorted(mut rows: Rows) -> Rows { rows.sort(); rows }

#[derive(Clone, Copy, Debug)]
enum Input { Edges, Allowed }

impl Input {
    fn program(self) -> &'static str {
        match self { Self::Edges => "graph", Self::Allowed => "allow" }
    }
}

#[derive(Clone, Debug)]
enum Step {
    Edges { time: Point, rows: Vec<(Edge, i64)> },
    Allowed { system: u64, rows: Vec<(usize, i64)> },
    Advance(Input, Point),
    Compact(Point),
}

#[derive(Default)]
struct Fed {
    edges: Vec<(Point, Edge, i64)>,
    allowed: Vec<(u64, usize, i64)>,
}

impl Fed {
    fn expected(&self, point: Point) -> [Rows; 2] {
        let mut edges = BTreeMap::<Edge, i64>::new();
        let mut allowed = BTreeMap::<usize, i64>::new();
        for &(time, edge, diff) in &self.edges {
            if visible(time, point) { *edges.entry(edge).or_default() += diff; }
        }
        for &(system, node, diff) in &self.allowed {
            if system <= point[0] { *allowed.entry(node).or_default() += diff; }
        }
        // A retraction must have a matching earlier insertion in product order. Negative
        // graph multiplicities would change the meaning of SCC and can prevent convergence.
        assert!(edges.values().all(|&d| d >= 0), "negative edge at {point:?}");
        assert!(allowed.values().all(|&d| d == 0 || d == 1), "invalid allow list at {point:?}");
        edges.retain(|&(src, dst), d| *d > 0 && allowed.get(&src) == Some(&1) && allowed.get(&dst) == Some(&1));
        let nodes: BTreeSet<_> = allowed.iter().filter(|(_, d)| **d == 1).map(|(&n, _)| n).collect();
        let mut forward = BTreeMap::<usize, Vec<usize>>::new();
        let mut backward = BTreeMap::<usize, Vec<usize>>::new();
        for &(src, dst) in edges.keys() {
            forward.entry(src).or_default().push(dst);
            backward.entry(dst).or_default().push(src);
        }
        fn visit(node: usize, graph: &BTreeMap<usize, Vec<usize>>, seen: &mut BTreeSet<usize>, order: &mut Vec<usize>) {
            if seen.insert(node) {
                if let Some(next) = graph.get(&node) {
                    for &next in next { visit(next, graph, seen, order); }
                }
                order.push(node);
            }
        }
        let mut seen = BTreeSet::new();
        let mut order = Vec::new();
        for node in nodes { visit(node, &forward, &mut seen, &mut order); }
        seen.clear();
        let mut component = BTreeMap::new();
        for node in order.into_iter().rev() {
            let mut members = Vec::new();
            visit(node, &backward, &mut seen, &mut members);
            for member in members { component.insert(member, node); }
        }
        let mut scc = Vec::new();
        let mut degrees = BTreeMap::<usize, i64>::new();
        for ((src, dst), diff) in edges {
            if component[&src] == component[&dst] {
                scc.push((src.to_string(), dst.to_string(), diff));
                *degrees.entry(src).or_default() += diff;
            }
        }
        [sorted(scc), sorted(degrees.into_iter().map(|(n, d)| (n.to_string(), d.to_string(), 1)).collect())]
    }
}

#[derive(Default, Debug)]
struct Checks {
    points: usize,
    unique: BTreeSet<Point>,
    after_compaction: usize,
    peeks: usize,
    refused_reads: usize,
}

/// Check every closed, readable point in a finite grid, including a boundary beyond the
/// events and unbounded a/b coordinates. Closure is derived from the commanded input
/// capabilities, never a worker-local probe: all workers must issue the same reads.
fn run(steps: Vec<Step>, workers: usize) -> Checks {
    let guards = timely::execute(timely::Config::process(workers), move |worker| {
        let mut server = Server::new();
        let allow = lower::lower_tree(parse::pipe::parse(r#"export "allowed" = input 0 : (int, ());"#));
        let program = lower::lower_tree(parse::pipe::parse(PROGRAM));
        server.install(worker, "allow", &allow).unwrap();
        server.install_with_root(worker, "graph", &program, 3, at([0, 0, 0])).unwrap();
        assert_eq!(server.trace_width("allowed"), Some(1));
        for export in EXPORTS { assert_eq!(server.trace_width(export), Some(3)); }
        let w0 = worker.index() == 0;
        let mut fed = Fed::default();
        let mut checks = Checks::default();
        let mut compacted = false;
        for (step_index, step) in steps.iter().enumerate() {
            match step {
                Step::Edges { time, rows } => {
                    if w0 {
                        let text = rows.iter().map(|((src, dst), d)| format!("{src},{dst} diff={d}")).collect::<Vec<_>>().join("\n");
                        server.feed_at("graph", 0, &text, &at(*time)).unwrap();
                    }
                    fed.edges.extend(rows.iter().map(|&(edge, diff)| (*time, edge, diff)));
                    continue;
                }
                Step::Allowed { system, rows } => {
                    if w0 {
                        let text = rows.iter().map(|(n, d)| format!("{n} diff={d}")).collect::<Vec<_>>().join("\n");
                        server.feed_at("allow", 0, &text, &at([*system, 0, 0])).unwrap();
                    }
                    fed.allowed.extend(rows.iter().map(|&(node, diff)| (*system, node, diff)));
                    continue;
                }
                Step::Advance(input, point) => {
                    let previous = server.input_frontier(input.program(), 0).unwrap().clone();
                    server.advance_input(input.program(), 0, &at(*point)).unwrap();
                    // Each advance moves exactly one coordinate, and feeds below that
                    // input's own frontier must be refused even if other inputs lag.
                    assert_eq!((0..3).filter(|&i| previous.get(i).copied().unwrap_or(0) != point[i]).count(), 1);
                    let row = match input { Input::Edges => "1,2", Input::Allowed => "1" };
                    assert!(server.feed_at(input.program(), 0, row, &previous).is_err());
                    if matches!(input, Input::Allowed) { server.wait_for(worker, "allowed", &at(*point)).unwrap(); }
                }
                Step::Compact(since) => {
                    for export in EXPORTS {
                        server.compact(export, &at(*since)).unwrap();
                        assert_eq!(server.trace_since(export), Some(&at(*since)));
                        // Below the floor in each coordinate, plus an incomparable point.
                        for point in [[1, 1, 1], [2, 0, 1], [2, 1, 0], [5, 0, 3]] {
                            let error = server.snapshot_as_of(worker, export, &at(point)).unwrap_err();
                            assert!(error.contains("read floor"), "{error}");
                            let error = server.peek_as_of(worker, export, Some("1"), &at(point)).unwrap_err();
                            assert!(error.contains("read floor"), "{error}");
                            if w0 { checks.refused_reads += 2; }
                        }
                        assert!(server.compact(export, &at([1, 1, 1])).is_err());
                    }
                    compacted = true;
                }
            }
            let edge_frontier = server.input_frontier("graph", 0).unwrap().clone();
            let allow_frontier = server.input_frontier("allow", 0).unwrap().clone();
            let system = edge_frontier.get(0).copied().unwrap_or(0).min(allow_frontier.get(0).copied().unwrap_or(0));
            for export in EXPORTS { server.wait_for(worker, export, &at([system, 0, 0])).unwrap(); }
            let mut peeked = false;
            for s in 0..=6 {
                for a in [0, 1, 2, 3, u64::MAX] {
                    for b in [0, 1, 2, 3, u64::MAX] {
                        let point = [s, a, b];
                        let time = at(point);
                        if edge_frontier.less_equal(&time) || allow_frontier.less_equal(&time)
                            || !server.trace_since("scc").unwrap().less_equal(&time) { continue; }
                        let expected = fed.expected(point);
                        for (export, expected) in EXPORTS.into_iter().zip(&expected) {
                            let got = server.snapshot_as_of(worker, export, &time).unwrap().rows();
                            if w0 {
                                assert_eq!(&sorted(got), expected,
                                    "{export} at {point:?}, step {step_index} {step:?}, frontiers {edge_frontier:?}/{allow_frontier:?}, {workers} workers");
                            } else { assert!(got.is_empty()); }
                        }
                        // One keyed peek per checkpoint, including exactly the read floor
                        // immediately after compaction, when it is the grid's first point.
                        if !peeked {
                            let got = server.peek_as_of(worker, "scc", Some("1"), &time).unwrap().rows();
                            if w0 {
                                let expected: Rows = expected[0].iter().filter(|(k, _, _)| k == "1").cloned().collect();
                                assert_eq!(sorted(got), expected, "peek at {point:?}, step {step_index}");
                                checks.peeks += 1;
                            }
                            peeked = true;
                        }
                        if w0 {
                            checks.points += 1;
                            checks.unique.insert(point);
                            checks.after_compaction += usize::from(compacted);
                        }
                    }
                }
            }
        }
        checks
    }).unwrap();
    let mut results: Vec<_> = guards.join().into_iter().map(Result::unwrap).collect();
    results.remove(0)
}

/// The local frontier follows (s, a, b) with a/b changing separately, and the width-1
/// producer advances before the local input. Some checkpoints therefore close points by
/// a or b while system time is still open locally. Compaction is midway through feeding.
fn schedule(mut rounds: Vec<Vec<Step>>, nodes: usize) -> Vec<Step> {
    let mut steps = vec![Step::Allowed { system: 0, rows: (1..=nodes).map(|n| (n, 1)).collect() }];
    let mut frontier = [0, 0, 0];
    for (s, round) in rounds.iter_mut().enumerate() {
        steps.append(round);
        let s = s as u64;
        let allowed = match s { 2 => vec![(2, -1)], 3 => vec![(2, 1)], 4 => vec![(5, -1)], 5 => vec![(5, 1)], _ => vec![] };
        if !allowed.is_empty() { steps.push(Step::Allowed { system: s, rows: allowed }); }
        steps.push(Step::Advance(Input::Allowed, [s + 1, 0, 0]));
        let [a, b] = event_floor(s);
        for (axis, value) in [(1, a), (2, b)] {
            if frontier[axis] != value {
                frontier[axis] = value;
                steps.push(Step::Advance(Input::Edges, frontier));
            }
        }
        frontier[0] = s + 1;
        steps.push(Step::Advance(Input::Edges, frontier));
        if s == 2 { steps.push(Step::Compact([2, 1, 1])); }
    }
    // Close the extra system boundary, too, with the local input ahead of the producer.
    frontier[0] = 7;
    steps.push(Step::Advance(Input::Edges, frontier));
    steps.push(Step::Advance(Input::Allowed, [7, 0, 0]));
    steps
}

fn event_floor(s: u64) -> [u64; 2] {
    match s { 0 | 1 => [0, 0], 2 => [1, 0], 3 => [1, 1], 4 => [2, 1], 5 => [2, 2], _ => unreachable!() }
}

fn hand_scenario() -> Vec<Step> {
    let edges = |time, rows| Step::Edges { time, rows };
    schedule(vec![
        vec![
            // Two triangles, a self-loop, and an isolated allowed node. Multiplicity 2
            // on 1 -> 2 checks that neither the SCC export nor `count` silently dedups.
            edges([0, 0, 0], vec![((1, 2), 2), ((2, 3), 1), ((3, 1), 1), ((4, 5), 1), ((5, 6), 1), ((6, 4), 1), ((7, 7), 1)]),
            // Incomparable times: the triangles merge only at their coordinatewise join.
            edges([0, 1, 0], vec![((3, 4), 1)]),
            edges([0, 0, 1], vec![((6, 1), 1)]),
            edges([0, 2, 0], vec![((6, 7), 1)]),
            edges([0, 2, 2], vec![((7, 6), 1)]),
        ],
        vec![
            // Corrections to earlier a/b, after (2, 2) was already seen.
            edges([1, 0, 0], vec![((1, 2), -1)]),
            edges([1, 0, 1], vec![((6, 1), -1)]),
            edges([1, 2, 2], vec![((7, 6), -1)]),
        ],
        vec![edges([2, 1, 1], vec![((6, 1), 1)])],
        vec![edges([3, 1, 1], vec![((6, 1), -1)])],
        vec![edges([4, 2, 1], vec![((6, 1), 1), ((7, 6), 1)])],
        vec![edges([5, 2, 2], vec![((6, 1), -1), ((7, 6), -1)])],
    ], 8)
}

/// Each unique edge has a fixed a/b coordinate and toggles at increasing system times.
/// This gives late corrections without ever producing a negative graph at a grid point.
/// Edges whose a/b is behind the advanced frontier stop changing; others keep toggling.
fn random_scenario(seed: u64) -> Vec<Step> {
    let mut state = seed;
    let mut next = |n: u64| { state ^= state << 13; state ^= state >> 7; state ^= state << 17; state % n };
    let mut candidates = BTreeMap::new();
    while candidates.len() < 120 {
        let edge = (1 + next(24) as usize, 1 + next(24) as usize);
        candidates.entry(edge).or_insert_with(|| ([next(3), next(3)], false));
    }
    let mut rounds = Vec::new();
    for s in 0..6 {
        let mut by_time = BTreeMap::<Point, Vec<(Edge, i64)>>::new();
        let [floor_a, floor_b] = event_floor(s);
        for (&edge, (cell, present)) in &mut candidates {
            let [a, b] = *cell;
            if a >= floor_a && b >= floor_b && (s == 0 || next(4) != 0) {
                let diff = if *present { -1 } else { 1 };
                *present = !*present;
                by_time.entry([s, a, b]).or_default().push((edge, diff));
            }
        }
        rounds.push(by_time.into_iter().map(|(time, rows)| Step::Edges { time, rows }).collect());
    }
    schedule(rounds, 24)
}

#[test]
#[ignore = "nested tri-temporal SCC oracle sweep; run with --release --ignored --nocapture"]
fn tritemporal_scc_matches_the_oracle() {
    for workers in [1, 2, 4] {
        for (name, steps) in [("hand", hand_scenario()), ("random", random_scenario(0x5EED_2026_1029))] {
            let events: usize = steps.iter().map(|s| match s { Step::Edges { rows, .. } => rows.len(), _ => 0 }).sum();
            eprintln!("starting {name}: workers={workers}, edge events={events}");
            let start = Instant::now();
            let checks = run(steps, workers);
            eprintln!("{name}: workers={workers}, edge events={events}, points={}, unique points={}, snapshot checks={}, points after compaction={}, peeks={}, refused reads={}, elapsed={:.3}s",
                checks.points, checks.unique.len(), checks.points * EXPORTS.len(), checks.after_compaction,
                checks.peeks, checks.refused_reads, start.elapsed().as_secs_f64());
            // All a/b points at s < 3 were read before compaction; later system times
            // retain the four readable a values and four readable b values.
            assert_eq!(checks.unique.len(), 3 * 5 * 5 + 4 * 4 * 4);
            assert_eq!(checks.points, 791);
            assert_eq!(checks.after_compaction, 511);
            assert_eq!(checks.peeks, 18);
            assert_eq!(checks.refused_reads, 16);
        }
    }
}
