//! A worst-case-optimal join inside a recursive scope: incrementally maintained k-truss.
//!
//! The k-truss of a graph is the maximal subgraph in which every edge takes part in at least
//! `k-2` triangles. It is computed by peeling: count triangles per edge, drop the edges below
//! the threshold, repeat until nothing changes. That makes it a genuine use of a multiway join
//! *inside a loop* rather than a synthetic one — the triangle count is the WCOJ, and the loop
//! is what the delta rules have to survive.
//!
//! # Why this is the test that matters
//!
//! Inside `iterative` the timestamp is `Product<usize, u64>`: an outer round and an iteration
//! counter. Times from different rounds are **incomparable** — `(0, 5)` and `(1, 2)` order
//! under neither direction of the partial order — so this exercises exactly the regime the
//! earlier work was about:
//!
//! * only a *total* order decides which delta rule claims a matching triple, so `Cut` (rather
//!   than a partial-order predicate) is what keeps matches from being dropped;
//! * an output tuple exists at the *join* of its three contributing times, which is neither the
//!   seed's time nor any one atom's, so the carried join time is what keeps outputs from
//!   appearing before their inputs.
//!
//! Feeding the graph over several rounds is what produces the incomparability. A static graph
//! would leave every time inside one outer round, where the iteration counter alone is totally
//! ordered and none of this would be tested.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use timely::order::Product;
use timely::progress::Antichain;
use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::operators::iterate::Variable;
use differential_dogs3::{CollectionIndex, ProposeExtensionMethod};
use differential_dogs3::operators::Cut;

/// Inner timestamp: outer round, then iteration.
type Inner = Product<usize, u64>;
/// An edge, stored canonically as `(min, max)`.
type Edge = (u32, u32);

/// `Cut::Before` is strict, so the compaction bound steps strictly back in both coordinates.
fn step_back(time: &Inner, antichain: &mut Antichain<Inner>) {
    antichain.insert(Product::new(time.outer.saturating_sub(1), time.inner.saturating_sub(1)));
}

fn canonical(a: u32, b: u32) -> Edge { if a < b { (a, b) } else { (b, a) } }

/// Brute-force k-truss: peel until fixpoint.
fn ktruss_oracle(edges: &BTreeSet<Edge>, k: usize) -> BTreeSet<Edge> {
    let mut live = edges.clone();
    loop {
        let mut support: BTreeMap<Edge, usize> = live.iter().map(|e| (*e, 0)).collect();
        for &(a, b) in live.iter() {
            for &(b2, c) in live.iter() {
                if b2 != b { continue; }
                if live.contains(&(a, c)) {
                    *support.get_mut(&(a, b)).unwrap() += 1;
                    *support.get_mut(&(b, c)).unwrap() += 1;
                    *support.get_mut(&(a, c)).unwrap() += 1;
                }
            }
        }
        let next: BTreeSet<Edge> = live.iter().filter(|e| support[e] >= k - 2).cloned().collect();
        if next == live { return live; }
        live = next;
    }
}

/// The maintained k-truss, read back after each round.
fn ktruss_dataflow(workers: usize, rounds: Vec<Vec<(Edge, isize)>>, k: usize) -> Vec<BTreeSet<Edge>> {
    let captured: Arc<Mutex<Vec<(Edge, usize, isize)>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let round_count = rounds.len();

    timely::execute(timely::Config::process(workers), move |worker| {
        let is_source = worker.index() == 0;
        let (sink, rounds) = (Arc::clone(&sink), rounds.clone());
        let mut probe = Handle::new();

        let mut input = worker.dataflow::<usize, _, _>(|scope| {
            let (input, edges) = scope.new_collection();

            let outer = scope.clone();
            let truss = scope.iterative::<u64, _, _>(|inner| {
                let (handle, live) = Variable::new_from(edges.enter(inner), Product::new(Default::default(), 1));

                // One relation in two orientations; the three atoms of the triangle query are
                // roles, not relations, and the cuts follow the roles.
                let fwd = CollectionIndex::index(live.clone());
                let rev = CollectionIndex::index(live.clone().map(|(a, b)| (b, a)));

                //   Q(a,b,c) :- E0(a,b), E1(b,c), E2(a,c)
                // rule 0 seeds E0, binds (a,b), extends by c
                let rule0 = live.clone()
                    .extend(&mut [
                        &mut fwd.extend_using(|&(_a, b): &Edge| b, Cut::for_positions(0, 1), step_back),
                        &mut fwd.extend_using(|&(a, _b): &Edge| a, Cut::for_positions(0, 2), step_back),
                    ])
                    .map(|((a, b), c)| (a, b, c));

                // rule 1 seeds E1, binds (b,c), extends by a
                let rule1 = live.clone()
                    .extend(&mut [
                        &mut rev.extend_using(|&(b, _c): &Edge| b, Cut::for_positions(1, 0), step_back),
                        &mut rev.extend_using(|&(_b, c): &Edge| c, Cut::for_positions(1, 2), step_back),
                    ])
                    .map(|((b, c), a)| (a, b, c));

                // rule 2 seeds E2, binds (a,c), extends by b
                let rule2 = live.clone()
                    .extend(&mut [
                        &mut fwd.extend_using(|&(a, _c): &Edge| a, Cut::for_positions(2, 0), step_back),
                        &mut rev.extend_using(|&(_a, c): &Edge| c, Cut::for_positions(2, 1), step_back),
                    ])
                    .map(|((a, c), b)| (a, b, c));

                // Edges are canonical, so `a < b < c` holds by construction and each triangle
                // is found exactly once. Each triangle lends one unit of support to its three
                // edges; an edge with no triangles never appears, which is the peel.
                let support = rule0.concat(rule1).concat(rule2)
                    .filter(|&(a, b, c)| a < b && b < c)
                    .flat_map(|(a, b, c)| [(a, b), (b, c), (a, c)]);

                let strong = support.threshold(move |_edge, count| if *count >= (k - 2) as isize { 1isize } else { 0 });

                handle.set(strong.clone());
                strong.leave(outer)
            });

            truss
                .consolidate()
                .inspect(move |(edge, time, diff): &(Edge, usize, isize)| {
                    sink.lock().unwrap().push((*edge, *time, *diff))
                })
                .probe_with(&mut probe);

            input
        });

        for (round, updates) in rounds.iter().enumerate() {
            if is_source {
                for (edge, diff) in updates.iter() { input.update(*edge, *diff); }
            }
            input.advance_to(round + 1);
            input.flush();
            while probe.less_than(input.time()) { worker.step(); }
        }
    })
    .unwrap();

    // Accumulate the output up to and including each round.
    let captured = Arc::try_unwrap(captured).unwrap().into_inner().unwrap();
    if std::env::var("DBG_KTRUSS").is_ok() {
        let mut sorted = captured.clone();
        sorted.sort_by_key(|(e, t, _)| (*t, *e));
        eprintln!("--- raw capture ({} updates) ---", sorted.len());
        for (edge, time, diff) in sorted.iter() { eprintln!("  {edge:?} @ {time} {diff:+}"); }
    }
    (0..round_count)
        .map(|round| {
            let mut totals: BTreeMap<Edge, isize> = BTreeMap::new();
            for (edge, _time, diff) in captured.iter().filter(|(_, t, _)| *t <= round) {
                *totals.entry(*edge).or_default() += diff;
            }
            totals.into_iter().filter(|(_, d)| *d > 0).map(|(e, _)| e).collect()
        })
        .collect()
}

/// A handful of rounds that add and remove edges around two overlapping triangles.
fn rounds() -> Vec<Vec<(Edge, isize)>> {
    vec![
        // Round 0: a triangle (0,1,2) plus a dangling edge that supports nothing.
        vec![((0, 1), 1), ((1, 2), 1), ((0, 2), 1), ((2, 3), 1)],
        // Round 1: close a second triangle (0,2,3) sharing the edge (0,2).
        vec![((0, 3), 1), ],
        // Round 2: remove an edge of the first triangle; (0,1) and (1,2) lose all support.
        vec![((0, 1), -1)],
        // Round 3: put it back, and add an isolated edge.
        vec![((0, 1), 1), ((4, 5), 1)],
    ]
}

#[test]
fn ktruss_matches_brute_force_across_rounds() {
    const K: usize = 3; // every surviving edge is in at least one triangle
    let rounds = rounds();

    // The oracle's view of the graph after each round.
    let mut accumulated: BTreeMap<Edge, isize> = BTreeMap::new();
    let expected: Vec<BTreeSet<Edge>> = rounds
        .iter()
        .map(|updates| {
            for (edge, diff) in updates.iter() { *accumulated.entry(*edge).or_default() += diff; }
            let live: BTreeSet<Edge> = accumulated.iter().filter(|(_, d)| **d > 0).map(|(e, _)| *e).collect();
            ktruss_oracle(&live, K)
        })
        .collect();

    for workers in [1, 4] {
        let actual = ktruss_dataflow(workers, rounds.clone(), K);
        assert_eq!(actual, expected, "k-truss at {workers} worker(s)");
    }
}

/// Guard: the edges really are canonical, so `canonical` is not silently unused.
#[test]
fn rounds_are_canonical() {
    for updates in rounds() {
        for ((a, b), _) in updates {
            assert_eq!((a, b), canonical(a, b), "edge ({a},{b}) is not canonical");
        }
    }
}
