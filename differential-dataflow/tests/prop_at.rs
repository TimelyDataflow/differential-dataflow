//! Issue #801, reduced: `propagate_at` alone (no SCC wrapper) against a
//! sequential min-label-reachability oracle, under multi-worker execution.
//!
//! Mirrors `trim_edges`' use exactly: seeds are `(dst, dst)` for each edge
//! (consolidated), labels flow along edge direction, each node keeps the
//! minimum label; labels are introduced in rounds `256 * bit_length(label)`.

use rand::{Rng, SeedableRng, StdRng};

use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use timely::Config;
use timely::dataflow::operators::Capture;
use timely::dataflow::operators::capture::Extract;

use differential_dataflow::input::Input;
type Node = usize;
type Edge = (Node, Node);

#[test] fn prop_at_10_20_1000() { test_sizes(10, 20, 1000, Config::process(3)); }

fn edge_script(nodes: usize, edges: usize, rounds: usize) -> Vec<(Edge, usize, isize)> {
    let mut edge_list = Vec::new();
    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng1: StdRng = SeedableRng::from_seed(seed);
    let mut rng2: StdRng = SeedableRng::from_seed(seed);
    for _ in 0 .. edges {
        edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 0, 1));
    }
    for round in 1 .. rounds {
        edge_list.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), round, 1));
        edge_list.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)), round, -1));
    }
    edge_list
}

fn test_sizes(nodes: usize, edges: usize, rounds: usize, config: Config) {

    let edge_list = edge_script(nodes, edges, rounds);

    let mut results1 = prop_sequential(edge_list.clone());
    let mut results2 = prop_differential(edge_list.clone(), config);

    results1.sort();
    results1.sort_by(|x, y| x.1.cmp(&y.1));
    results2.sort();
    results2.sort_by(|x, y| x.1.cmp(&y.1));

    if results1 != results2 {
        println!("RESULTS INEQUAL!!!");
        for x in &results1 { if !results2.contains(x) { println!("  in seq, not diff: {:?}", x); } }
        for x in &results2 { if !results1.contains(x) { println!("  in diff, not seq: {:?}", x); } }
    }
    assert_eq!(results1, results2);
}

/// Sequential oracle: per round, the fixpoint of
/// `label(v) = min({v if v has an in-edge} ∪ {label(u) : (u,v) in edges})`,
/// reported as per-round changes of the `(node, label)` collection.
fn prop_sequential(edge_list: Vec<(Edge, usize, isize)>) -> Vec<((Node, Node), usize, isize)> {

    let mut rounds = 0;
    for &(_, time, _) in &edge_list { rounds = ::std::cmp::max(rounds, time + 1); }

    let mut previous: HashMap<Node, Node> = HashMap::new();
    let mut results = Vec::new();

    for round in 0 .. rounds {

        let mut edges = HashMap::new();
        for &((src, dst), time, diff) in &edge_list {
            if time <= round { *edges.entry((src, dst)).or_insert(0) += diff; }
        }
        edges.retain(|_k, v| *v > 0);

        // Fixpoint of label(v) = min(seed(v), min over in-edges of label(u)).
        let mut labels: HashMap<Node, Node> = HashMap::new();
        for &(_src, dst) in edges.keys() { labels.insert(dst, dst); }
        let mut changed = true;
        while changed {
            changed = false;
            for &(src, dst) in edges.keys() {
                if let Some(&ls) = labels.get(&src) {
                    let entry = labels.get_mut(&dst).expect("dst is seeded");
                    if ls < *entry { *entry = ls; changed = true; }
                }
            }
        }

        // Delta against the previous round.
        let mut changes: HashMap<(Node, Node), isize> = HashMap::new();
        for (&v, &l) in labels.iter() { *changes.entry((v, l)).or_insert(0) += 1; }
        for (&v, &l) in previous.iter() { *changes.entry((v, l)).or_insert(0) -= 1; }
        changes.retain(|_k, v| *v != 0);
        for ((v, l), d) in changes.drain() {
            results.push(((v, l), round, d));
        }

        previous = labels;
    }

    results
}

fn prop_differential(
    edges_list: Vec<(Edge, usize, isize)>,
    config: Config,
) -> Vec<((Node, Node), usize, isize)> {

    let (send, recv) = ::std::sync::mpsc::channel();
    let send = Arc::new(Mutex::new(send));

    timely::execute(config, move |worker| {

        let mut edges_list = edges_list.clone();

        let mut edges = worker.dataflow(|scope| {

            let send = send.lock().unwrap().clone();

            let (edge_input, edges) = scope.new_collection();

            let nodes = edges.clone().map_in_place(|x: &mut Edge| x.0 = x.1).consolidate();

            differential_dataflow::algorithms::graphs::propagate::propagate_at(edges, nodes, |x: &Node| *x as u64)
                .consolidate()
                .inner
                .capture_into(send);

            edge_input
        });

        edges_list.sort_by(|x, y| y.1.cmp(&x.1));

        if worker.index() == 0 {
            let mut round = 0;
            while edges_list.len() > 0 {
                while edges_list.last().map(|x| x.1) == Some(round) {
                    let ((src, dst), _time, diff) = edges_list.pop().unwrap();
                    edges.update((src, dst), diff);
                }
                round += 1;
                edges.advance_to(round);
            }
        }

    }).unwrap();

    recv.extract()
        .into_iter()
        .flat_map(|(_, list)| list.into_iter())
        .collect()
}
