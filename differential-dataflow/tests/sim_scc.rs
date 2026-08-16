//! Deterministic-simulation repro for issue #801: `strongly_connected_at` /
//! `propagate_at` nondeterminism under multi-worker execution.
//!
//! Runs the same computation as `tests/scc.rs::scc_at_10_20_1000`, but on
//! `timely::simulate::Simulation`: all workers on one thread, message delivery
//! controlled by a seeded schedule. Any seed whose output disagrees with the
//! sequential oracle is a deterministic, replayable repro.

use rand::{Rng, SeedableRng, StdRng};

use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use timely::simulate::{Decision, Simulation};
use timely::dataflow::operators::Inspect;

use differential_dataflow::input::Input;

type Node = usize;
type Edge = (Node, Node);

fn peers() -> usize { std::env::var("PEERS").ok().and_then(|s| s.parse().ok()).unwrap_or(3) }

/// A tiny deterministic RNG (SplitMix64), as in timely's grind harness.
struct SplitMix64(u64);
impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, bound: usize) -> usize {
        (self.next() % (bound as u64)) as usize
    }
}

/// The edge script of `scc_at_10_20_1000`.
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

/// One simulated run under schedule seed `sseed`; `chunk` seeded decisions are
/// applied after each input round. Returns the consolidated output, or panics
/// on failure to quiesce.
fn sim_run(nodes: usize, edges: usize, rounds: usize, sseed: u64, chunk: usize) -> Vec<(Edge, usize, isize)> {

    let mut edge_list = edge_script(nodes, edges, rounds);

    let mut sim = Simulation::new(peers());
    let results = Rc::new(RefCell::new(Vec::new()));

    let plain = std::env::var("PLAIN").is_ok();
    let mut input0 = None;
    for index in 0 .. peers() {
        let results = Rc::clone(&results);
        let input = sim.worker_mut(index).dataflow(|scope| {
            let (edge_input, edges) = scope.new_collection();
            let result = if plain {
                plain_scc(edges)
            } else {
                differential_dataflow::algorithms::graphs::scc::strongly_connected_at(edges, |x: &Node| *x as u64)
            };
            result
                .consolidate()
                .inner
                .inspect(move |&(data, time, diff): &(Edge, usize, isize)| {
                    results.borrow_mut().push((data, time, diff));
                });
            edge_input
        });
        // Worker 0 feeds; the rest close their inputs immediately, as in tests/scc.rs.
        if index == 0 { input0 = Some(input); }
    }
    let mut input0 = input0.unwrap();

    let mut rng = SplitMix64(sseed);

    // Feed the script one round at a time. Most rounds run under a fair schedule
    // (bounded drain), as an uncontended execution would; seeded "chaos windows"
    // of a few rounds run under a skewed random schedule instead, mimicking the
    // stalls that CPU contention induces in the native repro.
    edge_list.sort_by(|x, y| y.1.cmp(&x.1));
    let mut round = 0;
    let mut chaos_left = 0usize;
    while !edge_list.is_empty() {
        while edge_list.last().map(|x| x.1) == Some(round) {
            let ((src, dst), _time, diff) = edge_list.pop().unwrap();
            input0.update((src, dst), diff);
        }
        round += 1;
        input0.advance_to(round);
        input0.flush();
        let chaos_freq: u64 = std::env::var("CHAOS_FREQ").ok().and_then(|s| s.parse().ok()).unwrap_or(16);
        let chaos_max: usize = std::env::var("CHAOS_MAX").ok().and_then(|s| s.parse().ok()).unwrap_or(8);
        let fair: usize = std::env::var("FAIR").ok().and_then(|s| s.parse().ok()).unwrap_or(2);
        if chaos_left == 0 && rng.next() % chaos_freq == 0 { chaos_left = 1 + rng.below(chaos_max); }
        if chaos_left > 0 {
            chaos_left -= 1;
            for _ in 0 .. chunk {
                if rng.next() % 2 == 0 {
                    sim.apply(Decision::Step(rng.below(peers())));
                } else {
                    let source = rng.below(peers());
                    let target = rng.below(peers());
                    sim.apply(Decision::Deliver { source, target, count: 1 + rng.below(4) });
                }
            }
        } else {
            sim.drain(fair);
        }
    }
    drop(input0);

    // Drain in chunks under a wall-clock budget, so a livelocked run is
    // classified as a hang rather than stalling the whole grind.
    let budget = std::time::Duration::from_secs(
        std::env::var("GRIND_BUDGET").ok().and_then(|s| s.parse().ok()).unwrap_or(120),
    );
    let start = std::time::Instant::now();
    let mut quiesced = false;
    while !quiesced && start.elapsed() < budget {
        quiesced = sim.drain(1_000);
    }
    assert!(quiesced, "HANG: simulation failed to quiesce within {:?} (seed {:#x})", budget, sseed);

    // Consolidate the captured output.
    let mut output: Vec<(Edge, usize, isize)> = Vec::new();
    let mut accum: HashMap<(Edge, usize), isize> = HashMap::new();
    for (data, time, diff) in results.borrow().iter() {
        *accum.entry((*data, *time)).or_insert(0) += *diff;
    }
    for ((data, time), diff) in accum.drain() {
        if diff != 0 { output.push((data, time, diff)); }
    }
    output.sort();
    output.sort_by(|x, y| x.1.cmp(&y.1));
    output
}

/// The plain (unrotated) SCC from tests/scc.rs: local `iterate` + `reduce`
/// reachability with the same 256·bitlen label waves as `propagate_at`.
fn plain_scc<'scope, T>(graph: differential_dataflow::VecCollection<'scope, T, Edge>) -> differential_dataflow::VecCollection<'scope, T, Edge>
where
    T: timely::progress::Timestamp + differential_dataflow::lattice::Lattice + Ord + std::hash::Hash,
{
    use differential_dataflow::operators::*;
    graph.clone().iterate(|scope, inner| {
        let edges = graph.enter(scope);
        let trans = edges.clone().map_in_place(|x| std::mem::swap(&mut x.0, &mut x.1));
        plain_trim(plain_trim(inner, edges), trans)
    })
}

fn plain_trim<'scope, T>(cycle: differential_dataflow::VecCollection<'scope, T, Edge>, edges: differential_dataflow::VecCollection<'scope, T, Edge>) -> differential_dataflow::VecCollection<'scope, T, Edge>
where
    T: timely::progress::Timestamp + differential_dataflow::lattice::Lattice + Ord + std::hash::Hash,
{
    let nodes = edges.clone()
                     .map_in_place(|x| x.0 = x.1)
                     .consolidate();

    let labels = plain_reach(cycle, nodes);

    edges.consolidate()
         .join_map(labels.clone(), |&e1, &e2, &l1| (e2, (e1, l1)))
         .join_map(labels.clone(), |&e2, &(e1, l1), &l2| ((e1, e2), (l1, l2)))
         .filter(|&(_, (l1, l2))| l1 == l2)
         .map(|((x1, x2), _)| (x2, x1))
}

fn plain_reach<'scope, T>(edges: differential_dataflow::VecCollection<'scope, T, Edge>, nodes: differential_dataflow::VecCollection<'scope, T, (Node, Node)>) -> differential_dataflow::VecCollection<'scope, T, Edge>
where
    T: timely::progress::Timestamp + differential_dataflow::lattice::Lattice + Ord + std::hash::Hash,
{
    use differential_dataflow::operators::*;
    edges.clone()
         .filter(|_| false)
         .iterate(|scope, inner| {
             let edges = edges.enter(scope);
             let nodes = nodes.enter_at(scope, |r| 256 * (64 - (r.0 as u64).leading_zeros() as u64));

             inner.join_map(edges, |_k, l, d| (*d, *l))
                  .concat(nodes)
                  .reduce(|_, s, t| t.push((*s[0].0, 1)))
         })
}

/// Sequential SCC oracle, as in tests/scc.rs.
fn scc_sequential(edge_list: Vec<(Edge, usize, isize)>) -> Vec<(Edge, usize, isize)> {

    let mut rounds = 0;
    for &(_, time, _) in &edge_list { rounds = ::std::cmp::max(rounds, time + 1); }

    let mut output = Vec::new();
    let mut results = Vec::new();

    for round in 0 .. rounds {

        let mut edges = HashMap::new();
        for &((src, dst), time, diff) in &edge_list {
            if time <= round { *edges.entry((src, dst)).or_insert(0) += diff; }
        }
        edges.retain(|_k, v| *v > 0);

        let mut forward = HashMap::new();
        let mut reverse = HashMap::new();
        for &(src, dst) in edges.keys() {
            forward.entry(src).or_insert(Vec::new()).push(dst);
            reverse.entry(dst).or_insert(Vec::new()).push(src);
        }

        let mut visited = HashSet::new();
        let mut list = Vec::new();
        for &node in forward.keys() {
            visit(node, &forward, &mut visited, &mut list)
        }

        let mut component = HashMap::new();
        while let Some(node) = list.pop() {
            assign(node, node, &reverse, &mut component);
        }

        let mut new_output = Vec::new();
        for (&(src, dst), &cnt) in edges.iter() {
            if component.get(&src) == component.get(&dst) {
                new_output.push(((src, dst), cnt));
            }
        }

        let mut changes = HashMap::new();
        for &((src, dst), cnt) in new_output.iter() {
            *changes.entry((src, dst)).or_insert(0) += cnt;
        }
        for &((src, dst), cnt) in output.iter() {
            *changes.entry((src, dst)).or_insert(0) -= cnt;
        }
        changes.retain(|_k, v| *v != 0);

        for ((src, dst), del) in changes.drain() {
            results.push(((src, dst), round, del));
        }

        output = new_output;
    }

    results.sort();
    results.sort_by(|x, y| x.1.cmp(&y.1));
    results
}

fn visit(node: usize, forward: &HashMap<usize, Vec<usize>>, visited: &mut HashSet<usize>, list: &mut Vec<usize>) {
    if !visited.contains(&node) {
        visited.insert(node);
        if let Some(edges) = forward.get(&node) {
            for &edge in edges.iter() {
                visit(edge, forward, visited, list)
            }
        }
        list.push(node);
    }
}

fn assign(node: usize, root: usize, reverse: &HashMap<usize, Vec<usize>>, component: &mut HashMap<usize, usize>) {
    if !component.contains_key(&node) {
        component.insert(node, root);
        if let Some(edges) = reverse.get(&node) {
            for &edge in edges.iter() {
                assign(edge, root, reverse, component);
            }
        }
    }
}

/// One seed, parameterized by env (SEED / ROUNDS / CHUNK); used by the
/// process-isolated seed sweep, where an external alarm bounds livelocks.
#[test]
fn sim_scc_at_one_seed() {
    let seed: u64 = std::env::var("SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(0x801);
    let rounds: usize = std::env::var("ROUNDS").ok().and_then(|s| s.parse().ok()).unwrap_or(100);
    let chunk: usize = std::env::var("CHUNK").ok().and_then(|s| s.parse().ok()).unwrap_or(50);
    let oracle = scc_sequential(edge_script(10, 20, rounds));
    let result = sim_run(10, 20, rounds, seed, chunk);
    if result != oracle {
        println!("RESULTS INEQUAL (seed {:#x})", seed);
        for x in &oracle { if !result.contains(x) { println!("  in oracle, not sim: {:?}", x); } }
        for x in &result { if !oracle.contains(x) { println!("  in sim, not oracle: {:?}", x); } }
    }
    assert_eq!(oracle, result, "seed {:#x} disagrees with oracle", seed);
}

/// Seeded schedule exploration: `GRIND_SEEDS=100 GRIND_ROUNDS=1000 cargo test -p
/// differential-dataflow --test sim_scc -- --ignored --nocapture sim_scc_grind`.
#[test]
#[ignore]
fn sim_scc_grind() {
    let seeds: u64 = std::env::var("GRIND_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(20);
    let start: u64 = std::env::var("GRIND_START").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    let rounds: usize = std::env::var("GRIND_ROUNDS").ok().and_then(|s| s.parse().ok()).unwrap_or(1000);
    let chunk: usize = std::env::var("GRIND_CHUNK").ok().and_then(|s| s.parse().ok()).unwrap_or(50);
    let oracle = scc_sequential(edge_script(10, 20, rounds));
    let mut failures = Vec::new();
    for seed in start .. start + seeds {
        use std::io::Write;
        let started = std::time::Instant::now();
        let outcome = std::panic::catch_unwind(|| sim_run(10, 20, rounds, seed, chunk));
        let elapsed = started.elapsed();
        match outcome {
            Ok(result) => {
                if result != oracle {
                    println!("seed {:#x}: WRONG ({} tuples vs oracle {}) [{:?}]", seed, result.len(), oracle.len(), elapsed);
                    for x in &oracle { if !result.contains(x) { println!("  in oracle, not sim: {:?}", x); } }
                    for x in &result { if !oracle.contains(x) { println!("  in sim, not oracle: {:?}", x); } }
                    failures.push((seed, "wrong"));
                } else {
                    println!("seed {:#x}: ok [{:?}]", seed, elapsed);
                }
            }
            Err(_) => {
                println!("seed {:#x}: PANIC/HANG [{:?}]", seed, elapsed);
                failures.push((seed, "panic/hang"));
            }
        }
        std::io::stdout().flush().ok();
    }
    assert!(failures.is_empty(), "failing seeds: {:?}", failures);
}
