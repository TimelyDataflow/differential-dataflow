extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::time::Instant;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: u32 = std::env::args().nth(3).unwrap().parse().unwrap();
    let waves: u32 = std::env::args().nth(4).unwrap().parse().unwrap();

    println!("performing reachability on {} nodes, {} edges:", nodes, edges);

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(5), move |worker| {
        
        let timer = Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let (mut graph, mut roots, probe) = worker.dataflow(|scope| {
            let (root_input, roots) = scope.new_collection();
            let (edge_input, graph) = scope.new_collection();
            let probe = reach(&graph, &roots).probe();
            (edge_input, root_input, probe)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        if worker.index() == 0 {
            for _ in 0 .. edges {
                graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
            }
        }

        roots.advance_to(1); roots.flush();
        graph.advance_to(1); graph.flush();
        worker.step_while(|| probe.less_than(graph.time()));

        if worker.index() == 0 {
            println!("stable; elapsed: {:?}", timer.elapsed());
        }

        for i in 0..1 {
            roots.insert(i);
            roots.advance_to(2 + i); roots.flush();
            graph.advance_to(2 + i); graph.flush();

            let timer = ::std::time::Instant::now();
            worker.step_while(|| probe.less_than(graph.time()));
            if worker.index() == 0 {
                println!("query; elapsed: {:?}", timer.elapsed());
            }
        }

        for _wave in 0..waves {
            let timer = ::std::time::Instant::now();
            let round = *graph.epoch();
            if worker.index() == 0 {
                for _ in 0..batch {
                    graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                    graph.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                }
            }
            roots.advance_to(round + 1); roots.flush();
            graph.advance_to(round + 1); graph.flush();

            worker.step_while(|| probe.less_than(graph.time()));

            if worker.index() == 0 {
                let elapsed = timer.elapsed();
                let nanos = elapsed.as_secs() * 1_000_000_000 + elapsed.subsec_nanos() as u64;
                // println!("wave {}: avg {:?}", wave, nanos / (batch as u32));
                println!("{}", (nanos as f64) / 1000000000.0f64);
            }
        }
    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn reach<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, Node>
where G::Timestamp: Lattice+Ord {

    roots.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let roots = roots.enter(&inner.scope());

        edges
            .semijoin(&roots)
            .map(|(_s,d)| d)
            .concat(&roots)
            .distinct()
     })
}
