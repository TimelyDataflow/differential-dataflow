extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::time::Instant;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::*;

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
    timely::execute_from_args(std::env::args().skip(4), move |computation| {
        
        let timer = Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let (mut graph, mut roots, probe) = computation.scoped(|scope| {

            let (root_input, roots) = scope.new_input();
            let roots = Collection::new(roots);

            let (edge_input, graph) = scope.new_input();
            let graph = Collection::new(graph);

            let probe = reach(&graph, &roots).probe().0;
            (edge_input, root_input, probe)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        if computation.index() == 0 {
            // trickle edges in to dataflow
            for _ in 0..(edges/1000) {
                for _ in 0..1000 {
                    graph.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
                }
                computation.step();
            }
        }

        if computation.index() == 0 {
            println!("loaded; elapsed: {:?}", timer.elapsed());
        }

        roots.advance_to(1);
        graph.advance_to(1);
        computation.step_while(|| probe.lt(graph.time()));

        if computation.index() == 0 {
            println!("stable; elapsed: {:?}", timer.elapsed());
        }
        for i in 0..10 {
            roots.send((i, 1));
            roots.advance_to(2 + i);
            graph.advance_to(2 + i);

            let timer = ::std::time::Instant::now();
            computation.step_while(|| probe.lt(graph.time()));
            if computation.index() == 0 {
                println!("query; elapsed: {:?}", timer.elapsed());
            }
        }

        let mut changes = Vec::new();
        for wave in 0..waves {
            for _ in 0..batch {
                changes.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
                changes.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1));
            }

            let timer = ::std::time::Instant::now();
            let round = *graph.epoch();
            if computation.index() == 0 {
                while let Some(change) = changes.pop() {
                    graph.send(change);
                }
            }
            roots.advance_to(round + 1);
            graph.advance_to(round + 1);

            computation.step_while(|| probe.lt(graph.time()));

            if computation.index() == 0 {
                let elapsed = timer.elapsed();
                let nanos = elapsed.as_secs() * 1_000_000_000 + elapsed.subsec_nanos() as u64;
                // println!("wave {}: avg {:?}", wave, nanos / (batch as u32));
                println!("{}", (nanos as f64) / 1000000000.0f64);
            }
        }
    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn reach<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, Node)>
where G::Timestamp: Lattice {

    let roots = roots.map(|x| (x,x));

    roots.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let roots = roots.enter(&inner.scope());

        inner.join_map(&edges, |_k,&l,&d| (d, l))
             .concat(&roots)
             .distinct()
     })
}
