extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::progress::timestamp::RootTimestamp;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::operators::*;
use differential_dataflow::collection::LeastUpperBound;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), |computation| {
        let start = time::precise_time_s();

        // define BFS dataflow; return handles to roots and edges inputs
        let (mut roots, mut graph, probe) = computation.scoped(|scope| {

            let (edge_input, graph) = scope.new_input();
            let (node_input, roots) = scope.new_input();

            let dists = bfs(&graph, &roots);    // determine distances to each graph node

            let probe = dists.probe().0;

            (node_input, edge_input, probe)
        });

        let nodes = 50_000_000u32; // the u32 helps type inference understand what nodes are
        let edges = 100_000_000;

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        if computation.index() == 0 {
            // trickle edges in to dataflow
            for _ in 0..(edges/1000) {
                for _ in 0..1000 {
                    graph.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
                }
                computation.step();
            }
        }

        // start the root set out with roots 0, 1, and 2
        // roots.advance_to(0);
        computation.step();
        computation.step();
        computation.step();
        println!("loaded; elapsed: {}s", time::precise_time_s() - start);

        roots.send((0, 1));
        roots.send((1, 1));
        roots.send((2, 1));
        roots.advance_to(1);
        roots.close();

        graph.advance_to(1);

        let mut changes = Vec::new();
        for wave in 0.. {

            for _ in 0..1000 {
                changes.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
                changes.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1));
            }

            let start = time::precise_time_s();
            for _ in 0..1000 {
                let round = *graph.epoch();
                graph.send(changes.pop().unwrap());
                graph.send(changes.pop().unwrap());
                graph.advance_to(round + 1);

                while probe.le(&RootTimestamp::new(round)) {
                    computation.step();
                }
            }

            println!("round {}: avg {}", wave, (time::precise_time_s() - start) / 1000.0f64);
        }
    });
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn stable<G: Scope>(prefs: &Stream<G, ((u32,u32,u32,u32), i32)>) -> Stream<G, ((u32,u32,u32,u32), i32)>
where G::Timestamp: LeastUpperBound {

    prefs.iterate(|inner| {

        let props = inner.group_by_u(|p| (p.0, p), |(&_, &v)| v, |x, s, t| t.push(s.next().unwrap()));
        let taken = props.group_by_u(|p| (p.2, p), |(&_, &v)| v, |x, s, t| t.push(s.next().unwrap()));

        inner.concat(props.map_in_place(|x| x.1 = -x.1))
             .concat(taken)
             .consolidate_by(|x| x.);
    })
}
