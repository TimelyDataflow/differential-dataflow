extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::*;
use timely::dataflow::operators::*;
use timely::progress::timestamp::RootTimestamp;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::join::JoinUnsigned;
use differential_dataflow::operators::group::GroupUnsigned;
use differential_dataflow::collection::LeastUpperBound;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: u32 = std::env::args().nth(3).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().nth(4).unwrap() == "inspect";

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(5), move |computation| {
        let start = time::precise_time_s();

        // define BFS dataflow; return handles to roots and edges inputs
        let (mut graph, probe) = computation.scoped(|scope| {

            let roots = vec![(0,1)].into_iter().to_stream(scope);
            let (edge_input, graph) = scope.new_input();
            let mut result = bfs(&Collection::new(graph), &Collection::new(roots));

            if !inspect {
                result = result.filter(|_| false);
            }

            let probe = result.map(|(_x,l)| l).consolidate_by(|&x| x).inner.inspect(|x| println!("\t{:?}", x)).probe();

            (edge_input, probe.0)
        });

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

        println!("loaded; elapsed: {}s", time::precise_time_s() - start);

        graph.advance_to(1);
        while probe.le(&RootTimestamp::new(0)) { computation.step(); }

        println!("stable; elapsed: {}s", time::precise_time_s() - start);

        if batch > 0 {
            let mut changes = Vec::new();
            for wave in 0.. {
                for _ in 0..batch {
                    changes.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
                    changes.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1));
                }

                let start = time::precise_time_s();
                let round = *graph.epoch();
                if computation.index() == 0 {
                    while let Some(change) = changes.pop() {
                        graph.send(change);
                    }
                }
                graph.advance_to(round + 1);

                while probe.le(&RootTimestamp::new(round)) { computation.step(); }

                if computation.index() == 0 {
                    println!("wave {}: avg {}", wave, (time::precise_time_s() - start) / (batch as f64));
                }
            }
        }
    });
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, u32)>
where G::Timestamp: LeastUpperBound {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));
    // let edges = edges.map_in_place(|x| x.0 = ((x.0).1, (x.0).0))
    //                  .concat(&edges);

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner.join_map_u(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .group_u(|_, s, t| t.push((*s.peek().unwrap().0, 1)))
     })
}
