extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::*;
use timely::dataflow::operators::*;

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
        let (mut roots, mut graph) = computation.scoped(|scope| {

            let (edge_input, graph) = scope.new_input();
            let (node_input, roots) = scope.new_input();

            let dists = bfs(&graph, &roots);    // determine distances to each graph node

            dists.map(|((_,s),w)| (s,w))        // keep only the distances, not node ids
                 .consolidate()           // aggregate into one record per distance
                 .inspect_batch(move |t, x| {   // print up something neat for each update
                     println!("observed at {:?}:", t);
                     println!("elapsed: {}s", time::precise_time_s() - (start + t.inner as f64));
                     for y in x {
                         println!("\t{:?}", y);
                     }
                 });

            (node_input, edge_input)
        });

        let nodes = 100_000_000u32; // the u32 helps type inference understand what nodes are
        let edges = 200_000_000;

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        // trickle edges in to dataflow
        for _ in 0..(edges/1000) {
            for _ in 0..1000 {
                graph.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
            }
            computation.step();
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

        // repeatedly change edges
        let mut round = 0 as u32;
        while computation.step() {
            // once each full second ticks, change an edge
            if time::precise_time_s() - start >= round as f64 {
                // add edges using prior rng; remove edges using fresh rng with the same seed
                graph.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
                graph.send(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1));
                graph.advance_to(round + 1);
                round += 1;
            }
        }
    });
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<G: Scope>(edges: &Stream<G, (Edge, i32)>, roots: &Stream<G, (Node, i32)>)
    -> Stream<G, ((Node, u32), i32)>
where G::Timestamp: LeastUpperBound {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|(x,w)| ((x, 0), w));
    let edges = edges.map(|((x,y),w)| ((y,x),w))
                     .concat(&edges);

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = inner.scope().enter(&edges);
        let nodes = inner.scope().enter(&nodes);

        inner.join_map_u(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .group_u(|_, s, t| t.push((*s.peek().unwrap().0, 1)))
     })
}
