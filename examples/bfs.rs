extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::collection_trace::lookup::UnsignedInt;
use differential_dataflow::collection_trace::LeastUpperBound;
use differential_dataflow::operators::*;

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
                 .consolidate(|x| *x)           // aggregate into one record per distance
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
        let mut _rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        // trickle edges in to dataflow
        for _ in 0..edges {
            for _ in 0..1 {
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

        roots.send((0,1));
        roots.send((1,1));
        roots.send((2,1));
        roots.advance_to(1);
        roots.close();

        // // repeatedly change edges
        // let mut round = 0 as u32;
        // while computation.step() {
        //     // once each full second ticks, change an edge
        //     if time::precise_time_s() - start >= round as f64 {
        //         // add edges using prior rng; remove edges using fresh rng with the same seed
        //         let changes = vec![((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1),
        //                            ((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1)];
        //         graph.send_at(round, changes.into_iter());
        //         graph.advance_to(round + 1);
        //         round += 1;
        //     }
        // }

        graph.close();                  // seal the source of edges
        while computation.step() { }    // wind down the computation
    });
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<G: Scope, U>(edges: &Stream<G, ((U, U), i32)>, roots: &Stream<G, (U, i32)>)
    -> Stream<G, ((U, u32), i32)>
where G::Timestamp: LeastUpperBound,
      U: UnsignedInt {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|(x,w)| ((x, 0), w));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(u32::max_value(), |x| x.0, |inner| {

        let edges = inner.scope().enter(&edges);
        let nodes = inner.scope().enter(&nodes);

        inner.join_u(&edges, |l| l, |e| e, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .group_by_u(|x| x, |k,v| (*k, *v), |_, s, t| {
                 t.push((*s.peek().unwrap().0, 1));
             })
     })
}
