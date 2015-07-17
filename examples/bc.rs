extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::Communicator;


use differential_dataflow::collection_trace::LeastUpperBound;
use differential_dataflow::operators::*;

fn main() {
    timely::initialize(std::env::args(), |communicator| {
        let start = time::precise_time_s();
        let mut computation = GraphRoot::new(communicator);

        // define BFS dataflow; return handles to roots and edges inputs
        let (mut roots, mut graph) = computation.subcomputation(|builder| {

            let (edge_input, graph) = builder.new_input();
            let (node_input, roots) = builder.new_input();

            let edges = graph.map(|((x,y),w)| ((y,x), w)).concat(&graph);

            let dists = bc(&edges, &roots);    // determine distances to each graph node

            dists.consolidate(|x| x.0)
                 .inspect_batch(move |t,b| {
                     println!("epoch: {:?}, length: {}, processing: {}",
                        t, b.len(), (time::precise_time_s() - start) - (t.inner as f64));
                 });

            (node_input, edge_input)
        });

        let nodes = 1u32; // the u32 helps type inference understand what nodes are
        let edges = 0;

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        if computation.index() == 0 {
            // trickle edges in to dataflow
            let mut left = edges;
            while left > 0 {
                let next = std::cmp::min(left, 1000);
                graph.send_at(0, (0..next).map(|_| ((rng1.gen_range(0, nodes),
                                                     rng1.gen_range(0, nodes)), 1)));
                computation.step();
                left -= next;
            }

            roots.send_at(0, (0..1).map(|x| (x,1)));
        }
        roots.close();

        // // repeatedly change edges
        // if computation.index() == 0 {
        //     let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions
        //     let mut round = 0 as u32;
        //     while computation.step() {
        //         // once each full second ticks, change an edge
        //         if time::precise_time_s() - start >= round as f64 {
        //             // add edges using prior rng; remove edges using fresh rng with the same seed
        //             let changes = vec![((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1),
        //                                ((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1)];
        //             graph.send_at(round, changes.into_iter());
        //             graph.advance_to(round + 1);
        //             round += 1;
        //         }
        //     }
        // }

        graph.close();                  // seal the source of edges
        while computation.step() { }    // wind down the computation
        println!("done!");
    });
}

// returns pairs (n, (r, b, s)) indicating node n can be reached from root r by b in s steps.
// one pair for each shortest path (so, this number can get quite large, but it is in binary)
fn bc<G: GraphBuilder>(edges: &Stream<G, ((u32, u32), i32)>,
                       roots: &Stream<G, (u32 ,i32)>)
                            -> Stream<G, ((u32, u32, u32, u32), i32)>
where G::Timestamp: LeastUpperBound {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|(x,w)| ((x, x, x, 0), w));

    let dists = nodes.iterate(u32::max_value(), |x| x.0, |dists| {

        let edges = dists.builder().enter(&edges);
        let nodes = dists.builder().enter(&nodes);

        dists.join_u(&edges, |(n,r,_,s)| (n, (r,s)), |e| e, |&n, &(r,s), &d| (d, r, n, s+1))
             .concat(&nodes)
             .group_by(|(n,r,b,s)| ((n,r),(s,b)),       // (key, val)
                       |&(n,r,_,_)| (n + r) as u64,     // how to hash records
                       |&(n,r)| (n + r) as u64,         // how to hash keys
                       |&(n,r), &(b,s)| (n,r,b,s),      // (key, val) -> out
                       |&(_n,_r), mut s, t| {           // (key, vals, outs) reducer
                 // keep only shortest paths
                 let ref_s: &(u32, u32) = s.peek().unwrap().0;
                 let min_s = ref_s.0;
                 t.extend(s.take_while(|x| (x.0).0 == min_s).map(|(&(s,b),w)| ((b,s), w)));
             })
             .inspect_batch(|t,b| println!("iteration: {:?}, length: {}", t, b.len()))
     });

     dists
}
