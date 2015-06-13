extern crate rand;
extern crate time;
extern crate columnar;
extern crate timely;
extern crate differential_dataflow;

use std::hash::Hash;
use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::ThreadCommunicator;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::collection_trace::lookup::UnsignedInt;
use differential_dataflow::collection_trace::LeastUpperBound;

use differential_dataflow::operators::*;

fn main() {

        let start = time::precise_time_s();
        let start2 = start.clone();
        let mut computation = GraphRoot::new(ThreadCommunicator);

        let (mut roots, mut graph) = computation.subcomputation(|builder| {

            let (edge_input, graph) = builder.new_input();
            let (node_input, roots) = builder.new_input();

            // TODO : Type inference falls down w/o the u32, I guess because default int stuffs.
            bfs::<_,u32>(&graph, &roots).map(|((_,s),w)| (s,w))
                               .consolidate(|x| *x, |x| *x)
                               .inspect_batch(move |t, x| {
                                   let elapsed = time::precise_time_s() - start2;
                                   println!("{}s:\tobserved at {:?}: {:?} changes; elapsed: {}s", elapsed, t, x.len(), elapsed - (t.inner as f64));
                                //    for y in x {
                                //        println!("\t{:?}", y);
                                //    }
                               });

            (node_input, edge_input)
        });

        let nodes = 100_000_000;
        let edge_count = 500_000_000;

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);
        let mut rng2: StdRng = SeedableRng::from_seed(seed);

        println!("performing BFS on {} nodes, {} edges:", nodes, edge_count);

        let mut left = edge_count;
        while left > 0 {
            let next = if left < 1000 { left } else { 1000 };
            graph.send_at(0, (0..next).map(|_| ((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1)));
            computation.step();
            left -= next;
        }

        roots.send_at(0, vec![(0, 1), (1, 1), (2, 1)].into_iter());
        roots.advance_to(1);
        roots.close();

        let mut round = 0 as u32;
        while computation.step() {
            if time::precise_time_s() - start >= round as f64 {

                let changes = vec![((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1),
                                   ((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1)];
                graph.send_at(round, changes.into_iter());
                graph.advance_to(round + 1);
                round += 1;
            }
        }

        graph.close();

        while computation.step() { }
        computation.step(); // shut down

}

// returns triples (n, r, s) indicating node n can be reached from root r in s steps.
fn bfs<G: GraphBuilder, U: UnsignedInt>(edges: &Stream<G, ((U, U), i32)>,
                                        roots: &Stream<G, (U, i32)>) -> Stream<G, ((U, u32), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|(x,w)| ((x, 0), w));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(u32::max_value(), |x| x.0, |x| x.0, |inner| {

        let edges = inner.builder().enter(&edges);
        let nodes = inner.builder().enter(&nodes);

        inner.join_u(&edges, |l| l, |e| e, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .group_by_u(|x| x, |k,v| (*k, *v), |_, mut s, t| t.push((*s.peek().unwrap().0, 1)))
     })
}
