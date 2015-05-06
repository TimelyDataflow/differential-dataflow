extern crate rand;
extern crate time;
extern crate columnar;
extern crate timely;
extern crate differential_dataflow;

use std::mem;

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

            bfs(&graph, &roots).map(|((_,r,s),w)| ((r,s),w))    // keep (root, step) pairs
                               .consolidate(|||x| x.0)          // aggregate up the counts
                               .inspect_batch(move |t, x| {
                                   let elapsed = time::precise_time_s() - start2;
                                   println!("{}s:\tobserved at {:?}: {:?} changes; elapsed: {}s", elapsed, t, x.len(), elapsed - (t.inner as f64));
                                   for y in x {
                                       println!("\t{:?}", y);
                                   }
                               });

            (node_input, edge_input)
        });

        let node_count = 1_000_000;
        let edge_count = 10_000_000;

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        rng.gen::<f64>();

        println!("performing BFS on {} nodes, {} edges:", node_count, edge_count);

        // let mut rng = rand::thread_rng();
        let mut edges = Vec::new();
        for _ in 0..edge_count {
            edges.push((((rng.gen::<u64>() % node_count) as u32, (rng.gen::<u64>() % node_count) as u32), 1))
        }

        graph.send_at(0, vec![((0,1), 1)].into_iter());
        graph.send_at(0, edges.clone().into_iter().map(|((x,y),w)|((x as u32, y as u32), w)));

        roots.send_at(0, vec![(0, 1), (1, 1), (2, 1)].into_iter());
        roots.advance_to(1);
        roots.close();

        let mut round = 0 as u32;
        while computation.step() {
            if time::precise_time_s() - start >= round as f64 {

                // println!("adding records!");
                let new_record = (((rng.gen::<u64>() % node_count) as u32,
                                   (rng.gen::<u64>() % node_count) as u32), 1);
                let new_position = rng.gen::<usize>() % edges.len();
                let mut old_record = mem::replace(&mut edges[new_position], new_record.clone());
                old_record.1 = -1;

                println!("{}s:\tintroducing {:?}", time::precise_time_s() - start, vec![old_record, new_record]);
                graph.send_at(round, vec![old_record, new_record].into_iter());
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
                                        roots: &Stream<G, (U, i32)>) -> Stream<G, ((U, U, u32), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|(x,w)| ((x, x, 0), w));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(u32::max_value(), |||x| x.0, |inner| {

             let edges = inner.builder().enter(&edges);
             let nodes = inner.builder().enter(&nodes);

             inner.join_u(&edges,
                      |l| (l.0, (l.1, l.2)),        // incoming (n,r,s) -> key: n, val: (r,s)
                      |e| e, |l| l.0, |e| e.0,      // nonsense about edges and partitioning
                      |_k,l,d| (*d, l.0, l.1 + 1))  // dst d can be reached from r in s + 1 steps
                  .concat(&nodes)
                  .group_by_u(
                      |x| (x.0, (x.1, x.2)),        // incoming (n,r,s) -> key: n, val: (r,s)
                      |x| x.0,                      //  nonsense about partitioning
                      |k:&U,v:&(U, u32)| (*k,v.0,v.1),  // nonsense about output (ew types)
                      |_, s, t| {
                          t.push(s[0]);             // first record is least dist to least root
                          let mut r = (s[0].0).0;   // track the root we are looking at
                          for &x in s {             // for each available distance ...
                              if (x.0).0 > r {      // ... if it is a new root ...
                                  t.push(x);        // ... push the measurement and ...
                                  r = (x.0).0;      // ... update the tracked root.
                              }
                          }
                      })

     })
}
