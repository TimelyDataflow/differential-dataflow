extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::Communicator;

use differential_dataflow::operators::*;

type Node = u32;
type Edge = (Node, Node);

fn main() {
    timely::initialize(std::env::args(), |communicator| {
        let start = time::precise_time_s();
        let mut computation = GraphRoot::new(communicator);

        // define BFS dataflow; return handles to roots and edges inputs
        let mut graph = computation.subcomputation(|builder| {
            let (input, edges) = builder.new_input();
            edges.consolidate(|x: &Edge| x.0)
                 .inspect_batch(move |t,b|
                     println!("epoch: {:?}, length: {}, processing: {}",
                        t, b.len(), (time::precise_time_s() - start) - (t.inner as f64))
                 );
            input
        });

        let nodes = 1_000u32; // the u32 helps type inference understand what nodes are

        let seed: &[_] = &[1, 2, 3, computation.index() as usize];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        graph.send_at(0, (0..100).map(|_| ((rng1.gen_range(0, nodes),
                                            rng1.gen_range(0, nodes)), 1)));

        // repeatedly change edges
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions
        let mut round = 0 as u32;
        while computation.step() {
            // once each full second ticks, change an edge
            if time::precise_time_s() - start >= round as f64 {
                // add edges using prior rng; remove edges using fresh rng with the same seed
                let changes = vec![((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1),
                                   ((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1)];
                graph.send_at(round, changes.into_iter());
                graph.advance_to(round + 1);
                round += 1;
            }
        }

        graph.close();                  // seal the source of edges
        while computation.step() { }    // wind down the computation
        println!("done!");
    });
}
