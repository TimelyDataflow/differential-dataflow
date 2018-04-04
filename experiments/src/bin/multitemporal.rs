extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

use timely::dataflow::operators::unordered_input::UnorderedInput;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::*;

fn main() {

    let nodes: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let rounds: usize = std::env::args().nth(4).unwrap().parse().unwrap();

    timely::execute_from_args(std::env::args().skip(5), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let (mut root_input, root_cap, mut edge_input, mut edge_cap, probe) =
        worker.dataflow(|scope| {

            let ((root_input, root_cap), roots) = scope.new_unordered_input();
            let ((edge_input, edge_cap), edges) = scope.new_unordered_input();

            let roots = roots.as_collection();
            let edges = edges.as_collection()
                             // .inspect(|x| println!("edge: {:?}", x))
                             ;

            let probe =
            roots.iterate(|inner| {

                let edges = edges.enter(&inner.scope());
                let roots = roots.enter(&inner.scope());

                edges
                    .semijoin(&inner)
                    .map(|(_s,d)| d)
                    .concat(&roots)
                    .distinct()
            })
            .map(|_| ())
            .consolidate()
            .inspect(|x| println!("{:?}\tchanges: {:?}", x.1, x.2))
            .probe();

            (root_input, root_cap, edge_input, edge_cap, probe)
        });

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        let worker_edges = edges / peers + if index < edges % peers { 1 } else { 0 };
        let worker_batch = batch / peers + if index < batch % peers { 1 } else { 0 };

        // load initial root.
        root_input.session(root_cap).give((0, RootTimestamp::new(Product::new(0, 0)), 1));

        // load initial edges
        {
            let mut session = edge_input.session(edge_cap.clone());
            for _ in 0 .. worker_edges {
                session.give(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), RootTimestamp::new(Product::new(0, 0)), 1));
            }
        }

        let edge_cap_next = edge_cap.delayed(&RootTimestamp::new(Product::new(1, 0)));

        edge_cap.downgrade(&RootTimestamp::new(Product::new(0, 1)));
        while probe.less_than(edge_cap.time()) {
            worker.step();
        }

        for round in 1 .. rounds {
            {
                let mut session = edge_input.session(edge_cap.clone());
                for _ in 0 .. worker_batch {
                    session.give(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), RootTimestamp::new(Product::new(0, round)), 1));
                    session.give(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)), RootTimestamp::new(Product::new(0, round)),-1));
                }
            }
            edge_cap.downgrade(&RootTimestamp::new(Product::new(0, round+1)));
            while probe.less_than(edge_cap.time()) {
                worker.step();
            }
        }

        edge_cap = edge_cap_next;
        let edge_cap_next = edge_cap.delayed(&RootTimestamp::new(Product::new(2, 0)));

        {
            let mut session = edge_input.session(edge_cap.clone());
            for _ in 0 .. worker_batch {
                session.give(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), RootTimestamp::new(Product::new(1, 0)), 1));
            }
        }

        edge_cap.downgrade(&RootTimestamp::new(Product::new(1, rounds+1)));
        while probe.less_than(edge_cap.time()) {
            worker.step();
        }

        edge_cap = edge_cap_next;
        let edge_cap_next = edge_cap.delayed(&RootTimestamp::new(Product::new(3, 0)));

        {
            let mut session = edge_input.session(edge_cap.clone());
            for _ in 0 .. worker_batch {
                session.give(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), RootTimestamp::new(Product::new(2, 3)), 1));
            }
        }

        edge_cap.downgrade(&RootTimestamp::new(Product::new(2, rounds+1)));
        while probe.less_than(edge_cap.time()) {
            worker.step();
        }


        edge_cap = edge_cap_next;
        let edge_cap_next = edge_cap.delayed(&RootTimestamp::new(Product::new(4, 0)));

        {
            let mut session = edge_input.session(edge_cap.clone());
            for _ in 0 .. worker_batch {
                session.give(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), RootTimestamp::new(Product::new(3, 1)), 1));
            }
        }

        edge_cap.downgrade(&RootTimestamp::new(Product::new(3, rounds+1)));
        while probe.less_than(edge_cap.time()) {
            worker.step();
        }


    }).unwrap();
}