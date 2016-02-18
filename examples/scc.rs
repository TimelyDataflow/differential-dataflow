extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use std::hash::Hash;
use std::mem;
use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::group::{GroupUnsigned};
use differential_dataflow::operators::join::{JoinUnsigned};
use differential_dataflow::collection::LeastUpperBound;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();

    timely::execute_from_args(std::env::args().skip(3), move |computation| {

        let start = time::precise_time_s();
        let start2 = start.clone();

        let mut input = computation.scoped::<u64,_,_>(|scope| {

            let (input, edges) = scope.new_input();
            let mut edges = Collection::new(edges);

            edges = _trim_and_flip(&edges);
            edges = _trim_and_flip(&edges);
            edges = _strongly_connected(&edges);

            let mut counter = 0;
            edges.consolidate_by(|x| x.0)
                 .inspect_batch(move |t, x| {
                     counter += x.len();
                     println!("{}s:\tobserved at {:?}: {:?} changes",
                              ((time::precise_time_s() - start2)) - (t.inner as f64),
                              t, counter)
                 });

            input
        });

        if computation.index() == 0 {

            println!("determining SCC of {} nodes, {} edges:", nodes, edges);

            let seed: &[_] = &[1, 2, 3, 4];
            let mut rng1: StdRng = SeedableRng::from_seed(seed);
            let mut rng2: StdRng = SeedableRng::from_seed(seed);

            rng1.gen::<f64>();
            rng2.gen::<f64>();

            for index in 0..edges {
                let edge = (rng1.gen_range(0, nodes), rng1.gen_range(0, nodes));
                input.send((edge, 1));
                if (index % (1 << 12)) == 0 {
                    computation.step();
                }
            }

            println!("input ingested after {}", time::precise_time_s() - start);

            // while computation.step() {
            //     if time::precise_time_s() - start >= *input.epoch() as f64 {
            //         let change_count = 1;
            //         for _ in 0..change_count {
            //             input.give(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
            //             input.give(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1));
            //         }
            //         let next_epoch = *input.epoch() + 1;
            //         input.advance_to(next_epoch);
            //     }
            // }
        }

        input.close();
    });
}

fn _trim_and_flip<G: Scope>(graph: &Collection<G, Edge>) -> Collection<G, Edge>
where G::Timestamp: LeastUpperBound {
    graph.iterate(|edges| {
        let inner = graph.enter(&edges.scope())
                         .map_in_place(|x| mem::swap(&mut x.0, &mut x.1));

        edges.map(|(x,_)| x)
             .threshold(|&x| x, |i| (Vec::new(), i), |_,_| 1)
             .map(|x| (x,()))
             .join_map_u(&inner, |&d,_,&s| (s,d))
    })
    .map_in_place(|x| mem::swap(&mut x.0, &mut x.1))
}

fn _strongly_connected<G: Scope>(graph: &Collection<G, Edge>) -> Collection<G, Edge>
where G::Timestamp: LeastUpperBound+Hash {
    graph.iterate(|inner| {
        let edges = graph.enter(&inner.scope());
        let trans = edges.map_in_place(|x| mem::swap(&mut x.0, &mut x.1));
        _trim_edges(&_trim_edges(inner, &edges), &trans)
    })
}

fn _trim_edges<G: Scope>(cycle: &Collection<G, Edge>, edges: &Collection<G, Edge>)
    -> Collection<G, Edge> where G::Timestamp: LeastUpperBound+Hash {

    let nodes = edges.map_in_place(|x| x.0 = x.1)
                     .consolidate_by(|&x| x.0);

    let labels = _reachability(&cycle, &nodes);

    edges.join_map_u(&labels, |&e1,&e2,&l1| (e2,(e1,l1)))
         .join_map_u(&labels, |&e2,&(e1,l1),&l2| ((e1,e2),(l1,l2)))
         .filter(|&(_,(l1,l2))| l1 == l2)
         .map(|((x1,x2),_)| (x2,x1))
}

fn _reachability<G: Scope>(edges: &Collection<G, Edge>, nodes: &Collection<G, (Node, Node)>) -> Collection<G, Edge>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(|inner| {
             let edges = edges.enter(&inner.scope());
             let nodes = nodes.enter_at(&inner.scope(), |r| 256 * (64 - ((r.0).0 as u64).leading_zeros() as u64));

             inner.join_map_u(&edges, |_k,l,d| (*d,*l))
                  .concat(&nodes)
                  .group_u(|_, mut s, t| t.push((*s.peek().unwrap().0, 1)))
         })
}