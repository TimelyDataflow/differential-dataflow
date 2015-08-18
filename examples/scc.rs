extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use std::hash::Hash;
use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::collection_trace::LeastUpperBound;
use differential_dataflow::operators::*;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();

    timely::execute_from_args(std::env::args().skip(3), move |computation| {

        let start = time::precise_time_s();
        let start2 = start.clone();

        let mut input = computation.scoped::<u64,_,_>(|scope| {

            let (input, mut edges) = scope.new_input();

            edges = _trim_and_flip(&edges);
            edges = _trim_and_flip(&edges);
            edges = _strongly_connected(&edges);

            let mut counter = 0;
            edges.consolidate(|x| x.0)
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
                // println!("edge: {:?}", edge);
                input.send((edge, 1));
                if (index % (1 << 16)) == 0 {
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

        while computation.step() { }
        computation.step(); // shut down
    });
}

fn _trim_and_flip<G: Scope>(graph: &Stream<G, (Edge, i32)>) -> Stream<G, (Edge, i32)>
where G::Timestamp: LeastUpperBound {
        graph
            .iterate(u32::max_value(), |x|x.0, |edges| {
                let inner = edges.scope().enter(&graph);
                edges.map(|((x,_),w)| (x,w))
                     .group_by_u(|x|(x,()), |&x,_| x, |_,_,target| target.push(((),1)))
                     .join_u(&inner, |x| (x,()), |(s,d)| (d,s), |&d,_,&s| (s,d))
            })
            .consolidate(|x| x.0)
            .map(|((x,y),w)| ((y,x),w))
}

fn _strongly_connected<G: Scope>(graph: &Stream<G, (Edge, i32)>) -> Stream<G, (Edge, i32)>
where G::Timestamp: LeastUpperBound+Hash {
    graph.iterate(u32::max_value(), |x| x.0, |inner| {
        let trans = inner.scope().enter(&graph).map(|((x,y),w)| ((y,x),w));
        let edges = inner.scope().enter(&graph);

        _trim_edges(&_trim_edges(inner, &edges), &trans)
    })
}

fn _trim_edges<G: Scope>(cycle: &Stream<G, (Edge, i32)>, edges: &Stream<G, (Edge, i32)>)
    -> Stream<G, (Edge, i32)> where G::Timestamp: LeastUpperBound+Hash {

    let nodes = edges.map(|((_,y),w)| (y,w)).consolidate(|&x| x);

    let labels = _reachability(&cycle, &nodes);

    edges.join_u(&labels, |e| e, |l| l, |&e1,&e2,&l1| (e2,(e1,l1)))
         .join_u(&labels, |e| e, |l| l, |&e2,&(e1,l1),&l2| ((e1,e2),(l1,l2)))
         .consolidate(|x| (x.0).0)
         .filter(|&((_,(l1,l2)), _)| l1 == l2)
         .map(|(((x1,x2),_),d)| ((x2,x1),d))
}

fn _reachability<G: Scope>(edges: &Stream<G, (Edge, i32)>, nodes: &Stream<G, (Node, i32)>) -> Stream<G, (Edge, i32)>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(u32::max_value(), |x| x.0, |inner| {
             let edges = inner.scope().enter(&edges);
             let nodes = inner.scope().enter_at(&nodes, |r| 256 * (64 - (r.0 as u64).leading_zeros() as u32))
                                        .map(|(x,w)| ((x,x),w));

             _improve_labels(inner, &edges, &nodes)
         })
         .consolidate(|x| x.0)
}

fn _improve_labels<G: Scope>(labels: &Stream<G, ((Node, Node), i32)>,
                                   edges: &Stream<G, (Edge, i32)>,
                                   nodes: &Stream<G, ((Node, Node), i32)>)
    -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound {

    labels.join_u(&edges, |l| l, |e| e, |_k,l,d| (*d,*l))
          .concat(&nodes)
        //   .inspect_batch(|t, b| println!("all labels at {:?}: {:?}", t, b))
          .group_by_u(|x| x, |k,v| (*k,*v), |_, s, t| { t.push((*s.peek().unwrap().0, 1)); } )
        //   .inspect_batch(|t, b| println!("min labels at {:?}: {:?}", t, b))
}
