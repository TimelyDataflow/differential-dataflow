extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use std::hash::Hash;
use rand::{Rng, SeedableRng, StdRng};

use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::*;

use differential_dataflow::collection_trace::LeastUpperBound;
use differential_dataflow::operators::*;

type Node = u32;
type Edge = (Node, Node);

fn main() {
    let nodes: u32 = std::env::args().skip(1).next().unwrap().parse().unwrap();
    let edges: u32 = std::env::args().skip(2).next().unwrap().parse().unwrap();
    timely::initialize(std::env::args().skip(3), move |communicator| {
        test_dataflow(communicator, nodes, edges);
    });
}

fn test_dataflow<C: Communicator>(communicator: C, nodes: u32, edges: u32) {

    let start = time::precise_time_s();
    let start2 = start.clone();
    let mut computation = GraphRoot::new(communicator);

    let mut input = computation.subcomputation(|builder| {

        let (input, mut edges) = builder.new_input();

        edges = _trim_and_flip(&edges);
        edges = _trim_and_flip(&edges);
        edges = _strongly_connected(&edges);

        edges//.consolidate(|x| x.0, |x| x.0)
             .inspect_batch(move |t, x| {
                 println!("{}s:\tobserved at {:?}: {:?} changes",
                          ((time::precise_time_s() - start2)) - (t.inner as f64),
                          t, x.len())
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

        let mut left = edges;
        while left > 0 {
            let next = if left < 1000 { left } else { 1000 };
            input.send_at(0, (0..next).map(|_| ((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1)));
            computation.step();
            left -= next;
        }

        println!("input ingested after {}", time::precise_time_s() - start);

        // let mut round = 0 as u32;
        // let mut changes = Vec::new();
        // while computation.step() {
        //     if time::precise_time_s() - start >= round as f64 {
        //         let change_count = 1000;
        //         for _ in 0..change_count {
        //             changes.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
        //             changes.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1));
        //         }
        //
        //         input.send_at(round, changes.drain(..));
        //         input.advance_to(round + 1);
        //         round += 1;
        //     }
        // }

    }

    input.close();

    while computation.step() { }
    computation.step(); // shut down
}

fn _trim_and_flip<G: GraphBuilder>(graph: &Stream<G, (Edge, i32)>) -> Stream<G, (Edge, i32)>
where G::Timestamp: LeastUpperBound {
        graph
            .iterate(u32::max_value(), |x|x.0, |edges| {
                let inner = edges.builder().enter(&graph);
                edges.map(|((x,_),w)| (x,w))
                     .group_by_u(|x|(x,()), |&x,_| x, |_,_,target| target.push(((),1)))
                     .join_u(&inner, |x| (x,()), |(s,d)| (d,s), |&d,_,&s| (s,d))
            })
            .consolidate(|x| x.0)
            .map(|((x,y),w)| ((y,x),w))
}

fn _strongly_connected<G: GraphBuilder>(graph: &Stream<G, (Edge, i32)>) -> Stream<G, (Edge, i32)>
where G::Timestamp: LeastUpperBound+Hash {
    graph.iterate(u32::max_value(), |x| x.0, |inner| {
        let trans = inner.builder().enter(&graph).map(|((x,y),w)| ((y,x),w));
        let edges = inner.builder().enter(&graph);

        _trim_edges(&_trim_edges(inner, &edges), &trans)
    })
}

fn _trim_edges<G: GraphBuilder>(cycle: &Stream<G, (Edge, i32)>, edges: &Stream<G, (Edge, i32)>)
    -> Stream<G, (Edge, i32)> where G::Timestamp: LeastUpperBound+Hash {

    let nodes = edges.map(|((_,y),w)| (y,w)).consolidate(|&x| x);

    let labels = _reachability(&cycle, &nodes);

    edges.join_u(&labels, |e| e, |l| l, |&e1,&e2,&l1| (e2,(e1,l1)))
         .join_u(&labels, |e| e, |l| l, |&e2,&(e1,l1),&l2| ((e1,e2),(l1,l2)))
         .consolidate(|x| (x.0).0)
         .filter(|&((_,(l1,l2)), _)| l1 == l2)
         .map(|(((x1,x2),_),d)| ((x2,x1),d))
}

fn _reachability<G: GraphBuilder>(edges: &Stream<G, (Edge, i32)>, nodes: &Stream<G, (Node, i32)>) -> Stream<G, (Edge, i32)>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(u32::max_value(), |x| x.0, |inner| {
             let edges = inner.builder().enter(&edges);
             let nodes = inner.builder().enter_at(&nodes, |r| 256 * (64 - (r.0 as u64).leading_zeros() as u32))
                                        .map(|(x,w)| ((x,x),w));

             _improve_labels(inner, &edges, &nodes)
         })
         .consolidate(|x| x.0)
}

fn _improve_labels<G: GraphBuilder>(labels: &Stream<G, ((Node, Node), i32)>,
                                   edges: &Stream<G, (Edge, i32)>,
                                   nodes: &Stream<G, ((Node, Node), i32)>)
    -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound {

    labels.join_u(&edges, |l| l, |e| e, |_k,l,d| (*d,*l))
          .concat(&nodes)
          .group_by_u(|x| x, |k,v| (*k,*v), |_, s, t| { t.push((*s.peek().unwrap().0, 1)); } )
}

// fn _fancy_reachability<G: GraphBuilder, U: UnsignedInt>(edges: &Stream<G, ((U, U), i32)>, nodes: &Stream<G, (U, i32)>)
//     -> Stream<G, ((U, U), i32)>
// where G::Timestamp: LeastUpperBound+Hash {
//
//     edges.filter(|_| false)
//          .iterate(u32::max_value(), |x| x.0, |x| x.0, |inner| {
//              let edges = inner.builder().enter(&edges);
//              let nodes = inner.builder().enter(&nodes)
//                               .map(|(x,_)| (x,1))
//                               .except(&inner.filter(|&((ref n, ref l),_)| l < n).map(|((n,_),w)| (n,w)))
//                               .delay(|r,t| { let mut t2 = t.clone(); t2.inner = 256 * (64 - r.0.as_u64().leading_zeros() as u32); t2 })
//                               .consolidate(|&x| x, |&x| x)
//                               .map(|(x,w)| ((x,x),w));
//
//              _improve_labels(inner, &edges, &nodes)
//          })
//          .consolidate(|x| x.0, |x| x.0)
// }
