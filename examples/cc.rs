extern crate rand;
extern crate time;
extern crate getopts;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::hash::Hash;
use timely::dataflow::*;
use timely::dataflow::operators::*;

// use differential_dataflow::collection_trace::lookup::UnsignedInt;
use differential_dataflow::collection_trace::LeastUpperBound;
use differential_dataflow::operators::*;

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let filename = std::env::args().nth(1).unwrap();

    timely::execute_from_args(std::env::args().skip(1), move |computation| {

        let start = time::precise_time_s();
        let mut input = computation.scoped::<u64,_,_>(|scope| {
            let (input, edges) = scope.new_input();
            connected_components(&edges);
            input
        });

        let graph = GraphMMap::new(&filename);

        {
            let mut sent = 0;
            for node in 0..graph.nodes() {
                if node % computation.peers() == computation.index() {
                    let edges = graph.edges(node);
                    for dest in edges {
                        sent += 1;
                        input.send(((node as u32, *dest), 1));
                        if sent % 1_000_000 == 0 {
                            computation.step();
                        }
                    }
                }
            }

            println!("{}: loaded {} edges", time::precise_time_s() - start, sent);
        }

        input.close();

        while computation.step() { }
        computation.step(); // shut down
        println!("{}: done", time::precise_time_s() - start);

    });
}

fn connected_components<G: Scope>(edges: &Stream<G, (Edge, i32)>) -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    let nodes = edges.map_in_place(|&mut ((ref mut x, ref mut y), _)| { *x = std::cmp::min(*x,*y); *y = *x; } )
                     .consolidate();

    let edges = edges.map(|((x,y),w)| ((y,x),w)).concat(&edges);

    reachability(&edges, &nodes)
}

fn reachability<G: Scope>(edges: &Stream<G, (Edge, i32)>, nodes: &Stream<G, ((Node, Node), i32)>)
    -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(|inner| {
             let edges = inner.scope().enter(&edges);
             let nodes = inner.scope().enter_at(&nodes, |r| 256 * (64 - (r.0).0.leading_zeros() as u64));

             improve_labels(inner, &edges, &nodes)
         })
         .consolidate()
}


fn improve_labels<G: Scope>(labels: &Stream<G, ((Node, Node), i32)>,
                            edges: &Stream<G, (Edge, i32)>,
                            nodes: &Stream<G, ((Node, Node), i32)>)
    -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound {

    labels.join_by_u(&edges, |l| l, |e| e, |_k,l,d| (*d,*l))
          .concat(&nodes)
          .group_u(|_, s, t| { t.push((*s.peek().unwrap().0, 1)); } )
}
