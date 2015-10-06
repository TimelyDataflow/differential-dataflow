extern crate rand;
extern crate time;
extern crate getopts;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::hash::Hash;
use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::collection::LeastUpperBound;
use differential_dataflow::operators::*;
use differential_dataflow::operators::join::JoinUnsigned;
use differential_dataflow::operators::group::GroupUnsigned;

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let filename = std::env::args().nth(1).unwrap();
    let start = time::precise_time_s();

    timely::execute_from_args(std::env::args().skip(1), move |computation| {

        let peers = computation.peers();
        let index = computation.index();

        computation.scoped::<u64,_,_>(|scope| {

            let graph = GraphMMap::new(&filename);
            let nodes = graph.nodes();
            let edges = (0..nodes)
                .filter(move |node| node % peers == index)      // TODO : below is pretty horrible.
                .flat_map(move |node| {
                    let vec = graph.edges(node).to_vec();
                    vec.into_iter().map(move |edge| ((node as u32, edge),1))
                })
                .to_stream(scope);

            connected_components(&edges);
        });
    });

    println!("{}: done", time::precise_time_s() - start);
}

fn connected_components<G: Scope>(edges: &Stream<G, (Edge, i32)>) -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    let nodes = edges.map_in_place(|pair| { let min = std::cmp::min((pair.0).0, (pair.0).1); pair.0 = (min, min); } )
                     .consolidate_by(|x| x.0);

    let edges = edges.map_in_place(|x| x.0 = ((x.0).1, (x.0).0))
                     .concat(&edges);

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
}


fn improve_labels<G: Scope>(labels: &Stream<G, ((Node, Node), i32)>,
                            edges: &Stream<G, (Edge, i32)>,
                            nodes: &Stream<G, ((Node, Node), i32)>)
    -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound {

    labels.join_map_u(&edges, |_k,l,d| (*d,*l))
          .concat(&nodes)
          .group_u(|_, s, t| { t.push((*s.peek().unwrap().0, 1)); } )
}
