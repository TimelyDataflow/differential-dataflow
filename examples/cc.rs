extern crate rand;
extern crate time;
extern crate getopts;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::hash::Hash;
use timely::construction::*;
use timely::construction::operators::*;
use timely::communication::Communicator;

use differential_dataflow::collection_trace::lookup::UnsignedInt;
use differential_dataflow::collection_trace::LeastUpperBound;
use differential_dataflow::operators::*;

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    timely::execute(std::env::args(), |computation| {
        let start = time::precise_time_s();
        let mut input = computation.subcomputation::<u64,_,_>(|builder| {
            let (input, mut edges) = builder.new_input();
            edges = connected_components(&edges);
            edges.inspect_batch(move |t, x| {
                println!("{}s:\tepoch {:?}: {:?} changes", time::precise_time_s() - start, t, x.len())
            });

            input
        });

        let graph = GraphMMap::new("/Users/mcsherry/Projects/Datasets/twitter-dedup");

        {
            let mut sent = 0;
            for node in 0..graph.nodes() {
                if node as u64 % computation.peers() == computation.index() {
                    let edges = graph.edges(node);
                    for dest in edges {
                        if node % 2 == 0 && *dest % 2 == 0 {
                            sent += 1;
                            input.give(((node as u32, *dest), 1));
                            if sent % 1_000_000 == 0 {
                                computation.step();
                            }
                        }
                    }
                }
            }

            println!("{}: loaded {} edges", time::precise_time_s() - start, sent);
        }

        input.close();

        while computation.step() { }
        computation.step(); // shut down
    });
}

fn connected_components<G: GraphBuilder>(edges: &Stream<G, (Edge, i32)>) -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    let nodes = edges.map_in_place(|&mut ((ref mut x, ref mut y), _)| { *x = std::cmp::min(*x,*y); *y = *x; } )
                     .consolidate(|x| x.0);

    let edges = edges.map(|((x,y),w)| ((y,x),w)).concat(&edges);

    reachability(&edges, &nodes)
}

fn reachability<G: GraphBuilder>(edges: &Stream<G, (Edge, i32)>, nodes: &Stream<G, ((Node, Node), i32)>)
    -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(u32::max_value(), |x| x.0, |inner| {
             let edges = inner.builder().enter(&edges);
             let nodes = inner.builder().enter_at(&nodes, |r| 256 * (64 - (r.0).0.as_u64().leading_zeros() as u32 ));

             improve_labels(inner, &edges, &nodes)
         })
         .consolidate(|x| x.0)
}


fn improve_labels<G: GraphBuilder>(labels: &Stream<G, ((Node, Node), i32)>,
                                   edges: &Stream<G, (Edge, i32)>,
                                   nodes: &Stream<G, ((Node, Node), i32)>)
    -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound {

    labels.join_u(&edges, |l| l, |e| e, |_k,l,d| (*d,*l))
          .concat(&nodes)
          .group_by_u(|x| x, |k,v| (*k,*v), |_, s, t| { t.push((*s.peek().unwrap().0, 1)); } )
}
