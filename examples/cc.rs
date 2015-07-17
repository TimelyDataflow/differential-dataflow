extern crate rand;
extern crate time;
extern crate getopts;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::hash::Hash;
use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::Communicator;
use timely::drain::DrainExt;

use differential_dataflow::collection_trace::lookup::UnsignedInt;
use differential_dataflow::collection_trace::LeastUpperBound;
use differential_dataflow::operators::*;

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    timely::initialize(std::env::args(), |communicator| {

        let start = time::precise_time_s();
        let mut computation = GraphRoot::new(communicator);

        // define the computation
        let mut input = computation.subcomputation(|builder| {
            let (input, mut edges) = builder.new_input();
            edges = connected_components(&edges);
            edges.inspect_batch(move |t, x| {
                println!("{}s:\tepoch {:?}: {:?} changes", time::precise_time_s() - start, t, x.len())
            });

            input
        });

        let graph = GraphMMap::new("/Users/mcsherry/Projects/Datasets/twitter-dedup");

        let mut sent = 0;
        let mut buffer = Vec::new();
        for node in 0..graph.nodes() {
            if (node % 2) == 0 && ((node as u64 / 2) % computation.peers() == computation.index()) {
                let edges = graph.edges(node);
                for dest in edges {
                    if (*dest % 2) == 0 {
                        buffer.push(((node as u32 / 2, *dest as u32 / 2), 1));
                        if buffer.len() > 400000 {
                            sent += buffer.len();
                            input.send_at(0, buffer.drain_temp());
                            computation.step();
                        }
                    }
                }
            }
        }

        sent += buffer.len();
        input.send_at(0, buffer.drain_temp());

        println!("{}: loaded {} edges", time::precise_time_s() - start, sent);


        input.close();

        while computation.step() { }
        computation.step(); // shut down
    });
}

fn connected_components<G: GraphBuilder>(edges: &Stream<G, (Edge, i32)>) -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    let nodes = edges.map(|((x,y),w)| (std::cmp::min(x,y), w))
                     .consolidate(|&x| x);

    let edges = edges.map(|((x,y),w)| ((y,x),w)).concat(&edges);

    reachability(&edges, &nodes)
}

fn reachability<G: GraphBuilder>(edges: &Stream<G, (Edge, i32)>, nodes: &Stream<G, (Node, i32)>)
    -> Stream<G, ((Node, Node), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(u32::max_value(), |x| x.0, |inner| {
             let edges = inner.builder().enter(&edges);
             let nodes = inner.builder().enter_at(&nodes, |r| 256 * (64 - r.0.as_u64().leading_zeros() as u32 ))
                                        .map(|(x,w)| ((x,x),w));

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
