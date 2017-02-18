extern crate rand;
extern crate getopts;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::hash::Hash;
use std::mem;
use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::Collection;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::*;

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();

    timely::execute_from_args(std::env::args().skip(1), move |computation| {

        let peers = computation.peers();
        let index = computation.index();

        // // What you might do if you used GraphMMap:
        let graph = GraphMMap::new(&filename);
        let nodes = graph.nodes();
        let edges = (0..nodes).filter(move |node| node % peers == index)
                              .flat_map(move |node| {
                                  let vec = graph.edges(node).to_vec();
                                  vec.into_iter().map(move |edge| ((node as u32, edge),1))
                              });

        computation.scoped::<(),_,_>(|scope| {
            connected_components(&Collection::new(edges.to_stream(scope)));
        });

    }).unwrap();
}

fn connected_components<G: Scope>(edges: &Collection<G, Edge>) -> Collection<G, (Node, Node)>
where G::Timestamp: Lattice+Hash+Ord {

    // each edge (x,y) means that we need at least a label for the min of x and y.
    let nodes = edges.map_in_place(|pair| {
                        let min = std::cmp::min(pair.0, pair.1);
                        *pair = (min, min);
                     })
                     .consolidate_by(|x| x.0);

    // each edge should exist in both directions.
    let edges = edges.map_in_place(|x| mem::swap(&mut x.0, &mut x.1))
                     .concat(&edges);

    // don't actually use these labels, just grab the type
    nodes.filter(|_| false)
         .iterate(|inner| {
             let edges = edges.enter(&inner.scope());
             let nodes = nodes.enter_at(&inner.scope(), |r| 256 * (64 - (r.0).1.leading_zeros() as u64));

            inner.join_map(&edges, |_k,l,d| (*d,*l))
                 .concat(&nodes)
                 // .consolidate()
                 .group(|_, s, t| { t.push((s[0].0, 1)); } )
         })
}
