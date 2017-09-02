extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::hash::Hash;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::{Collection, Data};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::*;

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);

type Nodes<G,V> = Collection<G, (Node, V)>;
type Edges<G> = Collection<G, Edge>;

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();

    timely::execute_from_args(std::env::args().skip(1), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        // What you might do if you used GraphMMap:
        let graph = GraphMMap::new(&filename);
        // let nodes = graph.nodes();
        let nodes = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
        let edges = (0..nodes).filter(move |node| node % peers == index)
                              .flat_map(move |node| {
                                  let vec = graph.edges(node).to_vec();
                                  vec.into_iter().map(move |edge| ((node as u32, edge), Default::default(), 1))
                              });

        // let edges = vec![((0,1),1),((1,2),1),((0,2),1)].into_iter();

        worker.dataflow::<u64,_,_>(|scope| {
            let edges = Collection::new(edges.to_stream(scope));
            let edges = edges.map(|(x,y)| (y,x)).concat(&edges);

            // _color(&edges)
            _reach(&edges).map(|(_x,c)| c).consolidate().inspect(|x| println!("{:?}", x));
        });

    }).unwrap();
}

fn _color<G: Scope>(edges: &Edges<G>) -> Nodes<G,Option<u32>> 
where G::Timestamp: Lattice+Hash+Ord {

    // need some bogus initial values.
    let start = edges.map(|(x,_y)| (x,u32::max_value()))
                     .consolidate();

    // repeatedly apply color-picking logic.
    sequence(&start, &edges, |_node, vals| {

        // look for the first absent positive integer.
        // start at 1 in case we ever use NonZero<u32>.

        let mut index = 0;
        while index < vals.len() && vals[index].0.unwrap() == (index as u32) + 1 {
          index += 1;
        }

        (index as u32) + 1
    })
}

fn _reach<G: Scope>(edges: &Edges<G>) -> Nodes<G,Option<bool>> 
where G::Timestamp: Lattice+Hash+Ord {

    // need some bogus initial values.
    let start = edges.map(|(x,_y)| (x, x < 10))
                     .consolidate();

    // repeatedly apply color-picking logic.
    sequence(&start, &edges, |_node, vals| {

        let mut reached = false;
        let mut iter = vals.iter();
        while let Some(&(b, _)) = iter.next() {
            if let &Some(b) = b {
                if b { reached = true; }
            }
        }
        reached
    })
}

fn sequence<G: Scope, V: Data, F>(state: &Nodes<G, V>, edges: &Edges<G>, logic: F) -> Nodes<G, Option<V>>
where G::Timestamp: Lattice+Hash+Ord,
      F: Fn(&Node, &[(&Option<V>, isize)])->V+'static {

        // start iteration with None messages for all.
        state.map(|(node, _state)| (node, None))
             .iterate(|new_state| {

                let edges = edges.enter(&new_state.scope());
                let old_state = state.enter(&new_state.scope())
                                     .map(|x| (x.0, Some(x.1)));

                // break edges into forward and reverse directions.
                let forward = edges.filter(|edge| edge.0 < edge.1); 
                let reverse = edges.filter(|edge| edge.0 > edge.1); 

                // new state goes along forward edges, old state along reverse edges
                let new_messages = new_state.join_map(&forward, |_k,v,d| (*d,v.clone()));
                let old_messages = old_state.join_map(&reverse, |_k,v,d| (*d,v.clone()));

                // merge messages; apply logic if no None messages remain.
                let result = new_messages.concat(&old_messages)
                                         .group(move |k, vs, t| {
                                             if vs[0].0.is_some() {
                                                 t.push((Some(logic(k, vs)), 1));
                                             }
                                             else {
                                                 t.push((None, 1));
                                             }
                                         });

                result.inner.count().inspect_batch(|t,xs| println!("count[{:?}]:\t{:?}", t, xs));
                result
             })
}
