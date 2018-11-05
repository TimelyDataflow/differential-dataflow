extern crate rand;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::hash::Hash;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
// use timely::dataflow::operators::*;

use differential_dataflow::input::InputSession;
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

    timely::execute_from_args(std::env::args().skip(3), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        let mut input = InputSession::new();

        let graph = GraphMMap::new(&filename);

        worker.dataflow::<u64,_,_>(|scope| {
            let edges = input.to_collection(scope);
            let edges = edges.map(|(x,y)| (y,x)).concat(&edges);

            _color(&edges)
            // _reach(&edges)
                .map(|(_x,c)| c)
                .consolidate()
                .inspect(|x| println!("{:?}", x));
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut renames = (0 .. graph.nodes() as u32).collect::<Vec<_>>();
        rng.shuffle(&mut renames);

        for node in 0 .. graph.nodes() {
            if node % peers == index {
                for &edge in graph.edges(node) {
                    // input.insert((node as u32, edge));
                    input.insert((renames[node], renames[edge as usize]));
                }
            }
        }

    }).unwrap();
}

fn _color<G: Scope>(edges: &Edges<G>) -> Nodes<G,Option<u32>>
where G::Timestamp: Lattice+Hash+Ord {

    // need some bogus initial values.
    let start = edges.map(|(x,_y)| (x,u32::max_value()))
                     .distinct();

    // repeatedly apply color-picking logic.
    sequence(&start, &edges, |_node, vals| {

        // look for the first absent positive integer.
        // start at 1 in case we ever use NonZero<u32>.

        (1u32 ..)
            .filter(|&i| vals.get(i as usize - 1).map(|x| *x.0) != Some(i))
            .next()
            .unwrap()
    })
}

// fn _reach<G: Scope>(edges: &Edges<G>) -> Nodes<G,Option<bool>>
// where G::Timestamp: Lattice+Hash+Ord {

//     // need some bogus initial values.
//     let start = edges.map(|(x,_y)| (x, x < 10))
//                      .consolidate();

//     // repeatedly apply color-picking logic.
//     sequence(&start, &edges, |_node, vals| {

//         let mut reached = false;
//         let mut iter = vals.iter();
//         while let Some(&(b, _)) = iter.next() {
//             if let &Some(b) = b {
//                 if b { reached = true; }
//             }
//         }
//         reached
//     })
// }

fn sequence<G: Scope, V: Data, F>(state: &Nodes<G, V>, edges: &Edges<G>, logic: F) -> Nodes<G, Option<V>>
where
    G::Timestamp: Lattice+Hash+Ord,
    F: Fn(&Node, &[(&V, isize)])->V+'static
{

    let timer = ::std::time::Instant::now();

    // start iteration with None messages for all.
    state
        .map(|(node, _state)| (node, None))
        .iterate(|new_state| {

            new_state.map(|x| x.1.is_some()).consolidate().inspect(move |x| println!("{:?}\t{:?}", timer.elapsed(), x));

            // immutable content: edges and initial state.
            let edges = edges.enter(&new_state.scope());
            let old_state = state.enter(&new_state.scope());
                                 // .map(|x| (x.0, Some(x.1)));

            // break edges into forward and reverse directions.
            let forward = edges.filter(|edge| edge.0 < edge.1);
            let reverse = edges.filter(|edge| edge.0 > edge.1);

            // new state goes along forward edges, old state along reverse edges
            let new_messages = new_state.join_map(&forward, |_k,v,d| (*d,v.clone()));

            let incomplete = new_messages.filter(|x| x.1.is_none()).map(|x| x.0).distinct();
            let new_messages = new_messages.filter(|x| x.1.is_some()).map(|x| (x.0, x.1.unwrap()));

            let old_messages = old_state.join_map(&reverse, |_k,v,d| (*d,v.clone()));

            let messages = new_messages.concat(&old_messages).antijoin(&incomplete);

            // // determine who has incoming `None` messages, and suppress all of them.
            // let incomplete = new_messages.filter(|x| x.1.is_none()).map(|x| x.0).distinct();

            // merge messages; suppress computation if not all inputs available yet.
            messages
                // .concat(&old_messages)  // /-- possibly too clever: None if any inputs None.
                // .antijoin(&incomplete)
                .group(move |k, vs, t| t.push((Some(logic(k,vs)),1)))
                .concat(&incomplete.map(|x| (x, None)))
        })
}
