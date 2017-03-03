extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: u32 = std::env::args().nth(3).unwrap().parse().unwrap();
    let rounds: u32 = std::env::args().nth(4).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().nth(5).unwrap() == "inspect";

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(6), move |computation| {
        
        // let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let (mut graph, probe) = computation.scoped(|scope| {

            let roots = vec![(1,1)].into_iter().to_stream(scope);
            let (edge_input, graph) = scope.new_input();

            let mut result = bfs(&Collection::new(graph.clone()), &Collection::new(roots.clone()));

            if !inspect {
                result = result.filter(|_| false);
            }

            let probe = result.map(|(_,l)| l)
                              .consolidate_by(|&x| x)
                              .inspect(|x| println!("\t{:?}", x)).probe()
                              ;

            (edge_input, probe.0)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        // println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        if computation.index() == 0 {
            // trickle edges in to dataflow
            for _ in 0..(edges/1000) {
                for _ in 0..1000 {
                    graph.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
                }
                computation.step();
            }
            for _ in 0.. (edges % 1000) {
                graph.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
            }
        }

        // println!("loaded; elapsed: {:?}", timer.elapsed());

        graph.advance_to(1);
        computation.step_while(|| probe.lt(graph.time()));

        // println!("stable; elapsed: {:?}", timer.elapsed());

        if batch > 0 {
            let mut changes = Vec::new();
            for _wave in 0 .. rounds {
                if computation.index() == 0 {
                    for _ in 0..batch {
                        changes.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
                        changes.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1));
                    }
                }

                // let timer = ::std::time::Instant::now();
                let round = *graph.epoch();
                if computation.index() == 0 {
                    while let Some(change) = changes.pop() {
                        graph.send(change);
                    }
                }
                graph.advance_to(round + 1);
                computation.step_while(|| probe.lt(&graph.time()));

                // if computation.index() == 0 {
                //     let elapsed = timer.elapsed();
                //     println!("{}", elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
                //     // println!()
                //     // println!("wave {}: avg {:?}", wave, timer.elapsed() / (batch as u32));
                // }
            }
        }

        // println!("finished; elapsed: {:?}", timer.elapsed());

    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, u32)>
where G::Timestamp: Lattice+Ord {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner.join_map(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .group(|_, s, t| t.push((s[0].0, 1)))
     })
}

// // Experimental implementation using arrangement to share the output of group_u with the input to join_map_u.
// // returns pairs (n, s) indicating node n can be reached from a root in s steps.
// fn _bfs2<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, u32)>
// where G::Timestamp: Lattice {

//     // initialize roots as reaching themselves at distance 0
//     let nodes = roots.map(|x| (x, 0));

//     // repeatedly update minimal distances each node can be reached from each root
//     nodes.scope().scoped(|scope| {

//         let edges = edges.enter(scope);
//         let nodes2 = nodes.enter(scope);

//         let variable = Variable::from(nodes.enter(scope));

//         let arranged = variable.concat(&nodes2)
//                                .arrange_by_key(|k| k.as_u64(), |x| (VecMap::new(), x))
//                                .group(|k| k.as_u64(), |x| (VecMap::new(), x), |_, s, t| t.push((*s.peek().unwrap().0, 1)));

//         let result = arranged.as_collection().leave();

//         variable.set(&arranged.join(&edges.arrange_by_key(|k| k.as_u64(), |x| (VecMap::new(), x)), |_k,l,d| (*d, l+1)));

//         result
//     })
// }
