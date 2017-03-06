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

            let roots = vec![(1,Default::default(),1)].into_iter().to_stream(scope);
            let (edge_input, graph) = scope.new_input();

            let mut result = bfs(&Collection::new(graph.clone()), &Collection::new(roots.clone()));

            if !inspect {
                result = result.filter(|_| false);
            }

            let probe = result.map(|(_,l)| l)
                              .consolidate_by(|&x| x)
                              .inspect(|x| println!("\t{:?}", x))
                              .probe();

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
                    graph.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), Default::default(), 1));
                }
                computation.step();
            }
            for _ in 0.. (edges % 1000) {
                graph.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), Default::default(), 1));
            }
        }

        // println!("loaded; elapsed: {:?}", timer.elapsed());

        graph.advance_to(1);
        computation.step_while(|| probe.lt(graph.time()));

        // println!("stable; elapsed: {:?}", timer.elapsed());

        if batch > 0 {
            for _wave in 0 .. rounds {
                if computation.index() == 0 {
                    let mut session = differential_dataflow::input::InputSession::from(&mut graph);
                    for _ in 0 .. batch {
                        session.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                        session.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                        let &round = session.epoch();
                        session.advance_to(round + 1);
                    }
                }

                let timer = ::std::time::Instant::now();
                computation.step_while(|| probe.lt(&graph.time()));

                if computation.index() == 0 {
                    let elapsed = timer.elapsed();
                    println!("{}", elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
                    // println!()
                    // println!("wave {}: avg {:?}", wave, timer.elapsed() / (batch as u32));
                }
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

        let edges = edges.enter(&inner.scope());//.inspect(|x| println!("edge: {:?}", x));
        let nodes = nodes.enter(&inner.scope());//.inspect(|x| println!("node: {:?}", x));

        inner.join_map(&edges, |_k,l,d| (*d, l+1))//.inspect(|x| println!("join: {:?}", x))
             // .inspect_batch(|t,xs| println!("\njoin:  {:?}:\t{:?}", t, xs))
             .concat(&nodes)
             .group(|_, s, t| t.push((s[0].0, 1)))//.inspect(|x| println!("grup: {:?}", x))
             // .inspect_batch(|t,xs| println!("\ngroup: {:?}:\t{:?}", t, xs))
     })
}