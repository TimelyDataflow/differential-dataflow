extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::{Collection, AsCollection};
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
        
        let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
        let (mut roots, mut graph, probe) = computation.scoped(|scope| {

            let (root_input, roots) = scope.new_input();
            let (edge_input, graph) = scope.new_input();

            let mut result = reach(&graph.as_collection(), &roots.as_collection());

            if !inspect {
                result = result.filter(|_| false);
            }

            let probe = result.map(|(_,l)| l)
                              .consolidate()
                              .inspect(|x| println!("\t{:?}", x))
                              .probe();

            (root_input, edge_input, probe.0)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        let mut names = Vec::new();
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        for _i in 0 .. nodes {
            // names.push(_i as u32);
            names.push(rng.next_u32());
        }

        roots.send((names[1], Default::default(), 1));
        roots.close();

        // println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        if computation.index() == 0 {
            // trickle edges in to dataflow
            for _ in 0..(edges/1000) {
                for _ in 0..1000 {
                    let src = *rng1.choose(&names[..]).unwrap();
                    let dst = *rng1.choose(&names[..]).unwrap();
                    graph.send(((src, dst), Default::default(), 1));
                }
                computation.step();
            }
            for _ in 0.. (edges % 1000) {
                let src = *rng1.choose(&names[..]).unwrap();
                let dst = *rng1.choose(&names[..]).unwrap();
                graph.send(((src, dst), Default::default(), 1));
            }
        }

        println!("loaded; elapsed: {:?}", timer.elapsed());

        graph.advance_to(1);
        computation.step_while(|| probe.lt(graph.time()));

        println!("stable; elapsed: {:?}", timer.elapsed());

        let mut session = differential_dataflow::input::InputSession::from(&mut graph);
        for round in 0 .. rounds {
            for element in 0 .. batch {
                if computation.index() == 0 {
                    session.insert((names[rng1.gen_range(0, nodes as usize)], names[rng1.gen_range(0, nodes as usize)]));
                    session.remove((names[rng2.gen_range(0, nodes as usize)], names[rng2.gen_range(0, nodes as usize)]));
                }
                session.advance_to(2 + round * batch + element);                
            }
            session.flush();

            let timer = ::std::time::Instant::now();
            computation.step_while(|| probe.lt(&session.time()));

            if computation.index() == 0 {
                let elapsed = timer.elapsed();
                println!();
                // println!("{:?}:\t{}", round, elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
            }
        }
        println!("finished; elapsed: {:?}", timer.elapsed());
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

        inner.join_map_u(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .group_u(|_, s, t| t.push((s[0].0, 1)))
     })
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn reach<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, u32)>
where G::Timestamp: Lattice+Ord {

    // repeatedly update minimal distances each node can be reached from each root
    roots.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let roots = roots.enter(&inner.scope());

        edges.semijoin_u(&inner)
             .map(|(_, dst)| dst)
             .concat(&roots)
             .distinct_u()
    })
    .map(|x| (x,0))
}