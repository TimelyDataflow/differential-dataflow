extern crate rand;
extern crate getopts;
extern crate timely;
extern crate timely_sort;
extern crate graph_map;
extern crate differential_dataflow;
extern crate vec_map;

use std::time::Instant;
use std::hash::Hash;
use std::mem;

use vec_map::VecMap;

use rand::{Rng, SeedableRng, StdRng};

use timely_sort::Unsigned;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::{Collection, AsCollection};
use differential_dataflow::operators::*;
use differential_dataflow::operators::join::JoinArranged;
use differential_dataflow::collection::LeastUpperBound;

use graph_map::GraphMMap;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    if std::env::args().find(|x| x == "pymk").is_some() {
        test_pymk();
    }
    else {
        test_graph();
    }

}

fn test_graph() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();

    let reach = std::env::args().find(|x| x == "reach").is_some();
    let cc = std::env::args().find(|x| x == "cc").is_some();
    let bfs = std::env::args().find(|x| x == "bfs").is_some();

    timely::execute_from_args(std::env::args().skip(1), move |computation| {

        let peers = computation.peers();
        let index = computation.index();

        // // What you might do if you used GraphMMap:
        let graph = GraphMMap::new(&filename);

        let mut input = computation.scoped::<u64,_,_>(|scope| {

            let (input, stream) = scope.new_input();

            let roots = Collection::new(vec![(1,1)].into_iter().to_stream(scope));
            let graph = Collection::new(stream);

            if reach { _reach(&graph, &roots); }
            else if cc { _connected_components(&graph); }
            else if bfs { _bfs(&graph, &roots); } 
            else {
                panic!("must specify one of 'reach', 'cc', 'bfs'.");
            }

            input
        });
        

        let timer = Instant::now();

        for node in 0..graph.nodes() {
            if node % peers == index {
                for &edge in graph.edges(node) {
                    input.send(((node as u32, edge), 1));
                }
            }
        }

        input.close();
        while computation.step() { }

        println!("loaded: {:?}", timer.elapsed());


    }).unwrap();
}

fn _test_trees() {

    let depth: usize = std::env::args().nth(1).unwrap().parse().unwrap();

    let timer = Instant::now();

    timely::execute_from_args(std::env::args().skip(1), move |computation| {

        computation.scoped::<(),_,_>(|scope| {

            let index = scope.index();
            let peers = scope.peers();

            let mut tree = vec![];
            let mut sum = 0usize;
            let mut cur_nodes = 0;
            let mut tot_nodes = 1;
            for _level in 0 .. depth {
                for node in cur_nodes .. tot_nodes {
                    cur_nodes += 1;
                    for _edge in 0 .. 3 {
                        if node % peers == index {
                            tree.push(((node as u32, tot_nodes as u32),1));
                        }
                        tot_nodes += 1;
                    }
                }
                sum += (tot_nodes - cur_nodes) * (tot_nodes - cur_nodes - 1) / 2;
            }

            println!("produced depth {} tree, {} nodes, {} edges", depth, tot_nodes, tree.len());
            println!("#(sg): {}", sum);

            let edges = tree.into_iter()
                            .to_stream(scope)
                            .as_collection();

            let dists = edges.map(|(p,c)| (p,(c,1)));

            dists.iterate(|tc| {
                    let edges = edges.enter(&tc.scope());
                    let dists = dists.enter(&tc.scope());
                    tc.map(|(a,(p,d))| (p,(a,d)))
                      .join_map_u(&edges, |_p,&(a,d),&c| (a,(c,d+1)))
                      .concat(&dists)
                      .group_u(|_,s,t| for (&x,_) in s { t.push((x,1)) })
                });
        });
    }).unwrap();

    println!("Elapsed: {:?}", timer.elapsed());
}

fn test_pymk() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();

    timely::execute_from_args(std::env::args().skip(1), move |computation| {

        let peers = computation.peers();
        let index = computation.index();

        // // What you might do if you used GraphMMap:
        let graph = GraphMMap::new(&filename);

        let (mut input, mut roots, probe) = computation.scoped::<u64,_,_>(|scope| {

            let (input, stream1) = scope.new_input();
            let (rootz, stream2) = scope.new_input();

            let graph = stream1.as_collection();
            let roots = stream2.as_collection();

            let graph = graph.map_in_place(|x: &mut (u32, u32)| ::std::mem::swap(&mut x.0, &mut x.1))
                             .concat(&graph);

            let graph = graph.arrange_by_key(|k| k.clone(), |x| (VecMap::new(), x));
            let roots = roots.arrange_by_self(|k: &u32| k.as_u64(), |x| (VecMap::new(), x));

            let probe = 
            graph.join(&roots, |k,v,_| (v.clone(), k.clone()))
                 .arrange_by_key(|k| k.clone(), |x| (VecMap::new(), x))
                 .join(&graph, |_,x,y| (x.clone(), y.clone()))
                 .group_u(|_,s,t| {
                    t.extend(s.map(|(x,y)| (*x,y)));
                    t.sort_by(|x,y| x.1.cmp(&y.1));
                    t.truncate(10);
                 })
                 .probe().0;

            (input, rootz, probe)
        });
        

        let timer = Instant::now();

        for node in 0..graph.nodes() {
            if node % peers == index {
                for &edge in graph.edges(node) {
                    input.send(((node as u32, edge), 1));
                }
            }
        }

        input.advance_to(1);
        roots.advance_to(1);
        while probe.lt(input.time()) { computation.step(); }

        println!("loaded: {:?}", timer.elapsed());

        let mut latencies = Vec::with_capacity(11);

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        for _count in 0..latencies.capacity() {
            let timer = Instant::now();
            if index == 0 {
                roots.send((rng.gen_range(0, graph.nodes() as u32), 1));
            }
            let next = input.epoch() + 1;
            input.advance_to(next);
            roots.advance_to(next);
            while probe.lt(input.time()) { computation.step(); }
            latencies.push(timer.elapsed());
        }

        if index == 0 {
            for lat in &latencies {
                println!("latency: {:?}", lat);
            }
        }

    }).unwrap();
}


// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn _pymk<G: Scope>(edges: &Collection<G, Edge>) -> Collection<G, (Node,Node)>
where G::Timestamp: LeastUpperBound {
    edges.join_map_u(&edges, |_x,&y,&z| (y,z))
         .consolidate_by(|x| x.0)
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn _reach<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, Node>
where G::Timestamp: LeastUpperBound {

    // initialize roots as reaching themselves at distance 0
    // repeatedly update minimal distances each node can be reached from each root
    roots.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = roots.enter(&inner.scope());

        edges.semijoin_u(&inner)
             .map(|(_,d)| d)
             .concat(&nodes)
             .distinct_u()
     })
}


fn _connected_components<G: Scope>(edges: &Collection<G, Edge>) -> Collection<G, (Node, Node)>
where G::Timestamp: LeastUpperBound+Hash {

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
             let nodes = nodes.enter_at(&inner.scope(), |r| 256 * (64 - (r.0).0.leading_zeros() as u64));

            inner.join_map_u(&edges, |_k,l,d| (*d,*l))
                 .concat(&nodes)
                 .group_u(|_, mut s, t| { t.push((*s.peek().unwrap().0, 1)); } )
         })
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn _bfs<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, u32)>
where G::Timestamp: LeastUpperBound {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner.join_map_u(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .group_u(|_, s, t| t.push((*s.peek().unwrap().0, 1)))
     })
}
