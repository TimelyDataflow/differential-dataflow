extern crate rand;
extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::time::Instant;
// use std::hash::Hash;
// use std::mem;

// use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::scopes::ScopeParent;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::lattice::Lattice;

use graph_map::GraphMMap;

use differential_dataflow::trace::implementations::ord::OrdValSpine as DefaultValTrace;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::Arranged;

type Arrange<G, K, V, R> = Arranged<G, K, V, R, TraceAgent<K, V, <G as ScopeParent>::Timestamp, R, DefaultValTrace<K, V, <G as ScopeParent>::Timestamp, R>>>;

type Node = u32;
// type Edge = (Node, Node);

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();
    let program = std::env::args().nth(2).unwrap();

    timely::execute_from_args(std::env::args().skip(3), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        let (mut input, _probe) = worker.dataflow::<(),_,_>(|scope| {

            let (input, graph) = scope.new_collection();
            // let (rootz, query) = scope.new_collection();

            // each edge should exist in both directions.
            let graph = graph.arrange_by_key();

            let probe = match program.as_str() {
                "tc"    => _tc(&graph).probe(),
                "sg"    => _sg(&graph).probe(),
                // "reach" => _reach(&graph, &query).probe(),
                // "cc"    => _connected_components(&graph).probe(),
                // "bfs"   => _bfs(&graph, &query).probe(),
                // "pymk"  => _pymk(&graph, &query, 10).probe(),
                _       => panic!("must specify one of 'tc', 'sg', reach', 'cc', 'bfs', 'pymk'.")
            };

            (input, probe)
        });


        let timer = Instant::now();

        let mut nodes = 0;
        if filename.ends_with(".txt") {

            use std::io::{BufReader, BufRead};
            use std::fs::File;

            let file = BufReader::new(File::open(filename.clone()).unwrap());
            for (count, readline) in file.lines().enumerate() {
                let line = readline.ok().expect("read error");
                if count % peers == index && !line.starts_with('#') {
                    let mut elts = line[..].split_whitespace();
                    let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
                    let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");
                    if nodes < src { nodes = src; }
                    if nodes < dst { nodes = dst; }
                    input.insert((src, dst));
                }
            }
        }
        else {
            // What you might do if you used GraphMMap:
            let graph = GraphMMap::new(&filename);

            // start loading up the graph
            for node in 0..graph.nodes() {
                if node % peers == index {
                    for &edge in graph.edges(node) {
                        input.insert((node as u32, edge));
                    }
                }
            }
            // nodes = graph.nodes() as u32;
        }

        println!("{:?}\tData ingested", timer.elapsed());

        // // run until graph is loaded
        // input.advance_to(1); input.flush();
        // query.advance_to(1); query.flush();

        // input.close();

        // worker.step_while(|| probe.less_than(query.time()));

        // if index == 0 {
        //     println!("{:?}\tData indexed", timer.elapsed());
        // }

        // // conduct latencies.capacity() measurements.
        // let mut latencies = Vec::with_capacity(11);

        // let seed: &[_] = &[1, 2, 3, 4];
        // let mut rng: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        // for _count in 0..latencies.capacity() {
        //     let timer = Instant::now();
        //     if index == 0 {
        //         query.insert((rng.gen_range(0, nodes)));
        //     }
        //     let next = query.epoch() + 1;
        //     // input.advance_to(next); input.flush();
        //     query.advance_to(next); query.flush();
        //     while probe.less_than(query.time()) { worker.step(); }
        //     latencies.push(timer.elapsed());
        // }

        // if index == 0 {
        //     for lat in &latencies {
        //         println!("latency: {:?}", lat);
        //     }
        // }

    }).unwrap();
}

// // returns pairs (root, friend-of-friend) for the top-k friends of friends by count.
// fn _pymk<G: Scope>(edges: &Arrange<G, Node, Node, isize>, query: &Collection<G, Node>, k: usize) -> Collection<G, (Node,Node)>
// where G::Timestamp: Lattice+Ord {

//     let query = query.arrange_by_self();

//     // restrict attention to edges from query nodes
//     query.join_core(&edges, |k,_,v| Some((v.clone(), k.clone())))
//          .join_core(&edges, |_,x,y| Some((x.clone(), y.clone())))
//          // the next thing is the "topk" worker. sorry!
//          .group(move |_,s,t| {
//              t.extend(s.iter().map(|&(x,y)| (*x,y)));   // propose all inputs as outputs
//              t.sort_by(|x,y| (-x.1).cmp(&(-y.1)));      // sort by negative count (large numbers first)
//              t.truncate(k)                              // keep at most k of these
//          })
//  }

// // returns pairs (n, s) indicating node n can be reached from a root in s steps.
// fn _reach<G: Scope>(edges: &Arrange<G, Node, Node, isize>, query: &Collection<G, Node>) -> Collection<G, Node>
// where G::Timestamp: Lattice+Ord {

//     // initialize query as reaching themselves at distance 0
//     // repeatedly update minimal distances each node can be reached from each root
//     query.iterate(|inner| {

//         let edges = edges.enter(&inner.scope());
//         let nodes = query.enter(&inner.scope());

//         // edges from active sources activate their destinations
//         edges.semijoin(&inner)
//              .map(|(_,d)| d)
//              .concat(&nodes)
//              .distinct()
//     })
// }


// // returns pairs (node, label) indicating the connected component containing each node
// fn _connected_components<G: Scope>(edges: &Arrange<G, Node, Node, isize>) -> Collection<G, (Node, Node)>
// where G::Timestamp: Lattice+Hash+Ord {

//     // each edge (x,y) means that we need at least a label for the min of x and y.
//     let nodes = edges.as_collection(|&k,&v| (k,v))
//                      .map_in_place(|pair| {
//                         let min = std::cmp::min(pair.0, pair.1);
//                         *pair = (min, min);
//                      })
//                      .consolidate();

//     let edges = edges.as_collection(|&k,&v| (k,v));
//     let edges = edges.map(|(x,y)| (y,x))
//                      .concat(&edges)
//                      .arrange_by_key();

//     // don't actually use these labels, just grab the type
//     nodes.filter(|_| false)
//          .iterate(|inner| {
//              let edges = edges.enter(&inner.scope());
//              let nodes = nodes.enter_at(&inner.scope(), |r| 256 * (64 - r.0.leading_zeros() as u64));

//              edges.join_map(&inner, |_k,d,l| (*d,*l))
//                   .concat(&nodes)
//                   .group(|_, s, t| { t.push((*s[0].0, 1)); } )
//          })
// }

// // returns pairs (n, s) indicating node n can be reached from a root in s steps.
// fn _bfs<G: Scope>(edges: &Arrange<G, Node, Node, isize>, query: &Collection<G, Node>) -> Collection<G, (Node, u32)>
// where G::Timestamp: Lattice+Ord {

//     // initialize query as reaching themselves at distance 0
//     let nodes = query.map(|x| (x, 0));

//     // repeatedly update minimal distances each node can be reached from each root
//     nodes.iterate(|inner| {

//         let edges = edges.enter(&inner.scope());
//         let nodes = nodes.enter(&inner.scope());

//         edges.join_map(&inner, |_k,d,l| (*d, l+1))
//              .concat(&nodes)
//              .group(|_, s, t| t.push((*s[0].0, 1)))
//      })
// }


// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn _tc<G: Scope>(edges: &Arrange<G, Node, Node, isize>) -> Collection<G, (Node, Node)>
where G::Timestamp: Lattice+Ord {

    use timely::dataflow::operators::Inspect;
    use timely::dataflow::operators::Accumulate;

    // repeatedly update minimal distances each node can be reached from each root
    edges
        .as_collection(|&k,&v| (k,v))
        .iterate(|inner| {

            inner.inner.count().inspect_batch(|t,xs| println!("{:?}\t{:?}", t, xs));

            let edges = edges.enter(&inner.scope());

            inner
                .map(|(x,y)| (y,x))
                .join_core(&edges, |_y,&x,&z| Some((x, z)))
                .concat(&edges.as_collection(|&k,&v| (k,v)))
                .distinct()
        }
    )
}


// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn _sg<G: Scope>(edges: &Arrange<G, Node, Node, isize>) -> Collection<G, (Node, Node)>
where G::Timestamp: Lattice+Ord {

    let peers = edges.join_core(&edges, |_, &x,&y| Some((x,y)));

    // repeatedly update minimal distances each node can be reached from each root
    peers
        .iterate(|inner| {

            let edges = edges.enter(&inner.scope());
            let peers = peers.enter(&inner.scope());

            inner
                .join_core(&edges, |_,&x,&z| Some((x, z)))
                .join_core(&edges, |_,&x,&z| Some((x, z)))
                .concat(&peers)
                .distinct()
        }
    )
}
