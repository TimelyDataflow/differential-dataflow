extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::Collection;
use differential_dataflow::hashable::OrdWrapper;

use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::*;
use differential_dataflow::operators::join::*;
use differential_dataflow::operators::group::*;

use differential_dataflow::lattice::Lattice;

use differential_dataflow::trace::Trace;
use differential_dataflow::trace::implementations::trie::Spine as TrieSpine;
use differential_dataflow::trace::implementations::rhh::Spine as RHHSpine;

type Node = u32;
type Edge = (Node, Node);

#[test]
fn main_test() {

    let nodes: u32 = 4;
    let edges: u32 = 6;
    let batch: u32 = 1;

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args().skip(6), move |computation| {
        
        // define BFS dataflow; return handles to roots and edges inputs
        let (mut graph, probe) = computation.scoped(|scope| {

            let roots = vec![(1,1)].into_iter().to_stream(scope);
            let (edge_input, graph) = scope.new_input();

            let result = bfs(&Collection::new(graph.clone()), &Collection::new(roots.clone()));

            let probe = result.probe();

            (edge_input, probe.0)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        // println!("performing BFS on {} nodes, {} edges:", nodes, edges);

        if computation.index() == 0 {
            // trickle edges in to dataflow
            for _ in 0..(edges/1000) {
                for _ in 0..10000 {
                    graph.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
                }
                computation.step();
            }
            for _ in 0.. (edges % 1000) {
                graph.send(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
            }
        }

        graph.advance_to(1);
        computation.step_while(|| probe.lt(graph.time()));

        if batch > 0 {
            let mut changes = Vec::new();
            for _wave in 0 .. 1000 {
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
                // }
            }
        }
    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, u32)>
where G::Timestamp: Lattice+Ord {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());//.inspect_batch(|t,xs| println!("{:?}\tedges: {:?}",t,xs));
        let nodes = nodes.enter(&inner.scope());

        let edges_tr = edges.arrange(|k,v| (k,v), TrieSpine::new(Default::default()));
        let edges_rh = edges.arrange(|k,v| (OrdWrapper { item:k }, v), RHHSpine::new(Default::default()));

        // let inner = inner.inspect_batch(|t,xs| println!("{:?}\tinner: {:?}",t,xs));

        let inner_tr = inner.arrange(|k,v| (k,v), TrieSpine::new(Default::default()));
        let inner_rh = inner.arrange(|k,v| (OrdWrapper { item:k }, v), RHHSpine::new(Default::default()));

        let props_tr = inner_tr.join_arranged(&edges_tr, |_k,l,d| (*d, l+1))/*.inspect_batch(|t,xs| println!("{:?}\tjoin_tr: {:?}", t, xs))*/.concat(&nodes);
        let props_rh = inner_rh.join_arranged(&edges_rh, |_k,l,d| (*d, l+1))/*.inspect_batch(|t,xs| println!("{:?}\tjoin_rh: {:?}", t, xs))*/.concat(&nodes);

        props_tr.negate().concat(&props_rh).consolidate().inspect_batch(|t,xs| if xs.len() > 0 { panic!("{:?}\tjoin discrepancy: {:?}", t, xs) });

        // let props_tr = props_tr.inspect_batch(|t,xs| println!("{:?}\tprops: {:?}",t,xs));

        let pregr_tr = props_tr.arrange(|k,v| (k,v), TrieSpine::new(Default::default()));
        let pregr_rh = props_tr.arrange(|k,v| (OrdWrapper { item:k }, v), RHHSpine::new(Default::default()));

        let groups_tr = pregr_tr.group_arranged(|_, s, t| t.push((s[0].0, 1)), TrieSpine::new(Default::default()))
                                .as_collection(|&k,&v| (k,v))
                                // .inspect_batch(|t,xs| println!("{:?}\tgroup_tr: {:?}", t, xs))
                                ;
        let groups_rh = pregr_rh.group_arranged(|_, s, t| t.push((s[0].0, 1)), RHHSpine::new(Default::default()))
                                .as_collection(|k,&v| (k.item, v))
                                // .inspect_batch(|t,xs| println!("{:?}\tgroup_rh: {:?}", t, xs))
                                ;

        groups_tr.negate().concat(&groups_rh).consolidate().inspect_batch(|t,xs| if xs.len() > 0 { panic!("{:?}\tgroup discrepancy: {:?}", t, xs) });

        groups_tr
             
     })
}