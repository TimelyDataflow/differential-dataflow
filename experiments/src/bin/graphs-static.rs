// extern crate rand;
extern crate timely;
extern crate differential_dataflow;
extern crate graph_map;
extern crate core_affinity;

use std::rc::Rc;

// use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;

use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely::dataflow::operators::ToStream;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::trace::Trace;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::AsCollection;

use graph_map::GraphMMap;

type Node = u32;

// use differential_dataflow::trace::implementations::graph::GraphBatch;
// type GraphTrace = Spine<Node, Node, Product<RootTimestamp, ()>, isize, Rc<GraphBatch<Node>>>;

use differential_dataflow::trace::implementations::ord::OrdValBatch;
type GraphTrace = Spine<Node, Node, Product<RootTimestamp, ()>, i32, Rc<OrdValBatch<Node, Node, Product<RootTimestamp, ()>, i32>>>;

fn main() {

    // let nodes: Node = std::env::args().nth(1).unwrap().parse().unwrap();
    // let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    let filename = std::env::args().nth(1).expect("Must supply filename");
    let rootnode = std::env::args().nth(2).expect("Must supply root node");

    timely::execute_from_args(std::env::args(), move |worker| {

        let index = worker.index();
        let peers = worker.peers();
        let timer = ::std::time::Instant::now();

        let core_ids = core_affinity::get_core_ids().unwrap();
        core_affinity::set_for_current(core_ids[index]);

        // Phase 1: Forward index.
        let mut forward = worker.dataflow(|scope| {

            let graph = GraphMMap::new(&filename);
            let nodes = graph.nodes();
            let edges = (0..nodes).filter(move |node| node % peers == index)
                                  .flat_map(move |node| {
                                      let vec = graph.edges(node).to_vec();
                                      vec.into_iter().map(move |edge| ((node as Node, edge as Node), Default::default(), 1))
                                  })
                                  .to_stream(scope)
                                  .as_collection();

            edges.arrange(GraphTrace::new()).trace
        });

        while worker.step() { }
        if index == 0 { println!("{:?}\tphase 1:\tforward graph indexed", timer.elapsed()); }

        // Phase 2: Reachability.
        let mut roots = worker.dataflow(|scope| {
            let (roots_input, roots) = scope.new_collection();
            reach(&mut forward, roots);
            roots_input
        });

        if index == 0 { roots.update(0, 1); }
        roots.close();
        while worker.step() { }
        if index == 0 { println!("{:?}\tphase 2:\treach complete", timer.elapsed()); }

        // Phase 3: Breadth-first distance labeling.
        let mut roots = worker.dataflow(|scope| {
            let (roots_input, roots) = scope.new_collection();
            bfs(&mut forward, roots);
            roots_input
        });

        if index == 0 { roots.update(0, 1); }
        roots.close();
        while worker.step() { }
        if index == 0 { println!("{:?}\tphase 3:\tbfs complete", timer.elapsed()); }

        // Phase 4: Reverse index.
        let mut reverse = worker.dataflow(|scope| {
            forward
                .import(scope)
                .as_collection(|&k,&v| (v,k))
                .arrange(GraphTrace::new())
                .trace
        });
        while worker.step() { }
        if index == 0 { println!("{:?}\tphase 4:\treverse graph indexed", timer.elapsed()); }

        // Phase 5: Undirected connectivity.
        worker.dataflow(|scope| { connected_components(scope, &mut forward, &mut reverse); });

        while worker.step() { }
        if index == 0 { println!("{:?}\tphase 5:\tcc complete", timer.elapsed()); }

    }).unwrap();
}

// use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::operators::arrange::TraceAgent;

type TraceHandle = TraceAgent<Node, Node, Product<RootTimestamp, ()>, i32, GraphTrace>;

fn reach<G: Scope<Timestamp = Product<RootTimestamp, ()>>> (
    graph: &mut TraceHandle,
    roots: Collection<G, Node, i32>
) -> Collection<G, Node, i32> {

    let graph = graph.import(&roots.scope());

    roots.iterate(|inner| {

        let graph = graph.enter(&inner.scope());
        let roots = roots.enter(&inner.scope());

        graph.join_core(&inner.arrange_by_self(), |_src,&dst,&()| Some(dst))
             .concat(&roots)
             // .distinct_total()
             .threshold_total(|c| if c == 0 { 0 } else { 1 })
        })
}


fn bfs<G: Scope<Timestamp = Product<RootTimestamp, ()>>> (
    graph: &mut TraceHandle,
    roots: Collection<G, Node, i32>
) -> Collection<G, (Node, u32), i32> {

    let graph = graph.import(&roots.scope());
    let roots = roots.map(|r| (r,0));

    roots.iterate(|inner| {

        let graph = graph.enter(&inner.scope());
        let roots = roots.enter(&inner.scope());

        graph.join_map(&inner, |_src,&dest,&dist| (dest, dist+1))
             .concat(&roots)
             .group(|_key, input, output| output.push((*input[0].0,1)))
    })
}

fn connected_components<G: Scope<Timestamp = Product<RootTimestamp, ()>>>(
    scope: &mut G,
    forward: &mut TraceHandle,
    reverse: &mut TraceHandle,
) -> Collection<G, (Node, Node), i32> {

    let forward = forward.import(scope);
    let reverse = reverse.import(scope);

    // each edge (x,y) means that we need at least a label for the min of x and y.
    let nodes =
    forward
        .as_collection(|&k,&v| {
            let min = std::cmp::min(k,v);
            (min, min)
        })
        .consolidate();

    // don't actually use these labels, just grab the type
    nodes
        .filter(|_| false)
        .iterate(|inner| {

            let forward = forward.enter(&inner.scope());
            let reverse = reverse.enter(&inner.scope());
            let nodes = nodes.enter_at(&inner.scope(), |r| 256 * (64 - r.1.leading_zeros() as u64));

            let inner = inner.arrange_by_key();

            let f_prop = inner.join_core(&forward, |_k,l,d| Some((*d,*l)));
            let r_prop = inner.join_core(&reverse, |_k,l,d| Some((*d,*l)));

            nodes
                .concat(&f_prop).concat(&r_prop)
                .group(|_, s, t| { t.push((*s[0].0, 1)); })
        })
}
