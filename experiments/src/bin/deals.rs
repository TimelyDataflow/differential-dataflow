extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::time::Instant;
use std::mem;
use std::hash::Hash;

use timely::dataflow::*;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::ArrangeByKey;

use differential_dataflow::trace::implementations::ord::OrdValSpine as DefaultValTrace;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::Arranged;

type Arrange<G, K, V, R> = Arranged<G, K, V, R, TraceAgent<K, V, <G as ScopeParent>::Timestamp, R, DefaultValTrace<K, V, <G as ScopeParent>::Timestamp, R>>>;

type Node = u32;
type Edge = (Node, Node);

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();
    let program = std::env::args().nth(2).unwrap();
    let inspect = std::env::args().any(|x| x == "inspect");

    timely::execute_from_args(std::env::args().skip(1), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        let mut input = worker.dataflow::<(),_,_>(|scope| {

            let (input, graph) = scope.new_collection();

            // each edge should exist in both directions.
            let graph = graph.arrange_by_key();

            match program.as_str() {
                "tc"    => tc(&graph).filter(move |_| inspect).map(|_| ()).consolidate().inspect(|x| println!("tc count: {:?}", x)).probe(),
                "sc"    => _strongly_connected(&graph.as_collection(|k,v| (*k,*v))).filter(move |_| inspect).map(|_| ()).consolidate().inspect(|x| println!("tc count: {:?}", x)).probe(),
                "sg"    => sg(&graph).filter(move |_| inspect).map(|_| ()).consolidate().inspect(|x| println!("sg count: {:?}", x)).probe(),
                _       => panic!("must specify one of 'tc', 'sg'.")
            };

            input
        });

        let timer = Instant::now();

        let mut nodes = 0;

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

        if index == 0 { println!("{:?}\tData ingested", timer.elapsed()); }

        input.close();
        while worker.step() { }

        if index == 0 { println!("{:?}\tComputation complete", timer.elapsed()); }

    }).unwrap();
}

use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

fn _trim_and_flip<G: Scope>(graph: &Collection<G, Edge>) -> Collection<G, Edge>
where G::Timestamp: Lattice+Ord+Hash {
    graph.iterate(|edges| {
        // keep edges from active edge destinations.
        let active = edges.map(|(src,_dst)| src)
                          .distinct();

        graph.enter(&edges.scope())
             .arrange_by_key()
             .semijoin(&active)
    })
    .map_in_place(|x| mem::swap(&mut x.0, &mut x.1))
}

fn _strongly_connected<G: Scope>(graph: &Collection<G, Edge>) -> Collection<G, Edge>
where G::Timestamp: Lattice+Ord+Hash {
    graph.iterate(|inner| {
        let edges = graph.enter(&inner.scope());
        let trans = edges.map_in_place(|x| mem::swap(&mut x.0, &mut x.1));
        _trim_edges(&_trim_edges(inner, &edges), &trans)
    })
}

fn _trim_edges<G: Scope>(cycle: &Collection<G, Edge>, edges: &Collection<G, Edge>)
    -> Collection<G, Edge> where G::Timestamp: Lattice+Ord+Hash {

    let nodes = edges.map_in_place(|x| x.0 = x.1)
                     .consolidate();

    let labels = _reachability(&cycle, &nodes);

    edges.join_map(&labels, |&e1,&e2,&l1| (e2,(e1,l1)))
         .join_map(&labels, |&e2,&(e1,l1),&l2| ((e1,e2),(l1,l2)))
         .filter(|&(_,(l1,l2))| l1 == l2)
         .map(|((x1,x2),_)| (x2,x1))
}

fn _reachability<G: Scope>(edges: &Collection<G, Edge>, nodes: &Collection<G, (Node, Node)>) -> Collection<G, Edge>
where G::Timestamp: Lattice+Ord+Hash {

    edges.filter(|_| false)
         .iterate(|inner| {
             let edges = edges.enter(&inner.scope());
             let nodes = nodes.enter_at(&inner.scope(), |r| 256 * (64 - (r.0 as u64).leading_zeros() as u64));

             inner.join_map(&edges, |_k,l,d| (*d,*l))
                  .concat(&nodes)
                  .group(|_, s, t| t.push((*s[0].0, 1)))

         })
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn tc<G: Scope<Timestamp=Product<RootTimestamp, ()>>>(edges: &Arrange<G, Node, Node, isize>) -> Collection<G, (Node, Node)> {

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
                .distinct_total()
        }
    )
}


// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn sg<G: Scope<Timestamp=Product<RootTimestamp, ()>>>(edges: &Arrange<G, Node, Node, isize>) -> Collection<G, (Node, Node)> {

    let peers = edges.join_core(&edges, |_,&x,&y| Some((x,y))).filter(|&(x,y)| x != y);

    // repeatedly update minimal distances each node can be reached from each root
    peers
        .iterate(|inner| {

            let edges = edges.enter(&inner.scope());
            let peers = peers.enter(&inner.scope());

            inner
                .join_core(&edges, |_,&x,&z| Some((x, z)))
                .join_core(&edges, |_,&x,&z| Some((x, z)))
                .concat(&peers)
                .distinct_total()
        }
    )
}