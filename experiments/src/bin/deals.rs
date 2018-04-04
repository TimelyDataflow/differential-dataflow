extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::time::Instant;

use timely::dataflow::*;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::ArrangeByKey;

use differential_dataflow::trace::implementations::ord::OrdValSpine as DefaultValTrace;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::Arranged;

type Arrange<G: Scope, K, V, R> = Arranged<G, K, V, R, TraceAgent<K, V, G::Timestamp, R, DefaultValTrace<K, V, G::Timestamp, R>>>;

type Node = u32;

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

        println!("{:?}\tData ingested", timer.elapsed());

    }).unwrap();
}

use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

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