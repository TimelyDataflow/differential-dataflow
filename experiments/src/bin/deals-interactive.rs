extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::time::Instant;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::lattice::Lattice;

use differential_dataflow::trace::implementations::ord::OrdValSpine as DefaultValTrace;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::operators::arrange::Arranged;

type Arrange<G: Scope, K, V, R> = Arranged<G, K, V, R, TraceAgent<K, V, G::Timestamp, R, DefaultValTrace<K, V, G::Timestamp, R>>>;

type Node = u32;

fn main() {

    // snag a filename to use for the input graph.
    let filename = std::env::args().nth(1).unwrap();
    let rounds: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let inspect = std::env::args().any(|x| x == "inspect");

    timely::execute_from_args(std::env::args().skip(1), move |worker| {

        let peers = worker.peers();
        let index = worker.index();

        let (mut input, mut query1, mut query2, mut query3, probe) = worker.dataflow(|scope| {

            let (input, graph) = scope.new_collection();

            let (query1_input, query1) = scope.new_collection();
            let (query2_input, query2) = scope.new_collection();
            let (query3_input, query3) = scope.new_collection();

            // each edge should exist in both directions.
            let graph = graph.arrange_by_key();

            let probe =
            interactive(&graph, query1, query2, query3)
                .filter(move |_| inspect)
                // .map(|_| ())
                .consolidate()
                .inspect(|x| println!("count: {:?}", x))
                .probe();

            (input, query1_input, query2_input, query3_input, probe)
        });


        let timer = Instant::now();

        let mut nodes = Vec::new();

        use std::io::{BufReader, BufRead};
        use std::fs::File;

        let file = BufReader::new(File::open(filename.clone()).unwrap());
        for (count, readline) in file.lines().enumerate() {
            let line = readline.ok().expect("read error");
            if count % peers == index && !line.starts_with('#') {
                let mut elts = line[..].split_whitespace();
                let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
                let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");
                nodes.push(src);
                nodes.push(dst);
                input.insert((src, dst));
            }
        }

        nodes.sort();
        nodes.dedup();

        if index == 0 { println!("{:?}\tData ingested", timer.elapsed()); }

        // run until graph is loaded
        input.advance_to(1); input.flush();
        query1.advance_to(1); query1.flush();
        query2.advance_to(1); query2.flush();
        query3.advance_to(1); query3.flush();

        worker.step_while(|| probe.less_than(input.time()));

        if index == 0 { println!("{:?}\tData indexed", timer.elapsed()); }

        let seed: &[_] = &[1, 2, 3, index];
        let mut rng: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions

        let worker_batch = batch / peers + if index < batch % peers { 1 } else { 0 };

        input.advance_to(1 + 1 * rounds); input.flush();
        query2.advance_to(1 + 1 * rounds); query2.flush();
        query3.advance_to(1 + 1 * rounds); query3.flush();

        // let mut latencies1 = Vec::with_capacity(rounds);

        for round in 1 .. (1 + 1 * rounds) {
            let timer = Instant::now();
            for _ in 0 .. worker_batch {
                query1.insert(*rng.choose(&nodes[..]).unwrap());
            }
            query1.advance_to(round);
            query1.flush();
            while probe.less_than(query1.time()) { worker.step(); }
            if index == 0 { println!("query1: {:?}", timer.elapsed()); }
        }

        if index == 0 { println!("{:?}\tRound 1 complete", timer.elapsed()); }

        input.advance_to(1 + 2 * rounds); input.flush();
        query1.advance_to(1 + 2 * rounds); query1.flush();
        query3.advance_to(1 + 2 * rounds); query3.flush();

        // let mut latencies2 = Vec::with_capacity(rounds);

        for round in (1 + 1 * rounds) .. (1 + 2 * rounds) {
            let timer = Instant::now();
            for _ in 0 .. worker_batch {
                query2.insert(*rng.choose(&nodes[..]).unwrap());
            }
            query2.advance_to(round);
            query2.flush();
            while probe.less_than(query2.time()) { worker.step(); }
            if index == 0 { println!("query2: {:?}", timer.elapsed()); }
        }

        if index == 0 { println!("{:?}\tRound 2 complete", timer.elapsed()); }

        input.advance_to(1 + 3 * rounds); input.flush();
        query1.advance_to(1 + 3 * rounds); query1.flush();
        query2.advance_to(1 + 3 * rounds); query2.flush();

        // let mut latencies3 = Vec::with_capacity(rounds);

        for round in (1 + 2 * rounds) .. (1 + 3 * rounds) {
            let timer = Instant::now();
            for _ in 0 .. worker_batch {
                query3.insert(*rng.choose(&nodes[..]).unwrap());
            }
            query3.advance_to(round);
            query3.flush();
            while probe.less_than(query3.time()) { worker.step(); }
            if index == 0 { println!("query3: {:?}", timer.elapsed()); }
        }

        if index == 0 { println!("{:?}\tRound 3 complete", timer.elapsed()); }

        query1.close();
        query2.close();
        query3.close();

        // let mut latencies4 = Vec::with_capacity(rounds);

        for round in (1 + 3 * rounds) .. (1 + 4 * rounds) {
            let timer = Instant::now();
            for _ in 0 .. worker_batch {
                input.insert((*rng.choose(&nodes[..]).unwrap(), *rng.choose(&nodes[..]).unwrap()));
            }
            input.advance_to(round);
            input.flush();
            while probe.less_than(input.time()) { worker.step(); }
            if index == 0 { println!("query4: {:?}", timer.elapsed()); }
        }

    }).unwrap();
}

fn interactive<G: Scope>(
    edges: &Arrange<G, Node, Node, isize>,
    tc_1: Collection<G, Node>,
    tc_2: Collection<G, Node>,
    sg_x: Collection<G, Node>
) -> Collection<G, Node>
where G::Timestamp: Lattice{

    // descendants of tc_1:
    let query1 =
    tc_1.map(|x| (x,x))
        .iterate(|inner|
            edges
                .enter(&inner.scope())
                .join_map(&inner, |_,&y,&q| (y,q))
                .concat(&tc_1.enter(&inner.scope()).map(|x| (x,x)))
                .distinct()
        )
        .map(|(x,q)| (q,x));

    // ancestors of tc_2:
    let query2 =
    tc_2.map(|x| (x,x))
        .iterate(|inner|
            edges
                .as_collection(|&k,&v| (v,k))
                .enter(&inner.scope())
                .join_map(&inner, |_,&y,&q| (y,q))
                .concat(&tc_2.enter(&inner.scope()).map(|x| (x,x)))
                .distinct()
        )
        .map(|(x,q)| (q,x));

    // Adapted from: http://ranger.uta.edu/~fegaras/cse6331/spring97/p25.html
    // sg(X,X) <- magic(X).
    // sg(X,Y) <- magic(X), par(X,Xp), par(Y,Yp), sg(Xp,Yp).

    // ancestors of sg_x:
    let magic =
    sg_x.iterate(|inner|
            edges
                .as_collection(|&k,&v| (v,k))
                .enter(&inner.scope())
                .semijoin(&inner)
                .map(|(_x,y)| y)
                .concat(&sg_x.enter(&inner.scope()))
                .distinct()
        );

    let magic_edges =
    edges
        .semijoin(&magic)
        .map(|(x,y)|(y,x))
        .semijoin(&magic)
        .map(|(x,y)|(y,x));

    let query3 =
    magic
        .map(|x| (x,x))   // for query q, sg(x,x)
        .iterate(|inner| {

            let edges = edges.enter(&inner.scope());
            let magic = magic.enter(&inner.scope());
            let magic_edges = magic_edges.enter(&inner.scope());

            let result =
            inner
                .join_map(&magic_edges, |_x,&y,&cx| (y,cx))
                .join_core(&edges, |_y,&cx,&cy| Some((cx,cy)))
                .concat(&magic.map(|x| (x,x)))
                .distinct();

            result.map(|_| ()).consolidate().inspect(|x| println!("\t{:?}", x));
            result
        })
        .semijoin(&sg_x);

    query1.concat(&query2).concat(&query3).map(|(q,_)| q)
}