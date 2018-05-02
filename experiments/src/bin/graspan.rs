extern crate timely;
extern crate graph_map;
extern crate differential_dataflow;

use std::io::{BufRead, BufReader};
use std::fs::File;

// use timely::dataflow::operators::{Accumulate, Inspect};
use differential_dataflow::input::Input;
// use differential_dataflow::trace::Trace;
// use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::operators::*;
// use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::operators::arrange::ArrangeByKey;

fn main() {

    timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        let peers = worker.peers();
        let index = worker.index();

        let (mut nodes, mut edges) = worker.dataflow::<(),_,_>(|scope| {

            // let timer = timer.clone();

            let (n_handle, nodes) = scope.new_collection();
            let (e_handle, edges) = scope.new_collection();

            let edges = edges.arrange_by_key();

            // a N c  <-  a N b && b E c
            // N(a,c) <-  N(a,b), E(b, c)
            nodes
                .iterate(|inner| {

                    let nodes = nodes.enter(&inner.scope());
                    let edges = edges.enter(&inner.scope());

                    let temp_result =
                    inner
                        .map(|(a,b)| (b,a))
                        .join_core(&edges, |_b,&a,&c| Some((a,c)))
                        .concat(&nodes);

                    // temp_result.map(|_| ()).consolidate().inspect(|x| println!("pre-agg:\t{:?}", x.2));

                    let result = temp_result
                        .distinct_total();

                    // result.map(|_| ()).consolidate().inspect(|x| println!("post-agg:\t{:?}", x.2));

                    result
                })
                // .map(|_| ())
                // .consolidate()
                // .inspect(|x| println!("{:?}", x))
                ;

            (n_handle, e_handle)
        });

        if index == 0 { println!("{:?}:\tDataflow assembled", timer.elapsed()); }

        // snag a filename to use for the input graph.
        let filename = std::env::args().nth(1).unwrap();
        let file = BufReader::new(File::open(filename).unwrap());
        for readline in file.lines() {
            let line = readline.ok().expect("read error");
            if !line.starts_with('#') && line.len() > 0 {
                let mut elts = line[..].split_whitespace();
                let src: u32 = elts.next().unwrap().parse().ok().expect("malformed src");
                if (src as usize) % peers == index {
                    let dst: u32 = elts.next().unwrap().parse().ok().expect("malformed dst");
                    let typ: &str = elts.next().unwrap();
                    match typ {
                        "n" => { nodes.insert((src, dst)); },
                        "e" => { edges.insert((src, dst)); },
                        unk => { panic!("unknown type: {}", unk)},
                    }
                }
            }
        }

        if index == 0 { println!("{:?}:\tData loaded", timer.elapsed()); }

        nodes.close();
        edges.close();
        while worker.step() { }

        if index == 0 { println!("{:?}:\tComputation complete", timer.elapsed()); }

    }).unwrap();
}