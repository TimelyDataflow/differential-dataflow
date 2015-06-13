extern crate rand;
extern crate time;
extern crate byteorder;
extern crate columnar;
extern crate timely;
extern crate differential_dataflow;

use byteorder::{ReadBytesExt, LittleEndian};

use std::io::{BufReader};
use std::fs::File;
use std::thread;

use std::hash::Hash;
use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::{Communicator, ProcessCommunicator};
use timely::drain::DrainExt;

use differential_dataflow::collection_trace::lookup::UnsignedInt;
use differential_dataflow::collection_trace::LeastUpperBound;

use differential_dataflow::operators::*;

fn main() {

    let communicators = ProcessCommunicator::new_vector(4);
    let mut guards = Vec::new();
    for communicator in communicators.into_iter() {
        guards.push(thread::Builder::new().name(format!("worker thread {}", communicator.index()))
                                          .spawn(move || test_dataflow(communicator))
                                          .unwrap());
    }

    for guard in guards { guard.join().unwrap(); }
}

fn test_dataflow<C: Communicator>(communicator: C) {

        let start = time::precise_time_s();
        let start2 = start.clone();
        let mut computation = GraphRoot::new(communicator);

        // define the computation
        let mut input = computation.subcomputation(|builder| {

            let (input, mut edges) = builder.new_input();

            edges = connected_components(&edges);

            edges.inspect_batch(move |t, x| {
                println!("{}s:\tobserved at {:?}: {:?} changes",
                         ((time::precise_time_s() - start2)) - (t.inner as f64),
                         t, x.len())
            });

            input
        });

        if computation.index() == 0 {

            let mut nodes = BufReader::new(File::open("/Users/mcsherry/Projects/Datasets/twitter-dedup.offsets").unwrap());
            let mut edges = BufReader::new(File::open("/Users/mcsherry/Projects/Datasets/twitter-dedup.targets").unwrap());

            let mut sent = 0;
            let mut buffer = Vec::new();
            let mut offset = nodes.read_u64::<LittleEndian>().unwrap();
            assert!(offset == 0);
            for node in (0..60000000) {
                let read = nodes.read_u64::<LittleEndian>().unwrap();
                for _ in 0.. (read - offset) {
                    let edge = edges.read_u32::<LittleEndian>().unwrap();
                    if node % 2 == 0 && edge % 2 == 0 {
                        buffer.push(((node / 2 as u32, edge / 2 as u32), 1));
                        if buffer.len() > 4000 {
                            sent += buffer.len();
                            input.send_at(0, buffer.drain_temp());
                            computation.step();
                        }
                    }
                }
                offset = read;
            }

            sent += buffer.len();
            input.send_at(0, buffer.drain_temp());

            println!("sent {} edges", sent);
        }

        input.close();

        while computation.step() { }
        computation.step(); // shut down
}

fn connected_components<G: GraphBuilder, U: UnsignedInt>(edges: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    let nodes = edges.map(|((x,y),w)| (std::cmp::min(x,y), w))
                     .consolidate(|x|x.as_u64(), |x|x.as_u64());

    let edges = edges.map(|((x,y),w)| ((y,x),w)).concat(&edges);

    reachability(&edges, &nodes)
}

fn reachability<G: GraphBuilder, U: UnsignedInt>(edges: &Stream<G, ((U, U), i32)>, nodes: &Stream<G, (U, i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(u32::max_value(), |x| x.0, |x| x.0, |inner| {
             let edges = inner.builder().enter(&edges);
             let nodes = inner.builder().enter_at(&nodes, |r| 256 * (64 - r.0.as_u64().leading_zeros() as u32 ))
                                        .map(|(x,w)| ((x,x),w));

             improve_labels(inner, &edges, &nodes)
         })
         .consolidate(|x| x.0, |x| x.0)
}


fn improve_labels<G: GraphBuilder, U: UnsignedInt>(labels: &Stream<G, ((U, U), i32)>,
                                                   edges: &Stream<G, ((U, U), i32)>,
                                                   nodes: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound {

    labels.join_u(&edges, |l| l, |e| e, |_k,l,d| (*d,*l))
          .concat(&nodes)
          .group_by_u(|x| x, |k,v| (*k,*v), |_, mut s, t| { t.push((*s.peek().unwrap().0, 1)); } )
}
