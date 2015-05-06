#![feature(collections_drain)]

extern crate rand;
extern crate time;
extern crate columnar;
extern crate timely;
extern crate differential_dataflow;

use std::mem;
// use std::rc::{Rc, try_unwrap};
// use std::cell::RefCell;

use std::hash::Hash;
use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::ThreadCommunicator;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::collection_trace::lookup::UnsignedInt;
use differential_dataflow::collection_trace::LeastUpperBound;

use differential_dataflow::operators::*;

// The typical differential dataflow vertex receives updates of the form (key, time, value, update),
// where the data are logically partitioned by key, and are then subject to various aggregations by time,
// accumulating for each value the update integers. The resulting multiset is the subjected to computation.

// The implementation I am currently most comfortable with is *conservative* in the sense that it will defer updates
// until it has received all updates for a time, at which point it commits these updates permanently. This is done
// to avoid issues with running logic on partially formed data, but should also simplify our data management story.
// Rather than requiring random access to diffs, we can store them as flat arrays (possibly sorted) and integrate
// them using merge techniques. Updating cached accumulations seems maybe harder w/o hashmaps, but we'll see...

fn main() {

        let start = time::precise_time_s();
        let start2 = start.clone();
        let mut computation = GraphRoot::new(ThreadCommunicator);

        let mut input = computation.subcomputation(|builder| {

            let (input, mut edges) = builder.new_input();

            edges = connected_components(&edges);

            edges.consolidate(|||x: &(u32, u32)| x.0)
                 .inspect_batch(move |t, x| { println!("{}s:\tobserved at {:?}: {:?} changes",
                                                     ((time::precise_time_s() - start2)) - (t.inner as f64),
                                                     t, x.len()) });

            input
        });

        let node_count = 40_000_000;
        let edge_count = 40_000_000;
        // let node_count = 40;
        // let edge_count = 40;

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng: StdRng = SeedableRng::from_seed(seed);
        rng.gen::<f64>();

        println!("determining CC of {} nodes, {} edges:", node_count, edge_count);

        // let mut rng = rand::thread_rng();
        let mut edges = Vec::new();
        for _ in 0..edge_count {
            edges.push((((rng.gen::<u64>() % node_count) as u32, (rng.gen::<u64>() % node_count) as u32), 1))
        }

        input.send_at(0, edges.clone().into_iter()
                                      .map(|((x,y),w)|((x as u32, y as u32), w))
                                      );

        let mut round = 0 as u32;
        let mut changes = Vec::new();
        while computation.step() {
            if time::precise_time_s() - start >= round as f64 {

                for _ in 0..100 {

                    let new_record = (((rng.gen::<u64>() % node_count) as u32,
                                       (rng.gen::<u64>() % node_count) as u32), 1);
                    let new_position = rng.gen::<usize>() % edges.len();
                    let mut old_record = mem::replace(&mut edges[new_position], new_record.clone());
                    old_record.1 = -1;
                    changes.push(new_record);
                    changes.push(old_record);
                }

                input.send_at(round, changes.drain(..));
                input.advance_to(round + 1);
                round += 1;
            }
        }

        input.close();

        while computation.step() { }
        computation.step(); // shut down

}

fn connected_components<G: GraphBuilder, U: UnsignedInt>(edges: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    let edges = edges.map(|((x,y),w)| ((y,x),w)).concat(&edges);
    let nodes = edges.map(|((_,x),w)| (x,w)).consolidate(|||&x|x.as_usize() as u64);

    reachability(&edges, &nodes)
}

fn improve_labels<G: GraphBuilder, U: UnsignedInt>(labels: &Stream<G, ((U, U), i32)>, edges: &Stream<G, ((U, U), i32)>, nodes: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound {

    labels.join_u(&edges, |l| l, |e| e, |l| l.0, |e| e.0, |_k,l,d| (*d,*l))
          .concat(&nodes)
          .group_by_u(|x| x, |x| x.0, |k,v| (*k,*v), |_, s, t| { t.push((s[0].0, 1)); } )
}

fn reachability<G: GraphBuilder, U: UnsignedInt>(edges: &Stream<G, ((U, U), i32)>, nodes: &Stream<G, (U, i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(u32::max_value(), |||x| x.0, |inner| {
             let edges = inner.builder().enter(&edges);
             let nodes = inner.builder().enter_at(&nodes, |r| 256 * (64 - r.0.as_usize().leading_zeros() as u32))
                                        .map(|(x,w)| ((x,x),w));

             improve_labels(inner, &edges, &nodes)
         })
         .consolidate(|||x| x.0)
}


fn fancy_reachability<G: GraphBuilder, U: UnsignedInt>(edges: &Stream<G, ((U, U), i32)>, nodes: &Stream<G, (U, i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(u32::max_value(), |||x| x.0, |inner| {
             let edges = inner.builder().enter(&edges);
             let nodes = inner.builder().enter(&nodes)
                              .except(&inner.filter(|&((ref n, ref l),_)| l < n).map(|((n,_),w)| (n,w)))
                              .delay(|r,t| { let mut t2 = t.clone(); t2.inner = 256 * (64 - r.0.as_usize().leading_zeros() as u32); t2 })
                              .consolidate(|||&x| x)
                              .map(|(x,w)| ((x,x),w));

             improve_labels(inner, &edges, &nodes)
         })
         .consolidate(|||x| x.0)
}
