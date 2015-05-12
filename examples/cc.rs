#![feature(collections_drain)]

extern crate rand;
extern crate time;
extern crate columnar;
extern crate timely;
extern crate differential_dataflow;

use std::hash::Hash;
use timely::example_shared::*;
use timely::example_shared::operators::*;
use timely::communication::ThreadCommunicator;
use timely::communication::pact::Pipeline;
use timely::communication::observer::ObserverSessionExt;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;

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
                 .unary_notify(Pipeline, format!("observer"), vec![RootTimestamp::new(0)], move |input, output, notificator| {
                    while let Some((t, _)) = notificator.next() {
                        println!("{}", ((time::precise_time_s() - start2)) - (t.inner as f64));
                        // println!("{}s:\tnotified at {:?}", ((time::precise_time_s() - start2)) - (t.inner as f64), t);
                        notificator.notify_at(&Product::new(t.outer, t.inner + 1));
                    }
                    while let Some((time, data)) = input.pull() {
                        output.give_at(&time, data.drain(..));
                    }
                 })
                //  .inspect_batch(move |t, x| { println!("{}s:\tobserved at {:?}: {:?} changes",
                //                                      ((time::precise_time_s() - start2)) - (t.inner as f64),
                //                                      t, x.len()) })
                ;

            input
        });

        let nodes = 20_000_000;
        let edges = 400_000_000;

        println!("determining CC of {} nodes, {} edges:", nodes, edges);

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);
        let mut rng2: StdRng = SeedableRng::from_seed(seed);

        rng1.gen::<f64>();
        rng2.gen::<f64>();

        let mut left = edges;
        while left > 0 {
            let next = if left < 1000 { left } else { 1000 };
            input.send_at(0, (0..next).map(|_| ((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1)));
            computation.step();
            left -= next;
        }


        let mut round = 0 as u32;
        let mut changes = Vec::new();
        while computation.step() {
            if time::precise_time_s() - start >= round as f64 {
                let change_count = 1000;
                for _ in 0..change_count {
                    changes.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
                    changes.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1));
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
    let nodes = edges.filter(|&((x,y),_)| x < y)
                     .map(|((x,_),w)| (x,w)).consolidate(|||&x|x.as_usize() as u64);

    reachability(&edges, &nodes)
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


fn improve_labels<G: GraphBuilder, U: UnsignedInt>(labels: &Stream<G, ((U, U), i32)>, edges: &Stream<G, ((U, U), i32)>, nodes: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound {

    labels.join_u(&edges, |l| l, |e| e, |_k,l,d| (*d,*l))
          .concat(&nodes)
          .group_by_u(|x| x, |k,v| (*k,*v), |_, s, t| { t.push((s[0].0, 1)); } )
}
