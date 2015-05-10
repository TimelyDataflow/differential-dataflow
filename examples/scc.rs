extern crate rand;
extern crate time;
extern crate columnar;
extern crate timely;
extern crate differential_dataflow;

use std::mem;

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
    test_dataflow();
}

fn _trim_and_flip<G: GraphBuilder, U: UnsignedInt>(graph: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)> where G::Timestamp: LeastUpperBound {

        graph.iterate(u32::max_value(), |||x|x.0, |edges| {
            let inner = edges.builder().enter(&graph);
            edges.map(|((x,_),w)| (x,w))
                 .group_by_u(|x|(x,()), |&x,_| x, |_,_,target| target.push(((),1)))
                 .join_u(&inner, |x| (x,()), |(s,d)| (d,s), |&d,_,&s| (s,d))
             })
             .consolidate(|||x| x.0)
             .map(|((x,y),w)| ((y,x),w))
}

fn improve_labels<G: GraphBuilder, U: UnsignedInt>(labels: &Stream<G, ((U, U), i32)>,
                                                   edges: &Stream<G, ((U, U), i32)>,
                                                   nodes: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound {

    labels.join_u(&edges, |l| l, |e| e, |_k,l,d| (*d,*l))
          .concat(&nodes)
          .group_by_u(|x| x, |k,v| (*k,*v), |_, s, t| { t.push((s[0].0, 1)); } )
}

fn _reachability<G: GraphBuilder, U: UnsignedInt>(edges: &Stream<G, ((U, U), i32)>, nodes: &Stream<G, (U, i32)>)
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


fn _fancy_reachability<G: GraphBuilder, U: UnsignedInt>(edges: &Stream<G, ((U, U), i32)>, nodes: &Stream<G, (U, i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(u32::max_value(), |||x| x.0, |inner| {
             let edges = inner.builder().enter(&edges);
             let nodes = inner.builder().enter(&nodes)
                              .map(|(x,_)| (x,1))
                              .except(&inner.filter(|&((ref n, ref l),_)| l < n).map(|((n,_),w)| (n,w)))
                              .delay(|r,t| { let mut t2 = t.clone(); t2.inner = 256 * (64 - r.0.as_usize().leading_zeros() as u32); t2 })
                              .consolidate(|||&x| x)
                              .map(|(x,w)| ((x,x),w));

             improve_labels(inner, &edges, &nodes)
         })
         .consolidate(|||x| x.0)
}

fn trim_edges<G: GraphBuilder, U: UnsignedInt>(cycle: &Stream<G, ((U, U), i32)>,
                                               edges: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)> where G::Timestamp: LeastUpperBound+Hash {

    let nodes = edges.map(|((_,y),w)| (y,w)).consolidate(|||&x| x);

    let labels = _reachability(&cycle, &nodes);

    edges.join_u(&labels, |e| e, |l| l, |&e1,&e2,&l1| (e2,(e1,l1)))
         .join_u(&labels, |e| e, |l| l, |&e2,&(e1,l1),&l2| ((e1,e2),(l1,l2)))
         .consolidate(|||x|(x.0).0)
         .filter(|&((_,(l1,l2)), _)| l1 == l2)
         .map(|(((x1,x2),_),d)| ((x2,x1),d))
}

fn strongly_connected<G: GraphBuilder, U: UnsignedInt>(graph: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)> where G::Timestamp: LeastUpperBound+Hash {

    graph.iterate(u32::max_value(), |||x| x.0, |inner| {
        let trans = inner.builder().enter(&graph).map(|((x,y),w)| ((y,x),w));
        let edges = inner.builder().enter(&graph);

        trim_edges(&trim_edges(inner, &edges), &trans)
    })
}

fn test_dataflow() {

    let start = time::precise_time_s();
    let start2 = start.clone();
    let mut computation = GraphRoot::new(ThreadCommunicator);

    let mut input = computation.subcomputation(|builder| {

        let (input, mut edges) = builder.new_input();

        edges = _trim_and_flip(&edges);
        edges = _trim_and_flip(&edges);
        edges = strongly_connected(&edges);

        edges.consolidate(|||x: &(u32, u32)| x.0)
            //  .inspect(|x| println!("{:?}", x));
             .inspect_batch(move |t, x| { println!("{}s:\tobserved at {:?}: {:?} changes",
                                                 ((time::precise_time_s() - start2)) - (t.inner as f64),
                                                 t, x.len()) });

        input
    });

    let node_count = 1_000_000;
    let edge_count = 2_000_000;

    let seed: &[_] = &[1, 2, 3, 4];
    let mut rng: StdRng = SeedableRng::from_seed(seed);
    rng.gen::<f64>();

    println!("determining SCC of {} nodes, {} edges:", node_count, edge_count);

    // let mut rng = rand::thread_rng();
    let mut edges = Vec::new();
    for _ in 0..edge_count {
        edges.push(((rng.gen_range(0, node_count), rng.gen_range(0, node_count)), 1));
    }

    // input.send_at(0, edges.clone().into_iter());
    {
        let mut slice = &edges[..];
        while slice.len() > 0 {
            let next = if slice.len() < 1000 { slice.len() } else { 1000 };
            input.send_at(0, slice[..next].to_vec().into_iter());
            computation.step();
            slice = &slice[next..];
        }
    }

    // let mut round = 0 as u32;
    // while computation.step() {
    //     if time::precise_time_s() - start >= round as f64 {
    //
    //         let new_record = ((rng.gen_range(0, node_count), rng.gen_range(0, node_count)), 1);
    //         let new_position = rng.gen_range(0, edges.len());
    //         let mut old_record = mem::replace(&mut edges[new_position], new_record.clone());
    //         old_record.1 = -1;
    //
    //         input.send_at(round, vec![old_record, new_record].into_iter());
    //         input.advance_to(round + 1);
    //         round += 1;
    //     }
    // }

    input.close();

    while computation.step() { }
    computation.step(); // shut down
}

// #[test]
// fn test_me () {
//     let mut trace = VectorCollectionTrace::new();
//     trace.set_difference((0,0), &mut vec![("a", 1), ("b", 2), ("c", 3)]);
//     trace.set_difference((0,1), &mut vec![("a", -1), ("c", -2)]);
//     trace.set_difference((1,0), &mut vec![("a", -1), ("c", -2)]);
//     trace.set_difference((1,1), &mut vec![("a", 1), ("b", -2), ("c", 2)]);
//     let mut result = Vec::new();
//     trace.get_collection(&(1,1), &mut result);
//     assert!(result == vec![("c", 1)]);
// }
