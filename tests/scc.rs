extern crate rand;
extern crate time;
extern crate timely;
extern crate differential_dataflow;

use std::rc::Rc;
use std::cell::RefCell;

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

fn _trim_and_flip<G: GraphBuilder, U: UnsignedInt>(graph: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)> where G::Timestamp: LeastUpperBound {

        graph.iterate(u32::max_value(), |x|x.0, |x|x.0, |edges| {
            let inner = edges.builder().enter(&graph);
            edges.map(|((x,_),w)| (x,w))
                 .group_by_u(|x|(x,()), |&x,_| x, |&x,_,target| target.push((x,1)))
                  .join_u(&inner, |x| (x,()), |(s,d)| (d,s), |&d,_,&s| (s,d))
             })
             .consolidate(|x| x.0, |x| x.0)
             .map(|((x,y),w)| ((y,x),w))
}

fn improve_labels<G: GraphBuilder, U: UnsignedInt>(labels: &Stream<G, ((U, U), i32)>, edges: &Stream<G, ((U, U), i32)>, nodes: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound {

    labels.join_u(&edges, |l| l, |e| e, |_k,l,d| (*d,*l))
          .concat(&nodes)
          .group_by_u(|x| x, |k,v| (*k,*v), |_, mut s, t| { t.push((*s.peek().unwrap().0, 1)); } )
}

fn _reachability<G: GraphBuilder, U: UnsignedInt>(edges: &Stream<G, ((U, U), i32)>, nodes: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(u32::max_value(), |x| x.0, |x| x.0, |inner| {
             let edges = inner.builder().enter(&edges);
             let nodes = inner.builder().enter_at(&nodes, |r| 256 * (64 - (r.0).1.as_u64().leading_zeros() as u32));

             improve_labels(inner, &edges, &nodes)
         })
         .consolidate(|x| x.0, |x| x.0)
}


fn fancy_reachability<G: GraphBuilder, U: UnsignedInt>(edges: &Stream<G, ((U, U), i32)>, nodes: &Stream<G, (U, i32)>)
    -> Stream<G, ((U, U), i32)>
where G::Timestamp: LeastUpperBound+Hash {

    edges.filter(|_| false)
         .iterate(u32::max_value(), |x| x.0, |x| x.0, |inner| {
             let edges = inner.builder().enter(&edges);
             let nodes = inner.builder().enter(&nodes)
                              .map(|(x,_)| (x,1))
                              .except(&inner.filter(|&((ref n, ref l),_)| l < n).map(|((n,_),w)| (n,w)))
                              .delay(|r,t| { let mut t2 = t.clone(); t2.inner = 256 * (64 - r.0.as_u64().leading_zeros() as u32); t2 })
                              .consolidate(|&x| x, |&x| x)
                              .map(|(x,w)| ((x,x),w));

             improve_labels(inner, &edges, &nodes)
         })
         .consolidate(|x| x.0, |x| x.0)
}

fn trim_edges<G: GraphBuilder, U: UnsignedInt>(cycle: &Stream<G, ((U, U), i32)>,
                                               edges: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)> where G::Timestamp: LeastUpperBound+Hash {

    let nodes = edges.map(|((_,y),w)| (y,w)).consolidate(|&x| x, |&x| x);

    let labels = fancy_reachability(&cycle, &nodes);

    edges.join_u(&labels, |e| e, |l| l, |&e1,&e2,&l1| (e2,(e1,l1)))
         .join_u(&labels, |e| e, |l| l, |&e2,&(e1,l1),&l2| ((e1,e2),(l1,l2)))
         .consolidate(|x|(x.0).0, |x|(x.0).0)
         .filter(|&((_,(l1,l2)), _)| l1 == l2)
         .map(|(((x1,x2),_),d)| ((x2,x1),d))
}

fn strongly_connected<G: GraphBuilder, U: UnsignedInt>(graph: &Stream<G, ((U, U), i32)>)
    -> Stream<G, ((U, U), i32)> where G::Timestamp: LeastUpperBound+Hash {

    graph.iterate(u32::max_value(), |x| x.0, |x| x.0, |inner| {
        let trans = inner.builder().enter(&graph).map(|((x,y),w)| ((y,x),w));
        let edges = inner.builder().enter(&graph);

        trim_edges(&trim_edges(inner, &edges), &trans)
    })
}

#[test]
fn _find_difference() {

    for i in 1..100 {
        println!("testing: {}", i);
        let node_count = i;
        let edge_count = 2 * i;

        for seed_idx in 0..1 {

            let seed: &[_] = &[1, 2, 3, seed_idx];
            let mut rng: StdRng = SeedableRng::from_seed(seed);

            let mut edges = Vec::new();
            for _ in 0..edge_count {
                edges.push(((rng.gen_range(0, node_count), rng.gen_range(0, node_count)), 1))
            }

            let ans1 = _scc1(&edges);
            let ans2 = _scc2(&edges);

            ans1.borrow_mut().sort();
            ans2.borrow_mut().sort();

            assert!(*ans1.borrow() == *ans2.borrow());
            // if ans1 != ans2 {
            //     println!("discrepancy found for ({}, {}), seed {}", node_count, edge_count, seed_idx);
            //     println!("graph:\t{:?}", edges);
            //     println!("ans1:\t{:?}", ans1);
            //     println!("ans2:\t{:?}", ans2);
            //     return();
            // }
        }
    }
}

fn _scc1(edges: &Vec<((u32, u32), i32)>) -> Rc<RefCell<Vec<((u32, u32), i32)>>> {
    let result = Rc::new(RefCell::new(Vec::new()));
    {
        let mut computation = GraphRoot::new(ThreadCommunicator);
        let mut input = computation.subcomputation(|builder| {

            let result = result.clone();
            let (input, mut edges) = builder.new_input();

            edges = _trim_and_flip(&edges);
            edges = _trim_and_flip(&edges);

            edges = strongly_connected(&edges);

            edges.consolidate(|x: &(u32, u32)| x.0, |x: &(u32, u32)| x.0)
                 .inspect(move |x| result.borrow_mut().push(x.clone()));

            input
        });

        input.send_at(0, edges.clone().into_iter());
        input.close();

        while computation.step() { }
    }

    result
}


fn _scc2(edges: &Vec<((u32, u32), i32)>) -> Rc<RefCell<Vec<((u32, u32), i32)>>> {
    let result = Rc::new(RefCell::new(Vec::new()));
    {
        let mut computation = GraphRoot::new(ThreadCommunicator);
        let mut input = computation.subcomputation(|builder| {

            let result = result.clone();
            let (input, mut edges) = builder.new_input();

            // edges = _trim_and_flip(edges);
            // edges = _trim_and_flip(edges);

            edges = strongly_connected(&edges);

            edges.consolidate(|x: &(u32, u32)| x.0, |x: &(u32, u32)| x.0)
                 .inspect(move |x| result.borrow_mut().push(x.clone()));

            input
        });

        input.send_at(0, edges.clone().into_iter());
        input.close();

        while computation.step() { }
    }

    result
}
