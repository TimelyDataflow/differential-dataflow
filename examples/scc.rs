extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::hash::Hash;
use std::mem;
use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::*;

use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use differential_dataflow::trace::Trace;
use differential_dataflow::operators::join::JoinArranged;
use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::hashable::UnsignedWrapper;
use differential_dataflow::trace::implementations::hash::HashValSpine as HashSpine;
use differential_dataflow::trace::implementations::hash::HashKeySpine as KeyHashSpine;


type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let inspect = std::env::args().find(|x| x == "inspect").is_some();

    timely::execute_from_args(std::env::args().skip(4), move |worker| {

        let timer = ::std::time::Instant::now();

        let (mut input, probe) = worker.dataflow::<u64,_,_>(|scope| {

            let (input, edges) = scope.new_input();
            let mut edges = Collection::new(edges);

            edges = _trim_and_flip_u(&edges);
            edges = _trim_and_flip_u(&edges);
            // edges = _strongly_connected(&edges);

            if inspect {
                let mut counter = 0;
                edges = edges.consolidate()
                             .inspect_batch(move |t, xs| {
                                 counter += xs.len();
                                 println!("{:?}:\tobserved at {:?}: {:?} changes", timer.elapsed(), t, counter)
                                 // println!("{:?}:\tcount{:?}", t, xs);
                             });
            }

            (input, edges.probe().0)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);
        let mut rng2: StdRng = SeedableRng::from_seed(seed);

        if worker.index() == 0 {

            // println!("determining SCC of {} nodes, {} edges:", nodes, edges);
            let seed: &[_] = &[1, 2, 3, 4];
            let mut rng: StdRng = SeedableRng::from_seed(seed);

            let mut names = Vec::with_capacity(nodes as usize);
            for _index in 0 .. nodes {
              names.push(rng.gen_range(0, u32::max_value()));
              // names.push(index as u32);
            }

            rng1.gen::<f64>();
            rng2.gen::<f64>();

            let &time = input.time();
            for index in 0..edges {
                let edge = (names[rng1.gen_range(0, nodes) as usize], names[rng1.gen_range(0, nodes) as usize]);
                input.send((edge, time, 1));
                if (index % (1 << 12)) == 0 {
                    worker.step();
                }
            }

            println!("input ingested after {:?}", timer.elapsed());
        }

        if batch > 0 {
            let mut changes = Vec::with_capacity(2 * batch);
            for _ in 0 .. {
                if worker.index() == 0 {
                    let &time = input.time();
                    for _ in 0 .. batch {
                        changes.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), time, 1));
                        changes.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)), time,-1));
                    }
                }

                let timer = ::std::time::Instant::now();
                let round = *input.epoch();
                if worker.index() == 0 {
                    while let Some(change) = changes.pop() {
                        input.send(change);
                    }
                }
                input.advance_to(round + 1);
                worker.step_while(|| probe.lt(&input.time()));

                if worker.index() == 0 {
                    let elapsed = timer.elapsed();
                    println!("{}", elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
                }
            }
        }

        // println!("finished after {:?}", timer.elapsed());

    }).unwrap();
}

fn _trim_and_flip_u<G: Scope>(graph: &Collection<G, Edge>) -> Collection<G, Edge>
where G::Timestamp: Lattice+Ord+Hash {
    graph.iterate(|edges| {
        // keep edges from active edge destinations.

        let active = edges.map(|(_,k)| (UnsignedWrapper::from(k), ()))
                          .arrange(KeyHashSpine::new())
                          .group_arranged(|_k,_s,t| t.push(((), 1)), KeyHashSpine::new());

        graph.enter(&edges.scope())
             .map(|(k,v)| (UnsignedWrapper::from(k), v))
             .arrange(HashSpine::new())
             .join_arranged(&active, |k,v,_| (k.item.clone(), v.clone()))

        // let active = edges.map(|(_,dst)| dst).distinct_u();
        // graph.enter(&edges.scope())
        //      .semijoin_u(&active)
    })
    .map_in_place(|x| mem::swap(&mut x.0, &mut x.1))
}

fn _trim_and_flip<G: Scope>(graph: &Collection<G, Edge>) -> Collection<G, Edge>
where G::Timestamp: Lattice+Ord+Hash {
    graph.iterate(|edges| {
        // keep edges from active edge destinations.
        let active = edges.map(|(_,dst)| dst).distinct();
        // active.inner.inspect_batch(|t,xs| println!("{:?}\tcount: {:?}", t, xs));
        graph.enter(&edges.scope())
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

    edges.join_map_u(&labels, |&e1,&e2,&l1| (e2,(e1,l1)))
         .join_map_u(&labels, |&e2,&(e1,l1),&l2| ((e1,e2),(l1,l2)))
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
                  .group(|_, s, t| t.push((s[0].0, 1)))

         })
}
