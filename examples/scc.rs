extern crate rand;
extern crate timely;
extern crate differential_dataflow;

use std::hash::Hash;
use std::mem;
use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;

use differential_dataflow::trace::Trace;
// use differential_dataflow::operators::join::JoinArranged;
use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::operators::arrange::{ArrangeByKey, ArrangeBySelf};
// use differential_dataflow::hashable::UnsignedWrapper;

// use differential_dataflow::trace::implementations::ord::OrdValSpine;// as HashSpine;
use differential_dataflow::trace::implementations::ord::OrdKeySpine;// as OrdKeyHashSpine;


type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let inspect = std::env::args().nth(4).unwrap() == "inspect";

    timely::execute_from_args(std::env::args().skip(5), move |worker| {

        let timer = ::std::time::Instant::now();

        let (mut input, probe) = worker.dataflow::<u64,_,_>(|scope| {

            let (input, mut edges) = scope.new_collection();

            edges = _trim_and_flip(&edges);
            edges = _trim_and_flip(&edges);
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

            (input, edges.probe())
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);
        let mut rng2: StdRng = SeedableRng::from_seed(seed);

        if worker.index() == 0 {

            rng1.gen::<f64>();
            rng2.gen::<f64>();

            for _index in 0..edges {
                input.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
            }

            println!("input ingested after {:?}", timer.elapsed());
        }

        if batch > 0 {
            for _ in 0 .. {
                let timer = ::std::time::Instant::now();
                let round = *input.epoch();
                if worker.index() == 0 {
                      for _ in 0 .. batch {
                        input.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                        input.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
                    }
                }
                input.advance_to(round + 1);
                input.flush();
                worker.step_while(|| probe.less_than(&input.time()));

                if worker.index() == 0 {
                    let elapsed = timer.elapsed();
                    println!("{}", elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
                }
            }
        }

        // println!("finished after {:?}", timer.elapsed());

    }).unwrap();
}

fn _trim_and_flip<G: Scope>(graph: &Collection<G, Edge>) -> Collection<G, Edge>
where G::Timestamp: Lattice+Ord+Hash {
    graph.iterate(|edges| {
        // keep edges from active edge destinations.
        let active = edges.map(|(src,_dst)| src)
                          .arrange_by_self()
                          .group_arranged(|_k,_s,t| t.push(((), 1)), OrdKeySpine::new());

        graph.enter(&edges.scope())
             .arrange_by_key()
             .join_core(&active, |k,v,_| Some((k.clone(), v.clone())))
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
