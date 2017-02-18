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

type Node = u32;
type Edge = (Node, Node);

fn main() {

    let nodes: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: u32 = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let inspect = std::env::args().find(|x| x == "inspect").is_some();

    timely::execute_from_args(std::env::args().skip(4), move |computation| {

        let timer = ::std::time::Instant::now();

        let (mut input, probe) = computation.scoped::<u64,_,_>(|scope| {

            let (input, edges) = scope.new_input();
            let mut edges = Collection::new(edges);

            edges = _trim_and_flip(&edges);
            edges = _trim_and_flip(&edges);
            edges = _strongly_connected(&edges);

            if inspect {
                let mut counter = 0;
                edges = edges.consolidate_by(|x| x.0)
                             .inspect_batch(move |t, x| {
                                 counter += x.len();
                                 println!("{:?}:\tobserved at {:?}: {:?} changes", timer.elapsed(), t, counter)
                             });
            }

            (input, edges.probe().0)
        });

        let seed: &[_] = &[1, 2, 3, 4];
        let mut rng1: StdRng = SeedableRng::from_seed(seed);
        let mut rng2: StdRng = SeedableRng::from_seed(seed);

        if computation.index() == 0 {

            // println!("determining SCC of {} nodes, {} edges:", nodes, edges);

            rng1.gen::<f64>();
            rng2.gen::<f64>();

            for index in 0..edges {
                let edge = (rng1.gen_range(0, nodes), rng1.gen_range(0, nodes));
                input.send((edge, 1));
                if (index % (1 << 12)) == 0 {
                    computation.step();
                }
            }

            // println!("input ingested after {:?}", timer.elapsed());
        }

        if batch > 0 {
            let mut changes = Vec::with_capacity(2 * batch);
            for _ in 0 .. {
                if computation.index() == 0 {
                    for _ in 0 .. batch {
                        changes.push(((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)), 1));
                        changes.push(((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)),-1));
                    }
                }

                let timer = ::std::time::Instant::now();
                let round = *input.epoch();
                if computation.index() == 0 {
                    while let Some(change) = changes.pop() {
                        input.send(change);
                    }
                }
                input.advance_to(round + 1);
                computation.step_while(|| probe.lt(&input.time()));

                if computation.index() == 0 {
                    let elapsed = timer.elapsed();
                    println!("{}", elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
                }
            }
        }
    }).unwrap();
}

fn _trim_and_flip<G: Scope>(graph: &Collection<G, Edge>) -> Collection<G, Edge>
where G::Timestamp: Lattice+Ord {
    graph.iterate(|edges| {
        let inner = graph.enter(&edges.scope())
                         .map_in_place(|x| mem::swap(&mut x.0, &mut x.1));

        edges.map(|(x,_)| x)
             .distinct()
             .map(|x| (x,()))
             .join_map(&inner, |&d,_,&s| (s,d))
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
                     .consolidate_by(|&x| x.0);

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
             let nodes = nodes.enter_at(&inner.scope(), |r| 256 * (64 - ((r.0).0 as u64).leading_zeros() as u64));

             inner.join_map(&edges, |_k,l,d| (*d,*l))
                  .concat(&nodes)
                  .group(|_, s, t| t.push((s[0].0, 1)))

         })
}
