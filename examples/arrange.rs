extern crate rand;
extern crate timely;
extern crate timely_sort;
extern crate differential_dataflow;

use timely::dataflow::operators::*;

use rand::{Rng, SeedableRng, StdRng};

use differential_dataflow::AsCollection;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::group::Group;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::operators::Iterate;
use differential_dataflow::operators::Consolidate;


// use differential_dataflow::trace::implementations::ord::OrdValSpine;
// use differential_dataflow::trace::{Cursor, Trace};
// use differential_dataflow::trace::Batch;
// use differential_dataflow::hashable::OrdWrapper;
// use differential_dataflow::trace::TraceReader;

fn main() {

    let nodes: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let edges: usize = std::env::args().nth(2).unwrap().parse().unwrap();
    let batch: usize = std::env::args().nth(3).unwrap().parse().unwrap();
    let pre: bool = std::env::args().nth(4).unwrap().parse().unwrap();

    // define a new timely dataflow computation. 
    timely::execute_from_args(std::env::args().skip(4), move |worker| {

        let index = worker.index();
        let peers = worker.peers();

        let mut probe = timely::dataflow::operators::probe::Handle::new();

        // create a dataflow managing an ever-changing edge collection.
    	let mut graph = worker.dataflow(|scope| {

            // create a source operator which will produce random edges and delete them.
            timely::dataflow::operators::operator::source(scope, "RandomGraph", |mut capability| {

                let seed: &[_] = &[1, 2, 3, index];
                let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
                let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

                let mut additions = 0;
                let mut deletions = 0;

                let handle = probe.clone();

                move |output| {

                    // do nothing if the probe is not caught up to us
                    if !handle.less_than(capability.time()) {

                        let mut time = capability.time().clone();
                        // println!("introducing edges for batch starting {:?}", time);

                        {   // scope to allow session to drop, un-borrow.
                            let mut session = output.session(&capability);

                            // we want to send at times.inner + (0 .. batch).
                            for _ in 0 .. batch {

                                while additions < time.inner + edges {
                                    if additions % peers == index {
                                        let src = rng1.gen_range(0, nodes);
                                        let dst = rng1.gen_range(0, nodes);
                                        session.give(((src, dst), time, 1));
                                    }
                                    additions += 1;
                                }
                                while deletions < time.inner {
                                    if deletions % peers == index {
                                        let src = rng2.gen_range(0, nodes);
                                        let dst = rng2.gen_range(0, nodes);
                                        session.give(((src, dst), time, -1));
                                    }
                                    deletions += 1;
                                }

                                time.inner += 1;
                            }
                        }

                        capability.downgrade(&time);
                    }
                }
            })
            .probe_with(&mut probe)
            .as_collection()
            .arrange_by_key_hashed()
            .trace
        });

        if pre {
            worker.step();
            worker.step();
            worker.step();
            worker.step();
            worker.step();
            worker.step();
        }

        let timer = ::std::time::Instant::now();

        let mut roots = worker.dataflow(|scope| {

            let edges = graph.import(scope);
            let (input, roots) = scope.new_input();
            let roots = roots.as_collection()
                             .map(|x| (x, 0));

            // repeatedly update minimal distances each node can be reached from each root
            roots.iterate(|dists| {

                let edges = edges.enter(&dists.scope());
                let roots = roots.enter(&dists.scope());

                dists.join_core(&edges, |_k,l,d| Some((*d, l+1)))
                     .concat(&roots)
                     .group_u(|_, s, t| t.push((s[0].0, 1)))
            })
            .map(|(_node, dist)| dist)
            .consolidate()
            .inspect(|x| println!("distance update: {:?}", x))
            .probe_with(&mut probe);

            input
        });

        let mut query = worker.dataflow(|scope| {

            let edges = graph.import(scope);
            let (input, query) = scope.new_input();
            let query = query.as_collection();

            query.map(|x| (x, x))
                 .join_core(&edges, |_n, &q, &d| Some((d, q)))
                 // .inspect(|x| println!("reachable @ 1: {:?}", x))
                 .join_core(&edges, |_n, &q, &d| Some((d, q)))
                 // .inspect(|x| println!("reachable @ 2: {:?}", x))
                 .join_core(&edges, |_n, &q, &d| Some((d, q)))
                 .map(|(_node, query)| query)
                 .consolidate()
                 // .inspect(|x| println!("neighbors @ 3: {:?}", x))
                 .probe_with(&mut probe);

            input
        });

        // the trace will not compact unless we release capabilities.
        // we drop rather than continually downgrade them as we run.
        drop(graph);

        if batch > 0 {
            let round = 0;
            // for round in 0 .. {

                let mut time = roots.time().clone();

                roots.send(((round % nodes), time, 1));
                query.send(((round % nodes), time, 1));

                time.inner += batch;

                roots.advance_to(time.inner);
                query.advance_to(time.inner);

                query.send(((round % nodes), time, -1));

                // println!("");
                worker.step_while(|| probe.less_than(&time));

                println!("done after: {:?}", timer.elapsed());
            // }
        }
    }).unwrap();
}