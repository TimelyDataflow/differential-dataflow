extern crate rand;

extern crate timely;
extern crate differential_dataflow;
extern crate dd_server;

use std::rc::Rc;
use std::cell::RefCell;

use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::operators::Probe;
use timely::dataflow::operators::generic::operator::source;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::arrange::ArrangeByKey;

use dd_server::{Environment, TraceHandle};

// load ./dataflows/random_graph/target/release/librandom_graph.dylib build <graph_name> 1000 2000 1000000
// load ./dataflows/random_graph/target/release/librandom_graph.dylib build <graph_name> 10000000 100000000 1000000
// drop <graph_name>-capability

#[no_mangle]
pub fn build((dataflow, handles, probe, args): Environment) -> Result<(), String> {

    // This call either starts or terminates the production of random graph edges.
    //
    // The arguments should be 
    //
    //    <graph_name> <nodes> <edges> <rate>
    //
    // where <rate> is the target number of edge changes per second. The source 
    // will play out changes to keep up with this, and timestamp them as if they
    // were emitted at the correct time. This currently uses a local timer, and
    // should probably take / use a "system time" from the worker.
    //
    // The method also registers a capability with name `<graph_name>-capability`, 
    // and will continue to execute until this capability is dropped from `handles`.

    if args.len() != 4 { return Err(format!("expected four arguments, instead: {:?}", args)); }

    let name = &args[0];
    let nodes: usize = args[1].parse().map_err(|_| format!("parse error, nodes: {:?}", args[1]))?;
    let edges: usize = args[2].parse().map_err(|_| format!("parse error, edges: {:?}", args[2]))?;
    let rate: usize = args[3].parse().map_err(|_| format!("parse error, rate: {:?}", args[3]))?;

    let requests_per_sec = rate;
    let ns_per_request = 1000000000 / requests_per_sec;

    // shared capability keeps graph generation going.
    let capability = Rc::new(RefCell::new(None));

    // create a trace from a source of random graph edges.
    let trace = 
        source(dataflow, "RandomGraph", |cap| {

            let index = dataflow.index();
            let peers = dataflow.peers();

            let seed: &[_] = &[1, 2, 3, index];
            let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
            let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

            // numbers of times we've stepped each RNG.
            let mut additions = 0;
            let mut deletions = 0;

            *capability.borrow_mut() = Some(cap);
            let capability = ::std::rc::Rc::downgrade(&capability);

            let timer = ::std::time::Instant::now();

            move |output| {

                // if our capability has not been cancelled ...
                if let Some(capability) = capability.upgrade() {

                    let mut borrow = capability.borrow_mut();
                    let capability = borrow.as_mut().unwrap();
                    let mut time = capability.time().clone();

                    // Open-loop latency-throughput test, parameterized by offered rate `ns_per_request`.
                    let elapsed = timer.elapsed();
                    let elapsed_ns = (elapsed.as_secs() as usize) * 1_000_000_000 + (elapsed.subsec_nanos() as usize);

                    {   // scope to allow session to drop, un-borrow.
                        let mut session = output.session(&capability);

                        let mut stepped = false;

                        // load initial graph.
                        while additions < edges {
                            time.inner = 0;
                            if additions % peers == index {
                                time.inner = 0;
                                let src = rng1.gen_range(0, nodes);
                                let dst = rng1.gen_range(0, nodes);
                                session.give(((src, dst), time, 1));
                            }
                            additions += 1;
                            stepped = true;
                        }

                        // ship any scheduled edge additions.
                        while ns_per_request * (additions - edges) < elapsed_ns {
                            if additions % peers == index {
                                time.inner = ns_per_request * (additions - edges);
                                let src = rng1.gen_range(0, nodes);
                                let dst = rng1.gen_range(0, nodes);
                                session.give(((src, dst), time, 1));
                            }
                            additions += 1;
                            stepped = true;
                        }

                        // ship any scheduled edge deletions.
                        while ns_per_request * deletions < elapsed_ns {
                            if deletions % peers == index {
                                time.inner = ns_per_request * deletions;
                                let src = rng2.gen_range(0, nodes);
                                let dst = rng2.gen_range(0, nodes);
                                session.give(((src, dst), time, 1));
                            }
                            deletions += 1;
                            stepped = true;
                        }

                        if stepped {
                            // println!("tick: additions: {:?}, deletions: {:?}", additions, deletions);
                        }
                    }

                    time.inner = elapsed_ns;
                    capability.downgrade(&time);
                }
            }
        })
        .probe_with(probe)
        .as_collection()
        .arrange_by_key_u()
        .trace;

    handles.set::<TraceHandle>(name.to_owned(), trace);
    handles.set(format!("{}-capability", name), capability);

    Ok(())
}