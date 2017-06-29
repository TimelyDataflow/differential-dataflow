extern crate rand;

extern crate timely;
extern crate timely_communication;
extern crate differential_dataflow;
extern crate dd_server;

use std::any::Any;
use std::collections::HashMap;

use rand::{Rng, SeedableRng, StdRng};

use timely_communication::Allocator;
use timely::dataflow::scopes::{Child, Root};
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::Probe;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::arrange::ArrangeByKey;

use dd_server::{Environment, RootTime, TraceHandle};

// load ./dataflows/random_graph/target/debug/librandom_graph.dylib build <graph_name> 1000 2000 10

#[no_mangle]
// pub fn build((dataflow, handles, probe, args): Environment) {
pub fn build(
    dataflow: &mut Child<Root<Allocator>,usize>, 
    handles: &mut HashMap<String, Box<Any>>, 
    probe: &mut ProbeHandle<RootTime>,
    args: &[String]) 
{
    if args.len() == 4 {

        let name = &args[0];
        let nodes: usize = args[1].parse().unwrap();
        let edges: usize = args[2].parse().unwrap();
        let batch: usize = args[3].parse().unwrap();

        // create a trace from a source of random graph edges.
        let trace: TraceHandle = timely::dataflow::operators::operator::source(dataflow, "RandomGraph", |mut capability| {

            let index = dataflow.index();
            let peers = dataflow.peers();

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
                    // println!("{:?}\tintroducing edges for batch starting {:?}", timer.elapsed(), time);

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

                    // println!("downgrading {:?} to {:?}", capability, time);
                    capability.downgrade(&time);
                }
            }
        })
        .probe_with(probe)
        .as_collection()
        .arrange_by_key_u()
        .trace;

        let boxed: Box<Any> = Box::new(trace);

        // println!("boxed correctly?: {:?}", boxed.downcast_ref::<TraceHandle>().is_some());

        handles.insert(name.to_owned(), boxed);
    }
    else {
        println!("expect four arguments, found: {:?}", args);
    }
}