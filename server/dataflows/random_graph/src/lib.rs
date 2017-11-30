extern crate rand;

extern crate timely;
extern crate differential_dataflow;
extern crate dd_server;

use std::rc::Rc;
use std::cell::RefCell;

use rand::{Rng, SeedableRng, StdRng};

use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::generic::operator::source;

use differential_dataflow::AsCollection;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::trace::TraceReader;

use dd_server::{Environment, TraceHandle};

// load ./dataflows/random_graph/target/release/librandom_graph.dylib build <graph_name> 1000 2000 1000000
// load ./dataflows/random_graph/target/release/librandom_graph.dylib build <graph_name> 10000000 100000000 1000000
// drop <graph_name>-capability

#[no_mangle]
pub fn build((dataflow, handles, probe, timer, args): Environment) -> Result<(), String> {

    // This call either starts or terminates the production of random graph edges.
    //
    // The arguments should be 
    //
    //    <graph_name> <nodes> <edges> <rate>
    //
    // where <rate> is the target number of edge changes per second. The source 
    // will play out changes to keep up with this, and timestamp them as if they
    // were emitted at the correct time. This currently uses a local timer, created
    // when this method is called, and should probably take / use a "system time" 
    // from the worker so that timestamps align. Until then, it is a fine test that
    // things work even without synchronized timestamps.
    //
    // The method also registers a capability with name `<graph_name>-capability`, 
    // and will continue to execute until this capability is dropped from `handles`.
    // 
    // The operator also holds an `Weak<RefCell<Option<TraceHandle>>>` which it will
    // attempt to borrow and call `advance_by` in order to advance the capability
    // as it runs, to allow compaction and the maintenance of bounded state.

    if args.len() != 4 { return Err(format!("expected four arguments, instead: {:?}", args)); }

    let name = &args[0];
    let nodes: usize = args[1].parse().map_err(|_| format!("parse error, nodes: {:?}", args[1]))?;
    let edges: usize = args[2].parse().map_err(|_| format!("parse error, edges: {:?}", args[2]))?;
    let rate: usize = args[3].parse().map_err(|_| format!("parse error, rate: {:?}", args[3]))?;

    let requests_per_sec = rate;
    let ns_per_request = 1000000000 / requests_per_sec;

    // shared capability keeps graph generation going.
    let capability = Rc::new(RefCell::new(None));

    // shared (optional) trace handle, so that the operator can advance capabilities.
    let trace_handle: Rc<RefCell<Option<TraceHandle>>> = Rc::new(RefCell::new(None));
    let trace_handle_weak = Rc::downgrade(&trace_handle);

    let timer = timer.clone();

    // create a trace from a source of random graph edges.
    let mut trace = 
        source(dataflow, "RandomGraph", |cap| {

            let index = dataflow.index();
            let peers = dataflow.peers();

            // RNGs for edge addition and deletion.
            let seed: &[_] = &[1, 2, 3, index];
            let mut rng1: StdRng = SeedableRng::from_seed(seed);
            let mut rng2: StdRng = SeedableRng::from_seed(seed);

            // numbers of times we've stepped each RNG.
            let mut additions = 0;
            let mut deletions = 0;

            // record delay between system start-up and operator start-up.
            let delay = timer.elapsed();
            let delay_ns = (delay.as_secs() as usize) * 1_000_000_000 + (delay.subsec_nanos() as usize);

            // stash capability in a rc::Weak.
            *capability.borrow_mut() = Some(cap);
            let capability = ::std::rc::Rc::downgrade(&capability);

            move |output| {

                // Open-loop latency-throughput test, parameterized by offered rate `ns_per_request`.
                let elapsed = timer.elapsed();
                let elapsed_ns = (elapsed.as_secs() as usize) * 1_000_000_000 + (elapsed.subsec_nanos() as usize);

                // attempt to advance the frontier of the trace handle.
                if let Some(trace_handle) = trace_handle_weak.upgrade() {
                    let mut borrow = trace_handle.borrow_mut();
                    if let Some(ref mut trace_handle) = borrow.as_mut() {
                        trace_handle.advance_by(&[Product::new(RootTimestamp, elapsed_ns)]);
                    }
                }

                // if our capability has not been cancelled ...
                if let Some(capability) = capability.upgrade() {

                    let mut borrow = capability.borrow_mut();
                    let capability = borrow.as_mut().unwrap();
                    let mut time = capability.time().clone();

                    {   // scope to allow session to drop, un-borrow.
                        let mut session = output.session(&capability);

                        // load initial graph.
                        while additions < edges + deletions {
                            time.inner = 0;
                            if additions % peers == index {
                                time.inner = 0;
                                let src = rng1.gen_range(0, nodes);
                                let dst = rng1.gen_range(0, nodes);
                                session.give(((src, dst), time, 1));
                            }
                            additions += 1;
                        }

                        // ship any scheduled edge additions.
                        while ns_per_request * (additions - edges) < (elapsed_ns - delay_ns) {
                            if additions % peers == index {
                                time.inner = ns_per_request * (additions - edges);
                                let src = rng1.gen_range(0, nodes);
                                let dst = rng1.gen_range(0, nodes);
                                session.give(((src, dst), time, 1));
                            }
                            additions += 1;
                        }

                        // ship any scheduled edge deletions.
                        while ns_per_request * deletions < (elapsed_ns - delay_ns) {
                            if deletions % peers == index {
                                time.inner = ns_per_request * deletions;
                                let src = rng2.gen_range(0, nodes);
                                let dst = rng2.gen_range(0, nodes);
                                session.give(((src, dst), time, -1));
                            }
                            deletions += 1;
                        }
                    }

                    time.inner = elapsed_ns;
                    capability.downgrade(&time);
                }
            }
        })
        .probe_with(probe)
        .as_collection()
        .arrange_by_key()
        .trace;

    // release all blocks on merging.
    trace.distinguish_since(&[]);
    *trace_handle.borrow_mut() = Some(trace);

    handles.set::<Rc<RefCell<Option<TraceHandle>>>>(name.to_owned(), trace_handle);
    handles.set(format!("{}-capability", name), capability);

    Ok(())
}