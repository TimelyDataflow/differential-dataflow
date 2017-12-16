extern crate timely;
extern crate differential_dataflow;
extern crate dd_server;

use std::rc::Rc;
use std::cell::RefCell;

use timely::dataflow::operators::inspect::Inspect;
use timely::dataflow::operators::Probe;
use differential_dataflow::operators::CountTotal;
use dd_server::{Environment, TraceHandle};

// load ./dataflows/degr_dist/target/release/libdegr_dist.dylib build <graph_name>

#[no_mangle]
pub fn build((dataflow, handles, probe, _timer, args): Environment) -> Result<(), String> {

    if args.len() != 1 { return Err(format!("expected one argument, instead: {:?}", args)); }

    let timer = _timer.clone();

    println!("{:?}: degree monitoring started", timer.elapsed());

    let mut delays = vec![0usize; 32];

    handles
        .get_mut::<Rc<RefCell<Option<TraceHandle>>>>(&args[0])?
        .borrow_mut().as_mut().unwrap()
        .import(dataflow)
        .as_collection(|k,v| (k.clone(), v.clone()))
        .map(|(src, _dst)| src as usize).count_total()
        .map(|(_src, cnt)| cnt as usize).count_total()
        // now we capture the observed latency on each record.
        .inner
        .inspect_batch(move |_, data| {

            let mut delays = [0usize; 32];

            let elapsed = timer.elapsed();
            let elapsed_ns = (elapsed.as_secs() as usize) * 1_000_000_000 + (elapsed.subsec_nanos() as usize);            
            for &(_, ref time, _) in data.iter() {
                let delay_us = (elapsed_ns - time.inner) / 1000;
                let bin = delay_us.next_power_of_two().trailing_zeros() as usize;
                delays[bin] += 1;
            }
            println!("delays: {:?}", delays);
        })
        .probe_with(probe);

    Ok(())
}