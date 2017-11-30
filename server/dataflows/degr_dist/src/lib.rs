extern crate differential_dataflow;
extern crate dd_server;

use std::rc::Rc;
use std::cell::RefCell;

use differential_dataflow::operators::CountTotal;
use dd_server::{Environment, TraceHandle};

// load ./dataflows/degr_dist/target/release/libdegr_dist.dylib build <graph_name>

#[no_mangle]
pub fn build((dataflow, handles, probe, _timer, args): Environment) -> Result<(), String> {

    if args.len() != 1 { return Err(format!("expected one argument, instead: {:?}", args)); }

    handles
        .get_mut::<Rc<RefCell<Option<TraceHandle>>>>(&args[0])?
        .borrow_mut().as_mut().unwrap()
        .import(dataflow)
        .as_collection(|k,v| (k.clone(), v.clone()))
        .map(|(src, _dst)| src as usize).count_total()
        .map(|(_src, cnt)| cnt as usize).count_total()
        .probe_with(probe);

    Ok(())
}