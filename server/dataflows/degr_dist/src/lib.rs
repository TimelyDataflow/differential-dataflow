extern crate differential_dataflow;
extern crate dd_server;

use differential_dataflow::operators::CountTotal;
use dd_server::{Environment, TraceHandle};

// load ./dataflows/degr_dist/target/debug/libdegr_dist.dylib build <graph_name>

#[no_mangle]
pub fn build((dataflow, handles, probe, args): Environment) -> Result<(), String> {

    if args.len() != 1 { return Err(format!("expected one argument, instead: {:?}", args)); }

    handles
        .get_mut::<TraceHandle>(&args[0])?
        .import(dataflow)
        .as_collection(|k,v| (k.item.clone(), v.clone()))
        .map(|(src, _dst)| src as usize).count_total_u()
        .map(|(_src, cnt)| cnt as usize).count_total_u()
        .inspect(|x| println!("count: {:?}", x))
        .probe_with(probe);

    Ok(())
}