extern crate timely;
extern crate timely_communication;
extern crate differential_dataflow;
extern crate dd_server;

use std::any::Any;
use std::collections::HashMap;

use timely_communication::Allocator;
use timely::dataflow::scopes::{Child, Root};
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::operators::CountTotal;

use dd_server::{Environment, RootTime, TraceHandle};

// load ./dataflows/degr_dist/target/debug/libdegr_dist.dylib build <graph_name>

#[no_mangle]
// pub fn build((dataflow, handles, probe, args): Environment) {
pub fn build(
    dataflow: &mut Child<Root<Allocator>,usize>, 
    handles: &mut HashMap<String, Box<Any>>, 
    probe: &mut ProbeHandle<RootTime>,
    args: &[String]) 
{
    if args.len() == 1 {

        let graph_name = &args[0];
        if let Some(boxed) = handles.get_mut(graph_name) {
            if let Some(mut handle) = boxed.downcast_mut::<TraceHandle>() {

                handle
                    .import(dataflow)
                    .as_collection(|k,v| (k.item.clone(), v.clone()))
                    .map(|(src, _dst)| src)
                    .count_total_u()
                    .map(|(_src, cnt)| cnt as usize)
                    .count_total_u()
                    .inspect(|x| println!("count: {:?}", x))
                    .probe_with(probe);
                }
            else {
                println!("failed to downcast: {:?}", graph_name);
            }
        }
        else {
            println!("failed to find graph: {:?}", graph_name);
        }
    }
}