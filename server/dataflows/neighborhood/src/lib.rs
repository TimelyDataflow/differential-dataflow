extern crate timely;
extern crate timely_communication;
extern crate differential_dataflow;
extern crate dd_server;

use std::any::Any;
use std::collections::HashMap;

use timely_communication::Allocator;
use timely::dataflow::scopes::{Child, Root};
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::input::Input;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::arrange::ArrangeByKey;

use dd_server::{RootTime, TraceHandle};


// load ./dataflows/neighborhood/target/debug/libneighborhood.dylib build <graph_name> 0

#[no_mangle]
pub fn build(
    dataflow: &mut Child<Root<Allocator>,usize>, 
    handles: &mut HashMap<String, Box<Any>>, 
    probe: &mut ProbeHandle<RootTime>,
    args: &[String]) 
{
    println!("initializing neighborhood dataflow");

    if args.len() == 2 {

        let graph_name = &args[0];
        if let Some(boxed) = handles.get_mut(graph_name) {
            if let Some(mut handle) = boxed.downcast_mut::<TraceHandle>() {

                if let Ok(source) = args[1].parse::<usize>() {

                    let edges = handle.import(dataflow);
                    let (_input, query) = dataflow.new_collection_from(Some(source));

                    query
                        .map(|x| (x, x))
                        .arrange_by_key_u()
                        .join_core(&edges, |_n, &q, &d| Some((d, q)))
                        .arrange_by_key_u()
                        .join_core(&edges, |_n, &q, &d| Some((d, q)))
                        .arrange_by_key_u()
                        .join_core(&edges, |_n, &q, &d| Some((d, q)))
                        .map(|x| x.1)
                        .consolidate()
                        .inspect(|x| println!("{:?}", x))
                        .probe_with(probe);
                }
                else {
                    println!("failed to parse {:?} as usize", args[1]);
                }
            }
            else {
                println!("failed to downcast_mut to TraceHandle");
            }
        }
        else {
            println!("failed to find graph: {:?}", graph_name);
        }
    }
    else {
        println!("expected two arguments: <graph name> <source node>; instead: {:?}", args);
    }
}