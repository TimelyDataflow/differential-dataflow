extern crate timely;
extern crate timely_communication;
extern crate differential_dataflow;
extern crate grapht;

use std::collections::HashMap;

use timely_communication::Allocator;
use timely::dataflow::scopes::{Child, Root};
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::input::Input;
use differential_dataflow::operators::JoinCore;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::operators::arrange::ArrangeByKey;

use grapht::{RootTime, TraceHandle};


#[no_mangle]
pub fn build(
    dataflow: &mut Child<Root<Allocator>,usize>, 
    handles: &mut HashMap<String, TraceHandle>, 
    probe: &mut ProbeHandle<RootTime>,
    args: &[String]) 
{
    println!("initializing neighborhood dataflow");

    if let Some(handle) = handles.get_mut("random") {

        let edges = handle.import(dataflow);
        let (_input, query) = dataflow.new_collection::<_,isize>();

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
        println!("failed to find graph: random");
    }
}