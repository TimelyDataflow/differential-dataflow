extern crate timely;
extern crate timely_communication;
extern crate differential_dataflow;
extern crate grapht;

use std::collections::HashMap;

use timely_communication::Allocator;
use timely::dataflow::scopes::{Child, Root};
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use differential_dataflow::input::Input;
use differential_dataflow::operators::{Group, Iterate, JoinCore, Consolidate};
use differential_dataflow::operators::arrange::ArrangeByKey;

use grapht::{RootTime, TraceHandle};


#[no_mangle]
pub fn build(
    dataflow: &mut Child<Root<Allocator>,usize>, 
    handles: &mut HashMap<String, TraceHandle>, 
    probe: &mut ProbeHandle<RootTime>) 
{
    println!("initializing reachability dataflow");

    if let Some(handle) = handles.get_mut("random") {

        let edges = handle.import(dataflow);
        let (_input, roots) = dataflow.new_collection::<_,isize>();

        let roots = roots.map(|x| (x, 0));

        // repeatedly update minimal distances each node can be reached from each root
        roots.iterate(|dists| {

            let edges = edges.enter(&dists.scope());
            let roots = roots.enter(&dists.scope());

            dists.arrange_by_key_u()
                 .join_core(&edges, |_k,l,d| Some((*d, l+1)))
                 .concat(&roots)
                 .group_u(|_, s, t| t.push((s[0].0, 1)))
        })
        .map(|(_node, dist)| dist)
        .consolidate()
        .inspect(|x| println!("distance update: {:?}", x))
        .probe_with(probe);

    }
    else {
        println!("failed to find graph: random");
    }
}