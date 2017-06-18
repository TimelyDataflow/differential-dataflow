#![feature(alloc_system)]

extern crate rand;
extern crate alloc_system;

extern crate libloading;

extern crate timely;
extern crate timely_communication;
extern crate differential_dataflow;
extern crate grapht;

use std::collections::HashMap;

use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely_communication::Allocator;

use libloading::{Library, Symbol};

use grapht::{RootTime, TraceHandle};

fn main() {

    // demonstrate dynamic loading of dataflows via shared libraries.
    timely::execute_from_args(std::env::args(), |worker| {

        // map from string name to arranged graph.
        let mut handles = HashMap::new();

        // common probe used by all dataflows to express progress information.
        let mut probe = timely::dataflow::operators::probe::Handle::new();

        // load the dataflow producing and maintaining a randomly changing graph.
        load_dataflow("dataflows/random_graph/target/release/librandom_graph.dylib".to_owned(), "build".to_owned(), worker, &mut handles, &mut probe);

        // load the dataflow which attaches a degree distribution computation.
        load_dataflow("dataflows/degr_dist/target/release/libdegr_dist.dylib".to_owned(), "build".to_owned(), worker, &mut handles, &mut probe);

        println!("loaded dataflow graph; up and running.");

    }).unwrap();

}

fn load_dataflow(lib_path: String, sym_name: String, worker: &mut Root<Allocator>, handles: &mut HashMap<String, TraceHandle>, probe: &mut ProbeHandle<RootTime>) {

    println!("loading: {:?} symbol: {:?}", lib_path, sym_name);

    // try to open the shared library
    if let Ok(lib) = Library::new(&lib_path) {

        // look up the symbol in the shared library
        worker.dataflow_using(lib, |lib, child| {
            unsafe {
                if let Ok(func) = lib.get::<Symbol<unsafe extern fn(&mut Child<Root<Allocator>,usize>, &mut HashMap<String, TraceHandle>, &mut ProbeHandle<RootTime>)>>(sym_name.as_bytes()) {
                    func(child, handles, probe)
                }
                else { println!("failed to find symbol in shared library."); }
            }
        });

    }
    else { println!("failed to open shared library."); }
}
