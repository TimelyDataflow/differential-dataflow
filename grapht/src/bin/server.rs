#![feature(alloc_system)]

extern crate rand;
extern crate alloc_system;

extern crate libloading;

extern crate timely;
extern crate timely_communication;
extern crate differential_dataflow;
extern crate grapht;

use std::io::BufRead;
use std::io::Write;
use std::collections::HashMap;

use std::sync::{Arc, RwLock};

use timely::dataflow::scopes::{Root, Child};
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely_communication::Allocator;

use libloading::{Library, Symbol};

use grapht::{RootTime, TraceHandle};

fn main() {

    let commands1 = Arc::new(RwLock::new(Vec::new()));
    let commands2 = commands1.clone();

    // demonstrate dynamic loading of dataflows via shared libraries.
    let guards = timely::execute_from_args(std::env::args(), move |worker| {

        let commands = commands1.clone();
        let mut command_count = 0;

        // map from string name to arranged graph.
        let mut handles = HashMap::new();

        // common probe used by all dataflows to express progress information.
        let mut probe = timely::dataflow::operators::probe::Handle::new();

        loop {

            if let Ok(lock) = commands.read() {
                while command_count < lock.len() {

                    let args: &Vec<String> = &lock[command_count];
                    command_count += 1;

                    if args.len() >= 2 {
                        load_dataflow(&args[0], &args[1], worker, &mut handles, &mut probe, &args[2..]);
                    }
                }
            }

            worker.step();
            // println!("stepping!");
        }

    });

    print!("> ");
    std::io::stdout().flush().unwrap();
    let input = std::io::stdin();
    for line in input.lock().lines().map(|x| x.unwrap()) {
        let elts: Vec<_> = line.split_whitespace().map(|x| x.to_owned()).collect();
        if let Ok(mut lock) = commands2.write() {
            lock.push(elts);
        }
        print!("> ");
        std::io::stdout().flush().unwrap();
    }

    guards.unwrap();
}

fn load_dataflow(
    lib_path: &str, 
    sym_name: &str, 
    worker: &mut Root<Allocator>, 
    handles: &mut HashMap<String, TraceHandle>, 
    probe: &mut ProbeHandle<RootTime>, 
    args: &[String]) 
{

    // println!("loading: {:?} symbol: {:?}", lib_path, sym_name);

    // try to open the shared library
    if let Ok(lib) = Library::new(lib_path) {

        // look up the symbol in the shared library
        worker.dataflow_using(lib, |lib, child| {
            unsafe {
                if let Ok(func) = lib.get::<Symbol<unsafe extern fn(&mut Child<Root<Allocator>,usize>, &mut HashMap<String, TraceHandle>, &mut ProbeHandle<RootTime>, &[String])>>(sym_name.as_bytes()) {
                    func(child, handles, probe, args)
                }
                else { println!("failed to find symbol in shared library."); }
            }
        });

    }
    else { println!("failed to open shared library."); }
}
