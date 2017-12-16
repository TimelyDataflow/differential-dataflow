#![feature(allocator_api, global_allocator, alloc_system)]

extern crate alloc_system;
use alloc_system::System;

#[global_allocator]
static ALLOC: System = System;

extern crate libloading;

extern crate timely_communication;
extern crate timely;
extern crate dd_server;

use std::io::BufRead;
use std::io::Write;

use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Weak, Mutex};

use std::collections::VecDeque;
use std::sync::mpsc::Receiver;

use timely_communication::Allocate;

use timely::PartialOrder;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::dataflow::scopes::root::Root;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::generic::source;

use libloading::{Library, Symbol};

use dd_server::{Environment, TraceHandler};

fn main() {

    // shared queue of commands to serialize (in the "put in an order" sense).
    let (send, recv) = std::sync::mpsc::channel();
    let recv = Arc::new(Mutex::new(recv));
    let weak = Arc::downgrade(&recv);

    // demonstrate dynamic loading of dataflows via shared libraries.
    let guards = timely::execute_from_args(std::env::args(), move |worker| {

        let timer = ::std::time::Instant::now();

        // map from string name to arranged graph.
        let mut handles = TraceHandler::new();

        // common probe used by all dataflows to express progress information.
        let mut probe = timely::dataflow::operators::probe::Handle::new();

        // queue shared between serializer (producer) and command loop (consumer).
        let command_queue_strong = Rc::new(RefCell::new(VecDeque::new()));
        build_command_serializer(worker, timer, weak.clone(), &command_queue_strong, &mut probe);
        let command_queue = Rc::downgrade(&command_queue_strong);
        drop(command_queue_strong);

        // continue running as long as we haven't dropped the queue.
        while let Some(command_queue) = command_queue.upgrade() {

            if let Ok(mut borrow) = command_queue.try_borrow_mut() {
                while let Some(mut command) = borrow.pop_front() {

                    let index = worker.index();
                    println!("worker {:?}: received command: {:?}", index, command);

                    if command.len() > 1 {
                        let operation = command.remove(0);
                        match operation.as_str() {
                            "list" => {
                                println!("worker {:?} listing", index);
                                for key in handles.keys() {
                                    println!("worker {:?} list: {:?}", index, key);
                                }
                            }
                            "load" => {
                                
                                if command.len() >= 2 {

                                    let library_path = &command[0];
                                    let symbol_name = &command[1];

                                    if let Ok(lib) = Library::new(library_path) {
                                        worker.dataflow_using(lib, |lib, child| {
                                            let result = unsafe {
                                                lib.get::<Symbol<unsafe fn(Environment)->Result<(),String>>>(symbol_name.as_bytes())
                                                .map(|func| func((child, &mut handles, &mut probe, &timer, &command[2..])))
                                            };

                                            match result {
                                                Err(_) => { println!("worker {:?}: failed to find symbol {:?} in shared library {:?}.", index, symbol_name, library_path); },
                                                Ok(Err(x)) => { println!("worker {:?}: error: {:?}", index, x); },
                                                Ok(Ok(())) => { /* Good news, everyone! */ },
                                            }
                                        });
                                    }
                                    else { 
                                        println!("worker {:?}: failed to open shared library: {:?}", index, library_path);
                                    }
                                }
                            },
                            "drop" => {
                                for name in command.iter() {
                                    handles.remove(name);
                                }
                            }
                            _ => {
                                println!("worker {:?}: unrecognized command: {:?}", index, operation);                                
                            }
                        }
                    }
                }
            }

            // arguably we should pick a time (now) and `step_while` until it has passed. 
            // this should ensure that we actually fully drain ranges of updates, rather
            // than providing no guaranteed progress for e.g. iterative computations.

            worker.step();
        }

        println!("worker {}: command queue unavailable; exiting command loop.", worker.index());
    });

    // the main thread now continues, to read from the console and issue work to the shared queue.

    std::io::stdout().flush().unwrap();
    let input = std::io::stdin();

    let mut done = false;

    while !done {

        if let Some(line) = input.lock().lines().map(|x| x.unwrap()).next() {
            let elts: Vec<_> = line.split_whitespace().map(|x| x.to_owned()).collect();

            if elts.len() > 0 {
                match elts[0].as_str() {
                    "help" => { println!("valid commands are currently: bind, drop, exit, help, list, load"); },
                    "bind" => { println!("ideally this would load and bind a library to some delightful name"); },
                    "drop" => { send.send(elts).expect("failed to send command"); }
                    "exit" => { done = true; },
                    "load" => { send.send(elts).expect("failed to send command"); },
                    "list" => { send.send(elts).expect("failed to send command"); },
                    _ => { println!("unrecognized command: {:?}", elts[0]); },
                }
            }

            std::io::stdout().flush().unwrap();
        }
    }

    println!("main: exited command loop");
    drop(send); // workers will only terminate if they know the input command queue is closed.
    drop(recv);

    guards.unwrap();
}

fn build_command_serializer<A: Allocate>(
    worker: &mut Root<A>,
    timer: ::std::time::Instant,
    input: Weak<Mutex<Receiver<Vec<String>>>>,
    target: &Rc<RefCell<VecDeque<Vec<String>>>>,
    handle: &mut ProbeHandle<Product<RootTimestamp, usize>>,
) {

    let target = target.clone();

    // build a dataflow used to serialize and circulate commands
    worker.dataflow(move |dataflow| {

        let peers = dataflow.peers();
        let mut recvd = Vec::new();

        // a source that attempts to pull from `recv` and produce commands for everyone
        source(dataflow, "InputCommands", move |capability| {

            // so we can drop, if input queue vanishes.
            let mut capability = Some(capability);

            // closure broadcasts any commands it grabs.
            move |output| {

                if let Some(input) = input.upgrade() {

                    // determine current nanoseconds
                    if let Some(capability) = capability.as_mut() {

                        // this could be less frequent if needed.
                        let mut time = capability.time().clone();
                        let elapsed = timer.elapsed();
                        time.inner = (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as usize;

                        // downgrade the capability.
                        capability.downgrade(&time);

                        if let Ok(input) = input.try_lock() {
                            while let Ok(command) = input.try_recv() {
                                let command: Vec<String> = command;
                                let mut session = output.session(&capability);
                                for worker_index in 0 .. peers {
                                    session.give((worker_index, command.clone()));
                                }
                            }
                        }
                    }
                    else { panic!("command serializer: capability lost while input queue valid"); }
                }
                else {
                    capability = None;
                }
            }
        })
        .unary_notify(
            Exchange::new(|x: &(usize, Vec<String>)| x.0 as u64), 
            "InputCommandsRecv", 
            Vec::new(), 
            move |input, output, notificator| {

            // grab each command and queue it up
            input.for_each(|time, data| {
                recvd.extend(data.drain(..).map(|(_,command)| (time.time().clone(), command)));
                if false { output.session(&time).give(0u64); }
            });

            recvd.sort();

            // try to move any commands at completed times to a shared queue.
            if let Ok(mut borrow) = target.try_borrow_mut() {
                while recvd.len() > 0 && !notificator.frontier(0).iter().any(|x| x.less_than(&recvd[0].0)) {
                    borrow.push_back(recvd.remove(0).1);
                }
            }
            else { panic!("failed to borrow shared command queue"); }

        })
        .probe_with(handle);
    });
}
