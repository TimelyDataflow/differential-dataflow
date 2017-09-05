#![feature(allocator_api, global_allocator, alloc_system)]

extern crate alloc_system;
use alloc_system::System;

#[global_allocator]
static ALLOC: System = System;

extern crate libloading;

extern crate timely;
extern crate dd_server;

use std::io::BufRead;
use std::io::Write;

use std::rc::Rc;
use std::cell::RefCell;
use std::sync::{Arc, Mutex};

use timely::PartialOrder;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::generic::source;

use libloading::{Library, Symbol};

use dd_server::{Environment, TraceHandler};

fn main() {

    // shared queue of commands to serialize (in the "put in an order" sense).
    let (send, recv) = std::sync::mpsc::channel();
    let recv = Arc::new(Mutex::new(recv));

    // demonstrate dynamic loading of dataflows via shared libraries.
    let guards = timely::execute_from_args(std::env::args(), move |worker| {

        let recv = recv.clone();
        let timer = ::std::time::Instant::now();

        // map from string name to arranged graph.
        let mut handles = TraceHandler::new();//HashMap::new();

        // common probe used by all dataflows to express progress information.
        let mut probe = timely::dataflow::operators::probe::Handle::new();
        let mut probe2 = probe.clone();

        let command_queue = std::collections::VecDeque::new();
        let command_queue1 = Rc::new(RefCell::new(command_queue));
        let command_queue2 = command_queue1.clone();

        // build a dataflow used to serialize and circulate commands
        worker.dataflow(move |dataflow| {

            let peers = dataflow.peers();

            let mut recvd = Vec::new();

            // a source that attempts to pull from `recv` and produce commands for everyone
            source(dataflow, "InputCommands", move |mut capability| {

                // closure broadcasts any commands it grabs.
                move |output| {

                    // determine current nanoseconds
                    let elapsed = timer.elapsed();
                    let ns = (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as usize;

                    // this could be less frequent if needed.
                    let mut time = capability.time().clone();
                    time.inner = ns;

                    capability.downgrade(&time);

                    if let Ok(recv) = recv.try_lock() {
                        while let Ok(command) = recv.try_recv() {
                            let command: Vec<String> = command;
                            let mut session = output.session(&capability);
                            for worker_index in 0 .. peers {
                                session.give((worker_index, command.clone()));
                            }
                        }
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
                if let Ok(mut borrow) = command_queue1.try_borrow_mut() {
                    while recvd.len() > 0 && !notificator.frontier(0).iter().any(|x| x.less_than(&recvd[0].0)) {
                        borrow.push_back(recvd.remove(0));
                    }
                }
                else { panic!("failed to borrow shared command queue"); }

            })
            .probe_with(&mut probe2);

        });

        loop {

            if let Ok(mut borrow) = command_queue2.try_borrow_mut() {
                while let Some((_time, mut command)) = borrow.pop_front() {

                    let index = worker.index();
                    println!("worker {:?}: received command: {:?}", index, command);

                    if command.len() > 1 {
                        let operation = command.remove(0);
                        match operation.as_str() {
                            "load" => {
                                
                                if command.len() >= 2 {

                                    let library_path = &command[0];
                                    let symbol_name = &command[1];

                                    if let Ok(lib) = Library::new(library_path) {
                                        worker.dataflow_using(lib, |lib, child| {
                                            let result = unsafe {
                                                lib.get::<Symbol<unsafe fn(Environment)->Result<(),String>>>(symbol_name.as_bytes())
                                                .map(|func| func((child, &mut handles, &mut probe, &command[2..])))
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
            std::thread::yield_now();   // so that over-subscribed worker counts still feel interactive
        }
    });

    // the main thread now continues, to read from the console and issue work to the shared queue.

    std::io::stdout().flush().unwrap();
    let input = std::io::stdin();
    for line in input.lock().lines().map(|x| x.unwrap()) {
        let elts: Vec<_> = line.split_whitespace().map(|x| x.to_owned()).collect();

        if elts.len() > 0 {
            match elts[0].as_str() {
                "help" => { println!("valid commands are currently: bind, drop, exit, help, list, load"); },
                "bind" => { println!("ideally this would load and bind a library to some delightful name"); },
                "drop" => { send.send(elts).expect("failed to send command"); }
                "exit" => { println!("gotta ^C, sorry."); },
                "load" => { send.send(elts).expect("failed to send command"); },
                "list" => { println!("this is where we would list loaded collections"); },
                _ => { println!("unrecognized command: {:?}", elts[0]); },
            }
        }

        std::io::stdout().flush().unwrap();
    }

    guards.unwrap();
}