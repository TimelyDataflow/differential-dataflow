extern crate timely;
extern crate differential_dataflow;
extern crate interactive;

use timely::synchronization::Sequencer;
use interactive::{Manager, Command, Value};

fn main() {

    let mut args = std::env::args();
    args.next();

    use std::sync::{Arc, Mutex};
    use std::collections::VecDeque;

    let command_queue = Arc::new(Mutex::new(VecDeque::<Command<Value>>::new()));
    let command_queue2 = command_queue.clone();

    // Detached thread for client connections.
    std::thread::Builder::new()
        .name("Listener".to_string())
        .spawn(move || {

            use std::net::TcpListener;
            let listener = TcpListener::bind("127.0.0.1:8000".to_string()).expect("failed to bind listener");
            for mut stream in listener.incoming() {
                let mut stream = stream.expect("listener error");
                let send = command_queue2.clone();
                std::thread::Builder::new()
                    .name("Client".to_string())
                    .spawn(move || {
                        while let Ok(command) = bincode::deserialize_from::<_,Command<Value>>(&mut stream) {
                            send.lock()
                                .expect("mutex poisoned")
                                .push_back(command);
                        }
                    })
                    .expect("failed to create thread");
            }
        })
        .expect("Failed to spawn listen thread");

    // Initiate timely computation.
    timely::execute_from_args(args, move |worker| {

        let timer = ::std::time::Instant::now();
        let recv = command_queue.clone();

        let mut manager = Manager::<Value>::new();
        let mut sequencer = Some(Sequencer::new(worker, timer));

        while sequencer.is_some() {

            // Check out channel status.
            while let Some(command) = recv.lock().expect("Mutex poisoned").pop_front() {
                sequencer
                    .as_mut()
                    .map(|s| s.push(command));
            }

            // Dequeue and act on commands.
            // Once per iteration, so that Shutdown works "immediately".
            if let Some(command) = sequencer.as_mut().and_then(|s| s.next()) {
                println!("{:?}\tExecuting {:?}", timer.elapsed(), command);
                if command == Command::Shutdown {
                    sequencer = None;
                }
                command.execute(&mut manager, worker);
            }

            worker.step();
        }

        println!("Shutting down");

    }).expect("Timely computation did not initialize cleanly");
}
