extern crate timely;
extern crate differential_dataflow;
extern crate interactive;

use timely::synchronization::Sequencer;
use interactive::{Manager, Command, Value};

use timely::logging::TimelyEvent;
use differential_dataflow::logging::DifferentialEvent;

fn main() {

    let mut args = std::env::args();
    args.next();

    use std::sync::{Arc, Mutex};
    use std::collections::VecDeque;

    let command_queue = Arc::new(Mutex::new(VecDeque::<Command<Value>>::new()));
    let command_queue2 = command_queue.clone();

    let guards =
    timely::execute_from_args(args, move |worker| {

        let timer = ::std::time::Instant::now();
        let mut manager = Manager::<Value>::new();

        let recv = command_queue.clone();

        use std::rc::Rc;
        use timely::dataflow::operators::capture::event::link::EventLink;
        use timely::logging::BatchLogger;

        let timely_events = Rc::new(EventLink::new());
        let differential_events = Rc::new(EventLink::new());

        manager.publish_timely_logging(worker, Some(timely_events.clone()));
        manager.publish_differential_logging(worker, Some(differential_events.clone()));

        let mut timely_logger = BatchLogger::new(timely_events.clone());
        worker
            .log_register()
            .insert::<TimelyEvent,_>("timely", move |time, data| timely_logger.publish_batch(time, data));

        let mut differential_logger = BatchLogger::new(differential_events.clone());
        worker
            .log_register()
            .insert::<DifferentialEvent,_>("differential/arrange", move |time, data| differential_logger.publish_batch(time, data));

        let mut sequencer = Sequencer::new(worker, timer);

        let mut done = false;
        while !done {

            {   // Check out channel status.
                let mut lock = recv.lock().expect("Mutex poisoned");
                while let Some(command) = lock.pop_front() {
                    sequencer.push(command);
                }
            }

            // Dequeue and act on commands.
            // One at a time, so that Shutdown works.
            if let Some(command) = sequencer.next() {
                println!("{:?}\tExecuting {:?}", timer.elapsed(), command);
                if command == Command::Shutdown {
                    done = true;
                }
                command.execute(&mut manager, worker);
            }

            worker.step();
        }

        println!("Shutting down");

        // Disable sequencer for shut down.
        drop(sequencer);

        // Deregister loggers, so that the logging dataflows can shut down.
        worker
            .log_register()
            .insert::<TimelyEvent,_>("timely", move |_time, _data| { });

        worker
            .log_register()
            .insert::<DifferentialEvent,_>("differential/arrange", move |_time, _data| { });

    }).expect("Timely computation did not initialize cleanly");

    println!("Now accepting commands");

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
}
