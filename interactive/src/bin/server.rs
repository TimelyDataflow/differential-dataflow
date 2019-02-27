extern crate timely;
extern crate differential_dataflow;
extern crate interactive;

extern crate serde;
#[macro_use]
extern crate serde_derive;

use timely::synchronization::Sequencer;
use interactive::{Manager, Command, Query, Rule, Plan};

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Value {
    Bool(bool),
    Usize(usize),
    String(String),
    Address(Vec<usize>),
    Duration(::std::time::Duration),
}

use interactive::manager::AsVector;
use timely::logging::TimelyEvent;

impl AsVector<Value> for TimelyEvent {
    fn as_vector(self) -> Vec<Value> {
        match self {
            TimelyEvent::Operates(x) => {
                vec![Value::Usize(x.id), Value::Address(x.addr), Value::String(x.name)]
            },
            TimelyEvent::Channels(x) => {
                vec![Value::Usize(x.id), Value::Address(x.scope_addr), Value::Usize(x.source.0), Value::Usize(x.source.1), Value::Usize(x.target.0), Value::Usize(x.target.1)]
            },
            TimelyEvent::Schedule(x) => {
                vec![Value::Usize(x.id), Value::Bool(x.start_stop == ::timely::logging::StartStop::Start)]
            },
            TimelyEvent::Messages(x) => {
                vec![Value::Usize(x.channel), Value::Bool(x.is_send), Value::Usize(x.source), Value::Usize(x.target), Value::Usize(x.seq_no), Value::Usize(x.length)]
            },
            _ => { vec![] },
        }
    }
}

use differential_dataflow::logging::DifferentialEvent;

impl AsVector<Value> for DifferentialEvent {
    fn as_vector(self) -> Vec<Value> {
        match self {
            DifferentialEvent::Batch(x) => {
                vec![
                    Value::Usize(x.operator),
                    Value::Usize(x.length),
                ]
            },
            DifferentialEvent::Merge(x) => {
                vec![
                    Value::Usize(x.operator),
                    Value::Usize(x.scale),
                    Value::Usize(x.length1),
                    Value::Usize(x.length2),
                    Value::Usize(x.complete.unwrap_or(0)),
                    Value::Bool(x.complete.is_some()),
                ]
            },
            _ => { vec![] },
        }
    }
}


fn main() {

    let mut args = std::env::args();
    args.next();

    timely::execute_from_args(args, |worker| {

        let timer = ::std::time::Instant::now();
        let mut manager = Manager::<Value>::new();

        use std::rc::Rc;
        use timely::dataflow::operators::capture::event::link::EventLink;
        use timely::logging::BatchLogger;

        // Capture timely logging events.
        let timely_events = Rc::new(EventLink::new());
        // Capture differential logging events.
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

        if worker.index() == 0 {

            sequencer.push(Command::Query(
                Query {
                    rules: vec![
            //             Rule {
            //                 name: "operates".to_string(),
            //                 plan: Plan::source("logs/timely/operates").inspect("operates:"),
            //             },
            //             Rule {
            //                 name: "channels".to_string(),
            //                 plan: Plan::source("logs/timely/channels").inspect("channels:"),
            //             },
            //             Rule {
            //                 name: "schedule".to_string(),
            //                 plan: Plan::source("logs/timely/schedule").inspect("schedule:"),
            //             },
            //             Rule {
            //                 name: "messages".to_string(),
            //                 plan: Plan::source("logs/timely/messages").inspect("messages:"),
            //             },
            //             Rule {
            //                 name: "batch".to_string(),
            //                 plan: Plan::source("logs/differential/arrange/batch").inspect("batch:"),
            //             },
            //             Rule {
            //                 name: "merge".to_string(),
            //                 plan: Plan::source("logs/differential/arrange/merge").inspect("merge:"),
            //             },
            //             Rule {
            //                 name: "active".to_string(),
            //                 plan: Plan::source("logs/timely/operates")
            //                         .join(Plan::source("logs/differential/arrange/batch"), vec![(0,0)])
            //                         .inspect("active"),
            //             }
                    ]
                }
            ));

            sequencer.push(Command::Shutdown);
        }

        let mut shutdown = false;
        while !shutdown {

            if let Some(command) = sequencer.next() {
                println!("{:?}\tExecuting {:?}", timer.elapsed(), command);
                if command == Command::Shutdown {
                    shutdown = true;
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

    }).expect("Timely computation did not exit cleanly");
}
