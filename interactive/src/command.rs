//! Commands accepted by the system.

// use std::hash::Hash;
use std::io::Write;

use timely::communication::Allocate;
use timely::worker::Worker;

use timely::logging::TimelyEvent;
use differential_dataflow::logging::DifferentialEvent;

// use differential_dataflow::ExchangeData;

use super::{Value, Query, Rule, Plan, Time, Diff, Manager};
// use manager::LoggingValue;

/// Commands accepted by the system.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Command {
    /// Installs the query and publishes public rules.
    Query(Query<Value>),
    /// Advances all inputs and traces to `time`, and advances computation.
    AdvanceTime(Time),
    /// Creates a new named input, with initial input.
    CreateInput(String, Vec<Vec<Value>>),
    /// Introduces updates to a specified input.
    UpdateInput(String, Vec<(Vec<Value>, Time, Diff)>),
    /// Closes a specified input.
    CloseInput(String),
    /// Attaches a logging source. (address, flavor, number, granularity, name_as)
    SourceLogging(String, String, usize, u64, String),
    /// Terminates the system.
    Shutdown,
}

impl Command {

    /// Executes a command.
    pub fn execute<A: Allocate>(self, manager: &mut Manager<Value>, worker: &mut Worker<A>) {

        match self {

            Command::Query(query) => {

                worker.dataflow(|scope| {

                    use timely::dataflow::operators::Probe;
                    use differential_dataflow::operators::arrange::ArrangeBySelf;
                    use plan::Render;

                    for Rule { name, plan } in query.rules.into_iter() {
                        let collection =
                        plan.render(scope, &mut manager.traces)
                            .arrange_by_self();

                        collection.stream.probe_with(&mut manager.probe);
                        let trace = collection.trace;

                        // Can bind the trace to both the plan and the name.
                        manager.traces.set_unkeyed(&plan, &trace);
                        manager.traces.set_unkeyed(&Plan::Source(name), &trace);
                    }

                });
            },

            Command::AdvanceTime(time) => {
                manager.advance_time(&time);
                while manager.probe.less_than(&time) {
                    worker.step();
                }
            },

            Command::CreateInput(name, updates) => {

                use differential_dataflow::input::Input;
                use differential_dataflow::operators::arrange::ArrangeBySelf;

                let (input, trace) = worker.dataflow(|scope| {
                    let (input, collection) = scope.new_collection_from(updates.into_iter());
                    let trace = collection.arrange_by_self().trace;
                    (input, trace)
                });

                manager.insert_input(name, input, trace);

            },

            Command::UpdateInput(name, updates) => {
                if let Some(input) = manager.inputs.sessions.get_mut(&name) {
                    for (data, time, diff) in updates.into_iter() {
                        input.update_at(data, time, diff);
                    }
                }
                else {
                    println!("Input not found: {:?}", name);
                }
            },

            Command::CloseInput(name) => {
                manager.inputs.sessions.remove(&name);
            },

            Command::SourceLogging(address, flavor, number, granularity, name_as) => {

                match flavor.as_str() {
                    "timely" => {

                        let mut streams = Vec::new();

                        // Only one worker can bind to listen.
                        if worker.index() == 0 {

                            use std::time::Duration;
                            use std::net::TcpListener;
                            use timely::dataflow::operators::capture::EventReader;

                            println!("Awaiting timely logging connections ({})", number);

                            // e.g. "127.0.0.1:8000"
                            let listener = TcpListener::bind(address).unwrap();
                            for index in 0 .. number {
                                println!("\tTimely logging connection {} of {}", index, number);
                                let socket = listener.incoming().next().unwrap().unwrap();
                                socket.set_nonblocking(true).expect("failed to set nonblocking");
                                streams.push(EventReader::<Duration, (Duration, usize, TimelyEvent),_>::new(socket));
                            }

                            println!("\tAll logging connections established");
                        }
                        crate::logging::publish_timely_logging(manager, worker, granularity, &name_as, streams);
                    },
                    "differential" => {

                        let mut streams = Vec::new();

                        // Only one worker can bind to listen.
                        if worker.index() == 0 {

                            use std::time::Duration;
                            use std::net::TcpListener;
                            use timely::dataflow::operators::capture::EventReader;

                            // "127.0.0.1:8000"
                            let listener = TcpListener::bind(address).unwrap();
                            for _ in 0 .. number {
                                let socket = listener.incoming().next().unwrap().unwrap();
                                socket.set_nonblocking(true).expect("failed to set nonblocking");
                                streams.push(EventReader::<Duration, (Duration, usize, DifferentialEvent),_>::new(socket));
                            }
                        }
                        crate::logging::publish_differential_logging(manager, worker, granularity, &name_as, streams);
                    },
                    _ => { println!("{}", format!("Unknown logging flavor: {}", flavor)); }
                }

            }

            Command::Shutdown => {
                println!("Shutdown received");
                manager.shutdown(worker);
            }
        }
    }

    /// Serialize the command at a writer.
    pub fn serialize_into<W: Write>(&self, writer: W) {
        bincode::serialize_into(writer, self).expect("bincode: serialization failed");
    }
}