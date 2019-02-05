
use std::hash::Hash;

use timely::communication::Allocate;
use timely::worker::Worker;
use differential_dataflow::{Data};

use super::{Query, Rule, Plan, Time, Diff, Manager};

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Command<Value> {
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
    /// Terminates the system.
    Shutdown,
}

impl<Value: Data+Hash> Command<Value> {

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

            Command::Shutdown => {
                println!("Shutdown received");
            }
        }

    }

}