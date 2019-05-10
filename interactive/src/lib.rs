//! Interactive differential dataflow
//!
//! This crate provides a demonstration of an interactive differential
//! dataflow system, which accepts query plans as data and then directly
//! implements them without compilation.

#![forbid(missing_docs)]

extern crate bincode;
extern crate timely;
extern crate differential_dataflow;
extern crate dogsdogsdogs;
extern crate serde;
#[macro_use]
extern crate serde_derive;

pub mod plan;
pub use plan::Plan;

pub mod manager;
pub use manager::{Manager, TraceManager, InputManager};

pub mod command;
pub use command::Command;

pub mod logging;

/// System-wide notion of time.
pub type Time = ::std::time::Duration;
/// System-wide update type.
pub type Diff = isize;

/// Multiple related collection definitions.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Query<Value> {
    /// A list of bindings of names to plans.
    pub rules: Vec<Rule<Value>>,
}

impl<Value> Query<Value> {
    /// Creates a new, empty query.
    pub fn new() -> Self {
        Query { rules: Vec::new() }
    }
    /// Adds a rule to an existing query.
    pub fn add_rule(mut self, rule: Rule<Value>) -> Self {
        self.rules.push(rule);
        self
    }
    /// Converts the query into a command.
    pub fn into_command(self) -> Command<Value> {
        Command::Query(self)
    }
}

/// Definition of a single collection.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Rule<Value> {
    /// Name of the rule.
    pub name: String,
    /// Plan describing contents of the rule.
    pub plan: Plan<Value>,
}

impl<Value> Rule<Value> {
    /// Converts the rule into a singleton query.
    pub fn into_query(self) -> Query<Value> {
        Query::new().add_rule(self)
    }
}

/// An example value type
#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Value {
    /// boolean
    Bool(bool),
    /// integer
    Usize(usize),
    /// string
    String(String),
    /// operator address
    Address(Vec<usize>),
    /// duration
    Duration(::std::time::Duration),
}

use manager::VectorFrom;
use timely::logging::TimelyEvent;

impl VectorFrom<TimelyEvent> for Value {
    fn vector_from(item: TimelyEvent) -> Vec<Value> {
        match item {
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

impl VectorFrom<DifferentialEvent> for Value {
    fn vector_from(item: DifferentialEvent) -> Vec<Value> {
        match item {
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

/// Serializes a command into a socket.
pub fn bincode_socket(socket: &mut std::net::TcpStream, command: &Command<Value>) {
    bincode::serialize_into(socket, command).expect("bincode: serialization failed");
}