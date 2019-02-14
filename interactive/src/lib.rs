extern crate timely;
extern crate differential_dataflow;
extern crate serde;
#[macro_use]
extern crate serde_derive;

pub mod plan;
pub use plan::Plan;

pub mod manager;
pub use manager::{Manager, TraceManager, InputManager};

pub mod command;
pub use command::Command;

pub type Time = ::std::time::Duration;
pub type Diff = isize;

/// Multiple related collection definitions.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Query<Value> {
    /// A list of bindings of names to plans.
    pub rules: Vec<Rule<Value>>,
}

/// Definition of a single collection.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Rule<Value> {
    /// Name of the rule.
    pub name: String,
    /// Plan describing contents of the rule.
    pub plan: Plan<Value>,
}