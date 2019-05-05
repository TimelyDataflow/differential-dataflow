//! Projection expression plan.

use std::hash::Hash;

use timely::dataflow::Scope;

use differential_dataflow::{Collection, ExchangeData};
use plan::{Plan, Render};
use {TraceManager, Time, Diff};

/// A plan which retains values at specified locations.
///
/// The plan does not ascribe meaning to specific locations (e.g. bindings)
/// to variable names, and simply selects out the indicated sequence of values,
/// panicking if some input record is insufficiently long.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Project<V> {
    /// Sequence (and order) of indices to be retained.
    pub indices: Vec<usize>,
    /// Plan for the data source.
    pub plan: Box<Plan<V>>,
}

impl<V: ExchangeData+Hash> Render for Project<V> {

    type Value = V;

    fn render<S: Scope<Timestamp = Time>>(
        &self,
        scope: &mut S,
        arrangements: &mut TraceManager<Self::Value>) -> Collection<S, Vec<Self::Value>, Diff>
    {
        let indices = self.indices.clone();

        // TODO: re-use `tuple` allocation.
        self.plan
            .render(scope, arrangements)
            .map(move |tuple| indices.iter().map(|index| tuple[*index].clone()).collect())
    }
}
