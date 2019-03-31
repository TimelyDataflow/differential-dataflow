//! Multi-way equijoin expression plan.
//!
//! This plan provides us the opportunity to map out a non-trivial differential
//! implementation for a complex join query. In particular, we are able to invoke
//! delta-query and worst-case optimal join plans, which avoid any intermediate
//! materialization.
//!
//! Each `MultiwayJoin` indicates several source collections, equality constraints
//! among their attributes, and then the set of attributes to produce as results.
//!
//! One naive implementation would take each input collection in order, and develop
//! the join restricted to the prefix of relations so far. Ideally the order would
//! be such that joined collections have equality constraints and prevent Cartesian
//! explosion. At each step, a new collection picks out some of the attributes and
//! instantiates a primitive binary join between the accumulated collection and the
//! next collection.
//!
//! A more sophisticated implementation establishes delta queries for each input
//! collection, which responds to changes in that input collection against the
//! current other input collections. For each input collection we may choose very
//! different join orders, as the order must follow equality constraints.
//!
//! A further implementation could develop the results attribute-by-attribute, as
//! opposed to collection-by-collection, which gives us the ability to use column
//! indices rather than whole-collection indices.

use std::hash::Hash;

use timely::dataflow::Scope;

use differential_dataflow::operators::JoinCore;

use differential_dataflow::{Collection, Data};
use plan::{Plan, Render};
use {TraceManager, Time, Diff};

/// A plan stage joining two source relations on the specified
/// symbols. Throws if any of the join symbols isn't bound by both
/// sources.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MultiwayJoin<Value> {
    /// Attributes to extract as `(attr, input)`.
    pub results: Vec<(usize, usize)>,
    /// Source collections.
    pub sources: Vec<Box<Plan<Value>>>,
    /// Equality constraints (as lists of equal `(attr, input)` pairs).
    pub equalities: Vec<Vec<(usize, usize)>>,
}

impl<V: Data+Hash> Render for MultiwayJoin<V> {

    type Value = V;

    fn render<S: Scope<Timestamp = Time>>(
        &self,
        scope: &mut S,
        arrangements: &mut TraceManager<Self::Value>) -> Collection<S, Vec<Self::Value>, Diff>
    {
        unimplemented!()
    }
}
