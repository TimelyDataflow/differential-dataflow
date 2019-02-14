//! Types and traits for implementing query plans.

use std::hash::Hash;

use timely::dataflow::Scope;
use differential_dataflow::{Collection, Data};

use {TraceManager, Time, Diff};

// pub mod count;
pub mod concat;
pub mod filter;
pub mod join;
pub mod project;

// pub use self::count::Count;
pub use self::concat::Concat;
pub use self::filter::{Filter, Predicate};
pub use self::join::Join;
pub use self::project::Project;

/// A type that can be rendered as a collection.
pub trait Render : Sized {

    /// Value type produced.
    type Value: Data;

    /// Renders the instance as a collection in the supplied scope.
    ///
    /// This method has access to arranged data, and may rely on and update the set
    /// of arrangements based on the needs and offerings of the rendering process.
    fn render<S: Scope<Timestamp = Time>>(
        &self,
        scope: &mut S,
        arrangements: &mut TraceManager<Self::Value>) -> Collection<S, Vec<Self::Value>, Diff>;
}

/// Possible query plan types.
#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Plan<Value> {
    /// Projection / Permutation
    Project(Project<Value>),
    /// Distinct
    Distinct(Box<Plan<Value>>),
    /// Concat
    Concat(Concat<Value>),
    /// Equijoin
    Join(Join<Value>),
    /// Negation
    Negate(Box<Plan<Value>>),
    /// Filters bindings by one of the built-in predicates
    Filter(Filter<Value>),
    /// Sources data from another relation.
    Source(String),
    /// Prints resulting updates.
    Inspect(String, Box<Plan<Value>>),
}

impl<V: Data+Hash> Plan<V> {
    /// Retains only the values at the indicated indices.
    pub fn project(self, indices: Vec<usize>) -> Self {
        Plan::Project(Project {
            indices,
            plan: Box::new(self),
        })
    }
    /// Reduces a collection to distinct tuples.
    pub fn distinct(self) -> Self {
        Plan::Distinct(Box::new(self))
    }
    /// Merges multiple collections.
    pub fn concat(plans: Vec<Self>) -> Self {
        Plan::Concat(Concat { plans } )
    }
    /// Equi-joins two collections using the specified pairs of keys.
    pub fn join(self, other: Plan<V>, keys: Vec<(usize, usize)>) -> Self {
        Plan::Join(Join {
            keys,
            plan1: Box::new(self),
            plan2: Box::new(other),
        })
    }
    /// Negates a collection (negating multiplicities).
    pub fn negate(self) -> Self {
        Plan::Negate(Box::new(self))
    }
    /// Restricts collection to tuples satisfying the predicate.
    pub fn filter(self, predicate: Predicate<V>) -> Self {
        Plan::Filter(Filter { predicate, plan: Box::new(self) } )
    }
    /// Loads a source of data by name.
    pub fn source(name: &str) -> Self {
        Plan::Source(name.to_string())
    }
    /// Prints each tuple prefixed by `text`.
    pub fn inspect(self, text: &str) -> Self {
        Plan::Inspect(text.to_string(), Box::new(self))
    }
}

impl<V: Data+Hash> Render for Plan<V> {

    type Value = V;

    fn render<S: Scope<Timestamp = Time>>(
        &self,
        scope: &mut S,
        arrangements: &mut TraceManager<Self::Value>) -> Collection<S, Vec<Self::Value>, Diff>
    {
        match self {
            Plan::Project(projection) => projection.render(scope, arrangements),
            Plan::Distinct(distinct) => {
                // TODO: Check for existing arrangement.
                use differential_dataflow::operators::Threshold;
                distinct.render(scope, arrangements).distinct()
            },
            // Plan::Count(count) => count.render(scope, arrangements),
            Plan::Concat(concat) => concat.render(scope, arrangements),
            Plan::Join(join) => join.render(scope, arrangements),
            Plan::Negate(negate) => {
                negate.render(scope, arrangements).negate()
            },
            Plan::Filter(filter) => filter.render(scope, arrangements),
            Plan::Source(source) => {
                arrangements
                    .get_unkeyed(self)
                    .expect(&format!("Failed to find source collection: {:?}", source))
                    .import(scope)
                    .as_collection(|k,()| k.to_vec())
            },
            Plan::Inspect(text, plan) => {
                let text = text.clone();
                plan.render(scope, arrangements)
                    .inspect(move |x| println!("{}\t{:?}", text, x))
            },
        }
    }
}
