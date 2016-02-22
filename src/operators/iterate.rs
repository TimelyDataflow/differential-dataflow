//! Iterative application of a differential dataflow fragment.
//!
//! The `iterate` operator takes as an argument a closure from a differential dataflow stream to a
//! stream of the same type. The output are differences which accumulate to the result of applying
//! this closure a specified number of times.
//!
//! The implementation of `iterate` does not directly apply the closure, but rather establishes an
//! iterative timely dataflow subcomputation, in which differences circulate until they dissipate
//! (indicating that the computation has reached fixed point), or until some number of iterations
//! have passed.
//!
//! The `iterate` method is written using a `Variable`, which lets you define your own iterative 
//! computations when `iterate` itself is not sufficient. This can happen when you have two 
//! collections that should evolve simultaneously, or when you would like to return an intermediate 
//! result from your iterative computation.
//! 
//! Using `Variable` requires more explicit arrangement of your computation, but isn't much more
//! complicated. You must define a new variable from an existing stream (its initial value), and 
//! then set it to be a function of this variable (and perhaps other collections and variables).
//!
//! A `Variable` derefences to a `Collection`, the one corresponding to its value in each iteration,
//! and it can be used in most situations where a collection can be used. The act of setting a 
//! `Variable` consumes it and returns the corresponding `Collection`, preventing you from setting
//! it multiple times.
//!
//! #Examples
//!
//! The example repeatedly divides even numbers by two, and leaves odd numbers as they are. Although
//! some numbers may take multiple iterations to converge, converged numbers have no overhead in
//! subsequent iterations.
//!
//! ```ignore
//! // repeatedly divide out factors of two.
//! let limits = numbers.iterate(|values| {
//!     values.map(|x if x % 2 == 0 { x/2 } else { x })
//! });
//! ```
//!
//! The same example written manually with a `Variable`:
//!
//! ```ignore
//! // repeatedly divide out factors of two.
//! let limits = computation.scoped(|scope| {
//!     let variable = Variable::from(numbers.enter(scope));
//!     let result = variable.map(|x if x % 2 == 0 { x/2 } else { x });
//!     variable.set(&result)
//!             .leave()
//! })

use std::fmt::Debug;
use std::ops::Deref;

use timely::dataflow::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::*;
use timely::dataflow::operators::feedback::Handle;

use ::{Data, Collection};
use collection::LeastUpperBound;

/// An extension trait for the `iterate` method.
pub trait IterateExt<G: Scope, D: Data> {
    /// Iteratively apply `logic` to the source collection until convergence.
    fn iterate<F>(&self, logic: F) -> Collection<G, D>
        where G::Timestamp: LeastUpperBound,
              F: FnOnce(&Collection<Child<G, u64>, D>)->Collection<Child<G, u64>, D>;
}

impl<G: Scope, D: Ord+Data+Debug> IterateExt<G, D> for Collection<G, D> {
    fn iterate<F>(&self, logic: F) -> Collection<G, D>
        where G::Timestamp: LeastUpperBound,
              F: FnOnce(&Collection<Child<G, u64>, D>)->Collection<Child<G, u64>, D> {

        self.inner.scope().scoped(|subgraph| {
            // create a new variable, apply logic, bind variable, return.
            //
            // this could be much more succinct if we returned the collection
            // wrapped by `variable`, but it also results in substantially more
            // diffs produced; `result` is post-consolidation, and means fewer
            // records are yielded out of the loop.
            let variable = Variable::from(self.enter(subgraph));
            let result = logic(&variable);
            variable.set(&result);
            result.leave()
        })
    }
}

/// A differential dataflow collection variable
///
/// The `Variable` struct allows differential dataflow programs requiring more sophisticated
/// iterative patterns than singly recursive iteration. For example: in mutual recursion two 
/// collections evolve simultaneously.
pub struct Variable<G: Scope, D: Data>
where G::Timestamp: LeastUpperBound {
    collection: Collection<Child<G, u64>, D>,
    feedback: Handle<G::Timestamp, u64,(D, i32)>,
    source: Collection<Child<G, u64>, D>,
}

impl<G: Scope, D: Data> Variable<G, D> where G::Timestamp: LeastUpperBound {
    /// Creates a new `Variable` and a `Stream` representing its output, from a supplied `source` stream.
    pub fn from(source: Collection<Child<G, u64>, D>) -> Variable<G, D> {
        let (feedback, updates) = source.inner.scope().loop_variable(u64::max_value(), 1);
        let collection = Collection::new(updates).concat(&source);
        Variable { collection: collection, feedback: feedback, source: source }
    }
    /// Adds a new source of data to the `Variable`.
    pub fn set(self, result: &Collection<Child<G, u64>, D>) -> Collection<Child<G, u64>, D> {
        self.source.negate()
                   .concat(result)
                   .inner
                   .connect_loop(self.feedback);

        self.collection
    }
}

impl<G: Scope, D: Data> Deref for Variable<G, D> where G::Timestamp: LeastUpperBound {
    type Target = Collection<Child<G, u64>, D>;
    fn deref(&self) -> &Self::Target {
        &self.collection
    }
}