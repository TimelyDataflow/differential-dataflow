//! Iterative application of a differential dataflow fragment.
//!
//! The `iterate` operator takes as an argument a closure from a differential dataflow collection
//! to a collection of the same type. The output collection is the result of applying this closure
//! an unbounded number of times.
//!
//! The implementation of `iterate` does not directly apply the closure, but rather establishes an
//! iterative timely dataflow subcomputation, in which differences circulate until they dissipate
//! (indicating that the computation has reached fixed point), or until some number of iterations
//! have passed.
//!
//! **Note**: The dataflow assembled by `iterate` does not automatically insert `consolidate` for
//! you. This means that either (i) you should insert one yourself, (ii) you should be certain that
//! all paths from the input to the output of the loop involve consolidation, or (iii) you should
//! be worried that logically cancelable differences may circulate indefinitely.
//!
//! #Details
//!
//! The `iterate` method is written using a `Variable`, which lets you define your own iterative
//! computations when `iterate` itself is not sufficient. This can happen when you have two
//! collections that should evolve simultaneously, or when you would like to rotate your loop and
//! return an intermediate result.
//!
//! Using `Variable` requires more explicit arrangement of your computation, but isn't much more
//! complicated. You must define a new variable from an existing stream (its initial value), and
//! then set it to be a function of this variable (and perhaps other collections and variables).
//!
//! A `Variable` dereferences to a `Collection`, the one corresponding to its value in each iteration,
//! and it can be used in most situations where a collection can be used. The act of setting a
//! `Variable` consumes it and returns the corresponding `Collection`, preventing you from setting
//! it multiple times.

use std::fmt::Debug;
use std::ops::Deref;

use timely::progress::{Timestamp, PathSummary};
use timely::progress::nested::product::Product;

use timely::dataflow::*;
use timely::dataflow::scopes::Child;
use timely::dataflow::operators::*;
use timely::dataflow::operators::feedback::Handle;

use ::{Data, Collection, Diff};
use lattice::Lattice;

/// An extension trait for the `iterate` method.
pub trait Iterate<G: Scope, D: Data, R: Diff> {
    /// Iteratively apply `logic` to the source collection until convergence.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Iterate;
    /// use differential_dataflow::operators::Consolidate;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         scope.new_collection_from(1 .. 10u32).1
    ///              .iterate(|values| {
    ///                  values.map(|x| if x % 2 == 0 { x/2 } else { x })
    ///                        .consolidate()
    ///              });
    ///     });
    /// }
    /// ```
    fn iterate<F>(&self, logic: F) -> Collection<G, D, R>
        where G::Timestamp: Lattice,
              for<'a> F: FnOnce(&Collection<Child<'a, G, u64>, D, R>)->Collection<Child<'a, G, u64>, D, R>;
}

impl<G: Scope, D: Ord+Data+Debug, R: Diff> Iterate<G, D, R> for Collection<G, D, R> {
    fn iterate<F>(&self, logic: F) -> Collection<G, D, R>
        where G::Timestamp: Lattice,
              for<'a> F: FnOnce(&Collection<Child<'a, G, u64>, D, R>)->Collection<Child<'a, G, u64>, D, R> {

        self.inner.scope().scoped(|subgraph| {
            // create a new variable, apply logic, bind variable, return.
            //
            // this could be much more succinct if we returned the collection
            // wrapped by `variable`, but it also results in substantially more
            // diffs produced; `result` is post-consolidation, and means fewer
            // records are yielded out of the loop.
            let variable = Variable::new_from(self.enter(subgraph), u64::max_value(), 1);
            let result = logic(&variable);
            variable.set(&result);
            result.leave()
        })
    }
}

/// A recursively defined collection.
///
/// The `Variable` struct allows differential dataflow programs requiring more sophisticated
/// iterative patterns than singly recursive iteration. For example: in mutual recursion two
/// collections evolve simultaneously.
///
/// # Examples
///
/// The following example is equivalent to the example for the `Iterate` trait.
///
/// ```
/// extern crate timely;
/// extern crate differential_dataflow;
///
/// use timely::dataflow::Scope;
///
/// use differential_dataflow::input::Input;
/// use differential_dataflow::operators::iterate::Variable;
/// use differential_dataflow::operators::Consolidate;
///
/// fn main() {
///     ::timely::example(|scope| {
///
///         let numbers = scope.new_collection_from(1 .. 10u32).1;
///
///         scope.scoped(|nested| {
///             let variable = Variable::from(numbers.enter(nested));
///             let result = variable.map(|x| if x % 2 == 0 { x/2 } else { x })
///                                  .consolidate();
///             variable.set(&result)
///                     .leave()
///         });
///     })
/// }
/// ```
pub struct Variable<'a, G: Scope, D: Data, T: Timestamp+Lattice, R: Diff>
where G::Timestamp: Lattice {
    collection: Collection<Child<'a, G, T>, D, R>,
    feedback: Handle<G::Timestamp, T,(D, Product<G::Timestamp, T>, R)>,
    source: Collection<Child<'a, G, T>, D, R>,
    step: <T as Timestamp>::Summary,
}

impl<'a, G: Scope, D: Data, R: Diff, T: Timestamp+Lattice> Variable<'a, G, D, T, R> where G::Timestamp: Lattice {
    /// Creates a new initially empty `Variable`.
    pub fn new(scope: &mut Child<'a, G, T>, max_steps: T, step: <T as Timestamp>::Summary) -> Self {
        use collection::AsCollection;
        let empty = ::timely::dataflow::operators::generic::operator::empty(scope).as_collection();
        Self::new_from(empty, max_steps, step)
    }

    /// Creates a new `Variable` from a supplied `source` stream.
    pub fn new_from(source: Collection<Child<'a, G, T>, D, R>, max_steps: T, step: <T as Timestamp>::Summary) -> Self {
        let (feedback, updates) = source.inner.scope().loop_variable(max_steps, step.clone());
        let collection = Collection::new(updates).concat(&source);
        Variable { collection: collection, feedback: feedback, source: source, step: step }
    }

    /// Adds a new source of data to the `Variable`.
    pub fn set(self, result: &Collection<Child<'a, G, T>, D, R>) -> Collection<Child<'a, G, T>, D, R> {
        let step = self.step;
        self.source
            .negate()
            .concat(result)
            .inner
            .flat_map(move |(x,t,d)| step.results_in(&t.inner).map(|t_inner| (x, Product::new(t.outer, t_inner), d)))
            .connect_loop(self.feedback);

        self.collection
    }
}

impl<'a, G: Scope, D: Data, R: Diff, T: Timestamp+Lattice> Deref for Variable<'a, G, D, T, R> where G::Timestamp: Lattice {
    type Target = Collection<Child<'a, G, T>, D, R>;
    fn deref(&self) -> &Self::Target {
        &self.collection
    }
}

// impl<'a, G: Scope, D: Data, R: Diff> Variable<'a, G, D, R, u64> where G::Timestamp: Lattice {
//     /// Allocates a new variable from a source collection.
//     pub fn from(source: Collection<Child<'a, G, u64>, D, R>) -> Self {
//         Self::new_from(u64::max_value(), 1, source)
//     }
// }
// impl<'a, G: Scope, D: Data, R: Diff> Variable<'a, G, D, R, u32> where G::Timestamp: Lattice {
//     /// Allocates a new variable from a source collection.
//     pub fn from(source: Collection<Child<'a, G, u32>, D, R>) -> Self {
//         Self::new_from(u32::max_value(), 1, source)
//     }
// }
// impl<'a, G: Scope, D: Data, R: Diff> Variable<'a, G, D, R, usize> where G::Timestamp: Lattice {
//     /// Allocates a new variable from a source collection.
//     pub fn from(source: Collection<Child<'a, G, usize>, D, R>) -> Self {
//         Self::new_from(usize::max_value(), 1, source)
//     }
// }