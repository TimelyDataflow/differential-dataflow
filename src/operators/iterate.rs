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
//! # Details
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
use timely::order::Product;

use timely::dataflow::*;
use timely::dataflow::scopes::child::Iterative;
use timely::dataflow::operators::{Feedback, ConnectLoop, Map};
use timely::dataflow::operators::feedback::Handle;

use ::{Data, Collection};
use ::difference::{Semigroup, Abelian};
use lattice::Lattice;

/// An extension trait for the `iterate` method.
pub trait Iterate<G: Scope, D: Data, R: Semigroup> {
    /// Iteratively apply `logic` to the source collection until convergence.
    ///
    /// Importantly, this method does not automatically consolidate results.
    /// It may be important to conclude with `consolidate()` to ensure that
    /// logically empty collections that contain cancelling records do not
    /// result in non-termination. Operators like `group`, `distinct`, and
    /// `count` also perform consolidation, and are safe to conclude with.
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
        where
            G::Timestamp: Lattice,
            for<'a> F: FnOnce(&Collection<Iterative<'a, G, u64>, D, R>)->Collection<Iterative<'a, G, u64>, D, R>;
}

impl<G: Scope, D: Ord+Data+Debug, R: Abelian> Iterate<G, D, R> for Collection<G, D, R> {
    fn iterate<F>(&self, logic: F) -> Collection<G, D, R>
        where G::Timestamp: Lattice,
              for<'a> F: FnOnce(&Collection<Iterative<'a, G, u64>, D, R>)->Collection<Iterative<'a, G, u64>, D, R> {

        self.inner.scope().scoped("Iterate", |subgraph| {
            // create a new variable, apply logic, bind variable, return.
            //
            // this could be much more succinct if we returned the collection
            // wrapped by `variable`, but it also results in substantially more
            // diffs produced; `result` is post-consolidation, and means fewer
            // records are yielded out of the loop.
            let variable = Variable::new_from(self.enter(subgraph), Product::new(Default::default(), 1));
            let result = logic(&variable);
            variable.set(&result);
            result.leave()
        })
    }
}

impl<G: Scope, D: Ord+Data+Debug, R: Semigroup> Iterate<G, D, R> for G {
    fn iterate<F>(&self, logic: F) -> Collection<G, D, R>
        where G::Timestamp: Lattice,
              for<'a> F: FnOnce(&Collection<Iterative<'a, G, u64>, D, R>)->Collection<Iterative<'a, G, u64>, D, R> {

        // TODO: This makes me think we have the wrong ownership pattern here.
        let mut clone = self.clone();
        clone
            .scoped("Iterate", |subgraph| {
                // create a new variable, apply logic, bind variable, return.
                //
                // this could be much more succinct if we returned the collection
                // wrapped by `variable`, but it also results in substantially more
                // diffs produced; `result` is post-consolidation, and means fewer
                // records are yielded out of the loop.
                let variable = SemigroupVariable::new(subgraph, Product::new(Default::default(), 1));
                let result = logic(&variable);
                variable.set(&result);
                result.leave()
            }
        )
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
/// use timely::order::Product;
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
///         scope.iterative::<u64,_,_>(|nested| {
///             let summary = Product::new(Default::default(), 1);
///             let variable = Variable::new_from(numbers.enter(nested), summary);
///             let result = variable.map(|x| if x % 2 == 0 { x/2 } else { x })
///                                  .consolidate();
///             variable.set(&result)
///                     .leave()
///         });
///     })
/// }
/// ```
pub struct Variable<G: Scope, D: Data, R: Abelian>
where G::Timestamp: Lattice {
    collection: Collection<G, D, R>,
    feedback: Handle<G, (D, G::Timestamp, R)>,
    source: Collection<G, D, R>,
    step: <G::Timestamp as Timestamp>::Summary,
}

impl<G: Scope, D: Data, R: Abelian> Variable<G, D, R> where G::Timestamp: Lattice {
    /// Creates a new initially empty `Variable`.
    pub fn new(scope: &mut G, step: <G::Timestamp as Timestamp>::Summary) -> Self {
        use collection::AsCollection;
        let empty = ::timely::dataflow::operators::generic::operator::empty(scope).as_collection();
        Self::new_from(empty, step)
    }

    /// Creates a new `Variable` from a supplied `source` stream.
    pub fn new_from(source: Collection<G, D, R>, step: <G::Timestamp as Timestamp>::Summary) -> Self {
        let (feedback, updates) = source.inner.scope().feedback(step.clone());
        let collection = Collection::new(updates).concat(&source);
        Variable { collection, feedback, source, step }
    }

    /// Adds a new source of data to the `Variable`.
    pub fn set(self, result: &Collection<G, D, R>) -> Collection<G, D, R> {
        let in_result = self.source.negate().concat(result);
        self.set_concat(&in_result)
    }


    /// Concat a new source of data to the data already in the `Variable`.
    pub fn set_concat(self, result: &Collection<G, D, R>) -> Collection<G, D, R> {
        let step = self.step;
        result
            .inner
            .flat_map(move |(x,t,d)| step.results_in(&t).map(|t| (x,t,d)))
            .connect_loop(self.feedback);

        self.collection
    }
}

impl<G: Scope, D: Data, R: Abelian> Deref for Variable<G, D, R> where G::Timestamp: Lattice {
    type Target = Collection<G, D, R>;
    fn deref(&self) -> &Self::Target {
        &self.collection
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
/// use timely::order::Product;
/// use timely::dataflow::Scope;
///
/// use differential_dataflow::input::Input;
/// use differential_dataflow::operators::iterate::SemigroupVariable;
/// use differential_dataflow::operators::Consolidate;
///
/// fn main() {
///     ::timely::example(|scope| {
///
///         let numbers = scope.new_collection_from(1 .. 10u32).1;
///
///         scope.iterative::<u64,_,_>(|nested| {
///             let summary = Product::new(Default::default(), 1);
///             let variable = SemigroupVariable::<_,usize,isize>::new(nested, summary);
///             let result = variable.map(|x| if x % 2 == 0 { x/2 } else { x })
///                                  .consolidate();
///             variable.set(&result)
///                     .leave()
///         });
///     })
/// }
/// ```
pub struct SemigroupVariable<G: Scope, D: Data, R: Semigroup>
where G::Timestamp: Lattice {
    collection: Collection<G, D, R>,
    feedback: Handle<G, (D, G::Timestamp, R)>,
    step: <G::Timestamp as Timestamp>::Summary,
}

impl<G: Scope, D: Data, R: Semigroup> SemigroupVariable<G, D, R> where G::Timestamp: Lattice {
    /// Creates a new initially empty `Variable`.
    pub fn new(scope: &mut G, step: <G::Timestamp as Timestamp>::Summary) -> Self {
        let (feedback, updates) = scope.feedback(step.clone());
        let collection = Collection::new(updates);
        SemigroupVariable { collection, feedback, step }
    }

    /// Adds a new source of data to the `Variable`.
    pub fn set(self, result: &Collection<G, D, R>) -> Collection<G, D, R> {
        let step = self.step;
        result
            .inner
            .flat_map(move |(x,t,d)| step.results_in(&t).map(|t| (x,t,d)))
            .connect_loop(self.feedback);

        self.collection
    }
}

impl<G: Scope, D: Data, R: Semigroup> Deref for SemigroupVariable<G, D, R> where G::Timestamp: Lattice {
    type Target = Collection<G, D, R>;
    fn deref(&self) -> &Self::Target {
        &self.collection
    }
}
