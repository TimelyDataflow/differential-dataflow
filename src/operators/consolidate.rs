//! Aggregates the weights of equal records into at most one record.
//!
//! As differential dataflow streams are unordered and taken to be the accumulation of all records,
//! no semantic change happens via `consolidate`. However, there is a practical difference between
//! a collection that aggregates down to zero records, and one that actually has no records. The
//! underlying system can more clearly see that no work must be done in the later case, and we can
//! drop out of, e.g. iterative computations.

use crate::{
    difference::Semigroup,
    lattice::Lattice,
    operators::arrange::{Arrange, Arranged, TraceAgent},
    trace::{implementations::ord::OrdKeySpine, Batch, Cursor, Trace, TraceReader},
    AsCollection, Collection, ExchangeData, Hashable,
};
use timely::dataflow::{channels::pact::Pipeline, operators::Operator, Scope};

/// An extension method for consolidating weighted streams.
pub trait Consolidate<S, D, R>: Sized
where
    S: Scope,
    S::Timestamp: Lattice,
    D: ExchangeData + Hashable,
    R: Semigroup,
{
    /// Aggregates the weights of equal records into at most one record.
    ///
    /// This method uses the type `D`'s [`hashed()`](Hashable) method to partition the data.
    /// The data is accumulated in place and held back until its timestamp has completed.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::Consolidate;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(1 .. 10u32).1;
    ///
    ///         x.negate()
    ///          .concat(&x)
    ///          .consolidate() // <-- ensures cancellation occurs
    ///          .assert_empty();
    ///     });
    /// }
    /// ```
    fn consolidate(&self) -> Collection<S, D, R> {
        self.consolidate_named("Consolidate")
    }

    /// A `consolidate` but with the ability to name the operator.
    fn consolidate_named(&self, name: &str) -> Collection<S, D, R> {
        self.consolidate_core::<OrdKeySpine<_, _, _>>(name)
            .as_collection(|data, &()| data.clone())
    }

    /// A `consolidate` that returns the intermediate [arrangement](Arranged)
    ///
    /// # Example
    ///
    /// ```rust
    /// use differential_dataflow::{
    ///     input::Input,
    ///     operators::{Consolidate, JoinCore},
    /// };
    ///
    /// timely::example(|scope| {
    ///     let (_, collection) = scope.new_collection_from(0..10u32);
    ///
    ///     let keys = collection
    ///         .flat_map(|x| (0..x))
    ///         .concat(&collection.negate())
    ///         .consolidate_arranged();
    ///
    ///     collection
    ///         .map(|x| (x, x * 2))
    ///         .join_core(&keys, |&key, &(), &value| (key, value))
    ///         .inspect(|x| println!("{:?}", x));
    /// });
    /// ```
    fn consolidate_arranged(&self) -> Arranged<S, TraceAgent<OrdKeySpine<D, S::Timestamp, R>>> {
        self.consolidate_core::<OrdKeySpine<_, _, _>>("Consolidate")
    }

    /// Aggregates the weights of equal records into at most one record,
    /// returning the intermediate [arrangement](Arranged)
    fn consolidate_core<Tr>(&self, name: &str) -> Arranged<S, TraceAgent<Tr>>
    where
        Tr: Trace + TraceReader<Key = D, Val = (), Time = S::Timestamp, R = R> + 'static,
        Tr::Batch: Batch<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
        Tr::Cursor: Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>;
}

impl<S, D, R> Consolidate<S, D, R> for Collection<S, D, R>
where
    S: Scope,
    S::Timestamp: Lattice + Ord,
    D: ExchangeData + Hashable,
    R: ExchangeData + Semigroup,
{
    fn consolidate_core<Tr>(&self, name: &str) -> Arranged<S, TraceAgent<Tr>>
    where
        Tr: Trace + TraceReader<Key = D, Val = (), Time = S::Timestamp, R = R> + 'static,
        Tr::Batch: Batch<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
        Tr::Cursor: Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    {
        self.map(|key| (key, ())).arrange_named(name)
    }
}

/// An extension method for consolidating weighted streams.
pub trait ConsolidateStream<D: ExchangeData + Hashable> {
    /// Aggregates the weights of equal records.
    ///
    /// Unlike `consolidate`, this method does not exchange data and does not
    /// ensure that at most one copy of each `(data, time)` pair exists in the
    /// results. Instead, it acts on each batch of data and collapses equivalent
    /// `(data, time)` pairs found therein, suppressing any that accumulate to
    /// zero.
    ///
    /// # Examples
    ///
    /// ```
    /// extern crate timely;
    /// extern crate differential_dataflow;
    ///
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::consolidate::ConsolidateStream;
    ///
    /// fn main() {
    ///     ::timely::example(|scope| {
    ///
    ///         let x = scope.new_collection_from(1 .. 10u32).1;
    ///
    ///         // nothing to assert, as no particular guarantees.
    ///         x.negate()
    ///          .concat(&x)
    ///          .consolidate_stream();
    ///     });
    /// }
    /// ```
    fn consolidate_stream(&self) -> Self;
}

impl<G: Scope, D, R> ConsolidateStream<D> for Collection<G, D, R>
where
    D: ExchangeData + Hashable,
    R: ExchangeData + Semigroup,
    G::Timestamp: Lattice + Ord,
{
    fn consolidate_stream(&self) -> Self {
        self.inner
            .unary(Pipeline, "ConsolidateStream", |_cap, _info| {
                let mut vector = Vec::new();
                move |input, output| {
                    input.for_each(|time, data| {
                        data.swap(&mut vector);
                        crate::consolidation::consolidate_updates(&mut vector);
                        output.session(&time).give_vec(&mut vector);
                    })
                }
            })
            .as_collection()
    }
}
