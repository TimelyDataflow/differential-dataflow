//! Aggregates the weights of equal records into at most one record.
//!
//! As differential dataflow streams are unordered and taken to be the accumulation of all records,
//! no semantic change happens via `consolidate`. However, there is a practical difference between
//! a collection that aggregates down to zero records, and one that actually has no records. The
//! underlying system can more clearly see that no work must be done in the later case, and we can
//! drop out of, e.g. iterative computations.

use timely::dataflow::Scope;

use crate::{VecCollection, ExchangeData, Hashable};
use crate::consolidation::ConsolidatingContainerBuilder;
use crate::difference::Semigroup;

use crate::Data;
use crate::lattice::Lattice;
use crate::trace::{Batcher, Builder};

/// Methods which require data be arrangeable.
impl<G, D, R> VecCollection<G, D, R>
where
    G: Scope<Timestamp: Data+Lattice>,
    D: ExchangeData+Hashable,
    R: Semigroup+ExchangeData,
{
    /// Aggregates the weights of equal records into at most one record.
    ///
    /// This method uses the type `D`'s `hashed()` method to partition the data. The data are
    /// accumulated in place, each held back until their timestamp has completed.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let x = scope.new_collection_from(1 .. 10u32).1;
    ///
    ///     x.negate()
    ///      .concat(&x)
    ///      .consolidate() // <-- ensures cancellation occurs
    ///      .assert_empty();
    /// });
    /// ```
    pub fn consolidate(&self) -> Self {
        use crate::trace::implementations::{KeyBatcher, KeyBuilder, KeySpine};
        self.consolidate_named::<KeyBatcher<_, _, _>,KeyBuilder<_,_,_>, KeySpine<_,_,_>,_>("Consolidate", |key,&()| key.clone())
    }

    /// As `consolidate` but with the ability to name the operator, specify the trace type,
    /// and provide the function `reify` to produce owned keys and values..
    pub fn consolidate_named<Ba, Bu, Tr, F>(&self, name: &str, reify: F) -> Self
    where
        Ba: Batcher<Input=Vec<((D,()),G::Timestamp,R)>, Time=G::Timestamp> + 'static,
        Tr: for<'a> crate::trace::Trace<Time=G::Timestamp,Diff=R>+'static,
        Bu: Builder<Time=Tr::Time, Input=Ba::Output, Output=Tr::Batch>,
        F: Fn(Tr::Key<'_>, Tr::Val<'_>) -> D + 'static,
    {
        use crate::operators::arrange::arrangement::Arrange;
        self.map(|k| (k, ()))
            .arrange_named::<Ba, Bu, Tr>(name)
            .as_collection(reify)
    }

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
    /// use differential_dataflow::input::Input;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let x = scope.new_collection_from(1 .. 10u32).1;
    ///
    ///     // nothing to assert, as no particular guarantees.
    ///     x.negate()
    ///      .concat(&x)
    ///      .consolidate_stream();
    /// });
    /// ```
    pub fn consolidate_stream(&self) -> Self {

        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::Operator;
        use crate::collection::AsCollection;

        self.inner
            .unary::<ConsolidatingContainerBuilder<_>, _, _, _>(Pipeline, "ConsolidateStream", |_cap, _info| {

                move |input, output| {
                    input.for_each(|time, data| {
                        output.session_with_builder(&time).give_iterator(data.drain(..));
                    })
                }
            })
            .as_collection()
    }
}
