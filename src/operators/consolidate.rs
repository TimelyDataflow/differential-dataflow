//! Aggregates the weights of equal records into at most one record.
//!
//! As differential dataflow streams are unordered and taken to be the accumulation of all records,
//! no semantic change happens via `consolidate`. However, there is a practical difference between
//! a collection that aggregates down to zero records, and one that actually has no records. The
//! underlying system can more clearly see that no work must be done in the later case, and we can
//! drop out of, e.g. iterative computations.

use timely::dataflow::Scope;

use crate::{Collection, ExchangeData, Hashable};
use crate::difference::Semigroup;

use crate::Data;
use crate::lattice::Lattice;
use crate::trace::{Batcher, Builder};

/// Methods which require data be arrangeable.
impl<G, D, R> Collection<G, D, R>
where
    G: Scope,
    G::Timestamp: Data+Lattice,
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
        use crate::trace::implementations::KeySpine;
        self.consolidate_named::<KeySpine<_,_,_>>("Consolidate")
    }

    /// As `consolidate` but with the ability to name the operator and specify the trace type.
    pub fn consolidate_named<Tr>(&self, name: &str) -> Self
    where
        Tr: crate::trace::Trace<KeyOwned = D,ValOwned = (),Time=G::Timestamp,Diff=R>+'static,
        Tr::Batch: crate::trace::Batch,
        Tr::Batcher: Batcher<Item = ((D,()),G::Timestamp,R), Time = G::Timestamp>,
        Tr::Builder: Builder<Item = ((D,()),G::Timestamp,R), Time = G::Timestamp>,
    {
        use crate::operators::arrange::arrangement::Arrange;
        use crate::trace::cursor::MyTrait;
        self.map(|k| (k, ()))
            .arrange_named::<Tr>(name)
            .as_collection(|d, _| d.into_owned())
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

pub mod neu {
    //! Consolidate without building batches.

    use timely::PartialOrder;
    use timely::dataflow::Scope;
    use timely::dataflow::channels::pact::Exchange;
    use timely::dataflow::channels::pushers::buffer::Session;
    use timely::dataflow::channels::pushers::{Counter, Tee};
    use timely::dataflow::operators::{Capability, Operator};
    use timely::progress::{Antichain, Timestamp};

    use crate::collection::AsCollection;
    use crate::difference::Semigroup;
    use crate::lattice::Lattice;
    use crate::trace::{Batcher, Builder};
    use crate::{Collection, Data, ExchangeData, Hashable};

    impl<G, D, R> Collection<G, D, R>
        where
            G: Scope,
            G::Timestamp: Data + Lattice,
            D: ExchangeData + Hashable,
            R: Semigroup + ExchangeData,
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
        /// use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
        ///
        /// ::timely::example(|scope| {
        ///
        ///     let x = scope.new_collection_from(1 .. 10u32).1;
        ///
        ///     x.negate()
        ///      .concat(&x)
        ///      .consolidate_named_neu::<MergeBatcher<_, _, _, _>>("Consolidate inputs") // <-- ensures cancellation occurs
        ///      .assert_empty();
        /// });
        /// ```
        pub fn consolidate_named_neu<B>(&self, name: &str) -> Self
            where
                B: Batcher<Item=((D, ()), G::Timestamp, R), Time=G::Timestamp> + 'static,
        {
            let exchange = Exchange::new(move |update: &((D, ()), G::Timestamp, R)| (update.0).0.hashed().into());
            self.map(|k| (k, ())).inner
                .unary_frontier(exchange, name, |_cap, info| {

                    // Acquire a logger for arrange events.
                    let logger = {
                        let scope = self.scope();
                        let register = scope.log_register();
                        register.get::<crate::logging::DifferentialEvent>("differential/arrange")
                    };

                    let mut batcher = B::new(logger, info.global_id);
                    // Capabilities for the lower envelope of updates in `batcher`.
                    let mut capabilities = Antichain::<Capability<G::Timestamp>>::new();
                    let mut prev_frontier = Antichain::from_elem(<G::Timestamp as Timestamp>::minimum());


                    move |input, output| {
                        input.for_each(|cap, data| {
                            capabilities.insert(cap.retain());
                            batcher.push_batch(data);
                        });

                        if prev_frontier.borrow() != input.frontier().frontier() {
                            if capabilities.elements().iter().any(|c| !input.frontier().less_equal(c.time())) {
                                let mut upper = Antichain::new();   // re-used allocation for sealing batches.

                                // For each capability not in advance of the input frontier ...
                                for (index, capability) in capabilities.elements().iter().enumerate() {
                                    if !input.frontier().less_equal(capability.time()) {

                                        // Assemble the upper bound on times we can commit with this capabilities.
                                        // We must respect the input frontier, and *subsequent* capabilities, as
                                        // we are pretending to retire the capability changes one by one.
                                        upper.clear();
                                        for time in input.frontier().frontier().iter() {
                                            upper.insert(time.clone());
                                        }
                                        for other_capability in &capabilities.elements()[(index + 1)..] {
                                            upper.insert(other_capability.time().clone());
                                        }

                                        // send the batch to downstream consumers, empty or not.
                                        let session = output.session(&capabilities.elements()[index]);
                                        // Extract updates not in advance of `upper`.
                                        let builder = ConsolidateBuilder(session);
                                        let () = batcher.seal(upper.borrow(), |_, _, _| builder);
                                    }
                                }

                                // Having extracted and sent batches between each capability and the input frontier,
                                // we should downgrade all capabilities to match the batcher's lower update frontier.
                                // This may involve discarding capabilities, which is fine as any new updates arrive
                                // in messages with new capabilities.

                                let mut new_capabilities = Antichain::new();
                                for time in batcher.frontier().iter() {
                                    if let Some(capability) = capabilities.elements().iter().find(|c| c.time().less_equal(time)) {
                                        new_capabilities.insert(capability.delayed(time));
                                    } else {
                                        panic!("failed to find capability");
                                    }
                                }

                                capabilities = new_capabilities;
                            }

                            prev_frontier.clear();
                            prev_frontier.extend(input.frontier().frontier().iter().cloned());
                        }
                    }
                })
                .as_collection()
        }
    }

    struct ConsolidateBuilder<'a, D: Data, T: Timestamp, R: Data>(Session<'a, T, Vec<(D, T, R)>, Counter<T, (D, T, R), Tee<T, (D, T, R)>>>);

    impl<'a, D: Data, T: Timestamp, R: Data> Builder for ConsolidateBuilder<'a, D, T, R> {
        type Item = ((D, ()), T, R);
        type Time = T;
        type Output = ();

        fn new() -> Self {
            unimplemented!()
        }

        fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
            unimplemented!()
        }

        fn push(&mut self, element: Self::Item) {
            self.0.give((element.0.0, element.1, element.2));
        }

        fn copy(&mut self, element: &Self::Item) {
            self.0.give((element.0.0.clone(), element.1.clone(), element.2.clone()));
        }

        fn done(self, _lower: Antichain<Self::Time>, _upper: Antichain<Self::Time>, _since: Antichain<Self::Time>) -> Self::Output {
            ()
        }
    }
}
