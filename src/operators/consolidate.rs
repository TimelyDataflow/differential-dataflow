//! Aggregates the weights of equal records into at most one record.
//!
//! As differential dataflow streams are unordered and taken to be the accumulation of all records,
//! no semantic change happens via `consolidate`. However, there is a practical difference between
//! a collection that aggregates down to zero records, and one that actually has no records. The
//! underlying system can more clearly see that no work must be done in the later case, and we can
//! drop out of, e.g. iterative computations.

use timely::dataflow::channels::pact::{Exchange, ParallelizationContract};
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::pushers::{Counter, Tee};
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::Scope;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::difference::Semigroup;
use crate::{AsCollection, Collection, ExchangeData, Hashable};

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
        let exchange =
            Exchange::new(move |update: &((D, ()), G::Timestamp, R)| (update.0).0.hashed().into());
        consolidate_pact::<Tr::Batcher, _, _, _, _, _>(&self.map(|d| (d, ())), exchange, name)
            .map(|(d, ())| d)
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

/// Aggregates the weights of equal records into at most one record.
///
/// The data are accumulated in place, each held back until their timestamp has completed.
///
/// This serves as a low-level building-block for more user-friendly functions.
///
/// # Examples
///
/// ```
/// use timely::dataflow::channels::pact::Exchange;
/// use differential_dataflow::Hashable;
/// use differential_dataflow::input::Input;
/// use differential_dataflow::operators::consolidate::consolidate_pact;
/// use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
///
/// ::timely::example(|scope| {
///
///     let x = scope.new_collection_from(1 .. 10u32).1.map(|i| (i, ()));
///
///     let c = x.negate().concat(&x);
///     let exchange = Exchange::new(|update: &((u32,()),u64,isize)| (update.0).0.hashed().into());
///     consolidate_pact::<MergeBatcher<_, _, _, _>, _, _, _, _, _>(&c, exchange, "Consolidate inputs") // <-- ensures cancellation occurs
///      .assert_empty();
/// });
/// ```
pub fn consolidate_pact<B, P, G, K, V, R>(
    collection: &Collection<G, (K, V), R>,
    pact: P,
    name: &str,
) -> Collection<G, (K, V), R>
where
    G: Scope,
    K: Data,
    V: Data,
    R: Data + Semigroup,
    B: Batcher<Item = ((K, V), G::Timestamp, R), Time = G::Timestamp> + 'static,
    P: ParallelizationContract<G::Timestamp, ((K, V), G::Timestamp, R)>,
{
    collection
        .inner
        .unary_frontier(pact, name, |_cap, info| {
            // Acquire a logger for arrange events.
            let logger = {
                let scope = collection.scope();
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
                    if capabilities
                        .elements()
                        .iter()
                        .any(|c| !input.frontier().less_equal(c.time()))
                    {
                        let mut upper = Antichain::new(); // re-used allocation for sealing batches.

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
                                let _: () = batcher.seal(upper.borrow(), |_, _, _| builder);
                            }
                        }

                        // Having extracted and sent batches between each capability and the input frontier,
                        // we should downgrade all capabilities to match the batcher's lower update frontier.
                        // This may involve discarding capabilities, which is fine as any new updates arrive
                        // in messages with new capabilities.

                        let mut new_capabilities = Antichain::new();
                        for time in batcher.frontier().iter() {
                            if let Some(capability) = capabilities
                                .elements()
                                .iter()
                                .find(|c| c.time().less_equal(time))
                            {
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

/// A builder that wraps a session for direct output to a stream.
struct ConsolidateBuilder<'a, K: Data, V: Data, T: Timestamp, R: Data>(
    Session<'a, T, Vec<((K, V), T, R)>, Counter<T, ((K, V), T, R), Tee<T, ((K, V), T, R)>>>,
);

impl<'a, K: Data, V: Data, T: Timestamp, R: Data> Builder for ConsolidateBuilder<'a, K, V, T, R> {
    type Item = ((K, V), T, R);
    type Time = T;
    type Output = ();

    fn new() -> Self {
        unimplemented!()
    }

    fn with_capacity(_keys: usize, _vals: usize, _upds: usize) -> Self {
        unimplemented!()
    }

    fn push(&mut self, element: Self::Item) {
        self.0.give((element.0, element.1, element.2));
    }

    fn copy(&mut self, element: &Self::Item) {
        self.0.give(element.clone());
    }

    fn done(
        self,
        _lower: Antichain<Self::Time>,
        _upper: Antichain<Self::Time>,
        _since: Antichain<Self::Time>,
    ) { }
}
