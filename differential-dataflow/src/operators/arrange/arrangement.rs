//! Arranges a collection into a re-usable trace structure.
//!
//! The `arrange` operator applies to a differential dataflow `Collection` and returns an `Arranged`
//! structure, provides access to both an indexed form of accepted updates as well as a stream of
//! batches of newly arranged updates.
//!
//! Several operators (`join`, `reduce`, and `count`, among others) are implemented against `Arranged`,
//! and can be applied directly to arranged data instead of the collection. Internally, the operators
//! will borrow the shared state, and listen on the timely stream for shared batches of data. The
//! resources to index the collection---communication, computation, and memory---are spent only once,
//! and only one copy of the index needs to be maintained as the collection changes.
//!
//! The arranged collection is stored in a trace, whose append-only operation means that it is safe to
//! share between the single `arrange` writer and multiple readers. Each reader is expected to interrogate
//! the trace only at times for which it knows the trace is complete, as indicated by the frontiers on its
//! incoming channels. Failing to do this is "safe" in the Rust sense of memory safety, but the reader may
//! see ill-defined data at times for which the trace is not complete. (All current implementations
//! commit only completed data to the trace).

use timely::dataflow::operators::{Enter, vec::Map};
use timely::order::PartialOrder;
use timely::dataflow::{Scope, Stream};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::progress::Timestamp;
use timely::progress::Antichain;
use timely::dataflow::operators::Capability;

use crate::{Data, VecCollection, AsCollection};
use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::{self, Trace, TraceReader, BatchReader, Batcher, Builder, Cursor};

use trace::wrappers::enter::{TraceEnter, BatchEnter,};
use trace::wrappers::enter_at::TraceEnter as TraceEnterAt;
use trace::wrappers::enter_at::BatchEnter as BatchEnterAt;

use super::TraceAgent;

/// An arranged collection of `(K,V)` values.
///
/// An `Arranged` allows multiple differential operators to share the resources (communication,
/// computation, memory) required to produce and maintain an indexed representation of a collection.
pub struct Arranged<'scope, Tr: TraceReader> {
    /// A stream containing arranged updates.
    ///
    /// This stream contains the same batches of updates the trace itself accepts, so there should
    /// be no additional overhead to receiving these records. The batches can be navigated just as
    /// the batches in the trace, by key and by value.
    pub stream: Stream<'scope, Tr::Time, Vec<Tr::Batch>>,
    /// A shared trace, updated by the `Arrange` operator and readable by others.
    pub trace: Tr,
}

impl<'scope, Tr: TraceReader+Clone> Clone for Arranged<'scope, Tr> {
    fn clone(&self) -> Self {
        Arranged {
            stream: self.stream.clone(),
            trace: self.trace.clone(),
        }
    }
}

use ::timely::progress::timestamp::Refines;
use timely::Container;

impl<'scope, Tr: TraceReader> Arranged<'scope, Tr> {
    /// Brings an arranged collection into a nested scope.
    ///
    /// This method produces a proxy trace handle that uses the same backing data, but acts as if the timestamps
    /// have all been extended with an additional coordinate with the default value. The resulting collection does
    /// not vary with the new timestamp coordinate.
    pub fn enter<'inner, TInner>(self, child: Scope<'inner, TInner>) -> Arranged<'inner, TraceEnter<Tr, TInner>>
    where
        TInner: Refines<Tr::Time>+Lattice,
    {
        Arranged {
            stream: self.stream.enter(child).map(|bw| BatchEnter::make_from(bw)),
            trace: TraceEnter::make_from(self.trace),
        }
    }

    /// Brings an arranged collection into a nested region.
    ///
    /// This method only applies to *regions*, which are subscopes with the same timestamp
    /// as their containing scope. In this case, the trace type does not need to change.
    pub fn enter_region<'inner>(self, child: Scope<'inner, Tr::Time>) -> Arranged<'inner, Tr> {
        Arranged {
            stream: self.stream.enter(child),
            trace: self.trace,
        }
    }

    /// Brings an arranged collection into a nested scope.
    ///
    /// This method produces a proxy trace handle that uses the same backing data, but acts as if the timestamps
    /// have all been extended with an additional coordinate with the default value. The resulting collection does
    /// not vary with the new timestamp coordinate.
    pub fn enter_at<'inner, TInner, F, P>(self, child: Scope<'inner, TInner>, logic: F, prior: P) -> Arranged<'inner, TraceEnterAt<Tr, TInner, F, P>>
    where
        TInner: Refines<Tr::Time>+Lattice+'static,
        F: FnMut(Tr::Key<'_>, Tr::Val<'_>, Tr::TimeGat<'_>)->TInner+Clone+'static,
        P: FnMut(&TInner)->Tr::Time+Clone+'static,
    {
        let logic1 = logic.clone();
        let logic2 = logic.clone();
        Arranged {
            trace: TraceEnterAt::make_from(self.trace, logic1, prior),
            stream: self.stream.enter(child).map(move |bw| BatchEnterAt::make_from(bw, logic2.clone())),
        }
    }

    /// Extracts a collection of any container from the stream of batches.
    ///
    /// This method is like `self.stream.flat_map`, except that it produces containers
    /// directly, rather than form a container of containers as `flat_map` would.
    pub fn as_container<I, L>(self, mut logic: L) -> crate::Collection<'scope, Tr::Time, I::Item>
    where
        I: IntoIterator<Item: Container>,
        L: FnMut(Tr::Batch) -> I+'static,
    {
        self.stream.unary(Pipeline, "AsContainer", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                let mut session = output.session(&time);
                for wrapper in data.drain(..) {
                    for mut container in logic(wrapper) {
                        session.give_container(&mut container);
                    }
                }
            });
        })
        .as_collection()
    }

    /// Flattens the stream into a `VecCollection`.
    ///
    /// The underlying `Stream<T, Vec<BatchWrapper<T::Batch>>>` is a much more efficient way to access the data,
    /// and this method should only be used when the data need to be transformed or exchanged, rather than
    /// supplied as arguments to an operator using the same key-value structure.
    pub fn as_collection<D: Data, L>(self, mut logic: L) -> VecCollection<'scope, Tr::Time, D, Tr::Diff>
        where
            L: FnMut(Tr::Key<'_>, Tr::Val<'_>) -> D+'static,
    {
        self.flat_map_ref(move |key, val| Some(logic(key,val)))
    }

    /// Flattens the stream into a `VecCollection`.
    ///
    /// The underlying `Stream<T, Vec<BatchWrapper<T::Batch>>>` is a much more efficient way to access the data,
    /// and this method should only be used when the data need to be transformed or exchanged, rather than
    /// supplied as arguments to an operator using the same key-value structure.
    ///
    /// The method takes `K` and `V` as generic arguments, in order to constrain the reference types to support
    /// cloning into owned types. If this bound does not work, the `as_collection` method allows arbitrary logic
    /// on the reference types.
    pub fn as_vecs<K, V>(self) -> VecCollection<'scope, Tr::Time, (K, V), Tr::Diff>
    where
        K: crate::ExchangeData,
        V: crate::ExchangeData,
        Tr: for<'a> TraceReader<Key<'a> = &'a K, Val<'a> = &'a V>,
    {
        self.flat_map_ref(move |key, val| [(key.clone(), val.clone())])
    }

    /// Extracts elements from an arrangement as a `VecCollection`.
    ///
    /// The supplied logic may produce an iterator over output values, allowing either
    /// filtering or flat mapping as part of the extraction.
    pub fn flat_map_ref<I, L>(self, logic: L) -> VecCollection<'scope, Tr::Time, I::Item, Tr::Diff>
        where
            I: IntoIterator<Item: Data>,
            L: FnMut(Tr::Key<'_>, Tr::Val<'_>) -> I+'static,
    {
        Self::flat_map_batches(self.stream, logic)
    }

    /// Extracts elements from a stream of batches as a `VecCollection`.
    ///
    /// The supplied logic may produce an iterator over output values, allowing either
    /// filtering or flat mapping as part of the extraction.
    ///
    /// This method exists for streams of batches without the corresponding arrangement.
    /// If you have the arrangement, its `flat_map_ref` method is equivalent to this.
    pub fn flat_map_batches<I, L>(stream: Stream<'scope, Tr::Time, Vec<Tr::Batch>>, mut logic: L) -> VecCollection<'scope, Tr::Time, I::Item, Tr::Diff>
    where
        I: IntoIterator<Item: Data>,
        L: FnMut(Tr::Key<'_>, Tr::Val<'_>) -> I+'static,
    {
        stream.unary(Pipeline, "AsCollection", move |_,_| move |input, output| {
            input.for_each(|time, data| {
                let mut session = output.session(&time);
                for wrapper in data.iter() {
                    let batch = &wrapper;
                    let mut cursor = batch.cursor();
                    while let Some(key) = cursor.get_key(batch) {
                        while let Some(val) = cursor.get_val(batch) {
                            for datum in logic(key, val) {
                                cursor.map_times(batch, |time, diff| {
                                    session.give((datum.clone(), Tr::owned_time(time), Tr::owned_diff(diff)));
                                });
                            }
                            cursor.step_val(batch);
                        }
                        cursor.step_key(batch);
                    }
                }
            });
        })
        .as_collection()
    }
}


use crate::difference::Multiply;
// Direct join implementations.
impl<'scope, Tr1: TraceReader+'static> Arranged<'scope, Tr1> {
    /// A convenience method to join and produce `VecCollection` output.
    ///
    /// Avoid this method, as it is likely to evolve into one without the `VecCollection` opinion.
    pub fn join_core<Tr2,I,L>(self, other: Arranged<'scope, Tr2>, mut result: L) -> VecCollection<'scope, Tr1::Time,I::Item,<Tr1::Diff as Multiply<Tr2::Diff>>::Output>
    where
        Tr2: for<'a> TraceReader<Key<'a>=Tr1::Key<'a>,Time=Tr1::Time>+Clone+'static,
        Tr1::Diff: Multiply<Tr2::Diff, Output: Semigroup+'static>,
        I: IntoIterator<Item: Data>,
        L: FnMut(Tr1::Key<'_>,Tr1::Val<'_>,Tr2::Val<'_>)->I+'static
    {
        let mut result = move |k: Tr1::Key<'_>, v1: Tr1::Val<'_>, v2: Tr2::Val<'_>, t: &Tr1::Time, r1: &Tr1::Diff, r2: &Tr2::Diff| {
            let t = t.clone();
            let r = (r1.clone()).multiply(r2);
            result(k, v1, v2).into_iter().map(move |d| (d, t.clone(), r.clone()))
        };

        use crate::operators::join::join_traces;
        join_traces::<_, _, _, crate::consolidation::ConsolidatingContainerBuilder<_>>(
            self,
            other,
            move |k, v1, v2, t, d1, d2, c| {
                for datum in result(k, v1, v2, t, d1, d2) {
                    c.give(datum);
                }
            }
        )
            .as_collection()
    }
}

// Direct reduce implementations.
use crate::difference::Abelian;
impl<'scope, Tr1: TraceReader+'static> Arranged<'scope, Tr1> {
    /// A direct implementation of `ReduceCore::reduce_abelian`.
    pub fn reduce_abelian<L, Bu, Tr2, P>(self, name: &str, mut logic: L, push: P) -> Arranged<'scope, TraceAgent<Tr2>>
    where
        Tr2: for<'a> Trace<
            Key<'a>= Tr1::Key<'a>,
            ValOwn: Data,
            Time=Tr1::Time,
            Diff: Abelian,
        >+'static,
        Bu: Builder<Time=Tr1::Time, Output = Tr2::Batch, Input: Default>,
        L: FnMut(Tr1::Key<'_>, &[(Tr1::Val<'_>, Tr1::Diff)], &mut Vec<(Tr2::ValOwn, Tr2::Diff)>)+'static,
        P: FnMut(&mut Bu::Input, Tr1::Key<'_>, &mut Vec<(Tr2::ValOwn, Tr2::Time, Tr2::Diff)>) + 'static,
    {
        self.reduce_core::<_,Bu,Tr2,_>(name, move |key, input, output, change| {
            if !input.is_empty() {
                logic(key, input, change);
            }
            change.extend(output.drain(..).map(|(x,mut d)| { d.negate(); (x, d) }));
            crate::consolidation::consolidate(change);
        }, push)
    }

    /// A direct implementation of `ReduceCore::reduce_core`.
    pub fn reduce_core<L, Bu, Tr2, P>(self, name: &str, logic: L, push: P) -> Arranged<'scope, TraceAgent<Tr2>>
    where
        Tr2: for<'a> Trace<
            Key<'a>=Tr1::Key<'a>,
            ValOwn: Data,
            Time=Tr1::Time,
        >+'static,
        Bu: Builder<Time=Tr1::Time, Output = Tr2::Batch, Input: Default>,
        L: FnMut(Tr1::Key<'_>, &[(Tr1::Val<'_>, Tr1::Diff)], &mut Vec<(Tr2::ValOwn, Tr2::Diff)>, &mut Vec<(Tr2::ValOwn, Tr2::Diff)>)+'static,
        P: FnMut(&mut Bu::Input, Tr1::Key<'_>, &mut Vec<(Tr2::ValOwn, Tr2::Time, Tr2::Diff)>) + 'static,
    {
        use crate::operators::reduce::reduce_trace;
        reduce_trace::<_,Bu,_,_,_>(self, name, logic, push)
    }
}


impl<'scope, Tr: TraceReader> Arranged<'scope, Tr> {
    /// Brings an arranged collection out of a nested region.
    ///
    /// This method only applies to *regions*, which are subscopes with the same timestamp
    /// as their containing scope. In this case, the trace type does not need to change.
    pub fn leave_region<'outer>(self, outer: Scope<'outer, Tr::Time>) -> Arranged<'outer, Tr> {
        use timely::dataflow::operators::Leave;
        Arranged {
            stream: self.stream.leave(outer),
            trace: self.trace,
        }
    }
}

/// A type that can be arranged as if a collection of updates.
pub trait Arrange<'scope, T: Timestamp+Lattice, C> : Sized {
    /// Arranges updates into a shared trace.
    fn arrange<Ba, Bu, Tr>(self) -> Arranged<'scope, TraceAgent<Tr>>
    where
        Ba: Batcher<Input=C, Time=T> + 'static,
        Bu: Builder<Time=T, Input=Ba::Output, Output = Tr::Batch>,
        Tr: Trace<Time=T> + 'static,
    {
        self.arrange_named::<Ba, Bu, Tr>("Arrange")
    }

    /// Arranges updates into a shared trace, with a supplied name.
    fn arrange_named<Ba, Bu, Tr>(self, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
    where
        Ba: Batcher<Input=C, Time=T> + 'static,
        Bu: Builder<Time=T, Input=Ba::Output, Output = Tr::Batch>,
        Tr: Trace<Time=T> + 'static,
    ;
}

/// Arranges a stream of updates by a key, configured with a name and a parallelization contract.
///
/// This operator arranges a stream of values into a shared trace, whose contents it maintains.
/// It uses the supplied parallelization contract to distribute the data, which does not need to
/// be consistently by key (though this is the most common).
pub fn arrange_core<'scope, P, Ba, Bu, Tr>(stream: Stream<'scope, Tr::Time, Ba::Input>, pact: P, name: &str) -> Arranged<'scope, TraceAgent<Tr>>
where
    P: ParallelizationContract<Tr::Time, Ba::Input>,
    Ba: Batcher<Time=Tr::Time,Input: Container> + 'static,
    Bu: Builder<Time=Tr::Time, Input=Ba::Output, Output = Tr::Batch>,
    Tr: Trace+'static,
{
    // The `Arrange` operator is tasked with reacting to an advancing input
    // frontier by producing the sequence of batches whose lower and upper
    // bounds are those frontiers, containing updates at times greater or
    // equal to lower and not greater or equal to upper.
    //
    // The operator uses its batch type's `Batcher`, which accepts update
    // triples and responds to requests to "seal" batches (presented as new
    // upper frontiers).
    //
    // Each sealed batch is presented to the trace, and if at all possible
    // transmitted along the outgoing channel. Empty batches may not have
    // a corresponding capability, as they are only retained for actual data
    // held by the batcher, which may prevents the operator from sending an
    // empty batch.

    let mut reader: Option<TraceAgent<Tr>> = None;

    // fabricate a data-parallel operator using the `unary_notify` pattern.
    let reader_ref = &mut reader;
    let scope = stream.scope();

    let stream = stream.unary_frontier(pact, name, move |_capability, info| {

        // Acquire a logger for arrange events.
        let logger = scope.worker().logger_for::<crate::logging::DifferentialEventBuilder>("differential/arrange").map(Into::into);

        // Where we will deposit received updates, and from which we extract batches.
        let mut batcher = Ba::new(logger.clone(), info.global_id);

        // Capabilities for the lower envelope of updates in `batcher`.
        let mut capabilities = Antichain::<Capability<Tr::Time>>::new();

        let activator = Some(scope.activator_for(std::rc::Rc::clone(&info.address)));
        let mut empty_trace = Tr::new(info.clone(), logger.clone(), activator);
        // If there is default exertion logic set, install it.
        if let Some(exert_logic) = scope.worker().config().get::<trace::ExertionLogic>("differential/default_exert_logic").cloned() {
            empty_trace.set_exert_logic(exert_logic);
        }

        let (reader_local, mut writer) = TraceAgent::new(empty_trace, info, logger);

        *reader_ref = Some(reader_local);

        // Initialize to the minimal input frontier.
        let mut prev_frontier = Antichain::from_elem(Tr::Time::minimum());

        move |(input, frontier), output| {

            // As we receive data, we need to (i) stash the data and (ii) keep *enough* capabilities.
            // We don't have to keep all capabilities, but we need to be able to form output messages
            // when we realize that time intervals are complete.

            input.for_each(|cap, data| {
                capabilities.insert(cap.retain(0));
                batcher.push_container(data);
            });

            // The frontier may have advanced by multiple elements, which is an issue because
            // timely dataflow currently only allows one capability per message. This means we
            // must pretend to process the frontier advances one element at a time, batching
            // and sending smaller bites than we might have otherwise done.

            // Assert that the frontier never regresses.
            assert!(PartialOrder::less_equal(&prev_frontier.borrow(), &frontier.frontier()));

            // Test to see if strict progress has occurred, which happens whenever the new
            // frontier isn't equal to the previous. It is only in this case that we have any
            // data processing to do.
            if prev_frontier.borrow() != frontier.frontier() {
                // There are two cases to handle with some care:
                //
                // 1. If any held capabilities are not in advance of the new input frontier,
                //    we must carve out updates now in advance of the new input frontier and
                //    transmit them as batches, which requires appropriate *single* capabilities;
                //    Until timely dataflow supports multiple capabilities on messages, at least.
                //
                // 2. If there are no held capabilities in advance of the new input frontier,
                //    then there are no updates not in advance of the new input frontier and
                //    we can simply create an empty input batch with the new upper frontier
                //    and feed this to the trace agent (but not along the timely output).

                // If there is at least one capability not in advance of the input frontier ...
                if capabilities.elements().iter().any(|c| !frontier.less_equal(c.time())) {

                    let mut upper = Antichain::new();   // re-used allocation for sealing batches.

                    // For each capability not in advance of the input frontier ...
                    for (index, capability) in capabilities.elements().iter().enumerate() {

                        if !frontier.less_equal(capability.time()) {

                            // Assemble the upper bound on times we can commit with this capabilities.
                            // We must respect the input frontier, and *subsequent* capabilities, as
                            // we are pretending to retire the capability changes one by one.
                            upper.clear();
                            for time in frontier.frontier().iter() {
                                upper.insert(time.clone());
                            }
                            for other_capability in &capabilities.elements()[(index + 1) .. ] {
                                upper.insert(other_capability.time().clone());
                            }

                            // Extract updates not in advance of `upper`.
                            let batch = batcher.seal::<Bu>(upper.clone());

                            writer.insert(batch.clone(), Some(capability.time().clone()));

                            // send the batch to downstream consumers, empty or not.
                            output.session(&capabilities.elements()[index]).give(batch);
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
                        }
                        else {
                            panic!("failed to find capability");
                        }
                    }

                    capabilities = new_capabilities;
                }
                else {
                    // Announce progress updates, even without data.
                    let _batch = batcher.seal::<Bu>(frontier.frontier().to_owned());
                    writer.seal(frontier.frontier().to_owned());
                }

                prev_frontier.clear();
                prev_frontier.extend(frontier.frontier().iter().cloned());
            }

            writer.exert();
        }
    });

    Arranged { stream, trace: reader.unwrap() }
}
