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

use timely::dataflow::operators::{Enter, Map};
use timely::order::PartialOrder;
use timely::dataflow::{Scope, Stream, StreamCore};
use timely::dataflow::operators::generic::Operator;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline, Exchange};
use timely::progress::Timestamp;
use timely::progress::Antichain;
use timely::dataflow::operators::Capability;

use crate::{Data, ExchangeData, Collection, AsCollection, Hashable};
use crate::difference::Semigroup;
use crate::lattice::Lattice;
use crate::trace::{self, Trace, TraceReader, BatchReader, Batcher, Builder, Cursor};
use crate::trace::implementations::{KeyBatcher, KeyBuilder, KeySpine, ValBatcher, ValBuilder, ValSpine};

use trace::wrappers::enter::{TraceEnter, BatchEnter,};
use trace::wrappers::enter_at::TraceEnter as TraceEnterAt;
use trace::wrappers::enter_at::BatchEnter as BatchEnterAt;
use trace::wrappers::filter::{TraceFilter, BatchFilter};

use super::TraceAgent;

/// An arranged collection of `(K,V)` values.
///
/// An `Arranged` allows multiple differential operators to share the resources (communication,
/// computation, memory) required to produce and maintain an indexed representation of a collection.
pub struct Arranged<G, Tr>
where
    G: Scope<Timestamp: Lattice+Ord>,
    Tr: TraceReader+Clone,
{
    /// A stream containing arranged updates.
    ///
    /// This stream contains the same batches of updates the trace itself accepts, so there should
    /// be no additional overhead to receiving these records. The batches can be navigated just as
    /// the batches in the trace, by key and by value.
    pub stream: Stream<G, Tr::Batch>,
    /// A shared trace, updated by the `Arrange` operator and readable by others.
    pub trace: Tr,
    // TODO : We might have an `Option<Collection<G, (K, V)>>` here, which `as_collection` sets and
    // returns when invoked, so as to not duplicate work with multiple calls to `as_collection`.
}

impl<G, Tr> Clone for Arranged<G, Tr>
where
    G: Scope<Timestamp=Tr::Time>,
    Tr: TraceReader + Clone,
{
    fn clone(&self) -> Self {
        Arranged {
            stream: self.stream.clone(),
            trace: self.trace.clone(),
        }
    }
}

use ::timely::dataflow::scopes::Child;
use ::timely::progress::timestamp::Refines;
use timely::Container;
use timely::container::PushInto;

impl<G, Tr> Arranged<G, Tr>
where
    G: Scope<Timestamp=Tr::Time>,
    Tr: TraceReader + Clone,
{
    /// Brings an arranged collection into a nested scope.
    ///
    /// This method produces a proxy trace handle that uses the same backing data, but acts as if the timestamps
    /// have all been extended with an additional coordinate with the default value. The resulting collection does
    /// not vary with the new timestamp coordinate.
    pub fn enter<'a, TInner>(&self, child: &Child<'a, G, TInner>)
        -> Arranged<Child<'a, G, TInner>, TraceEnter<Tr, TInner>>
        where
            TInner: Refines<G::Timestamp>+Lattice+Timestamp+Clone,
    {
        Arranged {
            stream: self.stream.enter(child).map(|bw| BatchEnter::make_from(bw)),
            trace: TraceEnter::make_from(self.trace.clone()),
        }
    }

    /// Brings an arranged collection into a nested region.
    ///
    /// This method only applies to *regions*, which are subscopes with the same timestamp
    /// as their containing scope. In this case, the trace type does not need to change.
    pub fn enter_region<'a>(&self, child: &Child<'a, G, G::Timestamp>)
        -> Arranged<Child<'a, G, G::Timestamp>, Tr> {
        Arranged {
            stream: self.stream.enter(child),
            trace: self.trace.clone(),
        }
    }

    /// Brings an arranged collection into a nested scope.
    ///
    /// This method produces a proxy trace handle that uses the same backing data, but acts as if the timestamps
    /// have all been extended with an additional coordinate with the default value. The resulting collection does
    /// not vary with the new timestamp coordinate.
    pub fn enter_at<'a, TInner, F, P>(&self, child: &Child<'a, G, TInner>, logic: F, prior: P)
        -> Arranged<Child<'a, G, TInner>, TraceEnterAt<Tr, TInner, F, P>>
        where
            TInner: Refines<G::Timestamp>+Lattice+Timestamp+Clone+'static,
            F: FnMut(Tr::Key<'_>, Tr::Val<'_>, Tr::TimeGat<'_>)->TInner+Clone+'static,
            P: FnMut(&TInner)->Tr::Time+Clone+'static,
        {
        let logic1 = logic.clone();
        let logic2 = logic.clone();
        Arranged {
            trace: TraceEnterAt::make_from(self.trace.clone(), logic1, prior),
            stream: self.stream.enter(child).map(move |bw| BatchEnterAt::make_from(bw, logic2.clone())),
        }
    }

    /// Filters an arranged collection.
    ///
    /// This method produces a new arrangement backed by the same shared
    /// arrangement as `self`, paired with user-specified logic that can
    /// filter by key and value. The resulting collection is restricted
    /// to the keys and values that return true under the user predicate.
    ///
    /// # Examples
    ///
    /// ```
    /// use differential_dataflow::input::Input;
    /// use differential_dataflow::operators::arrange::ArrangeByKey;
    ///
    /// ::timely::example(|scope| {
    ///
    ///     let arranged =
    ///     scope.new_collection_from(0 .. 10).1
    ///          .map(|x| (x, x+1))
    ///          .arrange_by_key();
    ///
    ///     arranged
    ///         .filter(|k,v| k == v)
    ///         .as_collection(|k,v| (*k,*v))
    ///         .assert_empty();
    /// });
    /// ```
    pub fn filter<F>(&self, logic: F)
        -> Arranged<G, TraceFilter<Tr, F>>
        where
            F: FnMut(Tr::Key<'_>, Tr::Val<'_>)->bool+Clone+'static,
    {
        let logic1 = logic.clone();
        let logic2 = logic.clone();
        Arranged {
            trace: TraceFilter::make_from(self.trace.clone(), logic1),
            stream: self.stream.map(move |bw| BatchFilter::make_from(bw, logic2.clone())),
        }
    }
    /// Flattens the stream into a `Collection`.
    ///
    /// The underlying `Stream<G, BatchWrapper<T::Batch>>` is a much more efficient way to access the data,
    /// and this method should only be used when the data need to be transformed or exchanged, rather than
    /// supplied as arguments to an operator using the same key-value structure.
    pub fn as_collection<D: Data, L>(&self, mut logic: L) -> Collection<G, D, Tr::Diff>
        where
            L: FnMut(Tr::Key<'_>, Tr::Val<'_>) -> D+'static,
    {
        self.flat_map_ref(move |key, val| Some(logic(key,val)))
    }

    /// Extracts elements from an arrangement as a collection.
    ///
    /// The supplied logic may produce an iterator over output values, allowing either
    /// filtering or flat mapping as part of the extraction.
    pub fn flat_map_ref<I, L>(&self, logic: L) -> Collection<G, I::Item, Tr::Diff>
        where
            I: IntoIterator<Item: Data>,
            L: FnMut(Tr::Key<'_>, Tr::Val<'_>) -> I+'static,
    {
        Self::flat_map_batches(&self.stream, logic)
    }

    /// Extracts elements from a stream of batches as a collection.
    ///
    /// The supplied logic may produce an iterator over output values, allowing either
    /// filtering or flat mapping as part of the extraction.
    ///
    /// This method exists for streams of batches without the corresponding arrangement.
    /// If you have the arrangement, its `flat_map_ref` method is equivalent to this.
    pub fn flat_map_batches<I, L>(stream: &Stream<G, Tr::Batch>, mut logic: L) -> Collection<G, I::Item, Tr::Diff>
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
impl<G, T1> Arranged<G, T1>
where
    G: Scope<Timestamp=T1::Time>,
    T1: TraceReader + Clone + 'static,
{
    /// A direct implementation of the `JoinCore::join_core` method.
    pub fn join_core<T2,I,L>(&self, other: &Arranged<G,T2>, mut result: L) -> Collection<G,I::Item,<T1::Diff as Multiply<T2::Diff>>::Output>
    where
        T2: for<'a> TraceReader<Key<'a>=T1::Key<'a>,Time=T1::Time>+Clone+'static,
        T1::Diff: Multiply<T2::Diff, Output: Semigroup+'static>,
        I: IntoIterator<Item: Data>,
        L: FnMut(T1::Key<'_>,T1::Val<'_>,T2::Val<'_>)->I+'static
    {
        let result = move |k: T1::Key<'_>, v1: T1::Val<'_>, v2: T2::Val<'_>, t: &G::Timestamp, r1: &T1::Diff, r2: &T2::Diff| {
            let t = t.clone();
            let r = (r1.clone()).multiply(r2);
            result(k, v1, v2).into_iter().map(move |d| (d, t.clone(), r.clone()))
        };
        self.join_core_internal_unsafe(other, result)
    }
    /// A direct implementation of the `JoinCore::join_core_internal_unsafe` method.
    pub fn join_core_internal_unsafe<T2,I,L,D,ROut> (&self, other: &Arranged<G,T2>, mut result: L) -> Collection<G,D,ROut>
    where
        T2: for<'a> TraceReader<Key<'a>=T1::Key<'a>, Time=T1::Time>+Clone+'static,
        D: Data,
        ROut: Semigroup+'static,
        I: IntoIterator<Item=(D, G::Timestamp, ROut)>,
        L: FnMut(T1::Key<'_>, T1::Val<'_>,T2::Val<'_>,&G::Timestamp,&T1::Diff,&T2::Diff)->I+'static,
    {
        use crate::operators::join::join_traces;
        join_traces::<_, _, _, _, crate::consolidation::ConsolidatingContainerBuilder<_>>(
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
impl<G, T1> Arranged<G, T1>
where
    G: Scope<Timestamp = T1::Time>,
    T1: TraceReader + Clone + 'static,
{
    /// A direct implementation of `ReduceCore::reduce_abelian`.
    pub fn reduce_abelian<L, K, V, Bu, T2>(&self, name: &str, mut logic: L) -> Arranged<G, TraceAgent<T2>>
    where
        T1: TraceReader<KeyOwn = K>,
        T2: for<'a> Trace<
            Key<'a>= T1::Key<'a>,
            KeyOwn = K,
            ValOwn = V,
            Time=T1::Time,
            Diff: Abelian,
        >+'static,
        K: Ord + 'static,
        V: Data,
        Bu: Builder<Time=G::Timestamp, Output = T2::Batch, Input: Container + PushInto<((K, V), T2::Time, T2::Diff)>>,
        L: FnMut(T1::Key<'_>, &[(T1::Val<'_>, T1::Diff)], &mut Vec<(V, T2::Diff)>)+'static,
    {
        self.reduce_core::<_,K,V,Bu,T2>(name, move |key, input, output, change| {
            if !input.is_empty() {
                logic(key, input, change);
            }
            change.extend(output.drain(..).map(|(x,mut d)| { d.negate(); (x, d) }));
            crate::consolidation::consolidate(change);
        })
    }

    /// A direct implementation of `ReduceCore::reduce_core`.
    pub fn reduce_core<L, K, V, Bu, T2>(&self, name: &str, logic: L) -> Arranged<G, TraceAgent<T2>>
    where
        T1: TraceReader<KeyOwn = K>,
        T2: for<'a> Trace<
            Key<'a>=T1::Key<'a>,
            KeyOwn = K,
            ValOwn = V,
            Time=T1::Time,
        >+'static,
        K: Ord + 'static,
        V: Data,
        Bu: Builder<Time=G::Timestamp, Output = T2::Batch, Input: Container + PushInto<((K, V), T2::Time, T2::Diff)>>,
        L: FnMut(T1::Key<'_>, &[(T1::Val<'_>, T1::Diff)], &mut Vec<(V, T2::Diff)>, &mut Vec<(V, T2::Diff)>)+'static,
    {
        use crate::operators::reduce::reduce_trace;
        reduce_trace::<_,_,Bu,_,_,V,_>(self, name, logic)
    }
}


impl<'a, G, Tr> Arranged<Child<'a, G, G::Timestamp>, Tr>
where
    G: Scope<Timestamp=Tr::Time>,
    Tr: TraceReader + Clone,
{
    /// Brings an arranged collection out of a nested region.
    ///
    /// This method only applies to *regions*, which are subscopes with the same timestamp
    /// as their containing scope. In this case, the trace type does not need to change.
    pub fn leave_region(&self) -> Arranged<G, Tr> {
        use timely::dataflow::operators::Leave;
        Arranged {
            stream: self.stream.leave(),
            trace: self.trace.clone(),
        }
    }
}

/// A type that can be arranged as if a collection of updates.
pub trait Arrange<G, C>
where
    G: Scope<Timestamp: Lattice>,
{
    /// Arranges updates into a shared trace.
    fn arrange<Ba, Bu, Tr>(&self) -> Arranged<G, TraceAgent<Tr>>
    where
        Ba: Batcher<Input=C, Time=G::Timestamp> + 'static,
        Bu: Builder<Time=G::Timestamp, Input=Ba::Output, Output = Tr::Batch>,
        Tr: Trace<Time=G::Timestamp> + 'static,
    {
        self.arrange_named::<Ba, Bu, Tr>("Arrange")
    }

    /// Arranges updates into a shared trace, with a supplied name.
    fn arrange_named<Ba, Bu, Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        Ba: Batcher<Input=C, Time=G::Timestamp> + 'static,
        Bu: Builder<Time=G::Timestamp, Input=Ba::Output, Output = Tr::Batch>,
        Tr: Trace<Time=G::Timestamp> + 'static,
    ;
}

impl<G, K, V, R> Arrange<G, Vec<((K, V), G::Timestamp, R)>> for Collection<G, (K, V), R>
where
    G: Scope<Timestamp: Lattice>,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    R: ExchangeData + Semigroup,
{
    fn arrange_named<Ba, Bu, Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        Ba: Batcher<Input=Vec<((K, V), G::Timestamp, R)>, Time=G::Timestamp> + 'static,
        Bu: Builder<Time=G::Timestamp, Input=Ba::Output, Output = Tr::Batch>,
        Tr: Trace<Time=G::Timestamp> + 'static,
    {
        let exchange = Exchange::new(move |update: &((K,V),G::Timestamp,R)| (update.0).0.hashed().into());
        arrange_core::<_, _, Ba, Bu, _>(&self.inner, exchange, name)
    }
}

/// Arranges a stream of updates by a key, configured with a name and a parallelization contract.
///
/// This operator arranges a stream of values into a shared trace, whose contents it maintains.
/// It uses the supplied parallelization contract to distribute the data, which does not need to
/// be consistently by key (though this is the most common).
pub fn arrange_core<G, P, Ba, Bu, Tr>(stream: &StreamCore<G, Ba::Input>, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
where
    G: Scope<Timestamp: Lattice>,
    P: ParallelizationContract<G::Timestamp, Ba::Input>,
    Ba: Batcher<Time=G::Timestamp,Input: Container + Clone + 'static> + 'static,
    Bu: Builder<Time=G::Timestamp, Input=Ba::Output, Output = Tr::Batch>,
    Tr: Trace<Time=G::Timestamp>+'static,
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
        let logger = scope.logger_for::<crate::logging::DifferentialEventBuilder>("differential/arrange").map(Into::into);

        // Where we will deposit received updates, and from which we extract batches.
        let mut batcher = Ba::new(logger.clone(), info.global_id);

        // Capabilities for the lower envelope of updates in `batcher`.
        let mut capabilities = Antichain::<Capability<G::Timestamp>>::new();

        let activator = Some(scope.activator_for(info.address.clone()));
        let mut empty_trace = Tr::new(info.clone(), logger.clone(), activator);
        // If there is default exertion logic set, install it.
        if let Some(exert_logic) = scope.config().get::<trace::ExertionLogic>("differential/default_exert_logic").cloned() {
            empty_trace.set_exert_logic(exert_logic);
        }

        let (reader_local, mut writer) = TraceAgent::new(empty_trace, info, logger);

        *reader_ref = Some(reader_local);

        // Initialize to the minimal input frontier.
        let mut prev_frontier = Antichain::from_elem(<G::Timestamp as Timestamp>::minimum());

        move |input, output| {

            // As we receive data, we need to (i) stash the data and (ii) keep *enough* capabilities.
            // We don't have to keep all capabilities, but we need to be able to form output messages
            // when we realize that time intervals are complete.

            input.for_each(|cap, data| {
                capabilities.insert(cap.retain());
                batcher.push_container(data);
            });

            // The frontier may have advanced by multiple elements, which is an issue because
            // timely dataflow currently only allows one capability per message. This means we
            // must pretend to process the frontier advances one element at a time, batching
            // and sending smaller bites than we might have otherwise done.

            // Assert that the frontier never regresses.
            assert!(PartialOrder::less_equal(&prev_frontier.borrow(), &input.frontier().frontier()));

            // Test to see if strict progress has occurred, which happens whenever the new
            // frontier isn't equal to the previous. It is only in this case that we have any
            // data processing to do.
            if prev_frontier.borrow() != input.frontier().frontier() {
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
                    let _batch = batcher.seal::<Bu>(input.frontier().frontier().to_owned());
                    writer.seal(input.frontier().frontier().to_owned());
                }

                prev_frontier.clear();
                prev_frontier.extend(input.frontier().frontier().iter().cloned());
            }

            writer.exert();
        }
    });

    Arranged { stream, trace: reader.unwrap() }
}

impl<G, K: ExchangeData+Hashable, R: ExchangeData+Semigroup> Arrange<G, Vec<((K, ()), G::Timestamp, R)>> for Collection<G, K, R>
where
    G: Scope<Timestamp: Lattice+Ord>,
{
    fn arrange_named<Ba, Bu, Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        Ba: Batcher<Input=Vec<((K,()),G::Timestamp,R)>, Time=G::Timestamp> + 'static,
        Bu: Builder<Time=G::Timestamp, Input=Ba::Output, Output = Tr::Batch>,
        Tr: Trace<Time=G::Timestamp> + 'static,
    {
        let exchange = Exchange::new(move |update: &((K,()),G::Timestamp,R)| (update.0).0.hashed().into());
        arrange_core::<_,_,Ba,Bu,_>(&self.map(|k| (k, ())).inner, exchange, name)
    }
}

/// Arranges something as `(Key,Val)` pairs according to a type `T` of trace.
///
/// This arrangement requires `Key: Hashable`, and uses the `hashed()` method to place keys in a hashed
/// map. This can result in many hash calls, and in some cases it may help to first transform `K` to the
/// pair `(u64, K)` of hash value and key.
pub trait ArrangeByKey<G: Scope, K: Data+Hashable, V: Data, R: Ord+Semigroup+'static>
where
    G: Scope<Timestamp: Lattice+Ord>,
{
    /// Arranges a collection of `(Key, Val)` records by `Key`.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times completed by the output stream, which can be used to
    /// safely identify the stable times and values in the trace.
    fn arrange_by_key(&self) -> Arranged<G, TraceAgent<ValSpine<K, V, G::Timestamp, R>>>;

    /// As `arrange_by_key` but with the ability to name the arrangement.
    fn arrange_by_key_named(&self, name: &str) -> Arranged<G, TraceAgent<ValSpine<K, V, G::Timestamp, R>>>;
}

impl<G, K: ExchangeData+Hashable, V: ExchangeData, R: ExchangeData+Semigroup> ArrangeByKey<G, K, V, R> for Collection<G, (K,V), R>
where
    G: Scope<Timestamp: Lattice+Ord>,
{
    fn arrange_by_key(&self) -> Arranged<G, TraceAgent<ValSpine<K, V, G::Timestamp, R>>> {
        self.arrange_by_key_named("ArrangeByKey")
    }

    fn arrange_by_key_named(&self, name: &str) -> Arranged<G, TraceAgent<ValSpine<K, V, G::Timestamp, R>>> {
        self.arrange_named::<ValBatcher<_,_,_,_>,ValBuilder<_,_,_,_>,_>(name)
    }
}

/// Arranges something as `(Key, ())` pairs according to a type `T` of trace.
///
/// This arrangement requires `Key: Hashable`, and uses the `hashed()` method to place keys in a hashed
/// map. This can result in many hash calls, and in some cases it may help to first transform `K` to the
/// pair `(u64, K)` of hash value and key.
pub trait ArrangeBySelf<G, K: Data+Hashable, R: Ord+Semigroup+'static>
where
    G: Scope<Timestamp: Lattice+Ord>,
{
    /// Arranges a collection of `Key` records by `Key`.
    ///
    /// This operator arranges a collection of records into a shared trace, whose contents it maintains.
    /// This trace is current for all times complete in the output stream, which can be used to safely
    /// identify the stable times and values in the trace.
    fn arrange_by_self(&self) -> Arranged<G, TraceAgent<KeySpine<K, G::Timestamp, R>>>;

    /// As `arrange_by_self` but with the ability to name the arrangement.
    fn arrange_by_self_named(&self, name: &str) -> Arranged<G, TraceAgent<KeySpine<K, G::Timestamp, R>>>;
}


impl<G, K: ExchangeData+Hashable, R: ExchangeData+Semigroup> ArrangeBySelf<G, K, R> for Collection<G, K, R>
where
    G: Scope<Timestamp: Lattice+Ord>,
{
    fn arrange_by_self(&self) -> Arranged<G, TraceAgent<KeySpine<K, G::Timestamp, R>>> {
        self.arrange_by_self_named("ArrangeBySelf")
    }

    fn arrange_by_self_named(&self, name: &str) -> Arranged<G, TraceAgent<KeySpine<K, G::Timestamp, R>>> {
        self.map(|k| (k, ()))
            .arrange_named::<KeyBatcher<_,_,_>,KeyBuilder<_,_,_>,_>(name)
    }
}
