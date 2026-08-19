/// Streaming asymmetric join between updates (K,V1) and an arrangement on (K,V2).
///
/// The asymmetry is that the join only responds to streamed updates, not to changes in the arrangement.
/// Streamed updates join only with matching arranged updates at lesser times *in the total order*,
/// where a `strict` flag says whether an equal time counts as lesser.
///
/// This behavior can ensure that any pair of matching updates interact exactly once.
///
/// There are various forms of this operator with tangled closures about how to emit the outputs and
/// wrangle the logical compaction frontier in order to preserve the distinctions around times that are
/// strictly less (conventional compaction logic would collapse unequal times to the frontier, and lose
/// the distiction).
///
/// The methods also carry an auxiliary time next to the value, which is used to advance the joined times.
/// This is .. a byproduct of wanting to allow advancing times a la `join_function`, without breaking the
/// coupling by total order on "initial time".
///
/// The doccomments for individual methods are a bit of a mess. Sorry.

use std::collections::VecDeque;
use std::ops::Mul;

use timely::{Container, ContainerBuilder};
use timely::container::{CapacityContainerBuilder, DrainContainer, NoopBuilder, PushInto, SizableContainer};
use timely::dataflow::Stream;
use timely::dataflow::channels::ContainerBytes;
use timely::dataflow::channels::pact::{Pipeline, ExchangeCore};
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::generic::OutputBuilderSession;
use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::MutableAntichain;

use differential_dataflow::{ExchangeData, VecCollection, AsCollection, Hashable};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchReader, BatchCursor, BatchDiff, BatchVal, Cursor, Navigable, TraceReader};
use differential_dataflow::trace::cursor::cursor_list;
use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::trace::implementations::BatchContainer;

use timely::dataflow::operators::CapabilitySet;

/// A binary equijoin that responds to updates on only its first input.
///
/// This operator responds to inputs of the form
///
/// ```ignore
/// ((key, val1, time1), initial_time, diff1)
/// ```
///
/// where `initial_time` is less or equal to `time1`, and produces as output
///
/// ```ignore
/// ((output_func(key, val1, val2), lub(time1, time2)), initial_time, diff1 * diff2)
/// ```
///
/// for each `((key, val2), time2, diff2)` present in `arrangement`, where
/// `time2` is less than `initial_time` *UNDER THE TOTAL ORDER ON TIMES*.
/// This last constraint is important to ensure that we correctly produce
/// all pairs of output updates across multiple `half_join` operators.
///
/// The `strict` argument selects the comparison: `true` requires `time2` to be strictly less than
/// `initial_time`, and `false` also admits `time2` equal to it. A delta query pairs a strict operator
/// with a non-strict one, which is what makes each pair of matching updates interact exactly once.
///
/// Notice that the time is hoisted up into data. The expectation is that
/// once out of the "delta flow region", the updates will be `delay`d to the
/// times specified in the payloads.
pub fn half_join<'scope, K, V, R, Tr, FF, DOut, S>(
    stream: VecCollection<'scope, Tr::Time, (K, V, Tr::Time), R>,
    arrangement: Arranged<'scope, Tr>,
    frontier_func: FF,
    strict: bool,
    mut output_func: S,
) -> VecCollection<'scope, Tr::Time, (DOut, Tr::Time), <R as Mul<BatchDiff<Tr>>>::Output>
where
    K: Hashable + ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Semigroup,
    Tr: TraceReader<Batch: Navigable>+Clone+'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned=K>,
    R: Mul<BatchDiff<Tr>, Output: Semigroup>,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    DOut: Clone+'static,
    S: FnMut(&K, &V, BatchVal<'_, Tr>)->DOut+'static,
{
    let output_func = move |builder: &mut CapacityContainerBuilder<Vec<_>>, k: &K, v1: &V, v2: BatchVal<'_, Tr>, initial: &Tr::Time, diff1: &R, output: &mut Vec<(Tr::Time, BatchDiff<Tr>)>| {
        for (time, diff2) in output.drain(..) {
            let diff = diff1.clone() * diff2.clone();
            let dout = (output_func(k, v1, v2), time.clone());
            use timely::container::PushInto;
            builder.push_into((dout, initial.clone(), diff));
        }
    };
    half_join_internal_unsafe::<_, _, _, _, _, _, _, CapacityContainerBuilder<Vec<_>>>(stream, arrangement, frontier_func, strict, |_timer, _count| false, output_func)
        .as_collection()
}

/// An unsafe variant of `half_join` where the `output_func` closure takes
/// additional arguments a vector of `time` and `diff` tuples as input and
/// writes its outputs at a container builder. The container builder
/// can, but isn't required to, accept `(data, time, diff)` triplets.
/// This allows for more flexibility, but is more error-prone.
///
/// This operator responds to inputs of the form
///
/// ```ignore
/// ((key, val1, time1), initial_time, diff1)
/// ```
///
/// where `initial_time` is less or equal to `time1`, and produces as output
///
/// ```ignore
/// output_func(session, key, val1, val2, initial_time, diff1, &[lub(time1, time2), diff2])
/// ```
///
/// for each `((key, val2), time2, diff2)` present in `arrangement`, where
/// `time2` is less than `initial_time` *UNDER THE TOTAL ORDER ON TIMES*.
///
/// The `strict` argument selects the comparison: `true` requires `time2` to be strictly less than
/// `initial_time`, and `false` also admits `time2` equal to it. A delta query pairs a strict operator
/// with a non-strict one, which is what makes each pair of matching updates interact exactly once.
///
/// The `yield_function` allows the caller to indicate when the operator should
/// yield control, as a function of the elapsed time and the number of records in
/// the output containers shipped so far. Joined work is suspended at container
/// boundaries, so the count advances a container at a time rather than a record
/// at a time.
pub fn half_join_internal_unsafe<'scope, K, V, R, Tr, FF, Y, S, CB>(
    stream: VecCollection<'scope, Tr::Time, (K, V, Tr::Time), R>,
    arrangement: Arranged<'scope, Tr>,
    frontier_func: FF,
    strict: bool,
    yield_function: Y,
    output_func: S,
) -> Stream<'scope, Tr::Time, CB::Container>
where
    K: Hashable + ExchangeData,
    V: ExchangeData,
    R: ExchangeData + Semigroup,
    Tr: TraceReader<Batch: Navigable>+Clone+'static,
    BatchCursor<Tr>: Cursor<Time = Tr::Time>,
    <BatchCursor<Tr> as Cursor>::KeyContainer: BatchContainer<Owned=K>,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    Y: Fn(std::time::Instant, usize) -> bool + 'static,
    S: FnMut(&mut CB, &K, &V, BatchVal<'_, Tr>, &Tr::Time, &R, &mut Vec<(Tr::Time, BatchDiff<Tr>)>) + 'static,
    CB: ContainerBuilder,
{
    // Updates are staged in a `BlobList` and joined by a cursor walk; the driver owns progress.
    let batcher = BlobList::<(K, V, Tr::Time), Tr::Time, R>::default();
    let tactic = cursors::CursorTactic::<K, V, R, Tr::Batch, S, CB>::new(output_func, strict);
    let route = |update: &((K, V, Tr::Time), Tr::Time, R)| (update.0).0.hashed().into();
    half_join_with_tactic(stream.inner, arrangement, route, frontier_func, strict, yield_function, batcher, tactic)
}

pub trait HalfJoinTactic<B: BatchReader, C0, C1> {
    /// Converts a list of chunks and a list of batches to a list of outputs, each with a time suitable as a capability.
    ///
    /// The `lower` argument lower bounds the times in `chunks`, and may be used to load `batches`
    /// compacted, or to bound the arrangement times the join must consider.
    fn prep(&mut self, chunks: Vec<C0>, batches: Vec<B>, lower: Antichain<B::Time>) -> Box<dyn Iterator<Item = (C1, B::Time)>>;
}

/// A type capable of accepting containers of updates, and carving them out by time.
///
/// The implementor is able to determine the meaning of extraction by a frontier;
/// it is not required to be by antichain partial order.
pub trait BatcherAlt<C, T: Timestamp> {
    /// Moves responsibility for `container` into the implementor.
    fn insert(&mut self, container: C);
    /// Extracts containers on the basis of `bound`, and the lower bounds of the times retained.
    ///
    /// The returned bound is reported at the one moment it is well defined: an implementor is not
    /// required to account for inserted updates until it is asked to extract, so this is the only
    /// bound a caller can safely downgrade capabilities to.
    fn extract(&mut self, bound: Eligibility<T>) -> (Vec<C>, &MutableAntichain<T>);
}

pub enum Eligibility<T> {
    All,
    Le(T),
    Lt(T),
}

struct BlobList<D, T: Timestamp, R> {
    stage: Vec<(T, D, R)>,
    blobs: Vec<VecDeque<(T, D, R)>>,
    lower: MutableAntichain<T>,
}

impl<D, T: Timestamp, R> Default for BlobList<D, T, R> {
    fn default() -> Self {
        BlobList { stage: Vec::new(), blobs: Vec::new(), lower: MutableAntichain::new() }
    }
}

impl<D: Ord, T: Timestamp, R: Semigroup> BatcherAlt<Vec<(D, T, R)>, T> for BlobList<D, T, R> {
    fn insert(&mut self, container: Vec<(D, T, R)>) {
        self.stage.extend(container.into_iter().map(|(d,t,r)| (t,d,r)));
    }
    fn extract(&mut self, bound: Eligibility<T>) -> (Vec<Vec<(D, T, R)>>, &MutableAntichain<T>) {
        // Handle any staged updates first.
        consolidate_updates(&mut self.stage);
        if !self.stage.is_empty() {
            let blob: VecDeque<_> = std::mem::take(&mut self.stage).into();
            self.lower.update_iter(blob.iter().map(|x| (x.0.clone(), 1)));
            self.blobs.push(blob);
        }

        let mut result = Vec::new();
        for blob in self.blobs.iter_mut() {
            let mut list = Vec::new();
            match &bound {
                Eligibility::Le(bound) => {
                    while blob.front().map(|(t,_,_)| t.le(bound)).unwrap_or(false) {
                        let (time, data, diff) = blob.pop_front().unwrap();
                        list.push((data, time, diff));
                    }
                }
                Eligibility::Lt(bound) => {
                    while blob.front().map(|(t,_,_)| t.lt(bound)).unwrap_or(false) {
                        let (time, data, diff) = blob.pop_front().unwrap();
                        list.push((data, time, diff));
                    }
                }
                Eligibility::All => {
                    list.extend(blob.drain(..).map(|(t,d,r)| (d,t,r)));
                }
            }
            if !list.is_empty() { result.push(list); }
        }

        self.blobs.retain(|b| !b.is_empty());

        self.lower.update_iter(result.iter().flat_map(|l| l.iter().map(|x| (x.1.clone(), -1))));
        (result, &self.lower)
    }
}

/// A `half_join` driven by a [`BatcherAlt`] and a [`HalfJoinTactic`].
///
/// This is the analogue of [`half_join_internal_unsafe`], with the join itself lifted out. The
/// driver owns progress and nothing else: it stages arriving updates in `batcher`, releases those
/// the arrangement frontier has unblocked, and hands each release to `tactic` as a unit of deferred
/// work paired with the capabilities required to ship its output. It holds no cursor, and has no
/// opinion on how the join is performed.
///
/// The `strict` argument plays the role of the `comparison` closure: an update is unblocked once
/// no arrangement time can still precede its initial time under the total order, and `strict` says
/// whether an equal time counts as preceding. It reaches the join itself through `tactic`, which
/// must be built with the same value.
pub fn half_join_with_tactic<'scope, Tr, FF, RF, Y, Bat, Tac, CIn, C>(
    stream: Stream<'scope, Tr::Time, CIn>,
    mut arrangement: Arranged<'scope, Tr>,
    route: RF,
    frontier_func: FF,
    strict: bool,
    yield_function: Y,
    mut batcher: Bat,
    mut tactic: Tac,
) -> Stream<'scope, Tr::Time, C>
where
    Tr: TraceReader + Clone + 'static,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    for<'a> RF: FnMut(&CIn::Item<'a>) -> u64 + 'static,
    Y: Fn(std::time::Instant, usize) -> bool + 'static,
    Bat: BatcherAlt<CIn, Tr::Time> + 'static,
    Tac: HalfJoinTactic<Tr::Batch, CIn, C> + 'static,
    CIn: Container + SizableContainer + DrainContainer + ContainerBytes + Send,
    for<'a> CIn: PushInto<<CIn as DrainContainer>::Item<'a>>,
    C: Container + 'static,
{
    // No need to block physical merging for this operator.
    arrangement.trace.set_physical_compaction(Antichain::new().borrow());
    let mut arrangement_trace = Some(arrangement.trace);
    let arrangement_stream = arrangement.stream;

    let exchange = ExchangeCore::<CapacityContainerBuilder<CIn>, _>::new(route);

    let scope = stream.scope();
    stream.binary_frontier(arrangement_stream, exchange, Pipeline, "HalfJoin", move |_, info| {

        // Acquire an activator to reschedule the operator when it has unfinished work.
        let activator = scope.activator_for(info.address);

        // Capabilities covering everything `batcher` retains. Downgraded after each extraction to
        // the bound it reports, and cloned into each unit of deferred work before that.
        let mut caps = CapabilitySet::new();

        // Deferred work, as `(capabilities, iterator)` pairs. Each iterator, prepared by the
        // tactic, yields output containers and the time at which each may be shipped; the paired
        // capability set covers those times. An iterator is dropped once it goes dry.
        let mut todo: VecDeque<(CapabilitySet<Tr::Time>, Box<dyn Iterator<Item = (C, Tr::Time)>>)> = VecDeque::new();

        move |(input1, frontier1), (input2, frontier2), output| {

            // The driver only ships finished containers (`give_container`), never pushing records,
            // so it pins the operator output to `NoopBuilder<C>`.
            let output: &mut OutputBuilderSession<'_, Tr::Time, NoopBuilder<C>> = output;

            // Stage all arriving updates, retaining capabilities that cover them.
            input1.for_each(|capability, data| {
                caps.insert(capability.retain(0));
                batcher.insert(std::mem::take(data));
            });

            // Drain input batches; although we do not observe them, we want access to the input
            // to observe the frontier and to drive scheduling.
            input2.for_each(|_, _| { });

            if let Some(ref mut trace) = arrangement_trace {

                // An update is unblocked once no arrangement time can still precede its initial
                // time. The arrangement frontier's total-order minimum bounds those times, and
                // `strict` says whether an equal time still counts as preceding.
                let eligibility = match frontier2.frontier().iter().min() {
                    None => Eligibility::All,
                    Some(bound) if strict => Eligibility::Le(bound.clone()),
                    Some(bound) => Eligibility::Lt(bound.clone()),
                };

                // `caps` covers everything `batcher` holds: capabilities retained for arriving
                // updates, downgraded after each extraction to what was left behind. It therefore
                // lower bounds whatever the extraction below releases, and can both mint the
                // capabilities that work will ship under and bound the times it will contain.
                let lower: Antichain<Tr::Time> = caps.iter().map(|c| c.time().clone()).collect();

                let (chunks, retained) = batcher.extract(eligibility);
                if !chunks.is_empty() {
                    // The batches are handed to the tactic, which holds them for as long as the
                    // work item lives; what it joins against cannot change underneath it.
                    let batches = trace.batches_through(Antichain::new().borrow()).unwrap();
                    let work = tactic.prep(chunks, batches, lower);
                    todo.push_back((caps.clone(), work));
                }

                // Release capabilities for everything `batcher` no longer holds. The bound comes
                // from the extraction itself, as an implementor need not account for inserted
                // updates until it is asked to extract, and a bound read before that could discard
                // a capability the extraction then needs.
                caps.downgrade(retained.frontier().iter());
            }

            // Perform some amount of outstanding work, shipping each container at a capability
            // covering the time the tactic reports for it. Work is measured in output records,
            // matching `half_join_internal_unsafe`, whose counter is also read after the join's
            // time filter and consolidation.
            let timer = std::time::Instant::now();
            let mut work = 0;
            while !yield_function(timer, work) {
                let Some((caps, iter)) = todo.front_mut() else { break };
                match iter.next() {
                    Some((mut container, time)) => {
                        work += container.record_count() as usize;
                        let cap = caps.iter().find(|c| c.time().less_equal(&time)).expect("no capability covers a produced container");
                        output.session_with_builder(cap).give_container(&mut container);
                    }
                    None => { todo.pop_front(); }
                }
            }
            if !todo.is_empty() { activator.activate(); }

            // The logical merging frontier depends on input1, on staged updates, and on the
            // deferred work that has already been released to the tactic.
            let mut frontier = Antichain::new();
            for time in frontier1.frontier().iter() {
                frontier_func(time, &mut frontier);
            }
            for cap in caps.iter() {
                frontier_func(cap.time(), &mut frontier);
            }
            for (caps, _) in todo.iter() {
                for cap in caps.iter() {
                    frontier_func(cap.time(), &mut frontier);
                }
            }
            arrangement_trace.as_mut().map(|trace| trace.set_logical_compaction(frontier.borrow()));

            if frontier1.is_empty() && caps.is_empty() && todo.is_empty() {
                arrangement_trace = None;
            }
        }
    })
}

/// Cursor-based half join: the conventional [`HalfJoinTactic`] implementation and its worker.
mod cursors {

    use std::cell::RefCell;
    use std::rc::Rc;

    use super::*;

    /// The cursor of a batch.
    type BCursor<B> = <B as Navigable>::Cursor;

    /// The conventional cursor-based [`HalfJoinTactic`].
    ///
    /// It builds a cursor over the batches it is handed and walks the released updates against it,
    /// at whatever rate the driver allows. `logic` is shared across all outstanding units (an
    /// `Rc<RefCell<_>>`), preserving the single mutable-state semantics of one closure threaded
    /// through every match: each unit is a self-contained `'static` iterator, so it cannot borrow
    /// the tactic.
    pub struct CursorTactic<K, V, R, B, L, CB> {
        logic: Rc<RefCell<L>>,
        /// Whether an arrangement time equal to an update's initial time is excluded.
        strict: bool,
        _marker: std::marker::PhantomData<(K, V, R, B, CB)>,
    }

    impl<K, V, R, B, L, CB> CursorTactic<K, V, R, B, L, CB> {
        /// Construct a tactic that applies `logic` to each matched `(key, val1, val2)`.
        pub fn new(logic: L, strict: bool) -> Self {
            CursorTactic { logic: Rc::new(RefCell::new(logic)), strict, _marker: std::marker::PhantomData }
        }
    }

    impl<K, V, R, B, L, CB> HalfJoinTactic<B, Vec<((K, V, B::Time), B::Time, R)>, CB::Container> for CursorTactic<K, V, R, B, L, CB>
    where
        B: BatchReader + Navigable + 'static,
        BCursor<B>: Cursor<Time = B::Time>,
        <BCursor<B> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
        K: Ord + 'static,
        V: Ord + 'static,
        R: 'static,
        L: for<'a> FnMut(&mut CB, &K, &V, <BCursor<B> as Cursor>::Val<'a>, &B::Time, &R, &mut Vec<(B::Time, <BCursor<B> as Cursor>::Diff)>) + 'static,
        CB: ContainerBuilder,
    {
        fn prep(&mut self, chunks: Vec<Vec<((K, V, B::Time), B::Time, R)>>, batches: Vec<B>, lower: Antichain<B::Time>) -> Box<dyn Iterator<Item = (CB::Container, B::Time)>> {
            // The batcher releases updates in time order, but the cursor is walked in data order.
            let mut updates: Vec<_> = chunks.into_iter().flatten().collect();
            updates.sort_by(|(d1, t1, _), (d2, t2, _)| (d1, t1).cmp(&(d2, t2)));

            let (cursor, storage) = cursor_list(batches);
            let lower = lower.elements().to_vec();
            let builders = (0 .. lower.len()).map(|_| CB::default()).collect::<Vec<_>>();

            Box::new(DeferredIter {
                updates,
                index: 0,
                cursor,
                storage,
                key_con: BatchContainer::with_capacity(1),
                time_con: BatchContainer::with_capacity(1),
                lower,
                builders,
                ready: VecDeque::new(),
                output_buffer: Vec::new(),
                logic: Rc::clone(&self.logic),
                strict: self.strict,
                done: false,
            })
        }
    }

    /// Deferred half-join computation, as an iterator of output containers and the times they ship at.
    ///
    /// Each `next` walks the released updates forward until a builder yields a container, or the
    /// updates run dry. The driver stops pulling once it has done enough work, and resumes the same
    /// iterator on the next activation.
    struct DeferredIter<K, V, R, C: Cursor, L, CB: ContainerBuilder> {
        /// Released updates, sorted by data for cursor traversal.
        updates: Vec<((K, V, C::Time), C::Time, R)>,
        /// How far through `updates` we have walked.
        index: usize,
        cursor: C,
        storage: C::Storage,
        key_con: C::KeyContainer,
        time_con: C::TimeContainer,
        /// The times output ships at, one per builder. An update is assigned the first that is
        /// less or equal to its initial time, mirroring the capability the driver will find.
        lower: Vec<C::Time>,
        builders: Vec<CB>,
        /// Completed containers awaiting a `next` call.
        ready: VecDeque<(CB::Container, C::Time)>,
        /// Stash for (time, diff) accumulation.
        output_buffer: Vec<(C::Time, C::Diff)>,
        /// The output closure, shared across all outstanding units.
        logic: Rc<RefCell<L>>,
        /// Whether an arrangement time equal to an update's initial time is excluded.
        strict: bool,
        done: bool,
    }

    impl<K, V, R, C, L, CB> Iterator for DeferredIter<K, V, R, C, L, CB>
    where
        C: Cursor,
        C::KeyContainer: BatchContainer<Owned = K>,
        CB: ContainerBuilder,
        L: for<'a> FnMut(&mut CB, &K, &V, C::Val<'a>, &C::Time, &R, &mut Vec<(C::Time, C::Diff)>),
    {
        type Item = (CB::Container, C::Time);

        fn next(&mut self) -> Option<Self::Item> {

            // Serve any container completed on an earlier call first.
            if let Some(item) = self.ready.pop_front() { return Some(item); }
            if self.done { return None; }

            {
                let updates = &self.updates;
                let cursor = &mut self.cursor;
                let storage = &self.storage;
                let key_con = &mut self.key_con;
                let time_con = &mut self.time_con;
                let lower = &self.lower;
                let builders = &mut self.builders;
                let ready = &mut self.ready;
                let output_buffer = &mut self.output_buffer;
                let strict = self.strict;
                let mut logic = self.logic.borrow_mut();
                let logic = &mut *logic;

                while ready.is_empty() && self.index < updates.len() {

                    let ((key, val1, time), initial, diff1) = &updates[self.index];
                    self.index += 1;

                    // Each capability has its own builder, so that output shipped at one is not
                    // mixed with output that requires another.
                    let builder_idx = lower.iter().position(|t| t.less_equal(initial)).expect("no capability covers a released update");

                    key_con.clear();
                    key_con.push_own(key);
                    cursor.seek_key(storage, key_con.index(0));
                    if cursor.get_key(storage) == key_con.get(0) {
                        time_con.clear();
                        time_con.push_own(initial);
                        let bound = time_con.index(0);
                        while let Some(val2) = cursor.get_val(storage) {
                            cursor.map_times(storage, |t, d| {
                                // `bound` and `t` are read from different containers, and their
                                // borrowed times only compare once narrowed to a common lifetime.
                                let bound = <C::TimeContainer as BatchContainer>::reborrow(bound);
                                let t = <C::TimeContainer as BatchContainer>::reborrow(t);
                                if if strict { t < bound } else { t <= bound } {
                                    let mut t = C::owned_time(t);
                                    t.join_assign(time);
                                    output_buffer.push((t, C::owned_diff(d)))
                                }
                            });
                            consolidate(output_buffer);
                            logic(&mut builders[builder_idx], key, val1, val2, initial, diff1, output_buffer);
                            output_buffer.clear();
                            cursor.step_val(storage);
                        }
                        cursor.rewind_vals(storage);
                    }

                    while let Some(container) = builders[builder_idx].extract() {
                        ready.push_back((std::mem::take(container), lower[builder_idx].clone()));
                    }
                }
            }

            // Flush the final partial containers once the updates are exhausted.
            if self.index == self.updates.len() {
                self.done = true;
                for builder_idx in 0 .. self.builders.len() {
                    while let Some(container) = self.builders[builder_idx].finish() {
                        self.ready.push_back((std::mem::take(container), self.lower[builder_idx].clone()));
                    }
                }
            }

            self.ready.pop_front()
        }
    }
}
