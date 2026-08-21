/// Streaming asymmetric join between an update stream and a maintained arrangement.
///
/// The asymmetry is that the join only responds to streamed updates, not to changes in the arrangement.
/// The operator is the basis of several multi-way join implementations, which use networks of stateless
/// operators rather than sequences of stateful operators.
///
/// The standard recipe for a multi-way join among collections A, B, .. Z is to create a dataflow path for
/// each collection, which responds to changes in the collection. Each path uses a sequence of half join
/// operators to prompt the response to each streamed input change when joined with "prior" updates from
/// the other collections. A total order on update times, often just the `Ord` implementation, can then be
/// extended to all relations to impose a total order on updates to any of the collections, which can then
/// ensure that each tuple of updates is processed exactly once (usually at the time of the "last" update).
///
/// The operator design is foremost as a "tactic", which allows an implementor to supply the understanding
/// of the streamed update containers and the maintained trace batches, without complicating the operator.
/// The implementation requires an explanation of how to mark updates as "eligible", via the `Batcher` trait,
/// and then what to do with the eligible updates and existing batches, through the `HalfJoinTactic` trait.

use std::collections::VecDeque;
use std::ops::Mul;

use timely::{Container, ContainerBuilder};
use timely::container::{CapacityContainerBuilder, NoopBuilder};
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::generic::OutputBuilderSession;
use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};
use timely::progress::frontier::{AntichainRef, MutableAntichain};

use differential_dataflow::{ExchangeData, VecCollection, AsCollection, Hashable};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchCursor, BatchDiff, BatchVal, Cursor, Navigable, TraceReader};
use differential_dataflow::trace::cursor::cursor_list;
use differential_dataflow::consolidation::{consolidate, consolidate_updates};
use differential_dataflow::trace::implementations::BatchContainer;

use timely::dataflow::operators::CapabilitySet;

/// An implementation suitable to define half-join behavior among its referenced types.
///
/// The implementor can half-join streams of `C0` with batches `B`, producing output streams of `C1`.
pub trait HalfJoinTactic<T, B, C0, C1> {
    /// Converts a list of chunks and a list of batch payloads to a list of outputs, each with a time suitable as a capability.
    ///
    /// The `lower` argument lower bounds the times in `chunks`, and may be used to load `batches`
    /// compacted, or to bound the arrangement times the join must consider.
    fn prep(&mut self, chunks: Vec<C0>, batches: Vec<B>, lower: Antichain<T>) -> Box<dyn Iterator<Item = (C1, T)>>;
}

/// A type capable of accepting containers of updates, and carving them out by time.
///
/// The implementor is able to determine the meaning of extraction by a frontier;
/// it is not required to be by antichain partial order.
///
/// Updates are accepted as `C0`, the containers that arrive on the dataflow edge, and released as
/// `C1`, whatever a [`HalfJoinTactic`] would rather consume. The two need not agree: an implementor
/// staging updates in a form of its own can release that form directly.
pub trait Batcher<T: Timestamp, C0, C1> {
    /// Moves responsibility for `container` into the implementor.
    fn insert(&mut self, container: C0);
    /// Extracts updates `frontier` unblocks, and lower bounds the time of retained updates.
    ///
    /// What `frontier` unblocks is the implementor's to decide. It can be based on the antichain up set,
    /// or it can be based on the total order of times (as used in delta join constructions).
    ///
    /// The reported lower bound antichain should accurately reflect the times of all accepted updates
    /// that have not been extracted. Over approximation can result in stalling dataflows, and under
    /// approximation is simply incorrect.
    fn extract(&mut self, frontier: AntichainRef<'_, T>) -> (Vec<C1>, &MutableAntichain<T>);
}

/// A `half_join` driven by a [`Batcher`] and a [`HalfJoinTactic`].
///
/// The operator introduces all streamed updates to the `batcher`, and then extracts all updates
/// unlocked by the trace frontier. The `tactic` converts unlocked updates into output containers,
/// which are produced lazily as the operator's yield logic permits.
///
/// The driver holds no opinion on when an update is unblocked: it hands `batcher` the arrangement
/// frontier and ships whatever `batcher` releases to `tactic`. The pair of `batcher` and `tactic`
/// should agree with each other for meaningful outcomes.
///
/// The `pact` distributes the streamed updates, which should land each at the appropriate worker.
/// A likely implementation is an exchange by a hash of a common key.
pub fn half_join_with_tactic<'scope, Tr, FF, P, Y, Bat, Tac, CIn, CMid, C>(
    stream: Stream<'scope, Tr::Time, CIn>,
    mut arrangement: Arranged<'scope, Tr>,
    pact: P,
    frontier_func: FF,
    yield_function: Y,
    mut batcher: Bat,
    mut tactic: Tac,
) -> Stream<'scope, Tr::Time, C>
where
    Tr: TraceReader + Clone + 'static,
    FF: Fn(&Tr::Time, &mut Antichain<Tr::Time>) + 'static,
    P: ParallelizationContract<Tr::Time, CIn>,
    Y: Fn(std::time::Instant, usize) -> bool + 'static,
    Bat: Batcher<Tr::Time, CIn, CMid> + 'static,
    Tac: HalfJoinTactic<Tr::Time, Tr::Batch, CMid, C> + 'static,
    CIn: Container,
    C: Container + 'static,
{
    // No need to block physical merging for this operator.
    arrangement.trace.set_physical_compaction(Antichain::new().borrow());
    let mut arrangement_trace = Some(arrangement.trace);
    let arrangement_stream = arrangement.stream;

    let scope = stream.scope();
    stream.binary_frontier(arrangement_stream, pact, Pipeline, "HalfJoin", move |_, info| {

        // Acquire an activator to reschedule the operator when it has unfinished work.
        let activator = scope.activator_for(info.address);

        // Capabilities covering everything `batcher` retains. Downgraded after each extraction to
        // the bound it reports, and cloned into each unit of deferred work before that.
        let mut caps = CapabilitySet::new();

        // Deferred work, as `(capabilities, iterator)` pairs. Each iterator, prepared by the
        // tactic, yields output containers and the time at which each may be shipped; the paired
        // capability set covers those times.
        let mut todo: VecDeque<(CapabilitySet<Tr::Time>, Box<dyn Iterator<Item = (C, Tr::Time)>>)> = VecDeque::new();

        move |(input1, frontier1), (input2, frontier2), output| {

            // The driver ships only finished containers, so it pins the operator output to `NoopBuilder<C>`.
            let output: &mut OutputBuilderSession<'_, Tr::Time, NoopBuilder<C>> = output;

            // Stage all arriving updates, retaining capabilities that cover them.
            // TODO: Tolerate multi-capability inputs.
            input1.for_each(|capability, data| {
                caps.insert(capability.retain(0));
                batcher.insert(std::mem::take(data));
            });

            // Drain input batches. We do not capture the batches, but we do use the frontier.
            input2.for_each(|_, _| { });

            if let Some(ref mut trace) = arrangement_trace {

                // Look for updates that are newly eligible against the current frontier.
                let (chunks, retained) = batcher.extract(frontier2.frontier());
                if !chunks.is_empty() {
                    // The batches are handed to the tactic, which holds them for as long as the
                    // work item lives; what it joins against cannot change underneath it.
                    let batches = trace.batches_through(Antichain::new().borrow()).unwrap();
                    let lower: Antichain<Tr::Time> = caps.iter().map(|c| c.time().clone()).collect();
                    let work = tactic.prep(chunks, batches, lower);
                    todo.push_back((caps.clone(), work));
                }

                // Downgrade capabilities to those held by `batcher`.
                caps.downgrade(retained.frontier().iter());
            }

            // Perform some amount of outstanding work, shipping each container at a capability
            // covering the time the tactic reports for it. Work is measured in output records.
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
            for time in frontier1.frontier().iter() { frontier_func(time, &mut frontier); }
            for cap in caps.iter() { frontier_func(cap.time(), &mut frontier); }
            for (caps, _) in todo.iter() { for cap in caps.iter() { frontier_func(cap.time(), &mut frontier); } }
            arrangement_trace.as_mut().map(|trace| trace.set_logical_compaction(frontier.borrow()));

            // If no updates incoming, no updates held in `batcher`, and no work in `todo`, we can drop the trace.
            if frontier1.is_empty() && caps.is_empty() && todo.is_empty() { arrangement_trace = None; }
        }
    })
}

/// The default yield policy: return control after a millisecond of work.
///
/// Without one, a single activation drains every outstanding work item, so one large epoch
/// monopolizes the worker and nothing downstream of the operator runs until it is done. This
/// does not change what the operator produces — [`half_join_with_tactic`] reschedules itself
/// while `todo` is non-empty — only when it produces it.
///
/// The `work > 0` clause is load-bearing rather than a tie-break. The driver tests this
/// *before* pulling from a work item, so a policy that can fire at zero work would yield, be
/// rescheduled, yield again, and never advance.
pub fn yield_after_a_millisecond(start: std::time::Instant, work: usize) -> bool {
    work > 0 && start.elapsed() > std::time::Duration::from_millis(1)
}

/// Cursor-based half join: the conventional [`HalfJoinTactic`] implementation, its worker, and the
/// `half_join` entry points built on them.
pub mod cursors {

    use std::cell::RefCell;
    use std::rc::Rc;

    use timely::dataflow::channels::pact::Exchange;

    use super::*;

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
        half_join_internal_unsafe::<_, _, _, _, _, _, _, CapacityContainerBuilder<Vec<_>>>(stream, arrangement, frontier_func, strict, super::yield_after_a_millisecond, output_func)
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
        let batcher = BlobList::<(K, V, Tr::Time), Tr::Time, R>::new(strict);
        let tactic = CursorTactic::<K, V, R, Tr::Batch, S, CB>::new(output_func, strict);
        // Updates are routed by key hash, matching how `arrangement` itself is distributed.
        let route = |update: &((K, V, Tr::Time), Tr::Time, R)| (update.0).0.hashed().into();
        let pact = Exchange::new(route);
        half_join_with_tactic(stream.inner, arrangement, pact, frontier_func, yield_function, batcher, tactic)
    }

    /// The cursor of a batch payload.
    type BCursor<B> = <B as Navigable>::Cursor;
    /// The time type of a batch payload's cursor.
    type BTime<B> = <<B as Navigable>::Cursor as Cursor>::Time;

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

    impl<K, V, R, B, L, CB> HalfJoinTactic<BTime<B>, B, Vec<((K, V, BTime<B>), BTime<B>, R)>, CB::Container> for CursorTactic<K, V, R, B, L, CB>
    where
        B: Navigable + 'static,
        <BCursor<B> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
        K: Ord + 'static,
        V: Ord + 'static,
        R: 'static,
        L: for<'a> FnMut(&mut CB, &K, &V, <BCursor<B> as Cursor>::Val<'a>, &BTime<B>, &R, &mut Vec<(BTime<B>, <BCursor<B> as Cursor>::Diff)>) + 'static,
        CB: ContainerBuilder,
    {
        fn prep(&mut self, chunks: Vec<Vec<((K, V, BTime<B>), BTime<B>, R)>>, batches: Vec<B>, lower: Antichain<BTime<B>>) -> Box<dyn Iterator<Item = (CB::Container, BTime<B>)>> {
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

    /// The reference [`Batcher`]: stages updates and releases those the arrangement frontier
    /// unblocks under the *total order* on times.
    ///
    /// Public because the release rule, not the cursor walk, is the subtle half of a half join,
    /// and any [`HalfJoinTactic`] over row updates wants exactly this rule rather than its own
    /// transcription of it.
    pub struct BlobList<D, T: Timestamp, R> {
        stage: Vec<(T, D, R)>,
        blobs: Vec<VecDeque<(T, D, R)>>,
        lower: MutableAntichain<T>,
        /// Whether an arrangement time equal to an update's time still blocks it.
        strict: bool,
    }

    impl<D, T: Timestamp, R> BlobList<D, T, R> {
        /// Allocates an empty list that releases updates under the `strict` comparison.
        pub fn new(strict: bool) -> Self {
            BlobList { stage: Vec::new(), blobs: Vec::new(), lower: MutableAntichain::new(), strict }
        }
    }

    impl<D: Ord, T: Timestamp, R: Semigroup> Batcher<T, Vec<(D, T, R)>, Vec<(D, T, R)>> for BlobList<D, T, R> {
        fn insert(&mut self, container: Vec<(D, T, R)>) {
            self.stage.extend(container.into_iter().map(|(d,t,r)| (t,d,r)));
        }
        fn extract(&mut self, frontier: AntichainRef<'_, T>) -> (Vec<Vec<(D, T, R)>>, &MutableAntichain<T>) {
            // Handle any staged updates first.
            consolidate_updates(&mut self.stage);
            if !self.stage.is_empty() {
                let blob: VecDeque<_> = std::mem::take(&mut self.stage).into();
                self.lower.update_iter(blob.iter().map(|x| (x.0.clone(), 1)));
                self.blobs.push(blob);
            }

            // An update is unblocked once no arrangement time can still precede it. The times the
            // frontier may yet produce are bounded below in the total order by its minimum, and
            // `strict` says whether a time equal to an update's own still counts as preceding. An
            // empty frontier will produce no times at all, and unblocks everything.
            let bound = frontier.iter().min();
            let strict = self.strict;
            let eligible = |time: &T| match bound {
                None => true,
                Some(bound) => if strict { time.le(bound) } else { time.lt(bound) },
            };

            // Blobs are stored in time order, and eligibility is monotone in time, so what each
            // releases is a prefix.
            let mut result = Vec::new();
            for blob in self.blobs.iter_mut() {
                let mut list = Vec::new();
                while blob.front().map(|(t,_,_)| eligible(t)).unwrap_or(false) {
                    let (time, data, diff) = blob.pop_front().unwrap();
                    list.push((data, time, diff));
                }
                if !list.is_empty() { result.push(list); }
            }

            self.blobs.retain(|b| !b.is_empty());

            self.lower.update_iter(result.iter().flat_map(|l| l.iter().map(|x| (x.1.clone(), -1))));
            (result, &self.lower)
        }
    }

}
