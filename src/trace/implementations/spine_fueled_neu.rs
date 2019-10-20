//! An append-only collection of update batches.
//!
//! The `Spine` is a general-purpose trace implementation based on collection and merging
//! immutable batches of updates. It is generic with respect to the batch type, and can be
//! instantiated for any implementor of `trace::Batch`.
//!
//! ## Design
//!
//! This spine is represented as a list of layers, where each element in the list is either
//!
//!   1. MergeState::Vacant  empty
//!   2. MergeState::Single  a single batch
//!   3. MergeState::Double  a pair of batches
//!
//! Each "batch" has the option to be `None`, indicating a non-batch that nonetheless acts
//! as a number of updates proportionate to the level at which it exists (for bookkeeping).
//!
//! Each of the batches at layer i contains at most 2^i elements. The sequence of batches
//! should have the upper bound of one match the lower bound of the next. Batches may be
//! logically empty, with matching upper and lower bounds, as a bookkeeping mechanism.
//!
//! Each batch at layer i is treated as if it contains exactly 2^i elements, even though it
//! may actually contain fewer elements. This allows us to decouple the physical representation
//! from logical amounts of effort invested in each batch. It allows us to begin compaction and
//! to reduce the number of updates, without compromising our ability to continue to move
//! updates along the spine. We are explicitly making the trade-off that while some batches
//! might compact at lower levels, we want to treat them as if they contained their full set of
//! updates for accounting reasons (to apply work to higher levels).
//!
//! We attempt to maintain the invariant that no two adjacent levels have pairs of batches.
//! This invariant exists to make sure we have the breathing room to initiate but not
//! immediately complete merges.
//!
//! ## Mathematics
//!
//! When a merge is initiated, there should be a non-negative *deficit* of updates before the layers
//! below could plausibly produce a new batch for the currently merging layer. We must determine a
//! factor of proportionality, so that newly arrived updates provide at least that amount of "fuel"
//! towards the merging layer.
//!
//! ### Deficit:
//!
//! A new merge is initiated only in response to the completion of a prior merge, and when a prior
//! merge completes it vacates its entire level. Assuming we have maintaned the invariant that no
//! adjacent levels house pairs of batches, then we can bound the total number of updates at levels
//! below that of the completed merge (let's call it "k"):
//!
//! As just a moment ago there were two batches at layer k, there can be at most one batch at layer
//! k-1, then two at layer k-2, and so on alternating. The number of updates is therefore bounded by
//!
//!         2^k-1 + 2*2^k-2 + 2^k-3 + 2*2^k-4 + ...
//!       = \---  2^k  ---/ + \--- 2^k-2 ---/ + ...
//!      <= 2^k+1 - 2^k-1 - 2^k-3 - ...
//!
//! The last inequality reveals that there *should* be a deficit of at least 2^k-1 plus a bit more.
//!
//! Generally, we can track the sum of the number of apparent records up through each layer, from
//! which we can determine this deficit. We want to ensure a relationship between the deficit and
//! the remaining amount of work to perform to complete a merge, at each layer of the trace.
//!
//! ### Fueling
//!
//! We have at most 2^k+1 computation to perform to complete a merge at level k+1, and a lower bound
//! of 2^k-1 on the number of updates we will accept before the merge *must* be completed. If each
//! inserted update applies four units of fuel to each in-progress merge, we should be good to go.
//! Perhaps importantly, the fuel should be applied before the updates are inserted, just to make
//! sure there is room.
//!
//! ### Fuel sharing
//!
//! We like the idea of applying fuel preferentially to merges at *lower* levels, under the idea that
//! they are easier to complete, and we benefit from fewer total merges in progress. This does delay
//! the completion of merges at higher levels, and may not obviously be a total win. If we choose to
//! do this, we should make sure that we correctly account for completed merges at low layers: they
//! should still extract fuel from new updates even though they have completed, at least until they
//! have paid back any "debt" to higher layers by continuing to provide fuel as updates arrive.


use std::fmt::Debug;

use ::difference::Semigroup;
use lattice::Lattice;
use trace::{Batch, BatchReader, Trace, TraceReader};
// use trace::cursor::cursor_list::CursorList;
use trace::cursor::{Cursor, CursorList};
use trace::Merger;

use ::timely::dataflow::operators::generic::OperatorInfo;

/// An append-only collection of update tuples.
///
/// A spine maintains a small number of immutable collections of update tuples, merging the collections when
/// two have similar sizes. In this way, it allows the addition of more tuples, which may then be merged with
/// other immutable collections.
pub struct Spine<K, V, T: Lattice+Ord, R: Semigroup, B: Batch<K, V, T, R>> {
    operator: OperatorInfo,
    logger: Option<::logging::Logger>,
    phantom: ::std::marker::PhantomData<(K, V, R)>,
    advance_frontier: Vec<T>,                   // Times after which the trace must accumulate correctly.
    through_frontier: Vec<T>,                   // Times after which the trace must be able to subset its inputs.
    merging: Vec<MergeState<K,V,T,R,B>>,// Several possibly shared collections of updates.
    pending: Vec<B>,                       // Batches at times in advance of `frontier`.
    upper: Vec<T>,
    effort: usize,
    activator: Option<timely::scheduling::activate::Activator>,
    timer: std::time::Instant,
}

impl<K, V, T, R, B> TraceReader for Spine<K, V, T, R, B>
where
    K: Ord+Clone,           // Clone is required by `batch::advance_*` (in-place could remove).
    V: Ord+Clone,           // Clone is required by `batch::advance_*` (in-place could remove).
    T: Lattice+Ord+Clone+Debug+Default,
    R: Semigroup,
    B: Batch<K, V, T, R>+Clone+'static,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type R = R;

    type Batch = B;
    type Cursor = CursorList<K, V, T, R, <B as BatchReader<K, V, T, R>>::Cursor>;

    fn cursor_through(&mut self, upper: &[T]) -> Option<(Self::Cursor, <Self::Cursor as Cursor<K, V, T, R>>::Storage)> {

        // The supplied `upper` should have the property that for each of our
        // batch `lower` and `upper` frontiers, the supplied upper is comparable
        // to the frontier; it should not be incomparable, because the frontiers
        // that we created form a total order. If it is, there is a bug.
        //
        // We should acquire a cursor including all batches whose upper is less
        // or equal to the supplied upper, excluding all batches whose lower is
        // greater or equal to the supplied upper, and if a batch straddles the
        // supplied upper it had better be empty.

        // We shouldn't grab a cursor into a closed trace, right?
        assert!(self.advance_frontier.len() > 0);

        // Check that `upper` is greater or equal to `self.through_frontier`.
        // Otherwise, the cut could be in `self.merging` and it is user error anyhow.
        assert!(upper.iter().all(|t1| self.through_frontier.iter().any(|t2| t2.less_equal(t1))));

        let mut cursors = Vec::new();
        let mut storage = Vec::new();

        for merge_state in self.merging.iter().rev() {
            match merge_state {
                MergeState::Double(variant) => {
                    match variant {
                        MergeVariant::InProgress(batch1, batch2, _, _) => {
                            if !batch1.is_empty() {
                                cursors.push(batch1.cursor());
                                storage.push(batch1.clone());
                            }
                            if !batch2.is_empty() {
                                cursors.push(batch2.cursor());
                                storage.push(batch2.clone());
                            }
                        },
                        MergeVariant::Complete(Some(batch)) => {
                            if !batch.is_empty() {
                                cursors.push(batch.cursor());
                                storage.push(batch.clone());
                            }
                        }
                        MergeVariant::Complete(None) => { },
                    }
                },
                MergeState::Single(Some(batch)) => {
                    if !batch.is_empty() {
                        cursors.push(batch.cursor());
                        storage.push(batch.clone());
                    }
                },
                MergeState::Single(None) => { },
                MergeState::Vacant => { },
            }
        }

        for batch in self.pending.iter() {

            if !batch.is_empty() {

                // For a non-empty `batch`, it is a catastrophic error if `upper`
                // requires some-but-not-all of the updates in the batch. We can
                // determine this from `upper` and the lower and upper bounds of
                // the batch itself.
                //
                // TODO: It is not clear if this is the 100% correct logic, due
                // to the possible non-total-orderedness of the frontiers.

                let include_lower = upper.iter().all(|t1| batch.lower().iter().any(|t2| t2.less_equal(t1)));
                let include_upper = upper.iter().all(|t1| batch.upper().iter().any(|t2| t2.less_equal(t1)));

                if include_lower != include_upper && upper != batch.lower() {
                    panic!("`cursor_through`: `upper` straddles batch");
                }

                // include pending batches
                if include_upper {
                    cursors.push(batch.cursor());
                    storage.push(batch.clone());
                }
            }
        }

        Some((CursorList::new(cursors, &storage), storage))
    }
    fn advance_by(&mut self, frontier: &[T]) {
        self.advance_frontier = frontier.to_vec();
        if self.advance_frontier.len() == 0 {
            self.pending.clear();
            self.merging.clear();
        }
    }
    fn advance_frontier(&mut self) -> &[T] { &self.advance_frontier[..] }
    fn distinguish_since(&mut self, frontier: &[T]) {
        self.through_frontier = frontier.to_vec();
        self.consider_merges();
    }
    fn distinguish_frontier(&mut self) -> &[T] { &self.through_frontier[..] }

    fn map_batches<F: FnMut(&Self::Batch)>(&mut self, mut f: F) {
        for batch in self.merging.iter().rev() {
            match batch {
                MergeState::Double(MergeVariant::InProgress(batch1, batch2, _, _)) => { f(batch1); f(batch2); },
                MergeState::Double(MergeVariant::Complete(Some(batch))) => { f(batch) },
                MergeState::Single(Some(batch)) => { f(batch) },
                _ => { },
            }
        }
        for batch in self.pending.iter() {
            f(batch);
        }
    }
}

// A trace implementation for any key type that can be borrowed from or converted into `Key`.
// TODO: Almost all this implementation seems to be generic with respect to the trace and batch types.
impl<K, V, T, R, B> Trace for Spine<K, V, T, R, B>
where
    K: Ord+Clone,
    V: Ord+Clone,
    T: Lattice+Ord+Clone+Debug+Default,
    R: Semigroup,
    B: Batch<K, V, T, R>+Clone+'static,
{
    fn new(
        info: ::timely::dataflow::operators::generic::OperatorInfo,
        logging: Option<::logging::Logger>,
        activator: Option<timely::scheduling::activate::Activator>,
    ) -> Self {
        Self::with_effort(4, info, logging, activator)
    }

    /// Apply some amount of effort to trace maintenance.
    ///
    /// The units of effort are updates, and the method should be
    /// thought of as analogous to inserting as many empty updates,
    /// where the trace is permitted to perform proportionate work.
    fn exert(&mut self, effort: &mut isize) {

        // If all merges are complete and multiple batches remain,
        // introduce empty batches to prompt merging.
        if !self.reduced() {

            // TODO : consider directly invoking `apply_fuel` and only introducing batches
            // should the fuel not be fully consumed. This delays the introduction of
            // empty batches which does a number on the balanced-ness of merging.

            // Introduce an empty batch with roughly *effort number of virtual updates.
            let level = (*effort as usize).next_power_of_two().trailing_zeros() as usize;
            self.introduce_batch(None, level);

            // We are here because we were not in reduced form.
            if let Some(activator) = &self.activator {
                activator.activate();
            }
        }
    }

    // Ideally, this method acts as insertion of `batch`, even if we are not yet able to begin
    // merging the batch. This means it is a good time to perform amortized work proportional
    // to the size of batch.
    fn insert(&mut self, batch: Self::Batch) {

        // self.logger.as_ref().map(|l| l.log(::logging::BatchEvent {
        //     operator: self.operator.global_id,
        //     length: batch.len()
        // }));

        assert!(batch.lower() != batch.upper());
        assert_eq!(batch.lower(), &self.upper[..]);

        self.upper = batch.upper().to_vec();

        // TODO: Consolidate or discard empty batches.
        self.pending.push(batch);
        self.consider_merges();
    }

    /// Completes the trace with a final empty batch.
    fn close(&mut self) {
        if !self.upper.is_empty() {
            use trace::Builder;
            let builder = B::Builder::new();
            let batch = builder.done(&self.upper[..], &[], &self.upper[..]);
            self.insert(batch);
        }
    }
}

impl<K, V, T, R, B> Spine<K, V, T, R, B>
where
    K: Ord+Clone,
    V: Ord+Clone,
    T: Lattice+Ord+Clone+Debug+Default,
    R: Semigroup,
    B: Batch<K, V, T, R>,
{
    /// True iff there is at most one batch in `self.merging`.
    ///
    /// When true, there is no maintenance work to perform in the trace, other than compaction.
    /// We do not yet have logic in place to determine if compaction would improve a trace, so
    /// for now we are ignoring that.
    fn reduced(&self) -> bool {
        let mut non_empty = 0;
        for index in 0 .. self.merging.len() {
            if self.merging[index].is_double() { return false; }
            if self.merging[index].non_trivial() { non_empty += 1; }
            if non_empty > 1 { return false; }
        }
        true
    }

    /// Describes the merge progress of layers in the trace.
    ///
    /// Intended for diagnostics rather than public consumption.
    fn describe(&self) -> Vec<(usize, usize)> {
        self.merging
            .iter()
            .map(|b| match b {
                MergeState::Vacant => (0, 0),
                x @ MergeState::Single(_) => (1, x.len()),
                x @ MergeState::Double(_) => (2, x.len()),
            })
            .collect()
    }

    /// Allocates a fueled `Spine` with a specified effort multiplier.
    ///
    /// This trace will merge batches progressively, with each inserted batch applying a multiple
    /// of the batch's length in effort to each merge. The `effort` parameter is that multiplier.
    /// This value should be at least one for the merging to happen; a value of zero is not helpful.
    pub fn with_effort(
        mut effort: usize,
        operator: OperatorInfo,
        logger: Option<::logging::Logger>,
        activator: Option<timely::scheduling::activate::Activator>,
    ) -> Self {

        // Zero effort is .. not smart.
        if effort == 0 { effort = 1; }

        Spine {
            operator,
            logger,
            phantom: ::std::marker::PhantomData,
            advance_frontier: vec![<T as Lattice>::minimum()],
            through_frontier: vec![<T as Lattice>::minimum()],
            merging: Vec::new(),
            pending: Vec::new(),
            upper: vec![Default::default()],
            effort,
            activator,
            timer: std::time::Instant::now(),
        }
    }

    /// Migrate data from `self.pending` into `self.merging`.
    ///
    /// This method reflects on the bookmarks held by others that may prevent merging, and in the
    /// case that new batches can be introduced to the pile of mergeable batches, it gets on that.
    #[inline(never)]
    fn consider_merges(&mut self) {

        // TODO: Consider merging pending batches before introducing them.
        // TODO: We could use a `VecDeque` here to draw from the front and append to the back.
        while self.pending.len() > 0 &&
              self.through_frontier.iter().all(|t1| self.pending[0].upper().iter().any(|t2| t2.less_equal(t1)))
        {
            let batch = self.pending.remove(0);
            let index = batch.len().next_power_of_two();
            self.introduce_batch(Some(batch), index.trailing_zeros() as usize);

            // Having performed all of our work, if more than one batch remains reschedule ourself.
            if !self.reduced() {
                if let Some(activator) = &self.activator {
                    activator.activate();
                }
            }
        }
    }

    /// Introduces a batch at an indicated level.
    ///
    /// The level indication is often related to the size of the batch, but
    /// it can also be used to artificially fuel the computation by supplying
    /// empty batches at non-trivial indices, to move merges along.
    pub fn introduce_batch(&mut self, batch: Option<B>, batch_index: usize) {

        // Step 0.  Determine an amount of fuel to use for the computation.
        //
        //          Fuel is used to drive maintenance of the data structure,
        //          and in particular are used to make progress through merges
        //          that are in progress. The amount of fuel to use should be
        //          proportional to the number of records introduced, so that
        //          we are guaranteed to complete all merges before they are
        //          required as arguments to merges again.
        //
        //          The fuel use policy is negotiable, in that we might aim
        //          to use relatively less when we can, so that we return
        //          control promptly, or we might account more work to larger
        //          batches. Not clear to me which are best, of if there
        //          should be a configuration knob controlling this.

        // The amount of fuel to use is proportional to 2^batch_index, scaled
        // by a factor of self.effort which determines how eager we are in
        // performing maintenance work. We need to ensure that each merge in
        // progress receives fuel for each introduced batch, and so multiply
        // by that as well.
        if batch_index > 32 { println!("Large batch index: {}", batch_index); }
        let mut fuel = 4 << batch_index;
        fuel *= self.effort;
        // let merges: usize = self.merging.iter().map(|b|
        //     if let MergeState::Double(MergeVariant::InProgress(_,_,_,_)) = b { 1 } else { 0 }
        // ).sum();
        // fuel *= merges;
        // fuel *= self.merging.len();
        // Convert to an `isize` so we can observe shortfall.
        let mut fuel = fuel as isize;

        // Step 1.  Apply fuel to each in-progress merge.
        //
        //          Before we can introduce new updates, we must apply any
        //          fuel to in-progress merges, as this fuel is what ensures
        //          that the merges will be complete by the time we insert
        //          the updates.
        self.apply_fuel(&mut fuel);

        // Step 2.  We must ensure the invariant that adjacent layers do not
        //          contain two batches will be satisfied when we insert the
        //          batch. We forcibly completing all merges at layers lower
        //          than and including `batch_index`, so that the new batch
        //          is inserted into an empty layer.
        //
        //          We could relax this to "strictly less than `batch_index`"
        //          if the layer above has only a single batch in it, which
        //          seems not implausible if it has been the focus of effort.
        //
        //          This should be interpreted as the introduction of some
        //          volume of fake updates, and we will need to fuel merges
        //          by a proportional amount to ensure that they are not
        //          surprised later on. The number of fake updates should
        //          correspond to the deficit for the layer, which perhaps
        //          we should track explicitly.
        self.roll_up(batch_index);

        // Step 3. This insertion should be into an empty layer. It is a
        //         logical error otherwise, as we may be violating our
        //         invariant, from which all wonderment derives.
        self.insert_at(batch, batch_index);

        // Step 4. Tidy the largest layers.
        //
        //         It is important that we not tidy only smaller layers,
        //         as their ascension is what ensures the merging and
        //         eventual compaction of the largest layers.
        self.tidy_layers();
    }

    /// Ensures that layers up through and including `index` are empty.
    ///
    /// This method is used to prepare for the insertion of a single batch
    /// at `index`, which should maintain the invariant that no adjacent
    /// layers both contain `MergeState::Double` variants.
    fn roll_up(&mut self, index: usize) {

        while self.merging.len() <= index {
            self.merging.push(MergeState::Vacant);
        }

        //  Merges should skip over vacant and structurally empty batches.
        let merge =
        self.merging[.. index+1]
            .iter_mut()
            .flat_map(|level| level.complete())
            .fold(None, |merge, level| MergeState::begin_merge(Some(level), merge, None).complete());

        // We have collected all batches at levels less or equal to index, which represents
        // 2^{index+1} updates. It now belongs at level index+1, which we hope has resolved
        // any merging through the prior application of fuel.
        self.insert_at(merge, index + 1);
    }

    /// Applies an amount of fuel to merges in progress.
    ///
    /// The intended invariants maintain that each merge in progress completes before
    /// there are enough records in lower levels to fully populate one batch at its
    /// layer. This invariant ensures that we can always apply an unbounded amount of
    /// fuel and not encounter merges in to merging layers (the "safety" does not result
    /// from insufficient fuel applied to lower levels).
    pub fn apply_fuel(&mut self, fuel: &mut isize) {
        // Apply fuel to each merge; do not share across layers at the moment.
        // This is an interesting idea, but we don't have accounting in place yet.
        // Specifically, we need completed merges at lower layers to "pay back" any
        // debt they may have taken on by borrowing against the fuel of higher layers.
        for index in 0 .. self.merging.len() {
            let mut fuel = *fuel;
            self.merging[index].work(&mut fuel);
            if self.merging[index].is_complete() {
                let complete = self.merging[index].complete();
                self.insert_at(complete, index+1);
            }
        }
    }

    /// Inserts a batch at a specific location.
    ///
    /// This is a non-public internal method that can panic if we try and insert into a
    /// layer which already contains two batches (and is in the process of merging).
    fn insert_at(&mut self, batch: Option<B>, index: usize) {
        while self.merging.len() <= index {
            self.merging.push(MergeState::Vacant);
        }
        let frontier = Some(self.advance_frontier.clone());
        self.merging[index].insert(batch, frontier);
    }

    /// Attempts to draw down large layers to size appropriate layers.
    fn tidy_layers(&mut self) {

        // If the largest layer is complete (not merging), we can attempt
        // to draw it down to the next layer if either that layer is empty,
        // or if it is a singleton and the layer below it is not merging.
        // We expect this should happen at various points if we have enough
        // fuel rolling around.

        let mut length = self.merging.len();
        if self.merging[length-1].is_single() {
            while (self.merging[length-1].len().next_power_of_two().trailing_zeros() as usize) < length && length > 1 && self.merging[length-2].is_vacant() {
                let batch = self.merging.pop().unwrap();
                self.merging[length-2] = batch;
                length = self.merging.len();
            }
        }
    }
}


/// Describes the state of a layer.
///
/// A layer can be empty, contain a single batch, or contain a pair of batches
/// that are in the process of merging into a batch for the next layer.
enum MergeState<K, V, T, R, B: Batch<K, V, T, R>> {
    /// An empty layer, containing no updates.
    Vacant,
    /// A layer containing a single batch.
    ///
    /// The `None` variant is used to represent a structurally empty batch present
    /// to ensure the progress of maintenance work.
    Single(Option<B>),
    /// A layer containing two batches, in the process of merging.
    Double(MergeVariant<K, V, T, R, B>),
}

impl<K, V, T: Eq, R, B: Batch<K, V, T, R>> MergeState<K, V, T, R, B> {

    /// The number of actual updates contained in the level.
    fn len(&self) -> usize {
        match self {
            MergeState::Single(Some(b)) => b.len(),
            MergeState::Double(MergeVariant::InProgress(b1,b2,_,_)) => b1.len() + b2.len(),
            MergeState::Double(MergeVariant::Complete(Some(b))) => b.len(),
            _ => 0,
        }
    }

    /// True only for the MergeState::Vacant variant.
    fn is_vacant(&self) -> bool {
        if let MergeState::Vacant = self { true } else { false }
    }

    /// True only for the MergeState::Vacant variant.
    fn non_trivial(&self) -> bool {
        match self {
            MergeState::Double(MergeVariant::Complete(None)) => false,
            MergeState::Single(None) => false,
            MergeState::Vacant => false,
            _ => true,
        }
    }

    /// True only for the MergeState::Single variant.
    fn is_single(&self) -> bool {
        if let MergeState::Single(_) = self { true } else { false }
    }

    /// True only for the MergeState::Double variant.
    fn is_double(&self) -> bool {
        if let MergeState::Double(_) = self { true } else { false }
    }

    /// Immediately complete any merge.
    ///
    /// The result is either a batch, if there is a non-trivial batch to return
    /// or `None` if there is no meaningful batch to return. This does not distinguish
    /// between Vacant entries and structurally empty batches, which should be done
    /// with the `is_complete()` method.
    fn complete(&mut self) -> Option<B>  {
        match std::mem::replace(self, MergeState::Vacant) {
            MergeState::Vacant => None,
            MergeState::Single(batch) => batch,
            MergeState::Double(variant) => variant.complete(),
        }
    }

    /// True iff the layer is a complete merge, ready for extraction.
    fn is_complete(&mut self) -> bool {
        if let MergeState::Double(MergeVariant::Complete(_)) = self {
            true
        }
        else {
            false
        }
    }

    /// Performs a bounded amount of work towards a merge.
    ///
    /// If the merge completes, the resulting batch is returned.
    /// If a batch is returned, it is the obligation of the caller
    /// to correctly install the result.
    fn work(&mut self, fuel: &mut isize) {
        // We only perform work for merges in progress.
        if let MergeState::Double(layer) = self {
            layer.work(fuel);
        }
    }

    /// Extract the merge state, typically temporarily.
    fn take(&mut self) -> Self {
        std::mem::replace(self, MergeState::Vacant)
    }

    /// Inserts a batch and begins a merge if needed.
    ///
    /// The return value is true when the merge has completed and the
    /// resulting batch is immediately available for promotion.
    fn insert(&mut self, batch: Option<B>, frontier: Option<Vec<T>>) {
        match self.take() {
            MergeState::Vacant => {
                *self = MergeState::Single(batch);
            }
            MergeState::Single(old) => {
                // logger.as_ref().map(|l| l.log(
                //     ::logging::MergeEvent {
                //         operator,
                //         scale,
                //         length1: batch1.len(),
                //         length2: batch2.len(),
                //         complete: None,
                //     }
                // ));
                *self = MergeState::begin_merge(old, batch, frontier);
            }
            MergeState::Double(_) => {
                panic!("Attempted to insert batch into incomplete merge!")
            }
        };
    }

    fn begin_merge(batch1: Option<B>, batch2: Option<B>, frontier: Option<Vec<T>>) -> MergeState<K, V, T, R, B> {
        let variant =
        match (batch1, batch2) {
            (Some(batch1),Some(batch2)) => {
                assert!(batch1.upper() == batch2.lower());
                let begin_merge = <B as Batch<K, V, T, R>>::begin_merge(&batch1, &batch2);
                MergeVariant::InProgress(batch1, batch2, frontier, begin_merge)
            }
            (None, x) => MergeVariant::Complete(x),
            (x, None) => MergeVariant::Complete(x),
        };

        MergeState::Double(variant)
    }
}

enum MergeVariant<K, V, T, R, B: Batch<K, V, T, R>> {
    /// Describes an actual in-progress merge between two non-trivial batches.
    InProgress(B, B, Option<Vec<T>>, <B as Batch<K,V,T,R>>::Merger),
    /// A merge that requires no further work. May or may not represent a non-trivial batch.
    Complete(Option<B>),
}

impl<K, V, T, R, B: Batch<K, V, T, R>> MergeVariant<K, V, T, R, B> {

    /// Completes and extracts the batch, unless structurally empty.
    fn complete(mut self) -> Option<B> {
        let mut fuel = isize::max_value();
        self.work(&mut fuel);
        if let MergeVariant::Complete(batch) = self { batch }
        else { panic!("Failed to complete a merge!"); }
    }

    // Applies some amount of work, potentially completing the merge.
    fn work(&mut self, fuel: &mut isize) {
        let variant = std::mem::replace(self, MergeVariant::Complete(None));
        if let MergeVariant::InProgress(b1,b2,frontier,mut merge) = variant {
            merge.work(&b1,&b2,&frontier,fuel);
            if *fuel > 0 {
                *self = MergeVariant::Complete(Some(merge.done()));
                }
            else {
                *self = MergeVariant::InProgress(b1,b2,frontier,merge);
            }
        }
        else {
            *self = variant;
        }
    }
}
