//! An append-only collection of update batches.
//!
//! The `Spine` is a general-purpose trace implementation based on collection and merging
//! immutable batches of updates. It is generic with respect to the batch type, and can be
//! instantiated for any implementor of `trace::Batch`.

use std::fmt::Debug;

use ::difference::Semigroup;
use lattice::Lattice;
use trace::{Batch, BatchReader, Trace, TraceReader};
// use trace::cursor::cursor_list::CursorList;
use trace::cursor::{Cursor, CursorList};
use trace::Merger;

use ::timely::dataflow::operators::generic::OperatorInfo;

enum MergeState<K, V, T, R, B: Batch<K, V, T, R>> {
    Merging(B, B, Option<Vec<T>>, <B as Batch<K,V,T,R>>::Merger),
    Complete(B),
}

impl<K, V, T: Eq, R, B: Batch<K, V, T, R>> MergeState<K, V, T, R, B> {
    fn complete(mut self, logger: &mut Option<::logging::Logger>, operator: usize, scale: usize) -> B {
        if let MergeState::Merging(ref source1, ref source2, ref frontier, ref mut in_progress) = self {
            let mut fuel = usize::max_value();
            in_progress.work(source1, source2, frontier, &mut fuel);
            assert!(fuel > 0);
        }
        match self {
            // ALLOC: Here is where we may de-allocate batches.
            MergeState::Merging(b1, b2, _, finished) => {
                let finished = finished.done();
                logger.as_ref().map(|l|
                    l.log(::logging::MergeEvent {
                        operator,
                        scale,
                        length1: b1.len(),
                        length2: b2.len(),
                        complete: Some(finished.len()),
                    })
                );
                finished
            },
            MergeState::Complete(x) => x,
        }
    }

    fn is_complete(&self) -> bool {
        match *self {
            MergeState::Complete(_) => true,
            _ => false,
        }
    }
    fn begin_merge(batch1: B, batch2: B, frontier: Option<Vec<T>>) -> Self {
        assert!(batch1.upper() == batch2.lower());
        let begin_merge = <B as Batch<K, V, T, R>>::begin_merge(&batch1, &batch2);
        MergeState::Merging(batch1, batch2, frontier, begin_merge)
    }
    fn work(mut self, fuel: &mut usize, logger: &mut Option<::logging::Logger>, operator: usize, scale: usize) -> Self {
        if let MergeState::Merging(ref source1, ref source2, ref frontier, ref mut in_progress) = self {
            in_progress.work(source1, source2, frontier, fuel);
        }
        if *fuel > 0 {
            match self {
                // ALLOC: Here is where we may de-allocate batches.
                MergeState::Merging(b1, b2, _, finished) => {
                    let finished = finished.done();
                    logger.as_ref().map(|l|
                        l.log(::logging::MergeEvent {
                            operator,
                            scale,
                            length1: b1.len(),
                            length2: b2.len(),
                            complete: Some(finished.len()),
                        })
                    );
                    MergeState::Complete(finished)
                },
                MergeState::Complete(x) => MergeState::Complete(x),
            }
        }
        else { self }
    }
    fn len(&self) -> usize {
        match *self {
            MergeState::Merging(ref batch1, ref batch2, _, _) => batch1.len() + batch2.len(),
            MergeState::Complete(ref batch) => batch.len(),
        }
    }
}

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
    merging: Vec<Option<MergeState<K,V,T,R,B>>>,// Several possibly shared collections of updates.
    pending: Vec<B>,                       // Batches at times in advance of `frontier`.
    upper: Vec<T>,
    effort: usize,
    activator: Option<timely::scheduling::activate::Activator>,
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
        assert!(self.advance_frontier.len() > 0, "cursor_through({:?}) called for closed trace", upper);

        // Check that `upper` is greater or equal to `self.through_frontier`.
        // Otherwise, the cut could be in `self.merging` and it is user error anyhow.
        assert!(upper.iter().all(|t1| self.through_frontier.iter().any(|t2| t2.less_equal(t1))));

        let mut cursors = Vec::new();
        let mut storage = Vec::new();

        for merge_state in self.merging.iter().rev() {
            match *merge_state {
                Some(MergeState::Merging(ref batch1, ref batch2, _, _)) => {
                    if !batch1.is_empty() {
                        cursors.push(batch1.cursor());
                        storage.push(batch1.clone());
                    }
                    if !batch2.is_empty() {
                        cursors.push(batch2.cursor());
                        storage.push(batch2.clone());
                    }
                },
                Some(MergeState::Complete(ref batch)) => {
                    if !batch.is_empty() {
                        cursors.push(batch.cursor());
                        storage.push(batch.clone());
                    }
                },
                None => { }
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

        // Commenting out for now; causes problems in `read_upper()`.
        // If one has an urgent need to release these resources, it
        // is probably best just to drop the trace.

        // if self.advance_frontier.len() == 0 {
        //     self.drop_batches();
        // }
    }
    fn advance_frontier(&mut self) -> &[T] { &self.advance_frontier[..] }
    fn distinguish_since(&mut self, frontier: &[T]) {
        self.through_frontier = frontier.to_vec();
        self.consider_merges();
    }
    fn distinguish_frontier(&mut self) -> &[T] { &self.through_frontier[..] }

    fn map_batches<F: FnMut(&Self::Batch)>(&mut self, mut f: F) {
        for batch in self.merging.iter().rev() {
            match *batch {
                Some(MergeState::Merging(ref batch1, ref batch2, _, _)) => { f(batch1); f(batch2); },
                Some(MergeState::Complete(ref batch)) => { f(batch); },
                None => { },
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

    fn exert(&mut self, effort: usize) {
        let batch_size = effort.next_power_of_two();
        let batch_index = batch_size.trailing_zeros() as usize;
        self.work_for(batch_size, batch_index);
    }

    // Ideally, this method acts as insertion of `batch`, even if we are not yet able to begin
    // merging the batch. This means it is a good time to perform amortized work proportional
    // to the size of batch.
    fn insert(&mut self, batch: Self::Batch) {

        self.logger.as_ref().map(|l| l.log(::logging::BatchEvent {
            operator: self.operator.global_id,
            length: batch.len()
        }));

        assert!(batch.lower() != batch.upper());
        assert_eq!(batch.lower(), &self.upper[..]);

        self.upper = batch.upper().to_vec();

        // TODO: Consolidate or discard empty batches.
        self.pending.push(batch);
        self.consider_merges();
    }

    fn close(&mut self) {
        if !self.upper.is_empty() {
            use trace::Builder;
            let builder = B::Builder::new();
            let batch = builder.done(&self.upper[..], &[], &self.upper[..]);
            self.insert(batch);
        }
    }
}

// Drop implementation allows us to log batch drops, to zero out maintained totals.
impl<K, V, T, R, B> Drop for Spine<K, V, T, R, B>
where
    T: Lattice+Ord,
    R: Semigroup,
    B: Batch<K, V, T, R>,
{
    fn drop(&mut self) {
        self.drop_batches();
    }
}


impl<K, V, T, R, B> Spine<K, V, T, R, B>
where
    T: Lattice+Ord,
    R: Semigroup,
    B: Batch<K, V, T, R>,
{
    /// Drops and logs batches. Used in advance_by and drop.
    fn drop_batches(&mut self) {
        if let Some(logger) = &self.logger {
            for batch in self.merging.drain(..) {
                if let Some(batch) = batch {
                    logger.log(::logging::DropEvent {
                        operator: self.operator.global_id,
                        length: batch.len(),
                    });
                }
            }
            for batch in self.pending.drain(..) {
                logger.log(::logging::DropEvent {
                    operator: self.operator.global_id,
                    length: batch.len(),
                });
            }
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
        }
    }

    // Migrate data from `self.pending` into `self.merging`.
    #[inline(never)]
    fn consider_merges(&mut self) {

        // We have a new design here, in an attempt to rationalize progressive merging of batches.
        //
        // Batches arrive with a number of records, and are assigned a power-of-two "size", which is this
        // number rounded up to the next power of two. Batches are placed in a slot based on their size,
        // and each slot can be one of:
        //
        //  i. empty,
        //  ii. occupied by a batch,
        //  iii. occupied by a merge in progress (two batches).
        //
        // Our goal is to track the behavior of hypothetical ideal merge strategy, in which whenever two
        // elements want to occupy the same slot, we merge them and re-evaluate which slot they should be in.
        // Perhaps they are fine where they were, but most likely they want to advance to the next slot.
        // If the next slot is free they move there and stop, and if it is occupied we continue to merge
        // until we find a free slot (or introduce a new empty slot at the end of the list).
        //
        // We do not actually want to execute the merges in this *sequence*, because it may involve a great
        // deal of computation all at once, but we do want to execute this *set* of merges.
        //
        // Our strategy is that we initiate merges and perform work progressively, and when a merge completes
        // we immediately consider initiating a merge with the next slot (if appropriate). The intent is that
        // any merge we initiate will cascade along exactly as in the eager merging case, as no new batch can
        // overtake any merges in progress.
        //
        // Our main technical issue is that we need to ensure that merges complete before other batches want
        // to initiate a merge with their resulting batch.
        //
        // To this end, as batches are introduced, we perform an amount of work on all existing merges that
        // is proportional to the size of the introduced batch. The intent is that once a merge is initiated,
        // in the eager case, we must introduce at least as many records as the slot the merge lands in before
        // we will spill out of the slot the merge is departing. There is the technicality that the amount of
        // work we perform on a merge is proportional to the new batch size *times* the number of slots above
        // it until the next merge is reached. These slots could be merges in progress, if the one we work on
        // would cascade in the reference merge sequence.

        // We maintain a few invariants as we execute, with the intent of maintaining a minimal
        // representation. First, batches are ordered from newest to oldest.
        //
        //     1. merging[i].lower == merging[i+1].upper (unless either is None).
        //     2. large batches never have small indices.

        while self.pending.len() > 0 &&
              self.through_frontier.iter().all(|t1| self.pending[0].upper().iter().any(|t2| t2.less_equal(t1)))
        {
            // this could be a VecDeque, if we ever notice this.
            let batch = self.pending.remove(0);

            // Step 0: Determine batch size and target slot.
            let batch_size = batch.len().next_power_of_two();
            let batch_index = batch_size.trailing_zeros() as usize;
            while self.merging.len() <= batch_index { self.merging.push(None); }

            if self.merging.len() > 32 { eprintln!("large progressive merge; len: {:?}", self.merging.len()); }

            // Step 1: Forcibly merge batches in lower slots.
            for position in 0 .. batch_index {
                if let Some(batch) = self.merging[position].take() {
                    let batch = batch.complete(&mut self.logger, self.operator.global_id, position);
                    if let Some(batch2) = self.merging[position+1].take() {
                        let batch2 = batch2.complete(&mut self.logger, self.operator.global_id, position);
                        self.logger.as_ref().map(|l| l.log(
                            ::logging::MergeEvent {
                                operator: self.operator.global_id,
                                scale: position,
                                length1: batch.len(),
                                length2: batch2.len(),
                                complete: None,
                            }
                        ));
                        self.merging[position+1] = Some(MergeState::begin_merge(batch2, batch, None));
                    }
                    else {
                        self.merging[position+1] = Some(MergeState::Complete(batch));
                    };
                }
            }

            // Step 2: Insert new batch at target position
            if let Some(batch2) = self.merging[batch_index].take() {
                let batch2 = batch2.complete(&mut self.logger, self.operator.global_id, batch_index);
                let frontier = if batch_index == self.merging.len()-1 { Some(self.advance_frontier.clone()) } else { None };
                self.logger.as_ref().map(|l| l.log(
                    ::logging::MergeEvent {
                        operator: self.operator.global_id,
                        scale: batch_index,
                        length1: batch.len(),
                        length2: batch2.len(),
                        complete: None,
                    }
                ));
                self.merging[batch_index] = Some(MergeState::begin_merge(batch2, batch, frontier));
            }
            else {
                self.merging[batch_index] = Some(MergeState::Complete(batch));
            }

            self.work_for(batch_size, batch_index);
        }
    }

    /// Performs merging work commensurate with `batch_size` records.
    ///
    /// This method is public so that it can be invoked by external parties.
    /// In principle, this work might be better suited to a thread pool with
    /// more insight into available cycles. As long as the method is used as
    /// above, the merging work needs to happen immediately (or thereabouts)
    /// to avoid falling behind on merges.
    pub fn work_for(&mut self, batch_size: usize, batch_index: usize) {

        // Step 3: Perform `size` work on each in-progress merge, from large to small.
        //         For non-merges, accumulate fuel, as we may need to apply it to merges
        //         that result at us.
        let mut fuel = 0;
        for position in (batch_index .. self.merging.len()).rev() {

            // We add fuel for any merge that may lead to this location.
            fuel += (2 * batch_size).saturating_mul(self.effort);

            // We now move to the right, merging until we stop merging or run out of fuel.
            let mut new_position = position;
            while self.merging[new_position].as_ref().map(|x| !x.is_complete()).unwrap_or(false) && fuel > 0 {
                if let Some(mut batch) = self.merging[new_position].take() {

                    // Apply work with accumulated fuel.
                    batch = batch.work(&mut fuel, &mut self.logger, self.operator.global_id, position);

                    // If we have a complete batch, and it wants to be in the next slot ...
                    if batch.is_complete() && batch.len() >= (1 << new_position) {//.next_power_of_two().trailing_zeros() as usize > new_position {

                        new_position += 1;
                        if self.merging.len() <= new_position { self.merging.push(None); }

                        // If the next slot is actually occupied, must start a merge.
                        if let Some(mut batch2) = self.merging[new_position].take() {
                            if !batch2.is_complete() {
                                let mut temp_fuel = usize::max_value();
                                batch2 = batch2.work(&mut temp_fuel, &mut self.logger, self.operator.global_id, position);
                                self.logger.as_ref().map(|l| l.log(
                                    ::logging::MergeShortfall {
                                        operator: self.operator.global_id,
                                        scale: new_position,
                                        shortfall: usize::max_value() - temp_fuel,
                                    }
                                ));
                            }
                            let batch1 = batch.complete(&mut self.logger, self.operator.global_id, position);
                            let batch2 = batch2.complete(&mut self.logger, self.operator.global_id, position);
                            // if this is the last position, engage compaction.
                            let frontier = if new_position+1 == self.merging.len() { Some(self.advance_frontier.clone()) } else { None };
                            self.logger.as_ref().map(|l| l.log(
                                ::logging::MergeEvent {
                                    operator: self.operator.global_id,
                                    scale: position,
                                    length1: batch1.len(),
                                    length2: batch2.len(),
                                    complete: None,
                                }
                            ));
                            self.merging[new_position] = Some(MergeState::begin_merge(batch2, batch1, frontier));
                        }
                        else {
                            self.merging[new_position] = Some(batch);
                        }
                    }
                    else {
                        self.merging[new_position] = Some(batch);
                    }
                }
                else {
                    // We can't be here. The while condition ensures that an entry exists.
                }
            }
        }

        // Step 4: Consider migrating complete batches to lower bins, if appropriate.
        for index in (1 .. self.merging.len()).rev() {
            if self.merging[index].as_ref().map(|x| x.is_complete() && x.len() < (1 << (index-1))).unwrap_or(false) {
                if self.merging[index-1].is_none() {
                    self.merging[index-1] = self.merging[index].take();
                }
            }
        }
        while self.merging.last().map(|x| x.is_none()) == Some(true) { self.merging.pop(); }

        if let Some(activator) = self.activator.as_mut() {
            let mut should_activate = false;
            let mut status = String::new();
            for batch in self.merging.iter() {
                if let Some(batch) = batch {
                    if !batch.is_complete() {
                        should_activate = true;
                        status.push('M');
                    }
                    else {
                        status.push('C');
                    }

                }
                else {
                    status.push('E');
                }
            }
            println!("activator present; should_activate: {:?},\t{:?}", should_activate, status);
            if should_activate {
                activator.activate();
            }
        }
    }
}
