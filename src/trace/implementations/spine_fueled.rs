//! An append-only collection of update batches.
//!
//! The `Spine` is a general-purpose trace implementation based on collection and merging
//! immutable batches of updates. It is generic with respect to the batch type, and can be
//! instantiated for any implementor of `trace::Batch`.

use std::fmt::Debug;

use ::difference::Monoid;
use lattice::Lattice;
use trace::{Batch, BatchReader, Trace, TraceReader};
use trace::{Merger, cursor::{Cursor, CursorList} };
use logging::{MergeEvent, MergeShortfall};

use ::timely::dataflow::operators::generic::OperatorInfo;

/// An append-only collection of update tuples.
///
/// A spine maintains a small number of immutable collections of update tuples, merging the collections when
/// two have similar sizes. In this way, it allows the addition of more tuples, which may then be merged with
/// other immutable collections.
pub struct Spine<K, V, T: Lattice+Ord, R: Monoid, B: Batch<K, V, T, R>> {
    operator: OperatorInfo,
    logger: Option<::logging::Logger>,
    phantom: ::std::marker::PhantomData<(K, V, R)>,
    advance_frontier: Vec<T>,            // Times after which the trace must accumulate correctly.
    through_frontier: Vec<T>,            // Times after which the trace must be able to subset its inputs.
    merging: Vec<MergeLevel<K,V,T,R,B>>, // Several possibly shared collections of updates.
    pending: Vec<B>,                     // Batches at times in advance of `frontier`.
    upper: Vec<T>,
    effort: usize,
}

impl<K, V, T, R, B> TraceReader<K, V, T, R> for Spine<K, V, T, R, B>
where
    K: Ord+Clone,           // Clone is required by `batch::advance_*` (in-place could remove).
    V: Ord+Clone,           // Clone is required by `batch::advance_*` (in-place could remove).
    T: Lattice+Ord+Clone+Debug,   // Clone is required by `advance_by` and `batch::advance_*`.
    R: Monoid,
    B: Batch<K, V, T, R>+Clone+'static,
{
    type Batch = B;
    type Cursor = CursorList<K, V, T, R, <B as BatchReader<K, V, T, R>>::Cursor>;

    fn cursor_through(&mut self, upper: &[T]) -> Option<(Self::Cursor, <Self::Cursor as Cursor<K, V, T, R>>::Storage)> {

        // we shouldn't grab a cursor into a closed trace, right?
        assert!(self.advance_frontier.len() > 0);

        // Check that `upper` is greater or equal to `self.through_frontier`.
        // Otherwise, the cut could be in `self.merging` and it is user error anyhow.
        if upper.iter().all(|t1| self.through_frontier.iter().any(|t2| t2.less_equal(t1))) {

            let mut cursors = Vec::new();
            let mut storage = Vec::new();

            for merge_state in self.merging.iter().rev() {
                match *merge_state.merge_state() {
                    Some(MergeState::Double(ref batch1, ref batch2, _, _)) => {
                        cursors.push(batch1.cursor());
                        storage.push(batch1.clone());
                        cursors.push(batch2.cursor());
                        storage.push(batch2.clone());
                    },
                    Some(MergeState::Single(ref batch)) => {
                        cursors.push(batch.cursor());
                        storage.push(batch.clone());
                    },
                    None => { }
                }
            }

            for batch in &self.pending {
                let include_lower = upper.iter().all(|t1| batch.lower().iter().any(|t2| t2.less_equal(t1)));
                let include_upper = upper.iter().all(|t1| batch.upper().iter().any(|t2| t2.less_equal(t1)));

                if include_lower != include_upper && upper != batch.lower() {
                    panic!("`cursor_through`: `upper` straddles batch");
                    // return None;
                }

                // include pending batches
                if include_upper {
                    cursors.push(batch.cursor());
                    storage.push(batch.clone());
                }
            }
            Some((CursorList::new(cursors, &storage), storage))
        }
        else {
            None
        }
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
            match *batch.merge_state() {
                Some(MergeState::Double(ref batch1, ref batch2, _, _)) => { f(batch1); f(batch2); },
                Some(MergeState::Single(ref batch)) => { f(batch); },
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
impl<K, V, T, R, B> Trace<K, V, T, R> for Spine<K, V, T, R, B>
where
    K: Ord+Clone,
    V: Ord+Clone,
    T: Lattice+Ord+Clone+Debug,
    R: Monoid,
    B: Batch<K, V, T, R>+Clone+'static,
{

    fn new(info: ::timely::dataflow::operators::generic::OperatorInfo, logging: Option<::logging::Logger>) -> Self {
        Self::with_effort(1, info, logging)
    }

    // Ideally, this method acts as insertion of `batch`, even if we are not yet able to begin
    // merging the batch. This means it is a good time to perform amortized work proportional
    // to the size of batch.
    fn insert(&mut self, batch: Self::Batch) {

        self.logger.as_ref().map(|l| l.log(::logging::BatchEvent {
            operator: self.operator.global_id,
            length: batch.len()
        }));

        // we can ignore degenerate batches (TODO: learn where they come from; suppress them?)
        if batch.lower() != batch.upper() {
            assert_eq!(batch.lower(), &self.upper[..]);
            self.upper = batch.upper().to_vec();
            self.pending.push(batch);
            self.consider_merges();
        }
        else {
            // degenerate batches had best be empty.
            assert!(batch.len() == 0);
        }
    }

    fn close(&mut self) {
        if self.upper != Vec::new() {
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
    T: Lattice+Ord+Clone+Debug,
    R: Monoid,
    B: Batch<K, V, T, R>,
{
    /// Allocates a fueled `Spine` with a specified effort multiplier.
    ///
    /// This trace will merge batches progressively, with each inserted batch applying a multiple
    /// of the batch's length in effort to each merge. The `effort` parameter is that multiplier.
    /// This value should be at least one for the merging to happen; a value of zero is not helpful.
    pub fn with_effort(mut effort: usize, operator: OperatorInfo, logger: Option<::logging::Logger>) -> Self {

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
            upper: vec![<T as Lattice>::minimum()],
            effort,
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

            // Ensure enough space to
            while self.merging.len() <= batch_index + 1 {
                let len = self.merging.len();
                self.merging.push(MergeLevel::new(len, self.operator.global_id));
            }

            if self.merging.len() > 32 { eprintln!("large progressive merge; len: {:?}", self.merging.len()); }

            // We are going to try and maintain the invariant that there are never two
            // MergeState::Double entries adjacent to one another. If this holds, then
            // we should never accidentally start a merge into a still-merging entry.
            //
            // We are going to try and do this by spending lots of fuel!

            // Step 3: Perform `size` work on each in-progress merge, from large to small.
            //         For non-merges, accumulate fuel, as we may need to apply it to merges
            //         that result at us.
            let mut fuel = 2 * self.effort * batch_size * self.merging.len();
            for position in 0 .. self.merging.len() {

                // We now move to the right, merging until we stop merging or run out of fuel.
                if let Some(batch) = self.merging[position].work(&mut fuel, &mut self.logger) {
                    let new_position = position + 1;
                    while self.merging.len() <= new_position {
                        let len = self.merging.len();
                        self.merging.push(MergeLevel::new(len, self.operator.global_id))
                    }
                    assert!(!self.merging[new_position].is_double());

                    // Advance timestamps if this is the last entry.
                    let mut frontier = None;
                    if new_position+1 == self.merging.len() {
                        frontier = Some(self.advance_frontier.clone());
                    }
                    self.merging[position+1].merge_with(batch, frontier, &mut self.logger);
                }
            }

            // Step 1: Forcibly merge batches in lower slots, clearing destination.
            for position in 0 .. (batch_index+1) {
                assert!(!self.merging[position+1].is_double());
                if let Some(batch) = self.merging[position].complete(&mut self.logger) {
                    self.merging[position+1].merge_with(batch, None, &mut self.logger);
                }
            }

            // Step 2: Insert new batch at target position.
            self.merging[batch_index].merge_with(batch, None, &mut self.logger);

            // Step 4: Consider migrating complete batches to lower bins, if appropriate.
            for index in (1 .. self.merging.len()).rev() {
                if self.merging[index-1].is_empty() {
                    if let Some(batch) = self.merging[index].could_reduce() {
                        self.merging[index-1].merge_with(batch, None, &mut self.logger);
                    }
                }
            }

            // Step 5: Delete any trailing empty levels.
            while self.merging.last().map(|x| x.is_empty()) == Some(true) { self.merging.pop(); }
        }
    }
}

// State for one "power of two" level.
struct MergeLevel<K, V, T, R, B: Batch<K, V, T, R>> {
    state: Option<MergeState<K, V, T, R, B>>,
    stash: Vec<B>,  // spare "empty" batches of size 1 << level.
    level: usize,   // contains batches of size at most 1 << level.
    op_id: usize,   // operator id (for logging).
}

enum MergeState<K, V, T, R, B: Batch<K, V, T, R>> {
    /// Represents two batchs, old then new, and their in-progress merge.
    Double(B, B, Option<Vec<T>>, <B as Batch<K,V,T,R>>::Merger),
    /// A single batch.
    Single(B),
}

impl<K, V, T: Eq, R, B: Batch<K, V, T, R>> MergeLevel<K, V, T, R, B> {
    /// Creates a new merge level.
    fn new(level: usize, op_id: usize) -> Self {
        MergeLevel {
            state: None,
            stash: Vec::new(),
            level,
            op_id,
        }
    }
    fn print(&self) {
        println!("status[{}]: {}", self.level, if self.state.is_none() { "None" } else { if self.is_single() { "Single" } else { "Double" }});
    }
    /// A reference to the merge state.
    fn merge_state(&self) -> &Option<MergeState<K, V, T, R, B>> {
        &self.state
    }
    /// True if the level holds two batches.
    fn is_double(&self) -> bool {
        if let Some(MergeState::Double(_,_,_,_)) = self.state { true } else { false }
    }
    /// True if the level holds one batch.
    fn is_single(&self) -> bool {
        if let Some(MergeState::Single(_)) = self.state { true } else { false }
    }
    /// True if the level holds no batch.
    fn is_empty(&self) -> bool { self.state.is_none() }
    /// Returns the batch if it is complete and larger than this slot calls for.
    fn could_reduce(&mut self) -> Option<B> {
        match self.state.take() {
            Some(MergeState::Single(x)) => {
                if x.len().next_power_of_two() < (1 << self.level) {
                    Some(x)
                }
                else {
                    self.state = Some(MergeState::Single(x));
                    None
                }
            },
            x => { self.state = x; None },
         }
    }
    /// Performs a merge with a "newer" batch.
    ///
    /// If the current merge is still in progress it is completed immediately.
    /// If the current merge is complete, a new merge is started.
    fn merge_with(&mut self, batch_new: B, frontier: Option<Vec<T>>, logger: &mut Option<::logging::Logger>) {

        // println!("entry {} merging! with {}", self.level, batch_new.len());

        match self.state.take() {
            Some(MergeState::Double(_,_,_,_)) => {
                panic!(format!("WHOAWTFNONONNONO: {}", self.level));
            },
            Some(MergeState::Single(batch_old)) => {

                logger.as_ref().map(|l| l.log(
                    MergeEvent::new(self.op_id, self.level, batch_old.len(), batch_new.len(), None)
                ));
                assert!(batch_old.upper() == batch_new.lower());
                // TODO: Steal stuff from our stash!
                let begin_merge = <B as Batch<K, V, T, R>>::begin_merge(&batch_old, &batch_new, &mut None);
                self.state = Some(MergeState::Double(batch_old, batch_new, frontier, begin_merge));
            },
            None => {
                self.state = Some(MergeState::Single(batch_new));
            },
        }
    }

    /// Completes any merge and returns the batch if it was a merge.
    fn complete(&mut self, logger: &mut Option<::logging::Logger>) -> Option<B> {
        let mut fuel = usize::max_value();
        let mut result = self.work(&mut fuel, logger);
        while fuel == 0 {
            fuel = usize::max_value();
            result = self.work(&mut fuel, logger);
        }
        if result.is_some() {
            result
        }
        else {
            if let Some(MergeState::Single(x)) = self.state.take() {
                Some(x)
            }
            else {
                None
            }
        }
    }

    /// Performs at most `fuel` work merging, returning any completed batch.
    ///
    /// If a merge completes the resulting batch is returned.
    fn work(&mut self, fuel: &mut usize, logger: &mut Option<::logging::Logger>) -> Option<B> {
        if let Some(merge) = self.state.take() {
            match merge {
                MergeState::Double(b1, b2, frontier, mut in_progress) => {
                    in_progress.work(&b1, &b2, &frontier, fuel);
                    if *fuel > 0 {
                        let done = in_progress.done();
                        logger.as_ref().map(|l|
                            l.log(MergeEvent::new(self.op_id, self.level, b1.len(), b2.len(), Some(done.len())))
                        );
                        // self.stash.push(b1);
                        // self.stash.push(b2);
                        self.state = None;
                        Some(done)
                    }
                    else {
                        self.state = Some(MergeState::Double(b1, b2, frontier, in_progress));
                        None
                    }
                },
                MergeState::Single(x) => {
                    self.state = Some(MergeState::Single(x));
                    None
                },
            }
        }
        else {
            None
        }
    }
}