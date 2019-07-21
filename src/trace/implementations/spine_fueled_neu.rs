//! An append-only collection of update batches.
//!
//! The `Spine` is a general-purpose trace implementation based on collection and merging
//! immutable batches of updates. It is generic with respect to the batch type, and can be
//! instantiated for any implementor of `trace::Batch`.

use std::fmt::Debug;

use ::difference::Monoid;
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
pub struct Spine<K, V, T: Lattice+Ord, R: Monoid, B: Batch<K, V, T, R>> {
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
    R: Monoid,
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
    R: Monoid,
    B: Batch<K, V, T, R>+Clone+'static,
{

    fn new(
        info: ::timely::dataflow::operators::generic::OperatorInfo,
        logging: Option<::logging::Logger>,
        activator: Option<timely::scheduling::activate::Activator>,
    ) -> Self {
        Self::with_effort(4, info, logging, activator)
    }

    fn exert(&mut self, batch_size: usize, batch_index: usize) {
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

impl<K, V, T, R, B> Spine<K, V, T, R, B>
where
    K: Ord+Clone,
    V: Ord+Clone,
    T: Lattice+Ord+Clone+Debug+Default,
    R: Monoid,
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

        while self.pending.len() > 0 &&
              self.through_frontier.iter().all(|t1| self.pending[0].upper().iter().any(|t2| t2.less_equal(t1)))
        {
            // this could be a VecDeque, if we ever notice this.
            let batch = self.pending.remove(0);

            self.introduce_batch(batch, batch.len().next_power_of_two());

            // Having performed all of our work, if more than one batch remains reschedule ourself.
            if self.merging.len() > 2 && self.merging[..self.merging.len()-1].iter().any(|b| b.present()) {
                self.activator.activate();
            }
        }
    }

    /// Introduces a batch at an indicated level.
    ///
    /// The level indication is often related to the size of the batch, but
    /// it can also be used to artificially fuel the computation by supplying
    /// empty batches at non-trivial indices, to move merges along.
    pub fn introduce_batch(&mut self, batch: B, batch_index: usize) {

        // This spine is represented as a list of layers, where each element in the list is either
        //
        //   1. MergeState::Empty   empty
        //   2. MergeState::Single  a single batch
        //   3. MergeState::Double  : a pair of batches
        //
        // Each of the batches at layer i contains at most 2^i elements. The sequence of batches
        // should have the upper bound of one match the lower bound of the next. Batches may be
        // logically empty, with matching upper and lower bounds, as a bookkeeping mechanism.
        //
        // We attempt to maintain the invariant that no two adjacent levels have pairs of batches.
        // This invariant exists to make sure we have the breathing room to initiate but not
        // immediately complete merges. As soon as a layer contains two batches we initiate a merge
        // into a batch of the next sized layer, and we start to apply fuel to this merge. When it
        // completes, we install the newly merged batch in the next layer and uninstall the two
        // batches from their layer.
        //
        // We a merge completes, it vacates an entire level. Assuming we have maintained our invariant,
        // the number of updates below that level (say "k") is at most
        //
        //         2^k-1 + 2*2^k-2 + 2^k-3 + 2*2^k-4 + ...
        //      <=
        //         2^k+1 - 2^k-1
        //
        // Fortunately, the now empty layer k needs 2^k+1 updates before it will initiate a new merge,
        // and the collection must accept 2^k-1 updates before such a merge could possibly be initiated.
        // This means that if each introduced merge applies a proportionate amount of fuel to the merge,
        // we should be able to complete any merge at the next level before a new one is proposed.
        //
        // I believe most of this reasoning holds under the properties of the invariant that no adjacent
        // layers have two batches. This gives us the ability to do further housekeeping, especially as
        // we work with the largest layers, which we might expect not to grow in size. If at any point we
        // can tidy the largest layer downward without violating the invariant (e.g. draw it down to an
        // empty layer, or to a singleton layer and commence a merge) we should consider that in the
        // interests of maintaining a compact representation that does not grow without bounds.

        // Step 2. Apply fuel to each in-progress merge.
        //
        //         Ideally we apply fuel to the merges of the smallest
        //         layers first, as progress on these layers is important,
        //         and the sooner it happens the sooner we have fewer layers
        //         for cursors to navigate, which improves read performance.
        //
        //          It is important that by the end of this, we have ensured
        //          that position `batch_index+1` has at most a single batch,
        //          as our roll-up strategy will insert into that position.
        self.apply_fuel(self.effort << batch_index);

        // Step 1. Before installing the batch we must ensure the invariant,
        //         that no adjacent layers contain two batches. We can make
        //         this happen by forcibly completing all lower merges and
        //         integrating them with the batch itself, which can go at
        //         level index+1 as a single batch.
        //
        //         Note that this corresponds to the introduction of some
        //         volume of fake updates, and we will need to fuel merges
        //         by a proportional amount to ensure that they are not
        //         surprised later on.
        self.roll_up(batch_index);

        // Step 4. This insertion should be into an empty layer. It is a
        //         logical error otherwise, as we may be violating our
        //         invariant, from which all derives.
        self.insert_at(batch, batch_index);

        // Step 3. Tidy the largest layers.
        //
        //         It is important that we not tidy only smaller layers,
        //         as their ascension is what ensures the merging and
        //         eventual compaction of the largest layers.
        self.tidy_layers();
    }

    /// Ensures that layers up through and including `index` are empty.
    ///
    /// This method is used to prepare for the insertion of a single batch
    /// at `index`, which should maintain the invariant.
    fn roll_up(&mut self, index: usize) {

        let merge =
        self.merging[.. index+1]
            .iter_mut()
            .fold(None, |merge, level|
                match (merge, level.complete()) {
                    (Some(batch_new), Some(batch_old)) => {
                        Some(MergeState::begin_merge(batch_old, batch_new, None).complete())
                    },
                    (None, batch) => batch,
                    (merge, None) => merge,
                }
            );

        if let Some(batch) = merge {
            self.insert_at(batch, index + 1);
        }
    }

    /// Applies an amount of fuel to merges in progress.
    pub fn apply_fuel(mut fuel: usize) {

        // Scale fuel up by number of in-progress merges, or by the length of the spine.
        // We just need to make sure that the issued fuel is applied to each in-progress
        // merge, by the time its layer is needed to accept a new merge.

        fuel *= self.merging.len();
        for index in 0 .. self.merging.len() {
            if let Some(batch) = self.merging.work(&mut fuel) {
                self.insert_at(batch, index+1);
            }
        }
    }

    /// Inserts a batch at a specific location.
    ///
    /// This is a non-public internal method that can panic if we try and insert into a
    /// layer which already contains two batches (and is in the process of merging).
    fn insert_at(&mut self, batch: B, index: usize) {
        while self.merging.len() <= index {
            self.merging.push(MergeState::Empty);
        }
        let frontier = if index == self.merging.len()-1 { Some(self.advance_frontier.clone()) } else { None };
        self.merging[index].insert(batch, frontier);
    }

    /// Attempts to draw down large layers to size appropriate layers.
    fn tidy_layers(&mut self) {

        // If the largest layer is complete (not merging), we can attempt
        // to draw it down to the next layer if either that layer is empty,
        // or if it is a singleton and the layer below it is not merging.
        // We expect this should happen at various points if we have enough
        // fuel rolling around.

        let done = false;
        while !done {



        }

    }

}


/// Describes the state of a layer.
///
/// A layer can be empty, contain a single batch, or contain a pair of batches that are in the process
/// of merging into a batch for the next layer.
enum MergeState<K, V, T, R, B: Batch<K, V, T, R>> {
    /// An empty layer, containing no updates.
    Empty,
    /// A layer containing a single batch.
    Single(B),
    /// A layer containing two batch, in the process of merging.
    Double(B, B, Option<Vec<T>>, <B as Batch<K,V,T,R>>::Merger),
}

impl<K, V, T: Eq, R, B: Batch<K, V, T, R>> MergeState<K, V, T, R, B> {

    /// Immediately complete any merge.
    ///
    /// This consumes the layer, though we should probably consider returning the resources of the
    /// underlying source batches if we can manage that.
    fn complete(&mut self) -> Option<B>  {
        match std::mem::replace(self, MergeState::Empty) {
            Empty => None,
            Single(batch) => Some(batch),
            Double(b1, b2, frontier, merge) => {
                let mut fuel = usize::max_value();
                in_progress.work(source1, source2, frontier, &mut fuel);
                assert!(fuel > 0);
                let finished = finished.done();
                // logger.as_ref().map(|l|
                //     l.log(::logging::MergeEvent {
                //         operator,
                //         scale,
                //         length1: b1.len(),
                //         length2: b2.len(),
                //         complete: Some(finished.len()),
                //     })
                // );
                Some(finished)
            },
        }
    }

    fn work(&mut self, fuel: &mut usize) -> Option<B> {
        match std::mem::replace(self, MergeState::Empty) {
            MergeState::Double(b1, b2, frontier, merge) => {
                merge.work(b1, b2, frontier, fuel);
                if fuel > 0 {
                    let finished = finished.done();
                    // logger.as_ref().map(|l|
                    //     l.log(::logging::MergeEvent {
                    //         operator,
                    //         scale,
                    //         length1: b1.len(),
                    //         length2: b2.len(),
                    //         complete: Some(finished.len()),
                    //     })
                    // );
                    Some(finished)
                }
            }
            _ => None,
        }
    }

    /// Extract the merge state, often temporarily.
    fn take(&mut self) -> MergeState {
        std::mem::replace(self, MergeState::Empty)
    }

    /// Inserts a batch and begins a merge if needed.
    fn insert(&mut self, batch: B, frontier: Option<Vec<T>>) {
        match self.take() {
            Empty => { *self = MergeState::Single(batch); },
            Single(batch_old) => {
                *self = MergeState::begin_merge(batch_old, batch, frontier);
            }
            Double(_,_,_,_) => {
                panic!("Attempted to insert batch into incomplete merge!");
            }
        };
    }

    fn is_complete(&self) -> bool {
        match *self {
            MergeState::Complete(_) => true,
            _ => false,
        }
    }
    fn begin_merge(batch1: B, batch2: B, frontier: Option<Vec<T>>) -> Self {
        // logger.as_ref().map(|l| l.log(
        //     ::logging::MergeEvent {
        //         operator,
        //         scale,
        //         length1: batch1.len(),
        //         length2: batch2.len(),
        //         complete: None,
        //     }
        // ));
        assert!(batch1.upper() == batch2.lower());
        let begin_merge = <B as Batch<K, V, T, R>>::begin_merge(&batch1, &batch2);
        MergeState::Merging(batch1, batch2, frontier, begin_merge)
    }
    fn work(mut self, fuel: &mut usize) -> Self {
        if let MergeState::Merging(ref source1, ref source2, ref frontier, ref mut in_progress) = self {
            in_progress.work(source1, source2, frontier, fuel);
        }
        if *fuel > 0 {
            match self {
                // ALLOC: Here is where we may de-allocate batches.
                MergeState::Merging(b1, b2, _, finished) => {
                    let finished = finished.done();
                    // logger.as_ref().map(|l|
                    //     l.log(::logging::MergeEvent {
                    //         operator,
                    //         scale,
                    //         length1: b1.len(),
                    //         length2: b2.len(),
                    //         complete: Some(finished.len()),
                    //     })
                    // );
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