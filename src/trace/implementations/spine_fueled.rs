//! An append-only collection of update batches.
//!
//! The `Spine` is a general-purpose trace implementation based on collection and merging 
//! immutable batches of updates. It is generic with respect to the batch type, and can be 
//! instantiated for any implementor of `trace::Batch`.

use ::Diff;
use lattice::Lattice;
use trace::{Batch, BatchReader, Trace, TraceReader};
use trace::cursor::cursor_list::CursorList;
use trace::cursor::Cursor;
use trace::Merger;

enum MergeState<K, V, T, R, B: Batch<K, V, T, R>> {
    Merging(B, B, Option<Vec<T>>, <B as Batch<K,V,T,R>>::Merger),
    Complete(B),
}

impl<K, V, T: Eq, R, B: Batch<K, V, T, R>> MergeState<K, V, T, R, B> {
    fn complete(mut self) -> B {
        if let MergeState::Merging(ref source1, ref source2, ref frontier, ref mut in_progress) = self {
            let mut fuel = usize::max_value();
            in_progress.work(source1, source2, frontier, &mut fuel);
            assert!(fuel > 0);
        }
        match self {
            // ALLOC: Here is where we may de-allocate batches.
            MergeState::Merging(_, _, _, finished) => finished.done(),
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
    fn work(mut self, fuel: &mut usize) -> Self {
        if let MergeState::Merging(ref source1, ref source2, ref frontier, ref mut in_progress) = self {
            in_progress.work(source1, source2, frontier, fuel);
        }
        if *fuel > 0 {
            match self {
                // ALLOC: Here is where we may de-allocate batches.
                MergeState::Merging(_, _, _, finished) => MergeState::Complete(finished.done()),
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
pub struct Spine<K, V, T: Lattice+Ord, R: Diff, B: Batch<K, V, T, R>> {
    phantom: ::std::marker::PhantomData<(K, V, R)>,
    advance_frontier: Vec<T>,            // Times after which the trace must accumulate correctly.
    through_frontier: Vec<T>,            // Times after which the trace must be able to subset its inputs.
    merging: Vec<Option<MergeState<K,V,T,R,B>>>, // Several possibly shared collections of updates.
    pending: Vec<B>,                     // Batches at times in advance of `frontier`.
    // max_len: usize,
    // max_dur: ::std::time::Duration,
    // reserve: usize,                      // fuel reserves.
}

impl<K, V, T, R, B> TraceReader<K, V, T, R> for Spine<K, V, T, R, B> 
where 
    K: Ord+Clone,           // Clone is required by `batch::advance_*` (in-place could remove).
    V: Ord+Clone,           // Clone is required by `batch::advance_*` (in-place could remove).
    T: Lattice+Ord+Clone,   // Clone is required by `advance_by` and `batch::advance_*`.
    R: Diff,
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
                match *merge_state {
                    Some(MergeState::Merging(ref batch1, ref batch2, _, _)) => { 
                        cursors.push(batch1.cursor());
                        storage.push(batch1.clone());
                        cursors.push(batch2.cursor());
                        storage.push(batch2.clone());
                    },
                    Some(MergeState::Complete(ref batch)) => {
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
impl<K, V, T, R, B> Trace<K, V, T, R> for Spine<K, V, T, R, B> 
where 
    K: Ord+Clone,           // Clone is required by `batch::advance_*` (in-place could remove).
    V: Ord+Clone,           // Clone is required by `batch::advance_*` (in-place could remove).
    T: Lattice+Ord+Clone,   // Clone is required by `advance_by` and `batch::advance_*`.
    R: Diff,
    B: Batch<K, V, T, R>+Clone+'static,
{

    fn new() -> Self {
        Spine { 
            phantom: ::std::marker::PhantomData,
            advance_frontier: vec![<T as Lattice>::minimum()],
            through_frontier: vec![<T as Lattice>::minimum()],
            merging: Vec::new(),
            pending: Vec::new(),
            // max_len: 0,
            // max_dur: Default::default(),
            // reserve: 0,
        }
    }

    // Ideally, this method acts as insertion of `batch`, even if we are not yet able to begin
    // merging the batch. This means it is a good time to perform amortized work proportional
    // to the size of batch.
    fn insert(&mut self, batch: Self::Batch) {

        // we can ignore degenerate batches (TODO: learn where they come from; suppress them?)
        if batch.lower() != batch.upper() {
            self.pending.push(batch);
            self.consider_merges();
        }
        else {
            // degenerate batches had best be empty.
            assert!(batch.len() == 0);
        }
    }
}

impl<K, V, T, R, B> Spine<K, V, T, R, B> 
where 
    K: Ord+Clone,           // Clone is required by `advance_mut`.
    V: Ord+Clone,           // Clone is required by `advance_mut`.
    T: Lattice+Ord+Clone,   // Clone is required by `advance_mut`.
    R: Diff,
    B: Batch<K, V, T, R>,
{
    // Migrate data from `self.pending` into `self.merging`.
    #[inline(never)]
    fn consider_merges(&mut self) {

        // We have a new design here, in an attempt to rationalize progressive merging of batches.
        //
        // Batches arrive with a number of records, and are assigned a power-of-two "size", which is this
        // number rounded up. Batches are placed in a slot based on their size, and each slot can either be
        // 
        //  i. empty,
        //  ii. occupied by a batch,
        //  iii. occupied by a merge in progress.
        //
        // As we introduce a new batch, we need to do a few things.
        //
        //  0. We determine a size for the batch, and an implied target slot.
        //  1. For each slot smaller than the size of the batch, we should merge the batches so that the first
        //     occupied slot is no less than the target slot of the new batch.
        //  2. We perform size units of work for each merge in progress, starting from the large batches and
        //     working backwards (so that we have had the chance to complete work before starting a new merge).
        //  3. We install the new batch in the target slot, initiating a merge if necessary.
        //
        // At various points we may find that the number of updates in a batch is out of line with the size
        // associated with the slot the batch currently occupies. In this case, we can slide a batch (or a merge
        // in progress) down through empty slots, as long as the number of updates is no more than the associated
        // size.

        while self.pending.len() > 0 && 
              self.through_frontier.iter().all(|t1| self.pending[0].upper().iter().any(|t2| t2.less_equal(t1))) 
        {
            // this could be a VecDeque, if we ever notice this.
            let batch = self.pending.remove(0);

            // println!("batch.len(): {:?}", batch.len());

            // Step 0: Determine batch size and target slot.
            let batch_size = batch.len().next_power_of_two();
            let batch_index = batch_size.trailing_zeros() as usize;
            while self.merging.len() <= batch_index { self.merging.push(None); }

            if self.merging.len() > 32 { println!("len: {:?}", self.merging.len()); }

            // Step 1: Forcibly merge batches in lower slots.
            for position in 0 .. batch_index {
                if let Some(batch) = self.merging[position].take() {
                    let batch = batch.complete();
                    if let Some(batch2) = self.merging[position+1].take() {
                        let batch2 = batch2.complete();
                        self.merging[position+1] = Some(MergeState::begin_merge(batch2, batch, None));
                    }
                    else {
                        self.merging[position+1] = Some(MergeState::Complete(batch));
                    };
                }
            }

            // Step 2: Perform `size` work on each in-progress merge, from large to small.
            let mut fuel;// = 0; //8 * batch_size * self.merging.len();
            for position in (batch_index .. self.merging.len()).rev() {
                fuel = 16 * batch_size;
                if let Some(batch) = self.merging[position].take() {
                    let batch = batch.work(&mut fuel);
                    if batch.is_complete() {
                        let batch = batch.complete();
                        let intended_position = batch.len().next_power_of_two().trailing_zeros() as usize;
                        if intended_position > position {
                            while self.merging.len() <= intended_position { self.merging.push(None); }
                            if let Some(batch2) = self.merging[position+1].take() {
                                if !batch2.is_complete() {
                                    println!("batch[{}] not complete (size: {:?})", position+1, batch2.len());
                                    println!("sizes: {:?}", self.merging.iter().map(|x| x.as_ref().map(|y|y.len()).unwrap_or(0)).collect::<Vec<_>>());
                                }
                                assert!(batch2.is_complete());
                                let batch2 = batch2.complete();
                                let frontier = if position + 1 == self.merging.len() { Some(self.advance_frontier.clone()) } else { None };
                                self.merging[position+1] = Some(MergeState::begin_merge(batch2, batch, frontier));
                            }
                            else {
                                self.merging[position+1] = Some(MergeState::Complete(batch));
                            };
                        }
                        else {
                            self.merging[position] = Some(MergeState::Complete(batch));
                        }
                    }
                    else {
                        self.merging[position] = Some(batch);
                    }
                }
            }

            // Step 3: Insert new batch at target position
            if let Some(batch2) = self.merging[batch_index].take() {
                assert!(batch2.is_complete());
                let batch2 = batch2.complete();
                self.merging[batch_index] = Some(MergeState::begin_merge(batch2, batch, None));
            }
            else {
                self.merging[batch_index] = Some(MergeState::Complete(batch));
            }

            // Step 4: Consider migrating complete batches to lower bins, if appropriate.
            for index in (self.merging.len() .. 1).rev() {
                if self.merging[index].as_ref().map(|x| x.is_complete() && x.len() < (1 << (index-1))).unwrap_or(false) {
                    if self.merging[index-1].is_none() {
                        self.merging[index-1] = self.merging[index].take();
                    }
                }
            }
        }
    }
}