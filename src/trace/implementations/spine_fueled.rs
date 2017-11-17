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
    Merging(B, B, <B as Batch<K,V,T,R>>::Merger),
    Complete(B),
}

impl<K, V, T: Eq, R, B: Batch<K, V, T, R>> MergeState<K, V, T, R, B> {
    fn is_complete(&self) -> bool {
        match *self {
            MergeState::Complete(_) => true,
            _ => false,
        }
    }
    fn begin_merge(batch1: B, batch2: B) -> Self {
        assert!(batch1.upper() == batch2.lower());
        let begin_merge = <B as Batch<K, V, T, R>>::begin_merge(&batch1, &batch2);
        MergeState::Merging(batch1, batch2, begin_merge)
    }
    fn work(mut self, fuel: &mut usize) -> Self {
        if let MergeState::Merging(_, _, ref mut in_progress) = self {
            in_progress.work(fuel);
        }
        if *fuel > 0 {
            match self {
                // ALLOC: Here is where we may de-allocate batches.
                MergeState::Merging(_, _, finished) => MergeState::Complete(finished.done()),
                MergeState::Complete(x) => MergeState::Complete(x),
            }
        }
        else { self }
    }
    fn len(&mut self) -> usize {
        match *self {
            MergeState::Merging(ref batch1, ref batch2, _) => batch1.len() + batch2.len(),
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
    merging: Vec<MergeState<K,V,T,R,B>>, // Several possibly shared collections of updates.
    pending: Vec<B>,                     // Batches at times in advance of `frontier`.
    max_len: usize,
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

            for merge_state in self.merging.iter() {
                match *merge_state {
                    MergeState::Merging(ref batch1, ref batch2, _) => { 
                        cursors.push(batch1.cursor());
                        storage.push(batch1.clone());
                        cursors.push(batch2.cursor());
                        storage.push(batch2.clone());
                    },
                    MergeState::Complete(ref batch) => {
                        cursors.push(batch.cursor());
                        storage.push(batch.clone());
                    }
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
        for batch in self.merging.iter() {
            match *batch {
                MergeState::Merging(ref batch1, ref batch2, _) => { f(batch1); f(batch2); },
                MergeState::Complete(ref batch) => { f(batch); },
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
            max_len: 0,
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

        // This method performs incremental merging of batches, so that we never *need* to spend a 
        // great deal of effort merging, but instead perform enough work to make progress on each 
        // of the merges, so that by the time the batch is needed for a subsequent merge its own
        // merge has completed. Done correctly, this should give us the same sequence of merges, 
        // just completing them in perhaps different orders. 
        //
        // The reference merge pattern, from the eager implementation, is that we maintain batches 
        // of geometrically decreasing size; when a new batch is introduced, we first merge all 
        // batches smaller than the recently introduced batch, then repeatedly merge the smallest
        // batch with the new batch as long as it (the smallest batch) does not contain twice the
        // number of elements in the accumulated new batch (including elements merged with it).
        //
        // This reference merge pattern should maintain the invariant of at most a logarithmic 
        // number of batches (because the number of updates halve with each batch), which should 
        // leads to an amortized cost of log n for each inserted tuple (analysis missing).
        //
        // Our amortized implementation will start merges and then perform some amount of work on
        // started merges. We want some discipline to ensure that we are able to complete each 
        // started merge before we require the results again for a new merge from below (as this
        // would require us to enqueue the more recent batch, at which point we risk growing 
        // beyond our log n bound, at least without an alternate analysis).
        //
        // To maintain strict adherence to the referecne merge pattern, we need additional states
        // in addition to "merge in progress" and "merge complete": it seems we need additionally
        // "merge complete but awaiting completion of prior merge", for merges that would consider
        // merging with a larger batch that results from an as-yet incomplete merge. Perhaps we can
        // rig the distribution of effort so that such never happens, but if not we want the ability
        // to reject merges with smaller batches if we might depart from the reference merge pattern.
        // 
        // A question is now "how should we distribute effort" among the in-progress merges? 
        // 
        // 1. Should we always work on the oldest merge, performing the work in the same order as it
        //    would be done in the reference merge? No, as this would leave a pile of recently added
        //    small batches awaiting merge effort, whose length increases unboundedly.
        //
        // 2. Should we always work on the most recent merges, minimizing the number of outstanding 
        //    merges at any time (if not the amount of un-merged data)? Perhaps, though we must keep
        //    some discipline about merge pattern (e.g. the reference pattern) to avoid merging and 
        //    growing the smallest batches, violating the geometric decrease invariant. 
        //    
        //    If we cleave strongly to the reference merge pattern, we may have a hard time starting 
        //    new merges, as any incomplete merge could result in an arbitrarily small result, and 
        //    need to be merged with the next batch before any subsequent batch is merged with it.
        //    We may require a different discipline to allow concurrent or out-of-order merges to 
        //    occur.
        //
        // Perhaps we should consider merging by "number of tuples pre-cancelation", which have the
        // delightful property that they only increase, and should less often block the merging of 
        // subsequent batches, but which have the defect that by growing unboundedly the "log n" 
        // bound we have also grows without bound, even as the accumluated differences stabilize.
        //
        // We could at various moments "correct" these estimates, drawing them down to at least twice
        // the estimate of the size of the next batch, which would allow the estimates to stabilize
        // as the sizes stabilize. Alternately, we could treat the size of any batch as at least twice
        // the size of the next smaller batch, until the smaller batch is merged. It seems delicate
        // to reason about the behavior here; delicate, but possible.

        // TODO: We could consider merging in batches here, rather than in sequence. 
        //       Little is currently known about whether this is important ...
        while self.pending.len() > 0 && 
              self.through_frontier.iter().all(|t1| self.pending[0].upper().iter().any(|t2| t2.less_equal(t1))) 
        {
            // this could be a VecDeque, if we ever notice this.
            let batch = self.pending.remove(0);

            let mut fuel = 8 * (self.merging.len()) * batch.len();

            // First, we want to complete any merges of batches smaller than `batch`.
            if self.merging.len() > 1 {

                let mut most_recent = self.merging.pop().unwrap();
                
                while self.merging.last_mut().map(|x| x.len() < batch.len()) == Some(true) {

                    let mut less_recent = self.merging.pop().unwrap();
                    most_recent = most_recent.work(&mut fuel);
                    less_recent = less_recent.work(&mut fuel);

                    match (less_recent, most_recent) {
                        (MergeState::Complete(less), MergeState::Complete(most)) => {
                            assert!(less.upper() == most.lower());
                            most_recent = MergeState::begin_merge(less, most);
                        },
                        _ => panic!("unmerged small data discovered; logic bug!"),
                    }
                }

                self.merging.push(most_recent);
            }

            let mut most_recent = MergeState::Complete(batch);

            while fuel > 0 && self.merging.last_mut().map(|x| x.len() < 2 * most_recent.len()) == Some(true) {

                let mut less_recent = self.merging.pop().unwrap();
                most_recent = most_recent.work(&mut fuel);
                less_recent = less_recent.work(&mut fuel);

                match (less_recent, most_recent) {

                    // TODO: This starts a merge here, even if the merge that "should" start is 
                    //       between `less_recent` and `even_less_recent`, for example when they
                    //       differ in size by less than a factor of two. This could draw out the
                    //       length of self.merging, and violate "factor of two" invariants, which
                    //       are important for performance.

                    (MergeState::Complete(mut less), MergeState::Complete(mut most)) => {
                        assert!(less.upper() == most.lower());
                        if self.merging.len() == 0 {
                            less.advance_mut(&self.advance_frontier[..]);
                            most.advance_mut(&self.advance_frontier[..]);
                        }
                        assert!(less.upper() == most.lower());
                        most_recent = MergeState::begin_merge(less, most);
                    }
                    (less, most) => { 
                        // Can't merge; stash `less` and assume stash `most` next round.
                        assert!(fuel == 0);
                        most_recent = most;
                        self.merging.push(less);
                    }
                }
            }

            self.merging.push(most_recent);

            // Spend any remaining fuel.
            for index in (0 .. self.merging.len()).rev() {
                if fuel > 0 {
                    let temp = self.merging.remove(index);
                    self.merging.insert(index, temp.work(&mut fuel));
                }
            }

            // Scan forward, looking for possible merges to start.
            let mut index = 1;
            while index < self.merging.len() {
                if self.merging[index-1].len() < 2 * self.merging[index].len() && self.merging[index-1].is_complete() && self.merging[index].is_complete() {
                    let less_recent = self.merging.remove(index-1);
                    let more_recent = self.merging.remove(index-1);
                    match (less_recent, more_recent) {
                        (MergeState::Complete(less), MergeState::Complete(more)) => {
                            self.merging.insert(index - 1, MergeState::begin_merge(less, more));
                        },
                        _ => panic!("unreachable"),
                    }
                }
                else {
                    index += 1;
                }
            }

            // TODO: We *may* still have fuel and could work on new merges.

            // if self.merging.len() > 32 {
            //     println!("len: {:?}", self.merging.len());
            //     for batch in self.merging.iter_mut() {
            //         match *batch {
            //             MergeState::Merging(ref mut x, ref mut y, _) => { println!("  len({}, {})", x.len(), y.len()); },
            //             MergeState::Complete(ref mut x) => { println!("  len({})", x.len()); },
            //         }
            //     }
            // }
        }

        if self.merging.len() + self.pending.len() > self.max_len {
            self.max_len = self.merging.len() + self.pending.len();
            println!("max_len increased to {:?}", self.max_len);
        }
    }
}
