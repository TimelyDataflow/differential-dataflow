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
                        let (cursor1, store1) = batch1.cursor();
                        cursors.push(cursor1);
                        storage.push(store1);
                        let (cursor2, store2) = batch2.cursor();
                        cursors.push(cursor2);
                        storage.push(store2);
                    },
                    MergeState::Complete(ref batch) => {
                        let (cursor, store) = batch.cursor();
                        cursors.push(cursor);
                        storage.push(store);
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
                    let (cursor, store) = batch.cursor();
                    cursors.push(cursor);
                    storage.push(store);
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

        // TODO: We could consider merging in batches here, rather than in sequence. 
        //       Little is currently known about whether this is important ...
        while self.pending.len() > 0 && 
              self.through_frontier.iter().all(|t1| self.pending[0].upper().iter().any(|t2| t2.less_equal(t1))) 
        {
            // this could be a VecDeque, if we ever notice this.
            let batch = self.pending.remove(0);

            let mut fuel = 1_000_000 * (self.merging.len()) * batch.len();

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
    }
}
