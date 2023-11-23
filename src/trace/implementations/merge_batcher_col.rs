//! A general purpose `Batcher` implementation based on radix sort for TimelyStack.

use std::marker::PhantomData;
use timely::Container;
use timely::communication::message::RefOrMut;
use timely::container::columnation::{Columnation, TimelyStack};
use timely::progress::frontier::Antichain;

use ::difference::Semigroup;

use trace::{Batcher, Builder};
use trace::implementations::Update;

/// Creates batches from unordered tuples.
pub struct ColumnatedMergeBatcher<U: Update>
where
    U::Key: Columnation,
    U::Val: Columnation,
    U::Time: Columnation,
    U::Diff: Columnation,
{
    sorter: MergeSorterColumnation<(U::Key, U::Val), U::Time, U::Diff>,
    lower: Antichain<U::Time>,
    frontier: Antichain<U::Time>,
    phantom: PhantomData<U>,
}

impl<U: Update> Batcher for ColumnatedMergeBatcher<U>
where
    U::Key: Columnation + 'static,
    U::Val: Columnation + 'static,
    U::Time: Columnation + 'static,
    U::Diff: Columnation + 'static,
{
    type Item = ((U::Key,U::Val),U::Time,U::Diff);
    type Time = U::Time;

    fn new() -> Self {
        ColumnatedMergeBatcher {
            sorter: MergeSorterColumnation::new(),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(<U::Time as timely::progress::Timestamp>::minimum()),
            phantom: PhantomData,
        }
    }

    #[inline]
    fn push_batch(&mut self, batch: RefOrMut<Vec<Self::Item>>) {
        // `batch` is either a shared reference or an owned allocations.
        match batch {
            RefOrMut::Ref(reference) => {
                // This is a moment at which we could capture the allocations backing
                // `batch` into a different form of region, rather than just  cloning.
                self.sorter.push(&mut reference.clone());
            },
            RefOrMut::Mut(reference) => {
                self.sorter.push(reference);
            }
        }
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    #[inline]
    fn seal<B: Builder<Item=Self::Item, Time=Self::Time>>(&mut self, upper: Antichain<U::Time>) -> B::Output {

        let mut builder = B::new();

        let mut merged = Default::default();
        self.sorter.finish_into(&mut merged);

        let mut kept = Vec::new();
        let mut keep = TimelyStack::default();

        self.frontier.clear();

        for buffer in merged.drain(..) {
            for datum @ ((_key, _val), time, _diff) in &buffer[..] {
                if upper.less_equal(time) {
                    self.frontier.insert(time.clone());
                    if !keep.is_empty() && keep.len() == keep.capacity() {
                        kept.push(keep);
                        keep = self.sorter.empty();
                    }
                    keep.copy(datum);
                }
                else {
                    builder.copy(datum);
                }
            }
            // Recycling buffer.
            self.sorter.recycle(buffer);
        }

        // Finish the kept data.
        if !keep.is_empty() {
            kept.push(keep);
        }
        if !kept.is_empty() {
            self.sorter.push_list(kept);
        }

        // Drain buffers (fast reclamation).
        self.sorter.clear_stash();

        let seal = builder.done(self.lower.clone(), upper.clone(), Antichain::from_elem(<U::Time as timely::progress::Timestamp>::minimum()));
        self.lower = upper;
        seal
    }

    // the frontier of elements remaining after the most recent call to `self.seal`.
    fn frontier(&mut self) -> timely::progress::frontier::AntichainRef<U::Time> {
        self.frontier.borrow()
    }
}

struct TimelyStackQueue<T: Columnation> {
    list: TimelyStack<T>,
    head: usize,
}

impl<T: Columnation> Default for TimelyStackQueue<T> {
    fn default() -> Self {
        Self::from(Default::default())
    }
}

impl<T: Columnation> TimelyStackQueue<T> {

    fn pop(&mut self) -> &T {
        self.head += 1;
        &self.list[self.head - 1]
    }

    fn peek(&self) -> &T {
        &self.list[self.head]
    }

    fn from(list: TimelyStack<T>) -> Self {
        TimelyStackQueue {
            list,
            head: 0,
        }
    }

    fn done(self) -> TimelyStack<T> {
        self.list
    }

    fn is_empty(&self) -> bool { self.head == self.list[..].len() }

    /// Return an iterator over the remaining elements.
    fn iter(&self) -> impl Iterator<Item=&T> + Clone + ExactSizeIterator {
        self.list[self.head..].iter()
    }
}

struct MergeSorterColumnation<D: Ord+Columnation, T: Ord+Columnation, R: Semigroup+Columnation> {
    queue: Vec<Vec<TimelyStack<(D, T, R)>>>,    // each power-of-two length list of allocations.
    stash: Vec<TimelyStack<(D, T, R)>>,
    pending: Vec<(D, T, R)>,
}

impl<D: Ord+Clone+Columnation+'static, T: Ord+Clone+Columnation+'static, R: Semigroup+Columnation+'static> MergeSorterColumnation<D, T, R> {

    const BUFFER_SIZE_BYTES: usize = 64 << 10;

    /// Buffer size (number of elements) to use for new/empty buffers.
    const fn buffer_size() -> usize {
        let size = std::mem::size_of::<(D, T, R)>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    /// Buffer size for pending updates, currently 2 * [`Self::buffer_size`].
    const fn pending_buffer_size() -> usize {
        Self::buffer_size() * 2
    }

    fn new() -> Self {
        Self {
            queue: Vec::new(),
            stash: Vec::new(),
            pending: Vec::new()
        }
    }

    fn empty(&mut self) -> TimelyStack<(D, T, R)> {
        self.stash.pop().unwrap_or_else(|| TimelyStack::with_capacity(Self::buffer_size()))
    }

    /// Remove all elements from the stash.
    fn clear_stash(&mut self) {
        self.stash.clear();
    }

    /// Insert an empty buffer into the stash. Panics if the buffer is not empty.
    fn recycle(&mut self, mut buffer: TimelyStack<(D, T, R)>) {
        if buffer.capacity() == Self::buffer_size() && self.stash.len() <= 2 {
            buffer.clear();
            self.stash.push(buffer);
        }
    }

    fn push(&mut self, batch: &mut Vec<(D, T, R)>) {
        // Ensure `self.pending` has a capacity of `Self::pending_buffer_size`.
        if self.pending.capacity() < Self::pending_buffer_size() {
            self.pending.reserve(Self::pending_buffer_size() - self.pending.capacity());
        }

        while !batch.is_empty() {
            self.pending.extend(batch.drain(..std::cmp::min(batch.len(), self.pending.capacity() - self.pending.len())));
            if self.pending.len() == self.pending.capacity() {
                crate::consolidation::consolidate_updates(&mut self.pending);
                if self.pending.len() > self.pending.capacity() / 2 {
                    // Flush if `self.pending` is more than half full after consolidation.
                    self.flush_pending();
                }
            }
        }
    }

    /// Move all elements in `pending` into `queue`. The data in `pending` must be compacted and
    /// sorted. After this function returns, `self.pending` is empty.
    fn flush_pending(&mut self) {
        if !self.pending.is_empty() {
            let mut stack = self.empty();
            stack.reserve_items(self.pending.iter());
            for tuple in self.pending.drain(..) {
                stack.copy(&tuple);
            }
            self.queue.push(vec![stack]);
            while self.queue.len() > 1 && (self.queue[self.queue.len()-1].len() >= self.queue[self.queue.len()-2].len() / 2) {
                let list1 = self.queue.pop().unwrap();
                let list2 = self.queue.pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue.push(merged);
            }
        }
    }

    // This is awkward, because it isn't a power-of-two length any more, and we don't want
    // to break it down to be so.
    fn push_list(&mut self, list: Vec<TimelyStack<(D, T, R)>>) {
        while self.queue.len() > 1 && self.queue[self.queue.len()-1].len() < list.len() {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue.push(merged);
        }
        self.queue.push(list);
    }

    fn finish_into(&mut self, target: &mut Vec<TimelyStack<(D, T, R)>>) {
        crate::consolidation::consolidate_updates(&mut self.pending);
        self.flush_pending();
        while self.queue.len() > 1 {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue.push(merged);
        }

        if let Some(mut last) = self.queue.pop() {
            std::mem::swap(&mut last, target);
        }
    }

    // merges two sorted input lists into one sorted output list.
    fn merge_by(&mut self, list1: Vec<TimelyStack<(D, T, R)>>, list2: Vec<TimelyStack<(D, T, R)>>) -> Vec<TimelyStack<(D, T, R)>> {

        use std::cmp::Ordering;

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = self.empty();

        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut head1 = TimelyStackQueue::from(list1.next().unwrap_or_default());
        let mut head2 = TimelyStackQueue::from(list2.next().unwrap_or_default());

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {

            while (result.capacity() - result.len()) > 0 && !head1.is_empty() && !head2.is_empty() {

                let cmp = {
                    let x = head1.peek();
                    let y = head2.peek();
                    (&x.0, &x.1).cmp(&(&y.0, &y.1))
                };
                match cmp {
                    Ordering::Less    => { result.copy(head1.pop()); }
                    Ordering::Greater => { result.copy(head2.pop()); }
                    Ordering::Equal   => {
                        let (data1, time1, diff1) = head1.pop();
                        let (_data2, _time2, diff2) = head2.pop();
                        let mut diff1 = diff1.clone();
                        diff1.plus_equals(diff2);
                        if !diff1.is_zero() {
                            result.copy_destructured(data1, time1, &diff1);
                        }
                    }
                }
            }

            if result.capacity() == result.len() {
                output.push(result);
                result = self.empty();
            }

            if head1.is_empty() {
                self.recycle(head1.done());
                head1 = TimelyStackQueue::from(list1.next().unwrap_or_default());
            }
            if head2.is_empty() {
                self.recycle(head2.done());
                head2 = TimelyStackQueue::from(list2.next().unwrap_or_default());
            }
        }

        if result.len() > 0 {
            output.push(result);
        } else {
            self.recycle(result);
        }

        if !head1.is_empty() {
            let mut result = self.empty();
            result.reserve_items(head1.iter());
            for item in head1.iter() { result.copy(item); }
            output.push(result);
        }
        output.extend(list1);

        if !head2.is_empty() {
            let mut result = self.empty();
            result.reserve_items(head2.iter());
            for item in head2.iter() { result.copy(item); }
            output.push(result);
        }
        output.extend(list2);

        output
    }
}
