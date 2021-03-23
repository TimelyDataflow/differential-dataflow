//! A general purpose `Batcher` implementation based on radix sort.

use timely::progress::frontier::Antichain;

use ::difference::Semigroup;

use lattice::Lattice;
use trace::{Batch, Batcher, Builder};

/// Creates batches from unordered tuples.
pub struct MergeBatcher<K: Ord, V: Ord, T: Ord, R: Semigroup, B: Batch<K, V, T, R>> {
    sorter: MergeSorter<(K, V), T, R>,
    lower: Antichain<T>,
    frontier: Antichain<T>,
    phantom: ::std::marker::PhantomData<B>,
}

impl<K, V, T, R, B> Batcher<K, V, T, R, B> for MergeBatcher<K, V, T, R, B>
where
    K: Ord+Clone,
    V: Ord+Clone,
    T: Lattice+timely::progress::Timestamp+Ord+Clone,
    R: Semigroup,
    B: Batch<K, V, T, R>,
{
    fn new() -> Self {
        MergeBatcher {
            sorter: MergeSorter::new(),
            frontier: Antichain::new(),
            lower: Antichain::from_elem(T::minimum()),
            phantom: ::std::marker::PhantomData,
        }
    }

    #[inline(never)]
    fn push_batch(&mut self, batch: &mut Vec<((K,V),T,R)>) {
        self.sorter.push(batch);
    }

    // Sealing a batch means finding those updates with times not greater or equal to any time
    // in `upper`. All updates must have time greater or equal to the previously used `upper`,
    // which we call `lower`, by assumption that after sealing a batcher we receive no more
    // updates with times not greater or equal to `upper`.
    #[inline(never)]
    fn seal(&mut self, upper: Antichain<T>) -> B {

        let mut builder = B::Builder::new();

        let mut merged = Vec::new();
        self.sorter.finish_into(&mut merged);

        let mut kept = Vec::new();
        let mut keep = Vec::new();

        self.frontier.clear();

        // TODO: Re-use buffer, rather than dropping.
        for mut buffer in merged.drain(..) {
            for ((key, val), time, diff) in buffer.drain(..) {
                if upper.less_equal(&time) {
                    // keep_count += 1;
                    self.frontier.insert(time.clone());
                    if keep.len() == keep.capacity() {
                        if keep.len() > 0 {
                            kept.push(keep);
                            keep = self.sorter.empty();
                        }
                    }
                    keep.push(((key, val), time, diff));
                }
                else {
                    // seal_count += 1;
                    builder.push((key, val, time, diff));
                }
            }
            // Recycling buffer.
            self.sorter.push(&mut buffer);
        }

        // Finish the kept data.
        if keep.len() > 0 {
            kept.push(keep);
        }
        if kept.len() > 0 {
            self.sorter.push_list(kept);
        }

        // Drain buffers (fast reclaimation).
        // TODO : This isn't obviously the best policy, but "safe" wrt footprint.
        //        In particular, if we are reading serialized input data, we may
        //        prefer to keep these buffers around to re-fill, if possible.
        let mut buffer = Vec::new();
        self.sorter.push(&mut buffer);
        // We recycle buffers with allocations (capacity, and not zero-sized).
        while buffer.capacity() > 0 && std::mem::size_of::<((K,V),T,R)>() > 0 {
            buffer = Vec::new();
            self.sorter.push(&mut buffer);
        }

        let seal = builder.done(self.lower.clone(), upper.clone(), Antichain::from_elem(T::minimum()));
        self.lower = upper;
        seal
    }

    // the frontier of elements remaining after the most recent call to `self.seal`.
    fn frontier(&mut self) -> timely::progress::frontier::AntichainRef<T> {
        self.frontier.borrow()
    }
}


use std::slice::{from_raw_parts};

pub struct VecQueue<T> {
    list: Vec<T>,
    head: usize,
    tail: usize,
}

impl<T> VecQueue<T> {
    #[inline]
    pub fn new() -> Self { VecQueue::from(Vec::new()) }
    #[inline]
    pub fn pop(&mut self) -> T {
        debug_assert!(self.head < self.tail);
        self.head += 1;
        unsafe { ::std::ptr::read(self.list.as_mut_ptr().offset((self.head as isize) - 1)) }
    }
    #[inline]
    pub fn peek(&self) -> &T {
        debug_assert!(self.head < self.tail);
        unsafe { self.list.get_unchecked(self.head) }
    }
    #[inline]
    pub fn _peek_tail(&self) -> &T {
        debug_assert!(self.head < self.tail);
        unsafe { self.list.get_unchecked(self.tail-1) }
    }
    #[inline]
    pub fn _slice(&self) -> &[T] {
        debug_assert!(self.head < self.tail);
        unsafe { from_raw_parts(self.list.get_unchecked(self.head), self.tail - self.head) }
    }
    #[inline]
    pub fn from(mut list: Vec<T>) -> Self {
        let tail = list.len();
        unsafe { list.set_len(0); }
        VecQueue {
            list: list,
            head: 0,
            tail: tail,
        }
    }
    // could leak, if self.head != self.tail.
    #[inline]
    pub fn done(self) -> Vec<T> {
        debug_assert!(self.head == self.tail);
        self.list
    }
    #[inline]
    pub fn len(&self) -> usize { self.tail - self.head }
    #[inline]
    pub fn is_empty(&self) -> bool { self.head == self.tail }
}

#[inline]
unsafe fn push_unchecked<T>(vec: &mut Vec<T>, element: T) {
    debug_assert!(vec.len() < vec.capacity());
    let len = vec.len();
    ::std::ptr::write(vec.get_unchecked_mut(len), element);
    vec.set_len(len + 1);
}

pub struct MergeSorter<D: Ord, T: Ord, R: Semigroup> {
    queue: Vec<Vec<Vec<(D, T, R)>>>,    // each power-of-two length list of allocations.
    stash: Vec<Vec<(D, T, R)>>,
}

impl<D: Ord, T: Ord, R: Semigroup> MergeSorter<D, T, R> {

    #[inline]
    pub fn new() -> Self { MergeSorter { queue: Vec::new(), stash: Vec::new() } }

    #[inline]
    pub fn empty(&mut self) -> Vec<(D, T, R)> {
        self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024))
    }

    #[inline(never)]
    pub fn _sort(&mut self, list: &mut Vec<Vec<(D, T, R)>>) {
        for mut batch in list.drain(..) {
            self.push(&mut batch);
        }
        self.finish_into(list);
    }

    #[inline]
    pub fn push(&mut self, batch: &mut Vec<(D, T, R)>) {
        // TODO: Reason about possible unbounded stash growth. How to / should we return them?
        // TODO: Reason about mis-sized vectors, from deserialized data; should probably drop.
        let mut batch = if self.stash.len() > 2 {
            ::std::mem::replace(batch, self.stash.pop().unwrap())
        }
        else {
            ::std::mem::replace(batch, Vec::new())
        };

        if batch.len() > 0 {
            crate::consolidation::consolidate_updates(&mut batch);
            self.queue.push(vec![batch]);
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
    pub fn push_list(&mut self, list: Vec<Vec<(D, T, R)>>) {
        while self.queue.len() > 1 && self.queue[self.queue.len()-1].len() < list.len() {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue.push(merged);
        }
        self.queue.push(list);
    }

    #[inline(never)]
    pub fn finish_into(&mut self, target: &mut Vec<Vec<(D, T, R)>>) {
        while self.queue.len() > 1 {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue.push(merged);
        }

        if let Some(mut last) = self.queue.pop() {
            ::std::mem::swap(&mut last, target);
        }
    }

    // merges two sorted input lists into one sorted output list.
    #[inline(never)]
    fn merge_by(&mut self, list1: Vec<Vec<(D, T, R)>>, list2: Vec<Vec<(D, T, R)>>) -> Vec<Vec<(D, T, R)>> {

        use std::cmp::Ordering;

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024));

        let mut list1 = VecQueue::from(list1);
        let mut list2 = VecQueue::from(list2);

        let mut head1 = if !list1.is_empty() { VecQueue::from(list1.pop()) } else { VecQueue::new() };
        let mut head2 = if !list2.is_empty() { VecQueue::from(list2.pop()) } else { VecQueue::new() };

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {

            while (result.capacity() - result.len()) > 0 && head1.len() > 0 && head2.len() > 0 {

                let cmp = {
                    let x = head1.peek();
                    let y = head2.peek();
                    (&x.0, &x.1).cmp(&(&y.0, &y.1))
                };
                match cmp {
                    Ordering::Less    => { unsafe { push_unchecked(&mut result, head1.pop()); } }
                    Ordering::Greater => { unsafe { push_unchecked(&mut result, head2.pop()); } }
                    Ordering::Equal   => {
                        let (data1, time1, mut diff1) = head1.pop();
                        let (_data2, _time2, diff2) = head2.pop();
                        diff1.plus_equals(&diff2);
                        if !diff1.is_zero() {
                            unsafe { push_unchecked(&mut result, (data1, time1, diff1)); }
                        }
                    }
                }
            }

            if result.capacity() == result.len() {
                output.push(result);
                result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024));
            }

            if head1.is_empty() {
                let done1 = head1.done();
                if done1.capacity() == 1024 { self.stash.push(done1); }
                head1 = if !list1.is_empty() { VecQueue::from(list1.pop()) } else { VecQueue::new() };
            }
            if head2.is_empty() {
                let done2 = head2.done();
                if done2.capacity() == 1024 { self.stash.push(done2); }
                head2 = if !list2.is_empty() { VecQueue::from(list2.pop()) } else { VecQueue::new() };
            }
        }

        if result.len() > 0 { output.push(result); }
        else if result.capacity() > 0 { self.stash.push(result); }

        if !head1.is_empty() {
            let mut result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024));
            for _ in 0 .. head1.len() { result.push(head1.pop()); }
            output.push(result);
        }
        while !list1.is_empty() {
            output.push(list1.pop());
        }

        if !head2.is_empty() {
            let mut result = self.stash.pop().unwrap_or_else(|| Vec::with_capacity(1024));
            for _ in 0 .. head2.len() { result.push(head2.pop()); }
            output.push(result);
        }
        while !list2.is_empty() {
            output.push(list2.pop());
        }

        output
    }
}

/// Reports the number of elements satisfing the predicate.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to
/// count the number of elements in time logarithmic in the result.
#[inline]
pub fn _advance<T, F: Fn(&T)->bool>(slice: &[T], function: F) -> usize {

    // start with no advance
    let mut index = 0;
    if index < slice.len() && function(&slice[index]) {

        // advance in exponentially growing steps.
        let mut step = 1;
        while index + step < slice.len() && function(&slice[index + step]) {
            index += step;
            step = step << 1;
        }

        // advance in exponentially shrinking steps.
        step = step >> 1;
        while step > 0 {
            if index + step < slice.len() && function(&slice[index + step]) {
                index += step;
            }
            step = step >> 1;
        }

        index += 1;
    }

    index
}
