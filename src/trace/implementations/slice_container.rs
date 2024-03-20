//! A container optimized for slices.

use crate::trace::implementations::BatchContainer;
use crate::trace::implementations::OffsetList;

/// A slice container with four bytes overhead per slice.
pub struct StableSliceContainer<T> {
    batches: Vec<SliceBatch<T>>,
}

impl<T: Ord+Clone+'static> BatchContainer for StableSliceContainer<T> {
    type PushItem = Vec<T>;
    type ReadItem<'a> = &'a [T];

    fn push(&mut self, mut item: Self::PushItem) {
        if let Some(batch) = self.batches.last_mut() {
            let success = batch.try_push(&mut item);
            if !success {
                let mut new_batch = SliceBatch::with_capacity(std::cmp::max(2 * batch.storage.capacity(), item.len()));
                assert!(new_batch.try_push(&mut item));
                self.batches.push(new_batch);
            }
        }
    }

    fn copy(&mut self, item: Self::ReadItem<'_>) {
        if let Some(batch) = self.batches.last_mut() {
            let success = batch.try_copy(item);
            if !success {
                let mut new_batch = SliceBatch::with_capacity(std::cmp::max(2 * batch.storage.capacity(), item.len()));
                assert!(new_batch.try_copy(item));
                self.batches.push(new_batch);
            }
        }
    }

    fn with_capacity(size: usize) -> Self {
        Self {
            batches: vec![SliceBatch::with_capacity(size)],
        }
    }

    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        Self {
            batches: vec![SliceBatch::with_capacity(cont1.len() + cont2.len())],
        }
    }

    fn index(&self, mut index: usize) -> Self::ReadItem<'_> {
        for batch in self.batches.iter() {
            if index < batch.len() {
                return batch.index(index);
            }
            index -= batch.len();
        }
        panic!("Index out of bounds");
    }

    fn len(&self) -> usize {
        let mut result = 0;
        for batch in self.batches.iter() {
            result += batch.len();
        }
        result
    }
}

/// A batch of slice storage.
///
/// The backing storage for this batch will not be resized.
pub struct SliceBatch<T> {
    offsets: OffsetList,
    storage: Vec<T>,
}

impl<T: Ord+Clone+'static> SliceBatch<T> {
    /// Either accepts the slice and returns true, 
    /// or does not and returns false.
    fn try_push(&mut self, slice: &mut Vec<T>) -> bool {
        if self.storage.len() + slice.len() <= self.storage.capacity() {
            self.storage.extend(slice.drain(..));
            self.offsets.push(self.storage.len());
            true
        }
        else {
            false
        }
    }
    /// Either accepts the slice and returns true, 
    /// or does not and returns false.
    fn try_copy(&mut self, slice: &[T]) -> bool {
        if self.storage.len() + slice.len() <= self.storage.capacity() {
            self.storage.extend(slice.iter().cloned());
            self.offsets.push(self.storage.len());
            true
        }
        else {
            false
        }
    }
    fn index(&self, index: usize) -> &[T] {
        let lower = self.offsets.index(index);
        let upper = self.offsets.index(index + 1);
        &self.storage[lower .. upper]
    }
    fn len(&self) -> usize { self.offsets.len() - 1 }

    fn with_capacity(cap: usize) -> Self {
        let mut offsets = OffsetList::with_capacity(cap + 1);
        offsets.push(0);
        Self {
            offsets,
            storage: Vec::with_capacity(cap),
        }
    }
}