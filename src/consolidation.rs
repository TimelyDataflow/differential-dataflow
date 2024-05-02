//! Common logic for the consolidation of vectors of Semigroups.
//!
//! Often we find ourselves with collections of records with associated weights (often
//! integers) where we want to reduce the collection to the point that each record occurs
//! at most once, with the accumulated weights. These methods supply that functionality.
//!
//! Importantly, these methods are used internally by differential dataflow, but are made
//! public for the convenience of others. Their precise behavior is driven by the needs of
//! differential dataflow (chiefly: canonicalizing sequences of non-zero updates); should
//! you need specific behavior, it may be best to defensively copy, paste, and maintain the
//! specific behavior you require.

use std::collections::VecDeque;
use timely::container::{ContainerBuilder, PushContainer, PushInto};
use crate::Data;
use crate::difference::Semigroup;

/// Sorts and consolidates `vec`.
///
/// This method will sort `vec` and then consolidate runs of more than one entry with
/// identical first elements by accumulating the second elements of the pairs. Should the final
/// accumulation be zero, the element is discarded.
pub fn consolidate<T: Ord, R: Semigroup>(vec: &mut Vec<(T, R)>) {
    consolidate_from(vec, 0);
}

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs of more than one entry with
/// identical first elements by accumulating the second elements of the pairs. Should the final
/// accumulation be zero, the element is discarded.
pub fn consolidate_from<T: Ord, R: Semigroup>(vec: &mut Vec<(T, R)>, offset: usize) {
    let length = consolidate_slice(&mut vec[offset..]);
    vec.truncate(offset + length);
}

/// Sorts and consolidates a slice, returning the valid prefix length.
pub fn consolidate_slice<T: Ord, R: Semigroup>(slice: &mut [(T, R)]) -> usize {

    if slice.len() > 1 {

        // We could do an insertion-sort like initial scan which builds up sorted, consolidated runs.
        // In a world where there are not many results, we may never even need to call in to merge sort.
        slice.sort_by(|x,y| x.0.cmp(&y.0));

        // Counts the number of distinct known-non-zero accumulations. Indexes the write location.
        let mut offset = 0;
        let mut accum = slice[offset].1.clone();

        for index in 1 .. slice.len() {
            if slice[index].0 == slice[index-1].0 {
                accum.plus_equals(&slice[index].1);
            }
            else {
                if !accum.is_zero() {
                    slice.swap(offset, index-1);
                    slice[offset].1.clone_from(&accum);
                    offset += 1;
                }
                accum.clone_from(&slice[index].1);
            }
        }
        if !accum.is_zero() {
            slice.swap(offset, slice.len()-1);
            slice[offset].1 = accum;
            offset += 1;
        }

        offset
    }
    else {
        slice.iter().filter(|x| !x.1.is_zero()).count()
    }
}

/// Sorts and consolidates `vec`.
///
/// This method will sort `vec` and then consolidate runs of more than one entry with
/// identical first two elements by accumulating the third elements of the triples. Should the final
/// accumulation be zero, the element is discarded.
pub fn consolidate_updates<D: Ord, T: Ord, R: Semigroup>(vec: &mut Vec<(D, T, R)>) {
    consolidate_updates_from(vec, 0);
}

/// Sorts and consolidate `vec[offset..]`.
///
/// This method will sort `vec[offset..]` and then consolidate runs of more than one entry with
/// identical first two elements by accumulating the third elements of the triples. Should the final
/// accumulation be zero, the element is discarded.
pub fn consolidate_updates_from<D: Ord, T: Ord, R: Semigroup>(vec: &mut Vec<(D, T, R)>, offset: usize) {
    let length = consolidate_updates_slice(&mut vec[offset..]);
    vec.truncate(offset + length);
}

/// Sorts and consolidates a slice, returning the valid prefix length.
pub fn consolidate_updates_slice<D: Ord, T: Ord, R: Semigroup>(slice: &mut [(D, T, R)]) -> usize {

    if slice.len() > 1 {

        // We could do an insertion-sort like initial scan which builds up sorted, consolidated runs.
        // In a world where there are not many results, we may never even need to call in to merge sort.
        slice.sort_unstable_by(|x,y| (&x.0, &x.1).cmp(&(&y.0, &y.1)));

        // Counts the number of distinct known-non-zero accumulations. Indexes the write location.
        let mut offset = 0;
        let mut accum = slice[offset].2.clone();

        for index in 1 .. slice.len() {
            if (slice[index].0 == slice[index-1].0) && (slice[index].1 == slice[index-1].1) {
                accum.plus_equals(&slice[index].2);
            }
            else {
                if !accum.is_zero() {
                    slice.swap(offset, index-1);
                    slice[offset].2.clone_from(&accum);
                    offset += 1;
                }
                accum.clone_from(&slice[index].2);
            }
        }
        if !accum.is_zero() {
            slice.swap(offset, slice.len()-1);
            slice[offset].2 = accum;
            offset += 1;
        }

        offset
    }
    else {
        slice.iter().filter(|x| !x.2.is_zero()).count()
    }
}


/// A container builder that consolidates data in-places into fixed-sized containers. Does not
/// maintain FIFO ordering.
#[derive(Default)]
pub struct ConsolidatingContainerBuilder<C>{
    current: C,
    empty: Vec<C>,
    outbound: VecDeque<C>,
}

impl<D,T,R> ConsolidatingContainerBuilder<Vec<(D, T, R)>>
where
    D: Data,
    T: Data,
    R: Semigroup,
{
    /// Flush `self.current` up to the biggest `multiple` of elements. Pass 1 to flush all elements.
    // TODO: Can we replace `multiple` by a bool?
    fn consolidate_and_flush_through(&mut self, multiple: usize) {
        let preferred_capacity = <Vec<(D,T,R)>>::preferred_capacity();
        consolidate_updates(&mut self.current);
        let mut drain = self.current.drain(..(self.current.len()/multiple)*multiple).peekable();
        while drain.peek().is_some() {
            let mut container = self.empty.pop().unwrap_or_else(|| Vec::with_capacity(preferred_capacity));
            container.extend((&mut drain).take(preferred_capacity));
            self.outbound.push_back(container);
        }
    }
}

impl<D,T,R> ContainerBuilder for ConsolidatingContainerBuilder<Vec<(D, T, R)>>
where
    D: Data,
    T: Data,
    R: Semigroup,
{
    type Container = Vec<(D,T,R)>;

    /// Push an element.
    ///
    /// Precondition: `current` is not allocated or has space for at least one element.
    #[inline]
    fn push<P: PushInto<Self::Container>>(&mut self, item: P) {
        let preferred_capacity = <Vec<(D,T,R)>>::preferred_capacity();
        if self.current.capacity() < preferred_capacity * 2 {
            self.current.reserve(preferred_capacity * 2 - self.current.capacity());
        }
        item.push_into(&mut self.current);
        if self.current.len() == self.current.capacity() {
            // Flush complete containers.
            self.consolidate_and_flush_through(preferred_capacity);
        }
    }

    fn push_container(&mut self, container: &mut Self::Container) {
        for item in container.drain(..) {
            self.push(item);
        }
    }

    fn extract(&mut self) -> Option<&mut Vec<(D,T,R)>> {
        if let Some(container) = self.outbound.pop_front() {
            self.empty.push(container);
            self.empty.last_mut()
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Vec<(D,T,R)>> {
        // Flush all
        self.consolidate_and_flush_through(1);
        // Remove all but two elements from the stash of empty to avoid memory leaks. We retain
        // two to match `current` capacity.
        self.empty.truncate(2);
        self.extract()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consolidate() {
        let test_cases = vec![
            (
                vec![("a", -1), ("b", -2), ("a", 1)],
                vec![("b", -2)],
            ),
            (
                vec![("a", -1), ("b", 0), ("a", 1)],
                vec![],
            ),
            (
                vec![("a", 0)],
                vec![],
            ),
            (
                vec![("a", 0), ("b", 0)],
                vec![],
            ),
            (
                vec![("a", 1), ("b", 1)],
                vec![("a", 1), ("b", 1)],
            ),
        ];

        for (mut input, output) in test_cases {
            consolidate(&mut input);
            assert_eq!(input, output);
        }
    }


    #[test]
    fn test_consolidate_updates() {
        let test_cases = vec![
            (
                vec![("a", 1, -1), ("b", 1, -2), ("a", 1, 1)],
                vec![("b", 1, -2)],
            ),
            (
                vec![("a", 1, -1), ("b", 1, 0), ("a", 1, 1)],
                vec![],
            ),
            (
                vec![("a", 1, 0)],
                vec![],
            ),
            (
                vec![("a", 1, 0), ("b", 1, 0)],
                vec![],
            ),
            (
                vec![("a", 1, 1), ("b", 2, 1)],
                vec![("a", 1, 1), ("b", 2, 1)],
            ),
        ];

        for (mut input, output) in test_cases {
            consolidate_updates(&mut input);
            assert_eq!(input, output);
        }
    }

    #[test]
    fn test_consolidating_container_builder() {
        let mut ccb = <ConsolidatingContainerBuilder<Vec<(usize, usize, usize)>>>::default();
        for _ in 0..1024 {
            ccb.push((0, 0, 0));
        }
        assert_eq!(ccb.extract(), None);
        assert_eq!(ccb.finish(), None);

        for i in 0..1024 {
            ccb.push((i, 0, 1));
        }

        let mut collected = Vec::default();
        while let Some(container) = ccb.finish() {
            collected.append(container);
        }
        // The output happens to be sorted, but it's not guaranteed.
        collected.sort();
        for i in 0..1024 {
            assert_eq!((i, 0, 1), collected[i]);
        }

        ccb = Default::default();
        ccb.push_container(&mut Vec::default());
        assert_eq!(ccb.extract(), None);
        assert_eq!(ccb.finish(), None);

        ccb.push_container(&mut Vec::from_iter((0..1024).map(|i| (i, 0, 1))));
        ccb.push_container(&mut Vec::from_iter((0..1024).map(|i| (i, 0, 1))));
        collected.clear();
        while let Some(container) = ccb.finish() {
            collected.append(container);
        }
        // The output happens to be sorted, but it's not guaranteed.
        consolidate_updates(&mut collected);
        for i in 0..1024 {
            assert_eq!((i, 0, 2), collected[i]);
        }
        
    }
}
