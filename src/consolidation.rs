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
use timely::Container;
use timely::container::{ContainerBuilder, PushInto, SizableContainer};
use crate::Data;
use crate::difference::{IsZero, Semigroup};

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
    R: Semigroup+'static,
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
    R: Semigroup+'static,
{
    type Container = Vec<(D,T,R)>;

    /// Push an element.
    ///
    /// Precondition: `current` is not allocated or has space for at least one element.
    #[inline]
    fn push<P>(&mut self, item: P) where Self::Container: PushInto<P> {
        let preferred_capacity = <Vec<(D,T,R)>>::preferred_capacity();
        if self.current.capacity() < preferred_capacity * 2 {
            self.current.reserve(preferred_capacity * 2 - self.current.capacity());
        }
        self.current.push_into(item);
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

/// Behavior to sort containers.
pub trait ContainerSorter<C> {
    /// Sort `container`, possibly replacing the contents by a different object.
    fn sort(&mut self, container: &mut C);
}

/// A generic container sorter for containers where the item implements [`ConsolidateLayout`].
pub struct ExternalContainerSorter<C: Container> {
    /// Storage to permute item.
    permutation: Vec<C::Item<'static>>,
    /// Empty container to write results at.
    empty: C,
}

impl<C> ContainerSorter<C> for ExternalContainerSorter<C>
where
    for<'a> C: Container + PushInto<C::Item<'a>>,
    for<'a> C::Item<'a>: ConsolidateLayout<C>,
{
    fn sort(&mut self, container: &mut C) {
        // SAFETY: `Permutation` is empty, types are equal but have a different lifetime
        let mut permutation: Vec<C::Item<'_>> = unsafe { std::mem::transmute::<Vec<C::Item<'static>>, Vec<C::Item<'_>>>(std::mem::take(&mut self.permutation)) };

        permutation.extend(container.drain());
        permutation.sort_by(|a, b| a.key().cmp(&b.key()));

        for item in permutation.drain(..) {
            self.empty.push(item);
        }

        // SAFETY: `Permutation` is empty, types are equal but have a different lifetime
        self.permutation = unsafe { std::mem::transmute::<Vec<C::Item<'_>>, Vec<C::Item<'static>>>(permutation) };
        std::mem::swap(container, &mut self.empty);
        self.empty.clear();
    }
}

/// Sort containers in-place, with specific implementations.
#[derive(Default, Debug)]
pub struct InPlaceSorter();

impl<T: Ord, R> ContainerSorter<Vec<(T, R)>> for InPlaceSorter {
    #[inline]
    fn sort(&mut self, container: &mut Vec<(T, R)>) {
        container.sort_by(|(a, _), (b, _)| a.cmp(b));
    }
}

impl<D: Ord, T: Ord, R> ContainerSorter<Vec<(D, T, R)>> for InPlaceSorter {
    #[inline]
    fn sort(&mut self, container: &mut Vec<(D, T, R)>) {
        container.sort_by(|(d1, t1, _), (d2, t2, _)| (d1, t1).cmp(&(d2, t2)));
    }
}

/// Layout of data to be consolidated. Generic over containers to enable `push`.
pub trait ConsolidateLayout<C> {
    /// Key to consolidate and sort.
    type Key<'a>: Ord where Self: 'a;

    /// Owned diff type.
    type DiffOwned: Semigroup;

    /// Get a key.
    fn key(&self) -> Self::Key<'_>;

    /// Get an owned diff.
    fn diff(&self) -> Self::DiffOwned;

    /// Add the diff of the current item into `target`.
    fn diff_plus_equals(&self, target: &mut Self::DiffOwned);

    /// Push an element to a compatible container.
    fn push(self, diff: Self::DiffOwned, target: &mut C);
}

impl<D: Ord, R: Semigroup + Clone> ConsolidateLayout<Vec<(D,R)>> for (D, R) {
    type Key<'a> = &'a D where Self: 'a;
    type DiffOwned = R;

    #[inline]
    fn key(&self) -> Self::Key<'_> {
        &self.0
    }

    #[inline]
    fn diff(&self) -> Self::DiffOwned {
        self.1.clone()
    }

    #[inline]
    fn diff_plus_equals(&self, target: &mut Self::DiffOwned) {
        target.plus_equals(&self.1);
    }

    #[inline]
    fn push(self, diff: Self::DiffOwned, target: &mut Vec<(D, R)>) {
        target.push((self.0, diff));
    }
}

impl<D: Ord, T: Ord, R: Semigroup + Clone> ConsolidateLayout<Vec<(D, T, R)>> for (D, T, R) {
    type Key<'a> = (&'a D, &'a T) where Self: 'a;
    type DiffOwned = R;

    #[inline]
    fn key(&self) -> Self::Key<'_> {
        (&self.0, &self.1)
    }

    #[inline]
    fn diff(&self) -> Self::DiffOwned {
        self.2.clone()
    }

    #[inline]
    fn diff_plus_equals(&self, target: &mut Self::DiffOwned) {
        target.plus_equals(&self.2);
    }

    #[inline]
    fn push(self, diff: Self::DiffOwned, target: &mut Vec<(D, T, R)>) {
        target.push((self.0, self.1, diff));
    }
}

/// Behavior for copying consolidation.
pub trait ConsolidateContainer<C> {
    /// Consolidate the contents of `container` and write the result to `target`.
    fn consolidate_container(container: &mut C, target: &mut C);
}

/// Container consolidator that requires the container's item to implement [`ConsolidateLayout`].
#[derive(Default, Debug)]
pub struct ContainerConsolidator{
}

impl<C> ConsolidateContainer<C> for ContainerConsolidator
where
    C: Container,
    for<'a> C::Item<'a>: ConsolidateLayout<C>,
{
    /// Consolidate the supplied container.
    fn consolidate_container(container: &mut C, target: &mut C)
    {
        let mut previous: Option<(C::Item<'_>, <C::Item<'_> as ConsolidateLayout<C>>::DiffOwned)> = None;
        for item in container.drain() {
            if let Some((previtem, d)) = &mut previous {
                // Second and following interation.
                if item.key() == previtem.key() {
                    item.diff_plus_equals(d);
                } else {
                    // Keys don't match, write down result if non-zero.
                    if !d.is_zero() {
                        // Unwrap because we checked for `Some` above.
                        let (pitem, diff) = previous.take().unwrap();
                        <C::Item<'_> as ConsolidateLayout<C>>::push(pitem, diff, target);
                    }
                    // Update `previous`
                    let diff = item.diff();
                    previous = Some((item, diff));
                }
            } else {
                // Initial iteration.
                let diff = item.diff();
                previous = Some((item, diff));
            }
        }
        // Write any residual data.
        if let Some((previtem, d)) = previous {
            if !d.is_zero() {
                <C::Item<'_> as ConsolidateLayout<C>>::push(previtem, d, target);
            }
        }
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

    #[test]
    fn test_consolidate_container() {
        let mut data = vec![(1,1), (2, 1), (1, -1)];
        let mut target = Vec::default();
        data.sort();
        ContainerConsolidator::consolidate_container(&mut data, &mut target);
        assert_eq!(target, [(2,1)]);
    }

    #[cfg(not(debug_assertions))]
    const LEN: usize = 256 << 10;
    #[cfg(not(debug_assertions))]
    const REPS: usize = 10 << 10;

    #[cfg(debug_assertions)]
    const LEN: usize = 256 << 1;
    #[cfg(debug_assertions)]
    const REPS: usize = 10 << 1;

    #[test]
    fn test_consolidator_duration() {
        let mut data = Vec::with_capacity(LEN);
        let mut data2 = Vec::with_capacity(LEN);
        let mut target = Vec::new();
        let mut duration = std::time::Duration::default();
        for _ in 0..REPS {
            data.clear();
            data2.clear();
            target.clear();
            data.extend((0..LEN).map(|i| (i/4, -2isize + ((i % 4) as isize))));
            data2.extend((0..LEN).map(|i| (i/4, -2isize + ((i % 4) as isize))));
            data.sort_by(|x,y| x.0.cmp(&y.0));
            let start = std::time::Instant::now();
            ContainerConsolidator::consolidate_container(&mut data, &mut target);
            duration += start.elapsed();

            consolidate(&mut data2);
            assert_eq!(target, data2);
        }
        println!("elapsed consolidator {duration:?}");
    }

    #[test]
    fn test_consolidator_duration_vec() {
        let mut data = Vec::with_capacity(LEN);
        let mut duration = std::time::Duration::default();
        for _ in 0..REPS {
            data.clear();
            data.extend((0..LEN).map(|i| (i/4, -2isize + ((i % 4) as isize))));
            data.sort_by(|x,y| x.0.cmp(&y.0));
            let start = std::time::Instant::now();
            consolidate(&mut data);
            duration += start.elapsed();
        }
        println!("elapsed vec {duration:?}");
    }
}
