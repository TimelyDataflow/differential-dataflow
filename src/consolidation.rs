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

use std::cmp::Ordering;
use std::collections::VecDeque;
use timely::Container;
use timely::container::{ContainerBuilder, PushInto, SizableContainer};
use timely::container::flatcontainer::{FlatStack, Push, Region};
use timely::container::flatcontainer::impls::tuple::{TupleABCRegion, TupleABRegion};
use crate::Data;
use crate::difference::{IsZero, Semigroup};
use crate::trace::cursor::IntoOwned;

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
    #[cold]
    fn consolidate_and_flush_through(&mut self, multiple: usize) {
        let preferred_capacity = <Vec<(D,T,R)>>::preferred_capacity();
        consolidate_updates(&mut self.current);
        let mut drain = self.current.drain(..(self.current.len()/multiple)*multiple).peekable();
        while drain.peek().is_some() {
            let mut container = self.empty.pop().unwrap_or_else(|| Vec::with_capacity(preferred_capacity));
            container.clear();
            container.extend((&mut drain).take(preferred_capacity));
            self.outbound.push_back(container);
        }
    }
}

impl<D, T, R, P> PushInto<P> for ConsolidatingContainerBuilder<Vec<(D, T, R)>>
where
    D: Data,
    T: Data,
    R: Semigroup+'static,
    Vec<(D, T, R)>: PushInto<P>,
{
    /// Push an element.
    ///
    /// Precondition: `current` is not allocated or has space for at least one element.
    #[inline]
    fn push_into(&mut self, item: P) {
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
}

impl<D,T,R> ContainerBuilder for ConsolidatingContainerBuilder<Vec<(D, T, R)>>
where
    D: Data,
    T: Data,
    R: Semigroup+'static,
{
    type Container = Vec<(D,T,R)>;

    #[inline]
    fn extract(&mut self) -> Option<&mut Vec<(D,T,R)>> {
        if let Some(container) = self.outbound.pop_front() {
            self.empty.push(container);
            self.empty.last_mut()
        } else {
            None
        }
    }

    #[inline]
    fn finish(&mut self) -> Option<&mut Vec<(D,T,R)>> {
        if !self.current.is_empty() {
            // Flush all
            self.consolidate_and_flush_through(1);
            // Remove all but two elements from the stash of empty to avoid memory leaks. We retain
            // two to match `current` capacity.
            self.empty.truncate(2);
        }
        self.extract()
    }
}

/// Layout of containers and their read items to be consolidated.
///
/// This trait specifies behavior to extract keys and diffs from container's read
/// items. Consolidation accumulates the diffs per key.
///
/// The trait requires `Container` to have access to its `Item` GAT.
pub trait ConsolidateLayout: Container {
    /// Key portion of data, essentially everything minus the diff
    type Key<'a>: Eq where Self: 'a;

    /// GAT diff type.
    type Diff<'a>: IntoOwned<'a, Owned = Self::DiffOwned> where Self: 'a;

    /// Owned diff type.
    type DiffOwned: for<'a> Semigroup<Self::Diff<'a>>;

    /// Deconstruct an item into key and diff. Must be cheap.
    fn into_parts(item: Self::Item<'_>) -> (Self::Key<'_>, Self::Diff<'_>);

    /// Push an element to a compatible container.
    ///
    /// This function is odd to have, so let's explain why it exists. Ideally, the container
    /// would accept a `(key, diff)` pair and we wouldn't need this function. However, we
    /// might never be in a position where this is true: Vectors can push any `T`, which would
    /// collide with a specific implementation for pushing tuples of mixes GATs and owned types.
    ///
    /// For this reason, we expose a function here that takes a GAT key and an owned diff, and
    /// leave it to the implementation to "patch" a suitable item that can be pushed into `self`.
    fn push_with_diff(&mut self, key: Self::Key<'_>, diff: Self::DiffOwned);

    /// Compare two items by key to sort containers.
    fn cmp(item1: &Self::Item<'_>, item2: &Self::Item<'_>) -> Ordering;
}

impl<D, T, R> ConsolidateLayout for Vec<(D, T, R)>
where
    D: Ord + Clone + 'static,
    T: Ord + Clone + 'static,
    for<'a> R: Semigroup + IntoOwned<'a, Owned = R> + Clone + 'static,
{
    type Key<'a> = (D, T) where Self: 'a;
    type Diff<'a> = R where Self: 'a;
    type DiffOwned = R;

    fn into_parts((data, time, diff): Self::Item<'_>) -> (Self::Key<'_>, Self::Diff<'_>) {
        ((data, time), diff)
    }

    fn cmp<'a>(item1: &Self::Item<'_>, item2: &Self::Item<'_>) -> Ordering {
        (&item1.0, &item1.1).cmp(&(&item2.0, &item2.1))
    }

    fn push_with_diff(&mut self, (data, time): Self::Key<'_>, diff: Self::DiffOwned) {
        self.push((data, time, diff));
    }
}

impl<K, V, T, R> ConsolidateLayout for FlatStack<TupleABCRegion<TupleABRegion<K, V>, T, R>>
where
    for<'a> K: Region + Push<<K as Region>::ReadItem<'a>> + Clone + 'static,
    for<'a> K::ReadItem<'a>: Ord + Copy,
    for<'a> V: Region + Push<<V as Region>::ReadItem<'a>> + Clone + 'static,
    for<'a> V::ReadItem<'a>: Ord + Copy,
    for<'a> T: Region + Push<<T as Region>::ReadItem<'a>> + Clone + 'static,
    for<'a> T::ReadItem<'a>: Ord + Copy,
    R: Region + Push<<R as Region>::Owned> + Clone + 'static,
    for<'a> R::Owned: Semigroup<R::ReadItem<'a>>,
{
    type Key<'a> = (K::ReadItem<'a>, V::ReadItem<'a>, T::ReadItem<'a>) where Self: 'a;
    type Diff<'a> = R::ReadItem<'a> where Self: 'a;
    type DiffOwned = R::Owned;

    fn into_parts(((key, val), time, diff): Self::Item<'_>) -> (Self::Key<'_>, Self::Diff<'_>) {
        ((key, val, time), diff)
    }

    fn cmp<'a>(((key1, val1), time1, _diff1): &Self::Item<'_>, ((key2, val2), time2, _diff2): &Self::Item<'_>) -> Ordering {
        (K::reborrow(*key1), V::reborrow(*val1), T::reborrow(*time1)).cmp(&(K::reborrow(*key2), V::reborrow(*val2), T::reborrow(*time2)))
    }

    fn push_with_diff(&mut self, (key, value, time): Self::Key<'_>, diff: Self::DiffOwned) {
        self.copy(((key, value), time, diff));
    }
}

/// Consolidate the supplied container.
pub fn consolidate_container<C: ConsolidateLayout>(container: &mut C, target: &mut C) {
    // Sort input data
    let mut permutation = Vec::new();
    permutation.extend(container.drain());
    permutation.sort_by(|a, b| C::cmp(a, b));

    // Consolidate sorted data.
    let mut previous: Option<(C::Key<'_>, C::DiffOwned)> = None;
    // TODO: We should ensure that `target` has sufficient capacity, but `Container` doesn't
    // offer a suitable API.
    for item in permutation.drain(..) {
        let (key, diff) = C::into_parts(item);
        match &mut previous {
            // Initial iteration, remeber key and diff.
            // TODO: Opportunity for GatCow for diff.
            None => previous = Some((key, diff.into_owned())),
            Some((prevkey, d)) => {
                // Second and following iteration, compare and accumulate or emit.
                if key == *prevkey {
                    // Keys match, keep accumulating.
                    d.plus_equals(&diff);
                } else {
                    // Keys don't match, write down result if non-zero.
                    if !d.is_zero() {
                        // Unwrap because we checked for `Some` above.
                        let (prevkey, diff) = previous.take().unwrap();
                        target.push_with_diff(prevkey, diff);
                    }
                    // Remember current key and diff as `previous`
                    previous = Some((key, diff.into_owned()));
                }
            }
        }
    }
    // Write any residual data, if non-zero.
    if let Some((previtem, d)) = previous {
        if !d.is_zero() {
            target.push_with_diff(previtem, d);
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
            ccb.push_into((0, 0, 0));
        }
        assert_eq!(ccb.extract(), None);
        assert_eq!(ccb.finish(), None);

        for i in 0..1024 {
            ccb.push_into((i, 0, 1));
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
    }

    #[test]
    fn test_consolidate_container() {
        let mut data = vec![(1, 1, 1), (2, 1, 1), (1, 1, -1)];
        let mut target = Vec::default();
        data.sort();
        consolidate_container(&mut data, &mut target);
        assert_eq!(target, [(2, 1, 1)]);
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
            data.extend((0..LEN).map(|i| (i/4, 1, -2isize + ((i % 4) as isize))));
            data2.extend((0..LEN).map(|i| (i/4, 1, -2isize + ((i % 4) as isize))));
            data.sort_by(|x,y| x.0.cmp(&y.0));
            let start = std::time::Instant::now();
            consolidate_container(&mut data, &mut target);
            duration += start.elapsed();

            consolidate_updates(&mut data2);
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
            data.extend((0..LEN).map(|i| (i/4, 1, -2isize + ((i % 4) as isize))));
            data.sort_by(|x,y| x.0.cmp(&y.0));
            let start = std::time::Instant::now();
            consolidate_updates(&mut data);
            duration += start.elapsed();
        }
        println!("elapsed vec {duration:?}");
    }
}
