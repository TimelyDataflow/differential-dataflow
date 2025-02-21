//! A columnar container based on the columnation library.

use std::iter::FromIterator;
use std::rc::Rc;

use columnation::{Columnation, Region};
use differential_dataflow::trace::implementations::containers::BatchIndex;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatch, OrdKeyBuilder, OrdValBatch, OrdValBuilder};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::{BatchContainer, Layout, OffsetList, Update};
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
use timely::container::PushInto;

mod merge_batcher;
mod chunker;

pub use merge_batcher::ColMerger;
pub use chunker::ColumnationChunker;

/// A layout based on timely stacks
pub struct TStack<U: Update> {
    phantom: std::marker::PhantomData<U>,
}

impl<U: Update> Layout for TStack<U>
where
    U::Key: Columnation,
    U::Val: Columnation,
    U::Time: Columnation,
    U::Diff: Columnation + Ord,
{
    type Target = U;
    type KeyContainer = TimelyStack<U::Key>;
    type ValContainer = TimelyStack<U::Val>;
    type TimeContainer = TimelyStack<U::Time>;
    type DiffContainer = TimelyStack<U::Diff>;
    type OffsetContainer = OffsetList;
}


/// A trace implementation backed by columnar storage.
pub type ColValSpine<K, V, T, R> = Spine<Rc<OrdValBatch<TStack<((K,V),T,R)>>>>;
/// A batcher for columnar storage.
pub type ColValBatcher<K, V, T, R> = MergeBatcher<Vec<((K,V),T,R)>, ColumnationChunker<((K,V),T,R)>, ColMerger<(K,V),T,R>>;
/// A builder for columnar storage.
pub type ColValBuilder<K, V, T, R> = RcBuilder<OrdValBuilder<TStack<((K,V),T,R)>, TimelyStack<((K,V),T,R)>>>;


/// A trace implementation backed by columnar storage.
pub type ColKeySpine<K, T, R> = Spine<Rc<OrdKeyBatch<TStack<((K,()),T,R)>>>>;
/// A batcher for columnar storage
pub type ColKeyBatcher<K, T, R> = MergeBatcher<Vec<((K,()),T,R)>, ColumnationChunker<((K,()),T,R)>, ColMerger<(K,()),T,R>>;
/// A builder for columnar storage
pub type ColKeyBuilder<K, T, R> = RcBuilder<OrdKeyBuilder<TStack<((K,()),T,R)>, TimelyStack<((K,()),T,R)>>>;

// The `ToOwned` requirement exists to satisfy `self.reserve_items`, who must for now
// be presented with the actual contained type, rather than a type that borrows into it.
impl<T: Clone + Ord + Columnation + 'static> BatchContainer for TimelyStack<T> {
    type Borrowed<'a> = &'a TimelyStack<T>;

    fn borrow(&self) -> Self::Borrowed<'_> { self }
    fn with_capacity(size: usize) -> Self {
        Self::with_capacity(size)
    }

    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        let mut new = Self::default();
        new.reserve_regions(std::iter::once(cont1).chain(std::iter::once(cont2)));
        new
    }
    fn len(&self) -> usize {
        self[..].len()
    }
    fn borrow_as<'a>(owned: &'a <Self::Borrowed<'a> as BatchIndex>::Owned) -> <Self::Borrowed<'a> as BatchIndex>::Ref {
        owned
    }
}

impl<'a, T: Columnation + Eq + Ord + Clone> BatchIndex for &'a TimelyStack<T> {
    type Owned = T;
    type Ref = &'a T;

    #[inline] fn len(&self) -> usize { self[..].len() }
    #[inline] fn eq(this: Self::Ref, other: &Self::Owned) -> bool { this == other }
    #[inline] fn to_owned(this: Self::Ref) -> Self::Owned { this.clone() }
    #[inline] fn clone_onto(this: Self::Ref, other: &mut Self::Owned) { other.clone_from(this)}
    #[inline] fn index(&self, index: usize) -> Self::Ref { &self[index] }
}


/// An append-only vector that store records as columns.
///
/// This container maintains elements that might conventionally own
/// memory allocations, but instead the pointers to those allocations
/// reference larger regions of memory shared with multiple instances
/// of the type. Elements can be retrieved as references, and care is
/// taken when this type is dropped to ensure that the correct memory
/// is returned (rather than the incorrect memory, from running the
/// elements `Drop` implementations).
pub struct TimelyStack<T: Columnation> {
    local: Vec<T>,
    inner: T::InnerRegion,
}

impl<T: Columnation> TimelyStack<T> {
    /// Construct a [TimelyStack], reserving space for `capacity` elements
    ///
    /// Note that the associated region is not initialized to a specific capacity
    /// because we can't generally know how much space would be required.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            local: Vec::with_capacity(capacity),
            inner: T::InnerRegion::default(),
        }
    }

    /// Ensures `Self` can absorb `items` without further allocations.
    ///
    /// The argument `items` may be cloned and iterated multiple times.
    /// Please be careful if it contains side effects.
    #[inline(always)]
    pub fn reserve_items<'a, I>(&mut self, items: I)
    where
        I: Iterator<Item= &'a T>+Clone,
        T: 'a,
    {
        self.local.reserve(items.clone().count());
        self.inner.reserve_items(items);
    }

    /// Ensures `Self` can absorb `items` without further allocations.
    ///
    /// The argument `items` may be cloned and iterated multiple times.
    /// Please be careful if it contains side effects.
    #[inline(always)]
    pub fn reserve_regions<'a, I>(&mut self, regions: I)
    where
        Self: 'a,
        I: Iterator<Item= &'a Self>+Clone,
    {
        self.local.reserve(regions.clone().map(|cs| cs.local.len()).sum());
        self.inner.reserve_regions(regions.map(|cs| &cs.inner));
    }



    /// Copies an element in to the region.
    ///
    /// The element can be read by indexing
    pub fn copy(&mut self, item: &T) {
        // TODO: Some types `T` should just be cloned.
        // E.g. types that are `Copy` or vecs of ZSTs.
        unsafe {
            self.local.push(self.inner.copy(item));
        }
    }
    /// Empties the collection.
    pub fn clear(&mut self) {
        unsafe {
            // Unsafety justified in that setting the length to zero exposes
            // no invalid data.
            self.local.set_len(0);
            self.inner.clear();
        }
    }
    /// Retain elements that pass a predicate, from a specified offset.
    ///
    /// This method may or may not reclaim memory in the inner region.
    pub fn retain_from<P: FnMut(&T) -> bool>(&mut self, index: usize, mut predicate: P) {
        let mut write_position = index;
        for position in index..self.local.len() {
            if predicate(&self[position]) {
                // TODO: compact the inner region and update pointers.
                self.local.swap(position, write_position);
                write_position += 1;
            }
        }
        unsafe {
            // Unsafety justified in that `write_position` is no greater than
            // `self.local.len()` and so this exposes no invalid data.
            self.local.set_len(write_position);
        }
    }

    /// Unsafe access to `local` data. The slices stor data that is backed by a region
    /// allocation. Therefore, it is undefined behavior to mutate elements of the `local` slice.
    ///
    /// # Safety
    /// Elements within `local` can be reordered, but not mutated, removed and/or dropped.
    pub unsafe fn local(&mut self) -> &mut [T] {
        &mut self.local[..]
    }

    /// Estimate the memory capacity in bytes.
    #[inline]
    pub fn heap_size(&self, mut callback: impl FnMut(usize, usize)) {
        let size_of = std::mem::size_of::<T>();
        callback(self.local.len() * size_of, self.local.capacity() * size_of);
        self.inner.heap_size(callback);
    }

    /// Estimate the consumed memory capacity in bytes, summing both used and total capacity.
    #[inline]
    pub fn summed_heap_size(&self) -> (usize, usize) {
        let (mut length, mut capacity) = (0, 0);
        self.heap_size(|len, cap| {
            length += len;
            capacity += cap
        });
        (length, capacity)
    }

    /// The length in items.
    #[inline]
    pub fn len(&self) -> usize {
        self.local.len()
    }

    /// Returns `true` if the stack is empty.
    pub fn is_empty(&self) -> bool {
        self.local.is_empty()
    }

    /// The capacity of the local vector.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.local.capacity()
    }

    /// Reserve space for `additional` elements.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.local.reserve(additional)
    }
}

impl<A: Columnation, B: Columnation> TimelyStack<(A, B)> {
    /// Copies a destructured tuple `(A, B)` into this column stack.
    ///
    /// This serves situations where a tuple should be constructed from its constituents but
    /// not all elements are available as owned data.
    ///
    /// The element can be read by indexing
    pub fn copy_destructured(&mut self, t1: &A, t2: &B) {
        unsafe {
            self.local.push(self.inner.copy_destructured(t1, t2));
        }
    }
}

impl<A: Columnation, B: Columnation, C: Columnation> TimelyStack<(A, B, C)> {
    /// Copies a destructured tuple `(A, B, C)` into this column stack.
    ///
    /// This serves situations where a tuple should be constructed from its constituents but
    /// not all elements are available as owned data.
    ///
    /// The element can be read by indexing
    pub fn copy_destructured(&mut self, r0: &A, r1: &B, r2: &C) {
        unsafe {
            self.local.push(self.inner.copy_destructured(r0, r1, r2));
        }
    }
}

impl<T: Columnation> std::ops::Deref for TimelyStack<T> {
    type Target = [T];
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.local[..]
    }
}

impl<T: Columnation> Drop for TimelyStack<T> {
    fn drop(&mut self) {
        self.clear();
    }
}

impl<T: Columnation> Default for TimelyStack<T> {
    fn default() -> Self {
        Self {
            local: Vec::new(),
            inner: T::InnerRegion::default(),
        }
    }
}

impl<'a, A: 'a + Columnation> FromIterator<&'a A> for TimelyStack<A> {
    fn from_iter<T: IntoIterator<Item = &'a A>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut c = TimelyStack::<A>::with_capacity(iter.size_hint().0);
        for element in iter {
            c.copy(element);
        }

        c
    }
}

impl<T: Columnation + PartialEq> PartialEq for TimelyStack<T> {
    fn eq(&self, other: &Self) -> bool {
        PartialEq::eq(&self[..], &other[..])
    }
}

impl<T: Columnation + Eq> Eq for TimelyStack<T> {}

impl<T: Columnation + std::fmt::Debug> std::fmt::Debug for TimelyStack<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self[..].fmt(f)
    }
}

impl<T: Columnation> Clone for TimelyStack<T> {
    fn clone(&self) -> Self {
        let mut new: Self = Default::default();
        for item in &self[..] {
            new.copy(item);
        }
        new
    }

    fn clone_from(&mut self, source: &Self) {
        self.clear();
        for item in &source[..] {
            self.copy(item);
        }
    }
}

impl<T: Columnation> PushInto<T> for TimelyStack<T> {
    #[inline]
    fn push_into(&mut self, item: T) {
        self.copy(&item);
    }
}

impl<T: Columnation> PushInto<&T> for TimelyStack<T> {
    #[inline]
    fn push_into(&mut self, item: &T) {
        self.copy(item);
    }
}


impl<T: Columnation> PushInto<&&T> for TimelyStack<T> {
    #[inline]
    fn push_into(&mut self, item: &&T) {
        self.copy(*item);
    }
}

mod container {
    use std::ops::Deref;

    use columnation::Columnation;
    use timely::Container;
    use timely::container::SizableContainer;

    use crate::columnation::TimelyStack;

    impl<T: Columnation> Container for TimelyStack<T> {
        type ItemRef<'a> = &'a T where Self: 'a;
        type Item<'a> = &'a T where Self: 'a;

        fn len(&self) -> usize {
            self.local.len()
        }

        fn is_empty(&self) -> bool {
            self.local.is_empty()
        }

        fn clear(&mut self) {
            TimelyStack::clear(self)
        }

        type Iter<'a> = std::slice::Iter<'a, T> where Self: 'a;

        fn iter(&self) -> Self::Iter<'_> {
            self.deref().iter()
        }

        type DrainIter<'a> = std::slice::Iter<'a, T> where Self: 'a;

        fn drain(&mut self) -> Self::DrainIter<'_> {
            (*self).iter()
        }
    }

    impl<T: Columnation> SizableContainer for TimelyStack<T> {
        fn at_capacity(&self) -> bool {
            self.len() == self.capacity()
        }
        fn ensure_capacity(&mut self, stash: &mut Option<Self>) {
            if self.capacity() == 0 {
                *self = stash.take().unwrap_or_default();
                self.clear();
            }
            let preferred = timely::container::buffer::default_capacity::<T>();
            if self.capacity() < preferred {
                self.reserve(preferred - self.capacity());
            }
        }
    }
}
