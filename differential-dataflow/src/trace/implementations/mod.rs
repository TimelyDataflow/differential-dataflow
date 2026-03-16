//! Implementations of `Trace` and associated traits.
//!
//! The `Trace` trait provides access to an ordered collection of `(key, val, time, diff)` tuples, but
//! there is substantial flexibility in implementations of this trait. Depending on characteristics of
//! the data, we may wish to represent the data in different ways. This module contains several of these
//! implementations, and combiners for merging the results of different traces.

pub mod spine_fueled;

pub mod merge_batcher;
pub mod chainless_batcher;
pub mod ord_neu;
pub mod rhh;
pub mod huffman_container;
pub mod chunker;

// Opinionated takes on default spines.
pub use self::ord_neu::OrdValSpine as ValSpine;
pub use self::ord_neu::OrdValBatcher as ValBatcher;
pub use self::ord_neu::RcOrdValBuilder as ValBuilder;
pub use self::ord_neu::OrdKeySpine as KeySpine;
pub use self::ord_neu::OrdKeyBatcher as KeyBatcher;
pub use self::ord_neu::RcOrdKeyBuilder as KeyBuilder;

use std::convert::TryInto;

use serde::{Deserialize, Serialize};
use timely::container::PushInto;
use timely::progress::Timestamp;

use crate::lattice::Lattice;
use crate::difference::Semigroup;

// ---------------------------------------------------------------------------
// Core columnar data abstraction
// ---------------------------------------------------------------------------

/// A columnar container whose references can be ordered.
pub trait OrdContainer: for<'a> columnar::Container<Ref<'a>: Ord> {}
impl<C: for<'a> columnar::Container<Ref<'a>: Ord>> OrdContainer for C {}

/// A type suitable for use as data in differential dataflow traces.
///
/// All data types must implement `Columnar` (for columnar storage), `Ord + Clone`
/// (for sorting and ownership), and their columnar reference types must be `Ord`
/// (for seeking and comparison in cursors).
pub trait Data: columnar::Columnar<Container: OrdContainer> + Ord + Clone + 'static {}
impl<T: columnar::Columnar<Container: OrdContainer> + Ord + Clone + 'static> Data for T {}

/// A type that names constituent update types.
pub trait Update {
    /// Key by which data are grouped.
    type Key: Data;
    /// Values associated with the key.
    type Val: Data;
    /// Time at which updates occur.
    type Time: Data + Lattice + Timestamp;
    /// Way in which updates occur.
    type Diff: Data + Semigroup;
}

impl<K,V,T,R> Update for ((K, V), T, R)
where
    K: Data,
    V: Data,
    T: Data + Lattice + Timestamp,
    R: Data + Semigroup,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type Diff = R;
}

// ---------------------------------------------------------------------------
// Coltainer: the columnar BatchContainer
// ---------------------------------------------------------------------------

/// A container backed by a columnar store.
///
/// This wraps a `Columnar::Container` and implements `BatchContainer`,
/// providing the bridge between the columnar storage system and
/// differential dataflow's batch infrastructure.
pub struct Coltainer<C: columnar::Columnar> {
    /// The underlying columnar container.
    pub container: C::Container,
}

impl<C: columnar::Columnar> Default for Coltainer<C> {
    fn default() -> Self { Self { container: Default::default() } }
}

impl<C: columnar::Columnar> Clone for Coltainer<C> {
    fn clone(&self) -> Self { Self { container: self.container.clone() } }
}

impl<C: columnar::Columnar> std::fmt::Debug for Coltainer<C>
where C::Container: std::fmt::Debug
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.container.fmt(f)
    }
}

impl<C: Data> BatchContainer for Coltainer<C> {
    type ReadItem<'a> = columnar::Ref<'a, C>;
    type Owned = C;

    #[inline(always)]
    fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned { C::into_owned(item) }
    #[inline(always)]
    fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) { other.copy_from(item) }

    #[inline(always)]
    fn push_ref(&mut self, item: Self::ReadItem<'_>) {
        use columnar::Push;
        self.container.push(item)
    }
    #[inline(always)]
    fn push_own(&mut self, item: &Self::Owned) {
        <C::Container as columnar::Push<&C>>::push(&mut self.container, item)
    }

    fn clear(&mut self) { columnar::Clear::clear(&mut self.container) }

    fn with_capacity(_size: usize) -> Self { Self::default() }
    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        use columnar::Borrow;
        Self {
            container: columnar::Container::with_capacity_for(
                [cont1.container.borrow(), cont2.container.borrow()].into_iter()
            ),
        }
    }

    #[inline(always)]
    fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
        <C::Container as columnar::Borrow>::reborrow_ref(item)
    }

    #[inline(always)]
    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        use columnar::{Borrow, Index};
        self.container.borrow().get(index)
    }

    fn len(&self) -> usize {
        columnar::Len::len(&self.container)
    }
}

impl<C: Data> PushInto<&C> for Coltainer<C> {
    fn push_into(&mut self, item: &C) {
        <C::Container as columnar::Push<&C>>::push(&mut self.container, item)
    }
}

// Serde support for Coltainer: delegate to the inner container.
impl<C: columnar::Columnar> Serialize for Coltainer<C>
where C::Container: Serialize
{
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.container.serialize(serializer)
    }
}
impl<'de, C: columnar::Columnar> Deserialize<'de> for Coltainer<C>
where C::Container: Deserialize<'de>
{
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self { container: C::Container::deserialize(deserializer)? })
    }
}

// ---------------------------------------------------------------------------
// OffsetList: compact offset storage
// ---------------------------------------------------------------------------

/// A list of unsigned integers that uses `u32` elements as long as they are small enough, and switches to `u64` once they are not.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Serialize, Deserialize)]
pub struct OffsetList {
    /// Length of a prefix of zero elements.
    pub zero_prefix: usize,
    /// Offsets that fit within a `u32`.
    pub smol: Vec<u32>,
    /// Offsets that either do not fit in a `u32`, or are inserted after some offset that did not fit.
    pub chonk: Vec<u64>,
}

impl std::fmt::Debug for OffsetList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.into_iter()).finish()
    }
}

impl OffsetList {
    /// Allocate a new list with a specified capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            zero_prefix: 0,
            smol: Vec::with_capacity(cap),
            chonk: Vec::new(),
        }
    }
    /// Inserts the offset, as a `u32` if that is still on the table.
    pub fn push(&mut self, offset: usize) {
        if self.smol.is_empty() && self.chonk.is_empty() && offset == 0 {
            self.zero_prefix += 1;
        }
        else if self.chonk.is_empty() {
            if let Ok(smol) = offset.try_into() {
                self.smol.push(smol);
            }
            else {
                self.chonk.push(offset.try_into().unwrap())
            }
        }
        else {
            self.chonk.push(offset.try_into().unwrap())
        }
    }
    /// Like `std::ops::Index`, which we cannot implement as it must return a `&usize`.
    pub fn index(&self, index: usize) -> usize {
        if index < self.zero_prefix {
            0
        }
        else if index - self.zero_prefix < self.smol.len() {
            self.smol[index - self.zero_prefix].try_into().unwrap()
        }
        else {
            self.chonk[index - self.zero_prefix - self.smol.len()].try_into().unwrap()
        }
    }
    /// The number of offsets in the list.
    pub fn len(&self) -> usize {
        self.zero_prefix + self.smol.len() + self.chonk.len()
    }
}

impl<'a> IntoIterator for &'a OffsetList {
    type Item = usize;
    type IntoIter = OffsetListIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        OffsetListIter {list: self, index: 0 }
    }
}

/// An iterator for [`OffsetList`].
pub struct OffsetListIter<'a> {
    list: &'a OffsetList,
    index: usize,
}

impl<'a> Iterator for OffsetListIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.list.len() {
            let res = Some(self.list.index(self.index));
            self.index += 1;
            res
        } else {
            None
        }
    }
}

impl PushInto<usize> for OffsetList {
    fn push_into(&mut self, item: usize) {
        self.push(item);
    }
}

impl BatchContainer for OffsetList {
    type Owned = usize;
    type ReadItem<'a> = usize;

    #[inline(always)]
    fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned { item }
    #[inline(always)]
    fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { item }

    fn push_ref(&mut self, item: Self::ReadItem<'_>) { self.push_into(item) }
    fn push_own(&mut self, item: &Self::Owned) { self.push_into(*item) }

    fn clear(&mut self) { self.zero_prefix = 0; self.smol.clear(); self.chonk.clear(); }

    fn with_capacity(size: usize) -> Self {
        Self::with_capacity(size)
    }

    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        Self::with_capacity(cont1.len() + cont2.len())
    }

    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        self.index(index)
    }

    fn len(&self) -> usize {
        self.len()
    }
}

// BuilderInput removed: Columnar already provides the owned↔ref relationship.

// ---------------------------------------------------------------------------
// BatchContainer trait
// ---------------------------------------------------------------------------

pub use self::containers::{BatchContainer, SliceContainer};

/// Containers for data that resemble `Vec<T>`, with leaner implementations.
pub mod containers {

    use timely::container::PushInto;

    /// A general-purpose container resembling `Vec<T>`.
    pub trait BatchContainer: 'static {
        /// An owned instance of `Self::ReadItem<'_>`.
        type Owned: Clone + Ord;

        /// The type that can be read back out of the container.
        type ReadItem<'a>: Copy + Ord;

        /// Conversion from an instance of this type to the owned type.
        #[must_use]
        fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned;
        /// Clones `self` onto an existing instance of the owned type.
        #[inline(always)]
        fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) {
            *other = Self::into_owned(item);
        }

        /// Push an item into this container
        fn push_ref(&mut self, item: Self::ReadItem<'_>);
        /// Push an item into this container
        fn push_own(&mut self, item: &Self::Owned);

        /// Clears the container. May not release resources.
        fn clear(&mut self);

        /// Creates a new container with sufficient capacity.
        fn with_capacity(size: usize) -> Self;
        /// Creates a new container with sufficient capacity.
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self;

        /// Converts a read item into one with a narrower lifetime.
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b>;

        /// Reference to the element at this position.
        fn index(&self, index: usize) -> Self::ReadItem<'_>;

        /// Reference to the element at this position, if it exists.
        fn get(&self, index: usize) -> Option<Self::ReadItem<'_>> {
            if index < self.len() {
                Some(self.index(index))
            }
            else { None }
        }

        /// Number of contained elements
        fn len(&self) -> usize;
        /// Returns the last item if the container is non-empty.
        fn last(&self) -> Option<Self::ReadItem<'_>> {
            if self.len() > 0 {
                Some(self.index(self.len()-1))
            }
            else {
                None
            }
        }
        /// Indicates if the length is zero.
        fn is_empty(&self) -> bool { self.len() == 0 }

        /// Reports the number of elements satisfying the predicate.
        ///
        /// This methods *relies strongly* on the assumption that the predicate
        /// stays false once it becomes false, a joint property of the predicate
        /// and the layout of `Self. This allows `advance` to use exponential search to
        /// count the number of elements in time logarithmic in the result.
        fn advance<F: for<'a> Fn(Self::ReadItem<'a>)->bool>(&self, start: usize, end: usize, function: F) -> usize {

            let small_limit = 8;

            // Exponential search if the answer isn't within `small_limit`.
            if end > start + small_limit && function(self.index(start + small_limit)) {

                // start with no advance
                let mut index = small_limit + 1;
                if start + index < end && function(self.index(start + index)) {

                    // advance in exponentially growing steps.
                    let mut step = 1;
                    while start + index + step < end && function(self.index(start + index + step)) {
                        index += step;
                        step <<= 1;
                    }

                    // advance in exponentially shrinking steps.
                    step >>= 1;
                    while step > 0 {
                        if start + index + step < end && function(self.index(start + index + step)) {
                            index += step;
                        }
                        step >>= 1;
                    }

                    index += 1;
                }

                index
            }
            else {
                let limit = std::cmp::min(end, start + small_limit);
                (start .. limit).filter(|x| function(self.index(*x))).count()
            }
        }
    }

    // Vec<T> impl retained for builder compatibility (accepts Vec<((K,V),T,R)> input).
    impl<T: Ord + Clone + 'static> BatchContainer for Vec<T> {
        type Owned = T;
        type ReadItem<'a> = &'a T;

        #[inline(always)] fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned { item.clone() }
        #[inline(always)] fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) { other.clone_from(item); }

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { item }

        fn push_ref(&mut self, item: Self::ReadItem<'_>) { self.push_into(item) }
        fn push_own(&mut self, item: &Self::Owned) { self.push_into(item.clone()) }

        fn clear(&mut self) { self.clear() }

        fn with_capacity(size: usize) -> Self {
            Vec::with_capacity(size)
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            Vec::with_capacity(cont1.len() + cont2.len())
        }
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            &self[index]
        }
        fn get(&self, index: usize) -> Option<Self::ReadItem<'_>> {
            <[T]>::get(&self, index)
        }
        fn len(&self) -> usize {
            self[..].len()
        }
    }

    /// A container that accepts slices `[B::Item]`.
    pub struct SliceContainer<B> {
        /// Offsets that bound each contained slice.
        ///
        /// The length will be one greater than the number of contained slices,
        /// starting with zero and ending with `self.inner.len()`.
        offsets: Vec<usize>,
        /// An inner container for sequences of `B` that dereferences to a slice.
        inner: Vec<B>,
    }

    impl<B: Ord + Clone + 'static> PushInto<&[B]> for SliceContainer<B> {
        fn push_into(&mut self, item: &[B]) {
            for x in item.iter() {
                self.inner.push_into(x);
            }
            self.offsets.push(self.inner.len());
        }
    }

    impl<B: Ord + Clone + 'static> PushInto<&Vec<B>> for SliceContainer<B> {
        fn push_into(&mut self, item: &Vec<B>) {
            self.push_into(&item[..]);
        }
    }

    impl<B> BatchContainer for SliceContainer<B>
    where
        B: Ord + Clone + Sized + 'static,
    {
        type Owned = Vec<B>;
        type ReadItem<'a> = &'a [B];

        #[inline(always)] fn into_owned<'a>(item: Self::ReadItem<'a>) -> Self::Owned { item.to_vec() }
        #[inline(always)] fn clone_onto<'a>(item: Self::ReadItem<'a>, other: &mut Self::Owned) { other.clone_from_slice(item); }

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { item }

        fn push_ref(&mut self, item: Self::ReadItem<'_>) { self.push_into(item) }
        fn push_own(&mut self, item: &Self::Owned) { self.push_into(item) }

        fn clear(&mut self) {
            self.offsets.clear();
            self.offsets.push(0);
            self.inner.clear();
        }

        fn with_capacity(size: usize) -> Self {
            let mut offsets = Vec::with_capacity(size + 1);
            offsets.push(0);
            Self {
                offsets,
                inner: Vec::with_capacity(size),
            }
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            let mut offsets = Vec::with_capacity(cont1.inner.len() + cont2.inner.len() + 1);
            offsets.push(0);
            Self {
                offsets,
                inner: Vec::with_capacity(cont1.inner.len() + cont2.inner.len()),
            }
        }
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            let lower = self.offsets[index];
            let upper = self.offsets[index+1];
            &self.inner[lower .. upper]
        }
        fn len(&self) -> usize {
            self.offsets.len() - 1
        }
    }

    /// Default implementation introduces a first offset.
    impl<B> Default for SliceContainer<B> {
        fn default() -> Self {
            Self {
                offsets: vec![0],
                inner: Default::default(),
            }
        }
    }
}
