//! Implementations of `Trace` and associated traits.
//!
//! The `Trace` trait provides access to an ordered collection of `(key, val, time, diff)` tuples, but
//! there is substantial flexibility in implementations of this trait. Depending on characteristics of
//! the data, we may wish to represent the data in different ways. This module contains several of these
//! implementations, and combiners for merging the results of different traces.
//!
//! As examples of implementations,
//!
//! *  The `trie` module is meant to represent general update tuples, with no particular assumptions made
//!    about their contents. It organizes the data first by key, then by val, and then leaves the rest
//!    in an unordered pile.
//!
//! *  The `keys` module is meant for collections whose value type is `()`, which is to say there is no
//!    (key, val) structure on the records; all of them are just viewed as "keys".
//!
//! *  The `time` module is meant for collections with a single time value. This can remove repetition
//!    from the representation, at the cost of requiring more instances and run-time merging.
//!
//! *  The `base` module is meant for collections with a single time value equivalent to the least time.
//!    These collections must always accumulate to non-negative collections, and as such we can indicate
//!    the frequency of an element by its multiplicity. This removes both the time and weight from the
//!    representation, but is only appropriate for a subset (often substantial) of the data.
//!
//! Each of these representations is best suited for different data, but they can be combined to get the
//! benefits of each, as appropriate. There are several `Cursor` combiners, `CursorList` and `CursorPair`,
//! for homogenous and inhomogenous cursors, respectively.
//!
//! #Musings
//!
//! What is less clear is how to transfer updates between the representations at merge time in a tasteful
//! way. Perhaps we could put an ordering on the representations, each pair with a dominant representation,
//! and part of merging the latter filters updates into the former. Although back and forth might be
//! appealing, more thinking is required to negotiate all of these policies.
//!
//! One option would be to require the layer builder to handle these smarts. Merging is currently done by
//! the layer as part of custom code, but we could make it simply be "iterate through cursor, push results
//! into 'ordered builder'". Then the builder would be bright enough to emit a "batch" for the composite
//! trace, rather than just a batch of the type merged.

pub mod spine_fueled;

mod merge_batcher;
pub(crate) mod merge_batcher_col;

pub use self::merge_batcher::MergeBatcher as Batcher;

pub mod ord;
pub mod ord_neu;
pub mod rhh;

// Opinionated takes on default spines.
pub use self::ord::OrdValSpine as ValSpine;
pub use self::ord::OrdKeySpine as KeySpine;

use std::ops::{Add, Sub};
use std::convert::{TryInto, TryFrom};
use std::borrow::{Borrow, ToOwned};

use timely::container::columnation::{Columnation, TimelyStack};
use lattice::Lattice;
use difference::Semigroup;

/// A type that names constituent update types.
pub trait Update {
    /// We will be able to read out references to this type, and must supply `Key::Owned` as input.
    type Key: Ord + ToOwned<Owned = Self::KeyOwned> + ?Sized;
    /// Key by which data are grouped.
    type KeyOwned: Ord+Clone + Borrow<Self::Key>;
    /// Values associated with the key.
    type Val: Ord + ToOwned<Owned = Self::ValOwned> + ?Sized;
    /// Values associated with the key, in owned form
    type ValOwned: Ord+Clone + Borrow<Self::Val>;
    /// Time at which updates occur.
    type Time: Ord+Lattice+timely::progress::Timestamp+Clone;
    /// Way in which updates occur.
    type Diff: Semigroup+Clone;
}

impl<K,V,T,R> Update for ((K, V), T, R)
where
    K: Ord+Clone,
    V: Ord+Clone,
    T: Ord+Lattice+timely::progress::Timestamp+Clone,
    R: Semigroup+Clone,
{
    type Key = K;
    type KeyOwned = K;
    type Val = V;
    type ValOwned = V;
    type Time = T;
    type Diff = R;
}

/// A type with opinions on how updates should be laid out.
pub trait Layout {
    /// The represented update.
    type Target: Update + ?Sized;
    /// Offsets to use from keys into vals.
    type KeyOffset: OrdOffset;
    /// Offsets to use from vals into updates.
    type ValOffset: OrdOffset;
    /// Container for update keys.
    type KeyContainer:
        RetainFrom<<Self::Target as Update>::Key>+
        BatchContainer<Item=<Self::Target as Update>::Key>;
    /// Container for update vals.
    type ValContainer:
        RetainFrom<<Self::Target as Update>::Val>+
        BatchContainer<Item=<Self::Target as Update>::Val>;
    /// Container for update vals.
    type UpdContainer:
        BatchContainer<Item=(<Self::Target as Update>::Time, <Self::Target as Update>::Diff)>;
}

/// A layout that uses vectors
pub struct Vector<U: Update, O: OrdOffset = usize> {
    phantom: std::marker::PhantomData<(U, O)>,
}

impl<U: Update, O: OrdOffset> Layout for Vector<U, O>
where
    U::Key: ToOwned<Owned = U::Key> + Sized + Clone,
    U::Val: ToOwned<Owned = U::Val> + Sized + Clone,
{
    type Target = U;
    type KeyOffset = O;
    type ValOffset = O;
    type KeyContainer = Vec<U::Key>;
    type ValContainer = Vec<U::Val>;
    type UpdContainer = Vec<(U::Time, U::Diff)>;
}

/// A layout based on timely stacks
pub struct TStack<U: Update, O: OrdOffset = usize> {
    phantom: std::marker::PhantomData<(U, O)>,
}

impl<U: Update+Clone, O: OrdOffset> Layout for TStack<U, O>
where
    U::Key: Columnation + ToOwned<Owned = U::Key>,
    U::Val: Columnation + ToOwned<Owned = U::Val>,
    U::Time: Columnation,
    U::Diff: Columnation,
{
    type Target = U;
    type KeyOffset = O;
    type ValOffset = O;
    type KeyContainer = TimelyStack<U::Key>;
    type ValContainer = TimelyStack<U::Val>;
    type UpdContainer = TimelyStack<(U::Time, U::Diff)>;
}

/// A type with a preferred container.
///
/// Examples include types that implement `Clone` who prefer 
pub trait PreferredContainer : ToOwned {
    /// The preferred container for the type.
    type Container: BatchContainer<Item=Self> + RetainFrom<Self>;
}

impl<T: Clone> PreferredContainer for T {
    type Container = Vec<T>;
}

impl<T: Clone> PreferredContainer for [T] {
    type Container = SliceContainer<T>;
}

/// An update and layout description based on preferred containers.
pub struct Preferred<K: ?Sized, V: ?Sized, T, D, O: OrdOffset = usize> {
    phantom: std::marker::PhantomData<(Box<K>, Box<V>, T, D, O)>,
}

impl<K,V,T,R,O> Update for Preferred<K, V, T, R, O>
where
    K: Ord+ToOwned + ?Sized,
    K::Owned: Ord+Clone,
    V: Ord+ToOwned + ?Sized,
    V::Owned: Ord+Clone,
    T: Ord+Lattice+timely::progress::Timestamp+Clone,
    R: Semigroup+Clone,
    O: OrdOffset,
{
    type Key = K;
    type KeyOwned = K::Owned;
    type Val = V;
    type ValOwned = V::Owned;
    type Time = T;
    type Diff = R;
}

impl<K, V, T, D, O> Layout for Preferred<K, V, T, D, O>
where
    K: Ord+ToOwned+PreferredContainer + ?Sized,
    K::Owned: Ord+Clone,
    V: Ord+ToOwned+PreferredContainer + ?Sized,
    V::Owned: Ord+Clone,
    T: Ord+Lattice+timely::progress::Timestamp+Clone,
    D: Semigroup+Clone,
    O: OrdOffset,
{
    type Target = Preferred<K, V, T, D, O>;
    type KeyOffset = O;
    type ValOffset = O;
    type KeyContainer = K::Container;
    type ValContainer = V::Container;
    type UpdContainer = Vec<(T, D)>;
}


/// A container that can retain/discard from some offset onward.
pub trait RetainFrom<T: ?Sized> {
    /// Retains elements from an index onwards that satisfy a predicate.
    fn retain_from<P: FnMut(usize, &T)->bool>(&mut self, index: usize, predicate: P);
}

impl<T> RetainFrom<T> for Vec<T> {
    fn retain_from<P: FnMut(usize, &T)->bool>(&mut self, index: usize, mut predicate: P) {
        let mut write_position = index;
        for position in index .. self.len() {
            if predicate(position, &self[position]) {
                self.swap(position, write_position);
                write_position += 1;
            }
        }
        self.truncate(write_position);
    }
}

impl<T: Columnation> RetainFrom<T> for TimelyStack<T> {
    fn retain_from<P: FnMut(usize, &T)->bool>(&mut self, index: usize, mut predicate: P) {
        let mut position = index;
        self.retain_from(index, |item| {
            let result = predicate(position, item);
            position += 1;
            result
        })
    }
}

/// Trait for types used as offsets into an ordered layer.
/// This is usually `usize`, but `u32` can also be used in applications
/// where huge batches do not occur to reduce metadata size.
pub trait OrdOffset: Copy + PartialEq + Add<Output=Self> + Sub<Output=Self> + TryFrom<usize> + TryInto<usize>
{}

impl<O> OrdOffset for O
where
    O: Copy + PartialEq + Add<Output=Self> + Sub<Output=Self> + TryFrom<usize> + TryInto<usize>,
{}

pub use self::containers::{BatchContainer, SliceContainer};

/// Containers for data that resemble `Vec<T>`, with leaner implementations.
pub mod containers {

    use timely::container::columnation::{Columnation, TimelyStack};

    use std::borrow::{Borrow, ToOwned};

    /// A general-purpose container resembling `Vec<T>`.
    pub trait BatchContainer: Default {
        /// The type of contained item.
        ///
        /// The container only supplies references to the item, so it needn't be sized.
        type Item: ?Sized;
        /// Inserts an owned item.
        fn push(&mut self, item: <Self::Item as ToOwned>::Owned) where Self::Item: ToOwned;
        /// Inserts a borrowed item.
        fn copy(&mut self, item: &Self::Item);
        /// Extends from a slice of items.
        fn copy_slice(&mut self, slice: &[<Self::Item as ToOwned>::Owned]) where Self::Item: ToOwned;
        /// Extends from a range of items in another`Self`.
        fn copy_range(&mut self, other: &Self, start: usize, end: usize);
        /// Creates a new container with sufficient capacity.
        fn with_capacity(size: usize) -> Self;
        /// Reserves additional capacity.
        fn reserve(&mut self, additional: usize);
        /// Creates a new container with sufficient capacity.
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self;

        /// Reference to the element at this position.
        fn index(&self, index: usize) -> &Self::Item;
        /// Number of contained elements
        fn len(&self) -> usize;
        /// Returns the last item if the container is non-empty.
        fn last(&self) -> Option<&Self::Item> {
            if self.len() > 0 {
                Some(self.index(self.len()-1))
            }
            else {
                None
            }
        }

        /// Reports the number of elements satisfing the predicate.
        ///
        /// This methods *relies strongly* on the assumption that the predicate
        /// stays false once it becomes false, a joint property of the predicate
        /// and the layout of `Self. This allows `advance` to use exponential search to
        /// count the number of elements in time logarithmic in the result.
        fn advance<F: Fn(&Self::Item)->bool>(&self, start: usize, end: usize, function: F) -> usize {

            let small_limit = 8;

            // Exponential seach if the answer isn't within `small_limit`.
            if end > start + small_limit && function(self.index(start + small_limit)) {

                // start with no advance
                let mut index = small_limit + 1;
                if start + index < end && function(self.index(start + index)) {

                    // advance in exponentially growing steps.
                    let mut step = 1;
                    while start + index + step < end && function(self.index(start + index + step)) {
                        index += step;
                        step = step << 1;
                    }

                    // advance in exponentially shrinking steps.
                    step = step >> 1;
                    while step > 0 {
                        if start + index + step < end && function(self.index(start + index + step)) {
                            index += step;
                        }
                        step = step >> 1;
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

    // All `T: Clone` also implement `ToOwned<Owned = T>`, but without the constraint Rust
    // struggles to understand why the owned type must be `T` (i.e. the one blanket impl).
    impl<T: Clone + ToOwned<Owned = T>> BatchContainer for Vec<T> {
        type Item = T;
        fn push(&mut self, item: T) {
            self.push(item);
        }
        fn copy(&mut self, item: &T) {
            self.push(item.clone());
        }
        fn copy_slice(&mut self, slice: &[T]) where T: Sized {
            self.extend_from_slice(slice);
        }
        fn copy_range(&mut self, other: &Self, start: usize, end: usize) {
            self.extend_from_slice(&other[start .. end]);
        }
        fn with_capacity(size: usize) -> Self {
            Vec::with_capacity(size)
        }
        fn reserve(&mut self, additional: usize) {
            self.reserve(additional);
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            Vec::with_capacity(cont1.len() + cont2.len())
        }
        fn index(&self, index: usize) -> &Self::Item {
            &self[index]
        }
        fn len(&self) -> usize {
            self[..].len()
        }
    }

    // The `ToOwned` requirement exists to satisfy `self.reserve_items`, who must for now
    // be presented with the actual contained type, rather than a type that borrows into it.
    impl<T: Columnation + ToOwned<Owned = T>> BatchContainer for TimelyStack<T> {
        type Item = T;
        fn push(&mut self, item: <Self::Item as ToOwned>::Owned) where Self::Item: ToOwned {
            self.copy(item.borrow());
        }
        fn copy(&mut self, item: &T) {
            self.copy(item);
        }
        fn copy_slice(&mut self, slice: &[<Self::Item as ToOwned>::Owned]) where Self::Item: ToOwned {
            self.reserve_items(slice.iter());
            for item in slice.iter() {
                self.copy(item);
            }
        }
        fn copy_range(&mut self, other: &Self, start: usize, end: usize) {
            let slice = &other[start .. end];
            self.reserve_items(slice.iter());
            for item in slice.iter() {
                self.copy(item);
            }
        }
        fn with_capacity(size: usize) -> Self {
            Self::with_capacity(size)
        }
        fn reserve(&mut self, _additional: usize) {
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            let mut new = Self::default();
            new.reserve_regions(std::iter::once(cont1).chain(std::iter::once(cont2)));
            new
        }
        fn index(&self, index: usize) -> &Self::Item {
            &self[index]
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

    impl<B> BatchContainer for SliceContainer<B>
    where
        B: Clone + Sized,
        [B]: ToOwned<Owned = Vec<B>>,
    {
        type Item = [B];
        fn push(&mut self, item: Vec<B>) where Self::Item: ToOwned {
            for x in item.into_iter() {
                self.inner.push(x);
            }
            self.offsets.push(self.inner.len());
        }
        fn copy(&mut self, item: &Self::Item) {
            for x in item.iter() {
                self.inner.copy(x);
            }
            self.offsets.push(self.inner.len());
        }
        fn copy_slice(&mut self, slice: &[Vec<B>]) where Self::Item: ToOwned {
            for item in slice {
                self.copy(item);
            }
        }
        fn copy_range(&mut self, other: &Self, start: usize, end: usize) {
            for index in start .. end {
                self.copy(other.index(index));
            }
        }
        fn with_capacity(size: usize) -> Self {
            let mut offsets = Vec::with_capacity(size + 1);
            offsets.push(0);
            Self {
                offsets,
                inner: Vec::with_capacity(size),
            }
        }
        fn reserve(&mut self, _additional: usize) {
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            let mut offsets = Vec::with_capacity(cont1.inner.len() + cont2.inner.len() + 1);
            offsets.push(0);
            Self {
                offsets,
                inner: Vec::with_capacity(cont1.inner.len() + cont2.inner.len()),
            }
        }
        fn index(&self, index: usize) -> &Self::Item {
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

    use trace::implementations::RetainFrom;
    /// A container that can retain/discard from some offset onward.
    impl<B> RetainFrom<[B]> for SliceContainer<B> {
        /// Retains elements from an index onwards that satisfy a predicate.
        fn retain_from<P: FnMut(usize, &[B])->bool>(&mut self, _index: usize, _predicate: P) {
            unimplemented!()
        }
    }
}
