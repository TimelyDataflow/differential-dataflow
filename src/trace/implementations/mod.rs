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

pub mod merge_batcher;
pub mod merge_batcher_col;

pub use self::merge_batcher::MergeBatcher as Batcher;

pub mod ord_neu;
pub mod rhh;
pub mod huffman_container;
pub mod option_container;

// Opinionated takes on default spines.
pub use self::ord_neu::OrdValSpine as ValSpine;
pub use self::ord_neu::OrdKeySpine as KeySpine;

use std::borrow::{ToOwned};
use std::cmp::Ordering;

use timely::container::columnation::{Columnation, TimelyStack};
use crate::lattice::Lattice;
use crate::difference::Semigroup;

/// A type that names constituent update types.
pub trait Update {
    /// Key by which data are grouped.
    type Key: Ord + Clone + 'static;
    /// Values associated with the key.
    type Val: Ord + Clone + 'static;
    /// Time at which updates occur.
    type Time: Ord+Lattice+timely::progress::Timestamp+Clone;
    /// Way in which updates occur.
    type Diff: Semigroup+Clone;
}

impl<K,V,T,R> Update for ((K, V), T, R)
where
    K: Ord+Clone+'static,
    V: Ord+Clone+'static,
    T: Ord+Lattice+timely::progress::Timestamp+Clone,
    R: Semigroup+Clone,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type Diff = R;
}

/// A type with opinions on how updates should be laid out.
pub trait Layout {
    /// The represented update.
    type Target: Update + ?Sized;
    /// Container for update keys.
    type KeyContainer:
        BatchContainer<PushItem=<Self::Target as Update>::Key>;
    /// Container for update vals.
    type ValContainer:
        BatchContainer<PushItem=<Self::Target as Update>::Val>;
    /// Container for update vals.
    type UpdContainer:
        for<'a> BatchContainer<PushItem=(<Self::Target as Update>::Time, <Self::Target as Update>::Diff), ReadItem<'a> = &'a (<Self::Target as Update>::Time, <Self::Target as Update>::Diff)>;
    /// Container for offsets.
    type OffsetContainer: BatchContainer<PushItem=usize>;
}

/// A layout that uses vectors
pub struct Vector<U: Update> {
    phantom: std::marker::PhantomData<U>,
}

impl<U: Update> Layout for Vector<U>
where
    U::Key: 'static,
    U::Val: 'static,
{
    type Target = U;
    type KeyContainer = Vec<U::Key>;
    type ValContainer = Vec<U::Val>;
    type UpdContainer = Vec<(U::Time, U::Diff)>;
    type OffsetContainer = OffsetList;
}

/// A layout based on timely stacks
pub struct TStack<U: Update> {
    phantom: std::marker::PhantomData<U>,
}

impl<U: Update> Layout for TStack<U>
where
    U::Key: Columnation + 'static,
    U::Val: Columnation + 'static,
    U::Time: Columnation,
    U::Diff: Columnation,
{
    type Target = U;
    type KeyContainer = TimelyStack<U::Key>;
    type ValContainer = TimelyStack<U::Val>;
    type UpdContainer = TimelyStack<(U::Time, U::Diff)>;
    type OffsetContainer = OffsetList;
}

/// A type with a preferred container.
///
/// Examples include types that implement `Clone` who prefer 
pub trait PreferredContainer : ToOwned {
    /// The preferred container for the type.
    type Container: BatchContainer<PushItem=Self::Owned>;
}

impl<T: Ord + Clone + 'static> PreferredContainer for T {
    type Container = Vec<T>;
}

impl<T: Ord + Clone + 'static> PreferredContainer for [T] {
    type Container = SliceContainer2<T>;
}

/// An update and layout description based on preferred containers.
pub struct Preferred<K: ?Sized, V: ?Sized, T, D> {
    phantom: std::marker::PhantomData<(Box<K>, Box<V>, T, D)>,
}

impl<K,V,T,R> Update for Preferred<K, V, T, R>
where
    K: ToOwned + ?Sized,
    K::Owned: Ord+Clone+'static,
    V: ToOwned + ?Sized + 'static,
    V::Owned: Ord+Clone,
    T: Ord+Lattice+timely::progress::Timestamp+Clone,
    R: Semigroup+Clone,
{
    type Key = K::Owned;
    type Val = V::Owned;
    type Time = T;
    type Diff = R;
}

impl<K, V, T, D> Layout for Preferred<K, V, T, D>
where
    K: Ord+ToOwned+PreferredContainer + ?Sized,
    K::Owned: Ord+Clone+'static,
    // for<'a> K::Container: BatchContainer<ReadItem<'a> = &'a K>,
    V: Ord+ToOwned+PreferredContainer + ?Sized + 'static,
    V::Owned: Ord+Clone,
    T: Ord+Lattice+timely::progress::Timestamp+Clone,
    D: Semigroup+Clone,
{
    type Target = Preferred<K, V, T, D>;
    type KeyContainer = K::Container;
    type ValContainer = V::Container;
    type UpdContainer = Vec<(T, D)>;
    type OffsetContainer = OffsetList;
}

use std::convert::TryInto;
use std::ops::Deref;
use abomonation_derive::Abomonation;
use crate::trace::cursor::MyTrait;

/// A list of unsigned integers that uses `u32` elements as long as they are small enough, and switches to `u64` once they are not.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Debug, Abomonation)]
pub struct OffsetList {
    /// Length of a prefix of zero elements.
    pub zero_prefix: usize,
    /// Offsets that fit within a `u32`.
    pub smol: Vec<u32>,
    /// Offsets that either do not fit in a `u32`, or are inserted after some offset that did not fit.
    pub chonk: Vec<u64>,
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

/// Helper struct to provide `MyTrait` for `Copy` types.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
pub struct Wrapper<T: Copy>(T);

impl<T: Copy> Deref for Wrapper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, T: Copy + Ord> MyTrait<'a> for Wrapper<T> {
    type Owned = T;

    fn into_owned(self) -> Self::Owned {
        self.0
    }

    fn clone_onto(&self, other: &mut Self::Owned) {
        *other = self.0;
    }

    fn compare(&self, other: &Self::Owned) -> Ordering {
        self.0.cmp(other)
    }

    fn borrow_as(other: &'a Self::Owned) -> Self {
        Self(*other)
    }
}

impl BatchContainer for OffsetList {
    type PushItem = usize;
    type ReadItem<'a> = Wrapper<usize>;

    fn push(&mut self, item: Self::PushItem) {
        self.push(item);
    }

    fn copy_push(&mut self, item: &Self::PushItem) {
        self.push(*item);
    }

    fn copy(&mut self, item: Self::ReadItem<'_>) {
        self.push(item.0);
    }

    fn copy_range(&mut self, other: &Self, start: usize, end: usize) {
        for offset in start..end {
            self.push(other.index(offset));
        }
    }

    fn with_capacity(size: usize) -> Self {
        Self::with_capacity(size)
    }

    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        Self::with_capacity(cont1.len() + cont2.len())
    }

    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        Wrapper(self.index(index))
    }

    fn len(&self) -> usize {
        self.len()
    }
}

pub use self::containers::{BatchContainer, SliceContainer, SliceContainer2};

/// Containers for data that resemble `Vec<T>`, with leaner implementations.
pub mod containers {

    use timely::container::columnation::{Columnation, TimelyStack};

    use std::borrow::ToOwned;
    use crate::trace::MyTrait;

    /// A general-purpose container resembling `Vec<T>`.
    pub trait BatchContainer: 'static {
        /// The type of contained item.
        ///
        /// The container only supplies references to the item, so it needn't be sized.
        type PushItem;
        /// The type that can be read back out of the container.
        type ReadItem<'a>: Copy + MyTrait<'a, Owned = Self::PushItem> + for<'b> PartialOrd<Self::ReadItem<'b>>;
        /// Inserts an owned item.
        fn push(&mut self, item: Self::PushItem) {
            self.copy_push(&item);
        }
        /// Inserts an owned item.
        fn copy_push(&mut self, item: &Self::PushItem) {
            self.copy(MyTrait::borrow_as(item));
        }
        /// Inserts a borrowed item.
        fn copy(&mut self, item: Self::ReadItem<'_>);
        /// Extends from a range of items in another`Self`.
        fn copy_range(&mut self, other: &Self, start: usize, end: usize) {
            for index in start .. end {
                self.copy(other.index(index));
            }
        }
        /// Creates a new container with sufficient capacity.
        fn with_capacity(size: usize) -> Self;
        /// Creates a new container with sufficient capacity.
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self;

        /// Reference to the element at this position.
        fn index(&self, index: usize) -> Self::ReadItem<'_>;
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

        /// Reports the number of elements satisfing the predicate.
        ///
        /// This methods *relies strongly* on the assumption that the predicate
        /// stays false once it becomes false, a joint property of the predicate
        /// and the layout of `Self. This allows `advance` to use exponential search to
        /// count the number of elements in time logarithmic in the result.
        fn advance<F: for<'a> Fn(Self::ReadItem<'a>)->bool>(&self, start: usize, end: usize, function: F) -> usize {

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

    // All `T: Clone` also implement `ToOwned<Owned = T>`, but without the constraint Rust
    // struggles to understand why the owned type must be `T` (i.e. the one blanket impl).
    impl<T: Ord + Clone + 'static> BatchContainer for Vec<T> {
        type PushItem = T;
        type ReadItem<'a> = &'a Self::PushItem;

        fn push(&mut self, item: T) {
            self.push(item);
        }
        fn copy_push(&mut self, item: &T) {
            self.copy(item);
        }
        fn copy(&mut self, item: &T) {
            self.push(item.clone());
        }
        fn copy_range(&mut self, other: &Self, start: usize, end: usize) {
            self.extend_from_slice(&other[start .. end]);
        }
        fn with_capacity(size: usize) -> Self {
            Vec::with_capacity(size)
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            Vec::with_capacity(cont1.len() + cont2.len())
        }
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            &self[index]
        }
        fn len(&self) -> usize {
            self[..].len()
        }
    }

    // The `ToOwned` requirement exists to satisfy `self.reserve_items`, who must for now
    // be presented with the actual contained type, rather than a type that borrows into it.
    impl<T: Ord + Columnation + ToOwned<Owned = T> + 'static> BatchContainer for TimelyStack<T> {
        type PushItem = T;
        type ReadItem<'a> = &'a Self::PushItem;

        fn copy_push(&mut self, item: &Self::PushItem) {
            self.copy(item);
        }
        fn copy(&mut self, item: &T) {
            self.copy(item);
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
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            let mut new = Self::default();
            new.reserve_regions(std::iter::once(cont1).chain(std::iter::once(cont2)));
            new
        }
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
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
        B: Ord + Clone + Sized + 'static,
    {
        type PushItem = Vec<B>;
        type ReadItem<'a> = &'a [B];
        fn push(&mut self, item: Vec<B>) {
            for x in item.into_iter() {
                self.inner.push(x);
            }
            self.offsets.push(self.inner.len());
        }
        fn copy_push(&mut self, item: &Vec<B>) {
            self.copy(&item[..]);
        }
        fn copy(&mut self, item: Self::ReadItem<'_>) {
            for x in item.iter() {
                self.inner.copy(x);
            }
            self.offsets.push(self.inner.len());
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

    /// A container that accepts slices `[B::Item]`.
    pub struct SliceContainer2<B> {
        text: String,
        /// Offsets that bound each contained slice.
        ///
        /// The length will be one greater than the number of contained slices,
        /// starting with zero and ending with `self.inner.len()`.
        offsets: Vec<usize>,
        /// An inner container for sequences of `B` that dereferences to a slice.
        inner: Vec<B>,
    }

    /// Welcome to GATs!
    pub struct Greetings<'a, B> {
        /// Text that decorates the data.
        pub text: Option<&'a str>,
        /// The data itself.
        pub slice: &'a [B],
    }

    impl<'a, B> Copy for Greetings<'a, B> { }
    impl<'a, B> Clone for Greetings<'a, B> { 
        fn clone(&self) -> Self { *self }
    }

    use std::cmp::Ordering;
    impl<'a, 'b, B: Ord> PartialEq<Greetings<'a, B>> for Greetings<'b, B> {
        fn eq(&self, other: &Greetings<'a, B>) -> bool {
            self.slice.eq(other.slice)
        }
    }
    impl<'a, B: Ord> Eq for Greetings<'a, B> { }
    impl<'a, 'b, B: Ord> PartialOrd<Greetings<'a, B>> for Greetings<'b, B> {
        fn partial_cmp(&self, other: &Greetings<'a, B>) -> Option<Ordering> {
            self.slice.partial_cmp(other.slice)
        }
    }
    impl<'a, B: Ord> Ord for Greetings<'a, B> {
        fn cmp(&self, other: &Self) -> Ordering {
            self.partial_cmp(other).unwrap()
        }
    }

    impl<'a, B: Ord + Clone> MyTrait<'a> for Greetings<'a, B> {
        type Owned = Vec<B>;
        fn into_owned(self) -> Self::Owned { self.slice.to_vec() }
        fn clone_onto(&self, other: &mut Self::Owned) { 
            self.slice.clone_into(other);
        }
        fn compare(&self, other: &Self::Owned) -> std::cmp::Ordering { 
            self.slice.cmp(&other[..])
        }
        fn borrow_as(other: &'a Self::Owned) -> Self {
            Self {
                text: None,
                slice: &other[..],
            }
        }
    }

    impl<B> BatchContainer for SliceContainer2<B>
    where
        B: Ord + Clone + Sized + 'static,
    {
        type PushItem = Vec<B>;
        type ReadItem<'a> = Greetings<'a, B>;
        fn push(&mut self, item: Vec<B>) {
            for x in item.into_iter() {
                self.inner.push(x);
            }
            self.offsets.push(self.inner.len());
        }
        fn copy_push(&mut self, item: &Vec<B>) {
            self.copy(<_ as MyTrait>::borrow_as(item));
        }
        fn copy(&mut self, item: Self::ReadItem<'_>) {
            for x in item.slice.iter() {
                self.inner.copy(x);
            }
            self.offsets.push(self.inner.len());
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
                text: format!("Hello!"),
                offsets,
                inner: Vec::with_capacity(size),
            }
        }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
            let mut offsets = Vec::with_capacity(cont1.inner.len() + cont2.inner.len() + 1);
            offsets.push(0);
            Self {
                text: format!("Hello!"),
                offsets,
                inner: Vec::with_capacity(cont1.inner.len() + cont2.inner.len()),
            }
        }
        fn index(&self, index: usize) -> Self::ReadItem<'_> {
            let lower = self.offsets[index];
            let upper = self.offsets[index+1];
            Greetings {
                text: Some(&self.text),
                slice: &self.inner[lower .. upper],
            }
        }
        fn len(&self) -> usize {
            self.offsets.len() - 1
        }
    }

    /// Default implementation introduces a first offset.
    impl<B> Default for SliceContainer2<B> {
        fn default() -> Self {
            Self {
                text: format!("Hello!"),
                offsets: vec![0],
                inner: Default::default(),
            }
        }
    }
}
