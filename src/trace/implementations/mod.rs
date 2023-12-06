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

pub mod ord_neu;
pub mod rhh;
pub mod huffman_container;

// Opinionated takes on default spines.
pub use self::ord_neu::OrdValSpine as ValSpine;
pub use self::ord_neu::OrdKeySpine as KeySpine;

use std::borrow::{ToOwned};

use timely::container::columnation::{Columnation, TimelyStack};
use lattice::Lattice;
use difference::Semigroup;

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
}

/// A layout that uses vectors
pub struct Vector<U: Update> {
    phantom: std::marker::PhantomData<U>,
}

impl<U: Update> Layout for Vector<U>
where
    U::Key: 'static,
    U::Val: 'static,
// where
//     U::Key: ToOwned<Owned = U::Key> + Sized + Clone + 'static,
//     U::Val: ToOwned<Owned = U::Val> + Sized + Clone + 'static,
{
    type Target = U;
    type KeyContainer = Vec<U::Key>;
    type ValContainer = Vec<U::Val>;
    type UpdContainer = Vec<(U::Time, U::Diff)>;
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
}


// /// A container that can retain/discard from some offset onward.
// pub trait RetainFrom<T: ?Sized> {
//     /// Retains elements from an index onwards that satisfy a predicate.
//     fn retain_from<P: FnMut(usize, &T)->bool>(&mut self, index: usize, predicate: P);
// }

// impl<T> RetainFrom<T> for Vec<T> {
//     fn retain_from<P: FnMut(usize, &T)->bool>(&mut self, index: usize, mut predicate: P) {
//         let mut write_position = index;
//         for position in index .. self.len() {
//             if predicate(position, &self[position]) {
//                 self.swap(position, write_position);
//                 write_position += 1;
//             }
//         }
//         self.truncate(write_position);
//     }
// }

// impl<T: Columnation> RetainFrom<T> for TimelyStack<T> {
//     fn retain_from<P: FnMut(usize, &T)->bool>(&mut self, index: usize, mut predicate: P) {
//         let mut position = index;
//         self.retain_from(index, |item| {
//             let result = predicate(position, item);
//             position += 1;
//             result
//         })
//     }
// }

use std::convert::TryInto;

/// A list of unsigned integers that uses `u32` elements as long as they are small enough, and switches to `u64` once they are not.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Debug, Abomonation)]
pub struct OffsetList {
    /// Offsets that fit within a `u32`.
    pub smol: Vec<u32>,
    /// Offsets that either do not fit in a `u32`, or are inserted after some offset that did not fit.
    pub chonk: Vec<u64>,
}

impl OffsetList {
    /// Inserts the offset, as a `u32` if that is still on the table.
    pub fn push(&mut self, offset: usize) {
        if self.chonk.is_empty() {
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
        if index < self.smol.len() {
            self.smol[index].try_into().unwrap()
        }
        else {
            self.chonk[index - self.smol.len()].try_into().unwrap()
        }
    }
    /// Set the offset at location index.
    ///
    /// Complicated if `offset` does not fit into `self.smol`.
    pub fn set(&mut self, index: usize, offset: usize) {
        if index < self.smol.len() {
            if let Ok(off) = offset.try_into() {
                self.smol[index] = off;
            }
            else {
                // Move all `smol` elements from `index` onward to the front of `chonk`.
                self.chonk.splice(0..0, self.smol.drain(index ..).map(|x| x.try_into().unwrap()));
                self.chonk[index - self.smol.len()] = offset.try_into().unwrap();
            }
        }
        else {
            self.chonk[index - self.smol.len()] = offset.try_into().unwrap();
        }
    }
    /// The last element in the list of offsets, if non-empty.
    pub fn last(&self) -> Option<usize> {
        if self.chonk.is_empty() {
            self.smol.last().map(|x| (*x).try_into().unwrap())
        }
        else {
            self.chonk.last().map(|x| (*x).try_into().unwrap())
        }
    }
    /// THe number of offsets in the list.
    pub fn len(&self) -> usize {
        self.smol.len() + self.chonk.len()
    }
    /// Allocate a new list with a specified capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            smol: Vec::with_capacity(cap),
            chonk: Vec::new(),
        }
    }
    /// Trim all elements at index `length` and greater.
    pub fn truncate(&mut self, length: usize) {
        if length > self.smol.len() {
            self.chonk.truncate(length - self.smol.len());
        }
        else {
            assert!(self.chonk.is_empty());
            self.smol.truncate(length);
        }
    }
}

pub use self::containers::{BatchContainer, SliceContainer, SliceContainer2};

/// Containers for data that resemble `Vec<T>`, with leaner implementations.
pub mod containers {

    use timely::container::columnation::{Columnation, TimelyStack};

    use std::borrow::{Borrow, ToOwned};
    use trace::MyTrait;

    /// A general-purpose container resembling `Vec<T>`.
    pub trait BatchContainer: Default + 'static {
        /// The type of contained item.
        ///
        /// The container only supplies references to the item, so it needn't be sized.
        type PushItem;
        /// The type that can be read back out of the container.
        type ReadItem<'a>: Copy + MyTrait<'a, Owned = Self::PushItem> + for<'b> PartialOrd<Self::ReadItem<'b>>;
        /// Inserts an owned item.
        fn push(&mut self, item: Self::PushItem);
        /// Inserts an owned item.
        fn copy_push(&mut self, item: &Self::PushItem);
        /// Inserts a borrowed item.
        fn copy<'a>(&mut self, item: Self::ReadItem<'a>);
        /// Extends from a slice of items.
        fn copy_slice(&mut self, slice: &[Self::PushItem]);
        /// Extends from a range of items in another`Self`.
        fn copy_range(&mut self, other: &Self, start: usize, end: usize);
        /// Creates a new container with sufficient capacity.
        fn with_capacity(size: usize) -> Self;
        /// Reserves additional capacity.
        fn reserve(&mut self, additional: usize);
        /// Creates a new container with sufficient capacity.
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self;

        /// Reference to the element at this position.
        fn index<'a>(&'a self, index: usize) -> Self::ReadItem<'a>;
        /// Number of contained elements
        fn len(&self) -> usize;
        /// Returns the last item if the container is non-empty.
        fn last<'a>(&'a self) -> Option<Self::ReadItem<'a>> {
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
        fn copy_slice(&mut self, slice: &[T]) {
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
        fn index<'a>(&'a self, index: usize) -> Self::ReadItem<'a> {
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

        fn push(&mut self, item: Self::PushItem) {
            self.copy(item.borrow());
        }
        fn copy_push(&mut self, item: &Self::PushItem) {
            self.copy(item);
        }
        fn copy(&mut self, item: &T) {
            self.copy(item);
        }
        fn copy_slice(&mut self, slice: &[Self::PushItem]) {
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
        fn index<'a>(&'a self, index: usize) -> Self::ReadItem<'a> {
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
        fn copy<'a>(&mut self, item: Self::ReadItem<'a>) {
            for x in item.iter() {
                self.inner.copy(x);
            }
            self.offsets.push(self.inner.len());
        }
        fn copy_slice(&mut self, slice: &[Vec<B>]) {
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
        fn index<'a>(&'a self, index: usize) -> Self::ReadItem<'a> {
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
        fn clone(&self) -> Self {
            Self {
                text: self.text.clone(),
                slice: self.slice,
            }
        }
    }

    use std::cmp::Ordering;
    impl<'a, 'b, B: Ord> PartialEq<Greetings<'a, B>> for Greetings<'b, B> {
        fn eq(&self, other: &Greetings<'a, B>) -> bool {
            self.slice.eq(&other.slice[..])
        }
    }
    impl<'a, B: Ord> Eq for Greetings<'a, B> { }
    impl<'a, 'b, B: Ord> PartialOrd<Greetings<'a, B>> for Greetings<'b, B> {
        fn partial_cmp(&self, other: &Greetings<'a, B>) -> Option<Ordering> {
            self.slice.partial_cmp(&other.slice[..])
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
        fn copy<'a>(&mut self, item: Self::ReadItem<'a>) {
            for x in item.slice.iter() {
                self.inner.copy(x);
            }
            self.offsets.push(self.inner.len());
        }
        fn copy_slice(&mut self, slice: &[Vec<B>]) {
            for item in slice {
                self.copy_push(item);
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
                text: format!("Hello!"),
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
                text: format!("Hello!"),
                offsets,
                inner: Vec::with_capacity(cont1.inner.len() + cont2.inner.len()),
            }
        }
        fn index<'a>(&'a self, index: usize) -> Self::ReadItem<'a> {
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

    // use trace::implementations::RetainFrom;
    // /// A container that can retain/discard from some offset onward.
    // impl<B> RetainFrom<[B]> for SliceContainer<B> {
    //     /// Retains elements from an index onwards that satisfy a predicate.
    //     fn retain_from<P: FnMut(usize, &[B])->bool>(&mut self, _index: usize, _predicate: P) {
    //         unimplemented!()
    //     }
    // }
}
