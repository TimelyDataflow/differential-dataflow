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
//! for homogeneous and inhomogeneous cursors, respectively.
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
pub mod ord_neu;
pub mod chunker;

// Opinionated takes on default spines.
pub use self::ord_neu::OrdValSpine as ValSpine;
pub use self::ord_neu::OrdValBatcher as ValBatcher;
pub use self::ord_neu::RcOrdValBuilder as ValBuilder;
pub use self::ord_neu::OrdKeySpine as KeySpine;
pub use self::ord_neu::OrdKeyBatcher as KeyBatcher;
pub use self::ord_neu::RcOrdKeyBuilder as KeyBuilder;

use std::borrow::{ToOwned};
use std::convert::TryInto;

use serde::{Deserialize, Serialize};
use timely::Container;
use timely::container::PushInto;
use timely::progress::Timestamp;

use crate::lattice::Lattice;
use crate::difference::Semigroup;
use crate::trace::implementations::containers::BatchIndex;

/// A type that names constituent update types.
pub trait Update {
    /// Key by which data are grouped.
    type Key: Ord + Clone + 'static;
    /// Values associated with the key.
    type Val: Ord + Clone + 'static;
    /// Time at which updates occur.
    type Time: Ord + Clone + Lattice + timely::progress::Timestamp;
    /// Way in which updates occur.
    type Diff: Ord + Semigroup + 'static;
}

impl<K,V,T,R> Update for ((K, V), T, R)
where
    K: Ord+Clone+'static,
    V: Ord+Clone+'static,
    T: Ord+Clone+Lattice+timely::progress::Timestamp,
    R: Ord+Semigroup+'static,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type Diff = R;
}

/// A type with opinions on how updates should be laid out.
pub trait Layout
where
    for<'a> <Self::KeyContainer as BatchContainer>::Borrowed<'a> : BatchIndex<Owned = <Self::Target as Update>::Key>,
    for<'a> <Self::ValContainer as BatchContainer>::Borrowed<'a> : BatchIndex<Owned = <Self::Target as Update>::Val>,
    for<'a> <Self::TimeContainer as BatchContainer>::Borrowed<'a> : BatchIndex<Owned = <Self::Target as Update>::Time>,
    for<'a> <Self::DiffContainer as BatchContainer>::Borrowed<'a> : BatchIndex<Owned = <Self::Target as Update>::Diff>,
    for<'a> <Self::OffsetContainer as BatchContainer>::Borrowed<'a> : BatchIndex<Ref = usize>,
{
    /// The represented update.
    type Target: Update + ?Sized;
    /// Container for update keys.
    // NB: The `PushInto` constraint is only required by `rhh.rs` to push default values.
    type KeyContainer: BatchContainer + PushInto<<Self::Target as Update>::Key>;
    /// Container for update vals.
    type ValContainer: BatchContainer;
    /// Container for times.
    type TimeContainer: BatchContainer + PushInto<<Self::Target as Update>::Time>;
    /// Container for diffs.
    type DiffContainer: BatchContainer + PushInto<<Self::Target as Update>::Diff>;
    /// Container for offsets.
    type OffsetContainer: for<'a> BatchContainer;
}

/// A layout that uses vectors
pub struct Vector<U: Update> {
    phantom: std::marker::PhantomData<U>,
}

impl<U: Update> Layout for Vector<U>
where
    U::Diff: Ord,
{
    type Target = U;
    type KeyContainer = Vec<U::Key>;
    type ValContainer = Vec<U::Val>;
    type TimeContainer = Vec<U::Time>;
    type DiffContainer = Vec<U::Diff>;
    type OffsetContainer = OffsetList;
}

/// A type with a preferred container.
///
/// Examples include types that implement `Clone` who prefer
pub trait PreferredContainer : ToOwned {
    /// The preferred container for the type.
    type Container: BatchContainer + PushInto<Self::Owned>;
}

impl<T: Ord + Clone + 'static> PreferredContainer for T {
    type Container = Vec<T>;
}

impl<T: Ord + Clone + 'static> PreferredContainer for [T] {
    type Container = SliceContainer<T>;
}

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
    type Borrowed<'a> = &'a OffsetList;
    fn borrow(&self) -> Self::Borrowed<'_> {
        self
    }

    fn with_capacity(size: usize) -> Self {
        Self::with_capacity(size)
    }

    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        Self::with_capacity(cont1.len() + cont2.len())
    }

    fn len(&self) -> usize {
        self.len()
    }
    #[inline] fn borrow_as<'a>(owned: &'a <Self::Borrowed<'a> as BatchIndex>::Owned) -> <Self::Borrowed<'a> as BatchIndex>::Owned { *owned }
}

impl<'a> BatchIndex for &'a OffsetList {
    type Owned = usize;
    type Ref = usize;

    #[inline] fn eq(this: Self::Ref, other: &Self::Owned) -> bool { this == *other }
    #[inline] fn to_owned(this: Self::Ref) -> Self::Owned { this }
    #[inline] fn clone_onto(this: Self::Ref, other: &mut Self::Owned) { *other = this; }
    #[inline] fn len(&self) -> usize { OffsetList::len(self) }
    #[inline] fn index(&self, index: usize) -> Self::Ref { OffsetList::index(self, index) }
}

/// Behavior to split an update into principal components.
pub trait BuilderInput<K: BatchContainer, V: BatchContainer>: Container {
    /// Key portion
    type Key<'a>: Ord;
    /// Value portion
    type Val<'a>: Ord;
    /// Time
    type Time;
    /// Diff
    type Diff;

    /// Split an item into separate parts.
    fn into_parts<'a>(item: Self::Item<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time, Self::Diff);

    /// Test that the key equals a key in the layout's key container.
    fn key_eq(this: &Self::Key<'_>, other: <K::Borrowed<'_> as BatchIndex>::Ref) -> bool;

    /// Test that the value equals a key in the layout's value container.
    fn val_eq(this: &Self::Val<'_>, other: <V::Borrowed<'_> as BatchIndex>::Ref) -> bool;

    /// Count the number of distinct keys, (key, val) pairs, and total updates.
    fn key_val_upd_counts(chain: &[Self]) -> (usize, usize, usize);
}

impl<K,KBC,V,VBC,T,R> BuilderInput<KBC, VBC> for Vec<((K, V), T, R)>
where
    K: Ord + Clone + 'static,
    KBC: BatchContainer,
    for<'a> KBC::Borrowed<'a>: BatchIndex<Owned=K>,
    V: Ord + Clone + 'static,
    VBC: BatchContainer,
    for<'a> VBC::Borrowed<'a>: BatchIndex<Owned=V>,
    T: Timestamp + Lattice + Clone + 'static,
    R: Ord + Semigroup + 'static,
{
    type Key<'a> = K;
    type Val<'a> = V;
    type Time = T;
    type Diff = R;

    fn into_parts<'a>(((key, val), time, diff): Self::Item<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time, Self::Diff) {
        (key, val, time, diff)
    }

    fn key_eq(this: &K, other: <KBC::Borrowed<'_> as BatchIndex>::Ref) -> bool {
        KBC::Borrowed::eq(other, this)
    }

    fn val_eq(this: &V, other: <VBC::Borrowed<'_> as BatchIndex>::Ref) -> bool {
        VBC::Borrowed::eq(other, this)
    }

    fn key_val_upd_counts(chain: &[Self]) -> (usize, usize, usize) {
        let mut keys = 0;
        let mut vals = 0;
        let mut upds = 0;
        let mut prev_keyval = None;
        for link in chain.iter() {
            for ((key, val), _, _) in link.iter() {
                if let Some((p_key, p_val)) = prev_keyval {
                    if p_key != key {
                        keys += 1;
                        vals += 1;
                    } else if p_val != val {
                        vals += 1;
                    }
                } else {
                    keys += 1;
                    vals += 1;
                }
                upds += 1;
                prev_keyval = Some((key, val));
            }
        }
        (keys, vals, upds)
    }
}


pub use self::containers::{BatchContainer, SliceContainer};

/// Containers for data that resemble `Vec<T>`, with leaner implementations.
pub mod containers {

    use timely::container::PushInto;

    /// A general-purpose container resembling `Vec<T>`.
    pub trait BatchContainer: for<'a> PushInto<<Self::Borrowed<'a> as BatchIndex>::Ref> + 'static {
        /// TODO
        type Borrowed<'a>: BatchIndex;
        /// TODO
        fn borrow(&self) -> Self::Borrowed<'_>;

        /// Push an item into this container
        fn push<D>(&mut self, item: D) where Self: PushInto<D> {
            self.push_into(item);
        }
        /// Creates a new container with sufficient capacity.
        fn with_capacity(size: usize) -> Self;
        /// Creates a new container with sufficient capacity.
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self;

        /// Number of contained elements
        fn len(&self) -> usize;
        /// TODO
        fn is_empty(&self) -> bool { self.len() == 0 }
        /// TODO
        fn borrow_as<'a>(owned: &'a <Self::Borrowed<'a> as BatchIndex>::Owned) -> <Self::Borrowed<'a> as BatchIndex>::Ref;
    }

    /// TODO
    pub trait BatchIndex {
        /// TODO
        type Owned;
        /// TODO
        type Ref: Eq + Ord + Copy;

        /// Number of contained elements
        fn len(&self) -> usize;
        /// TODO
        fn is_empty(&self) -> bool { self.len() == 0 }

        /// TODO
        fn eq(this: Self::Ref, other: &Self::Owned) -> bool;
        /// TODO
        fn to_owned(this: Self::Ref) -> Self::Owned;
        /// TODO
        fn clone_onto(this: Self::Ref, other: &mut Self::Owned);

        /// Reference to the element at this position.
        fn index(&self, index: usize) -> Self::Ref;
        /// Returns the last item if the container is non-empty.
        fn last(&self) -> Option<Self::Ref> {
            if self.len() > 0 {
                Some(self.index(self.len()-1))
            }
            else {
                None
            }
        }
        /// Indicates if the length is zero.
        /// Reports the number of elements satisfying the predicate.
        ///
        /// This methods *relies strongly* on the assumption that the predicate
        /// stays false once it becomes false, a joint property of the predicate
        /// and the layout of `Self. This allows `advance` to use exponential search to
        /// count the number of elements in time logarithmic in the result.
        fn advance<F: for<'a> Fn(Self::Ref)->bool>(&self, start: usize, end: usize, function: F) -> usize {

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

    impl<'a, T: Eq + Ord + Clone> BatchIndex for &'a [T] {
        type Owned = T;
        type Ref = &'a T;

        #[inline] fn eq(this: Self::Ref, other: &Self::Owned) -> bool { this == other }
        #[inline] fn to_owned(this: Self::Ref) -> Self::Owned { this.clone() }
        #[inline] fn clone_onto(this: Self::Ref, other: &mut Self::Owned) { other.clone_from(this) }
        #[inline] fn len(&self) -> usize { (*self).len() }
        #[inline] fn index(&self, index: usize) -> Self::Ref { &self[index] }
    }

    // All `T: Clone` also implement `ToOwned<Owned = T>`, but without the constraint Rust
    // struggles to understand why the owned type must be `T` (i.e. the one blanket impl).
    impl<T: Ord + Clone + 'static> BatchContainer for Vec<T> {
        type Borrowed<'a> = &'a [T];
        fn borrow(&self) -> Self::Borrowed<'_> { self }
        fn with_capacity(size: usize) -> Self { Vec::with_capacity(size) }
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self { Vec::with_capacity(cont1.len() + cont2.len()) }
        fn len(&self) -> usize { self[..].len() }
        fn borrow_as<'a>(owned: &'a <Self::Borrowed<'a> as BatchIndex>::Owned) -> <Self::Borrowed<'a> as BatchIndex>::Ref {
            owned
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

    impl<B> PushInto<Vec<B>> for SliceContainer<B> {
        fn push_into(&mut self, item: Vec<B>) {
            for x in item.into_iter() {
                self.inner.push(x);
            }
            self.offsets.push(self.inner.len());
        }
    }

    impl<B> BatchContainer for SliceContainer<B>
    where
        B: Ord + Clone + Sized + 'static,
    {
        type Borrowed<'a> = &'a Self;
        fn borrow(&self) -> Self::Borrowed<'_> { self }

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
        fn len(&self) -> usize { self.offsets.len() - 1 }
        fn borrow_as<'a>(owned: &'a <Self::Borrowed<'a> as BatchIndex>::Owned) -> <Self::Borrowed<'a> as BatchIndex>::Ref { owned }
    }

    impl<'a, B: Eq + Ord + Clone> BatchIndex for &'a SliceContainer<B> {
        type Ref = &'a [B];
        type Owned = Vec<B>;

        fn eq(this: Self::Ref, other: &Self::Owned) -> bool { this == &other[..] }
        fn to_owned(this: Self::Ref) -> Self::Owned { this.to_vec() }
        fn clone_onto(this: Self::Ref, other: &mut Self::Owned) { other.clone_from_slice(this); }
        fn len(&self) -> usize { self.offsets.len() - 1 }
        fn index(&self, index: usize) -> Self::Ref {
            let lower = self.offsets[index];
            let upper = self.offsets[index+1];
            &self.inner[lower .. upper]
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
