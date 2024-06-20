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
pub mod merge_batcher_flat;
pub mod ord_neu;
pub mod rhh;
pub mod huffman_container;
pub mod chunker;

// Opinionated takes on default spines.
pub use self::ord_neu::OrdValSpine as ValSpine;
pub use self::ord_neu::OrdKeySpine as KeySpine;

use std::borrow::{ToOwned};

use timely::Container;
use timely::container::columnation::{Columnation, TimelyStack};
use timely::container::PushInto;
use timely::progress::Timestamp;
use crate::lattice::Lattice;
use crate::difference::Semigroup;

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
pub trait Layout {
    /// The represented update.
    type Target: Update + ?Sized;
    /// Container for update keys.
    // NB: The `PushInto` constraint is only required by `rhh.rs` to push default values.
    type KeyContainer: BatchContainer + PushInto<<Self::Target as Update>::Key>;
    /// Container for update vals.
    type ValContainer: BatchContainer;
    /// Container for times.
    type TimeContainer: BatchContainer<Owned = <Self::Target as Update>::Time> + PushInto<<Self::Target as Update>::Time>;
    /// Container for diffs.
    type DiffContainer: BatchContainer<Owned = <Self::Target as Update>::Diff> + PushInto<<Self::Target as Update>::Diff>;
    /// Container for offsets.
    type OffsetContainer: for<'a> BatchContainer<ReadItem<'a> = usize>;
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

/// A layout based on flat containers.
pub struct FlatLayout<K, V, T, R> {
    phantom: std::marker::PhantomData<(K, V, T, R)>,
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

/// An update and layout description based on preferred containers.
pub struct Preferred<K: ?Sized, V: ?Sized, T, D> {
    phantom: std::marker::PhantomData<(Box<K>, Box<V>, T, D)>,
}

impl<K,V,T,R> Update for Preferred<K, V, T, R>
where
    K: ToOwned + ?Sized,
    K::Owned: Ord+Clone+'static,
    V: ToOwned + ?Sized,
    V::Owned: Ord+Clone+'static,
    T: Ord+Clone+Lattice+timely::progress::Timestamp,
    R: Ord+Clone+Semigroup+'static,
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
    V: Ord+ToOwned+PreferredContainer + ?Sized,
    V::Owned: Ord+Clone+'static,
    T: Ord+Clone+Lattice+timely::progress::Timestamp,
    D: Ord+Clone+Semigroup+'static,
{
    type Target = Preferred<K, V, T, D>;
    type KeyContainer = K::Container;
    type ValContainer = V::Container;
    type TimeContainer = Vec<T>;
    type DiffContainer = Vec<D>;
    type OffsetContainer = OffsetList;
}

use std::convert::TryInto;
use abomonation_derive::Abomonation;

/// A list of unsigned integers that uses `u32` elements as long as they are small enough, and switches to `u64` once they are not.
#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Abomonation)]
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

    fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { item }

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
    fn key_eq(this: &Self::Key<'_>, other: K::ReadItem<'_>) -> bool;

    /// Test that the value equals a key in the layout's value container.
    fn val_eq(this: &Self::Val<'_>, other: V::ReadItem<'_>) -> bool;
}

impl<K,KBC,V,VBC,T,R> BuilderInput<KBC, VBC> for Vec<((K, V), T, R)>
where
    K: Ord + Clone + 'static,
    KBC: BatchContainer,
    for<'a> KBC::ReadItem<'a>: PartialEq<&'a K>,
    V: Ord + Clone + 'static,
    VBC: BatchContainer,
    for<'a> VBC::ReadItem<'a>: PartialEq<&'a V>,
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

    fn key_eq(this: &K, other: KBC::ReadItem<'_>) -> bool {
        KBC::reborrow(other) == this
    }

    fn val_eq(this: &V, other: VBC::ReadItem<'_>) -> bool {
        VBC::reborrow(other) == this
    }
}

impl<K,V,T,R> BuilderInput<K, V> for TimelyStack<((K::Owned, V::Owned), T, R)>
where
    K: BatchContainer,
    for<'a> K::ReadItem<'a>: PartialEq<&'a K::Owned>,
    K::Owned: Ord + Columnation + Clone + 'static,
    V: BatchContainer,
    for<'a> V::ReadItem<'a>: PartialEq<&'a V::Owned>,
    V::Owned: Ord + Columnation + Clone + 'static,
    T: Timestamp + Lattice + Columnation + Clone + 'static,
    R: Ord + Clone + Semigroup + Columnation + 'static,
{
    type Key<'a> = &'a K::Owned;
    type Val<'a> = &'a V::Owned;
    type Time = T;
    type Diff = R;

    fn into_parts<'a>(((key, val), time, diff): Self::Item<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time, Self::Diff) {
        (key, val, time.clone(), diff.clone())
    }

    fn key_eq(this: &&K::Owned, other: K::ReadItem<'_>) -> bool {
        K::reborrow(other) == *this
    }

    fn val_eq(this: &&V::Owned, other: V::ReadItem<'_>) -> bool {
        V::reborrow(other) == *this
    }
}

mod flatcontainer {
    use timely::container::flatcontainer::{FlatStack, IntoOwned, Push, Region};
    use timely::container::flatcontainer::impls::tuple::{TupleABCRegion, TupleABRegion};
    use timely::progress::Timestamp;

    use crate::difference::Semigroup;
    use crate::lattice::Lattice;
    use crate::trace::implementations::{BatchContainer, BuilderInput, FlatLayout, Layout, OffsetList, Update};

    impl<K, V, T, R> Update for FlatLayout<K, V, T, R>
    where
        K: Region,
        V: Region,
        T: Region,
        R: Region,
        K::Owned: Ord + Clone + 'static,
        V::Owned: Ord + Clone + 'static,
        T::Owned: Ord + Clone + Lattice + Timestamp + 'static,
        R::Owned: Ord + Semigroup + 'static,
    {
        type Key = K::Owned;
        type Val = V::Owned;
        type Time = T::Owned;
        type Diff = R::Owned;
    }

    impl<K, V, T, R> Layout for FlatLayout<K, V, T, R>
    where
        K: Region + Push<<K as Region>::Owned> + for<'a> Push<<K as Region>::ReadItem<'a>> + 'static,
        V: Region + Push<<V as Region>::Owned> + for<'a> Push<<V as Region>::ReadItem<'a>> + 'static,
        T: Region + Push<<T as Region>::Owned> + for<'a> Push<<T as Region>::ReadItem<'a>> + 'static,
        R: Region + Push<<R as Region>::Owned> + for<'a> Push<<R as Region>::ReadItem<'a>> + 'static,
        K::Owned: Ord + Clone + 'static,
        V::Owned: Ord + Clone + 'static,
        T::Owned: Ord + Clone + Lattice + Timestamp + 'static,
        R::Owned: Ord + Semigroup + 'static,
        for<'a> K::ReadItem<'a>: Copy + Ord,
        for<'a> V::ReadItem<'a>: Copy + Ord,
        for<'a> T::ReadItem<'a>: Copy + Ord,
        for<'a> R::ReadItem<'a>: Copy + Ord,
    {
        type Target = Self;
        type KeyContainer = FlatStack<K>;
        type ValContainer = FlatStack<V>;
        type TimeContainer = FlatStack<T>;
        type DiffContainer = FlatStack<R>;
        type OffsetContainer = OffsetList;
    }

    impl<K,KBC,V,VBC,T,R> BuilderInput<KBC, VBC> for FlatStack<TupleABCRegion<TupleABRegion<K,V>,T,R>>
    where
        K: Region + Clone + 'static,
        V: Region + Clone + 'static,
        T: Region + Clone + 'static,
        R: Region + Clone + 'static,
        for<'a> K::ReadItem<'a>: Copy + Ord,
        for<'a> V::ReadItem<'a>: Copy + Ord,
        for<'a> T::ReadItem<'a>: Copy + Ord,
        for<'a> R::ReadItem<'a>: Copy + Ord,
        KBC: BatchContainer,
        VBC: BatchContainer,
        for<'a> KBC::ReadItem<'a>: PartialEq<K::ReadItem<'a>>,
        for<'a> VBC::ReadItem<'a>: PartialEq<V::ReadItem<'a>>,
    {
        type Key<'a> = K::ReadItem<'a>;
        type Val<'a> = V::ReadItem<'a>;
        type Time = T::Owned;
        type Diff = R::Owned;

        fn into_parts<'a>(((key, val), time, diff): Self::Item<'a>) -> (Self::Key<'a>, Self::Val<'a>, Self::Time, Self::Diff) {
            (key, val, time.into_owned(), diff.into_owned())
        }

        fn key_eq(this: &Self::Key<'_>, other: KBC::ReadItem<'_>) -> bool {
            KBC::reborrow(other) == K::reborrow(*this)
        }

        fn val_eq(this: &Self::Val<'_>, other: VBC::ReadItem<'_>) -> bool {
            VBC::reborrow(other) == V::reborrow(*this)
        }
    }
}

pub use self::containers::{BatchContainer, SliceContainer};

/// Containers for data that resemble `Vec<T>`, with leaner implementations.
pub mod containers {

    use timely::container::columnation::{Columnation, TimelyStack};
    use timely::container::PushInto;
    use crate::trace::IntoOwned;

    /// A general-purpose container resembling `Vec<T>`.
    pub trait BatchContainer: for<'a> PushInto<Self::ReadItem<'a>> + 'static {
        /// An owned instance of `Self::ReadItem<'_>`.
        type Owned;

        /// The type that can be read back out of the container.
        type ReadItem<'a>: Copy + Ord + IntoOwned<'a, Owned = Self::Owned>;

        /// Push an item into this container
        fn push<D>(&mut self, item: D) where Self: PushInto<D> {
            self.push_into(item);
        }
        /// Creates a new container with sufficient capacity.
        fn with_capacity(size: usize) -> Self;
        /// Creates a new container with sufficient capacity.
        fn merge_capacity(cont1: &Self, cont2: &Self) -> Self;

        /// Converts a read item into one with a narrower lifetime.
        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b>;

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
        type Owned = T;
        type ReadItem<'a> = &'a T;

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { item }

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
    impl<T: Clone + Ord + Columnation + 'static> BatchContainer for TimelyStack<T> {
        type Owned = T;
        type ReadItem<'a> = &'a T;

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { item }

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

    mod flatcontainer {
        use timely::container::flatcontainer::{FlatStack, Push, Region};
        use crate::trace::implementations::BatchContainer;

        impl<R> BatchContainer for FlatStack<R>
        where
            for<'a> R: Region + Push<<R as Region>::ReadItem<'a>> + 'static,
            for<'a> R::ReadItem<'a>: Copy + Ord,
        {
            type Owned = R::Owned;
            type ReadItem<'a> = R::ReadItem<'a>;

            fn with_capacity(size: usize) -> Self {
                Self::with_capacity(size)
            }

            fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
                Self::merge_capacity([cont1, cont2].into_iter())
            }

            fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> {
                R::reborrow(item)
            }

            fn index(&self, index: usize) -> Self::ReadItem<'_> {
                self.get(index)
            }

            fn len(&self) -> usize {
                self.len()
            }
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
        type Owned = Vec<B>;
        type ReadItem<'a> = &'a [B];

        fn reborrow<'b, 'a: 'b>(item: Self::ReadItem<'a>) -> Self::ReadItem<'b> { item }

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
