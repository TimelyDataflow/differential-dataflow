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

// Opinionated takes on default spines.
pub use self::ord::OrdValSpine as ValSpine;
pub use self::ord::OrdKeySpine as KeySpine;

use timely::container::columnation::{Columnation, TimelyStack};
use lattice::Lattice;
use difference::Semigroup;
use trace::layers::BatchContainer;
use trace::layers::ordered::OrdOffset;

/// A type that names constituent update types.
pub trait Update {
    /// Key by which data are grouped.
    type Key: Ord+Clone;
    /// Values associated with the key.
    type Val: Ord+Clone;
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
    type Val = V;
    type Time = T;
    type Diff = R;
}

/// A type with opinions on how updates should be laid out.
pub trait Layout {
    /// The represented update.
    type Target: Update;
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

impl<U: Update+Clone, O: OrdOffset> Layout for Vector<U, O> {
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
    U::Key: Columnation,
    U::Val: Columnation,
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

/// A container that can retain/discard from some offset onward.
pub trait RetainFrom<T> {
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
