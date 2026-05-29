//! Layout traits for columnar arrangements.
//!
//! `ColumnarUpdate` names the four constituent columnar types of an update,
//! and `ColumnarLayout` glues them into a DD `Layout` backed by `Coltainer`.

use std::fmt::Debug;
use columnar::Columnar;
use crate::trace::implementations::{Layout, OffsetList};
use crate::difference::Semigroup;
use crate::lattice::Lattice;
use timely::progress::Timestamp;

/// A layout based on columnar
pub struct ColumnarLayout<U: ColumnarUpdate> {
    phantom: std::marker::PhantomData<U>,
}

impl<K, V, T, R> ColumnarUpdate for (K, V, T, R)
where
    K: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Clone + 'static,
    V: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Clone + 'static,
    T: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Default + Clone + Lattice + Timestamp,
    R: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Default + Semigroup + 'static,
{
    type Key = K;
    type Val = V;
    type Time = T;
    type Diff = R;
}

impl<U: ColumnarUpdate> Layout for ColumnarLayout<U> {
    type KeyContainer    = super::arrangement::Coltainer<U::Key>;
    type ValContainer    = super::arrangement::Coltainer<U::Val>;
    type TimeContainer   = super::arrangement::Coltainer<U::Time>;
    type DiffContainer   = super::arrangement::Coltainer<U::Diff>;
    type OffsetContainer = OffsetList;
}

/// A type that names constituent update types.
///
/// We will use their associated `Columnar::Container`
pub trait ColumnarUpdate : Debug + 'static {
    /// The key type.
    type Key:  Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Clone + 'static;
    /// The value type.
    type Val:  Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Clone + 'static;
    /// The time type.
    type Time: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Default + Clone + Lattice + Timestamp;
    /// The difference type.
    type Diff: Columnar<Container: OrdContainer + Debug + Default> + Debug + Ord + Default + Semigroup + 'static;
}

/// A container whose references can be ordered.
pub trait OrdContainer : for<'a> columnar::Container<Ref<'a> : Ord> { }
impl<C: for<'a> columnar::Container<Ref<'a> : Ord>> OrdContainer for C { }
