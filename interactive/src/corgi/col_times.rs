//! Columnar per-tuple times for `CorgiChunk`.
//!
//! `Inner.times` was `Vec<T>` — one heap-ish `PointStamp` (`SmallVec`) per row, the dominant SCC
//! allocation (~40% of the profile). Since our `T = Product<u64, PointStamp<u64>>` already derives
//! `columnar::Columnar` (Product, PointStamp, and u64 all do), the same times live in SoA form in
//! `<T as Columnar>::Container` — one pair of allocations (offsets + values) for the whole column
//! instead of `n` `SmallVec`s.
//!
//! Times are compared IN PLACE via the container's derived `Ord` on `Ref` (both `Product` and
//! `PointStamp` carry `#[columnar(derive(Ord, PartialOrd))]`), so merge/sort never materialize a
//! `T`. An owned `T` is reconstructed (`get`) only where a `Lattice` op is unavoidable — `join` in
//! the join cross-product, `advance_by` in compaction — or at the emit boundary handing `T` back to
//! DD. Range copies (`emit`/`concat`) push `Ref`s straight across (`push_ref`), also no `T`.
//!
//! This is the O(data) time store; DD's `Chunk` boundary only ever sees whole chunks + frontier
//! antichains (control complexity), so this stays entirely inside the backend — no DD change.

use std::cmp::Ordering;

use columnar::{Borrow, Clear, Columnar, Index, Len, Push};

use differential_dataflow::lattice::Lattice;
use timely::progress::Timestamp;

/// A timestamp usable as a columnar time column: `Timestamp + Lattice` (DD's algebra) plus
/// `Columnar` with an *ordered* `Ref` (so times compare in their SoA form). Our
/// `Product<u64, PointStamp<u64>>` satisfies it — every layer derives `Columnar` with
/// `#[columnar(derive(Ord, PartialOrd))]`.
///
/// The `Ref: Ord` requirement is an HRTB on a projection (`for<'a> Ref<'a, Self>: Ord`), which Rust
/// does NOT imply from a plain `T: ColTime` bound at use sites — so it would otherwise go viral
/// across every fn driving the `Chunk` methods. We discharge it ONCE here, in the blanket impl,
/// behind [`ColTime::cmp_refs`]; downstream code compares times through that method and needs only
/// `T: ColTime`.
pub trait ColTime: Timestamp + Lattice + Columnar {
    /// Order two SoA time references (via the derived `Ref: Ord`, lifetimes reborrowed to unify).
    fn cmp_refs(a: columnar::Ref<'_, Self>, b: columnar::Ref<'_, Self>) -> Ordering;
}

impl<T> ColTime for T
where
    T: Timestamp + Lattice + Columnar,
    for<'a> columnar::Ref<'a, T>: Ord,
{
    #[inline]
    fn cmp_refs(a: columnar::Ref<'_, T>, b: columnar::Ref<'_, T>) -> Ordering {
        let a = <<T as Columnar>::Container as Borrow>::reborrow_ref(a);
        let b = <<T as Columnar>::Container as Borrow>::reborrow_ref(b);
        a.cmp(&b)
    }
}

/// SoA column of per-tuple times, backed by `<T as Columnar>::Container`.
#[derive(Default)]
pub struct ColTimes<T: Columnar> {
    store: <T as Columnar>::Container,
}

impl<T: Columnar> ColTimes<T> {
    #[inline]
    pub fn new() -> Self {
        ColTimes { store: Default::default() }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.store.borrow().len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Empty the column, retaining allocation (the emit-flush reuse in `advance`).
    #[inline]
    pub fn clear(&mut self) {
        self.store.clear();
    }

    /// Append an owned time (columnar `Push<&T>`); the value is stored SoA, not cloned whole.
    #[inline]
    pub fn push(&mut self, t: &T) {
        self.store.push(t);
    }

    /// Append `other`'s row `i` by pushing its `Ref` straight across — no `T` materialized. Used by
    /// the range copies in `emit`/`concat` (`Container: Push<Self::Ref>`).
    #[inline]
    pub fn push_ref(&mut self, other: &ColTimes<T>, i: usize) {
        self.store.push(other.store.borrow().get(i));
    }

    /// The owned time at row `i` — materializes a `T`. Reserve for `Lattice` ops and the emit
    /// boundary; use `cmp`/`cmp_cross` for ordering.
    #[inline]
    pub fn get(&self, i: usize) -> T {
        <T as Columnar>::into_owned(self.store.borrow().get(i))
    }

    /// Append rows `[s, e)` of `other`, pushing `Ref`s straight across — no `T` materialized. The
    /// range copy used by `emit`/`concat`/merge-suffix.
    #[inline]
    pub fn push_range(&mut self, other: &ColTimes<T>, s: usize, e: usize) {
        let b = other.store.borrow();
        for i in s..e {
            self.store.push(b.get(i));
        }
    }

    /// Materialize the whole column to `Vec<T>` — the egress boundary (`SortedRun` for the join,
    /// `CorgiContainer` for `as_collection`), where owned `T` is wanted anyway.
    pub fn to_vec(&self) -> Vec<T> {
        let b = self.store.borrow();
        (0..b.len()).map(|i| <T as Columnar>::into_owned(b.get(i))).collect()
    }

    /// Order rows `i` and `j` within this column, in place via [`ColTime::cmp_refs`] (no `T`).
    #[inline]
    pub fn cmp(&self, i: usize, j: usize) -> Ordering
    where
        T: ColTime,
    {
        let b = self.store.borrow();
        T::cmp_refs(b.get(i), b.get(j))
    }

    /// Order this column's row `i` against `other`'s row `j` (the two-pointer merge compare).
    #[inline]
    pub fn cmp_cross(&self, i: usize, other: &ColTimes<T>, j: usize) -> Ordering
    where
        T: ColTime,
    {
        let ba = self.store.borrow();
        let bb = other.store.borrow();
        T::cmp_refs(ba.get(i), bb.get(j))
    }
}

/// Build a column from an iterator of owned times (the `FromIterator` path used at construction).
impl<T: Columnar> FromIterator<T> for ColTimes<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut store: <T as Columnar>::Container = Default::default();
        for t in iter {
            store.push(&t);
        }
        ColTimes { store }
    }
}
