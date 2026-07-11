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

use columnar::{Borrow, Columnar, Index, Len, Push};

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

    /// Order rows `i` and `j` within this column, via the derived `Ref` `Ord` (no `T`).
    #[inline]
    pub fn cmp(&self, i: usize, j: usize) -> Ordering
    where
        for<'a> columnar::Ref<'a, T>: Ord,
    {
        let b = self.store.borrow();
        let ri = <<T as Columnar>::Container as columnar::Borrow>::reborrow_ref(b.get(i));
        let rj = <<T as Columnar>::Container as columnar::Borrow>::reborrow_ref(b.get(j));
        ri.cmp(&rj)
    }

    /// Order this column's row `i` against `other`'s row `j` (the two-pointer merge compare).
    #[inline]
    pub fn cmp_cross(&self, i: usize, other: &ColTimes<T>, j: usize) -> Ordering
    where
        for<'a> columnar::Ref<'a, T>: Ord,
    {
        let ba = self.store.borrow();
        let bb = other.store.borrow();
        let ri = <<T as Columnar>::Container as columnar::Borrow>::reborrow_ref(ba.get(i));
        let rj = <<T as Columnar>::Container as columnar::Borrow>::reborrow_ref(bb.get(j));
        ri.cmp(&rj)
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
