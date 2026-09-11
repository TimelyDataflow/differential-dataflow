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

use columnar::{Borrow, Clear, Columnar, Container, Index, Len, Push};

use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::AntichainRef;
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

    /// Append rows `[s, e)` of `other` using the container's bulk range copy.
    /// Primitive lanes copy as slices, and nested-vector offsets are rebased
    /// without reconstructing timestamps or pushing each row separately.
    #[inline]
    pub fn push_range(&mut self, other: &ColTimes<T>, s: usize, e: usize) {
        self.store.extend_from_self(other.store.borrow(), s..e);
    }

    /// Materialize the whole column to `Vec<T>` — the egress boundary (owned times for
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

    /// The times of rows `[0, n)` advanced by `frontier`, as a class per row and the classes'
    /// times: two rows share a class iff their advanced times are equal, and the classes are
    /// numbered in time order, so ordering rows by class orders them by advanced time.
    ///
    /// `Lattice::advance_by` runs once per DISTINCT stored time, not once per row. A batch of a
    /// million rows carries a few hundred distinct times, and each `advance_by` on a nested time
    /// allocates, so this is the difference between a few hundred allocations and a million. The
    /// distinct times are found in their stored form: a row is matched against the representative
    /// of the time seen just before it, then the next one, then by binary search, and only a time
    /// never seen is materialized and advanced.
    pub fn advance_classes(&self, n: usize, frontier: AntichainRef<T>) -> (Vec<u32>, ColTimes<T>)
    where
        T: ColTime,
    {
        let b = self.store.borrow();
        let same = |i: usize, j: usize| T::cmp_refs(b.get(i), b.get(j)) == Ordering::Equal;
        // Representatives of the distinct stored times, kept in stored order for the search, each
        // naming the advanced time it maps to; ids are stable, positions are not.
        let mut reps: Vec<(usize, u32)> = Vec::new();
        let mut advanced: Vec<T> = Vec::new();
        let mut id_of_row: Vec<u32> = Vec::with_capacity(n);
        let mut hint = 0usize;
        for i in 0..n {
            let at = if hint < reps.len() && same(reps[hint].0, i) {
                hint
            } else if hint + 1 < reps.len() && same(reps[hint + 1].0, i) {
                hint + 1
            } else {
                match reps.binary_search_by(|&(rep, _)| T::cmp_refs(b.get(rep), b.get(i))) {
                    Ok(at) => at,
                    Err(at) => {
                        let mut t = self.get(i);
                        t.advance_by(frontier);
                        reps.insert(at, (i, advanced.len() as u32));
                        advanced.push(t);
                        at
                    }
                }
            };
            hint = at;
            id_of_row.push(reps[at].1);
        }
        // Distinct advanced times in time order are the classes.
        let mut order: Vec<usize> = (0..advanced.len()).collect();
        order.sort_by(|&x, &y| advanced[x].cmp(&advanced[y]));
        let mut class_of_id = vec![0u32; advanced.len()];
        let mut class_times = ColTimes::new();
        for (k, &id) in order.iter().enumerate() {
            if k == 0 || advanced[order[k - 1]] != advanced[id] {
                class_times.push(&advanced[id]);
            }
            class_of_id[id] = (class_times.len() - 1) as u32;
        }
        for id in id_of_row.iter_mut() {
            *id = class_of_id[*id as usize];
        }
        (id_of_row, class_times)
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

#[cfg(test)]
mod cmp_agreement_tests {
    use super::*;
    use differential_dataflow::dynamic::pointstamp::PointStamp;
    use timely::order::Product;

    type T = Product<u64, PointStamp<u64>>;

    fn t(outer: u64, coords: &[u64]) -> T {
        Product::new(outer, PointStamp::new(coords.iter().copied().collect()))
    }

    #[test]
    fn range_copy_rebases_variable_length_timestamps() {
        let values = vec![t(0, &[]), t(1, &[2]), t(2, &[3, 4]), t(3, &[]), t(4, &[5, 6, 7])];
        let source: ColTimes<T> = values.iter().cloned().collect();
        let mut copy: ColTimes<T> = std::iter::once(t(9, &[8, 7])).collect();
        copy.push_range(&source, 1, 4);
        copy.push_range(&source, 2, 2);
        copy.push_range(&source, 0, 5);
        let expected: Vec<_> = std::iter::once(t(9, &[8, 7]))
            .chain(values[1..4].iter().cloned()).chain(values.iter().cloned()).collect();
        assert_eq!(copy.to_vec(), expected);
        copy.clear();
        copy.push_range(&source, 4, 5);
        assert_eq!(copy.to_vec(), values[4..5]);
    }

    /// `advance_classes` must agree with a per-row `advance_by`: same advanced time per row,
    /// classes numbered in time order, for single- and multi-element frontiers.
    #[test]
    fn advance_classes_matches_per_row_advance() {
        use timely::progress::Antichain;
        let times: Vec<T> = vec![
            t(0, &[]), t(0, &[2]), t(1, &[3, 1]), t(0, &[2]), t(2, &[1, 4]), t(0, &[]),
            t(1, &[3, 1]), t(3, &[2]), t(2, &[3, 1]), t(1, &[1]), t(0, &[2]), t(3, &[2, 2]),
        ];
        let store: ColTimes<T> = times.iter().cloned().collect();
        for frontier in [
            Antichain::new(),
            Antichain::from_elem(t(0, &[])),
            Antichain::from_elem(t(2, &[2, 2])),
            Antichain::from(vec![t(1, &[4, 1]), t(3, &[1, 2])]),
        ] {
            let (classes, class_times) = store.advance_classes(times.len(), frontier.borrow());
            let class_times = class_times.to_vec();
            assert!(class_times.windows(2).all(|w| w[0] < w[1]), "classes are distinct and ordered");
            for (i, time) in times.iter().enumerate() {
                let mut expected = time.clone();
                expected.advance_by(frontier.borrow());
                assert_eq!(class_times[classes[i] as usize], expected, "row {i} under {frontier:?}");
            }
        }
    }

    /// `ColTime::cmp_refs` (the derived `Ord` on the columnar `Ref`) must agree with the
    /// timestamp's OWN `Ord` for every pair — the chunk layer sorts and merges by the former
    /// and every other layer reasons with the latter.
    #[test]
    fn col_times_order_matches_owned_order() {
        let times: Vec<T> = vec![
            t(0, &[]), t(0, &[0]), t(0, &[1]), t(0, &[2]), t(0, &[3]),
            t(0, &[1, 1]), t(0, &[1, 2]), t(0, &[2, 1]), t(0, &[3, 1]), t(0, &[3, 2]),
            t(0, &[1, 1, 1]), t(0, &[3, 1, 2]), t(0, &[3, 2, 1]),
            t(1, &[]), t(1, &[3]), t(1, &[3, 1]),
        ];
        let mut store = ColTimes::<T>::new();
        for x in &times { store.push(x); }
        for i in 0..times.len() {
            for j in 0..times.len() {
                let owned = times[i].cmp(&times[j]);
                let col = store.cmp(i, j);
                assert_eq!(
                    owned, col,
                    "order disagrees for {:?} vs {:?}: owned {:?}, columnar {:?}",
                    times[i], times[j], owned, col
                );
            }
        }
    }
}
