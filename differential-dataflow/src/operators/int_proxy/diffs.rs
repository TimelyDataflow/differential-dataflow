//! A prototype of collective difference operations.
//!
//! Proxy reduce uses this boundary throughout history replay and correction feedback.
//! Containers own accumulation and movement; callers supply row selections and groups.
//! No scalar difference type, random access, ordering, or constructible zero is required.

use crate::difference::{Multiply, Semigroup};

/// Storage for differences, with collective movement and accumulation.
///
/// Row numbers refer to logical positions, not to a required physical layout.
/// Destinations must have the same runtime schema and accumulation semantics as their sources.
/// Operations append to their destinations, allowing several sources to contribute to one collection.
pub trait DiffContainer: Sized {
    /// The number of differences.
    fn len(&self) -> usize;
    /// Whether there are no differences.
    fn is_empty(&self) -> bool { self.len() == 0 }
    /// Empty storage with this container's schema and accumulation semantics.
    fn empty(&self) -> Self;
    /// Discard differences while retaining the schema and reusable storage.
    fn clear(&mut self);

    /// Append the selected source rows in the supplied order, including repetitions.
    fn copy_from(&mut self, source: &Self, rows: &[usize]);

    /// Append one sum for each nonempty group of selected source rows.
    ///
    /// Group `g` occupies `rows[ends[g - 1]..ends[g]]`, with the first group starting at zero.
    /// Ends must strictly increase and the last end must equal `rows.len()`.
    /// Empty `rows` and `ends` describe no groups.
    /// Sums follow the selected row order and have the semantics of repeated `Semigroup::plus_equals`.
    /// Zero results are retained so that composed containers preserve group correspondence.
    fn sum_from(&mut self, source: &Self, rows: &[usize], ends: &[usize]);

    /// Keep exactly these positions, which must be strictly increasing.
    /// Containers can override this to compact in place without copying survivors.
    fn retain(&mut self, rows: &[usize]) {
        let mut kept = self.empty();
        kept.copy_from(self, rows);
        *self = kept;
    }

    /// Append the positions of nonzero differences in increasing order.
    /// The test has the semantics of `IsZero`; a semigroup without zero reports every position.
    fn nonzero(&self, into: &mut Vec<usize>);
}

impl<R: Semigroup> DiffContainer for Vec<R> {
    fn len(&self) -> usize { Vec::len(self) }
    fn empty(&self) -> Self { Vec::new() }
    fn clear(&mut self) { Vec::clear(self); }

    fn copy_from(&mut self, source: &Self, rows: &[usize]) {
        self.extend(rows.iter().map(|&row| source[row].clone()));
    }

    fn sum_from(&mut self, source: &Self, rows: &[usize], ends: &[usize]) {
        let mut start = 0;
        for &end in ends {
            assert!(start < end, "sum groups must be nonempty");
            let mut sum = source[rows[start]].clone();
            for &row in &rows[start + 1..end] { sum.plus_equals(&source[row]); }
            self.push(sum);
            start = end;
        }
        assert_eq!(start, rows.len(), "sum groups must cover the selection");
    }

    fn retain(&mut self, rows: &[usize]) {
        for (dest, &source) in rows.iter().enumerate() { self.swap(dest, source); }
        self.truncate(rows.len());
    }

    fn nonzero(&self, into: &mut Vec<usize>) {
        into.extend(self.iter().enumerate().filter(|(_, diff)| !diff.is_zero()).map(|(row, _)| row));
    }
}

/// Bilinear multiplication of selected differences, separate from accumulation.
///
/// Callers may submit bounded blocks of pairs rather than materialize a complete cross product.
/// The backend supplies output storage with the appropriate schema.
pub trait MultiplyContainer<Rhs: DiffContainer>: DiffContainer {
    /// Storage for the product differences.
    type Output: DiffContainer;
    /// Append products in pair order, including zero products.
    fn multiply_into(&self, rhs: &Rhs, pairs: &[(usize, usize)], into: &mut Self::Output);
}

impl<R0, R1, RO> MultiplyContainer<Vec<R1>> for Vec<R0>
where
    R0: Semigroup + Multiply<R1, Output = RO>,
    R1: Semigroup,
    RO: Semigroup,
{
    type Output = Vec<RO>;
    fn multiply_into(&self, rhs: &Vec<R1>, pairs: &[(usize, usize)], into: &mut Vec<RO>) {
        into.extend(pairs.iter().map(|&(a, b)| self[a].clone().multiply(&rhs[b])));
    }
}

/// Row metadata aligned with a container of differences.
/// The backend owns the interpretation of differences; the tactic only reads metadata.
pub struct Records<D, C> {
    /// One metadata entry per logical difference.
    pub data: Vec<D>,
    /// Differences aligned with `data`.
    pub diffs: C,
    selection: Vec<usize>,
}

impl<D, C: DiffContainer> Records<D, C> {
    /// Empty records with the supplied difference storage and schema.
    pub fn new(diffs: C) -> Self {
        assert!(diffs.is_empty());
        Self { data: Vec::new(), diffs, selection: Vec::new() }
    }
    /// Number of records.
    pub fn len(&self) -> usize { self.data.len() }
    /// Whether there are no records.
    pub fn is_empty(&self) -> bool { self.data.is_empty() }
    /// Discard records while retaining storage and schema.
    pub fn clear(&mut self) { self.data.clear(); self.diffs.clear(); }
    /// Append selected records, transforming their metadata without inspecting differences.
    pub fn extend<S>(&mut self, source: &Records<S, C>, rows: impl IntoIterator<Item = usize>, mut map: impl FnMut(&S) -> D) {
        self.selection.clear();
        self.selection.extend(rows);
        self.data.extend(self.selection.iter().map(|&row| map(&source.data[row])));
        self.diffs.copy_from(&source.diffs, &self.selection);
    }
}

/// Reusable storage for collective consolidation.
/// Sorting touches only metadata and row numbers; containers accumulate the resulting groups.
/// Data can be `(key, value, time)` or just `value` for an accumulation at a selected time.
pub struct Consolidation<D, C> {
    rows: Vec<usize>,
    ends: Vec<usize>,
    kept: Vec<usize>,
    data: Vec<D>,
    sums: C,
}

impl<D: Ord + Clone, C: DiffContainer> Consolidation<D, C> {
    /// Scratch with the same schema as `diffs`.
    pub fn new(diffs: &C) -> Self {
        Self { rows: Vec::new(), ends: Vec::new(), kept: Vec::new(), data: Vec::new(), sums: diffs.empty() }
    }
    /// Sort, sum equal metadata, and discard zero sums, retaining scratch allocations.
    pub fn consolidate(&mut self, data: &mut Vec<D>, diffs: &mut C) {
        assert_eq!(data.len(), diffs.len());
        self.rows.clear();
        self.rows.extend(0..data.len());
        self.rows.sort_unstable_by(|&a, &b| data[a].cmp(&data[b]));
        self.ends.clear();
        for i in 1..self.rows.len() {
            if data[self.rows[i - 1]] != data[self.rows[i]] { self.ends.push(i); }
        }
        if !self.rows.is_empty() { self.ends.push(self.rows.len()); }

        self.sums.sum_from(diffs, &self.rows, &self.ends);
        self.kept.clear();
        self.sums.nonzero(&mut self.kept);
        self.data.extend(self.kept.iter().map(|&group| {
            let start = if group == 0 { 0 } else { self.ends[group - 1] };
            data[self.rows[start]].clone()
        }));
        self.sums.retain(&self.kept);
        std::mem::swap(data, &mut self.data);
        std::mem::swap(diffs, &mut self.sums);
        self.data.clear();
        self.sums.clear();
    }
}

/// Consolidate aligned data and differences through collective operations.
pub fn consolidate<D: Ord + Clone, C: DiffContainer>(data: &mut Vec<D>, diffs: &mut C) {
    Consolidation::new(diffs).consolidate(data, diffs);
}
