//! A prototype of collective difference operations.
//!
//! The proxy tactics still use scalar differences.
//! This module isolates the proposed storage boundary and exercises it without changing their time logic.
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

/// Consolidate aligned data and differences through collective operations.
///
/// This is an executable consumer of the proposed interface, not a replacement for row consolidation.
/// Sorting touches only data and row numbers; differences decide how to accumulate each group.
/// Data can be `(key, value, time)` or just `value` for an accumulation at a selected time.
/// A production integration should reuse the temporary storage and allow fused implementations.
pub fn consolidate<D: Ord + Clone, C: DiffContainer>(data: &mut Vec<D>, diffs: &mut C) {
    assert_eq!(data.len(), diffs.len());
    let mut rows: Vec<_> = (0..data.len()).collect();
    rows.sort_unstable_by(|&a, &b| data[a].cmp(&data[b]));
    let mut ends = Vec::new();
    for i in 1..rows.len() {
        if data[rows[i - 1]] != data[rows[i]] { ends.push(i); }
    }
    if !rows.is_empty() { ends.push(rows.len()); }

    let mut sums = diffs.empty();
    sums.sum_from(diffs, &rows, &ends);
    let mut kept = Vec::new();
    sums.nonzero(&mut kept);
    let output = kept.iter().map(|&group| {
        let start = if group == 0 { 0 } else { ends[group - 1] };
        data[rows[start]].clone()
    }).collect();
    diffs.clear();
    diffs.copy_from(&sums, &kept);
    *data = output;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two independent columns; neither storage nor a scalar row implements Semigroup here.
    struct Pair { left: Vec<i64>, right: Vec<i64> }

    impl DiffContainer for Pair {
        fn len(&self) -> usize { self.left.len() }
        fn empty(&self) -> Self { Self { left: vec![], right: vec![] } }
        fn clear(&mut self) { self.left.clear(); self.right.clear(); }
        fn copy_from(&mut self, source: &Self, rows: &[usize]) {
            DiffContainer::copy_from(&mut self.left, &source.left, rows);
            DiffContainer::copy_from(&mut self.right, &source.right, rows);
        }
        fn sum_from(&mut self, source: &Self, rows: &[usize], ends: &[usize]) {
            self.left.sum_from(&source.left, rows, ends);
            self.right.sum_from(&source.right, rows, ends);
        }
        fn nonzero(&self, into: &mut Vec<usize>) {
            into.extend(self.left.iter().zip(&self.right).enumerate()
                .filter(|(_, (a, b))| **a != 0 || **b != 0).map(|(row, _)| row));
        }
    }

    #[test]
    fn component_zeroes_are_retained_until_the_whole_diff_is_tested() {
        let mut data = vec!["a", "b", "a", "b", "c", "c"];
        let mut diffs = Pair { left: vec![1, 0, -1, 0, 1, -1], right: vec![0, 1, 2, -1, 0, 0] };
        consolidate(&mut data, &mut diffs);
        assert_eq!(data, vec!["a"]);
        assert_eq!((diffs.left, diffs.right), (vec![0], vec![2]));
    }

}
