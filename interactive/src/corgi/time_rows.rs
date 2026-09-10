//! Temporary, fixed-width rows for advancing DDIR's numeric product timestamps.
use super::ColTimes;
use crate::ir::Time;
use columnar::{Borrow, Index};
use std::cmp::Ordering;
use timely::progress::frontier::AntichainRef;

/// One flat coordinate buffer; the width is determined at runtime, with no arity cutoff.
/// Rows include the outer epoch and zero-pad PointStamp's omitted trailing coordinates.
pub(crate) struct TimeRows {
    data: Vec<u64>,
    width: usize,
}

impl TimeRows {
    /// Read and advance the first `rows` timestamps. Scratch space is `rows * width` coordinates.
    pub(crate) fn advance(
        times: &ColTimes<Time>,
        rows: usize,
        frontier: AntichainRef<Time>,
    ) -> Self {
        let source = times.store.borrow();
        let width = (0..rows)
            .map(|r| source.get(r).inner.vector.len() + 1)
            .chain(frontier.iter().map(|t| t.inner.len() + 1))
            .max()
            .unwrap_or(1);
        let mut data = vec![0; rows.checked_mul(width).expect("timestamp storage overflow")];
        for (index, row) in data.chunks_exact_mut(width).enumerate() {
            let time = source.get(index);
            row[0] = *time.outer;
            for (value, coordinate) in row[1..].iter_mut().zip(time.inner.vector.into_iter()) {
                *value = *coordinate;
            }
        }

        // Numeric product lattices distribute: meet_f(t join f) = t join meet_f(f).
        // Thus each coordinate advances to max(t_i, min_f(f_i)), with frontier minima
        // computed once for the buffer. This is specific to this lattice, not arbitrary T.
        // An empty frontier leaves times unchanged, matching Lattice::advance_by.
        if !frontier.is_empty() {
            let minima: Vec<u64> = (0..width)
                .map(|c| {
                    frontier
                        .iter()
                        .map(|t| {
                            if c == 0 {
                                t.outer
                            } else {
                                t.inner.get(c - 1).copied().unwrap_or(0)
                            }
                        })
                        .min()
                        .unwrap()
                })
                .collect();
            for row in data.chunks_exact_mut(width) {
                for (value, minimum) in row.iter_mut().zip(&minima) {
                    *value = (*value).max(*minimum);
                }
            }
        }
        Self { data, width }
    }

    fn row(&self, index: usize) -> &[u64] {
        &self.data[index * self.width..(index + 1) * self.width]
    }

    pub(crate) fn cmp(&self, left: usize, right: usize) -> Ordering {
        self.row(left).cmp(self.row(right))
    }

    /// Write coordinates directly to the column, omitting trailing zeros as PointStamp requires.
    pub(crate) fn push_to(&self, index: usize, output: &mut ColTimes<Time>) {
        let row = self.row(index);
        let inner = &row[1..];
        let end = inner.iter().rposition(|v| *v != 0).map_or(0, |i| i + 1);
        output.store.outer.push(row[0]);
        output
            .store
            .inner
            .vector
            .push_iter(inner[..end].iter().copied());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow::{dynamic::pointstamp::PointStamp, lattice::Lattice};
    use timely::progress::Antichain;

    #[test]
    fn rows_preserve_advancement_order_and_canonical_times() {
        let time = |outer, coordinates: &[u64]| {
            Time::new(
                outer,
                PointStamp::new(coordinates.iter().copied().collect()),
            )
        };
        let times = [
            time(0, &[]),
            time(1, &[2]),
            time(2, &[0, 4, 1]),
            time(3, &[u64::MAX, 0, 7]),
            time(u64::MAX, &[0; 16]),
            time(4, &[3; 16]),
        ];
        let source: ColTimes<Time> = times.iter().cloned().collect();
        for frontier in [
            Antichain::new(),
            Antichain::from_elem(time(2, &[1])),
            Antichain::from(vec![time(4, &[2, 0, 9]), time(2, &[0, 4, 1])]),
            Antichain::from_elem(time(0, &[2; 17])),
        ] {
            for count in [0, 1, 3, times.len()] {
                let rows = TimeRows::advance(&source, count, frontier.borrow());
                let mut output = ColTimes::new();
                let expected: Vec<_> = times[..count]
                    .iter()
                    .cloned()
                    .map(|mut t| {
                        t.advance_by(frontier.borrow());
                        t
                    })
                    .collect();
                for i in 0..count {
                    rows.push_to(i, &mut output);
                    assert_eq!(output.get(i), expected[i]);
                    for j in 0..count {
                        assert_eq!(rows.cmp(i, j), expected[i].cmp(&expected[j]));
                    }
                }
            }
        }
    }
}
