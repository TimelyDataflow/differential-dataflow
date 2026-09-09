//! Match sorted key sets without restarting a binary search for every key.

use std::ops::Range;

/// Matching ranges in a sorted identifier lane, indexed by a sorted, unique
/// needle set. Both positions advance monotonically; galloping skips long gaps
/// while adjacent keys take constant work. Absent keys produce no item.
pub(crate) struct MatchingRanges<'a> {
    needles: &'a [u64],
    haystack: &'a [u64],
    needle: usize,
    row: usize,
}

impl<'a> MatchingRanges<'a> {
    pub(crate) fn new(needles: &'a [u64], haystack: &'a [u64]) -> Self {
        debug_assert!(needles.windows(2).all(|w| w[0] < w[1]));
        debug_assert!(haystack.windows(2).all(|w| w[0] <= w[1]));
        Self {
            needles,
            haystack,
            needle: 0,
            row: 0,
        }
    }
}

impl Iterator for MatchingRanges<'_> {
    type Item = (usize, Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        while let (Some(&needle), Some(&key)) =
            (self.needles.get(self.needle), self.haystack.get(self.row))
        {
            match key.cmp(&needle) {
                std::cmp::Ordering::Less => {
                    self.row = gallop(self.haystack, self.row + 1, |x| *x < needle);
                }
                std::cmp::Ordering::Greater => {
                    self.needle = gallop(self.needles, self.needle + 1, |x| *x < key);
                }
                std::cmp::Ordering::Equal => {
                    let start = self.row;
                    self.row = gallop(self.haystack, start + 1, |x| *x == key);
                    let index = self.needle;
                    self.needle += 1;
                    return Some((index, start..self.row));
                }
            }
        }
        None
    }
}

/// First index at or after `start` outside a predicate's prefix.
fn gallop(xs: &[u64], start: usize, predicate: impl Fn(&u64) -> bool) -> usize {
    let mut pos = start;
    if pos < xs.len() && predicate(&xs[pos]) {
        let mut step = 1;
        while pos + step < xs.len() && predicate(&xs[pos + step]) {
            pos += step;
            step <<= 1;
        }
        step >>= 1;
        while step > 0 {
            if pos + step < xs.len() && predicate(&xs[pos + step]) {
                pos += step;
            }
            step >>= 1;
        }
        pos += 1;
    }
    pos
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sorted_matches_agree_with_independent_searches() {
        let mut seed = 0x9e3779b97f4a7c15u64;
        let mut random = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for trial in 0..200 {
            let mut needles: Vec<_> = (0..trial % 40).map(|_| random() % 97).collect();
            let mut haystack: Vec<_> = (0..trial).map(|_| random() % 97).collect();
            if trial % 3 == 0 {
                needles.push(u64::MAX);
                haystack.push(u64::MAX);
            }
            needles.sort_unstable();
            needles.dedup();
            haystack.sort_unstable();
            let expected: Vec<_> = needles
                .iter()
                .enumerate()
                .filter_map(|(i, key)| {
                    let start = haystack.iter().position(|x| x == key)?;
                    let end = haystack.iter().rposition(|x| x == key).unwrap() + 1;
                    Some((i, start..end))
                })
                .collect();
            assert_eq!(
                MatchingRanges::new(&needles, &haystack).collect::<Vec<_>>(),
                expected
            );
        }
    }
}
