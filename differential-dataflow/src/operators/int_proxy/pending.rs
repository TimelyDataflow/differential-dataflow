//! Pending associations, grouped by time in sorted runs with flat key ranges.
use std::ops::Range;
use timely::progress::{Antichain, frontier::AntichainRef, Timestamp};

/// Activated associations refer to a shared time table, sorted by (key, time).
pub(super) struct Due<T> {
    pub times: Vec<T>,
    pub rows: Vec<(u64, usize)>,
}

struct Run<T> {
    times: Vec<T>,
    keys: Vec<u64>,
    ends: Vec<usize>,
    live: Vec<usize>,
    live_keys: usize,
    frontier: Antichain<T>,
}
impl<T: Timestamp> Run<T> {
    fn empty() -> Self {
        Self { times: vec![], keys: vec![], ends: vec![], live: vec![], live_keys: 0, frontier: Antichain::new() }
    }
    fn range(&self, row: usize) -> Range<usize> {
        (if row == 0 { 0 } else { self.ends[row - 1] })..self.ends[row]
    }
    fn weight(&self) -> usize { self.live.len() + self.live_keys }
    fn new(mut pairs: Vec<(T, u64)>) -> Self {
        pairs.sort_unstable();
        pairs.dedup();
        let mut run = Self::empty();
        for (time, key) in pairs {
            if run.times.last() != Some(&time) {
                run.frontier.insert_ref(&time);
                run.live.push(run.times.len());
                run.times.push(time);
                run.ends.push(run.keys.len());
            }
            run.keys.push(key);
            *run.ends.last_mut().unwrap() += 1;
        }
        run.live_keys = run.keys.len();
        run
    }
    fn append(&mut self, source: &Self, row: usize) {
        self.frontier.insert_ref(&source.times[row]);
        self.times.push(source.times[row].clone());
        self.keys.extend_from_slice(&source.keys[source.range(row)]);
        self.ends.push(self.keys.len());
    }
    /// Linear merge of live groups; each timestamp is copied once per group.
    fn merge(self, other: Self) -> Self {
        let mut out = Self::empty();
        let (mut a, mut b) = (0, 0);
        while a < self.live.len() && b < other.live.len() {
            let (i, j) = (self.live[a], other.live[b]);
            match self.times[i].cmp(&other.times[j]) {
                std::cmp::Ordering::Less => { out.append(&self, i); a += 1; }
                std::cmp::Ordering::Greater => { out.append(&other, j); b += 1; }
                std::cmp::Ordering::Equal => {
                    out.frontier.insert_ref(&self.times[i]);
                    out.times.push(self.times[i].clone());
                    let mut x = self.keys[self.range(i)].iter().peekable();
                    let mut y = other.keys[other.range(j)].iter().peekable();
                    while x.peek().is_some() || y.peek().is_some() {
                        let key = match (x.peek(), y.peek()) {
                            (Some(x), Some(y)) => **x.min(y),
                            (Some(x), None) | (None, Some(x)) => **x,
                            _ => unreachable!(),
                        };
                        if x.peek() == Some(&&key) { x.next(); }
                        if y.peek() == Some(&&key) { y.next(); }
                        out.keys.push(key);
                    }
                    out.ends.push(out.keys.len()); a += 1; b += 1;
                }
            }
        }
        for &i in &self.live[a..] { out.append(&self, i); }
        for &j in &other.live[b..] { out.append(&other, j); }
        out.live.extend(0..out.times.len());
        out.live_keys = out.keys.len();
        out
    }
}

pub(super) struct Pending<T> { runs: Vec<Option<Run<T>>> }
impl<T> Default for Pending<T> {
    fn default() -> Self { Self { runs: vec![] } }
}
impl<T: Timestamp> Pending<T> {
    pub fn insert(&mut self, pairs: Vec<(T, u64)>) {
        if pairs.is_empty() { return; }
        let mut run = Run::new(pairs);
        loop {
            let level = run.weight().ilog2() as usize;
            self.runs.resize_with(self.runs.len().max(level + 1), || None);
            if let Some(old) = self.runs[level].take() { run = old.merge(run); }
            else { self.runs[level] = Some(run); break; }
        }
    }
    pub fn frontier(&self) -> Antichain<T> {
        let mut frontier = Antichain::new();
        for run in self.runs.iter().flatten() {
            for time in run.frontier.iter() { frontier.insert_ref(time); }
        }
        frontier
    }
    pub fn activate(&mut self, upper: AntichainRef<T>) -> Due<T> {
        let mut due = Due { times: vec![], rows: vec![] };
        for bin in &mut self.runs {
            let Some(run) = bin else { continue; };
            if run.frontier.iter().all(|t| upper.less_equal(t)) { continue; }
            run.frontier.clear();
            let mut kept = 0;
            for pos in 0..run.live.len() {
                let row = run.live[pos];
                if upper.less_equal(&run.times[row]) {
                    run.frontier.insert_ref(&run.times[row]);
                    run.live[kept] = row; kept += 1;
                } else {
                    let time_row = due.times.len();
                    due.times.push(run.times[row].clone());
                    let range = run.range(row);
                    run.live_keys -= range.len();
                    due.rows.extend(run.keys[range].iter().map(|&key| (key, time_row)));
                }
            }
            run.live.truncate(kept);
            if kept == 0 { *bin = None; }
            else if run.weight() <= (run.keys.len() + run.times.len()) / 2 {
                // Reclaim only when the dead payload pays for copying the survivors.
                let old = bin.take().unwrap();
                *bin = Some(old.merge(Run::empty()));
            }
        }
        // Rank the small time table once; association sorting then compares only integers.
        let mut order: Vec<_> = (0..due.times.len()).collect();
        order.sort_unstable_by_key(|&row| &due.times[row]);
        let mut ranks = vec![0; order.len()];
        let mut times = Vec::new();
        for row in order {
            if times.last() != Some(&due.times[row]) { times.push(due.times[row].clone()); }
            ranks[row] = times.len() - 1;
        }
        due.times = times;
        for row in &mut due.rows { row.1 = ranks[row.1]; }
        due.rows.sort_unstable();
        due.rows.dedup();
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use timely::order::Product;
    type T = Product<u64, u64>;

    fn read(pending: &Pending<T>) -> BTreeSet<(u64, T)> {
        let mut rows = BTreeSet::new();
        for run in pending.runs.iter().flatten() {
            let mut expected = Antichain::new();
            for &row in &run.live {
                expected.insert(run.times[row]);
                let keys = &run.keys[run.range(row)];
                assert!(keys.windows(2).all(|w| w[0] < w[1]));
                rows.extend(keys.iter().map(|&key| (key, run.times[row])));
            }
            assert_eq!(run.frontier, expected);
            assert!(run.keys.len() + run.times.len() < 2 * run.weight());
        }
        rows
    }

    #[test]
    fn shared_times_non_prefix_activation_and_reclamation() {
        let mut pending = Pending::default();
        pending.insert((0..100).flat_map(|key| [T::new(0, 2), T::new(1, 2), T::new(2, 0)]
            .map(|time| (time, key))).collect());
        let run = pending.runs.iter().flatten().next().unwrap();
        assert_eq!((run.times.len(), run.keys.len()), (3, 300));
        let pointer = run.keys.as_ptr();
        assert!(pending.activate(Antichain::from_elem(T::new(0, 0)).borrow()).rows.is_empty());
        assert_eq!(pending.runs.iter().flatten().next().unwrap().keys.as_ptr(), pointer);
        let due = pending.activate(Antichain::from_elem(T::new(1, 1)).borrow());
        let actual: Vec<_> = due.rows.iter().map(|&(key, row)| (key, due.times[row])).collect();
        let expected: Vec<_> = (0..100).flat_map(|key| [(key, T::new(0, 2)), (key, T::new(2, 0))]).collect();
        assert_eq!(actual, expected);
        let run = pending.runs.iter().flatten().next().unwrap();
        assert_eq!((run.times.len(), run.keys.len()), (1, 100));
        assert_eq!(read(&pending).len(), 100);
        assert_eq!(pending.activate(Antichain::new().borrow()).rows.len(), 100);
        assert!(pending.frontier().is_empty());
    }

    #[test]
    fn runs_match_set_oracle_through_insertion_activation_and_merges() {
        let mut random = 19u64;
        let mut next = || { random = random.wrapping_mul(6364136223846793005).wrapping_add(1); random >> 32 };
        let mut pending = Pending::default();
        let mut expected = BTreeSet::new();
        for step in 0..1000 {
            let pairs: Vec<_> = (0..next() % 120).map(|_| {
                let time = if step % 5 == 0 { T::new(u64::MAX, u64::MAX) }
                    else { T::new(next() % 9, next() % 9) };
                let key = next() % 100;
                expected.insert((key, time));
                (time, key)
            }).collect();
            pending.insert(pairs);
            assert_eq!(read(&pending), expected);
            let upper: Antichain<T> = (0..next() % 4).map(|_| T::new(next() % 10, next() % 10)).collect();
            let due = pending.activate(upper.borrow());
            let actual: Vec<_> = due.rows.iter().map(|&(key, row)| (key, due.times[row])).collect();
            assert!(actual.windows(2).all(|w| w[0] < w[1]));
            let ready: Vec<_> = expected.iter().copied().filter(|(_, t)| !upper.less_equal(t)).collect();
            assert_eq!(actual, ready, "step {step}");
            expected.retain(|(_, t)| upper.less_equal(t));
            assert_eq!(read(&pending), expected);
            assert_eq!(pending.frontier(), expected.iter().map(|(_, t)| *t).collect::<Antichain<_>>());
        }
        let due = pending.activate(Antichain::new().borrow());
        assert_eq!(due.rows.iter().map(|&(k, r)| (k, due.times[r])).collect::<BTreeSet<_>>(), expected);
        assert!(read(&pending).is_empty());
    }
}
