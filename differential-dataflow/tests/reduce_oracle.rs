//! Checks reduce against from-scratch accumulation at product-ordered times.
//!
//! The queries cover the full grid of times the input draws from.

use std::collections::BTreeMap;

use rand::{Rng, SeedableRng, StdRng};

use timely::PartialOrder;
use timely::dataflow::operators::capture::{Capture, Extract};
use timely::dataflow::operators::vec::unordered_input::UnorderedInput;

use differential_dataflow::AsCollection;

use pair::Pair;
type Time = Pair<u64, u64>;

fn count(_key: &u64, input: &[(&u64, isize)], output: &mut Vec<(i64, isize)>) {
    output.push((input.len() as i64, 1));
}
fn sum(_key: &u64, input: &[(&u64, isize)], output: &mut Vec<(i64, isize)>) {
    let s: i64 = input.iter().map(|(v, d)| **v as i64 * *d as i64).sum();
    output.push((s, 1));
}
fn min(_key: &u64, input: &[(&u64, isize)], output: &mut Vec<(i64, isize)>) {
    if let Some(m) = input.iter().map(|(v, _)| **v).min() {
        output.push((m as i64, 1));
    }
}

type Logic = fn(&u64, &[(&u64, isize)], &mut Vec<(i64, isize)>);

// A random input: `((key, val), Pair(a, b), diff)`, diffs ±1 so updates cancel.
fn random_input(seed: usize, n: usize, keys: u64, vals: u64, span: u64) -> Vec<((u64, u64), Time, isize)> {
    let s: &[usize] = &[seed, 7, 13, 29];
    let mut rng: StdRng = SeedableRng::from_seed(s);
    (0..n).map(|_| {
        let k = rng.gen_range(0, keys);
        let v = rng.gen_range(0, vals);
        let t = Pair::new(rng.gen_range(0, span), rng.gen_range(0, span));
        let d = if rng.gen::<bool>() { 1 } else { -1 };
        ((k, v), t, d)
    }).collect()
}

// Compare reduce with a from-scratch `f(acc(input))` at each queried time.

#[test] fn oracle_count() { for s in 0..15 { oracle(s, count); } }
#[test] fn oracle_sum()   { for s in 0..15 { oracle(s, sum); } }
#[test] fn oracle_min()   { for s in 0..15 { oracle(s, min); } }

fn oracle(seed: usize, logic: Logic) {
    let span = 6u64;
    let updates = random_input(seed, 300, 10, 8, span);

    // Capture the reduce output as `((key, out_val), time, diff)` updates.
    let (send, recv) = std::sync::mpsc::channel();
    let send = std::sync::Arc::new(std::sync::Mutex::new(send));
    let drive = updates.clone();
    timely::execute_directly(move |worker| {
        let send = send.lock().unwrap().clone();
        let (mut input, capability) = worker.dataflow::<Time, _, _>(|scope| {
            let ((input, capability), stream) = scope.new_unordered_input::<((u64, u64), Time, isize)>();
            let collection = stream.as_collection();
            let out = collection.reduce(logic);
            out.inner.capture_into(send);
            (input, capability)
        });
        for u in drive { input.activate().session(&capability).give(u); }
        drop(capability);
    });
    let output: Vec<((u64, i64), Time, isize)> =
        recv.extract().into_iter().flat_map(|(_, batch)| batch).collect();

    // Everything is finalized, so `acc(output, q) == f(acc(input, q))` must hold at every `q`. All
    // times in play lie on the `span × span` grid, so query every grid point (plus one beyond, to
    // catch updates the reduce should not have produced): a set closed under `join`, unlike e.g.
    // the pairwise joins of the input and output times.
    let times: Vec<Time> =
        (0 .. span + 1).flat_map(|a| (0 .. span + 1).map(move |b| Pair::new(a, b))).collect();

    for q in &times {
        // expected: per key, f applied to the accumulated input at `q`.
        let mut expected: BTreeMap<(u64, i64), i64> = BTreeMap::new();
        let mut per_key: BTreeMap<u64, BTreeMap<u64, isize>> = BTreeMap::new();
        for ((k, v), t, d) in &updates {
            if t.less_equal(q) { *per_key.entry(*k).or_default().entry(*v).or_default() += *d; }
        }
        for (k, vals) in &per_key {
            let acc: Vec<(&u64, isize)> = vals.iter().filter(|(_, d)| **d != 0).map(|(v, d)| (v, *d)).collect();
            if acc.is_empty() { continue; }
            let mut out = Vec::new();
            logic(k, &acc, &mut out);
            for (ov, od) in out { *expected.entry((*k, ov)).or_default() += od as i64; }
        }
        expected.retain(|_, d| *d != 0);

        // actual: the accumulated reduce output at `q`.
        let mut actual: BTreeMap<(u64, i64), i64> = BTreeMap::new();
        for ((k, ov), t, d) in &output {
            if t.less_equal(q) { *actual.entry((*k, *ov)).or_default() += *d as i64; }
        }
        actual.retain(|_, d| *d != 0);

        assert_eq!(expected, actual, "seed {seed} at time {q:?}");
    }
}

/// A minimal product-order (partially ordered) timestamp, so the harness exercises the synthetic
/// interesting-times determination. Copied from `examples/multitemporal.rs`.
mod pair {

    #[derive(Hash, Default, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
    pub struct Pair<S, T> { pub first: S, pub second: T }

    impl<S, T> Pair<S, T> {
        pub fn new(first: S, second: T) -> Self { Pair { first, second } }
    }

    use timely::order::PartialOrder;
    impl<S: PartialOrder, T: PartialOrder> PartialOrder for Pair<S, T> {
        fn less_equal(&self, other: &Self) -> bool {
            self.first.less_equal(&other.first) && self.second.less_equal(&other.second)
        }
    }

    use timely::progress::timestamp::Refines;
    impl<S: Timestamp, T: Timestamp> Refines<()> for Pair<S, T> {
        fn to_inner(_outer: ()) -> Self { Self::minimum() }
        fn to_outer(self) -> () { () }
        fn summarize(_summary: <Self>::Summary) -> () { () }
    }

    use timely::progress::PathSummary;
    impl<S: Timestamp, T: Timestamp> PathSummary<Pair<S,T>> for () {
        fn results_in(&self, timestamp: &Pair<S, T>) -> Option<Pair<S,T>> { Some(timestamp.clone()) }
        fn followed_by(&self, other: &Self) -> Option<Self> { Some(other.clone()) }
    }

    use timely::progress::Timestamp;
    impl<S: Timestamp, T: Timestamp> Timestamp for Pair<S, T> {
        fn minimum() -> Self { Pair { first: S::minimum(), second: T::minimum() }}
        type Summary = ();
    }

    use differential_dataflow::lattice::Lattice;
    impl<S: Lattice, T: Lattice> Lattice for Pair<S, T> {
        fn join(&self, other: &Self) -> Self {
            Pair { first: self.first.join(&other.first), second: self.second.join(&other.second) }
        }
        fn meet(&self, other: &Self) -> Self {
            Pair { first: self.first.meet(&other.first), second: self.second.meet(&other.second) }
        }
    }

    use std::fmt::{Formatter, Error, Debug};
    use serde::{Deserialize, Serialize};
    impl<TOuter: Debug, TInner: Debug> Debug for Pair<TOuter, TInner> {
        fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
            f.write_str(&format!("({:?}, {:?})", self.first, self.second))
        }
    }
}
