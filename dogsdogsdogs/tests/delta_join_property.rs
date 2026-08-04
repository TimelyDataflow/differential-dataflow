//! Property test: an assembled delta join accounts every pair of updates exactly once.
//!
//! The individual operators here have no standalone collection semantics — `half_join` is
//! deliberately "half" of one. What has a semantics is the *assembled set*: for a join over
//! atoms numbered `0 .. k`, the `k` delta rules together must produce, for each combination of
//! one update from each atom, exactly one output, at the join of the combination's times, with
//! the product of its diffs.
//!
//! That statement is directly checkable against a brute-force oracle, which is what this file
//! does for `k = 2` over randomly generated updates at `Product<usize, usize>` times. Random
//! times over a small grid make ties (where the cuts' strictness decides) and incomparable
//! pairs (where only a total order decides at all) both common, and random `±1` diffs make
//! cancellation common. The cuts are not hand-written: they come from
//! [`Cut::for_positions`], so the test exercises the derivation as well as the operator.

use std::sync::{Arc, Mutex};

use timely::order::Product;
use timely::progress::Antichain;
use timely::dataflow::operators::vec::UnorderedInput;
use timely::dataflow::operators::vec::Map;
use timely::dataflow::operators::vec::unordered_input::UnorderedHandle;

use differential_dataflow::AsCollection;
use differential_dataflow::lattice::Lattice;
use differential_dogs3::operators::{half_join, Cut};

type Time = Product<usize, usize>;
/// `((key, val), time, diff)` — one update of one atom.
type Update = ((u32, u32), Time, isize);
/// `((key, (val0, val1)), time, diff)` — one update of the join.
type Joined = ((u32, (u32, u32)), Time, isize);

/// Deterministic xorshift, so a failure reproduces from its seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 { self.next() % n }
}

/// Updates over a deliberately small grid: keys collide, times tie, times sit incomparable.
fn generate(rng: &mut Rng, count: usize) -> Vec<Update> {
    (0..count)
        .map(|_| {
            let key = rng.below(3) as u32;
            let val = rng.below(3) as u32;
            let outer = rng.below(3) as usize;
            let inner = rng.below(3) as usize;
            let diff = if rng.below(2) == 0 { 1 } else { -1 };
            ((key, val), Product::new(outer, inner), diff)
        })
        .collect()
}

fn consolidate(mut updates: Vec<Joined>) -> Vec<Joined> {
    updates.sort_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    let mut out: Vec<Joined> = Vec::new();
    for (data, time, diff) in updates {
        match out.last_mut() {
            Some(last) if last.0 == data && last.1 == time => last.2 += diff,
            _ => out.push((data, time, diff)),
        }
    }
    out.retain(|(_, _, diff)| *diff != 0);
    out
}

/// Brute force: every matching pair of updates, once, at the join of their times.
///
/// This is the definition the assembled rules have to meet. It says nothing about *which* rule
/// produces a given pair — only that the pair is accounted exactly once overall, which is
/// precisely the property that double counting and dropped matches both violate.
fn oracle(atom0: &[Update], atom1: &[Update]) -> Vec<Joined> {
    let mut out = Vec::new();
    for ((key0, val0), time0, diff0) in atom0 {
        for ((key1, val1), time1, diff1) in atom1 {
            if key0 == key1 {
                out.push(((*key0, (*val0, *val1)), time0.join(time1), diff0 * diff1));
            }
        }
    }
    consolidate(out)
}

/// Compaction bound: one step back in every coordinate (see `Cut::Before`'s strictness).
fn step_back(time: &Time, antichain: &mut Antichain<Time>) {
    antichain.insert(Product::new(time.outer.saturating_sub(1), time.inner.saturating_sub(1)));
}

/// The assembled two-rule delta join, with cuts derived from atom positions.
fn delta_join(workers: usize, atom0: Vec<Update>, atom1: Vec<Update>) -> Vec<Joined> {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);

    timely::execute(timely::Config::process(workers), move |worker| {
        let is_source = worker.index() == 0;
        let (sink, atom0, atom1) = (Arc::clone(&sink), atom0.clone(), atom1.clone());

        let (mut i0, mut i1, c0, c1) = worker.dataflow::<usize, _, _>(|scope| {
            scope.scoped("Delta", |inner| {
                let ((input0, capability0), data0): ((UnorderedHandle<Time, Update>, _), _) = inner.new_unordered_input();
                let ((input1, capability1), data1): ((UnorderedHandle<Time, Update>, _), _) = inner.new_unordered_input();

                let edges0 = data0.as_collection();
                let edges1 = data1.as_collection();

                let arranged0 = edges0.clone().arrange_by_key();
                let arranged1 = edges1.clone().arrange_by_key();

                // Each rule's seed hoists its own time into the payload; the half join
                // compares against it and lifts the output onto it.
                let changes0 = edges0.inner.map(|((k, v), t, r)| ((k, v, t.clone()), t, r)).as_collection();
                let changes1 = edges1.inner.map(|((k, v), t, r)| ((k, v, t.clone()), t, r)).as_collection();

                // Rule 0 reads atom 1; rule 1 reads atom 0. Cuts are derived, not chosen.
                let rule0 = half_join(changes0, arranged1, step_back, Cut::for_positions(0, 1),
                    |key, val0, val1| (*key, (*val0, *val1)));
                let rule1 = half_join(changes1, arranged0, step_back, Cut::for_positions(1, 0),
                    |key, val1, val0| (*key, (*val0, *val1)));

                // Leaving the delta region: the carried time becomes the update's time.
                rule0.concat(rule1)
                    .inner.map(|((data, lifted), _order, diff)| (data, lifted, diff)).as_collection()
                    .inspect(move |x: &Joined| sink.lock().unwrap().push(*x));

                (input0, input1, capability0, capability1)
            })
        });

        if is_source {
            let mut session0 = i0.activate();
            let mut session0 = session0.session(&c0);
            for update in atom0.iter() { session0.give(*update); }
            drop(session0);
            let mut session1 = i1.activate();
            let mut session1 = session1.session(&c1);
            for update in atom1.iter() { session1.give(*update); }
        }
    })
    .unwrap();

    let captured = Arc::try_unwrap(captured).unwrap().into_inner().unwrap();
    consolidate(captured)
}

/// Every pair of updates accounted exactly once, at the join of its times, over random inputs.
#[test]
fn delta_join_matches_brute_force() {
    for workers in [1, 4] {
        let mut rng = Rng(0x5eed_1234_9abc_def1);
        for instance in 0..24 {
            let atom0 = generate(&mut rng, 6);
            let atom1 = generate(&mut rng, 6);

            let expected = oracle(&atom0, &atom1);
            let actual = delta_join(workers, atom0.clone(), atom1.clone());

            assert_eq!(
                actual, expected,
                "instance {instance} at {workers} worker(s)\n  atom0: {atom0:?}\n  atom1: {atom1:?}",
            );
        }
    }
}

/// The derivation itself: exactly one rule claims each combination of times.
///
/// Checked directly against the characterization in [`Cut::for_positions`] — the claiming rule
/// is the largest index achieving the `Ord`-maximum — over every assignment of times from a
/// small grid to three atoms, so ties and incomparable pairs are both covered exhaustively.
#[test]
fn exactly_one_rule_claims_each_combination() {
    let grid: Vec<Time> = (0..3).flat_map(|o| (0..3).map(move |i| Product::new(o, i))).collect();
    const ATOMS: usize = 3;

    for t0 in grid.iter() {
        for t1 in grid.iter() {
            for t2 in grid.iter() {
                let times = [t0, t1, t2];
                let claimants = (0..ATOMS)
                    .filter(|&seed| {
                        (0..ATOMS)
                            .filter(|&atom| atom != seed)
                            .all(|atom| Cut::for_positions(seed, atom).admits(times[atom], times[seed]))
                    })
                    .collect::<Vec<_>>();

                assert_eq!(
                    claimants.len(), 1,
                    "times {times:?} claimed by rules {claimants:?}, expected exactly one",
                );

                // And the claimant is the largest index achieving the maximum.
                let maximum = times.iter().max().unwrap();
                let expected = (0..ATOMS).filter(|&i| times[i] == *maximum).next_back().unwrap();
                assert_eq!(claimants[0], expected, "times {times:?}");
            }
        }
    }
}
