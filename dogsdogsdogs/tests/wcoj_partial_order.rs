//! The worst-case-optimal `extend` path over a *partially ordered* time.
//!
//! This is the case the payload time exists for. Times are compared totally, so a prefix
//! at `initial` matches records at times incomparable to it, and `lub(t2, payload)` then differs
//! from match to match. Two updates to the same extension therefore land at two *different*
//! output times rather than collapsing onto one, and no longer cancel.
//!
//! With a totally ordered time the two coincide and the distinction is invisible, which is why
//! `wcoj_triangle.rs` cannot see it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use timely::order::{PartialOrder, Product};
use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::vec::{Map, UnorderedInput};
use timely::progress::Antichain;
use differential_dataflow::AsCollection;
use differential_dataflow::lattice::Lattice;

use differential_dogs3::{CollectionIndex, ProposeExtensionMethod};

type Time = Product<usize, usize>;

/// Runs `Q(a,b,c) := E(a,b), E(b,c), E(a,c)` over `deltas`, and returns the output updates.
fn triangles(deltas: &[((u32, u32), Time, isize)]) -> Vec<((u32, u32, u32), Time, isize)> {
    let found = Arc::new(Mutex::new(Vec::new()));
    let found_outer = found.clone();
    let deltas = deltas.to_vec();

    timely::execute(timely::Config::thread(), move |worker| {
        let found_outer = found_outer.clone();
        let deltas = deltas.clone();
        let mut probe = Handle::new();

        // A `Product` root doesn't refine `()`, so the partially ordered times live one scope in.
        let (mut input, capability) = worker.dataflow::<usize, _, _>(|scope| {
            let (handles, output) = scope.scoped::<Time, _, _>("Times", |outer| {
                let ((input, capability), stream) = outer.new_unordered_input::<((u32, u32), Time, isize)>();
                let edges = stream.as_collection();

                let forward = edges.clone();
                let reverse = edges.map(|(x, y)| (y, x));

                // Hold compaction back one step in each coordinate, so that total-order
                // comparisons are not misled. Times compare in the total order on `Product`,
                // which is lexicographic and therefore a linear extension of the lattice.
                let frontier_func = |time: &Time, antichain: &mut Antichain<Time>| {
                    antichain.insert(Product::new(time.outer.saturating_sub(1), time.inner.saturating_sub(1)));
                };

                let index_forward = CollectionIndex::index(forward.clone(), frontier_func);
                let index_reverse = CollectionIndex::index(reverse.clone(), frontier_func);

                let deltas = forward.inner.map(|(d, t, r)| ((d, t.clone()), t, r)).as_collection();

                //   dQ/dE1 := dE1(a,b), E2(b,c), E3(a,c)
                let changes1 = deltas.clone()
                    .extend(&mut [
                        &mut index_forward.extend_using(|(_a, b)| *b, true),
                        &mut index_forward.extend_using(|(a, _b)| *a, true),
                    ])
                    .map(|(((a, b), c), payload)| ((a, b, c), payload));

                //   dQ/dE2 := dE2(b,c), E1(a,b), E3(a,c)
                let changes2 = deltas.clone()
                    .extend(&mut [
                        &mut index_reverse.extend_using(|(b, _c)| *b, false),
                        &mut index_reverse.extend_using(|(_b, c)| *c, true),
                    ])
                    .map(|(((b, c), a), payload)| ((a, b, c), payload));

                //   dQ/dE3 := dE3(a,c), E1(a,b), E2(b,c)
                let changes3 = deltas
                    .extend(&mut [
                        &mut index_forward.extend_using(|(a, _c)| *a, false),
                        &mut index_reverse.extend_using(|(_a, c)| *c, false),
                    ])
                    .map(|(((a, c), b), payload)| ((a, b, c), payload));

                // Delay updates to the payload time worked out while extending.
                let triangles = changes1.concat(changes2).concat(changes3)
                    .inner.map(|((d, payload), _time, r)| (d, payload, r)).as_collection();

                let left = triangles
                    .inspect_batch(move |_t, xs| {
                        let mut v = found_outer.lock().unwrap();
                        v.extend(xs.iter().cloned());
                    })
                    .leave(scope);

                ((input, capability), left)
            });
            output.probe_with(&mut probe);
            handles
        });

        input.activate().session(&capability).give_iterator(deltas.iter().cloned().map(|(e, t, r)| (e, t, r)));
        drop(capability);
        while worker.step() { }
    })
    .unwrap();

    let updates = found.lock().unwrap().clone();
    updates
}

/// Accumulates `updates` as of `time`, in the *lattice* order, discarding anything that cancels.
fn accumulate<D: Ord + Clone>(updates: &[(D, Time, isize)], time: &Time) -> BTreeSet<D> {
    let mut counts = BTreeMap::new();
    for (data, _, diff) in updates.iter().filter(|(_, when, _)| when.less_equal(time)) {
        *counts.entry(data.clone()).or_insert(0isize) += diff;
    }
    counts.into_iter().filter(|(_, c)| *c != 0).map(|(d, _)| d).collect()
}

/// The triangles of `edges`, by exhaustive search.
fn triangles_of(edges: &BTreeSet<(u32, u32)>) -> BTreeSet<(u32, u32, u32)> {
    let mut found = BTreeSet::new();
    for &(a, b) in edges.iter() {
        for &(b2, c) in edges.iter() {
            if b2 == b && edges.contains(&(a, c)) {
                found.insert((a, b, c));
            }
        }
    }
    found
}

/// A deterministic xorshift, so a failing seed can be replayed.
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn wcoj_matches_exhaustive_search_at_incomparable_times() {
    const NODES: u32 = 4;
    const COORD: usize = 3;

    let universe: Vec<(u32, u32)> = (0..NODES).flat_map(|a| (0..NODES).filter(move |b| *b != a).map(move |b| (a, b))).collect();

    // Times spread over both coordinates, so that many pairs are incomparable.
    let times: Vec<Time> = (0..COORD).flat_map(|o| (0..COORD).map(move |i| Product::new(o, i))).collect();

    for seed in 1..=12u64 {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);

        // Each edge gets an independent life: a time it arrives, and possibly a later time in
        // the total order at which it departs. An edge that comes and goes between two times
        // incomparable to a third is exactly the case a summed count cannot see.
        let mut deltas = Vec::new();
        for edge in universe.iter() {
            if next(&mut state) % 3 == 0 { continue; }
            let birth = times[(next(&mut state) as usize) % times.len()].clone();
            deltas.push((*edge, birth.clone(), 1));
            if next(&mut state) % 2 == 0 {
                let death = times[(next(&mut state) as usize) % times.len()].clone();
                // Retract only strictly later in the lattice, to keep the collection a set.
                if birth.less_than(&death) {
                    deltas.push((*edge, death, -1));
                }
            }
        }

        let updates = triangles(&deltas);

        // Check at every input time and at every pairwise join of them, which is where the
        // payload times land.
        let mut probes: BTreeSet<Time> = deltas.iter().map(|(_, t, _)| t.clone()).collect();
        for t1 in probes.clone().iter() {
            for t2 in probes.clone().iter() {
                probes.insert(t1.join(t2));
            }
        }

        for time in probes.iter() {
            let expected = triangles_of(&accumulate(&deltas, time));
            let actual = accumulate(&updates, time);
            assert_eq!(
                actual, expected,
                "seed {} time {:?}\n  deltas {:?}\n  extend gave {:?}\n  search gave {:?}",
                seed, time, deltas, actual, expected,
            );
        }
    }
}

#[test]
fn extension_that_lives_and_dies_inside_the_comparison_interval() {
    // Times: (0,0) < (0,2) < (1,0) in the *total* order, but (0,2) and (1,0) are incomparable
    // in the lattice. So an edge born at (0,0) and retracted at (0,2) is still present at (1,0),
    // while both of its updates lie inside the comparison interval of a delta at (1,0).
    //
    // Node 0's only in-edge into 2 is (0,2), so the count for that key sums to +1 - 1 = 0 even
    // though the edge is live at (1,0). A count that gates on a non-zero sum drops the prefix
    // and loses the triangle.
    let deltas = vec![
        ((0, 1), Product::new(0, 0), 1),
        ((0, 2), Product::new(0, 0), 1),
        ((0, 2), Product::new(0, 2), -1),
        ((1, 2), Product::new(1, 0), 1),
    ];

    let updates = triangles(&deltas);

    for time in [Product::new(0, 0), Product::new(0, 2), Product::new(1, 0), Product::new(1, 2)] {
        let expected = triangles_of(&accumulate(&deltas, &time));
        let actual = accumulate(&updates, &time);
        assert_eq!(
            actual, expected,
            "time {:?}: extend gave {:?}, search gave {:?}\n  all updates {:?}",
            time, actual, expected, updates,
        );
    }
}
