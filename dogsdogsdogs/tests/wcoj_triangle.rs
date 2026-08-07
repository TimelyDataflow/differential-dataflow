//! End-to-end checks of the worst-case-optimal `extend` path.
//!
//! Both tests drive a triangle query through `extend` -> `count`/`propose`/`validate` ->
//! `half_join`. That path carries a payload time: each delta enters holding its own time, the
//! extension stages join the times of the records they match into it, and the result is delayed
//! to that payload on the way out.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use timely::dataflow::operators::probe::Handle;
use timely::dataflow::operators::vec::Map;
use timely::progress::Antichain;
use differential_dataflow::AsCollection;
use differential_dataflow::input::Input;

use differential_dogs3::{CollectionIndex, altneu::AltNeu, ProposeExtensionMethod};

/// Runs `Q(a,b,c) := E(a,b), E(b,c), E(a,c)` over `deltas`, and returns the output updates.
///
/// Updates are applied at the times they carry, and the computation is run out to `rounds`.
fn triangles(deltas: &[((u32, u32), usize, isize)], rounds: usize) -> Vec<((u32, u32, u32), usize, isize)> {
    let found = Arc::new(Mutex::new(Vec::new()));
    let found_outer = found.clone();
    let deltas = deltas.to_vec();

    timely::execute(timely::Config::thread(), move |worker| {
        let found_outer = found_outer.clone();
        let deltas = deltas.clone();
        let mut probe = Handle::new();

        let mut input = worker.dataflow::<usize, _, _>(|scope| {
            let (input, edges) = scope.new_collection::<(u32, u32), isize>();

            let forward = edges.clone();
            let reverse = edges.map(|(x, y)| (y, x));

            let triangles = scope.scoped::<AltNeu<usize>, _, _>("Triangles", |inner| {
                let forward = forward.enter(inner);
                let reverse = reverse.enter(inner);

                // Hold compaction back one base-time step so the alt/neu distinction survives,
                // and compare in the total order on `AltNeu` (lexicographic on `(time, neu)`).
                let frontier_func = |time: &AltNeu<usize>, antichain: &mut Antichain<AltNeu<usize>>| {
                    antichain.insert(AltNeu::alt(time.time.saturating_sub(1)));
                };
                let comparison = |t1: &AltNeu<usize>, t2: &AltNeu<usize>| t1 <= t2;

                let alt_forward = CollectionIndex::index(forward.clone(), frontier_func, comparison);
                let alt_reverse = CollectionIndex::index(reverse.clone(), frontier_func, comparison);
                let neu_forward = CollectionIndex::index(forward.clone().delay(|t| AltNeu::neu(t.time.clone())), frontier_func, comparison);
                let neu_reverse = CollectionIndex::index(reverse.clone().delay(|t| AltNeu::neu(t.time.clone())), frontier_func, comparison);

                let deltas = forward.inner.map(|(d, t, r)| ((d, t.clone()), t, r)).as_collection();

                //   dQ/dE1 := dE1(a,b), E2(b,c), E3(a,c)
                let changes1 = deltas.clone()
                    .extend(&mut [
                        &mut neu_forward.extend_using(|(_a, b)| *b),
                        &mut neu_forward.extend_using(|(a, _b)| *a),
                    ])
                    .map(|(((a, b), c), payload)| ((a, b, c), payload));

                //   dQ/dE2 := dE2(b,c), E1(a,b), E3(a,c)
                let changes2 = deltas.clone()
                    .extend(&mut [
                        &mut alt_reverse.extend_using(|(b, _c)| *b),
                        &mut neu_reverse.extend_using(|(_b, c)| *c),
                    ])
                    .map(|(((b, c), a), payload)| ((a, b, c), payload));

                //   dQ/dE3 := dE3(a,c), E1(a,b), E2(b,c)
                let changes3 = deltas
                    .extend(&mut [
                        &mut alt_forward.extend_using(|(a, _c)| *a),
                        &mut alt_reverse.extend_using(|(_a, c)| *c),
                    ])
                    .map(|(((a, c), b), payload)| ((a, b, c), payload));

                // Delay updates to the payload time worked out while extending.
                changes1.concat(changes2).concat(changes3)
                    .inner.map(|((d, payload), _time, r)| (d, payload, r)).as_collection()
                    .leave(scope)
            });

            triangles
                .inspect_batch(move |_t, xs| {
                    let mut v = found_outer.lock().unwrap();
                    v.extend(xs.iter().cloned());
                })
                .probe_with(&mut probe);

            input
        });

        for round in 0..rounds {
            input.advance_to(round);
            for (edge, _, diff) in deltas.iter().filter(|(_, time, _)| *time == round) {
                input.update(*edge, *diff);
            }
        }
        input.advance_to(rounds);
        input.flush();
        while probe.less_than(input.time()) {
            worker.step();
        }
    })
    .unwrap();

    let updates = found.lock().unwrap().clone();
    updates
}

/// Accumulates `updates` as of `time`, discarding anything that cancels.
fn accumulate<D: Ord + Clone>(updates: &[(D, usize, isize)], time: usize) -> BTreeSet<D> {
    let mut counts = std::collections::BTreeMap::new();
    for (data, _, diff) in updates.iter().filter(|(_, when, _)| *when <= time) {
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

#[test]
fn wcoj_extend_finds_triangle() {
    // A single triangle: directed edges 0->1, 1->2, 0->2, all at time 0.
    let deltas = vec![((0, 1), 0, 1), ((1, 2), 0, 1), ((0, 2), 0, 1)];
    let mut got: Vec<_> = triangles(&deltas, 1).into_iter().filter(|(_, _, r)| *r > 0).map(|(d, _, _)| d).collect();
    got.sort();
    // Not deduplicated: all three edges arrive at the same time, so every pair of them ties
    // and the alt/neu discipline is what keeps exactly one delta fragment from producing the
    // triangle. Deduplicating here would hide a fragment producing it a second time.
    assert_eq!(got, vec![(0, 1, 2)], "expected the triangle (0,1,2) exactly once; got {:?}", got);
}

/// A deterministic xorshift, so a failing seed can be replayed.
fn next(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn wcoj_matches_exhaustive_search_under_churn() {
    const NODES: u32 = 5;
    const ROUNDS: usize = 6;

    let universe: Vec<(u32, u32)> = (0..NODES).flat_map(|a| (0..NODES).filter(move |b| *b != a).map(move |b| (a, b))).collect();

    for seed in 1..=20u64 {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);

        // Draw a fresh edge set for each round, and record the differences between rounds.
        // Drawing states rather than differences keeps the collection a set at every time,
        // which is what these operators are documented to expect, while still exercising
        // edges that arrive and depart -- including within a single comparison interval.
        let mut deltas = Vec::new();
        let mut previous: BTreeSet<(u32, u32)> = BTreeSet::new();
        let mut states = Vec::new();
        for round in 0..ROUNDS {
            let current: BTreeSet<(u32, u32)> = universe.iter().filter(|_| next(&mut state) % 3 != 0).cloned().collect();
            for edge in current.difference(&previous) { deltas.push((*edge, round, 1)); }
            for edge in previous.difference(&current) { deltas.push((*edge, round, -1)); }
            states.push(current.clone());
            previous = current;
        }

        let updates = triangles(&deltas, ROUNDS);

        for round in 0..ROUNDS {
            let expected = triangles_of(&states[round]);
            let actual = accumulate(&updates, round);
            assert_eq!(
                actual, expected,
                "seed {} round {}: edges {:?}\n  extend gave {:?}\n  search gave {:?}",
                seed, round, states[round], actual, expected,
            );
        }
    }
}
