//! Behavioral tests for the proxy tactics against the shared edge-batch backend.
//!
//! The value-token instantiation from the seam design: data is edges `(src, dst)`,
//! grouped by source. `Group = u32` is the source itself (exact — no hashing, no
//! collisions), and the value token is the whole edge (self-redeeming: `cross` and
//! `finish` build outputs straight from tokens without consulting storage). Window
//! sizes are kept tiny to force many windows and exercise the streaming protocols.
//! The scaffold lives in `tests/common/mod.rs`, shared with the partial-order test.

mod common;

use std::collections::BTreeMap;

use timely::progress::Antichain;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::operators::int_proxy::{ProxyJoinTactic, ProxyReduceTactic};
use differential_dataflow::operators::join::{Fresh, JoinTactic};
use differential_dataflow::operators::reduce::ReduceTactic;

use common::{accumulate, batch, naive_join, Diff, Edge, EdgeBatch, EdgeJoinBackend, EdgeReduceBackend};

type Time = u64;
type GraphBatch = EdgeBatch<Time>;

#[test]
fn join_matches_naive() {
    // Edges spread over several groups, several batches per side, including a
    // retraction; window size 1 forces one group per window.
    let a0 = batch(vec![((0, 1), 0, 1), ((0, 2), 1, 1), ((1, 5), 0, 1), ((3, 9), 2, 1)], 0, 3);
    let a1 = batch(vec![((0, 2), 2, -1), ((2, 4), 2, 1)], 0, 3);
    let b0 = batch(vec![((0, 7), 1, 1), ((1, 8), 2, 1), ((2, 6), 0, 1)], 0, 3);
    let b1 = batch(vec![((1, 8), 2, 1), ((4, 4), 0, 1)], 0, 3);

    let all_a: Vec<_> = a0.updates.iter().chain(&a1.updates).cloned().collect();
    let all_b: Vec<_> = b0.updates.iter().chain(&b1.updates).cloned().collect();
    let expected = naive_join(&all_a, &all_b);

    for window in [1, 2, 100] {
        for fresh in [Fresh::Input0, Fresh::Input1] {
            let mut tactic = ProxyJoinTactic::new(EdgeJoinBackend { window });
            let work = tactic.prep(
                vec![a0.clone(), a1.clone()],
                vec![b0.clone(), b1.clone()],
                fresh,
                0,
            );
            let mut got: Vec<((Edge, Edge), Time, Diff)> = work
                .flatten()
                .map(|(l, r, t, d)| ((l, r), t, d))
                .collect();
            consolidate_updates(&mut got);
            assert_eq!(got, expected, "window={window}");
        }
    }
}

#[test]
fn interleaved_units_share_backend() {
    // Two units prepped from one tactic and drained alternately: the scenario that
    // motivated per-unit `Cursor` state and the shared `Rc<RefCell>` backend — the
    // driver holds half-drained units from both queues and polls them under fuel.
    let a = vec![
        batch(vec![((0, 1), 0, 1), ((1, 5), 0, 1), ((2, 2), 1, 1)], 0, 2),
        batch(vec![((3, 3), 1, 1)], 0, 2),
    ];
    let b = vec![batch(vec![((0, 7), 1, 1), ((2, 6), 0, 1), ((3, 8), 1, 1)], 0, 2)];
    let c = vec![batch(vec![((5, 1), 0, 1), ((6, 2), 0, 1)], 0, 2)];
    let d = vec![
        batch(vec![((5, 9), 1, 1), ((6, 4), 0, 1)], 0, 2),
        batch(vec![((5, 9), 1, 1)], 0, 2),
    ];

    let all = |bs: &[GraphBatch]| bs.iter().flat_map(|b| b.updates.iter().cloned()).collect::<Vec<_>>();
    let expected1 = naive_join(&all(&a), &all(&b));
    let expected2 = naive_join(&all(&c), &all(&d));

    let mut tactic = ProxyJoinTactic::new(EdgeJoinBackend { window: 1 });
    let mut u1 = tactic.prep(a, b, Fresh::Input0, 0);
    let mut u2 = tactic.prep(c, d, Fresh::Input1, 0);
    let mut got1: Vec<((Edge, Edge), Time, Diff)> = Vec::new();
    let mut got2: Vec<((Edge, Edge), Time, Diff)> = Vec::new();
    let (mut done1, mut done2) = (false, false);
    while !done1 || !done2 {
        if !done1 {
            match u1.next() {
                Some(out) => got1.extend(out.into_iter().map(|(l, r, t, d)| ((l, r), t, d))),
                None => done1 = true,
            }
        }
        if !done2 {
            match u2.next() {
                Some(out) => got2.extend(out.into_iter().map(|(l, r, t, d)| ((l, r), t, d))),
                None => done2 = true,
            }
        }
    }
    consolidate_updates(&mut got1);
    consolidate_updates(&mut got2);
    assert_eq!(got1, expected1);
    assert_eq!(got2, expected2);
}

/// Per-time count oracle shared by the reduce tests.
fn assert_counts(source: &[GraphBatch], outputs: &[GraphBatch], t: Time, label: &str) {
    let input_acc = accumulate(source.iter().flat_map(|b| b.updates.iter().cloned()), &t);
    let mut counts: BTreeMap<u32, Diff> = BTreeMap::new();
    for ((src, _dst), d) in &input_acc {
        *counts.entry(*src).or_insert(0) += d;
    }
    let mut expected: Vec<(Edge, Diff)> = Vec::new();
    for (src, c) in counts {
        assert!(c >= 0);
        if c > 0 {
            expected.push(((src, c as u32), 1));
        }
    }
    let got = accumulate(outputs.iter().flat_map(|b| b.updates.iter().cloned()), &t);
    assert_eq!(got, expected, "{label} at time={t}");
}

#[test]
fn reduce_multitime_rounds() {
    // Intervals of width two, two distinct times per batch: under total order this
    // exercises the slow (discovery) path even with the single-moment fast path in
    // place — a key with two distinct seed times cannot take it — and mixes fast
    // and slow keys within one window.
    let rounds: Vec<(Vec<(Edge, Time, Diff)>, Time, Time)> = vec![
        (vec![((0, 1), 0, 1), ((0, 2), 1, 1), ((1, 5), 0, 1), ((1, 6), 1, 1)], 0, 2),
        (vec![((0, 3), 2, 1), ((1, 5), 3, -1), ((2, 7), 2, 1), ((2, 8), 3, 1)], 2, 4),
    ];
    for window in [1, 100] {
        let mut tactic = ProxyReduceTactic::new(EdgeReduceBackend::new(window));
        let mut source: Vec<GraphBatch> = Vec::new();
        let mut outputs: Vec<GraphBatch> = Vec::new();
        for (updates, lo, hi) in rounds.iter() {
            let input = batch(updates.clone(), *lo, *hi);
            let lower = Antichain::from_elem(*lo);
            let upper = Antichain::from_elem(*hi);
            let held = Antichain::from_elem(*lo);
            let (produced, frontier) = tactic.retire(
                source.clone(),
                outputs.clone(),
                vec![input.clone()],
                &lower,
                &upper,
                &held,
            );
            assert!(frontier.is_empty(), "total order should defer nothing");
            for (time, b) in produced {
                assert!(held.elements().contains(&time));
                outputs.push(b);
            }
            source.push(input);
        }
        for t in 0..4u64 {
            assert_counts(&source, &outputs, t, &format!("multitime window={window}"));
        }
    }
}

#[test]
fn reduce_counts_match_naive() {
    // Rounds of edge updates, including deletions that drop a source's count to zero
    // (full retraction) and change another's (correction).
    let rounds: Vec<Vec<(Edge, Time, Diff)>> = vec![
        vec![((0, 1), 0, 1), ((0, 2), 0, 1), ((1, 5), 0, 1)],
        vec![((0, 3), 1, 1), ((2, 7), 1, 1), ((1, 5), 1, -1)],
        vec![((2, 8), 2, 1), ((0, 2), 2, -1), ((3, 4), 2, 1)],
    ];

    for window in [1, 2, 100] {
        let mut tactic = ProxyReduceTactic::new(EdgeReduceBackend::new(window));
        let mut source: Vec<GraphBatch> = Vec::new();
        let mut outputs: Vec<GraphBatch> = Vec::new();

        for (r, updates) in rounds.iter().enumerate() {
            let r = r as Time;
            let input = batch(updates.clone(), r, r + 1);
            let lower = Antichain::from_elem(r);
            let upper = Antichain::from_elem(r + 1);
            let held = Antichain::from_elem(r);
            let (produced, frontier) = tactic.retire(
                source.clone(),
                outputs.clone(),
                vec![input.clone()],
                &lower,
                &upper,
                &held,
            );
            assert!(frontier.is_empty(), "total order should defer nothing");
            for (time, b) in produced {
                assert!(held.elements().contains(&time));
                outputs.push(b);
            }
            source.push(input);
        }

        // Oracle: at each round boundary, the accumulated output is exactly the
        // per-source counts of the accumulated input.
        for t in 0..rounds.len() as Time {
            let input_acc = accumulate(source.iter().flat_map(|b| b.updates.iter().cloned()), &t);
            let mut expected: Vec<(Edge, Diff)> = Vec::new();
            let mut counts: BTreeMap<u32, Diff> = BTreeMap::new();
            for ((src, _dst), d) in &input_acc {
                *counts.entry(*src).or_insert(0) += d;
            }
            for (src, c) in counts {
                assert!(c >= 0);
                if c > 0 {
                    expected.push(((src, c as u32), 1));
                }
            }
            let got = accumulate(outputs.iter().flat_map(|b| b.updates.iter().cloned()), &t);
            assert_eq!(got, expected, "window={window} at time={t}");
        }
    }
}
