//! The partial-order test: carried interesting times (the *pending* path), which can
//! only arise under partially ordered timestamps and was the last unexercised clause
//! of the windowed reduce contract.
//!
//! With `Pair` times, updates at incomparable moments `(0,1)` and `(1,0)` make their
//! join `(1,1)` an interesting time. When `(1,1)` lies beyond the retire's upper
//! frontier it must be *pended*: returned in the frontier (holding the capability),
//! carried by the tactic, handed to the backend as a `pending` group next retire —
//! where the group must appear in a window even though it has no novel input — and
//! evaluated there, producing the correction that collapses the two partial counts.

mod common;

use std::collections::BTreeMap;

use timely::progress::Antichain;

use differential_dataflow::operators::int_proxy::ProxyReduceTactic;
use differential_dataflow::operators::reduce::ReduceTactic;

use common::pair::Pair;
use common::{accumulate, batch_frontiers, Diff, Edge, EdgeBatch, EdgeReduceBackend};

type Time = Pair<u64, u64>;

fn t(a: u64, b: u64) -> Time {
    Pair::new(a, b)
}

#[test]
fn pending_times_carry_across_retires() {
    // Key 7 receives updates at incomparable times in round A; key 3 receives novel
    // input only in round B, so round B's windows must merge a pending-only group
    // (7) with a novel-only group (3), in group order, at window size 1.
    for window in [1, 2, 100] {
        let mut tactic = ProxyReduceTactic::new(EdgeReduceBackend::new(window));

        // Round A: interval [ {(0,0)}, {(1,1)} ), capability held at (0,0).
        let input_a = batch_frontiers(
            vec![((7, 100), t(0, 1), 1), ((7, 200), t(1, 0), 1)],
            Antichain::from_elem(t(0, 0)),
            Antichain::from_elem(t(1, 1)),
        );
        let lower = Antichain::from_elem(t(0, 0));
        let upper = Antichain::from_elem(t(1, 1));
        let held = Antichain::from_elem(t(0, 0));
        let (produced_a, frontier_a) = tactic.retire(
            Vec::new(),
            Vec::new(),
            vec![input_a.clone()],
            &lower,
            &upper,
            &held,
        );
        // The joint interesting time (1,1) lies beyond upper: it must be deferred,
        // and the returned frontier must hold it.
        assert_eq!(frontier_a, Antichain::from_elem(t(1, 1)), "the join of incomparable updates must be pended");

        let mut outputs: Vec<EdgeBatch<Time>> = produced_a.into_iter().map(|(_, b)| b).collect();

        // Round B: interval [ {(1,1)}, {(2,2)} ), capability held at (1,1). Novel
        // input only for key 3; key 7 participates as a pending-only group.
        let input_b = batch_frontiers(
            vec![((3, 5), t(1, 1), 1)],
            Antichain::from_elem(t(1, 1)),
            Antichain::from_elem(t(2, 2)),
        );
        let lower = Antichain::from_elem(t(1, 1));
        let upper = Antichain::from_elem(t(2, 2));
        let held = Antichain::from_elem(t(1, 1));
        let (produced_b, frontier_b) = tactic.retire(
            vec![input_a.clone()],
            outputs.clone(),
            vec![input_b.clone()],
            &lower,
            &upper,
            &held,
        );
        assert!(frontier_b.is_empty(), "nothing further to defer");
        outputs.extend(produced_b.into_iter().map(|(_, b)| b));

        // Oracle over a grid of times, using the partial order's less_equal: the
        // accumulated output at t must be exactly the per-source counts of the
        // accumulated input at t. At (1,1) this requires round B's correction to
        // have collapsed the two partial counts of key 7 into one count of 2.
        let inputs: Vec<(Edge, Time, Diff)> = input_a
            .updates
            .iter()
            .chain(&input_b.updates)
            .cloned()
            .collect();
        for probe in [t(0, 0), t(0, 1), t(1, 0), t(1, 1), t(2, 1), t(2, 2)] {
            let input_acc = accumulate(inputs.iter().cloned(), &probe);
            let mut counts: BTreeMap<u32, Diff> = BTreeMap::new();
            for ((src, _dst), d) in &input_acc {
                *counts.entry(*src).or_insert(0) += d;
            }
            let mut expected: Vec<(Edge, Diff)> = Vec::new();
            for (src, c) in counts {
                if c > 0 {
                    expected.push(((src, c as u32), 1));
                }
            }
            let got = accumulate(outputs.iter().flat_map(|b| b.updates.iter().cloned()), &probe);
            assert_eq!(got, expected, "window={window} at probe={probe:?}");
        }
    }
}
