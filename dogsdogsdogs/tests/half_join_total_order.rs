//! What the total order buys, and that the rewritten `half_join` still buys it.
//!
//! A delta join splits a two-way join into two halves, and each matching *pair* of updates
//! must be accounted by exactly one half. The halves decide by comparing the two updates'
//! times, and these tests pin the two ways that can go wrong:
//!
//! * **The order must be total.** Under the partial order, two updates at incomparable times
//!   satisfy neither `a ⪯ b` nor `b ⪯ a`, so neither half claims the pair and the match is
//!   silently lost. `Ord` refines `PartialOrder` and is total, so exactly one half claims it.
//!   [`incomparable_times`] is the witness: `(0,13)` and `(11,0)` are incomparable, and their
//!   match is found only because the comparison is `Ord`.
//! * **The halves must disagree on ties.** When the two times are *equal*, a total order alone
//!   does not decide. One half must use [`Cut::Before`] and the other [`Cut::AtOrBefore`]; two
//!   lax halves both claim the pair (double count) and two strict halves claim neither (loss).
//!
//! Times here are `Product<usize, usize>`, which is genuinely partially ordered — the setting
//! where these distinctions have teeth and where the original bug was found. Every case runs
//! at one and four workers.

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
type Update = ((usize, usize), Time, isize);
type Output = ((usize, (usize, usize)), Time, isize);

/// Compaction bound for these cuts: one step back in every coordinate.
///
/// `Cut::Before` is strict, and logical compaction destroys strictness — it can advance an
/// update at `t < initial` up to exactly `initial`, after which the half no longer claims a
/// pair it should have. The bound must therefore sit strictly below every time still held,
/// which is a strict predecessor the lattice cannot supply, so the caller writes it.
fn step_back(time: &Time, antichain: &mut Antichain<Time>) {
    antichain.insert(Product::new(time.outer.saturating_sub(1), time.inner.saturating_sub(1)));
}

/// The two edges each scenario feeds, one to each input.
struct Scenario {
    left: Update,
    right: Update,
}

/// Incomparable times: neither `(0,13) ⪯ (11,0)` nor the converse holds.
fn incomparable_times() -> Scenario {
    Scenario {
        left: ((5, 6), Product::new(0, 13), 1),
        right: ((5, 7), Product::new(11, 0), 1),
    }
}

/// Equal times: the total order does not decide, so the cuts' strictness must.
fn equal_times() -> Scenario {
    Scenario {
        left: ((5, 6), Product::new(5, 5), 1),
        right: ((5, 7), Product::new(5, 5), 1),
    }
}

/// Runs the two-half delta join with the rewritten `half_join`, at the given cuts.
fn run_new(workers: usize, scenario: &Scenario, cut1: Cut, cut2: Cut) -> Vec<Output> {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let (left, right) = (scenario.left.clone(), scenario.right.clone());

    timely::execute(timely::Config::process(workers), move |worker| {
        let is_source = worker.index() == 0;
        let sink = Arc::clone(&sink);

        let (mut i1, mut i2, c1, c2) = worker.dataflow::<usize, _, _>(|scope| {
            scope.scoped("Inner", |inner| {
                let ((input1, capability1), data1): ((UnorderedHandle<Time, Update>, _), _) = inner.new_unordered_input();
                let ((input2, capability2), data2): ((UnorderedHandle<Time, Update>, _), _) = inner.new_unordered_input();

                let edges1 = data1.as_collection();
                let edges2 = data2.as_collection();

                let forward1 = edges1.clone().arrange_by_key();
                let forward2 = edges2.clone().arrange_by_key();

                // Hoist each update's own time into its payload; the half join compares
                // against it and lifts the output onto it.
                let changes1 = edges1.inner.map(|((k, v), t, r)| ((k, v, t.clone()), t, r)).as_collection();
                let changes2 = edges2.inner.map(|((k, v), t, r)| ((k, v, t.clone()), t, r)).as_collection();

                let path1 = half_join(changes1, forward2, step_back, cut1,
                    |key, val1, val2| (key.clone(), (val1.clone(), val2.clone())));
                let path2 = half_join(changes2, forward1, step_back, cut2,
                    |key, val1, val2| (key.clone(), (val2.clone(), val1.clone())));

                path1.concat(path2)
                    .inner.map(|(((k, v), t), _, r)| ((k, v), t, r)).as_collection()
                    .inspect(move |x: &Output| sink.lock().unwrap().push(x.clone()));

                (input1, input2, capability1, capability2)
            })
        });

        if is_source {
            i1.activate().session(&c1).give(left.clone());
            i2.activate().session(&c2).give(right.clone());
        }
    }).unwrap();

    let mut out = Arc::try_unwrap(captured).unwrap().into_inner().unwrap();
    out.sort();
    out
}

/// The premise: incomparable times are ordered by `Ord` and by nothing else.
///
/// This is why a delta join cannot use `PartialOrder::less_equal` as its comparison. Neither
/// half would claim the pair below, and the match would never be produced.
#[test]
fn partial_order_cannot_decide_incomparable_times() {
    use timely::order::PartialOrder;
    let a = Product::new(0usize, 13usize);
    let b = Product::new(11usize, 0usize);

    // Neither direction holds in the partial order: no half claims the pair.
    assert!(!PartialOrder::less_equal(&a, &b));
    assert!(!PartialOrder::less_equal(&b, &a));

    // `Ord` decides, and consistently with the partial order where that has an opinion.
    assert!(a < b);
    assert_eq!(a.join(&b), Product::new(11, 13));
}

/// Incomparable times: the pair is matched, exactly once, at the join of the two times.
#[test]
fn incomparable_times_match_exactly_once() {
    for workers in [1, 4] {
        let out = run_new(workers, &incomparable_times(), Cut::Before, Cut::AtOrBefore);
        assert_eq!(
            out,
            vec![((5, (6, 7)), Product::new(11, 13), 1)],
            "incomparable times, {workers} worker(s)",
        );
    }
}

/// Equal times, opposite strictness: the pair is matched exactly once.
#[test]
fn equal_times_match_exactly_once() {
    for workers in [1, 4] {
        let out = run_new(workers, &equal_times(), Cut::Before, Cut::AtOrBefore);
        assert_eq!(
            out,
            vec![((5, (6, 7)), Product::new(5, 5), 1)],
            "equal times, {workers} worker(s)",
        );
    }
}

/// Equal times, both halves lax: both claim the pair, and it is produced twice.
///
/// Pins that the strictness split is load-bearing rather than stylistic.
#[test]
fn equal_times_both_lax_double_counts() {
    for workers in [1, 4] {
        let out = run_new(workers, &equal_times(), Cut::AtOrBefore, Cut::AtOrBefore);
        let total: isize = out.iter().map(|(_, _, r)| r).sum();
        assert_eq!(total, 2, "equal times, both lax, {workers} worker(s): {out:?}");
    }
}

/// Equal times, both halves strict: neither claims the pair, and it is lost.
#[test]
fn equal_times_both_strict_drops_the_match() {
    for workers in [1, 4] {
        let out = run_new(workers, &equal_times(), Cut::Before, Cut::Before);
        assert!(out.is_empty(), "equal times, both strict, {workers} worker(s): {out:?}");
    }
}
