//! Benchmarks the cursor join tactic across the key shapes its two strategies cover.
//!
//! The tactic runs a key to completion in one call when the key's cross product is small, and
//! otherwise reloads the key and replays it a container at a time so that it can suspend within the
//! key. The workloads here bracket that choice:
//! * `unique` — one value per key on each side: the per-key overhead, where a join spends its time
//!   opening and retiring keys rather than crossing them.
//! * `fanout` — a handful of values per key on each side: still a key per call, with real work in it.
//! * `warm-key` — a single key just under the threshold: the largest key run in one call.
//! * `hot-key` — a single key well over the threshold: the suspending path, and the shape that
//!   buffered its whole cross product before the tactic could suspend within a key.
//!
//! Each workload loads its inputs, then measures the round that joins them. Run with:
//! ```text
//! cargo test --release --test join_bench -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::Probe;

use differential_dataflow::input::InputSession;

/// The `(key, value)` pairs of one side of a join: `keys` keys, each carrying `vals` values.
fn side(keys: u64, vals: u64) -> impl Iterator<Item = (u64, u64)> {
    (0..keys).flat_map(move |key| (0..vals).map(move |val| (key, val)))
}

/// Joins `keys` keys of `vals1` by `vals2` values, returning how long the joining round took.
///
/// The inputs are loaded and flushed at time `0`, and the round timed is the one that advances to
/// time `1`, so the measurement covers the join and not the arrangement of its inputs.
fn join_round(keys: u64, vals1: u64, vals2: u64) -> Duration {
    let elapsed = timely::execute_directly(move |worker| {
        let probe = ProbeHandle::new();
        let mut input1 = <InputSession<u64, (u64, u64), isize>>::new();
        let mut input2 = <InputSession<u64, (u64, u64), isize>>::new();
        worker.dataflow(|scope| {
            let collection1 = input1.to_collection(scope);
            let collection2 = input2.to_collection(scope);
            collection1
                .arrange_by_key()
                .join_core(collection2.arrange_by_key(), |key, val1: &u64, val2: &u64| {
                    Some((*key, val1 ^ val2))
                })
                .inner
                .probe_with(&probe);
        });

        // Load both sides at time `0`, and let the join of an empty side against them settle.
        for (key, val) in side(keys, vals1) { input1.insert((key, val)); }
        for (key, val) in side(keys, vals2) { input2.insert((key, val)); }
        input1.advance_to(1);
        input2.advance_to(1);
        input1.flush();
        input2.flush();

        let start = Instant::now();
        worker.step_while(|| probe.less_than(input1.time()));
        start.elapsed()
    });
    elapsed
}

/// Runs one workload and reports its shape, its match count, and the time the joining round took.
fn report(name: &str, keys: u64, vals1: u64, vals2: u64) {
    let elapsed = join_round(keys, vals1, vals2);
    let matches = keys * vals1 * vals2;
    println!(
        "{name:<10} keys {keys:>8}  vals {vals1:>7} x {vals2:<7}  matches {matches:>10}  {elapsed:>12.3?}",
    );
}

#[test]
#[ignore]
fn join_bench() {
    // Sized against the tactic's threshold, which is half the join driver's fuel: 1,000,000 matches.
    report("unique", 1_000_000, 1, 1);
    report("fanout", 100_000, 8, 8);
    report("warm-key", 1, 900, 900);
    report("hot-key", 1, 1_500, 1_500);
    report("hot-key", 1, 3_000, 3_000);
}
