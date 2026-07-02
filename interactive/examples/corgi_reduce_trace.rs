//! M2(b)-3: the Route-B reduce MECHANISM, end-to-end over a manually maintained corgi trace —
//! before the arrange_core/reduce_with_tactic timely plumbing. A trace is an accumulated
//! `CorgiBatch` (merged via `merge`, consolidated per (key,val)); the reducer is a corgi program
//! (Count = GroupKey + fold_add over the diff column, so retractions just subtract). We apply
//! batches AND retractions and check the corgi-reduced output matches a reference each step.
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_reduce_trace

use std::collections::BTreeMap;

use timely::progress::Antichain;
use differential_dataflow::trace::Description;

use interactive::corgi_arrange::{merge, CorgiBatch};

use corgi::{ArithOp, Builder, CmpOp, Graph, NumOp, Op, Red, Value as CValue};

/// Build a single-time (t=0) CorgiBatch from `((key,val), diff)` updates.
fn make_batch(updates: &[((u64, u64), i64)]) -> CorgiBatch<u64, i64> {
    let keys = CValue::u64(updates.iter().map(|u| u.0 .0).collect());
    let vals = CValue::u64(updates.iter().map(|u| u.0 .1).collect());
    let times = vec![0u64; updates.len()];
    let diffs = updates.iter().map(|u| u.1).collect();
    let desc = Description::new(Antichain::from_elem(0), Antichain::from_elem(1), Antichain::from_elem(0));
    CorgiBatch::from_unsorted(keys, vals, times, diffs, desc)
}

/// corgi Count reducer: `input: List<(key, diff)> -> group -> map((k, ds) -> (k, ds fold_add))`.
fn count_program() -> Graph<NumOp> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let grouped = b.add(CmpOp::GroupKey, vec![input]);
    let body = {
        let mut bb = Builder::<NumOp>::default();
        let e = bb.input();
        let k = bb.add(Op::Field(0), vec![e]);
        let ds = bb.add(Op::Field(1), vec![e]);
        let sum = bb.add(ArithOp::Reduce(Red::Add), vec![ds]); // fold_add — u64 add == i64 add on bits
        let out = bb.tuple(vec![k, sum]);
        bb.finish(out)
    };
    let mapped = b.add(Op::MapList(Box::new(body)), vec![grouped]);
    b.finish(mapped)
}

/// Run the corgi Count reducer over the trace; key -> net count (nonzero only).
fn reduce_count(trace: &CorgiBatch<u64, i64>, g: &Graph<NumOp>) -> BTreeMap<u64, i64> {
    let n = trace.times.len();
    if n == 0 {
        return BTreeMap::new();
    }
    let diffcol = CValue::u64(trace.diffs.iter().map(|&d| d as u64).collect());
    let pairs = CValue::Prod(vec![trace.keys.clone(), diffcol]);
    let input = CValue::List(vec![n].into(), Box::new(pairs));
    let out = corgi::eval_graph(g, input);
    let (_b, inner) = out.into_list("count out");
    let cols = inner.into_prod("kc");
    let ks = cols[0].clone().into_u64("k");
    let cs = cols[1].clone().into_u64("c");
    ks.into_iter()
        .zip(cs)
        .map(|(k, c)| (k, c as i64))
        .filter(|(_, c)| *c != 0)
        .collect()
}

fn main() {
    let g = count_program();
    let z = Antichain::from_elem(0u64);

    // A reference: cumulative diff per key over all applied updates.
    let mut ref_counts: BTreeMap<u64, i64> = BTreeMap::new();
    let apply_ref = |ups: &[((u64, u64), i64)], r: &mut BTreeMap<u64, i64>| {
        for ((k, _v), d) in ups {
            *r.entry(*k).or_insert(0) += d;
        }
        r.retain(|_, c| *c != 0);
    };

    let steps: Vec<Vec<((u64, u64), i64)>> = vec![
        vec![((1, 10), 1), ((1, 20), 1), ((2, 30), 1)],     // +: {1:2, 2:1}
        vec![((1, 10), -1), ((3, 40), 1)],                   // retract one of key 1; add key 3 -> {1:1,2:1,3:1}
        vec![((1, 20), -1)],                                 // key 1 -> 0, disappears -> {2:1,3:1}
        vec![((2, 30), 2), ((2, 31), 1)],                    // key 2 multi-val -> {2:4,3:1}
    ];

    let mut trace: Option<CorgiBatch<u64, i64>> = None;
    for (i, step) in steps.iter().enumerate() {
        let nb = make_batch(step);
        trace = Some(match trace.take() {
            None => nb,
            Some(t) => merge(&t, &nb, z.borrow()),
        });
        let got = reduce_count(trace.as_ref().unwrap(), &g);
        apply_ref(step, &mut ref_counts);
        assert_eq!(got, ref_counts, "step {i}: corgi reduce != reference");
        println!("  step {i}: {:?}  OK", got);
    }

    println!("\nM2(b)-3: Route-B reduce MECHANISM verified incrementally (adds + retractions).");
    println!("  trace = accumulated CorgiBatch (merged over corgi columns); reducer = corgi Count");
    println!("  program; each step's corgi output == reference. Next: wire into reduce_with_tactic.");
}
