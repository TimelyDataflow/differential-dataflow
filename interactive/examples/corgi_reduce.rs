//! M2(a): corgi can EXPRESS a reduce — group-by-key then fold each group — as one corgi program.
//! This is the per-key scalar logic the incremental arrangement (M2b) will drive. Here we validate
//! the LOGIC standalone (whole batch, non-incremental) vs a reference: sum-per-key and min-per-key.
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_reduce
//!
//! Program shape (built by hand):  input: List<(K,V)>  -group->  List<(K, List<V>)>
//!   -map((k,vs) -> (k, REDUCE vs))->  List<(K, V)>     (one row; REDUCE = fold_add / fold_min)

use std::collections::BTreeMap;

use corgi::{ArithOp, Builder, CmpOp, Graph, NumOp, Op, Red, Value as CValue};

/// Build `input: List<(K,V)> -> group -> map((k,vs) -> (k, vs `reduce`))`.
fn reduce_program(reduce: Red) -> Graph<NumOp> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input(); // List<Prod[K,V]>
    let grouped = b.add(CmpOp::GroupKey, vec![input]); // List<Prod[K, List<V>]>

    // MapList body: one element = Prod[K, List<V>]  ->  Prod[K, reduced V]
    let body = {
        let mut bb = Builder::<NumOp>::default();
        let e = bb.input();
        let k = bb.add(Op::Field(0), vec![e]);
        let vs = bb.add(Op::Field(1), vec![e]);
        let red = bb.add(ArithOp::Reduce(reduce), vec![vs]); // List<V> -> V
        let out = bb.tuple(vec![k, red]);
        bb.finish(out)
    };
    let mapped = b.add(Op::MapList(Box::new(body)), vec![grouped]);
    b.finish(mapped)
}

/// Run a reduce program over `(keys, vals)` as one `List<(K,V)>` row; return key -> reduced.
fn run_reduce(g: &Graph<NumOp>, keys: &[u64], vals: &[u64]) -> BTreeMap<u64, u64> {
    let n = keys.len();
    let pairs = CValue::Prod(vec![CValue::u64(keys.to_vec()), CValue::u64(vals.to_vec())]);
    let input = CValue::List(vec![n].into(), Box::new(pairs)); // one row of n elements
    let out = eval_graph_local(g, input);
    let (_bounds, inner) = out.into_list("reduce out");
    let cols = inner.into_prod("reduce pairs");
    let ks = cols[0].clone().into_u64("k");
    let vs = cols[1].clone().into_u64("v");
    ks.into_iter().zip(vs).collect()
}

fn eval_graph_local(g: &Graph<NumOp>, v: CValue) -> CValue {
    corgi::eval_graph(g, v)
}

fn main() {
    let n = 10_000usize;
    let keys: Vec<u64> = (0..n).map(|i| (i % 7) as u64).collect();
    let vals: Vec<u64> = (0..n).map(|i| ((i * 13) % 101) as u64).collect();

    // references
    let mut ref_sum: BTreeMap<u64, u64> = BTreeMap::new();
    let mut ref_min: BTreeMap<u64, u64> = BTreeMap::new();
    for (&k, &v) in keys.iter().zip(&vals) {
        *ref_sum.entry(k).or_insert(0) += v;
        let e = ref_min.entry(k).or_insert(u64::MAX);
        *e = (*e).min(v);
    }

    let g_sum = reduce_program(Red::Add);
    let g_min = reduce_program(Red::Min);
    let got_sum = run_reduce(&g_sum, &keys, &vals);
    let got_min = run_reduce(&g_min, &keys, &vals);

    assert_eq!(got_sum, ref_sum, "sum-per-key mismatch");
    assert_eq!(got_min, ref_min, "min-per-key mismatch");

    println!("M2(a): corgi expresses reduce (GroupKey + per-group fold)");
    println!("  {n} updates, {} distinct keys", ref_sum.len());
    println!("  sum-per-key:  OK   (e.g. key 0 -> {})", got_sum[&0]);
    println!("  min-per-key:  OK   (e.g. key 0 -> {})", got_min[&0]);
    println!("\ncorgi can group-by-key and fold each group in one program — the reduce logic the");
    println!("incremental arrangement (M2b) will drive per changed key.");
}
