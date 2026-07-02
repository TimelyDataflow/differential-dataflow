//! M4: run the canonical RECURSIVE DDIR program (`reach.ddp` — reachability) end-to-end through the
//! corgi backend and check it matches the vec backend. This is the first program that exercises a
//! dynamic sub-scope with a feedback `var` — i.e. `leave_dynamic` over `CorgiContainer` — plus join,
//! distinct (reduce), concat (`+`), key, and arrange, all corgi-native.
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_reach

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

type Row = Value;

fn edge(s: i64, d: i64) -> (Row, Row) {
    (Value::Tuple(vec![Value::Int(s), Value::Int(d)]), Value::unit())
}
fn node(n: i64) -> (Row, Row) {
    (Value::Tuple(vec![Value::Int(n)]), Value::unit())
}

fn main() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/programs/reach.ddp");
    let src = interactive::load_program(path);
    let stmts = parse::pipe::parse(&src);
    let mut prog = lower::lower_tree(stmts);
    prog.optimize();

    // A small graph: 1->2->3->4 and a disconnected 5->6; root = {1}. reach = {1,2,3,4}.
    let edges = vec![edge(1, 2), edge(2, 3), edge(3, 4), edge(5, 6)];
    let roots = vec![node(1)];
    let inputs = vec![edges, roots];

    let want = vec::evaluate(&prog, &inputs);
    let got = corgi::evaluate(&prog, &inputs);

    assert_eq!(got, want, "corgi backend != vec backend on reach.ddp");

    // reach.ddp's export is `reach::reach | key(;)` — it collapses node identity, so `result` is a
    // single (unit, unit) row whose diff is the COUNT of reachable nodes.
    let total: i64 = got["result"].iter().map(|(_, d)| *d).sum();
    println!("M4 RECURSIVE end-to-end: reach.ddp via render_tree::<CorgiBackend> == vec backend");
    println!("  reach.ddp result == vec backend; reachable-node count from {{1}} = {total}");
    assert_eq!(total, 4, "expected 4 reachable nodes");

    // Identity-preserving variant (no `key(;)`) so we can name the reachable set.
    let src2 = r#"
        let edges = input 0 | key($0[0] ; $0[1]);
        let roots = input 1 | key($0[0] ;);
        reach: {
            let proposals = reach | join(edges, ($2 ;));
            var reach = roots + proposals | distinct;
        }
        export "result" = reach::reach | arrange;
    "#;
    let mut prog2 = lower::lower_tree(parse::pipe::parse(src2));
    prog2.optimize();
    let got2 = corgi::evaluate(&prog2, &inputs);
    assert_eq!(got2, vec::evaluate(&prog2, &inputs), "corgi != vec on identity reach");
    let reached: std::collections::BTreeSet<i64> = got2["result"]
        .iter()
        .map(|((k, _), _)| match k {
            Value::Tuple(xs) if xs.len() == 1 => xs[0].as_int(),
            other => panic!("unexpected reach key {other:?}"),
        })
        .collect();
    println!("  reachable set from {{1}}: {reached:?}");
    assert_eq!(reached, [1, 2, 3, 4].into_iter().collect());
    println!("\nThe corgi backend runs a real RECURSIVE DDIR program (feedback var + leave_dynamic) correctly.");
}
