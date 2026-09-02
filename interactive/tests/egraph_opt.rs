//! The e-graph optimizer against the hand-written one: on every corpus program, and on its
//! explanation rewrite (a large, mechanically generated, redundant IR), the e-graph-optimized
//! program must evaluate to the same outputs as the original, and reach an operator count no
//! worse than `Scope::optimize`'s.

use interactive::backend::vec;
use interactive::ir::Value;
use interactive::{egraph, explain, lower, parse, scope_ir};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}
fn rows(rs: &[&[i64]]) -> Vec<(Value, Value)> {
    rs.iter().map(|f| (tup(f), Value::unit())).collect()
}

/// Per-program inputs (as in `tests/corgi_backend.rs`).
fn inputs_for(prog: &str) -> Vec<Vec<(Value, Value)>> {
    let edges = rows(&[&[1, 2], &[2, 3], &[3, 4], &[5, 6], &[4, 2]]);
    match prog {
        "reach" => vec![edges, rows(&[&[1]])],
        "scc" => vec![edges],
        "stable" => vec![rows(&[&[1, 1, 10, 1], &[1, 2, 11, 1], &[2, 1, 10, 2], &[2, 2, 11, 2]])],
        "unnest" => vec![rows(&[&[1, 2], &[3, 4]])],
        "adt" => vec![edges.clone()],
        "ast" => vec![edges],
        "binders" => vec![rows(&[&[1, 2], &[3, 4]])],
        "tour" => vec![rows(&[&[1, 2], &[2, 3], &[3, 1], &[3, 4], &[5, 2]]), rows(&[&[1], &[5]])],
        other => panic!("no inputs configured for {other}"),
    }
}

fn load(prog: &str) -> scope_ir::Program {
    let path = format!("{}/examples/programs/{prog}.ddp", env!("CARGO_MANIFEST_DIR"));
    lower::lower_tree(parse::pipe::parse(&interactive::load_program(&path)))
}

/// Optimize `tree` both ways; check the e-graph result evaluates like the original and is no
/// bigger than the hand-written optimizer's. Returns (original, rust, egraph) op counts.
fn check(name: &str, tree: &scope_ir::Program, inputs: &[Vec<(Value, Value)>]) -> (usize, usize, usize) {
    let want = vec::evaluate(tree, inputs);
    let mut rust = tree.clone();
    rust.optimize();
    let eg = egraph::optimize(tree);
    assert_eq!(vec::evaluate(&eg, inputs), want, "{name}: the e-graph-optimized program changed the outputs");
    let counts = (tree.op_count(), rust.op_count(), eg.op_count());
    assert!(counts.2 <= counts.1, "{name}: e-graph left {} ops, the hand-written optimizer {}", counts.2, counts.1);
    counts
}

#[test]
fn corpus_programs() {
    for prog in ["reach", "scc", "stable", "unnest", "adt", "ast", "binders", "tour"] {
        let tree = load(prog);
        let (o, r, e) = check(prog, &tree, &inputs_for(prog));
        println!("{prog:>8}: {o} ops -> rust {r}, egraph {e}");
    }
}

#[test]
fn explain_corpus() {
    // the explanation rewrite is the redundant corpus: reach and scc grow ten-fold.
    for prog in ["reach", "scc"] {
        let tree = load(prog);
        let inputs0 = inputs_for(prog);
        // each input's (key arity, value arity), read off its rows
        let shapes: Vec<(usize, usize)> = inputs0
            .iter()
            .map(|rows| match &rows[0].0 { Value::Tuple(xs) => (xs.len(), 0usize), _ => (1, 0) })
            .collect();
        let rewritten = explain::explain(&tree, &shapes);
        // the explained program has an extra (query) input; feed it nothing.
        let mut inputs = inputs0;
        while inputs.len() < rewritten.root.imports.len() {
            inputs.push(Vec::new());
        }
        let (o, r, e) = check(&format!("explain({prog})"), &rewritten, &inputs);
        println!("explain({prog}): {o} ops -> rust {r}, egraph {e}");
    }
}
