//! The corgi backend's correctness gate: each canonical `.ddp` program must evaluate
//! identically through the corgi backend and the reference vec backend.

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}
fn rows(rs: &[&[i64]]) -> Vec<(Value, Value)> {
    rs.iter().map(|f| (tup(f), Value::unit())).collect()
}

/// Per-program inputs (arity matches each `.ddp`'s `input N` usage).
fn inputs_for(prog: &str) -> Vec<Vec<(Value, Value)>> {
    let edges = rows(&[&[1, 2], &[2, 3], &[3, 4], &[5, 6], &[4, 2]]);
    match prog {
        "reach" => vec![edges, rows(&[&[1]])],
        "scc" => vec![edges],
        // stable: edges (l_node, l_pref, r_node, r_pref)
        "stable" => vec![rows(&[&[1, 1, 10, 1], &[1, 2, 11, 1], &[2, 1, 10, 2], &[2, 2, 11, 2]])],
        "unnest" => vec![rows(&[&[1, 2], &[3, 4]])],
        "adt" => vec![edges],
        "binders" => vec![rows(&[&[1, 2], &[3, 4]])],
        // join_fallback: two keyed relations with overlapping keys (incl. a key with fanout).
        "join_fallback" => vec![
            rows(&[&[1, 10], &[2, 20], &[2, 21], &[3, 30]]),
            rows(&[&[1, 5], &[2, 6], &[4, 7]]),
        ],
        // scalar_ops: (key, a, b) triples; a values straddle the `> 2` and `= -5` tests.
        "scalar_ops" => vec![rows(&[&[1, 1, 9], &[1, 4, 8], &[2, 3, 7], &[3, -5, 6], &[3, 2, 5]])],
        "sum_ops" => vec![rows(&[&[1, 10], &[2, 20], &[2, 21]])],
        "case_ops" => vec![rows(&[&[1, 10], &[2, 20], &[3, 14], &[3, 30]])],
        // pair_keys: composite keys with overlap, fanout, and one-sided keys on both sides.
        "pair_keys" => vec![
            rows(&[&[1, 1, 10], &[1, 2, 20], &[2, 1, 30], &[2, 1, 31], &[9, 9, 90]]),
            rows(&[&[1, 1, 5], &[2, 1, 6], &[3, 3, 7]]),
        ],
        // tour: edges (with a cycle and a chord) + roots.
        "tour" => vec![
            rows(&[&[1, 2], &[2, 3], &[3, 1], &[3, 4], &[5, 2]]),
            rows(&[&[1], &[5]]),
        ],
        other => panic!("no inputs configured for {other}"),
    }
}

/// Evaluate `prog` through both backends and assert the outputs match.
fn assert_backends_agree(prog: &str) {
    // Fixtures pinning individual lowerings live with the gate (tests/programs); the
    // algorithm programs double as examples and stay in examples/programs.
    let fixture = format!("{}/tests/programs/{prog}.ddp", env!("CARGO_MANIFEST_DIR"));
    let path = if std::path::Path::new(&fixture).exists() {
        fixture
    } else {
        format!("{}/examples/programs/{prog}.ddp", env!("CARGO_MANIFEST_DIR"))
    };
    let src = interactive::load_program(&path);
    let mut tree = lower::lower_tree(parse::pipe::parse(&src));
    tree.optimize();
    let inputs = inputs_for(prog);
    assert_eq!(
        corgi::evaluate(&tree, &inputs),
        vec::evaluate(&tree, &inputs),
        "corgi backend disagrees with the vec backend on {prog}",
    );
}

#[test] fn reach() { assert_backends_agree("reach"); }
#[test] fn scc() { assert_backends_agree("scc"); }
#[test] fn stable() { assert_backends_agree("stable"); }
#[test] fn unnest() { assert_backends_agree("unnest"); }
#[test] fn adt() { assert_backends_agree("adt"); }
#[test] fn binders() { assert_backends_agree("binders"); }
#[test] fn join_fallback() { assert_backends_agree("join_fallback"); }
#[test] fn scalar_ops() { assert_backends_agree("scalar_ops"); }
#[test] fn sum_ops() { assert_backends_agree("sum_ops"); }
#[test] fn case_ops() { assert_backends_agree("case_ops"); }
#[test] fn tour() { assert_backends_agree("tour"); }
#[test] fn pair_keys() { assert_backends_agree("pair_keys"); }
