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
        other => panic!("no inputs configured for {other}"),
    }
}

/// Evaluate `prog` through both backends and assert the outputs match.
fn assert_backends_agree(prog: &str) {
    let path = format!("{}/examples/programs/{prog}.ddp", env!("CARGO_MANIFEST_DIR"));
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
