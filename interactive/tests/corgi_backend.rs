//! The corgi backend's correctness gate: each canonical `.ddp` program must evaluate
//! identically through the corgi backend and the reference vec backend.
//!
//! Every evaluation goes through the server (`server::evaluate`): install, feed, tick,
//! snapshot — the same path a live install takes.

use interactive::ir::Value;
use interactive::server::{evaluate, RenderBackend};
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
        "cc" => vec![rows(&[&[1, 2], &[2, 3], &[5, 6], &[7, 7], &[9, 8]])],
        // triangles: {1,2,3} and {2,3,4} close; {1,3,4} does not; a duplicate and a self-loop.
        "triangles" => vec![rows(&[&[1, 2], &[2, 3], &[1, 3], &[3, 4], &[2, 4], &[4, 1], &[1, 2], &[5, 5]])],
        // stable: edges (l_node, l_pref, r_node, r_pref)
        "stable" => vec![rows(&[&[1, 1, 10, 1], &[1, 2, 11, 1], &[2, 1, 10, 2], &[2, 2, 11, 2]])],
        "unnest" => vec![rows(&[&[1, 2], &[3, 4]])],
        "adt" => vec![edges.clone()],
        // ast: pairs; small and non-negative, per the program's stated input contract.
        "ast" => vec![edges],
        "binders" => vec![rows(&[&[1, 2], &[3, 4]])],
        // join_fallback: two keyed relations with overlapping keys (incl. a key with fanout).
        "join_fallback" => vec![
            rows(&[&[1, 10], &[2, 20], &[2, 21], &[3, 30]]),
            rows(&[&[1, 5], &[2, 6], &[4, 7]]),
        ],
        // scalar_ops: (key, a, b) triples; a values straddle the `> 2` and `= -5` tests.
        "scalar_ops" => vec![rows(&[&[1, 1, 9], &[1, 4, 8], &[2, 3, 7], &[3, -5, 6], &[3, 2, 5]])],
        "sum_ops" => vec![rows(&[&[1, 10], &[2, 20], &[2, 21]])],
        // empty_batch: only key 1 has the two values the filter keeps, so at several workers at
        // least one gets a batch that arrives non-empty and leaves empty.
        "empty_batch" => vec![rows(&[
            &[1, 10], &[1, 20],
            &[2, 1], &[3, 1], &[4, 1], &[5, 1], &[6, 1], &[7, 1], &[8, 1], &[9, 1],
        ])],
        // sum_skew: any keyed pairs — the skew is in the program, not the data.
        "sum_skew" => vec![rows(&[&[1, 10], &[2, 20], &[2, 21], &[3, 30]])],
        "case_ops" => vec![rows(&[&[1, 10], &[2, 20], &[3, 14], &[3, 30]])],
        "if_literal" => vec![rows(&[&[1, 10], &[1, 20], &[2, 30]])],
        // pair_keys: composite keys with overlap, fanout, and one-sided keys on both sides.
        "pair_keys" => vec![
            rows(&[&[1, 1, 10], &[1, 2, 20], &[2, 1, 30], &[2, 1, 31], &[9, 9, 90]]),
            rows(&[&[1, 1, 5], &[2, 1, 6], &[3, 3, 7]]),
        ],
        "signed_min" => vec![rows(&[
            &[1, 0],
            &[1, -1],
            &[1, -3],
            &[2, 5],
            &[2, -2],
        ])],
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
    let want = evaluate(RenderBackend::Vec, timely::Config::process(1), &tree, &inputs);
    // At every worker count: the exchange places each key on one worker and every operator is
    // key-local from there, so the answer must not depend on how many workers ran it. 3 is in the
    // list on purpose — it is not a power of two, so it takes the modulus path rather than the
    // mask, and it cannot divide these inputs evenly.
    for workers in [1, 2, 3, 4] {
        assert_eq!(
            evaluate(RenderBackend::Corgi, timely::Config::process(workers), &tree, &inputs),
            want,
            "corgi backend at {workers} worker(s) disagrees with the vec backend on {prog}",
        );
    }
    // The same programs again with serializing channels, so every exchanged container makes the
    // round trip through the wire format. This is the multi-process path: `Config::process` above
    // hands containers between threads as typed values and never encodes a byte.
    assert_eq!(
        evaluate(RenderBackend::Corgi, serializing(3), &tree, &inputs),
        want,
        "corgi backend over serializing channels disagrees with the vec backend on {prog}",
    );
}

/// `n` worker threads whose exchange channels serialize — the wire format in the loop.
fn serializing(n: usize) -> timely::Config {
    timely::Config {
        communication: timely::CommunicationConfig::ProcessBinary(n),
        worker: timely::WorkerConfig::default(),
    }
}

#[test] fn reach() { assert_backends_agree("reach"); }
#[test] fn scc() { assert_backends_agree("scc"); }
#[test] fn cc() { assert_backends_agree("cc"); }
#[test] fn triangles() { assert_backends_agree("triangles"); }
#[test] fn stable() { assert_backends_agree("stable"); }
#[test] fn unnest() { assert_backends_agree("unnest"); }
#[test] fn adt() { assert_backends_agree("adt"); }
#[test] fn ast() { assert_backends_agree("ast"); }
#[test] fn binders() { assert_backends_agree("binders"); }
#[test] fn join_fallback() { assert_backends_agree("join_fallback"); }
#[test] fn scalar_ops() { assert_backends_agree("scalar_ops"); }
#[test] fn sum_ops() { assert_backends_agree("sum_ops"); }
#[test] fn empty_batch() { assert_backends_agree("empty_batch"); }
#[test] fn sum_skew() { assert_backends_agree("sum_skew"); }
#[test] fn case_ops() { assert_backends_agree("case_ops"); }
#[test] fn if_literal() { assert_backends_agree("if_literal"); }
#[test] fn tour() { assert_backends_agree("tour"); }
#[test] fn pair_keys() { assert_backends_agree("pair_keys"); }
#[test] fn signed_min() { assert_backends_agree("signed_min"); }
