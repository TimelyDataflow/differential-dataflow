//! `weigh` semantics on the reference vec backend, and the fused-vs-unfused
//! agreement of time-effect chains — the regression for the two step-algebra
//! fixes (EnterAt joins the accumulated delta; a fused LiftIter reads the
//! accumulated time at its position in the chain).

use std::collections::BTreeMap;

use interactive::backend::vec;
use interactive::ir::Value;
use interactive::{lower, parse};

fn tup(fields: &[i64]) -> Value {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}
fn rows(rs: &[&[i64]]) -> Vec<(Value, Value)> {
    rs.iter().map(|f| (tup(f), Value::unit())).collect()
}

/// Parse, lower, optionally optimize, evaluate on vec.
fn run(
    src: &str,
    inputs: &[Vec<(Value, Value)>],
    optimize: bool,
) -> BTreeMap<String, Vec<((Value, Value), i64)>> {
    let mut tree = lower::lower_tree(parse::pipe::parse(src));
    if optimize {
        tree.optimize();
    }
    vec::evaluate(&tree, inputs)
}

/// `weigh(v) | count` is per-key SUM: positive, negative (the `!= 0` guard
/// reports it), and exact-zero (absent — absence encodes exactly zero).
#[test]
fn weigh_count_is_sum() {
    let src = r#"
        let pairs = input 0 | key($0[0] ; $0[1]);
        export "sums" = pairs | weigh($1[0]) | count;
    "#;
    let inputs = vec![rows(&[&[1, 10], &[1, 20], &[2, -7], &[2, 2], &[3, 4], &[3, -4]])];
    let got = run(src, &inputs, true);
    // Key 1 sums to 30, key 2 to -5, key 3 to exactly 0 (absent).
    assert_eq!(
        got["sums"],
        vec![((tup(&[1]), tup(&[30])), 1), ((tup(&[2]), tup(&[-5])), 1)]
    );
}

/// `weigh(-1)` is `negate`: the two programs agree output-for-output.
#[test]
fn weigh_neg_one_is_negate() {
    let a = r#"
        let keys = input 0 | key($0[0] ;);
        let gone = keys | weigh(0 - 1);
        let net = keys + gone;
        export "out" = net | count;
    "#;
    let b = r#"
        let keys = input 0 | key($0[0] ;);
        let gone = keys | negate;
        let net = keys + gone;
        export "out" = net | count;
    "#;
    let inputs = vec![rows(&[&[1, 10], &[2, 20], &[2, 21]])];
    let (ga, gb) = (run(a, &inputs, true), run(b, &inputs, true));
    assert_eq!(ga, gb);
    assert_eq!(ga["out"], vec![]); // everything cancelled
}

/// `weigh(0)` elides rows entirely: no group reaches any reducer.
#[test]
fn weigh_zero_is_empty() {
    let src = r#"
        let pairs = input 0 | key($0[0] ; $0[1]);
        let none = pairs | weigh(0);
        export "c" = none | count;
        export "m" = none | min;
        export "d" = none | map($0 ;) | distinct;
        export "l" = none | collect;
    "#;
    let inputs = vec![rows(&[&[1, 10], &[2, 20]])];
    let got = run(src, &inputs, true);
    for name in ["c", "m", "d", "l"] {
        assert_eq!(got[name], vec![], "weigh(0) leaked rows into {name}");
    }
}

/// A fused `enter_at(a) | enter_at(b) | lift_iter` chain must agree with its
/// unfused rendering: EnterAt JOINS the accumulated time delta (not
/// overwrite), and LiftIter reads the accumulated time at its position.
/// Before the fix, the fused form kept only `b`'s delay and lifted the
/// pre-shift coordinate.
#[test]
fn fused_time_ops_agree_with_unfused() {
    let src = r#"
        let seeds = input 0 | key($0[0] ;);
        r: {
            var acc = seeds | distinct;
            let obs = seeds | enter_at(4) | enter_at(1) | lift_iter;
        }
        export "obs" = r::obs | map($0 ; $1[0]);
    "#;
    let inputs = vec![rows(&[&[7]])];
    let unfused = run(src, &inputs, false);
    let fused = run(src, &inputs, true);
    assert_eq!(fused, unfused, "optimizer fusion changed time-op semantics");
    // And the shared answer reflects BOTH delays joined: enter_at(4) lands at
    // coordinate 768, enter_at(1) at 256; the join is 768, which is what a
    // lift_iter placed after both must observe.
    assert_eq!(unfused["obs"], vec![((tup(&[7]), tup(&[768])), 1)]);
}
