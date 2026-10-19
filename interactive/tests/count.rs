//! `count` is the sum of a key's diffs, of either sign, as DD's `count`: a key whose values
//! accumulate negatively has a negative count, and one whose values cancel has none.

use interactive::ir::Value;
use interactive::server::evaluate;
use interactive::{lower, parse};

const PROGRAM: &str = r#"
    let pairs = input 0 : ((int, int) ; ()) | key($0[0] ; $0[1]);
    let shifted = pairs | key($0 ; $1[0] + 1);
    export "positive" = pairs | count;
    export "negative" = pairs | negate | count;
    export "mixed" = (pairs + pairs - shifted - shifted - shifted) | count;
    export "cancel" = (pairs - shifted) | count;
"#;

fn ints(ns: &[i64]) -> Value { Value::Tuple(ns.iter().map(|&n| Value::Int(n)).collect()) }

#[test]
fn counts_carry_their_sign() {
    let mut program = lower::lower_tree(parse::pipe::parse(PROGRAM));
    program.optimize();
    // Key 1 has two values, key 2 one.
    let rows: Vec<(Value, Value)> = [[1, 10], [1, 20], [2, 30]].iter().map(|r| (ints(r), Value::unit())).collect();
    let count = |k: i64, c: i64| ((ints(&[k]), Value::Int(c)), 1);
    let mut want = std::collections::BTreeMap::new();
    want.insert("positive".to_string(), vec![count(1, 2), count(2, 1)]);
    want.insert("negative".to_string(), vec![count(1, -2), count(2, -1)]);
    // Each key: twice its values, less three times as many shifted ones.
    want.insert("mixed".to_string(), vec![count(1, -2), count(2, -1)]);
    // Each key's values cancel against as many shifted ones.
    want.insert("cancel".to_string(), vec![]);
    for workers in [1, 3] {
        let got = evaluate(timely::Config::process(workers), &program, &[rows.clone()]);
        assert_eq!(got, want, "{workers} workers");
    }
}
