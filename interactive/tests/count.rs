//! `count` is the sum of a key's diffs, of either sign, as DD's `count`: a key whose values
//! accumulate negatively has a negative count, and one whose values cancel has none.

use interactive::server::evaluate;
use interactive::{lower, parse};

const PROGRAM: &str = r#"
    let pairs = input 0 : ((int, int) ; ()) | map(((a, b), ()) -> ((a), (b)));
    let shifted = pairs | map((a, (b)) -> (a, ((b, 1) add)));

    export "positive" = pairs | count;
    export "negative" = pairs | negate | count;
    export "mixed" = (pairs + pairs - shifted - shifted - shifted) | count;
    export "cancel" = (pairs - shifted) | count;
"#;

#[test]
fn counts_carry_their_sign() {
    let mut program = lower::lower_tree(parse::pipe::parse(PROGRAM));
    program.optimize();
    // Key 1 has two values, key 2 one.
    let rows: Vec<String> = ["1,10", "1,20", "2,30"].map(String::from).to_vec();
    let count = |k: i64, c: i64| ((format!("tuple({k})"), c.to_string()), 1);
    let mut want = std::collections::BTreeMap::new();
    want.insert("positive".to_string(), vec![count(1, 2), count(2, 1)]);
    want.insert("negative".to_string(), vec![count(1, -2), count(2, -1)]);
    // Each key: twice its values, less three times as many shifted ones.
    want.insert("mixed".to_string(), vec![count(1, -2), count(2, -1)]);
    // Each key's values cancel against as many shifted ones.
    want.insert("cancel".to_string(), vec![]);
    for workers in [1, 3] {
        let got: std::collections::BTreeMap<_, Vec<_>> = evaluate(timely::Config::process(workers), &program, &[rows.clone()])
            .into_iter()
            .map(|(name, updates)| (name, updates.rows().into_iter().map(|(k, v, d)| ((k, v), d)).collect()))
            .collect();
        assert_eq!(got, want, "{workers} workers");
    }
}
