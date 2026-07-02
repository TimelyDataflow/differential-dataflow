//! M3 (partial backend, end-to-end): run a real flat DDIR program through `render_tree::<CorgiBackend>`
//! and check it matches the vec backend. Exercises Linear (key/filter), join, and reduce(count) — all
//! corgi-native — composed via the real DDIR backend path. (Flat: no sub-scope, so `leave_dynamic`
//! isn't reached; reduce works via the cursor-less `active_times` tactic.)
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_program

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

type Row = Value;

fn row(fields: &[i64]) -> Row {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}

fn main() {
    let src = r#"
        let a = input 0 | key($0[0] ; $0[1]);
        let b = input 1 | key($0[0] ; $0[1]);
        let j = a | join(b, ($0 ; tuple($1, $2)));
        export "join"     = j;
        export "count"    = j | count;
        export "filtered" = j | filter($0 != 2);
    "#;
    let prog = lower::lower_tree(parse::pipe::parse(src));

    let input0: Vec<(Row, Row)> = vec![
        (row(&[1, 10]), Value::unit()),
        (row(&[1, 11]), Value::unit()),
        (row(&[2, 20]), Value::unit()),
        (row(&[4, 40]), Value::unit()),
    ];
    let input1: Vec<(Row, Row)> = vec![
        (row(&[1, 100]), Value::unit()),
        (row(&[2, 200]), Value::unit()),
        (row(&[3, 300]), Value::unit()),
    ];
    let inputs = vec![input0, input1];

    let want = vec::evaluate(&prog, &inputs); // reference: the vec backend
    let got = corgi::evaluate(&prog, &inputs); // the corgi backend

    assert_eq!(got, want, "corgi backend output != vec backend");

    println!("M3 end-to-end: a flat DDIR program (key + join + count + filter) via render_tree::<CorgiBackend>");
    for name in ["join", "count", "filtered"] {
        println!("  export {:?}: {} rows, == vec backend", name, got[name].len());
    }
    println!("  e.g. count = {:?}", got["count"].iter().map(|((k, v), d)| (k, v, d)).collect::<Vec<_>>());
    println!("\nThe corgi backend renders a real multi-operator DDIR program correctly (vs vec).");
}
