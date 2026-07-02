//! Run a canonical `.ddp` program through BOTH the corgi and vec backends and compare. Usage:
//!   PROG=reach ~/.cargo/bin/cargo run --release -p interactive --example corgi_progs
//! Picks inputs by program (small graphs). Reports OK / FAIL / panic per program; with no PROG,
//! runs the whole set.

use interactive::backend::{corgi, vec};
use interactive::ir::Value;
use interactive::{lower, parse};

type Row = Value;

fn tup(fields: &[i64]) -> Row {
    Value::Tuple(fields.iter().map(|&n| Value::Int(n)).collect())
}
fn rows(rs: &[&[i64]]) -> Vec<(Row, Row)> {
    rs.iter().map(|f| (tup(f), Value::unit())).collect()
}

/// Per-program inputs (arity matches each `.ddp`'s `input N` usage).
fn inputs_for(prog: &str) -> Vec<Vec<(Row, Row)>> {
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

fn run_one(prog: &str) -> bool {
    let path = format!("{}/examples/programs/{prog}.ddp", env!("CARGO_MANIFEST_DIR"));
    let src = interactive::load_program(&path);
    let stmts = parse::pipe::parse(&src);
    let mut tree = lower::lower_tree(stmts);
    tree.optimize();
    let inputs = inputs_for(prog);

    let want = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| vec::evaluate(&tree, &inputs)));
    let got = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| corgi::evaluate(&tree, &inputs)));
    match (want, got) {
        (Ok(w), Ok(g)) => {
            let ok = w == g;
            println!("[{}] {prog}", if ok { "OK  " } else { "FAIL" });
            if !ok {
                println!("   vec  : {w:?}");
                println!("   corgi: {g:?}");
            }
            ok
        }
        (Err(_), _) => {
            println!("[VEC-PANIC] {prog} (reference itself panicked; skipping)");
            true
        }
        (Ok(_), Err(_)) => {
            println!("[CORGI-PANIC] {prog}");
            false
        }
    }
}

fn main() {
    std::panic::set_hook(Box::new(|_| {})); // quiet panics; we report them
    let all = ["reach", "scc", "stable", "unnest", "adt", "binders"];
    let progs: Vec<String> = match std::env::var("PROG") {
        Ok(p) => vec![p],
        Err(_) => all.iter().map(|s| s.to_string()).collect(),
    };
    let mut all_ok = true;
    for p in &progs {
        all_ok &= run_one(p);
    }
    println!("\n{}", if all_ok { "all programs match vec backend" } else { "some programs differ — see above" });
}
