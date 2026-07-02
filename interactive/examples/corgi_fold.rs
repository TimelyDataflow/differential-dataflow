//! Q3: corgi covers beyond map/filter — a List + Fold (per-row aggregation), the construct
//! datalog/eqsat lean on (Collect/Count/nested data). Rows are List<Int>; the Term folds each
//! row's list (acc+elem). Validates corgi vs `ir::eval` and measures the columnar speedup.
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_fold

use std::time::Instant;

use interactive::corgi_logic::{compile_term_single, infer_shape, transcode, untranscode};
use interactive::ir::{self, Value as DValue};
use interactive::parse::{BinOp, Term};

fn bench<F: FnMut()>(n: usize, mut f: F) -> f64 {
    let reps = (2_000_000 / n.max(1)).max(5);
    f();
    let t = Instant::now();
    for _ in 0..reps {
        f();
    }
    t.elapsed().as_secs_f64() / reps as f64
}

fn main() {
    // Fold over Var(0) (the row, a List<Int>): seed 0, step = acc + elem.
    // DDIR Fold binders: elem=Bound(0), acc=Bound(1).
    let step = Term::Binary(BinOp::Add, Box::new(Term::Bound(1)), Box::new(Term::Bound(0)));
    let term = Term::Fold {
        list: Box::new(Term::Var(0)),
        init: Box::new(Term::Int(0)),
        step: Box::new(step),
    };
    let g = compile_term_single(&term);

    // Rows: lists of length 1..=20 of small ints.
    let n = 100_000usize;
    let rows: Vec<DValue> = (0..n)
        .map(|i| {
            let len = 1 + (i % 20);
            DValue::List((0..len).map(|j| DValue::Int(((i + j) % 50) as i64)).collect())
        })
        .collect();
    let shape = infer_shape(&rows[0]); // List(Prim(64))

    // correctness vs interpreter
    let input = transcode(&rows, &shape);
    let out = untranscode(corgi::eval_graph(&g, input), &corgi::Shape::Prim(64));
    for (i, r) in rows.iter().enumerate() {
        let mut env = vec![r.clone()];
        let want = ir::eval(&term, &mut env).as_int();
        assert_eq!(out[i].as_int(), want, "fold row {i}");
    }
    println!("Q3: List + Fold (per-row aggregation)");
    println!("  correctness vs ir::eval: OK ({n} rows, list len 1..=20)");

    // throughput
    let interp = bench(n, || {
        let mut acc = Vec::with_capacity(n);
        for r in &rows {
            let mut env = vec![r.clone()];
            acc.push(ir::eval(&term, &mut env));
        }
        std::hint::black_box(&acc);
    });
    let pre = transcode(&rows, &shape);
    let corgi_eval = bench(n, || {
        std::hint::black_box(corgi::eval_graph(&g, pre.clone()));
    });
    let total_elems: usize = rows.iter().map(|r| if let DValue::List(xs) = r { xs.len() } else { 0 }).sum();
    println!("  total elements folded: {total_elems}");
    println!("  interp:     {:>7.1} ns/row", interp * 1e9 / n as f64);
    println!("  corgi eval: {:>7.1} ns/row   ({:.1}x)", corgi_eval * 1e9 / n as f64, interp / corgi_eval);
    println!("\ncorgi covers List/Fold — the aggregation/nested-data path beyond map/filter.");
}
