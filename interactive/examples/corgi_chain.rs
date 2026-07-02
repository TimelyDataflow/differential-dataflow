//! Rung 4 (measurement): how corgi-columnar *storage* amortizes the dominant transcode.
//! A chain of K Linear map ops, three ways:
//!   - interp:   per-row `ir::eval`, K ops per row.
//!   - per-op:   corgi, but transcode rows<->corgi at every op boundary (the rung-2 "floor":
//!               what you pay if each operator hands off row-major data).
//!   - columnar: corgi, transcode IN once, eval x K staying in corgi columns, transcode OUT once
//!               (the rung-4 model: data lives corgi-columnar between ops).
//! Shows per-op cost ~ K*(2*xcode + eval) vs columnar ~ 2*xcode + K*eval → columnar approaches
//! the eval-only ceiling as K grows. Quantifies the payoff of a corgi-data CHUNK/container.
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_chain

use std::time::Instant;

use interactive::corgi_logic::{compile_term_single, infer_shape, transcode, untranscode};
use interactive::ir::{self, Value as DValue};
use interactive::parse::{BinOp, Term};

fn op_term(c: i64) -> Term {
    Term::Binary(BinOp::Add, Box::new(Term::Var(0)), Box::new(Term::Int(c)))
}

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
    let n = 100_000usize;
    let consts: Vec<i64> = (1..=16).map(|i| i * 3).collect();

    println!("chain of K Linear ops (x -> x+c), n = {n} rows; ns/row totals over the whole chain");
    println!("{:>4} {:>12} {:>12} {:>12} {:>12} {:>12}", "K", "interp", "per-op", "columnar", "col vs interp", "col vs perop");

    for &k in &[1usize, 2, 4, 8, 16] {
        let terms: Vec<Term> = consts[..k].iter().map(|&c| op_term(c)).collect();
        let graphs: Vec<_> = terms.iter().map(compile_term_single).collect();

        let rows: Vec<DValue> = (0..n).map(|i| DValue::Int((i % 1000) as i64)).collect();
        let shape = infer_shape(&rows[0]);

        // interp: per-row, K ops.
        let interp = bench(n, || {
            let mut out = Vec::with_capacity(n);
            for r in &rows {
                let mut v = r.clone();
                for t in &terms {
                    let mut env = vec![v];
                    v = ir::eval(t, &mut env);
                }
                out.push(v);
            }
            std::hint::black_box(&out);
        });

        // per-op: transcode round-trip at every op boundary.
        let per_op = bench(n, || {
            let mut cur = rows.clone();
            for g in &graphs {
                let col = transcode(&cur, &shape);
                let out = corgi::eval_graph(g, col);
                cur = untranscode(out, &shape);
            }
            std::hint::black_box(&cur);
        });

        // columnar: transcode once, stay in corgi columns across all K ops, transcode out once.
        let columnar = bench(n, || {
            let mut col = transcode(&rows, &shape);
            for g in &graphs {
                col = corgi::eval_graph(g, col);
            }
            let out = untranscode(col, &shape);
            std::hint::black_box(&out);
        });

        let nsr = |s: f64| s * 1e9 / n as f64;
        println!(
            "{:>4} {:>12.1} {:>12.1} {:>12.1} {:>11.1}x {:>11.1}x",
            k,
            nsr(interp),
            nsr(per_op),
            nsr(columnar),
            interp / columnar,
            per_op / columnar,
        );
    }

    // correctness: all three agree at K=8.
    {
        let k = 8;
        let terms: Vec<Term> = consts[..k].iter().map(|&c| op_term(c)).collect();
        let graphs: Vec<_> = terms.iter().map(compile_term_single).collect();
        let rows: Vec<DValue> = (0..1000).map(|i| DValue::Int(i)).collect();
        let shape = infer_shape(&rows[0]);
        let bump: i64 = consts[..k].iter().sum();

        let mut col = transcode(&rows, &shape);
        for g in &graphs {
            col = corgi::eval_graph(g, col);
        }
        let got = untranscode(col, &shape);
        for (i, v) in got.iter().enumerate() {
            assert_eq!(v.as_int(), i as i64 + bump, "chain row {i}");
        }
        println!("\ncorrectness (K=8 chain): OK");
    }
}
