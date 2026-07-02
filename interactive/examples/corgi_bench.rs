//! Rung 1: compile a DDIR scalar `Term` to a corgi `Graph<NumOp>`, run it columnar, and
//! measure it against DDIR's per-row `ir::eval` interpreter — sweeping column width to find
//! the crossover. Also measures the AoS->SoA transcode tax (open question: does it dominate?).
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_bench
//!
//! Subset supported in this rung: the map/filter core over non-negative ints —
//! Var/Proj/Int/Tuple/Binary(arith,cmp,logical)/If. Variant/List/Fold/Hash are later rungs.

use std::time::Instant;

use interactive::ir::{self, Value as DValue};
use interactive::parse::{BinOp, Term};

use corgi::{ArithOp, BinOp as CBinOp, Builder, CmpOp, Kind, NumOp, Op, Pred, Shape, Value as CValue};

// ---------- shape inference (the dynamic-typing primitive: observe data -> Shape) ----------

fn infer_shape(sample: &DValue) -> Shape {
    match sample {
        DValue::Int(_) => Shape::Prim(64),
        DValue::Tuple(xs) => Shape::Prod(xs.iter().map(infer_shape).collect()),
        DValue::List(xs) => Shape::List(Box::new(infer_shape(xs.first().expect("nonempty list")))),
        DValue::Variant(..) => panic!("variant shape inference is a later rung"),
    }
}

// ---------- AoS rows -> SoA corgi columns, directed by the inferred shape ----------

fn transcode(rows: &[DValue], shape: &Shape) -> CValue {
    match shape {
        Shape::Prim(_) => CValue::u64(rows.iter().map(|r| r.as_int() as u64).collect()),
        Shape::Prod(field_shapes) => {
            let cols = field_shapes
                .iter()
                .enumerate()
                .map(|(i, fs)| {
                    let sub: Vec<DValue> = rows
                        .iter()
                        .map(|r| match r {
                            DValue::Tuple(xs) => xs[i].clone(),
                            other => panic!("transcode: expected Tuple, got {other:?}"),
                        })
                        .collect();
                    transcode(&sub, fs)
                })
                .collect();
            CValue::Prod(cols)
        }
        other => panic!("transcode: shape not supported in this rung: {other:?}"),
    }
}

// ---------- the compiler: Term -> corgi node (flat, columnar) ----------
// `env[i]` is the node for `Var(i)`; binders (none yet in this subset) would push on top.
// `anchor` is any node used to broadcast `Lit` to the column length.

fn compile(term: &Term, b: &mut Builder<NumOp>, env: &[usize], anchor: usize) -> usize {
    match term {
        Term::Var(i) => env[*i],
        Term::Bound(k) => env[env.len() - 1 - *k],
        Term::Int(n) => b.add(Op::Lit(CValue::u64(vec![*n as u64])), vec![anchor]),
        Term::Tuple(fields) => {
            let ids: Vec<usize> = fields.iter().map(|f| compile(f, b, env, anchor)).collect();
            b.tuple(ids)
        }
        Term::Proj(t, i) => {
            let id = compile(t, b, env, anchor);
            b.add(Op::Field(*i), vec![id])
        }
        Term::Binary(op, l, r) => {
            let lid = compile(l, b, env, anchor);
            let rid = compile(r, b, env, anchor);
            let pair = |b: &mut Builder<NumOp>, x, y| b.tuple(vec![x, y]);
            match op {
                BinOp::Add => { let p = pair(b, lid, rid); b.add(ArithOp::Bin(CBinOp::Add, Kind::U, 64), vec![p]) }
                BinOp::Sub => { let p = pair(b, lid, rid); b.add(ArithOp::Bin(CBinOp::Sub, Kind::U, 64), vec![p]) }
                BinOp::Mul => { let p = pair(b, lid, rid); b.add(ArithOp::Bin(CBinOp::Mul, Kind::U, 64), vec![p]) }
                BinOp::Eq => { let p = pair(b, lid, rid); b.add(CmpOp::Rel(Pred::Eq), vec![p]) }
                BinOp::Ne => { let p = pair(b, lid, rid); b.add(CmpOp::Rel(Pred::Ne), vec![p]) }
                BinOp::Lt => { let p = pair(b, lid, rid); b.add(CmpOp::Rel(Pred::Lt), vec![p]) }
                BinOp::Le => { let p = pair(b, lid, rid); b.add(CmpOp::Rel(Pred::Le), vec![p]) }
                BinOp::Gt => { let p = pair(b, rid, lid); b.add(CmpOp::Rel(Pred::Lt), vec![p]) } // a>b == b<a
                BinOp::Ge => { let p = pair(b, rid, lid); b.add(CmpOp::Rel(Pred::Le), vec![p]) } // a>=b == b<=a
                BinOp::And => { let p = pair(b, lid, rid); b.add(CmpOp::Min, vec![p]) } // 0/1 masks
                BinOp::Or => { let p = pair(b, lid, rid); b.add(CmpOp::Max, vec![p]) }
            }
        }
        Term::If { cond, then, els } => {
            let c = compile(cond, b, env, anchor);
            let t = compile(then, b, env, anchor);
            let e = compile(els, b, env, anchor);
            let sel = b.tuple(vec![c, t, e]);
            b.add(Op::Select, vec![sel])
        }
        other => panic!("compile: unsupported Term in this rung: {other:?}"),
    }
}

/// Build the full graph for `term`. `Var(0)` = the whole input row. The graph is
/// shape-independent in this subset (corgi resolves layout at eval); shape drives transcode.
fn build_graph(term: &Term) -> corgi::Graph<NumOp> {
    let mut b = Builder::<NumOp>::default();
    let input = b.input();
    let env = vec![input]; // Var(0) is the input column
    let out = compile(term, &mut b, &env, input);
    b.finish(out)
}

// ---------- timing ----------

fn bench<F: FnMut()>(n: usize, mut f: F) -> f64 {
    let reps = (4_000_000 / n.max(1)).max(5);
    f(); // warmup
    let t = Instant::now();
    for _ in 0..reps {
        f();
    }
    t.elapsed().as_secs_f64() / reps as f64 // seconds per call
}

fn per_row_ns(secs_per_call: f64, n: usize) -> f64 {
    secs_per_call * 1e9 / n as f64
}

fn main() {
    // Two representative scalar programs over rows of shape Tuple([a, b]) (Var(0) = the row):
    let a = || Box::new(Term::Proj(Box::new(Term::Var(0)), 0));
    let bb = || Box::new(Term::Proj(Box::new(Term::Var(0)), 1));

    // MAP:    (a + b, a * 3)
    let term_map = Term::Tuple(vec![
        Term::Binary(BinOp::Add, a(), bb()),
        Term::Binary(BinOp::Mul, a(), Box::new(Term::Int(3))),
    ]);
    // FILTER: a < b   (produces the 0/1 keep-mask)
    let term_filter = Term::Binary(BinOp::Lt, a(), bb());

    for (name, term) in [("map (a+b, a*3)", &term_map), ("filter a<b", &term_filter)] {
        println!("\n=== {name} ===");
        println!(
            "{:>10} {:>12} {:>12} {:>12} {:>10} {:>10}",
            "rows", "interp ns/r", "corgi ns/r", "+xcode ns/r", "speedup", "w/xcode"
        );
        let g = build_graph(term);

        for &n in &[1usize, 10, 100, 1_000, 10_000, 100_000, 1_000_000] {
            let rows: Vec<DValue> = (0..n)
                .map(|i| DValue::Tuple(vec![DValue::Int((i % 1000) as i64), DValue::Int(((i * 7) % 997) as i64)]))
                .collect();
            let shape = infer_shape(&rows[0]); // dynamic typing: form the corgi Shape from the data

            // baseline: per-row interpreter
            let interp = bench(n, || {
                let mut out = Vec::with_capacity(n);
                for r in &rows {
                    let mut env = vec![r.clone()];
                    out.push(ir::eval(term, &mut env));
                }
                std::hint::black_box(&out);
            });

            // corgi eval only (input pre-transcoded; clone = cheap Arc bumps)
            let input = transcode(&rows, &shape);
            let corgi_eval = bench(n, || {
                let out = corgi::eval_graph(&g, input.clone());
                std::hint::black_box(&out);
            });

            // corgi total: transcode each call + eval (the rung-1 tax)
            let corgi_total = bench(n, || {
                let inp = transcode(&rows, &shape);
                let out = corgi::eval_graph(&g, inp);
                std::hint::black_box(&out);
            });

            println!(
                "{:>10} {:>12.1} {:>12.1} {:>12.1} {:>9.2}x {:>9.2}x",
                n,
                per_row_ns(interp, n),
                per_row_ns(corgi_eval, n),
                per_row_ns(corgi_total, n),
                interp / corgi_eval,
                interp / corgi_total,
            );
        }
    }

    // correctness: corgi == interpreter on a mid-size column
    check_correctness(&term_map, true);
    check_correctness(&term_filter, false);
    println!("\ncorrectness: OK");
}

fn check_correctness(term: &Term, is_map: bool) {
    let n = 1000;
    let rows: Vec<DValue> = (0..n)
        .map(|i| DValue::Tuple(vec![DValue::Int((i % 1000) as i64), DValue::Int(((i * 7) % 997) as i64)]))
        .collect();
    let shape = infer_shape(&rows[0]);
    let g = build_graph(term);
    let out = corgi::eval_graph(&g, transcode(&rows, &shape));

    let base: Vec<DValue> = rows
        .iter()
        .map(|r| {
            let mut env = vec![r.clone()];
            ir::eval(term, &mut env)
        })
        .collect();

    if is_map {
        let cols = out.into_prod("map result");
        let c0 = cols[0].clone().into_u64("col0");
        let c1 = cols[1].clone().into_u64("col1");
        for (i, bv) in base.iter().enumerate() {
            let DValue::Tuple(xs) = bv else { panic!("expected tuple") };
            assert_eq!(c0[i], xs[0].as_int() as u64, "map col0 row {i}");
            assert_eq!(c1[i], xs[1].as_int() as u64, "map col1 row {i}");
        }
    } else {
        let mask = out.into_u64("filter mask");
        for (i, bv) in base.iter().enumerate() {
            assert_eq!(mask[i], bv.as_int() as u64, "filter row {i}");
        }
    }
}
