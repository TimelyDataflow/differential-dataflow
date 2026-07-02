//! Rung 2: corgi as the scalar logic of a DDIR **Linear** map, running inside a real
//! differential-dataflow computation. A container-level unary operator transcodes each
//! update batch to corgi columns, runs the compiled `Graph` over the whole batch at once,
//! and transcodes back — preserving (time, diff) per row. Verified against the per-row
//! `ir::eval` interpreter, including retractions (incremental soundness).
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_linear_dataflow

use std::collections::BTreeMap;
use std::time::Instant;

use interactive::corgi_logic::{compile_projection, infer_shape, transcode, untranscode};
use interactive::ir::{self, Diff, Value as DValue};
use interactive::parse::{BinOp, Term};

use corgi::{Graph, NumOp, Shape, Value as CValue};

type Row = DValue;

/// Apply the compiled projection `graph` to a whole update batch, columnar, via corgi.
/// (time, diff) pass through untouched; only (key, val) are transformed.
fn corgi_apply(graph: &Graph<NumOp>, batch: Vec<((Row, Row), u64, Diff)>) -> Vec<((Row, Row), u64, Diff)> {
    if batch.is_empty() {
        return batch;
    }
    let kshape = infer_shape(&batch[0].0 .0);
    let vshape = infer_shape(&batch[0].0 .1);
    let in_shape = Shape::Prod(vec![kshape.clone(), vshape.clone()]);

    let keys: Vec<DValue> = batch.iter().map(|x| x.0 .0.clone()).collect();
    let vals: Vec<DValue> = batch.iter().map(|x| x.0 .1.clone()).collect();
    let input = CValue::Prod(vec![transcode(&keys, &kshape), transcode(&vals, &vshape)]);

    let out = corgi::eval_graph(graph, input);
    let out_shape = corgi::shape_of(graph, &in_shape).expect("shape_of");
    let Shape::Prod(out_fs) = &out_shape else { panic!("expected Prod output shape") };
    let mut cols = out.into_prod("out");
    let newvals = untranscode(cols.pop().unwrap(), &out_fs[1]);
    let newkeys = untranscode(cols.pop().unwrap(), &out_fs[0]);

    batch
        .into_iter()
        .enumerate()
        .map(|(i, ((_, _), t, d))| ((newkeys[i].clone(), newvals[i].clone()), t, d))
        .collect()
}

/// Run the corgi-Linear dataflow on `data` (rows with diffs), return consolidated output.
fn run_corgi_dataflow(graph: Graph<NumOp>, data: Vec<((Row, Row), Diff)>) -> BTreeMap<(Row, Row), Diff> {
    use std::sync::mpsc::channel;
    use timely::dataflow::operators::*;
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::capture::{Capture, Event};
    use differential_dataflow::AsCollection;
    use differential_dataflow::input::Input;

    let (tx, rx) = channel::<Event<u64, Vec<((Row, Row), u64, Diff)>>>();
    timely::execute_directly(move |worker| {
        let mut handle = worker.dataflow::<u64, _, _>(|scope| {
            let (handle, coll) = scope.new_collection::<(Row, Row), Diff>();
            let graph = graph.clone();
            coll.inner
                .unary(Pipeline, "CorgiLinear", move |_, _| {
                    move |input, output| {
                        input.for_each(|time, data| {
                            let mut result = corgi_apply(&graph, std::mem::take(data));
                            output.session(&time).give_container(&mut result);
                        });
                    }
                })
                .as_collection()
                .inner
                .capture_into(tx);
            handle
        });
        for (row, diff) in data {
            handle.update(row, diff);
        }
    });

    let mut acc: BTreeMap<(Row, Row), Diff> = BTreeMap::new();
    for event in rx {
        if let Event::Messages(_, data) = event {
            for ((k, v), _, d) in data {
                *acc.entry((k, v)).or_insert(0) += d;
            }
        }
    }
    acc.retain(|_, d| *d != 0);
    acc
}

/// Reference: the same projection via the per-row `ir::eval` interpreter.
fn reference(key: &Term, val: &Term, data: &[((Row, Row), Diff)]) -> BTreeMap<(Row, Row), Diff> {
    let mut acc: BTreeMap<(Row, Row), Diff> = BTreeMap::new();
    for ((k, v), d) in data {
        let mut env = vec![k.clone(), v.clone()];
        let nk = ir::eval(key, &mut env);
        let nv = ir::eval(val, &mut env);
        *acc.entry((nk, nv)).or_insert(0) += d;
    }
    acc.retain(|_, d| *d != 0);
    acc
}

fn main() {
    // Projection: keep the key; reval val=[a,b] -> (a+b, a*3).  Var(0)=key, Var(1)=val.
    let a = || Box::new(Term::Proj(Box::new(Term::Var(1)), 0));
    let b = || Box::new(Term::Proj(Box::new(Term::Var(1)), 1));
    let key = Term::Var(0);
    let val = Term::Tuple(vec![
        Term::Binary(BinOp::Add, a(), b()),
        Term::Binary(BinOp::Mul, a(), Box::new(Term::Int(3))),
    ]);

    // Input shapes: key=Int (Prim), val=(Int,Int) (Prod[Prim,Prim]).
    let kshape = infer_shape(&DValue::Int(0));
    let vshape = infer_shape(&DValue::Tuple(vec![DValue::Int(0), DValue::Int(0)]));
    let graph = compile_projection(&key, &val, &kshape, &vshape);

    // Data with mixed diffs INCLUDING retractions (net-zero rows must vanish from both).
    let n = 20_000usize;
    let data: Vec<((Row, Row), Diff)> = (0..n)
        .flat_map(|i| {
            let row = (
                DValue::Int((i % 10) as i64),
                DValue::Tuple(vec![DValue::Int((i % 100) as i64), DValue::Int(((i * 7) % 97) as i64)]),
            );
            match i % 3 {
                0 => vec![(row.clone(), 1), (row, -1)], // net zero -> must disappear
                1 => vec![(row, 2)],
                _ => vec![(row, 1)],
            }
        })
        .collect();

    let t0 = Instant::now();
    let got = run_corgi_dataflow(graph, data.clone());
    let elapsed = t0.elapsed();
    let want = reference(&key, &val, &data);

    assert_eq!(got.len(), want.len(), "output cardinality mismatch");
    assert_eq!(got, want, "corgi-Linear dataflow output != interpreter reference");

    println!("rung 2: corgi-Linear inside a real DD dataflow");
    println!("  input updates (with retractions): {}", data.len());
    println!("  output rows (consolidated):       {}", got.len());
    println!("  matches interpreter reference:    YES (incl. net-zero retractions vanished)");
    println!("  dataflow wall-clock:              {:?}", elapsed);
    println!("\nINTEGRATION SOUND: corgi runs as DDIR's columnar Linear logic; (time,diff) preserved.");
}
