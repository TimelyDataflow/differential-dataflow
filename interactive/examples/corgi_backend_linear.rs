//! M1: corgi as DDIR's NATIVE representation inside a real dataflow. Rows enter, are transcoded
//! to a `CorgiContainer` ONCE (ingest boundary), flow through K corgi-native Linear (Project)
//! operators — each `eval_graph` over the corgi columns, ZERO inter-operator transcode — and are
//! read back ONCE (egress boundary). Verified vs the per-row `ir::eval` interpreter; timed.
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_backend_linear

use std::collections::BTreeMap;
use std::time::Instant;

use timely::dataflow::operators::*;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::{Capture, Event};

use interactive::corgi_backend::CorgiContainer;
use interactive::corgi_logic::{compile_projection, infer_shape};
use interactive::ir::{self, Diff, Value as DValue};
use interactive::parse::{BinOp, Term};

use corgi::Value as CValue;

type Row = DValue;
type CC = CorgiContainer<u64, Diff>;

fn main() {
    // Project: key'=key (Var0 passthrough), val'=(a+b, a*3) over val=(a,b) (Var1). Shape-stable
    // (val stays a 2-tuple of ints), so it chains K times.
    let a = || Box::new(Term::Proj(Box::new(Term::Var(1)), 0));
    let b = || Box::new(Term::Proj(Box::new(Term::Var(1)), 1));
    let key_term = Term::Var(0);
    let val_term = Term::Tuple(vec![
        Term::Binary(BinOp::Add, a(), b()),
        Term::Binary(BinOp::Mul, a(), Box::new(Term::Int(3))),
    ]);

    let k = 8usize;
    let n = 100_000usize;
    let data: Vec<(Row, Row)> = (0..n)
        .map(|i| {
            (
                DValue::Int((i % 10) as i64),
                DValue::Tuple(vec![DValue::Int((i % 100) as i64), DValue::Int(((i * 7) % 97) as i64)]),
            )
        })
        .collect();

    // ---- run the corgi-native dataflow ----
    let (tx, rx) = std::sync::mpsc::channel::<Event<u64, Vec<((Row, Row), u64, Diff)>>>();
    let data_in = data.clone();
    let (kt, vt) = (key_term.clone(), val_term.clone());
    let t0 = Instant::now();
    timely::execute_directly(move |worker| {
        let mut handle = worker.dataflow::<u64, _, _>(|scope| {
            use differential_dataflow::input::Input;
            let (handle, coll) = scope.new_collection::<(Row, Row), Diff>();

            // ingest boundary: rows -> CorgiContainer (transcode once)
            let mut corgi_stream = coll.inner.unary(Pipeline, "ToCorgi", |_, _| {
                |input, output| {
                    input.for_each(|cap, data| {
                        let mut c = CorgiContainer::from_updates(std::mem::take(data));
                        output.session(&cap).give_container(&mut c);
                    });
                }
            });
            // K corgi-native Linear ops — eval_graph over corgi columns, no transcode between them
            // Shapes are stable across the K iterations (key=Prim, val=Prod[Prim,Prim]).
            let (kshape, vshape) = (infer_shape(&data_in[0].0), infer_shape(&data_in[0].1));
            for _ in 0..k {
                let graph = compile_projection(&kt, &vt, &kshape, &vshape);
                corgi_stream = corgi_stream.unary(Pipeline, "CorgiProject", move |_, _| {
                    move |input, output| {
                        input.for_each(|cap, data| {
                            let c: CC = std::mem::take(data);
                            let mut out = if c.times.is_empty() {
                                c
                            } else {
                                let inp = CValue::Prod(vec![c.keys, c.vals]);
                                let mut cols = corgi::eval_graph(&graph, inp).into_prod("project");
                                let nv = cols.pop().unwrap();
                                let nk = cols.pop().unwrap();
                                CorgiContainer { keys: nk, vals: nv, times: c.times, diffs: c.diffs }
                            };
                            output.session(&cap).give_container(&mut out);
                        });
                    }
                });
            }
            // egress boundary: CorgiContainer -> rows (transcode once)
            corgi_stream
                .unary(Pipeline, "FromCorgi", |_, _| {
                    |input, output| {
                        input.for_each(|cap, data| {
                            let mut updates = std::mem::take(data).into_updates();
                            output.session(&cap).give_container(&mut updates);
                        });
                    }
                })
                .capture_into(tx);
            handle
        });
        for r in data_in {
            handle.update(r, 1);
        }
    });
    let elapsed = t0.elapsed();

    let mut got: BTreeMap<(Row, Row), Diff> = BTreeMap::new();
    for event in rx {
        if let Event::Messages(_, d) = event {
            for ((k, v), _, df) in d {
                *got.entry((k, v)).or_insert(0) += df;
            }
        }
    }
    got.retain(|_, d| *d != 0);

    // ---- reference: interpreter, K-chain per row ----
    let mut want: BTreeMap<(Row, Row), Diff> = BTreeMap::new();
    for (k0, v0) in &data {
        let (mut ck, mut cv) = (k0.clone(), v0.clone());
        for _ in 0..k {
            let mut env = vec![ck.clone(), cv.clone()];
            let nk = ir::eval(&key_term, &mut env);
            let nv = ir::eval(&val_term, &mut env);
            ck = nk;
            cv = nv;
        }
        *want.entry((ck, cv)).or_insert(0) += 1;
    }
    want.retain(|_, d| *d != 0);

    assert_eq!(got, want, "corgi-native dataflow output != interpreter reference");

    println!("M1: corgi-native Linear chain inside a real DD dataflow");
    println!("  K (chained Project ops): {k}");
    println!("  input rows:              {n}");
    println!("  output rows:             {}", got.len());
    println!("  matches interpreter:     YES");
    println!("  transcodes:              2 (ingest + egress) — ZERO between the {k} operators");
    println!("  wall-clock:              {elapsed:?}");
}
