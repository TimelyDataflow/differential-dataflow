//! M2(b)-4 (live): corgi JOIN inside a real DD dataflow via the cursor-less tactic seam. Arrange two
//! corgi-container collections into `Spine<Rc<CorgiBatch>>` traces, then `join_with_tactic` with
//! `CorgiJoinTactic` (bulk merge-join over corgi columns + corgi projection). Output == reference
//! hash-join, incl. retractions. Projection: (key, val0, val1) -> (key, val0+val1).
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_join_dataflow

use std::collections::BTreeMap;
use std::rc::Rc;

use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::capture::{Capture, Event};
use timely::dataflow::operators::*;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::join::join_with_tactic;
use differential_dataflow::trace::implementations::chunker::ContainerChunker;
use differential_dataflow::trace::implementations::merge_batcher::vec::VecMerger;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;

use interactive::corgi_arrange::{CorgiBatch, CorgiBatchBuilder};
use interactive::corgi_backend::CorgiContainer;
use interactive::corgi_join::CorgiJoinTactic;
use interactive::ir::{Diff, Value as DValue};
use interactive::parse::{BinOp, Term};

type Row = DValue;
type Upd = ((Row, Row), u64, Diff);
type CC = CorgiContainer<u64, Diff>;
type Trace = Spine<Rc<CorgiBatch<u64, Diff>>>;
type OutCB = CapacityContainerBuilder<Vec<Upd>>;

fn reference(lu: &[((u64, u64), Diff)], ru: &[((u64, u64), Diff)]) -> BTreeMap<(u64, u64), Diff> {
    // join by key, project (key, val0+val1), sum diffs.
    let mut lmap: BTreeMap<u64, Vec<(u64, Diff)>> = BTreeMap::new();
    let mut rmap: BTreeMap<u64, Vec<(u64, Diff)>> = BTreeMap::new();
    for ((k, v), d) in lu {
        lmap.entry(*k).or_default().push((*v, *d));
    }
    for ((k, v), d) in ru {
        rmap.entry(*k).or_default().push((*v, *d));
    }
    let mut out = BTreeMap::new();
    for (k, lvs) in &lmap {
        if let Some(rvs) = rmap.get(k) {
            for (vl, dl) in lvs {
                for (vr, dr) in rvs {
                    *out.entry((*k, vl + vr)).or_insert(0) += dl * dr;
                }
            }
        }
    }
    out.retain(|_, d| *d != 0);
    out
}

fn main() {
    let lu: Vec<((u64, u64), Diff)> = vec![((1, 10), 1), ((1, 20), 1), ((2, 5), 1), ((2, 6), 1), ((3, 1), 1), ((2, 6), -1)];
    let ru: Vec<((u64, u64), Diff)> = vec![((1, 100), 1), ((2, 200), 1), ((2, 300), 1), ((4, 9), 1)];

    let l_rows: Vec<(Row, Row)> = lu.iter().map(|((k, v), _)| (DValue::Int(*k as i64), DValue::Int(*v as i64))).collect();
    let l_diffs: Vec<Diff> = lu.iter().map(|(_, d)| *d).collect();
    let r_rows: Vec<(Row, Row)> = ru.iter().map(|((k, v), _)| (DValue::Int(*k as i64), DValue::Int(*v as i64))).collect();
    let r_diffs: Vec<Diff> = ru.iter().map(|(_, d)| *d).collect();

    let (tx, rx) = std::sync::mpsc::channel::<Event<u64, Vec<Upd>>>();
    timely::execute_directly(move |worker| {
        let (mut h0, mut h1) = worker.dataflow::<u64, _, _>(|scope| {
            let (h0, c0) = scope.new_collection::<(Row, Row), Diff>();
            let (h1, c1) = scope.new_collection::<(Row, Row), Diff>();

            let s0 = c0.inner.unary(Pipeline, "ToCorgi0", |_, _| {
                |input, output| {
                    input.for_each(|cap, data| {
                        let mut c = CorgiContainer::from_updates(std::mem::take(data));
                        output.session(&cap).give_container(&mut c);
                    });
                }
            });
            let s1 = c1.inner.unary(Pipeline, "ToCorgi1", |_, _| {
                |input, output| {
                    input.for_each(|cap, data| {
                        let mut c = CorgiContainer::from_updates(std::mem::take(data));
                        output.session(&cap).give_container(&mut c);
                    });
                }
            });

            let a0 = arrange_core::<_, CC, ContainerChunker<Vec<Upd>>, MergeBatcher<VecMerger<(Row, Row), u64, Diff>>, RcBuilder<CorgiBatchBuilder<u64, Diff>>, Trace>(s0, Pipeline, "CorgiArr0");
            let a1 = arrange_core::<_, CC, ContainerChunker<Vec<Upd>>, MergeBatcher<VecMerger<(Row, Row), u64, Diff>>, RcBuilder<CorgiBatchBuilder<u64, Diff>>, Trace>(s1, Pipeline, "CorgiArr1");

            // projection: (key, val0, val1) -> (key, val0 + val1); the tactic compiles it per work-unit.
            let key = Term::Var(0);
            let val = Term::Binary(BinOp::Add, Box::new(Term::Var(1)), Box::new(Term::Var(2)));
            let out = join_with_tactic::<_, _, _, OutCB>(a0, a1, CorgiJoinTactic::new(key, val));
            out.capture_into(tx);

            (h0, h1)
        });
        for (r, d) in l_rows.into_iter().zip(l_diffs) {
            h0.update(r, d);
        }
        for (r, d) in r_rows.into_iter().zip(r_diffs) {
            h1.update(r, d);
        }
    });

    let mut got: BTreeMap<(u64, u64), Diff> = BTreeMap::new();
    for event in rx {
        if let Event::Messages(_, data) = event {
            for ((k, v), _, d) in data {
                if let (DValue::Int(k), DValue::Int(v)) = (k, v) {
                    *got.entry((k as u64, v as u64)).or_insert(0) += d;
                }
            }
        }
    }
    got.retain(|_, d| *d != 0);

    let want = reference(&lu, &ru);
    assert_eq!(got, want, "corgi join dataflow != reference");

    println!("M2(b)-4 LIVE: corgi JOIN in a real DD dataflow via CorgiJoinTactic");
    println!("  output rows: {} — matches reference hash-join (incl. retraction)", got.len());
    println!("  e.g. {:?}", got);
    println!("\nJoin operator works cursor-less over corgi columns through the #771–773 tactic seam.");
}
