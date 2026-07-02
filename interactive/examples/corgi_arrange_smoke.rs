//! M2(b) arrange wiring: arrange a corgi-container collection into a `Spine<Rc<CorgiBatch>>` via
//! `arrange_core`, reusing DD's `MergeBatcher`+`VecMerger` (Batcher) and `ContainerChunker` (the
//! container drains corgi→rows), with `CorgiBatchBuilder` transcoding the sorted chain → corgi
//! columns at build. Milestone: it COMPILES and RUNS (content correctness comes with reduce).
//!
//!   ~/.cargo/bin/cargo run --release -p interactive --example corgi_arrange_smoke

use std::rc::Rc;

use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::*;

use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::trace::implementations::chunker::ContainerChunker;
use differential_dataflow::trace::implementations::merge_batcher::vec::VecMerger;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;

use interactive::corgi_arrange::{CorgiBatch, CorgiBatchBuilder};
use interactive::corgi_backend::CorgiContainer;
use interactive::ir::{Diff, Value as DValue};

type Row = DValue;
type Upd = ((Row, Row), u64, Diff);
type CC = CorgiContainer<u64, Diff>;
type Trace = Spine<Rc<CorgiBatch<u64, Diff>>>;

fn main() {
    // Unique (key,val) so nothing consolidates away; all at time 0.
    let data: Vec<(Row, Row)> = (0..1000).map(|i| (DValue::Int(i), DValue::Int(i * 2))).collect();
    let n = data.len();

    timely::execute_directly(move |worker| {
        let mut handle = worker.dataflow::<u64, _, _>(|scope| {
            let (handle, coll) = scope.new_collection::<(Row, Row), Diff>();

            // ingest: rows -> CorgiContainer
            let corgi_stream = coll.inner.unary(Pipeline, "ToCorgi", |_, _| {
                |input, output| {
                    input.for_each(|cap, data| {
                        let mut c = CorgiContainer::from_updates(std::mem::take(data));
                        output.session(&cap).give_container(&mut c);
                    });
                }
            });

            // arrange the corgi-container stream into a corgi-columnar trace.
            let _arranged = arrange_core::<
                _,
                CC,
                ContainerChunker<Vec<Upd>>,
                MergeBatcher<VecMerger<(Row, Row), u64, Diff>>,
                RcBuilder<CorgiBatchBuilder<u64, Diff>>,
                Trace,
            >(corgi_stream, Pipeline, "CorgiArrange");

            handle
        });
        for r in data {
            handle.update(r, 1);
        }
    });

    println!("M2(b) arrange: arrange_core over corgi containers COMPILED and RAN.");
    println!("  fed {n} unique (key,val) updates → Spine<Rc<CorgiBatch>> (corgi-columnar trace).");
    println!("  Batcher = reused MergeBatcher<VecMerger>; Builder transcodes rows→corgi at ingest.");
}
