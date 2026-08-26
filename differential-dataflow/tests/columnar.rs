use std::time::{Duration, Instant};

use timely::dataflow::operators::{Input, Probe};
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::Config;

use differential_dataflow::columnar::collection;
use differential_dataflow::columnar::trace::{Batcher, Builder, Chunker, Spine};
use differential_dataflow::operators::arrange::arrangement::arrange_core;

type Upd = (u64, (), u64, i64);

/// Feed the dataflow with a single (key, val) at the provided `diffs` the
/// advance the frontier to 1.
///
/// Returns false if the dataflow hangs for longer than 5s.
fn arrange_reaches_one(config: Config, diffs: &'static [i64]) -> bool {
    let guards = timely::execute(config, move |worker| {
        let index = worker.index();
        let mut probe = ProbeHandle::new();
        let mut input = <InputHandle<u64, collection::Builder<Upd>>>::new_with_builder();

        worker.dataflow::<u64, _, _>(|scope| {
            let stream = scope.input_from(&mut input);
            let pact = collection::Pact {
                hashfunc: |k: columnar::Ref<'_, u64>| *k,
            };

            arrange_core::<
                _,
                _,
                Chunker<Upd>,
                _,
                Builder<u64, (), u64, i64>,
                Spine<u64, (), u64, i64>,
            >(stream, pact, "Arrange", Batcher::new)
            .stream
            .probe_with(&mut probe);
        });

        for &diff in diffs {
            input.send((index as u64, (), 0, diff));
        }
        input.advance_to(1);
        input.flush();

        let start = Instant::now();
        while probe.less_than(&1) {
            worker.step();
            if start.elapsed() > Duration::from_secs(5) {
                for id in worker.installed_dataflows() {
                    worker.drop_dataflow(id);
                }
                return false;
            }
        }
        true
    })
    .expect("timely execute");

    guards.join().into_iter().all(|r| r.unwrap_or(false))
}

#[test]
fn columnar_exchange_net_empty_container() {
    // [+1, -1] diffs should consolidate to nothing, but the exchange must
    // still deliver a message with `records = 2`.
    //
    // Previously the exchange did not emit a message and the dataflow froze.
    assert!(
        arrange_reaches_one(Config::process(2), &[1, -1]),
        "frontier stalled: record count lost for an all-cancelled container"
    );
}

#[test]
fn columnar_exchange_consolidated_container() {
    assert!(arrange_reaches_one(Config::process(2), &[1, 1, 1]));
    assert!(arrange_reaches_one(Config::process(3), &[2, -1, 1]));
}
