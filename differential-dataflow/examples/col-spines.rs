//! Arrangement experiments starting from columnar containers.
//!
//! This example builds `ColContainer` inputs directly using a
//! `ColContainerBuilder` and `InputHandle`, then feeds them through
//! the arrangement pipeline via `arrange_core` with `ColumnarKeyBatcher`.

use timely::container::{ContainerBuilder, PushInto};
use timely::dataflow::InputHandle;
use timely::dataflow::ProbeHandle;

use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::containers::ColContainerBuilder;

fn main() {

    type Update = ((String, ()), u64, isize);
    type Builder = ColContainerBuilder<Update>;

    let keys: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let size: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    let timer1 = ::std::time::Instant::now();
    let timer2 = timer1.clone();

    timely::execute_from_args(std::env::args(), move |worker| {

        let mut data_input = <InputHandle<_, Builder>>::new_with_builder();
        let mut keys_input = <InputHandle<_, Builder>>::new_with_builder();
        let mut probe = ProbeHandle::new();

        worker.dataflow::<u64, _, _>(|scope| {

            let data = data_input.to_stream(scope);
            let keys = keys_input.to_stream(scope);

            use timely::dataflow::channels::pact::Pipeline;
            use differential_dataflow::trace::implementations::ord_neu::{
                ColumnarColKeyBatcher, RcColumnarKeyBuilder, ColumnarKeySpine,
            };

            let data = arrange_core::<_, _, ColumnarColKeyBatcher<_,_,_>, RcColumnarKeyBuilder<_,_,_>, ColumnarKeySpine<_,_,_>>(
                data, Pipeline, "Data",
            );
            let keys = arrange_core::<_, _, ColumnarColKeyBatcher<_,_,_>, RcColumnarKeyBuilder<_,_,_>, ColumnarKeySpine<_,_,_>>(
                keys, Pipeline, "Keys",
            );

            keys.join_core(data, |_k, (), ()| Option::<()>::None)
                .probe_with(&mut probe);
        });

        let mut data_builder = Builder::default();
        let mut keys_builder = Builder::default();

        // Load up data in batches.
        // Note: `format_args!` avoids allocating a String; the columnar
        // `Strings` container accepts `fmt::Arguments` directly and writes
        // the formatted output straight into its byte buffer.
        let mut counter = 0;
        while counter < 10 * keys {
            let mut i = worker.index();
            let time = *data_input.time();
            while i < size {
                let val = (counter + i) % keys;
                data_builder.push_into(((format_args!("{:?}", val), ()), time, 1isize));
                i += worker.peers();
            }
            while let Some(container) = data_builder.finish() {
                data_input.send_batch(container);
            }
            counter += size;
            data_input.advance_to(data_input.time() + 1);
            keys_input.advance_to(keys_input.time() + 1);
            while probe.less_than(data_input.time()) {
                worker.step_or_park(None);
            }
        }
        println!("{:?}\tloading complete", timer1.elapsed());

        let mut queries = 0;
        while queries < 10 * keys {
            let mut i = worker.index();
            let time = *data_input.time();
            while i < size {
                let val = (queries + i) % keys;
                keys_builder.push_into(((format_args!("{:?}", val), ()), time, 1isize));
                i += worker.peers();
            }
            while let Some(container) = keys_builder.finish() {
                keys_input.send_batch(container);
            }
            queries += size;
            data_input.advance_to(data_input.time() + 1);
            keys_input.advance_to(keys_input.time() + 1);
            while probe.less_than(data_input.time()) {
                worker.step_or_park(None);
            }
        }

        println!("{:?}\tqueries complete", timer1.elapsed());

    }).unwrap();

    println!("{:?}\tshut down", timer2.elapsed());

}
