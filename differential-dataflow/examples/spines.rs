use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {

    let keys: usize = std::env::args().nth(1).unwrap().parse().unwrap();
    let size: usize = std::env::args().nth(2).unwrap().parse().unwrap();

    let mode: String = std::env::args().nth(3).unwrap();

    println!("Running [{:?}] arrangement", mode);

    let timer1 = ::std::time::Instant::now();
    let timer2 = timer1.clone();

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        // define BFS dataflow; return handles to roots and edges inputs
        let mut probe = Handle::new();
        let (mut data_input, mut keys_input) = worker.dataflow(|scope| {

            use differential_dataflow::operators::{arrange::Arrange};

            let (data_input, data) = scope.new_collection::<String, isize>();
            let (keys_input, keys) = scope.new_collection::<String, isize>();

            match mode.as_str() {
                "key" => {
                    use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatcher, RcOrdKeyBuilder, OrdKeySpine};
                    let data = data.arrange::<OrdKeyBatcher<String,_,isize>, RcOrdKeyBuilder<String,_,isize>, OrdKeySpine<String,_,isize>>();
                    let keys = keys.arrange::<OrdKeyBatcher<String,_,isize>, RcOrdKeyBuilder<String,_,isize>, OrdKeySpine<String,_,isize>>();
                    keys.join_core(data, |_k, &(), &()| Option::<()>::None)
                        .probe_with(&mut probe);
                },
                "val" => {
                    use differential_dataflow::trace::implementations::ord_neu::{OrdValBatcher, RcOrdValBuilder, OrdValSpine};
                    let data = data.map(|x| (x, ())).arrange::<OrdValBatcher<String,(),_,isize>, RcOrdValBuilder<String,(),_,isize>, OrdValSpine<String,(),_,isize>>();
                    let keys = keys.map(|x| (x, ())).arrange::<OrdValBatcher<String,(),_,isize>, RcOrdValBuilder<String,(),_,isize>, OrdValSpine<String,(),_,isize>>();
                    keys.join_core(data, |_k, &(), &()| Option::<()>::None)
                        .probe_with(&mut probe);
                },
                "col" => {
                    use timely::dataflow::operators::generic::Operator;
                    use timely::dataflow::channels::pact::Pipeline;
                    use differential_dataflow::columnar::{ValBatcher, ValBuilder, ValColBuilder, ValPact, ValSpine};
                    use differential_dataflow::operators::arrange::arrangement::arrange_core;

                    type DataU = (String, (), u64, i64);

                    fn string_hash(s: columnar::Ref<'_, String>) -> u64 {
                        use std::hash::{Hash, Hasher};
                        let mut h = std::collections::hash_map::DefaultHasher::new();
                        s.hash(&mut h);
                        h.finish()
                    }

                    // Convert a Vec-container stream of `((K, ()), Time, Diff)` into
                    // a `RecordedUpdates`-container stream feeding the columnar batcher.
                    fn to_recorded<'scope>(
                        stream: timely::dataflow::Stream<'scope, u64, Vec<((String, ()), u64, isize)>>,
                    ) -> timely::dataflow::Stream<'scope, u64, differential_dataflow::columnar::RecordedUpdates<DataU>> {
                        stream.unary::<ValColBuilder<DataU>, _, _, _>(Pipeline, "ToRecorded", |_, _| {
                            move |input, output| {
                                input.for_each_time(|cap, batches| {
                                    let mut session = output.session_with_builder(&cap);
                                    for batch in batches {
                                        for ((k, _), t, d) in batch.drain(..) {
                                            session.give((k.as_str(), (), t, d as i64));
                                        }
                                    }
                                });
                            }
                        })
                    }

                    let data_stream = to_recorded(data.map(|x| (x, ())).inner);
                    let keys_stream = to_recorded(keys.map(|x| (x, ())).inner);

                    let data = arrange_core::<_, ValBatcher<String,(),u64,i64>, ValBuilder<String,(),u64,i64>, ValSpine<String,(),u64,i64>>(
                        data_stream, ValPact { hashfunc: |k: columnar::Ref<'_, String>| string_hash(k) }, "DataArrange",
                    );
                    let keys = arrange_core::<_, ValBatcher<String,(),u64,i64>, ValBuilder<String,(),u64,i64>, ValSpine<String,(),u64,i64>>(
                        keys_stream, ValPact { hashfunc: |k: columnar::Ref<'_, String>| string_hash(k) }, "KeysArrange",
                    );
                    keys.join_core(data, |_k, (), ()| Option::<()>::None)
                        .probe_with(&mut probe);
                },
                _ => {
                    panic!("unrecognized mode: {:?}", mode);
                }
            }

            (data_input, keys_input)
        });

        // Load up data in batches.
        let mut counter = 0;
        while counter < 10 * keys {
            let mut i = worker.index();
            while i < size {
                let val = (counter + i) % keys;
                data_input.insert(format!("{:?}", val));
                i += worker.peers();
            }
            counter += size;
            data_input.advance_to(data_input.time() + 1);
            data_input.flush();
            keys_input.advance_to(keys_input.time() + 1);
            keys_input.flush();
            while probe.less_than(data_input.time()) {
                worker.step();
            }
        }
        println!("{:?}\tloading complete", timer1.elapsed());

        let mut queries = 0;

        while queries < 10 * keys {
            let mut i = worker.index();
            while i < size {
                let val = (queries + i) % keys;
                keys_input.insert(format!("{:?}", val));
                i += worker.peers();
            }
            queries += size;
            data_input.advance_to(data_input.time() + 1);
            data_input.flush();
            keys_input.advance_to(keys_input.time() + 1);
            keys_input.flush();
            while probe.less_than(data_input.time()) {
                worker.step();
            }
        }

        println!("{:?}\tqueries complete", timer1.elapsed());

        // loop { }

    }).unwrap();

    println!("{:?}\tshut down", timer2.elapsed());

}
