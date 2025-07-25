use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;

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
                "new" => {
                    use differential_dataflow::trace::implementations::ord_neu::{ColKeyBatcher, ColKeyBuilder, ColKeySpine};
                    let data = data.arrange::<ColKeyBatcher<_,_,_>, ColKeyBuilder<_,_,_>, ColKeySpine<_,_,_>>();
                    let keys = keys.arrange::<ColKeyBatcher<_,_,_>, ColKeyBuilder<_,_,_>, ColKeySpine<_,_,_>>();
                    keys.join_core(&data, |_k, &(), &()| Option::<()>::None)
                        .probe_with(&mut probe);
                },
                "old" => {
                    use differential_dataflow::trace::implementations::ord_neu::{OrdKeyBatcher, RcOrdKeyBuilder, OrdKeySpine};
                    let data = data.arrange::<OrdKeyBatcher<_,_,_>, RcOrdKeyBuilder<_,_,_>, OrdKeySpine<_,_,_>>();
                    let keys = keys.arrange::<OrdKeyBatcher<_,_,_>, RcOrdKeyBuilder<_,_,_>, OrdKeySpine<_,_,_>>();
                    keys.join_core(&data, |_k, &(), &()| Option::<()>::None)
                        .probe_with(&mut probe);
                },
                "rhh" => {
                    use differential_dataflow::trace::implementations::rhh::{HashWrapper, VecBatcher, VecBuilder, VecSpine};
                    let data = data.map(|x| HashWrapper { inner: x }).arrange::<VecBatcher<_,(),_,_>, VecBuilder<_,(),_,_>, VecSpine<_,(),_,_>>();
                    let keys = keys.map(|x| HashWrapper { inner: x }).arrange::<VecBatcher<_,(),_,_>, VecBuilder<_,(),_,_>, VecSpine<_,(),_,_>>();
                    keys.join_core(&data, |_k, &(), &()| Option::<()>::None)
                        .probe_with(&mut probe);
                },
                _ => {
                    println!("unrecognized mode: {:?}", mode)
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
