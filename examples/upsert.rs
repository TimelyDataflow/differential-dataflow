extern crate timely;
extern crate differential_dataflow;

fn main() {

    // define a new timely dataflow computation.
    timely::execute_from_args(std::env::args().skip(1), move |worker| {

        type Key = String;
        type Val = String;

        let mut input = timely::dataflow::InputHandle::<usize, (Key, Option<Val>, usize)>::new();
        let mut probe = timely::dataflow::ProbeHandle::new();

        // create a dataflow managing an ever-changing edge collection.
        worker.dataflow::<usize,_,_>(|scope| {

            use timely::dataflow::operators::Input;
            use differential_dataflow::trace::implementations::ord::OrdValSpine;
            use differential_dataflow::operators::arrange::upsert;

            let stream = scope.input_from(&mut input);
            let arranged = upsert::arrange_from_upsert::<_, OrdValSpine<Key, Val, usize, isize>>(&stream, &"test");

            arranged
                .as_collection(|k,v| (k.clone(), v.clone()))
                .inspect(|x| println!("Observed: {:?}", x))
                .probe_with(&mut probe);
        });

        input.send(("frank".to_string(), Some("mcsherry".to_string()), 3));
        input.advance_to(4);
        while probe.less_than(input.time()) { worker.step(); }

        input.send(("frank".to_string(), Some("zappa".to_string()), 4));
        input.advance_to(5);
        while probe.less_than(input.time()) { worker.step(); }

        input.send(("frank".to_string(), None, 5));
        input.advance_to(9);
        while probe.less_than(input.time()) { worker.step(); }

        input.send(("frank".to_string(), Some("oz".to_string()), 9));
        input.advance_to(10);
        while probe.less_than(input.time()) { worker.step(); }

        input.send(("frank".to_string(), None, 15));
        input.close();

    }).unwrap();
}
