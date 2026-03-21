use timely::dataflow::Scope;
use timely::dataflow::operators::{Input, Probe, Enter, Leave};
use timely::dataflow::operators::core::Filter;

fn main() {
    // initializes and runs a timely dataflow.
    timely::execute_from_args(std::env::args(), |worker| {

        let timer = std::time::Instant::now();

        let mut args = std::env::args();
        args.next();

        let dataflows = args.next().unwrap().parse::<usize>().unwrap();
        let length = args.next().unwrap().parse::<usize>().unwrap();
        let local = args.next() == Some("local".to_string());
        println!("Local: {:?}", local);

        let mut inputs = Vec::new();
        let mut probes = Vec::new();

        // create a new input, exchange data, and inspect its output
        for _dataflow in 0 .. dataflows {
            worker.dataflow(|scope| {
                let (input, stream) = scope.new_input();
                let stream = scope.region(|inner| {
                    let mut stream = stream.enter(inner);
                    use differential_dataflow::operators::arrange::arrangement::{arrange_intra, arrange_inter};
                    use differential_dataflow::trace::implementations::{ValBatcher, ValBuilder, ValSpine};
                    stream = if local { arrange_intra::<_,_,ValBatcher<_,_,_,_>,ValBuilder<_,_,_,_>,ValSpine<_,_,_,_>>(stream, timely::dataflow::channels::pact::Pipeline, "test").as_collection(|k: &i32,v: &i32| (*k, *v)).inner }
                    else {              arrange_inter::<_,_,ValBatcher<_,_,_,_>,ValBuilder<_,_,_,_>,ValSpine<_,_,_,_>>(stream, timely::dataflow::channels::pact::Pipeline, "test").as_collection(|k: &i32,v: &i32| (*k, *v)).inner };
                    for _step in 0 .. length {
                        stream = stream.filter(|_| false);
                    }
                    stream.leave()
                });
                let (probe, _stream) = stream.probe();
                inputs.push(input);
                probes.push(probe);
            });
        }

        println!("{:?}\tdataflows built ({} x {})", timer.elapsed(), dataflows, length);

        // Repeatedly, insert a record in one dataflow, tick all dataflow inputs.
        for round in 0 .. {
            let dataflow = round % dataflows;
            inputs[dataflow].send(((0i32, 0i32), round, 1));
            for d in 0 .. dataflows {  inputs[d].advance_to(round); }
            let mut steps = 0;
            while probes[dataflow].less_than(&round) {
                worker.step();
                steps += 1;
            }

            println!("{:?}\tround {} complete in {} steps", timer.elapsed(), round, steps);
        }

    }).unwrap();
}
