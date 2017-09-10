extern crate timely;
extern crate itertools;
extern crate differential_dataflow;

use timely::dataflow::operators::*;
use timely::dataflow::operators::capture::Extract;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::arrange::{ArrangeByKey, Arrange};
use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::trace::{Trace, TraceReader};
// use differential_dataflow::hashable::{OrdWrapper, UnsignedWrapper};
use itertools::Itertools;

type Result = std::sync::mpsc::Receiver<timely::dataflow::operators::capture::Event<timely::progress::nested::product::Product<timely::progress::timestamp::RootTimestamp, usize>, ((u64, i64), timely::progress::nested::product::Product<timely::progress::timestamp::RootTimestamp, usize>, i64)>>;

fn run_test<T>(test: T, expected: Vec<(Product<RootTimestamp, usize>, Vec<((u64, i64), i64)>)>) -> ()
        where T: FnOnce(Vec<Vec<((u64, u64), i64)>>)-> Result + ::std::panic::UnwindSafe
{
    let input_epochs: Vec<Vec<((u64, u64), i64)>> = vec![
        vec![((2, 0), 1), ((1, 0), 1), ((1, 3), 1), ((4, 2), 1)],
        vec![((2, 0), -1), ((1, 0), -1)],
        vec![((2, 0), 1), ((1, 3), -1)],
        vec![((1, 0), 1), ((1, 3), 1), ((2, 0), -1), ((4, 2), -1)],
        vec![((2, 0), 1), ((4, 2), 1), ((1, 3), -1)],
        vec![((1, 3), 1), ((4, 2), -1)],
    ];
    let captured = (test)(input_epochs);
    let mut results = captured.extract().into_iter().flat_map(|(_, data)| data).collect::<Vec<_>>();
    results.sort_by_key(|&(_, t, _)| t);
    let out = 
    results
        .into_iter()
        .group_by(|&(_, t, _)| t)
        .into_iter()
        .map(|(t, vals)| { 
            let mut vec = vals.map(|(v, _, w)| (v, w)).collect::<Vec<_>>();
            vec.sort();
            (t, vec)
        }).collect::<Vec<_>>();
    // println!("out: {:?}", out);
    assert_eq!(out, expected);
}

#[test]
fn test_import() {
    run_test(|input_epochs| {
        timely::execute(timely::Configuration::Process(4), move |worker| {
            let ref input_epochs = input_epochs;
            let index = worker.index();
            let peers = worker.peers();

            let (mut input, mut trace) = worker.dataflow(|scope| {
                let (input, edges) = scope.new_input();
                let arranged = edges.as_collection()
                                    .arrange_by_key();
                (input, arranged.trace.clone())
            });
            let (captured,) = worker.dataflow(move |scope| {
                let imported = trace.import(scope);
                ::std::mem::drop(trace);
                let captured = imported.group_arranged::<_, i64, _, _>(|_k, s, t| t.push((s.iter().map(|&(_, w)| w).sum(), 1i64)), OrdValSpine::new())
                      .as_collection(|k: &u64, c: &i64| (k.clone(), *c))
                      .inner.exchange(|_| 0)
                      .capture();
                (captured,)
            });


            for (t, changes) in input_epochs.into_iter().enumerate() {
                if t != input.time().inner {
                    input.advance_to(t);
                }
                let &time = input.time();
                for &((src, dst), w) in changes.into_iter().filter(|&&((src, _), _)| (src as usize) % peers == index) {
                    input.send(((src, dst), time, w));
                }
            }
            input.close();

            captured
        }).unwrap().join().into_iter().map(|x| x.unwrap()).next().unwrap()
    }, vec![
        (RootTimestamp::new(0), vec![
             ((1, 2), 1), ((2, 1), 1), ((4, 1), 1)]),
        (RootTimestamp::new(1), vec![
             ((1, 1), 1), ((1, 2), -1), ((2, 1), -1)]),
        (RootTimestamp::new(2), vec![
             ((1, 1), -1), ((2, 1), 1)]),
        (RootTimestamp::new(3), vec![
             ((1, 2), 1), ((2, 1), -1), ((4, 1), -1)]),
        (RootTimestamp::new(4), vec![
             ((1, 1), 1), ((1, 2), -1), ((2, 1), 1), ((4, 1), 1)]),
        (RootTimestamp::new(5), vec![
             ((1, 1), -1), ((1, 2), 1), ((4, 1), -1)]),
    ]);
}

#[test]
fn test_import_completed_dataflow() {
    // Runs the first dataflow to completion before constructing the subscriber.
    run_test(|input_epochs| {
        timely::execute(timely::Configuration::Process(4), move |worker| {
            let ref input_epochs = input_epochs;
            let index = worker.index();
            let peers = worker.peers();

            let (mut input, mut trace, probe) = worker.dataflow(|scope| {
                let (input, edges) = scope.new_input();
                let arranged = edges.as_collection()
                                    .arrange_by_key();
                (input, arranged.trace.clone(), arranged.stream.probe())
            });

            for (t, changes) in input_epochs.into_iter().enumerate() {
                if t != input.time().inner {
                    input.advance_to(t);
                }
                let &time = input.time();
                for &((src, dst), w) in changes.into_iter().filter(|&&((src, _), _)| (src as usize) % peers == index) {
                    input.send(((src, dst), time, w));
                }
            }
            input.close();

            worker.step_while(|| !probe.done());

            let (_probe2, captured,) = worker.dataflow(move |scope| {
                let imported = trace.import(scope);
                ::std::mem::drop(trace);
                let stream = imported.group_arranged::<_, i64, _, _>(|_k, s, t| t.push((s.iter().map(|&(_, w)| w).sum(), 1i64)), OrdValSpine::new())
                      .as_collection(|k: &u64, c: &i64| (k.clone(), *c))
                      .inner.exchange(|_| 0);
                let probe = stream.probe();
                let captured = stream.capture();
                (probe, captured,)
            });

            // println!("probe2: {}", probe2.with_frontier(|f| format!("{:?}", f)));

            captured
        }).unwrap().join().into_iter().map(|x| x.unwrap()).next().unwrap()
    }, vec![
        (RootTimestamp::new(0), vec![
             ((1, 2), 1), ((2, 1), 1), ((4, 1), 1)]),
        (RootTimestamp::new(1), vec![
             ((1, 1), 1), ((1, 2), -1), ((2, 1), -1)]),
        (RootTimestamp::new(2), vec![
             ((1, 1), -1), ((2, 1), 1)]),
        (RootTimestamp::new(3), vec![
             ((1, 2), 1), ((2, 1), -1), ((4, 1), -1)]),
        (RootTimestamp::new(4), vec![
             ((1, 1), 1), ((1, 2), -1), ((2, 1), 1), ((4, 1), 1)]),
        (RootTimestamp::new(5), vec![
             ((1, 1), -1), ((1, 2), 1), ((4, 1), -1)]),
    ]);
}

#[ignore]
#[test]
fn import_skewed() {

    run_test(|_input| {
        let captured = timely::execute(timely::Configuration::Process(4), |worker| {
            let index = worker.index();
            let peers = worker.peers();

            let (mut input, mut trace) = worker.dataflow(|scope| {
                let (input, edges) = scope.new_input();
                let arranged = edges.as_collection()
                                    .arrange(OrdValSpine::new());
                (input, arranged.trace.clone())
            });

            input.send(((index as u64, 1), RootTimestamp::new(index), 1));
            input.close();

            trace.advance_by(&[RootTimestamp::new(peers - index)]);

            let (captured,) = worker.dataflow(move |scope| {
                let imported = trace.import(scope);
                let captured = imported
                      .as_collection(|k: &u64, c: &i64| (k.clone(), *c))
                      .inner.exchange(|_| 0)
                      .capture();
                (captured,)
            });

            captured
        }).unwrap().join().into_iter().map(|x| x.unwrap()).next().unwrap();

        // Worker `index` sent `index` at time `index`, but advanced its handle to `peers - index`. 
        // As its data should be shuffled back to it (we used an UnsignedWrapper) this means that 
        // `index` should be present at time `max(index, peers-index)`.

        captured
    }, vec![
        (RootTimestamp::new(2), vec![((2, 1), 1)]),
        (RootTimestamp::new(3), vec![((1, 1), 1), ((3, 1), 1)]),
        (RootTimestamp::new(4), vec![((0, 1), 1)]),
    ]);
}
