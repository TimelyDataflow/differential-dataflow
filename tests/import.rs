extern crate timely;
extern crate rand;
extern crate itertools;
extern crate differential_dataflow;
use timely::dataflow::operators::*;
use timely::dataflow::operators::capture::Extract;
use timely::progress::timestamp::RootTimestamp;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::arrange::ArrangeByKey;
use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::trace::Trace;
use differential_dataflow::hashable::OrdWrapper;
use itertools::Itertools;

#[test]
fn test_import() {

    let captured = timely::execute(timely::Configuration::Process(4), |worker| {
        let index = worker.index();
        let peers = worker.peers();

        let (mut input, mut trace) = worker.dataflow(|scope| {
            let (input, edges) = scope.new_input();
            let arranged = edges.as_collection()
                                .arrange_by_key_hashed();
            (input, arranged.trace.clone())
        });
        let (captured,) = worker.dataflow(move |scope| {
            let imported = trace.import(scope);
            ::std::mem::drop(trace);
            let captured = imported.group_arranged::<_, i64, _, _>(|_k, s, t| t.push((s.iter().map(|&(_, w)| w).sum(), 1i64)), OrdValSpine::new())
                  .as_collection(|k: &OrdWrapper<u64>, c: &i64| (k.item.clone(), *c))
                  .inner.exchange(|_| 0)
                  .capture();
            (captured,)
        });

        let input_epochs: Vec<Vec<((u64, u64), i64)>> = vec![
            vec![((2, 0), 1), ((1, 0), 1), ((1, 3), 1), ((4, 2), 1)],
            vec![((2, 0), -1), ((1, 0), -1)],
            vec![((2, 0), 1), ((1, 3), -1)],
            vec![((1, 0), 1), ((1, 3), 1), ((2, 0), -1), ((4, 2), -1)],
            vec![((2, 0), 1), ((4, 2), 1), ((1, 3), -1)],
            vec![((1, 3), 1), ((4, 2), -1)],
        ];

        for (t, changes) in input_epochs.into_iter().enumerate() {
            if t != input.time().inner {
                input.advance_to(t);
            }
            let &time = input.time();
            for ((src, dst), w) in changes.into_iter().filter(|&((src, _), _)| (src as usize) % peers == index) {
                input.send(((src, dst), time, w));
            }
        }
        input.close();

        captured
    }).unwrap().join().into_iter().map(|x| x.unwrap()).next().unwrap();

    let mut results = captured.extract().into_iter().flat_map(|(_, data)| data).collect::<Vec<_>>();
    results.sort_by_key(|&(_, t, _)| t);
    let out = results.into_iter().group_by(|&(_, t, _)| t).into_iter()
        .map(|(t, vals)| (t, vals.map(|(v, _, w)| (v, w)).collect::<Vec<_>>())).collect::<Vec<_>>();
    // println!("out: {:?}", out);
    assert!(out == vec![
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
