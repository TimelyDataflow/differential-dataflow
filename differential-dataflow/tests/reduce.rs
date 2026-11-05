use timely::dataflow::operators::{ToStream, Capture, vec::Map};
use timely::dataflow::operators::capture::Extract;
use differential_dataflow::AsCollection;
use differential_dataflow::input::InputSession;
use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};

#[test]
fn reduce() {

    let data = timely::example(|scope| {

        let col1 = vec![((0,0), Default::default(), 1),((1,2),Default::default(), 1),((1,1),Default::default(), 1)]
                        .into_iter()
                        .to_stream(scope)
                        .as_collection();

        col1.reduce(|_,s,t| t.push((*s[0].0, s.len() as isize))).inner.capture()
    });

    let extracted = data.extract();
    assert_eq!(extracted.len(), 1);
    assert_eq!(extracted[0].1, vec![((0,0),Default::default(), 1), ((1,1),Default::default(), 2)]);
}

#[test]
fn reduce_scaling() {

    let data = timely::example(|scope| {

        // let scale = 100;
        let scale = 100_000;

        (0 .. 1).to_stream(scope)
                .flat_map(move |_| (0..scale).map(|i| ((), i, 1)))
                .as_collection()
                .count()
                .inner
                .capture()
    });

    let extracted = data.extract();
    assert_eq!(extracted.len(), 1);
}

/// The reduce can see an input frontier ahead of the upper of its input trace when it runs before
/// the arrange that feeds it. Here `arrange_by_key` builds in the outer scope after `region` has
/// reserved its slot, so timely schedules the region, and the reduce in it, before the arrange.
#[test]
fn reduce_frontier_ahead_of_trace() {
    let (send, recv) = std::sync::mpsc::channel();
    timely::execute_directly(move |worker| {
        let mut input = InputSession::<u64, (u64, u64), isize>::new();
        let probe = worker.dataflow::<u64, _, _>(|scope| {
            let collection = input.to_collection(scope);
            scope.region(|inner| {
                let (probe, output) = collection
                    .arrange_by_key()
                    .enter_region(inner)
                    .reduce_abelian::<_, ValBuilder<u64, u64, u64, isize>, ValSpine<u64, u64, u64, isize>, _, _>(
                        "Reduce",
                        |_key, src, dst| dst.push((src.len() as u64, 1)),
                        |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v, t, r)| ((*key, v), t, r))); },
                    )
                    .as_collection(|k, v| (*k, *v))
                    .leave_region(scope)
                    .probe();
                output.inner.capture_into(send);
                probe
            })
        });

        // Once the frontier reaches 6, the arrange seals [0, 6) and holds a capability at 7.
        input.update_at((0, 0), 7, 1);
        input.advance_to(6);
        input.flush();
        worker.step_while(|| probe.less_than(&6));

        // The reduce now sees input frontier 7 before the arrange seals [6, 10).
        input.advance_to(10);
        input.flush();
        worker.step_while(|| probe.less_than(&10));
    });

    let updates: Vec<_> = recv.extract().into_iter().flat_map(|(_, data)| data).collect();
    assert_eq!(updates, vec![((0, 1), 7, 1)]);
}
