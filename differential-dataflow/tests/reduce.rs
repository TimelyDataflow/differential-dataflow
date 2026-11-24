use timely::dataflow::operators::{ToStream, Capture, vec::Map};
use timely::dataflow::operators::capture::Extract;
use differential_dataflow::AsCollection;
use differential_dataflow::operators::Iterate;
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

/// A time in a three-dimensional product order. The case below needs three dimensions.
type Time3 = timely::order::Product<u64, timely::order::Product<u64, u64>>;
fn time3(a: u64, b: u64, c: u64) -> Time3 { timely::order::Product::new(a, timely::order::Product::new(b, c)) }

/// Feeds `rounds` to `logic` through an unordered input whose frontier starts at `initial`. Each
/// round gives its updates, advances the input frontier to the round's frontier, and runs the
/// dataflow until the output frontier catches up. Returns every output update.
fn run_rounds<D, F>(initial: Vec<Time3>, rounds: Vec<(Vec<(D, Time3, isize)>, Vec<Time3>)>, logic: F) -> Vec<(D, Time3, isize)>
where
    D: differential_dataflow::Data + Send + Sync,
    F: for<'scope> FnOnce(differential_dataflow::VecCollection<'scope, Time3, D, isize>) -> differential_dataflow::VecCollection<'scope, Time3, D, isize> + Send + Sync + 'static,
{
    use timely::PartialOrder;
    use timely::dataflow::operators::vec::unordered_input::UnorderedInput;

    let (send, recv) = std::sync::mpsc::channel();
    timely::execute_directly(move |worker| {
        let (mut input, root, probe) = worker.dataflow::<u64, _, _>(|outer| {
            outer.scoped::<Time3, _, _>("Product", |scope| {
                let ((input, root), stream) = scope.new_unordered_input::<(D, Time3, isize)>();
                // `inspect` rather than `capture`: the output can carry multi-capability stamps.
                let (probe, _) = logic(stream.as_collection()).inspect(move |update| send.send(update.clone()).unwrap()).probe();
                (input, root, probe)
            })
        });
        let mut caps: Vec<_> = initial.iter().map(|t| root.delayed(t)).collect();
        drop(root);
        for (updates, frontier) in rounds {
            for (data, time, diff) in updates {
                let cap = caps.iter().find(|c| c.time().less_equal(&time)).expect("update behind the frontier");
                input.activate().session(cap).give((data, time, diff));
            }
            caps = frontier.iter().map(|t| caps.iter().find(|c| c.time().less_equal(t)).expect("frontier regresses").delayed(t)).collect();
            worker.step_while(|| probe.with_frontier(|f| f.iter().any(|p| !caps.iter().any(|c| c.time().less_equal(p)))));
        }
    });
    recv.try_iter().collect()
}

/// The accumulation of `updates` at `time`, without zeros.
fn accumulate<D: Ord + Clone>(updates: &[(D, Time3, isize)], time: &Time3) -> std::collections::BTreeMap<D, isize> {
    use timely::PartialOrder;
    let mut acc = std::collections::BTreeMap::new();
    for (data, t, diff) in updates {
        if t.less_equal(time) { *acc.entry(data.clone()).or_insert(0) += diff; }
    }
    acc.retain(|_, diff| *diff != 0);
    acc
}

/// Every time in `[0, bound)^3`.
fn grid(bound: u64) -> impl Iterator<Item = Time3> {
    (0 .. bound).flat_map(move |a| (0 .. bound).flat_map(move |b| (0 .. bound).map(move |c| time3(a, b, c))))
}

/// A history time can need re-evaluation at its join with a new time even though both lie outside
/// the interval the reduce retires. Here `(0,1,0)` arrives first; `(0,0,2)` and `(1,0,1)` follow in
/// `[(0,0,1), (0,1,1))`. The reduce advances the history time by the meet of the new times, to
/// `(0,1,1)`, past the interval. Its join with `(0,0,2)`, at `(0,1,2)`, must still come back as a
/// pending time, where `distinct` retracts the second copy of the record.
#[test]
fn distinct_join_with_history_past_upper() {
    let rounds = vec![
        (vec![(0u64, time3(0, 1, 0), 1)], vec![time3(0, 0, 1)]),
        (vec![(0, time3(0, 0, 2), 1), (0, time3(1, 0, 1), 1)], vec![time3(0, 1, 1)]),
    ];
    let input: Vec<_> = rounds.iter().flat_map(|(updates, _)| updates.clone()).collect();
    let output = run_rounds(vec![time3(0, 0, 0)], rounds, |c| c.distinct());
    for time in grid(4) {
        let expected = accumulate(&input, &time).into_keys().map(|k| (k, 1)).collect();
        assert_eq!(accumulate(&output, &time), expected, "at {time:?}");
    }
}

/// The case above, as a randomized test found it (seed 219): a transitive closure by `iterate` and
/// `distinct`, over edges that arrive at incomparable times in two rounds. The edge `1 -> 6` is
/// derived at `(0,1,2)` (`1 -> 2 -> 6`) and at `(2,1,0)` (`1 -> 2 -> 5 -> 6`), and at their join
/// `(2,1,2)` the closure must still hold it once.
#[test]
fn iterate_distinct_join_with_history_past_upper() {
    let rounds = vec![
        (vec![((5u8, 6u8), time3(1, 0, 0), 1), ((1, 2), time3(0, 1, 0), 1), ((2, 5), time3(2, 0, 0), 1), ((1, 6), time3(3, 0, 0), 1)],
         vec![time3(4, 0, 0), time3(0, 2, 0), time3(0, 0, 1)]),
        (vec![((1, 1), time3(0, 0, 1), 1), ((2, 6), time3(0, 0, 2), 1)],
         vec![time3(4, 0, 0), time3(0, 2, 0), time3(0, 0, 3)]),
    ];
    let input: Vec<_> = rounds.iter().flat_map(|(updates, _)| updates.clone()).collect();
    let initial = vec![time3(1, 0, 0), time3(0, 1, 0), time3(0, 0, 1)];
    let output = run_rounds(initial, rounds, |edges| {
        let links = edges.clone();
        edges.iterate(move |inner, reach| {
            let links = links.enter(inner);
            reach.map(|(a, b)| (b, a)).join_map(links.clone(), |_b, a, c| (*a, *c)).concat(links).distinct()
        })
    });
    for time in grid(4) {
        let mut expected = accumulate(&input, &time);
        loop {
            let paths: Vec<_> = expected.keys().flat_map(|&(a, b)| expected.keys().filter(move |&&(c, _)| c == b).map(move |&(_, d)| (a, d))).collect();
            let size = expected.len();
            expected.extend(paths.into_iter().map(|path| (path, 1)));
            if expected.len() == size { break; }
        }
        let expected = expected.into_keys().map(|path| (path, 1)).collect();
        assert_eq!(accumulate(&output, &time), expected, "at {time:?}");
    }
}

/// Records that each live for one step of the first coordinate, all retired in one round. At step
/// `i`, record `2i` is present at `(i, 0)` and record `2i+1` at `(i, n+1-i)`, and both are retracted
/// at step `i+1`. A later step `(s, 0)` joins `(i, n+1-i)` at `(s, n+1-i)`, where nothing changes,
/// since record `2i+1` has come and gone there. If the reduce remembers those times after the
/// record cancels, step `s` evaluates about `s` of them, and the round takes about `n²/2`
/// evaluations rather than about `3n`. The third coordinate is unused.
#[test]
fn reduce_scaling_cancelled_history() {
    let n: u64 = 100_000;
    let mut updates = Vec::new();
    for i in 0 ..= n {
        updates.push(((0u64, 2 * i), time3(i, 0, 0), 1));
        updates.push(((0u64, 2 * i), time3(i + 1, 0, 0), -1));
        updates.push(((0u64, 2 * i + 1), time3(i, n + 1 - i, 0), 1));
        updates.push(((0u64, 2 * i + 1), time3(i + 1, n + 1 - i, 0), -1));
    }
    let output = run_rounds(vec![time3(0, 0, 0)], vec![(updates, vec![])], |c| {
        c.reduce(|_key, input, output| output.push((input.len() as u64, 1)))
    });
    // At step `i`, one record is present below `(i, n+1-i)`, and two from there on.
    for i in [0, 1, n / 2, n] {
        assert_eq!(accumulate(&output, &time3(i, n - i, 0)), [((0, 1), 1)].into(), "at step {i}");
        assert_eq!(accumulate(&output, &time3(i, n + 1 - i, 0)), [((0, 2), 1)].into(), "at step {i}");
    }
}
