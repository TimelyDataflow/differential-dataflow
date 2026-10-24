use timely::dataflow::operators::capture::{Capture, Extract};
use differential_dataflow::VecCollection;
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::ThresholdTotal;

/// Feeds `updates` to `logic`, one time per batch if `per_time` is set, and returns the
/// consolidated output.
fn run<F>(updates: &[(u64, u64, isize)], per_time: bool, logic: F) -> Vec<(u64, u64, isize)>
where
    F: for<'scope> FnOnce(VecCollection<'scope, u64, u64, isize>) -> VecCollection<'scope, u64, u64, isize> + Send + Sync + 'static,
{
    let updates = updates.to_vec();
    let (send, recv) = std::sync::mpsc::channel();
    timely::execute_directly(move |worker| {
        let mut input = InputSession::<u64, u64, isize>::new();
        let probe = worker.dataflow::<u64, _, _>(|scope| {
            let (probe, output) = logic(input.to_collection(scope)).probe();
            output.inner.capture_into(send);
            probe
        });
        for (key, time, diff) in updates {
            input.update_at(key, time, diff);
            if per_time {
                input.advance_to(time + 1);
                input.flush();
                worker.step_while(|| probe.less_than(&(time + 1)));
            }
        }
    });
    let mut output: Vec<_> = recv.extract().into_iter().flat_map(|(_, data)| data).collect();
    differential_dataflow::consolidation::consolidate_updates(&mut output);
    output
}

/// A key whose count returns to zero leaves the output, and returns with its count, whether or
/// not the updates that cancel are still separate in the trace.
#[test]
fn distinct_total_zero_count() {
    let updates = [(0, 1, 1), (0, 2, -1), (0, 3, 1), (0, 4, 1), (0, 5, -2)];
    let expected = vec![(0, 1, 1), (0, 2, -1), (0, 3, 1), (0, 5, -1)];
    for per_time in [false, true] {
        assert_eq!(run(&updates, per_time, |c| c.distinct_total()), expected, "per_time: {per_time}");
    }
}

/// `threshold_total` treats a zero count as absent, as `threshold` does, even when `thresh`
/// maps zero to something else.
#[test]
fn threshold_total_zero_count() {
    let updates = [(0, 1, 1), (0, 2, -1), (0, 3, 2)];
    let expected = vec![(0, 1, 2), (0, 2, -2), (0, 3, 3)];
    for per_time in [false, true] {
        assert_eq!(run(&updates, per_time, |c| c.threshold_total(|_, c| c + 1)), expected, "per_time: {per_time}");
    }
}
