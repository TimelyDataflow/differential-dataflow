use timely::dataflow::operators::capture::{Capture, Extract};
use differential_dataflow::input::InputSession;
use differential_dataflow::operators::ThresholdTotal;

/// A key whose count returns to zero leaves the output, and can come back, whether its
/// cancelling updates arrive in one batch or in separate ones.
#[test]
fn distinct_total_zero_count() {
    let updates = [(0u64, 1u64, 1isize), (0, 2, -1), (0, 3, 1), (0, 4, 1), (0, 5, -2)];
    for per_time in [false, true] {
        let (send, recv) = std::sync::mpsc::channel();
        timely::execute_directly(move |worker| {
            let mut input = InputSession::<u64, u64, isize>::new();
            let probe = worker.dataflow::<u64, _, _>(|scope| {
                let (probe, output) = input.to_collection(scope).distinct_total().probe();
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
        assert_eq!(output, vec![(0, 1, 1), (0, 2, -1), (0, 3, 1), (0, 5, -1)], "per_time: {per_time}");
    }
}
