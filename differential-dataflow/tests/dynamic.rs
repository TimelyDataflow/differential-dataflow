//! Leaving a dynamic scope on a message that carries two timestamps.
//!
//! Since timely's stamps became multisets, a message can carry several timestamps, and DD's
//! batch-shipping operators make such messages: `arrange` ships a batch under the set of
//! capabilities it retires, `reduce` likewise, and `join` forwards its input batch's set. In an
//! iterative scope with two epochs in flight, an arrange holding `(1, [18])` (epoch 1, round 18)
//! and `(2, [])` (epoch 2, just entered) retires both in one batch when its input frontier passes
//! both at once. `as_collection` then forwards the batch's records under that same set, and the
//! feedback delays it element-wise, so the set reaches whatever reads the *message's* time
//! rather than the records' — which `leave_dynamic` did, to truncate it, and so panicked on
//! "expected a singleton stamp". (Observed on a DDIR program at 20k nodes with four workers;
//! the first multi-element stamp came from an arrange, then a join, then a reduce, then the
//! arrange whose batch reached the scope's exit.)
//!
//! Which retirements coincide depends on scheduling, so this test makes the message directly: an
//! operator that ships its input under a capability for epoch `e` round 3 and one for epoch
//! `e + 1` round 0, into `leave_dynamic`.

use differential_dataflow::collection::AsCollection;
use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::input::Input;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::{builder_rc::OperatorBuilder, OutputBuilder};
use timely::dataflow::operators::CapabilitySet;
use timely::order::Product;

type Time = Product<u64, PointStamp<u64>>;

#[test]
fn leave_dynamic_on_a_message_over_two_epochs() {
    let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = std::sync::Arc::clone(&received);
    timely::execute_directly(move |worker| {
        let mut probe = timely::dataflow::ProbeHandle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, data) = scope.new_collection::<u64, isize>();
            let left = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered = data.enter(inner);
                let mut builder = OperatorBuilder::new("TwoEpochs".to_string(), inner);
                let (output, stream) = builder.new_output();
                let mut output = OutputBuilder::from(output);
                let mut input = builder.new_input(entered.inner, Pipeline);
                builder.build(move |mut caps| {
                    // the initial capability, at (0, []), can be delayed to any later time; it is
                    // dropped once the input is done, so the computation can finish
                    let mut root = caps.pop();
                    move |frontier| {
                        let mut output = output.activate();
                        input.for_each(|cap, data| {
                            let Some(root) = root.as_ref() else { return };
                            let epoch = cap.time().outer;
                            let t1 = Product::new(epoch, PointStamp::new([3].into_iter().collect()));
                            let t2 = Product::new(epoch + 1, PointStamp::new([0].into_iter().collect()));
                            let caps: CapabilitySet<Time> = [root.delayed(&t1), root.delayed(&t2)].into_iter().collect();
                            for (i, (_datum, time, _diff)) in data.iter_mut().enumerate() {
                                *time = if i % 2 == 0 { t1.clone() } else { t2.clone() };
                            }
                            output.session(&caps).give_container(data);
                        });
                        if frontier[0].frontier().is_empty() {
                            root = None;
                        }
                    }
                });
                stream.as_collection().leave_dynamic(1).leave(scope)
            });
            left.inspect(move |(datum, time, diff)| seen.lock().unwrap().push((*datum, *time, *diff))).probe_with(&mut probe);
            input
        });
        for x in 0..8u64 {
            input.insert(x);
        }
        input.close();
        worker.step_while(|| !probe.done());
    });
    let mut got = received.lock().unwrap().clone();
    got.sort();
    // even records at epoch 0 (round 3, truncated away), odd ones at epoch 1 (round 0, likewise)
    let want: Vec<(u64, u64, isize)> = (0..8u64).map(|x| (x, x % 2, 1)).collect();
    assert_eq!(got, want);
}

/// The seam itself: `as_collection` on a message of batches carrying two timestamps hands each
/// record on under the one at or before its time, so the collection's messages carry one each
/// and an operator over records may read `cap.time()`.
#[test]
fn as_collection_routes_records_under_singleton_stamps() {
    use differential_dataflow::batcher::Batcher;
    use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
    use differential_dataflow::trace::implementations::{ValBatcher, ValSpine};
    use differential_dataflow::trace::{Description, Span, SpanOf};
    use timely::dataflow::operators::generic::Operator;
    use timely::progress::Antichain;

    type Tr = ValSpine<u64, u64, Time, isize>;

    let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = std::sync::Arc::clone(&received);
    timely::execute_directly(move |worker| {
        let mut probe = timely::dataflow::ProbeHandle::new();
        let mut input = worker.dataflow::<u64, _, _>(|scope| {
            let (input, data) = scope.new_collection::<u64, isize>();
            let left = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered = data.enter(inner);
                // on its first input, ship one batch holding an update at epoch 0 round 3 and one
                // at epoch 1 round 0, under a capability for each: what an arrange does when it
                // retires both at once.
                let mut builder = OperatorBuilder::new("TwoEpochBatch".to_string(), inner);
                let (output, stream) = builder.new_output();
                let mut output = OutputBuilder::from(output);
                let mut input = builder.new_input(entered.inner, Pipeline);
                builder.build(move |mut caps| {
                    let mut root = caps.pop();
                    move |frontier| {
                        let mut output = output.activate();
                        input.for_each(|_cap, _data| {
                            let Some(root) = root.take() else { return };
                            let t1 = Product::new(0, PointStamp::new([3].into_iter().collect()));
                            let t2 = Product::new(1, PointStamp::new([0].into_iter().collect()));
                            let mut batcher = ValBatcher::<u64, u64, Time, isize>::new(None, 0);
                            let mut updates = vec![((1u64, 10u64), t1.clone(), 1isize), ((2, 20), t2.clone(), 1)];
                            batcher.insert(&mut updates);
                            let upper = Antichain::from_elem(Product::new(2, PointStamp::new(Default::default())));
                            let (batch, _retained) = Batcher::<Vec<((u64, u64), Time, isize)>>::extract(&mut batcher, upper.borrow());
                            let lower = Antichain::from_elem(Product::new(0, PointStamp::new(Default::default())));
                            let description = Description::new(lower.clone(), upper, lower);
                            let mut spans: Vec<SpanOf<Tr>> = vec![Span::new(description, batch.map(Into::into))];
                            let caps: CapabilitySet<Time> = [root.delayed(&t1), root.delayed(&t2)].into_iter().collect();
                            output.session(&caps).give_container(&mut spans);
                        });
                        if frontier[0].frontier().is_empty() {
                            root = None;
                        }
                    }
                });
                let records = Arranged::<TraceAgent<Tr>>::flat_map_batches(stream, |key, val| Some((*key, *val)));
                // every message of records now names one time
                let checked = records.inner.unary(Pipeline, "OneTime", |_, _| {
                    move |input, output| {
                        input.for_each(|time, data| {
                            let _one = time.time();
                            output.session(&time).give_container(data);
                        });
                    }
                });
                checked.as_collection().leave(scope)
            });
            left.inspect(move |(datum, time, diff)| seen.lock().unwrap().push((*datum, *time, *diff))).probe_with(&mut probe);
            input
        });
        input.insert(0);
        input.close();
        worker.step_while(|| !probe.done());
    });
    let mut got = received.lock().unwrap().clone();
    got.sort();
    assert_eq!(got, vec![((1, 10), 0, 1), ((2, 20), 1, 1)]);
}
