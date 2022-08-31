extern crate timely;
extern crate differential_dataflow;

use std::ops::Mul;

use timely::Data;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::broadcast::Broadcast;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};
use timely::order::TotalOrder;
use differential_dataflow::{Collection, AsCollection};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::input::Input;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arrange, Arranged};
use differential_dataflow::trace::{Cursor, BatchReader, TraceReader};
use differential_dataflow::trace::implementations::ord::OrdKeySpineAbomArc;

// This function is supposed to do one half of a cross join but its implementation is currently
// incorrect
// TODO: actually implement a half cross join
fn half_cross_join<G, Tr1, Key2, R2, Batch2>(
    left: Arranged<G, Tr1>,
    right: &Stream<G, Batch2>,
) -> Collection<G, (Tr1::Key, Key2), <R2 as Mul<Tr1::R>>::Output>
where
    G: Scope,
    G::Timestamp: Lattice + TotalOrder + Ord,
    Tr1: TraceReader<Time = G::Timestamp> + Clone + 'static,
    Tr1::Key: Clone,
    Tr1::R: Clone,
    Batch2: BatchReader<Key2, (), G::Timestamp, R2> + Data,
    Key2: Clone + 'static,
    R2: Semigroup + Clone + Mul<Tr1::R>,
    <R2 as Mul<Tr1::R>>::Output: Semigroup,
{
    let mut trace = left.trace;
    right.unary(Pipeline, "CrossJoin", move |_cap, _info| {
        let mut vector = Vec::new();
        move |input, output| {
            while let Some((time, data)) = input.next() {
                data.swap(&mut vector);
                for batch in vector.drain(..) {
                    let mut cursor = batch.cursor();
                    while let Some(key1) = cursor.get_key(&batch) {
                        let (mut trace_cursor, trace_storage) = trace.cursor();
                        cursor.map_times(&batch, |time1, diff1| {
                            while let Some(key2) = trace_cursor.get_key(&trace_storage) {
                                trace_cursor.map_times(&trace_storage, |time2, diff2| {
                                    let effect_time = std::cmp::max(time1.clone(), time2.clone());
                                    let cap_time = time.delayed(&effect_time);
                                    let diff = diff1.clone().mul(diff2.clone());
                                    let mut session = output.session(&cap_time);
                                    session.give((((key2.clone(), key1.clone())), effect_time, diff));
                                });
                                trace_cursor.step_key(&trace_storage);
                            }
                        });
                        cursor.step_key(&batch);
                    }
                }
            }
        }
    })
    .as_collection()
}

fn main() {
    timely::execute_from_args(::std::env::args(), move |worker| {
        let worker_idx = worker.index();
        let (mut handle1, mut handle2, probe) = worker.dataflow::<u64, _, _>(|scope| {
            let (handle1, input1) = scope.new_collection();
            let (handle2, input2) = scope.new_collection();

            let arranged1 = input1.arrange::<OrdKeySpineAbomArc<_, _, _>>();
            let arranged2 = input2.arrange::<OrdKeySpineAbomArc<_, _, _>>();

            let batches1 = arranged1.stream.broadcast();
            let batches2 = arranged2.stream.broadcast();

            // Changes from input1 need to be joined with the per-worker arrangement state of input2
            let cross1 = half_cross_join(arranged2, &batches1);

            // Changes from input2 need to be joined with the per-worker arrangement state of input1
            let cross2 = half_cross_join(arranged1, &batches2);

            // The final cross join is the combination of these two
            let cross_join = cross1.map(|(key1, key2)| (key2, key1)).concat(&cross2);

            let probe = cross_join
                .inspect(move |d| {
                    println!("worker {} produced: {:?}", worker_idx, d);
                })
                .probe();

            (handle1, handle2, probe)
        });

        handle1.insert(1i64);
        handle1.advance_to(1);
        handle1.insert(2);
        handle1.advance_to(2);
        handle1.flush();

        handle2.insert("apple".to_string());
        handle2.advance_to(1);
        handle2.insert("orange".to_string());
        handle2.advance_to(2);
        handle2.flush();

        while probe.less_than(handle1.time()) {
            worker.step();
        }
    }).unwrap();
}
