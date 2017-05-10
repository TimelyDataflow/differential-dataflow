extern crate timely;
extern crate rand;
extern crate itertools;
extern crate differential_dataflow;
use timely::dataflow::operators::*;
use timely::dataflow::operators::capture::Extract;
use timely::progress::timestamp::RootTimestamp;
use differential_dataflow::collection::AsCollection;
use differential_dataflow::operators::arrange::{ArrangeByKey, Arrange};
use differential_dataflow::operators::group::GroupArranged;
use differential_dataflow::trace::implementations::ord::{OrdValSpine, OrdValBatch};
use differential_dataflow::trace::{Trace, TraceReader, Batch, Batcher, Cursor};
use differential_dataflow::trace::cursor::CursorDebug;
// use differential_dataflow::operators::ValueHistory2;
use differential_dataflow::hashable::{OrdWrapper, UnsignedWrapper, HashOrdered};
use itertools::Itertools;

type IntegerTrace = OrdValSpine<UnsignedWrapper<u64>, u64, usize, i64>;

#[test]
fn test_trace() {
    let mut trace = IntegerTrace::new();
    {
        let mut batcher = <<
            IntegerTrace as TraceReader<UnsignedWrapper<u64>, u64, usize, i64>>::Batch as Batch<
            UnsignedWrapper<u64>, u64, usize, i64>>::Batcher::new();

        batcher.push_batch(&mut vec![
            ((1.into(), 2), 0, 1),
            ((2.into(), 3), 1, 1),
        ]);

        let batch_ts = &[1, 2];
        let mut batches = batch_ts.iter().map(move |i| batcher.seal(&[*i]));
        for b in batches {
            trace.insert(b);
        }
    }

    let vec_1 = trace.cursor_through(&[1]).unwrap().to_vec();
    assert_eq!(vec_1, vec![((1.into(), 2), vec![(0, 1)])]);

    let vec_2 = trace.cursor_through(&[2]).unwrap().to_vec();
    assert_eq!(vec_2, vec![
               ((1.into(), 2), vec![(0, 1)]),
               ((2.into(), 3), vec![(1, 1)]),
    ]);

    let vec_3 = trace.cursor().to_vec();
    assert_eq!(vec_3, vec_2);
}
