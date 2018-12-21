extern crate timely;
extern crate differential_dataflow;

use std::rc::Rc;

use timely::dataflow::operators::generic::OperatorInfo;

use differential_dataflow::hashable::UnsignedWrapper;

use differential_dataflow::trace::implementations::ord::OrdValBatch;
use differential_dataflow::trace::{Trace, TraceReader, Batch, Batcher};
use differential_dataflow::trace::cursor::CursorDebug;
use differential_dataflow::trace::implementations::spine_fueled::Spine;

pub type OrdValSpine<K, V, T, R> = Spine<K, V, T, R, Rc<OrdValBatch<K, V, T, R>>>;

type IntegerTrace = OrdValSpine<UnsignedWrapper<u64>, u64, usize, i64>;

fn get_trace() -> Spine<UnsignedWrapper<u64>, u64, usize, i64, Rc<OrdValBatch<UnsignedWrapper<u64>, u64, usize, i64>>> {
    let op_info = OperatorInfo::new(0, 0, &[]);
    let mut trace = IntegerTrace::new(op_info, None);
    {
        let mut batcher = <<
            IntegerTrace as TraceReader<UnsignedWrapper<u64>, u64, usize, i64>>::Batch as Batch<
            UnsignedWrapper<u64>, u64, usize, i64>>::Batcher::new();

        batcher.push_batch(&mut vec![
            ((1.into(), 2), 0, 1),
            ((2.into(), 3), 1, 1),
            ((2.into(), 3), 2, -1),
        ]);

        let batch_ts = &[1, 2, 3];
        let batches = batch_ts.iter().map(move |i| batcher.seal(&[*i]));
        for b in batches {
            trace.insert(b);
        }
    }
    trace
}

#[test]
fn test_trace() {
    let mut trace = get_trace();

    let (mut cursor1, storage1) = trace.cursor_through(&[1]).unwrap();
    let vec_1 = cursor1.to_vec(&storage1);
    assert_eq!(vec_1, vec![((1.into(), 2), vec![(0, 1)])]);

    let (mut cursor2, storage2) = trace.cursor_through(&[2]).unwrap();
    let vec_2 = cursor2.to_vec(&storage2);
    println!("--> {:?}", vec_2);
    assert_eq!(vec_2, vec![
               ((1.into(), 2), vec![(0, 1)]),
               ((2.into(), 3), vec![(1, 1)]),
    ]);

    let (mut cursor3, storage3) = trace.cursor_through(&[3]).unwrap();
    let vec_3 = cursor3.to_vec(&storage3);
    assert_eq!(vec_3, vec![
               ((1.into(), 2), vec![(0, 1)]),
               ((2.into(), 3), vec![(1, 1), (2, -1)]),
    ]);

    let (mut cursor4, storage4) = trace.cursor();
    let vec_4 = cursor4.to_vec(&storage4);
    assert_eq!(vec_4, vec_3);
}

#[test]
fn test_advance() {
    let mut trace = get_trace();

    trace.advance_by(&[2]);
    trace.distinguish_since(&[2]);

    let (mut cursor1, storage1) = trace.cursor_through(&[2]).unwrap();

    assert_eq!(
        cursor1.to_vec(&storage1),
        vec![((1.into(), 2), vec![(2, 1)]), ((2.into(), 3), vec![(2, 1)])]);

    trace.distinguish_since(&[3]);

    let (mut cursor2, storage2) = trace.cursor_through(&[3]).unwrap();

    assert_eq!(
        cursor2.to_vec(&storage2),
        vec![((1.into(), 2), vec![(2, 1)]), ((2.into(), 3), vec![(2, 1), (2, -1)])]);
}
