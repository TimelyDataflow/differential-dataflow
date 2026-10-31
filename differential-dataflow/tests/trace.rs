use std::sync::{Arc, Mutex};

use timely::container::PushInto;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::progress::{Antichain, frontier::AntichainRef};

use differential_dataflow::trace::implementations::{ValBatcher, ValBuilder, ValSpine};
use differential_dataflow::trace::{Batch, BatchReader, Trace, TraceReader, Batcher, Builder};
use differential_dataflow::trace::cursor::Cursor;

type IntegerTrace = ValSpine<u64, u64, usize, i64>;
type IntegerBuilder = ValBuilder<u64, u64, usize, i64>;

fn get_trace() -> ValSpine<u64, u64, usize, i64> {
    let op_info = OperatorInfo::new(0, 0, [].into());
    let mut trace = IntegerTrace::new(op_info, None, None);
    {
        let mut batcher = ValBatcher::<u64,u64,usize,i64>::new(None, 0);

        batcher.push_into(vec![
            ((1, 2), 0, 1),
            ((2, 3), 1, 1),
            ((2, 3), 2, -1),
        ]);

        let batch_ts = &[1, 2, 3];
        for i in batch_ts {
            let (mut chain, description) = batcher.seal(Antichain::from_elem(*i));
            trace.insert(IntegerBuilder::seal(&mut chain, description));
        }
    }
    trace
}

#[test]
fn test_trace() {
    let mut trace = get_trace();

    let (mut cursor1, storage1) = trace.cursor_through(AntichainRef::new(&[1])).unwrap();
    let vec_1 = cursor1.to_vec(&storage1, |k| k.clone(), |v| v.clone());
    assert_eq!(vec_1, vec![((1, 2), vec![(0, 1)])]);

    let (mut cursor2, storage2) = trace.cursor_through(AntichainRef::new(&[2])).unwrap();
    let vec_2 = cursor2.to_vec(&storage2, |k| k.clone(), |v| v.clone());
    println!("--> {:?}", vec_2);
    assert_eq!(vec_2, vec![
               ((1, 2), vec![(0, 1)]),
               ((2, 3), vec![(1, 1)]),
    ]);

    let (mut cursor3, storage3) = trace.cursor_through(AntichainRef::new(&[3])).unwrap();
    let vec_3 = cursor3.to_vec(&storage3, |k| k.clone(), |v| v.clone());
    assert_eq!(vec_3, vec![
               ((1, 2), vec![(0, 1)]),
               ((2, 3), vec![(1, 1), (2, -1)]),
    ]);

    let (mut cursor4, storage4) = trace.cursor();
    let vec_4 = cursor4.to_vec(&storage4, |k| k.clone(), |v| v.clone());
    assert_eq!(vec_4, vec_3);
}

/// An idle trace that receives one empty batch per tick should settle without an in-progress merge.
#[test]
fn test_idle_trace_settles() {
    let op_info = OperatorInfo::new(0, 0, [].into());
    let mut trace = IntegerTrace::new(op_info, None, None);

    // Record the layers the exertion logic observes, without ever granting effort.
    let observed: Arc<Mutex<Vec<(usize, usize, usize)>>> = Arc::default();
    let observed_in_logic = Arc::clone(&observed);
    trace.set_exert_logic(Arc::new(move |layers| {
        observed_in_logic.lock().unwrap().extend_from_slice(layers);
        None
    }));

    let mut batcher = ValBatcher::<u64,u64,usize,i64>::new(None, 0);
    batcher.push_into(vec![((1, 2), 0, 1)]);
    let (mut chain, description) = batcher.seal(Antichain::from_elem(1));
    trace.insert(IntegerBuilder::seal(&mut chain, description));

    for time in 1 .. 100 {
        trace.set_logical_compaction(AntichainRef::new(&[time]));
        trace.set_physical_compaction(AntichainRef::new(&[time]));
        let lower = Antichain::from_elem(time);
        let upper = Antichain::from_elem(time + 1);
        trace.insert(<IntegerTrace as TraceReader>::Batch::empty(lower, upper));
        trace.set_physical_compaction(AntichainRef::new(&[time + 1]));
        observed.lock().unwrap().clear();
        trace.exert();
        let layers = std::mem::take(&mut *observed.lock().unwrap());
        if time > 10 {
            assert!(
                layers.iter().all(|(_, count, _)| *count < 2),
                "merge in progress at time {time}: {layers:?}",
            );
        }
    }

    let mut lens = Vec::new();
    trace.map_batches(|batch| lens.push(batch.len()));
    assert_eq!(lens.iter().sum::<usize>(), 1);
    let (mut cursor, storage) = trace.cursor();
    let contents = cursor.to_vec(&storage, |k| k.clone(), |v| v.clone());
    assert_eq!(contents, vec![((1, 2), vec![(99, 1)])]);
}
