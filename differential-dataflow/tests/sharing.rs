//! Sharing arrangements across threads and runtimes.

use std::sync::mpsc;

use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::{Capture, Probe};
use timely::dataflow::ProbeHandle;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::sharing::SharedTraceHandle;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::{
    ArcValBuilder, ArcValSpine, ValBatcher, ValBuilder, ValSpine,
};

// The shared spine is `Arc`-backed, since `publish` requires `Send + Sync` batches. The reduce
// output below stays on the default `Rc`-backed `ValSpine`/`ValBuilder`, as it is not shared.
type Spine = ArcValSpine<u64, u64, u64, isize>;
type Handle = SharedTraceHandle<Spine>;

/// A worker on one thread publishes an arrangement. A separate thread holds a `Send` handle, waits
/// for the publication frontier to pass a time, snapshots, and reads the collection at that time.
#[test]
fn snapshot_from_another_thread() {
    let (handle_tx, handle_rx) = mpsc::channel::<Handle>();
    // The reader raises this once it has its snapshot, so the publishing worker knows it can stop
    // stepping. A retained trace handle keeps the dataflow from quiescing, so the worker never
    // finishes on its own.
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reader_done = std::sync::Arc::clone(&done);

    // Reader thread: receive the handle, snapshot as of time 2, accumulate.
    let reader = std::thread::spawn(move || {
        let handle = handle_rx.recv().unwrap();
        let snapshot = handle.snapshot_at(&2).expect("publisher closed early");
        reader_done.store(true, std::sync::atomic::Ordering::SeqCst);
        let (mut cursor, storage) = snapshot.cursor();
        // Accumulate `(key, val) -> diff` for all times <= 2.
        let mut acc: Vec<((u64, u64), isize)> = Vec::new();
        while cursor.key_valid(&storage) {
            let key = *cursor.key(&storage);
            while cursor.val_valid(&storage) {
                let val = *cursor.val(&storage);
                let mut sum = 0isize;
                cursor.map_times(&storage, |t, d| {
                    if *t <= 2 {
                        sum += *d;
                    }
                });
                if sum != 0 {
                    acc.push(((key, val), sum));
                }
                cursor.step_val(&storage);
            }
            cursor.step_key(&storage);
        }
        acc.sort();
        acc
    });

    timely::execute_directly(move |worker| {
        let mut input = InputSession::<u64, (u64, u64), isize>::new();
        let published = worker.dataflow(|scope| {
            let arranged = input
                .to_collection(scope)
                .arrange::<ValBatcher<_, _, _, _>, ArcValBuilder<_, _, _, _>, Spine>();
            arranged.publish()
        });
        handle_tx.send(published.handle()).unwrap();

        // Time 0: (1,10)+1, (2,20)+1. Time 1: retract (2,20). Time 2: (3,30)+1.
        input.advance_to(0);
        input.insert((1, 10));
        input.insert((2, 20));
        input.advance_to(1);
        input.remove((2, 20));
        input.advance_to(2);
        input.insert((3, 30));
        input.advance_to(3);
        input.flush();

        // Step until the reader has taken its snapshot. The publisher advances `upper` as it steps,
        // which unblocks the reader's `snapshot_at`.
        while !done.load(std::sync::atomic::Ordering::SeqCst) {
            worker.step();
        }
    });

    let got = reader.join().unwrap();
    // As of time 2: (1,10) present, (2,20) inserted then retracted, (3,30) inserted at 2.
    assert_eq!(got, vec![((1, 10), 1), ((3, 30), 1)]);
}

/// A publisher in one dataflow, an importer in another dataflow of the same worker, connected only
/// through a `SharedTraceHandle`. The importer reduces the shared arrangement and captures the
/// result, which must match a direct reduction of the input.
#[test]
fn import_through_shared_handle() {
    let (capture_tx, capture_rx) = mpsc::channel();

    timely::execute_directly(move |worker| {
        let mut input = InputSession::<u64, (u64, u64), isize>::new();

        // Publisher dataflow.
        let handle = worker.dataflow(|scope| {
            let arranged = input
                .to_collection(scope)
                .arrange::<ValBatcher<_, _, _, _>, ArcValBuilder<_, _, _, _>, Spine>();
            arranged.publish().handle()
        });

        // Importer dataflow: import the shared arrangement, count values per key, capture.
        let mut probe = ProbeHandle::new();
        worker.dataflow(|scope| {
            let imported = handle.import(scope, "Import");
            let counted = imported
                .reduce_abelian::<_, ValBuilder<_, _, _, _>, ValSpine<u64, u64, u64, isize>, _, _>(
                    "Count",
                    |_key, input, output| {
                        let count = input.iter().map(|&(_, d)| d).sum::<isize>() as u64;
                        output.push((count, 1));
                    },
                    |vec, key, upds| {
                        vec.clear();
                        vec.extend(upds.drain(..).map(|(v, t, r)| ((*key, v), t, r)));
                    },
                )
                .as_collection(|k, v| (*k, *v));
            counted
                .inner
                .probe_with(&mut probe)
                .capture_into(capture_tx.clone());
        });

        // Key 1 has two values (10, 20), key 2 has one value (30), all at time 0.
        input.advance_to(0);
        input.insert((1, 10));
        input.insert((1, 20));
        input.insert((2, 30));
        input.advance_to(1);
        input.flush();

        // Step until the imported-and-reduced output has sealed time 0. The worker closure then
        // returns, dropping the dataflows and flushing the capture.
        while probe.less_than(&1) {
            worker.step();
        }
    });

    // Reduce output at time 0: key 1 -> count 2, key 2 -> count 1.
    let mut results: Vec<((u64, u64), u64, isize)> = capture_rx
        .extract()
        .into_iter()
        .flat_map(|(_, data)| data)
        .collect();
    results.sort();
    assert_eq!(results, vec![((1, 2), 0, 1), ((2, 1), 0, 1)]);
}
