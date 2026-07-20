//! Sharing arrangements across threads and runtimes.

use std::sync::{mpsc, Mutex};

use timely::dataflow::operators::capture::Extract;
use timely::dataflow::operators::{Capture, Probe};
use timely::dataflow::ProbeHandle;

use timely::progress::Antichain;

use differential_dataflow::input::InputSession;
use differential_dataflow::operators::arrange::sharing::SharedTraceHandle;
use differential_dataflow::operators::arrange::Arrange;
use differential_dataflow::trace::cursor::Cursor;
use differential_dataflow::trace::implementations::{
    ArcValBuilder, ArcValSpine, ValBatcher, ValBuilder, ValSpine,
};
use differential_dataflow::trace::TraceReader;

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

/// Feeds `input` a fresh update at `time` and steps the worker, so the publisher operator
/// reactivates and republishes its `since` from the trace. The publisher only runs on stream
/// input, so a bare compaction advance on another handle is invisible until the next batch.
fn tick(
    worker: &mut timely::worker::Worker,
    input: &mut InputSession<u64, (u64, u64), isize>,
    time: u64,
) {
    input.advance_to(time);
    input.insert((time, time));
    input.advance_to(time + 1);
    input.flush();
    for _ in 0..20 {
        worker.step();
    }
}

/// Publishing must not pin compaction. With no registered reader holds, as the trace's writer
/// advances logical (and physical) compaction the publisher's own hold must follow, so the trace
/// actually compacts. We keep a writer handle on the original trace, advance its compaction to
/// `10`, tick the publisher, and assert the published `since` advanced: a fresh snapshot rejects a
/// read at `5` (a compacted time) while still serving `10`. Before the fix the publisher pins the
/// trace at the publish-time frontier, so `since` stays at `0` and the read at `5` is served.
#[test]
fn publish_without_readers_does_not_pin_compaction() {
    timely::execute_directly(move |worker| {
        let mut input = InputSession::<u64, (u64, u64), isize>::new();
        // Keep a writer handle (a plain `TraceAgent` clone) alongside the publication. This is the
        // only hold besides the publisher's own agent. Crucially, no `SharedTraceHandle` is minted,
        // so `state.logical_holds` stays empty: zero registered reader holds.
        let (mut writer, published) = worker.dataflow(|scope| {
            let arranged = input
                .to_collection(scope)
                .arrange::<ValBatcher<_, _, _, _>, ArcValBuilder<_, _, _, _>, Spine>();
            (arranged.trace.clone(), arranged.publish())
        });

        // Seed some updates and let the publisher settle.
        for t in 0..5 {
            tick(worker, &mut input, t);
        }

        // The writer advances its logical and physical compaction to 10, as a controller would,
        // then a fresh batch reactivates the publisher so it republishes `since`.
        writer.set_logical_compaction(Antichain::from_elem(10).borrow());
        writer.set_physical_compaction(Antichain::from_elem(10).borrow());
        tick(worker, &mut input, 10);

        // The published `since` followed the writer to 10: a fresh handle cannot snapshot at the
        // compacted time 5, but can at 10.
        let handle = published.handle();
        assert!(
            handle.snapshot_at(&5).is_none(),
            "snapshot at a compacted time must be rejected (since did not advance)"
        );
        assert!(
            handle.snapshot_at(&10).is_some(),
            "snapshot at the compaction frontier must succeed"
        );
    });
}

/// A live reader hold holds the trace back at its own frontier, and releasing it (drop) lets the
/// trace follow the writer. A reader registered at `since` 0 keeps time 0 readable even as the
/// writer advances to 10: the publisher forwards the reader's hold, so the trace stays at 0 and the
/// published `since` stays at 0. After the reader drops, a further tick lets the publisher follow
/// the writer, and a read at the now-compacted time 5 is rejected. This guards the fix on the "with
/// holds" side. Before the fix the publisher pins the trace regardless, so the post-drop compaction
/// never happens.
#[test]
fn import_hold_pins_then_releases() {
    timely::execute_directly(move |worker| {
        let mut input = InputSession::<u64, (u64, u64), isize>::new();
        let (mut writer, published) = worker.dataflow(|scope| {
            let arranged = input
                .to_collection(scope)
                .arrange::<ValBatcher<_, _, _, _>, ArcValBuilder<_, _, _, _>, Spine>();
            (arranged.trace.clone(), arranged.publish())
        });

        // A live reader hold registered at the publish-time `since` (0).
        let reader = published.handle();

        for t in 0..5 {
            tick(worker, &mut input, t);
        }

        // The writer advances to 10, but the live reader hold (0) holds the trace back: the
        // publisher forwards the reader's hold, so the published `since` stays at 0 and a read at 0
        // still succeeds.
        writer.set_logical_compaction(Antichain::from_elem(10).borrow());
        writer.set_physical_compaction(Antichain::from_elem(10).borrow());
        tick(worker, &mut input, 10);
        assert!(
            reader.snapshot_at(&0).is_some(),
            "a live reader hold must keep its frontier readable"
        );

        // Drop the reader. Now only the publisher holds, and it follows the writer, so the trace
        // compacts to 10. A handle minted afterward (so it does not itself hold the trace at 0)
        // observes the advanced `since`: a read at the compacted time 5 is rejected. Before the fix
        // the publisher pins the trace even with no readers, so `since` stays at 0 and this read
        // succeeds.
        drop(reader);
        tick(worker, &mut input, 11);
        let observer = published.handle();
        assert!(
            observer.snapshot_at(&5).is_none(),
            "after the reader drops, the trace must compact to the writer frontier"
        );
    });
}

/// A publisher on a two-worker runtime (`peers() == 2`) hands its handle to an importer on a
/// single-threaded runtime (`peers() == 1`). Pairwise import assumes both sides shard keys the
/// same way, which requires equal total peers, so `import` must assert and panic rather than
/// silently reading the wrong shard.
#[test]
#[should_panic(expected = "peers")]
fn import_asserts_equal_peers() {
    let (handle_tx, handle_rx) = mpsc::channel::<Handle>();
    let handle_tx = Mutex::new(handle_tx);

    // Publisher runtime: two worker threads, so the publishing scope's `peers()` is 2.
    timely::execute(timely::Config::process(2), move |worker| {
        let mut input = InputSession::<u64, (u64, u64), isize>::new();
        let published = worker.dataflow(|scope| {
            let arranged = input
                .to_collection(scope)
                .arrange::<ValBatcher<_, _, _, _>, ArcValBuilder<_, _, _, _>, Spine>();
            arranged.publish()
        });
        // Only one worker needs to hand out a handle; the others publish redundantly (mirroring
        // real SPMD dataflows) but nobody reads their handles.
        if worker.index() == 0 {
            handle_tx.lock().unwrap().send(published.handle()).unwrap();
        }
    })
    .expect("publisher runtime failed to start");

    let handle = handle_rx.recv().expect("publisher did not send a handle");

    // Importer runtime: single-threaded (`execute_directly` never spawns worker threads), so
    // `peers()` is 1, mismatching the publisher's 2. `import` runs on this same thread, so its
    // panic unwinds directly into the test rather than being swallowed at a thread boundary.
    timely::execute_directly(move |worker| {
        worker.dataflow::<u64, _, _>(|scope| {
            let _imported = handle.import(scope, "Import");
        });
    });
}
