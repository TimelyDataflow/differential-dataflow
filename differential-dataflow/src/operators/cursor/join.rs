//! Cursor-based join implementation.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::rc::Rc;

use timely::ContainerBuilder;
use timely::dataflow::Stream;
use timely::progress::Timestamp;

use crate::lattice::Lattice;
use crate::operators::ValueHistory;
use crate::operators::arrange::Arranged;
use crate::operators::join::{Fresh, JoinTactic, join_with_tactic};
use crate::trace::{BatchCursor, BatchDiff, BatchVal, Cursor, Navigable, TraceReader};
use crate::trace::cursor::cursor_list;
use crate::trace::implementations::containers::BatchContainer;

/// An equijoin of two traces, sharing a common key type.
///
/// This method exists to provide join functionality without opinions on the specific input types, keys and values,
/// that should be presented. The two traces here can have arbitrary key and value types, which can be unsized and
/// even potentially unrelated to the input collection data. Importantly, the key and value types could be generic
/// associated types (GATs) of the traces, and we would seemingly struggle to frame these types as trait arguments.
///
/// The implementation produces a caller-specified container. Implementations can use [`AsCollection`] to wrap the
/// output stream in a collection.
///
/// The "correctness" of this method depends heavily on the behavior of the supplied `result` function.
///
/// [`AsCollection`]: crate::collection::AsCollection
pub fn join_traces<'scope, Tr1, Tr2, KC, L, CB>(arranged1: Arranged<'scope, Tr1>, arranged2: Arranged<'scope, Tr2>, name: &str, result: L) -> Stream<'scope, Tr1::Time, CB::Container>
where
    Tr1: TraceReader<Batch: Navigable>+'static,
    Tr2: TraceReader<Batch: Navigable, Time = Tr1::Time>+'static,
    KC: BatchContainer,
    BatchCursor<Tr1>: Cursor<Time = Tr1::Time, KeyContainer = KC>,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>, Time = Tr1::Time>,
    L: FnMut(KC::ReadItem<'_>,BatchVal<'_, Tr1>,BatchVal<'_, Tr2>,Tr1::Time,&BatchDiff<Tr1>,&BatchDiff<Tr2>,&mut CB)+'static,
    CB: ContainerBuilder<Container: Default> + 'static,
{
    join_with_tactic(arranged1, arranged2, name, CursorTactic::<Tr1::Batch, Tr2::Batch, _, CB>::new(result))
}


/// The conventional cursor-based [`JoinTactic`].
///
/// It builds a [`CursorList`](crate::trace::cursor::CursorList) over each input batch list and plays the merge-join out at whatever rate
/// the driver's fuel allows. Each prepared unit joins a `B0`-side cursor against a `B1`-side cursor,
/// emitting `(val0, val1)` to `logic` and yielding the output containers `logic` fills. `logic` is
/// shared across all outstanding units (an `Rc<RefCell<_>>`), preserving the single mutable-state
/// semantics of one closure threaded through every match — each unit is a self-contained `'static`
/// iterator, so it cannot borrow the tactic.
///
/// It is parameterized by the builder `CB` into which `logic` pushes output; the [`JoinTactic`] it
/// implements is over the container `CB` yields (`CB::Container`).
pub struct CursorTactic<B0, B1, L, CB>
where
    B0: Navigable,
    B1: Navigable,
    B1::Cursor: for<'a> Cursor<Key<'a> = <B0::Cursor as Cursor>::Key<'a>, Time = <B0::Cursor as Cursor>::Time>,
{
    logic: Rc<RefCell<L>>,
    _marker: std::marker::PhantomData<(B0, B1, CB)>,
}

impl<B0, B1, L, CB> CursorTactic<B0, B1, L, CB>
where
    B0: Navigable,
    B1: Navigable,
    B1::Cursor: for<'a> Cursor<Key<'a> = <B0::Cursor as Cursor>::Key<'a>, Time = <B0::Cursor as Cursor>::Time>,
{
    /// Construct a tactic that applies `logic` to each matched `(key, val0, val1)`.
    pub fn new(logic: L) -> Self {
        CursorTactic { logic: Rc::new(RefCell::new(logic)), _marker: std::marker::PhantomData }
    }
}

impl<B0, B1, L, CB> JoinTactic<<B0::Cursor as Cursor>::Time, B0, B1, CB::Container> for CursorTactic<B0, B1, L, CB>
where
    B0: Navigable + 'static,
    B1: Navigable + 'static,
    B1::Cursor: for<'a> Cursor<Key<'a> = <B0::Cursor as Cursor>::Key<'a>, Time = <B0::Cursor as Cursor>::Time>,
    CB: ContainerBuilder<Container: Default> + 'static,
    L: for<'a> FnMut(<B0::Cursor as Cursor>::Key<'a>, <B0::Cursor as Cursor>::Val<'a>, <B1::Cursor as Cursor>::Val<'a>, <B0::Cursor as Cursor>::Time, &<B0::Cursor as Cursor>::Diff, &<B1::Cursor as Cursor>::Diff, &mut CB) + 'static,
{
    fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, fresh: Fresh, meet: <B0::Cursor as Cursor>::Time) -> Box<dyn Iterator<Item = CB::Container>> {
        // The accumulated side's history is advanced by `meet` to consolidate it before the
        // cross-product; the fresh side is left, as its times already lie at or beyond `meet`. `fresh`
        // fixes which side is which. The advance is output-neutral either way (the fresh side's times are
        // at or beyond `meet`, so the joined time is too), so it is purely a consolidation: it pays off
        // when the accumulated side carries times below `meet`, and is a wasted scan when it does not. A
        // more precise rule would skip the scan when the side is already entirely at or beyond `meet`,
        // but detecting that needs both frontiers, not just `lower`: a batch's times lie at or beyond
        // both its `lower` and its `since`, so the side is entirely beyond `meet` exactly when
        // `meet <= lower` or `meet <= since`. A fresh batch is caught by `lower` (its `since` is
        // `minimum`), a compacted trace by `since` (its `lower` is `minimum`); checking `lower` alone
        // would wrongly advance a compacted trace whose times are all already at or beyond `meet`. We
        // keep the simpler fresh-based choice and accept the occasional no-op scan.
        let (cursor1, storage1) = cursor_list(input0);
        let (cursor2, storage2) = cursor_list(input1);
        let (advance1, advance2) = match fresh {
            Fresh::Input0 => (false, true),
            Fresh::Input1 => (true, false),
        };
        Box::new(DeferredIter {
            cursor1,
            storage1,
            cursor2,
            storage2,
            meet,
            advance1,
            advance2,
            logic: Rc::clone(&self.logic),
            builder: CB::default(),
            ready: VecDeque::new(),
            done: false,
        })
    }
}

/// Deferred join computation, as an iterator of output containers.
///
/// The structure wraps cursors which allow us to play out join computation at whatever rate we like.
/// This allows us to avoid producing and buffering massive amounts of data, without giving the timely
/// dataflow system a chance to run operators that can consume and aggregate the data. Each `next` plays
/// the merge-join forward until the builder yields a container (or the cursors run dry), matching the
/// former per-unit `work` loop but suspending at container boundaries rather than under a fuel budget:
/// the driver stops pulling once its budget is spent and resumes the same iterator next activation.
struct DeferredIter<T, C1, C2, L, CB>
where
    T: Timestamp+Lattice,
    C1: Cursor<Time=T>,
    C2: for<'a> Cursor<Key<'a>=C1::Key<'a>, Time=T>,
    CB: ContainerBuilder,
{
    cursor1: C1,
    storage1: C1::Storage,
    cursor2: C2,
    storage2: C2::Storage,
    /// The capability's time, at which this unit's output ships; the lower envelope for consolidation.
    meet: T,
    /// Whether to advance each side's history by `meet` before consolidation.
    advance1: bool,
    advance2: bool,
    /// The output closure, shared across all outstanding units.
    logic: Rc<RefCell<L>>,
    /// The builder `logic` fills; drained into `ready` as containers complete.
    builder: CB,
    /// Completed containers awaiting a `next` call.
    ready: VecDeque<CB::Container>,
    done: bool,
}

impl<T, C1, C2, L, CB> Iterator for DeferredIter<T, C1, C2, L, CB>
where
    T: Timestamp+Lattice,
    C1: Cursor<Time=T>,
    C2: for<'a> Cursor<Key<'a>=C1::Key<'a>, Time=T>,
    CB: ContainerBuilder<Container: Default>,
    L: for<'a> FnMut(C1::Key<'a>, C1::Val<'a>, C2::Val<'a>, T, &C1::Diff, &C2::Diff, &mut CB),
{
    type Item = CB::Container;

    /// Play the merge-join forward until a container is ready, or the cursors run dry.
    #[inline(never)]
    fn next(&mut self) -> Option<CB::Container> {
        // Serve any container completed on an earlier call first.
        if let Some(container) = self.ready.pop_front() { return Some(container); }
        if self.done { return None; }

        // The accumulated side is advanced by `meet` to consolidate its history; the fresh side is left,
        // as its times already lie at or beyond `meet`. The choice was fixed per side at construction,
        // from which input carried the fresh batch.
        let meet1 = if self.advance1 { Some(&self.meet) } else { None };
        let meet2 = if self.advance2 { Some(&self.meet) } else { None };

        let storage1 = &self.storage1;
        let storage2 = &self.storage2;
        let cursor1 = &mut self.cursor1;
        let cursor2 = &mut self.cursor2;
        let builder = &mut self.builder;
        let ready = &mut self.ready;
        let mut logic = self.logic.borrow_mut();
        let logic = &mut *logic;

        let mut thinker = JoinThinker::new();
        let mut exhausted = false;

        while ready.is_empty() {
            match (cursor1.get_key(storage1), cursor2.get_key(storage2)) {
                (Some(key1), Some(key2)) => match key1.cmp(&key2) {
                    Ordering::Less => cursor1.seek_key(storage1, key2),
                    Ordering::Greater => cursor2.seek_key(storage2, key1),
                    Ordering::Equal => {

                        thinker.history1.edits.load(cursor1, storage1, meet1);
                        thinker.history2.edits.load(cursor2, storage2, meet2);

                        thinker.think(|v1,v2,t,r1,r2| {
                            logic(key1, v1, v2, t, r1, r2, builder);
                        });

                        cursor1.step_key(storage1);
                        cursor2.step_key(storage2);

                        thinker.history1.clear();
                        thinker.history2.clear();

                        // Move any completed containers aside; we yield them one at a time.
                        while let Some(container) = builder.extract() {
                            // Avoiding the mem::take would require a non-iterator trait.
                            ready.push_back(std::mem::take(container));
                        }
                    }
                },
                // One side is exhausted; no further keys can match.
                _ => { exhausted = true; break; }
            }
        }

        if exhausted {
            self.done = true;
            // Flush the final partial container.
            while let Some(container) = builder.finish() {
                // Avoiding the mem::take would require a non-iterator trait.
                ready.push_back(std::mem::take(container));
            }
        }

        ready.pop_front()
    }
}

struct JoinThinker<V1, V2, T, D1, D2> {
    pub history1: ValueHistory<V1, T, D1>,
    pub history2: ValueHistory<V2, T, D2>,
}

impl<V1, V2, T, D1, D2> JoinThinker<V1, V2, T, D1, D2>
where
    V1: Copy + Ord,
    V2: Copy + Ord,
    T: Ord + Clone + Lattice,
    D1: Clone + crate::difference::Semigroup,
    D2: Clone + crate::difference::Semigroup,
{
    fn new() -> Self {
        JoinThinker {
            history1: ValueHistory::new(),
            history2: ValueHistory::new(),
        }
    }

    fn think<F: FnMut(V1, V2, T, &D1, &D2)>(&mut self, mut results: F) {

        // for reasonably sized edits, do the dead-simple thing.
        if self.history1.edits.len() < 10 || self.history2.edits.len() < 10 {
            self.history1.edits.map(|v1, t1, d1| {
                self.history2.edits.map(|v2, t2, d2| {
                    results(v1, v2, t1.join(t2), d1, d2);
                })
            })
        }
        else {

            let mut replay1 = self.history1.replay();
            let mut replay2 = self.history2.replay();

            // TODO: It seems like there is probably a good deal of redundant `advance_buffer_by`
            //       in here. If a time is ever repeated, for example, the call will be identical
            //       and accomplish nothing. If only a single record has been added, it may not
            //       be worth the time to collapse (advance, re-sort) the data when a linear scan
            //       is sufficient.

            while !replay1.is_done() && !replay2.is_done() {

                if replay1.time().unwrap().cmp(replay2.time().unwrap()) == ::std::cmp::Ordering::Less {
                    replay2.advance_buffer_by(replay1.meet().unwrap());
                    for &((val2, ref time2), ref diff2) in replay2.buffer().iter() {
                        let (val1, time1, diff1) = replay1.edit().unwrap();
                        results(val1, val2, time1.join(time2), diff1, diff2);
                    }
                    replay1.step();
                }
                else {
                    replay1.advance_buffer_by(replay2.meet().unwrap());
                    for &((val1, ref time1), ref diff1) in replay1.buffer().iter() {
                        let (val2, time2, diff2) = replay2.edit().unwrap();
                        results(val1, val2, time1.join(time2), diff1, diff2);
                    }
                    replay2.step();
                }
            }

            while !replay1.is_done() {
                replay2.advance_buffer_by(replay1.meet().unwrap());
                for &((val2, ref time2), ref diff2) in replay2.buffer().iter() {
                    let (val1, time1, diff1) = replay1.edit().unwrap();
                    results(val1, val2, time1.join(time2), diff1, diff2);
                }
                replay1.step();
            }
            while !replay2.is_done() {
                replay1.advance_buffer_by(replay2.meet().unwrap());
                for &((val1, ref time1), ref diff1) in replay1.buffer().iter() {
                    let (val2, time2, diff2) = replay2.edit().unwrap();
                    results(val1, val2, time1.join(time2), diff1, diff2);
                }
                replay2.step();
            }
        }
    }
}
