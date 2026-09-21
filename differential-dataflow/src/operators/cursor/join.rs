//! Cursor-based join implementation.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::rc::Rc;

use timely::ContainerBuilder;
use timely::dataflow::Stream;
use timely::progress::Timestamp;

use super::history::{load_current, load_current_indexed};
use crate::lattice::Lattice;
use crate::operators::arrange::Arranged;
use crate::operators::history::ValueHistory;
use crate::operators::join::{Fresh, JoinTactic, KEY_WORK_LIMIT, join_with_tactic};
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
            vals1: BatchContainer::with_capacity(0),
            vals2: BatchContainer::with_capacity(0),
            history1: ValueHistory::new(),
            history2: ValueHistory::new(),
            resume: None,
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
///
/// A container boundary can fall in the middle of a key, and a single key's cross product can be
/// arbitrarily larger than the driver's whole budget, so suspending only between keys would let one
/// key buffer unboundedly. Keys are therefore handled by one of two strategies, chosen once the key's
/// edits are loaded and its work is known: a key whose cross product fits [`KEY_WORK_LIMIT`] runs to
/// completion in a single call, over histories of borrowed values; a larger key is reloaded into the
/// iterator's own `vals`/`history` fields, which are indexed rather than borrowed and so survive a
/// suspension, and is then replayed by [`replay_suspending`] a container at a time.
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
    /// The suspending strategy's copy of the open key's values, which its histories index.
    vals1: C1::ValContainer,
    vals2: C2::ValContainer,
    /// The suspending strategy's histories for the open key, over indices into `vals1`/`vals2`.
    history1: ValueHistory<usize, T, C1::Diff>,
    history2: ValueHistory<usize, T, C2::Diff>,
    /// Where in the open key's replay this iterator suspended; `None` when no key is open.
    resume: Option<Resume>,
    done: bool,
}

/// Progress through one key, for the strategy that suspends within a key.
///
/// The variants mirror the two strategies of [`JoinThinker::think`]: the nested cross product it
/// uses for small histories, and the time-ordered wave it uses for large ones. A wave step advances
/// one history's buffer before emitting against it, so the step it is part-way through is part of
/// the state: resuming must not advance that buffer a second time, which would consolidate it and
/// invalidate the offset resumed from.
#[derive(Clone, Copy)]
enum Resume {
    /// The nested cross product, at edit indices `(index1, index2)` of the two histories.
    Simple(usize, usize),
    /// The wave, between steps; the next step follows from the histories' least times.
    WaveChoose,
    /// The wave, emitting `history2`'s buffer against `history1`'s next edit, from this offset.
    WaveStep1(usize),
    /// The wave, emitting `history1`'s buffer against `history2`'s next edit, from this offset.
    WaveStep2(usize),
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
        let vals1 = &mut self.vals1;
        let vals2 = &mut self.vals2;
        let history1 = &mut self.history1;
        let history2 = &mut self.history2;
        let resume = &mut self.resume;
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

                        // A key is opened once and then either retired in this iteration or suspended
                        // part-way; the cursors are not stepped until it retires, so a suspended key is
                        // still the key both cursors report on re-entry.
                        if resume.is_none() {
                            load_current(&mut thinker.history1, cursor1, storage1, meet1);
                            load_current(&mut thinker.history2, cursor2, storage2, meet2);

                            let len1 = thinker.history1.edit_len();
                            let len2 = thinker.history2.edit_len();
                            if len1.saturating_mul(len2) <= KEY_WORK_LIMIT {
                                thinker.think(|v1,v2,t,r1,r2| {
                                    logic(key1, v1, v2, t, r1, r2, builder);
                                });

                                cursor1.step_key(storage1);
                                cursor2.step_key(storage2);

                                thinker.history1.clear();
                                thinker.history2.clear();

                                // Move any completed containers aside; we yield them one at a time.
                                drain_builder(builder, ready);
                                continue;
                            }

                            // The key can outrun the budget, so reload it into histories that survive a
                            // suspension. The strategy is chosen from the same edit counts `think` uses,
                            // so both paths visit a key's matches in the same order.
                            let simple = len1 < 10 || len2 < 10;
                            thinker.history1.clear();
                            thinker.history2.clear();
                            cursor1.rewind_vals(storage1);
                            cursor2.rewind_vals(storage2);
                            load_current_indexed(vals1, history1, cursor1, storage1, meet1);
                            load_current_indexed(vals2, history2, cursor2, storage2, meet2);
                            *resume = Some(if simple {
                                Resume::Simple(0, 0)
                            } else {
                                history1.build();
                                history2.build();
                                Resume::WaveChoose
                            });
                        }

                        let state = resume.as_mut().expect("key is open");
                        let retired = {
                            let (loaded1, loaded2) = (&*vals1, &*vals2);
                            replay_suspending(history1, history2, state, |index1, index2, t, r1, r2| {
                                // `logic` takes its key and its values at one lifetime, and the values
                                // here are borrowed from `vals1`/`vals2` rather than from `storage1`.
                                // Reborrowing the key narrows it to match, instead of holding the
                                // values' borrow open for as long as the storage's.
                                let key1 = <C1::KeyContainer as BatchContainer>::reborrow(key1);
                                logic(key1, loaded1.index(index1), loaded2.index(index2), t, r1, r2, builder);
                                // Move any completed containers aside; we yield them one at a time.
                                drain_builder(builder, ready)
                            })
                        };

                        if retired {
                            cursor1.step_key(storage1);
                            cursor2.step_key(storage2);

                            history1.clear();
                            history2.clear();
                            vals1.clear();
                            vals2.clear();
                            *resume = None;

                            drain_builder(builder, ready);
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

/// Moves every container the builder has completed into `ready`, reporting whether any moved.
fn drain_builder<CB>(builder: &mut CB, ready: &mut VecDeque<CB::Container>) -> bool
where
    CB: ContainerBuilder<Container: Default>,
{
    let mut extracted = false;
    while let Some(container) = builder.extract() {
        // Avoiding the mem::take would require a non-iterator trait.
        ready.push_back(std::mem::take(container));
        extracted = true;
    }
    extracted
}

/// Replays one key's loaded histories, emitting each match, until the key is done or `emit` stops it.
///
/// This produces the same matches in the same order as [`JoinThinker::think`], over histories of value
/// indices rather than of values. It differs in that it can be interrupted: `emit` returns `true` to
/// suspend, `replay_suspending` then returns `false` having recorded in `resume` where it stopped, and
/// a later call resumes from there. It returns `true` once the key is fully replayed, at which point
/// `resume` is spent and the caller may retire the key.
fn replay_suspending<T, D1, D2, F>(
    history1: &mut ValueHistory<usize, T, D1>,
    history2: &mut ValueHistory<usize, T, D2>,
    resume: &mut Resume,
    mut emit: F,
) -> bool
where
    T: Ord + Clone + Lattice,
    D1: Clone + crate::difference::Semigroup,
    D2: Clone + crate::difference::Semigroup,
    F: FnMut(usize, usize, T, &D1, &D2) -> bool,
{
    loop {
        match *resume {
            Resume::Simple(mut index1, mut index2) => {
                let len1 = history1.edit_len();
                let len2 = history2.edit_len();
                while index1 < len1 {
                    while index2 < len2 {
                        let (val1, time1, diff1) = history1.edit_at(index1);
                        let (val2, time2, diff2) = history2.edit_at(index2);
                        let suspend = emit(val1, val2, time1.join(time2), diff1, diff2);
                        index2 += 1;
                        if suspend {
                            *resume = Resume::Simple(index1, index2);
                            return false;
                        }
                    }
                    index2 = 0;
                    index1 += 1;
                }
                return true;
            }
            Resume::WaveChoose => {
                // Step whichever history holds the earlier un-replayed time, and once one is spent,
                // whichever remains. A tie steps the second, as `think`'s comparison does.
                let step1 = match (history1.time(), history2.time()) {
                    (Some(time1), Some(time2)) => time1.cmp(time2) == Ordering::Less,
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (None, None) => return true,
                };
                if step1 {
                    history2.advance_buffer_by(history1.meet().unwrap());
                    *resume = Resume::WaveStep1(0);
                }
                else {
                    history1.advance_buffer_by(history2.meet().unwrap());
                    *resume = Resume::WaveStep2(0);
                }
            }
            Resume::WaveStep1(mut offset) => {
                while offset < history2.buffer().len() {
                    let (val1, time1, diff1) = history1.edit().unwrap();
                    let ((val2, time2), diff2) = &history2.buffer()[offset];
                    let suspend = emit(val1, *val2, time1.join(time2), diff1, diff2);
                    offset += 1;
                    if suspend {
                        *resume = Resume::WaveStep1(offset);
                        return false;
                    }
                }
                history1.step();
                *resume = Resume::WaveChoose;
            }
            Resume::WaveStep2(mut offset) => {
                while offset < history1.buffer().len() {
                    let ((val1, time1), diff1) = &history1.buffer()[offset];
                    let (val2, time2, diff2) = history2.edit().unwrap();
                    let suspend = emit(*val1, val2, time1.join(time2), diff1, diff2);
                    offset += 1;
                    if suspend {
                        *resume = Resume::WaveStep2(offset);
                        return false;
                    }
                }
                history2.step();
                *resume = Resume::WaveChoose;
            }
        }
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
        if self.history1.edit_len() < 10 || self.history2.edit_len() < 10 {
            self.history1.map_edits(|v1, t1, d1| {
                self.history2.map_edits(|v2, t2, d2| {
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

#[cfg(test)]
mod tests {

    use std::cell::Cell;
    use std::rc::Rc;

    use timely::container::CapacityContainerBuilder;
    use timely::container::PushInto;

    use super::{CursorTactic, KEY_WORK_LIMIT};
    use crate::operators::join::{Fresh, JoinTactic};
    use crate::trace::Builder;
    use crate::trace::implementations::ValBuilder;
    use crate::trace::implementations::ord_neu::OrdValBatch;
    use crate::trace::implementations::Vector;

    type Update = ((u64, u64), u64, isize);
    type Batch = Rc<OrdValBatch<Vector<Update>>>;
    type Output = CapacityContainerBuilder<Vec<Update>>;

    /// A batch holding `vals` values under the single key `0`, each at time `0` with diff `1`.
    fn one_key(vals: u64) -> Batch {
        let mut updates: Vec<Update> = (0..vals).map(|val| ((0, val), 0, 1)).collect();
        let mut builder = <ValBuilder<u64, u64, u64, isize>>::default();
        builder.push(&mut updates);
        Rc::new(builder.done().expect("non-empty batch"))
    }

    /// Joins two single-key batches, reporting the first container, the total matches produced by
    /// the time it arrived, and the whole run's matches as `(pair count, pair checksum)`.
    fn join_one_key(vals1: u64, vals2: u64) -> (usize, usize, (usize, u64)) {
        let produced = Rc::new(Cell::new(0));
        let counter = Rc::clone(&produced);
        let mut tactic = CursorTactic::<_, _, _, Output>::new(
            move |_key: &u64, val1: &u64, val2: &u64, time, diff1: &isize, diff2: &isize, out: &mut Output| {
                counter.set(counter.get() + 1);
                out.push_into(((*val1, *val2), time, diff1 * diff2));
            },
        );

        let mut work = tactic.prep(vec![one_key(vals1)], vec![one_key(vals2)], Fresh::Input1, 0);
        let first = work.next().expect("at least one container");
        let (first_len, produced_by_first) = (first.len(), produced.get());

        let mut count = 0;
        let mut checksum = 0;
        for container in std::iter::once(first).chain(work) {
            for ((val1, val2), time, diff) in container {
                assert_eq!((time, diff), (0, 1));
                count += 1;
                checksum += val1 * vals2 + val2;
            }
        }
        (first_len, produced_by_first, (count, checksum))
    }

    /// The matches of a `vals1` by `vals2` cross product of distinct values: one per pair, and each
    /// pair exactly once, which the sum over `val1 * vals2 + val2` pins down.
    fn expected(vals1: u64, vals2: u64) -> (usize, u64) {
        let pairs = vals1 * vals2;
        (pairs as usize, pairs * (pairs - 1) / 2)
    }

    /// A key large enough to outrun the driver's budget suspends within itself, rather than
    /// buffering its whole cross product before the first container is served. Both sides carry
    /// enough edits for the time-ordered wave.
    #[test]
    fn wave_suspends_within_a_key() {
        let vals = (KEY_WORK_LIMIT as f64).sqrt() as u64 + 1;
        assert!((vals * vals) as usize > KEY_WORK_LIMIT);

        let (first_len, produced_by_first, totals) = join_one_key(vals, vals);
        assert!(produced_by_first <= first_len + 1, "produced {produced_by_first} for a container of {first_len}");
        assert_eq!(totals, expected(vals, vals));
    }

    /// The same, for the nested cross product the join uses when one side is small: few edits on a
    /// side does not bound the work, only the other side's edit count does.
    #[test]
    fn nested_suspends_within_a_key() {
        let vals1 = 5;
        let vals2 = KEY_WORK_LIMIT as u64 / vals1 + 1;
        assert!((vals1 * vals2) as usize > KEY_WORK_LIMIT);

        let (first_len, produced_by_first, totals) = join_one_key(vals1, vals2);
        assert!(produced_by_first <= first_len + 1, "produced {produced_by_first} for a container of {first_len}");
        assert_eq!(totals, expected(vals1, vals2));
    }

    /// A batch holding `vals` values under the single key `0`, each present at `times` distinct
    /// times, with diffs that differ per value and time so a dropped or duplicated match shows up.
    fn one_key_over_time(vals: u64, times: u64) -> Batch {
        let mut updates: Vec<Update> = Vec::new();
        for val in 0..vals {
            for time in 0..times {
                updates.push(((0, val), time, 1 + isize::try_from(val + time).unwrap() % 3));
            }
        }
        let mut builder = <ValBuilder<u64, u64, u64, isize>>::default();
        builder.push(&mut updates);
        Rc::new(builder.done().expect("non-empty batch"))
    }

    /// The `(value, time, diff)` edits `one_key_over_time` produces, as the join sees them.
    fn edits_over_time(vals: u64, times: u64) -> Vec<(u64, u64, isize)> {
        (0..vals)
            .flat_map(|val| (0..times).map(move |time| (val, time, 1 + isize::try_from(val + time).unwrap() % 3)))
            .collect()
    }

    /// Accumulates updates into a sorted, consolidated list, dropping those that cancel.
    fn consolidate(mut updates: Vec<Update>) -> Vec<Update> {
        updates.sort();
        let mut out: Vec<Update> = Vec::new();
        for (data, time, diff) in updates {
            match out.last_mut() {
                Some(last) if (last.0, last.1) == (data, time) => last.2 += diff,
                _ => out.push((data, time, diff)),
            }
        }
        out.retain(|update| update.2 != 0);
        out
    }

    /// A key with many distinct times exercises the wave's buffer advance and consolidation, which
    /// a suspension must resume without repeating. The join's output must consolidate to the same
    /// collection as the cross product of the two sides' edits, with times joined and diffs
    /// multiplied.
    #[test]
    fn suspended_wave_matches_the_cross_product() {
        let (vals, times) = (200, 8);
        assert!(((vals * times) * (vals * times)) as usize > KEY_WORK_LIMIT);

        let mut tactic = CursorTactic::<_, _, _, Output>::new(
            |_key: &u64, val1: &u64, val2: &u64, time, diff1: &isize, diff2: &isize, out: &mut Output| {
                out.push_into(((*val1, *val2), time, diff1 * diff2));
            },
        );
        let work = tactic.prep(
            vec![one_key_over_time(vals, times)],
            vec![one_key_over_time(vals, times)],
            Fresh::Input1,
            0,
        );
        let produced: Vec<Update> = work.flatten().collect();

        let mut expected = Vec::new();
        for (val1, time1, diff1) in edits_over_time(vals, times) {
            for (val2, time2, diff2) in edits_over_time(vals, times) {
                expected.push(((val1, val2), time1.max(time2), diff1 * diff2));
            }
        }

        // The collections run to millions of updates, so report the first difference rather than
        // both of them in full.
        let (produced, expected) = (consolidate(produced), consolidate(expected));
        let differs = produced.iter().zip(expected.iter()).position(|(x, y)| x != y);
        assert_eq!(
            differs.map(|at| (at, produced[at], expected[at])),
            None,
            "consolidated join output differs from the cross product",
        );
        assert_eq!(produced.len(), expected.len());
    }

    /// A key whose cross product fits the budget runs to completion in one call, and still produces
    /// exactly its cross product.
    #[test]
    fn small_key_runs_in_one_call() {
        let vals = 64;
        assert!((vals * vals) as usize <= KEY_WORK_LIMIT);

        let (_, _, totals) = join_one_key(vals, vals);
        assert_eq!(totals, expected(vals, vals));
    }
}
