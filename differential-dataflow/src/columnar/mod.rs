//! Columnar container infrastructure for differential dataflow.
//!
//! **Experimental.** API and internals are still settling. Expect breaking
//! changes; do not rely on stability across releases.
//!
//! Known rough edges:
//! - `ContainerBytes` for `RecordedUpdates` and `Updates` is `unimplemented!()`;
//!   multi-process dataflows that exchange these containers will panic.
//! - `leave_dynamic` consolidates eagerly on each batch; the
//!   [`crate::dynamic`] counterpart defers consolidation. Same observable
//!   semantics, different work distribution.
//! - `join_function` is restricted to same-`ColumnarUpdate` input and output;
//!   it does not yet generalize to `Key`/`Val`/`Diff`-changing maps.
//! - Several public items (`join_function`, `leave_dynamic`, `DynTime`) have
//!   no in-tree callers yet and are not exercised by tests.
//!
//! Files inside this module that touch both the local module path and the
//! [`columnar`](https://docs.rs/columnar) crate should `use columnar as col;`
//! to disambiguate.
//!
//! Module layout (bottom-up):
//! - [`layout`] — `ColumnarUpdate` / `ColumnarLayout` / `OrdContainer`.
//! - [`updates`] — `Updates<U>` trie, `Consolidating`, `UpdatesBuilder`.
//! - [`builder`] — `ValColBuilder`: the input-side `ContainerBuilder`.
//! - [`exchange`] — `ValPact` / `ValDistributor`: PACT for shuffling.
//! - [`arrangement`] — type aliases + `Coltainer` + `TrieChunker` +
//!   `trie_merger` + `ValMirror` (trace Builder).
//! - This file — `RecordedUpdates<U>` (the stream container), container-trait
//!   impls (`Negate`, `Enter`, `Leave`, `ResultsIn`), and top-level operators
//!   (`join_function`, `leave_dynamic`, `as_recorded_updates`).


pub mod layout;
pub mod updates;
pub mod builder;
pub mod exchange;
pub mod arrangement;

pub use updates::Updates;
pub use builder::ValBuilder as ValColBuilder;
pub use exchange::ValPact;
pub use arrangement::{ValBatcher, ValBuilder, ValSpine};

/// A thin wrapper around `Updates` that tracks the pre-consolidation record count
/// for timely's exchange accounting. This wrapper is the stream container type;
/// the `TrieChunker` strips it, passing bare `Updates` into the merge batcher.
pub struct RecordedUpdates<U: layout::ColumnarUpdate> {
    /// The trie of `(key, val, time, diff)` updates.
    pub updates: Updates<U>,
    /// Number of records in `updates` before consolidation.
    pub records: usize,
    /// Whether `updates` is known to be sorted and consolidated
    /// (no duplicate (key, val, time) triples, no zero diffs).
    pub consolidated: bool,
}

impl<U: layout::ColumnarUpdate> Default for RecordedUpdates<U> {
    fn default() -> Self { Self { updates: Default::default(), records: 0, consolidated: true } }
}

impl<U: layout::ColumnarUpdate> Clone for RecordedUpdates<U> {
    fn clone(&self) -> Self { Self { updates: self.updates.clone(), records: self.records, consolidated: self.consolidated } }
}

impl<U: layout::ColumnarUpdate> timely::Accountable for RecordedUpdates<U> {
    #[inline] fn record_count(&self) -> i64 { self.records as i64 }
}

impl<U: layout::ColumnarUpdate> timely::dataflow::channels::ContainerBytes for RecordedUpdates<U> {
    fn from_bytes(_bytes: timely::bytes::arc::Bytes) -> Self { unimplemented!() }
    fn length_in_bytes(&self) -> usize { unimplemented!() }
    fn into_bytes<W: std::io::Write>(&self, _writer: &mut W) { unimplemented!() }
}

// Container trait impls for RecordedUpdates, enabling iterative scopes.
mod container_impls {
    use columnar::{Borrow, Columnar, Index, Len, Push};
    use timely::progress::{Timestamp, timestamp::Refines};
    use crate::difference::Abelian;
    use crate::collection::containers::{Negate, Enter, Leave, ResultsIn};

    use super::layout::ColumnarUpdate as Update;
    use super::updates::Updates;
    use super::RecordedUpdates;

    impl<U: Update<Diff: Abelian>> Negate for RecordedUpdates<U> {
        fn negate(mut self) -> Self {
            let len = self.updates.diffs.values.len();
            let mut new_diffs = <<U::Diff as Columnar>::Container as Default>::default();
            let mut owned = U::Diff::default();
            for i in 0..len {
                columnar::Columnar::copy_from(&mut owned, self.updates.diffs.values.borrow().get(i));
                owned.negate();
                new_diffs.push(&owned);
            }
            self.updates.diffs.values = new_diffs;
            self
        }
    }

    impl<K, V, T1, T2, R> Enter<T1, T2> for RecordedUpdates<(K, V, T1, R)>
    where
        (K, V, T1, R): Update<Key=K, Val=V, Time=T1, Diff=R>,
        (K, V, T2, R): Update<Key=K, Val=V, Time=T2, Diff=R>,
        T1: Timestamp + Columnar + Default + Clone,
        T2: Refines<T1> + Columnar + Default + Clone,
        K: Columnar, V: Columnar, R: Columnar,
    {
        type InnerContainer = RecordedUpdates<(K, V, T2, R)>;
        fn enter(self) -> Self::InnerContainer {
            // Rebuild the time column; everything else moves as-is.
            let mut new_times = <<T2 as Columnar>::Container as Default>::default();
            let mut t1_owned = T1::default();
            for i in 0..self.updates.times.values.len() {
                Columnar::copy_from(&mut t1_owned, self.updates.times.values.borrow().get(i));
                let t2 = T2::to_inner(t1_owned.clone());
                new_times.push(&t2);
            }
            // TODO: Assumes Enter (to_inner) is order-preserving on times.
            RecordedUpdates {
                consolidated: self.consolidated,
                updates: Updates {
                    keys: self.updates.keys,
                    vals: self.updates.vals,
                    times: super::updates::Lists { values: new_times, bounds: self.updates.times.bounds },
                    diffs: self.updates.diffs,
                },
                records: self.records,
            }
        }
    }

    impl<K, V, T1, T2, R> Leave<T1, T2> for RecordedUpdates<(K, V, T1, R)>
    where
        (K, V, T1, R): Update<Key=K, Val=V, Time=T1, Diff=R>,
        (K, V, T2, R): Update<Key=K, Val=V, Time=T2, Diff=R>,
        T1: Refines<T2> + Columnar + Default + Clone,
        T2: Timestamp + Columnar + Default + Clone,
        K: Columnar, V: Columnar, R: Columnar,
    {
        type OuterContainer = RecordedUpdates<(K, V, T2, R)>;
        fn leave(self) -> Self::OuterContainer {
            // Flatten, convert times, and reconsolidate via consolidate.
            // Leave can collapse distinct T1 times to the same T2 time,
            // so the trie must be rebuilt with consolidation.
            let mut flat = Updates::<(K, V, T2, R)>::default();
            let mut t1_owned = T1::default();
            for (k, v, t, d) in self.updates.iter() {
                Columnar::copy_from(&mut t1_owned, t);
                let t2: T2 = t1_owned.clone().to_outer();
                flat.push((k, v, &t2, d));
            }
            RecordedUpdates {
                updates: flat.consolidate(),
                records: self.records,
                consolidated: true,
            }
        }
    }

    impl<U: Update> ResultsIn<<U::Time as Timestamp>::Summary> for RecordedUpdates<U> {
        fn results_in(self, step: &<U::Time as Timestamp>::Summary) -> Self {
            use timely::progress::PathSummary;
            // Apply results_in to each time; drop updates whose time maps to None.
            // This must rebuild the trie since some entries may be removed.
            let mut output = Updates::<U>::default();
            let mut time_owned = U::Time::default();
            for (k, v, t, d) in self.updates.iter() {
                Columnar::copy_from(&mut time_owned, t);
                if let Some(new_time) = step.results_in(&time_owned) {
                    output.push((k, v, &new_time, d));
                }
            }
            // TODO: Time advancement may not be order preserving, but .. it could be.
            // TODO: Before this is consolidated the above would need to be `form`ed.
            RecordedUpdates { updates: output, records: self.records, consolidated: false }
        }
    }
}

/// A columnar flat_map: iterates RecordedUpdates, calls logic per (key, val, time, diff),
/// joins output times with input times, multiplies output diffs with input diffs.
///
/// This subsumes map, filter, negate, and enter_at for columnar collections.
pub fn join_function<U, I, L>(
    input: crate::Collection<U::Time, RecordedUpdates<U>>,
    mut logic: L,
) -> crate::Collection<U::Time, RecordedUpdates<U>>
where
    U::Time: crate::lattice::Lattice,
    U: layout::ColumnarUpdate<Diff: crate::difference::Multiply<U::Diff, Output = U::Diff>>,
    I: IntoIterator<Item = (U::Key, U::Val, U::Time, U::Diff)>,
    L: FnMut(
        columnar::Ref<'_, U::Key>,
        columnar::Ref<'_, U::Val>,
        columnar::Ref<'_, U::Time>,
        columnar::Ref<'_, U::Diff>,
    ) -> I + 'static,
{
    use timely::dataflow::operators::generic::Operator;
    use timely::dataflow::channels::pact::Pipeline;
    use crate::AsCollection;
    use crate::difference::Multiply;
    use crate::lattice::Lattice;
    use columnar::Columnar;

    input
        .inner
        .unary::<ValColBuilder<U>, _, _, _>(Pipeline, "JoinFunction", move |_, _| {
            move |input, output| {
                let mut t1o = U::Time::default();
                let mut d1o = U::Diff::default();
                input.for_each(|time, data| {
                    let mut session = output.session_with_builder(&time);
                    for (k1, v1, t1, d1) in data.updates.iter() {
                        Columnar::copy_from(&mut t1o, t1);
                        Columnar::copy_from(&mut d1o, d1);
                        for (k2, v2, t2, d2) in logic(k1, v1, t1, d1) {
                            let t3 = t2.join(&t1o);
                            let d3 = d2.multiply(&d1o);
                            session.give((&k2, &v2, &t3, &d3));
                        }
                    }
                });
            }
        })
        .as_collection()
}

/// Timestamp shape of a dynamic iterative scope: an outer timestamp paired
/// with a per-level `PointStamp` of loop counters.
pub type DynTime<TOuter, T> = timely::order::Product<TOuter, crate::dynamic::pointstamp::PointStamp<T>>;

/// Leave a dynamic iterative scope, truncating PointStamp coordinates.
///
/// Uses OperatorBuilder (not unary) for the custom input connection summary
/// that tells timely how the PointStamp is affected (retain `level - 1` coordinates).
///
/// Consolidates after truncation since distinct PointStamp coordinates can collapse.
pub fn leave_dynamic<K, V, R, TOuter, T>(
    input: crate::Collection<DynTime<TOuter, T>, RecordedUpdates<(K, V, DynTime<TOuter, T>, R)>>,
    level: usize,
) -> crate::Collection<DynTime<TOuter, T>, RecordedUpdates<(K, V, DynTime<TOuter, T>, R)>>
where
    K: columnar::Columnar,
    V: columnar::Columnar,
    R: columnar::Columnar,
    TOuter: timely::progress::Timestamp + Default + columnar::Columnar,
    T: timely::progress::Timestamp + Default + columnar::Columnar,
    (K, V, DynTime<TOuter, T>, R): layout::ColumnarUpdate<Key = K, Val = V, Time = DynTime<TOuter, T>, Diff = R>,
{
    assert!(level > 0, "leave_dynamic requires level > 0");
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
    use timely::dataflow::operators::generic::OutputBuilder;
    use timely::order::Product;
    use timely::progress::Antichain;
    use timely::container::{ContainerBuilder, PushInto};
    use crate::AsCollection;
    use crate::dynamic::pointstamp::{PointStamp, PointStampSummary};
    use columnar::Columnar;

    let mut builder = OperatorBuilder::new("LeaveDynamic".to_string(), input.inner.scope());
    let (output, stream) = builder.new_output();
    let mut output = OutputBuilder::from(output);
    let mut op_input = builder.new_input_connection(
        input.inner,
        Pipeline,
        [(
            0,
            Antichain::from_elem(Product {
                outer: Default::default(),
                inner: PointStampSummary {
                    retain: Some(level - 1),
                    actions: Vec::new(),
                },
            }),
        )],
    );

    builder.build(move |_capability| {
        let mut col_builder = ValColBuilder::<(K, V, DynTime<TOuter, T>, R)>::default();
        let mut time = DynTime::<TOuter, T>::default();
        move |_frontier| {
            let mut output = output.activate();
            op_input.for_each(|cap, data| {
                // Truncate the capability's timestamp.
                let mut new_time = cap.time().clone();
                let mut vec = std::mem::take(&mut new_time.inner).into_inner();
                vec.truncate(level - 1);
                new_time.inner = PointStamp::new(vec);
                let new_cap = cap.delayed(&new_time, 0);
                // Push updates with truncated times into the builder.
                // The builder's form call on flush sorts and consolidates,
                // handling the duplicate times that truncation can produce.
                // TODO: The input trie is already sorted; a streaming form
                // that accepts pre-sorted, potentially-collapsing timestamps
                // could avoid the re-sort inside the builder.
                for (k, v, t, d) in data.updates.iter() {
                    Columnar::copy_from(&mut time, t);
                    let mut inner_vec = std::mem::take(&mut time.inner).into_inner();
                    inner_vec.truncate(level - 1);
                    time.inner = PointStamp::new(inner_vec);
                    col_builder.push_into((k, v, &time, d));
                }
                let mut session = output.session(&new_cap);
                while let Some(container) = col_builder.finish() {
                    session.give_container(container);
                }
            });
        }
    });

    stream.as_collection()
}

/// Extract a `Collection<_, RecordedUpdates<U>>` from a columnar `Arranged`.
///
/// Cursors through each batch and pushes `(key, val, time, diff)` refs into
/// a `ValColBuilder`, which sorts and consolidates on flush.
pub fn as_recorded_updates<U>(
    arranged: crate::operators::arrange::Arranged<
        crate::operators::arrange::TraceAgent<ValSpine<U::Key, U::Val, U::Time, U::Diff>>,
    >,
) -> crate::Collection<U::Time, RecordedUpdates<U>>
where
    U: layout::ColumnarUpdate,
{
    use timely::dataflow::operators::generic::Operator;
    use timely::dataflow::channels::pact::Pipeline;
    use crate::trace::{BatchReader, Cursor};
    use crate::AsCollection;

    arranged.stream
        .unary::<ValColBuilder<U>, _, _, _>(Pipeline, "AsRecordedUpdates", |_, _| {
            move |input, output| {
                input.for_each(|time, batches| {
                    let mut session = output.session_with_builder(&time);
                    for batch in batches.drain(..) {
                        let mut cursor = batch.cursor();
                        while cursor.key_valid(&batch) {
                            while cursor.val_valid(&batch) {
                                let key = cursor.key(&batch);
                                let val = cursor.val(&batch);
                                cursor.map_times(&batch, |time, diff| {
                                    session.give((key, val, time, diff));
                                });
                                cursor.step_val(&batch);
                            }
                            cursor.step_key(&batch);
                        }
                    }
                });
            }
        })
        .as_collection()
}
