//! Columnar rendering substrate.
//!
//! Rows are a stride-encoded `Row(Vec<i64>)`; the differential container is
//! columnar `RecordedUpdates`. Supplies the substrate leaf operators (join,
//! reduce, arrange over columnar builders/batchers/spines); the scope-tree walk
//! lives in [`crate::backend::render_tree`].

mod types {
    /// A row type backed by Vec but using Strides for columnar bounds.
    /// This ensures uniform-length rows (common in the IR) get compact
    /// stride-based offset encoding rather than per-element u64 bounds.
    #[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
    pub struct Row(pub Vec<i64>);

    impl Row {
        pub fn new() -> Self { Row(Vec::new()) }
        pub fn push(&mut self, v: i64) { self.0.push(v); }
    }

    impl std::ops::Deref for Row {
        type Target = [i64];
        fn deref(&self) -> &[i64] { &self.0 }
    }

    impl std::iter::FromIterator<i64> for Row {
        fn from_iter<I: IntoIterator<Item = i64>>(iter: I) -> Self {
            Row(iter.into_iter().collect())
        }
    }

    impl<'a> IntoIterator for &'a Row {
        type Item = &'a i64;
        type IntoIter = std::slice::Iter<'a, i64>;
        fn into_iter(self) -> Self::IntoIter { self.0.iter() }
    }

    impl IntoIterator for Row {
        type Item = i64;
        type IntoIter = std::vec::IntoIter<i64>;
        fn into_iter(self) -> Self::IntoIter { self.0.into_iter() }
    }

    impl columnar::Columnar for Row {
        type Container = columnar::Vecs<Vec<i64>, columnar::primitive::offsets::Strides>;

        fn into_owned<'a>(other: columnar::Ref<'a, Self>) -> Self {
            Row(other.into_iter().copied().collect())
        }

        fn copy_from<'a>(&mut self, other: columnar::Ref<'a, Self>) {
            self.0.clear();
            self.0.extend(other.into_iter().copied());
        }
    }

    impl crate::ir::RowLike for Row {
        fn new() -> Self { Row::new() }
        fn push(&mut self, v: i64) { Row::push(self, v); }
        fn as_slice(&self) -> &[i64] { &self.0 }
        fn extend_from_slice(&mut self, other: &[i64]) { self.0.extend_from_slice(other); }
    }

    pub type Diff = i64;
    pub type Time = timely::order::Product<u64, differential_dataflow::dynamic::pointstamp::PointStamp<u64>>;
}

use differential_dataflow::columnar as columnar_support;

mod columnar {
    use super::types::*;

    pub use super::columnar_support::*;
    pub use super::columnar_support::{ValSpine, ValBatcher, ValBuilder};

    pub type DdirUpdate = (Row, Row, Time, Diff);
    pub type DdirRecordedUpdates = RecordedUpdates<DdirUpdate>;

    pub type ColValSpine<K, V, T, R> = ValSpine<K, V, T, R>;
    pub type ColValBatcher<K, V, T, R> = ValBatcher<K, V, T, R>;
    pub type ColValBuilder<K, V, T, R> = ValBuilder<K, V, T, R>;
    pub type ColValChunker<U> = ValChunker<U>;
}

mod render {
    use std::sync::Arc;
    use timely::order::Product;
    use differential_dataflow::Collection;
    use differential_dataflow::dynamic::pointstamp::PointStamp;
    use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
    use columnar::Columnar;
    use super::types::*;
    use crate::ir::{LinearOp, RowLike, eval_fields, eval_field_into, eval_condition};
    use crate::parse::{Projection, Reducer};
    use crate::backend::Backend;

    use super::columnar::{DdirUpdate, DdirRecordedUpdates};
    use super::columnar::{ColValSpine, ColValBuilder};

    type ConcreteTime = Product<u64, PointStamp<u64>>;

    pub type Col<'scope> = Collection<'scope, ConcreteTime, DdirRecordedUpdates>;
    type Arr<'scope> = Arranged<'scope, TraceAgent<ColValSpine<Row, Row, ConcreteTime, Diff>>>;

    /// Render a Linear chain: one pass applying the ops in sequence. `level` is
    /// the op's scope depth — it locates the iteration coord for LiftIter and
    /// the coordinate position EnterAt's delay lands in.
    fn render_linear<'scope>(c: Col<'scope>, ops: Vec<LinearOp>, level: usize) -> Col<'scope> {
        super::columnar::join_function(c, move |k, v, t_in, _d| {
            use timely::progress::Timestamp;
            let k: Row = Columnar::into_owned(k);
            let v: Row = Columnar::into_owned(v);
            // Materialize input time once so LiftIter can read
            // the iter coord at the operator's scope depth.
            let t_owned: Time = Columnar::into_owned(t_in);
            let iter_at_level: i64 = level
                .checked_sub(1)
                .and_then(|idx| t_owned.inner.get(idx).copied())
                .unwrap_or(0) as i64;
            let mut results: Vec<(Row, Row, Time, Diff)> = vec![(k, v, Time::minimum(), 1i64)];
            for op in &ops {
                let mut next = Vec::new();
                for (k, v, t, d) in results {
                    match op {
                        LinearOp::Project(proj) => {
                            let i = [k.as_slice(), v.as_slice()];
                            next.push((eval_fields(&proj.key, &i), eval_fields(&proj.val, &i), t, d));
                        },
                        LinearOp::Filter(cond) => {
                            let i = [k.as_slice(), v.as_slice()];
                            if eval_condition(cond, &i) { next.push((k, v, t, d)); }
                        },
                        LinearOp::Negate => {
                            next.push((k, v, t, -d));
                        },
                        LinearOp::EnterAt(field) => {
                            let delay = {
                                let mut r = Row::new();
                                eval_field_into(field, &[k.as_slice(), v.as_slice()], &mut r);
                                256 * (64 - (r.as_slice().first().copied().unwrap_or(0) as u64).leading_zeros() as u64)
                            };
                            let mut coords = smallvec::SmallVec::<[u64; 1]>::new();
                            for _ in 0..level.saturating_sub(1) { coords.push(0); }
                            coords.push(delay);
                            next.push((k, v, Product::new(0u64, PointStamp::new(coords)), d));
                        },
                        LinearOp::LiftIter => {
                            let mut new_v = v.clone();
                            new_v.push(iter_at_level);
                            next.push((k, new_v, t, d));
                        },
                    }
                }
                results = next;
            }
            results.into_iter()
        })
    }

    fn render_join<'scope>(l: Arr<'scope>, r: Arr<'scope>, projection: &Projection) -> Col<'scope> {
        let proj = projection.clone();
        use differential_dataflow::operators::join::join_traces;
        use differential_dataflow::collection::AsCollection;
        use super::columnar::ValColBuilder;
        let stream = join_traces::<_, _, _, _, ValColBuilder<DdirUpdate>>(l, r, move |k, v1, v2, t, d1, d2, c| {
            use differential_dataflow::difference::Multiply;
            let d = d1.clone().multiply(d2);
            let i = [k.as_slice(), v1.as_slice(), v2.as_slice()];
            let (k2, v2): (Row, Row) = (eval_fields(&proj.key, &i), eval_fields(&proj.val, &i));
            c.give((k2, v2, t, d));
        });
        stream.as_collection()
    }

    fn render_reduce<'scope>(a: Arr<'scope>, reducer: &Reducer) -> Arr<'scope> {
        let reducer = reducer.clone();
        type ReduceFn = dyn for<'a> Fn(columnar::Ref<'a, Row>, &[(columnar::Ref<'a, Row>, Diff)], &mut Vec<(Row, Diff)>) + Send + Sync;
        let f: Arc<ReduceFn> = match reducer {
            Reducer::Min => Arc::new(|_key, vals, output| {
                if let Some(min) = vals.iter().map(|(v, _)| v.as_slice()).min() {
                    output.push((Row(min.to_vec()), 1));
                }
            }),
            Reducer::Distinct => Arc::new(|_key, _vals, output| { output.push((Row::new(), 1)); }),
            Reducer::Count => Arc::new(|_key, vals, output| {
                let count: Diff = vals.iter().map(|(_, d)| *d).sum();
                if count != 0 { let mut r = Row::new(); r.push(count); output.push((r, 1)); }
            }),
        };
        a.reduce_abelian::<_, ColValBuilder<_,_,_,_>, ColValSpine<_,_,_,_>, _, _>(
            "Reduce",
            move |k, vals, output| { f(k, vals, output); },
            |col, key, upds| {
                use columnar::{Clear, Push};
                col.keys.clear();
                col.vals.clear();
                col.times.clear();
                col.diffs.clear();
                for (val, time, diff) in upds.drain(..) { col.push((key, &val, &time, &diff)); }
                // NOTE: required because push above doesn't group by key, val.
                *col = std::mem::take(col).consolidate();
            },
        )
    }

    fn render_inspect<'scope>(col: Col<'scope>, label: String) -> Col<'scope> {
        col.inspect_container(move |event| {
            if let Ok((_time, container)) = event {
                for (k, v, t, d) in container.updates.view().iter() {
                    eprintln!("  [{}] ({:?}, {:?}, {:?}, {:?})", label, <Row as Columnar>::into_owned(k), <Row as Columnar>::into_owned(v), <Time as Columnar>::into_owned(t), <Diff as Columnar>::into_owned(d));
                }
            }
        })
    }

    /// The columnar rendering substrate.
    pub enum ColBackend {}

    impl Backend for ColBackend {
        type Container = DdirRecordedUpdates;
        type Arr<'scope> = Arr<'scope>;

        fn linear<'s>(c: Collection<'s, Time, Self::Container>, ops: Vec<LinearOp>, level: usize) -> Collection<'s, Time, Self::Container> {
            render_linear(c, ops, level)
        }
        fn arrange<'s>(c: Collection<'s, Time, Self::Container>) -> Self::Arr<'s> {
            use differential_dataflow::operators::arrange::arrangement::arrange_core;
            use super::columnar::{ColValBatcher, ColValChunker};
            arrange_core::<_, _, ColValChunker<(Row,Row,Time,Diff)>, ColValBatcher<Row,Row,Time,Diff>, ColValBuilder<Row,Row,Time,Diff>, ColValSpine<Row,Row,Time,Diff>>(c.inner.clone(), timely::dataflow::channels::pact::Pipeline, "Arrange")
        }
        fn as_collection<'s>(a: Self::Arr<'s>) -> Collection<'s, Time, Self::Container> {
            super::columnar_support::as_recorded_updates::<DdirUpdate>(a)
        }
        fn join<'s>(l: Self::Arr<'s>, r: Self::Arr<'s>, projection: &Projection) -> Collection<'s, Time, Self::Container> {
            render_join(l, r, projection)
        }
        fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer) -> Self::Arr<'s> {
            render_reduce(a, reducer)
        }
        fn inspect<'s>(c: Collection<'s, Time, Self::Container>, label: String) -> Collection<'s, Time, Self::Container> {
            render_inspect(c, label)
        }
        fn leave_dynamic<'s>(c: Collection<'s, Time, Self::Container>, depth: usize) -> Collection<'s, Time, Self::Container> {
            super::columnar::leave_dynamic(c, depth)
        }
    }
}

pub use types::{Row, Diff, Time};
pub use render::{Col, ColBackend};

/// Render `s` with the columnar substrate. See [`crate::backend::render_tree`].
pub fn render_tree<'s>(
    s: &crate::scope_ir::Scope,
    scope: timely::dataflow::Scope<'s, Time>,
    depth: usize,
    imports: Vec<Col<'s>>,
) -> Vec<Col<'s>> {
    crate::backend::render_tree::<ColBackend>(s, scope, depth, imports)
}
