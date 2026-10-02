//! Vec-backed rendering substrate.
//!
//! Rows are `interactive::ir::Value` — an ADT (Int / Tuple / Variant / List);
//! a collection element is a `(key, val)` pair of `Value`s. The differential
//! container is a flat `Vec<((Row, Row), Time, Diff)>`. Scalar work in
//! `map`/`join`/`reduce`/`filter` is the tree-walking `Term` interpreter
//! (`ir::eval`). Supplies the substrate leaf operators; the scope-tree walk
//! lives in [`crate::backend::render_tree`].

use std::sync::Arc;
use timely::order::Product;
use timely::dataflow::Scope;
use differential_dataflow::{Collection, VecCollection};
use differential_dataflow::dynamic::pointstamp::PointStamp;
use differential_dataflow::trace::implementations::{ValSpine, ValBuilder};
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::wrappers::enter::TraceEnter;
use smallvec::SmallVec;
use smallvec::smallvec as svec;

use crate::backend::{Backend, Rendered};
use crate::scope_ir as st;
use crate::ir::{LinearOp, Diff, Projection, Reducer, Time, Value, eval};

/// The row type: a single `Value` (an `Int`, or a `Tuple`/`List`/`Variant`).
pub type Row = Value;
/// A rendered collection at the renderer's (inner, dynamic) time.
pub type Col<'scope> = VecCollection<'scope, Time, (Row, Row), Diff>;

/// Validate explicit encoding contracts at the row boundary on either backend.
/// A mismatch panics inside a dataflow operator, which can take down the shared
/// server on either backend. This is not transactional feed admission or
/// per-program failure isolation.
pub(crate) fn check_import_shapes<'s>(s: &st::Scope, imports: Vec<Col<'s>>) -> Vec<Col<'s>> {
    assert_eq!(s.imports.len(), imports.len());
    imports.into_iter().zip(&s.imports).map(|(c, import)| check_shape(c, import)).collect()
}

/// Check one import's rows against its shape ascription, if it has one.
pub(crate) fn check_shape<'s>(c: Col<'s>, import: &st::Import) -> Col<'s> {
    use differential_dataflow::AsCollection;
    use timely::dataflow::operators::core::Map;
    if let Some((key, val)) = import.shape.clone() {
        c.inner.map(move |row| {
            assert!(row.0.0.has_shape(&key) && row.0.1.has_shape(&val), "input does not match its shape ascription");
            row
        }).as_collection()
    } else { c }
}

/// An arrangement this program built, at the renderer's time.
pub type LocalArr<'scope> = Arranged<'scope, TraceAgent<ValSpine<Row, Row, Time, Diff>>>;
/// A published trace (arranged at the server's host time, `u64`), entered into the program's
/// scope: the producer's own batches, read with each host time `t` as `(t, [])`.
pub type ImportArr<'scope> = Arranged<'scope, TraceEnter<TraceAgent<ValSpine<Row, Row, u64, Diff>>, Time>>;

/// The vec backend's arrangement: one it built, or one it imported. Join and reduce accept
/// either (they read batches through cursors, and an entered batch's cursor lifts its times).
#[derive(Clone)]
pub enum Arr<'scope> {
    Local(LocalArr<'scope>),
    Imported(ImportArr<'scope>),
}

/// Append the user-iter coordinate to a value: extend a `Tuple` in place, or
/// wrap any other value as `(value, iter)`.
fn append_iter(val: Row, iter: i64) -> Row {
    match val {
        Value::Tuple(mut xs) => { xs.push(Value::Int(iter)); Value::Tuple(xs) }
        other => Value::Tuple(vec![other, Value::Int(iter)]),
    }
}

/// Render a Linear chain: one flat_map applying the ops in sequence. `level` is
/// the op's scope depth — it locates the iteration coord for LiftIter and the
/// coordinate position EnterAt's delay lands in.
fn render_linear<'scope>(c: Col<'scope>, ops: Vec<LinearOp>, level: usize) -> Col<'scope> {
    use differential_dataflow::AsCollection;
    use differential_dataflow::lattice::Lattice;
    use timely::dataflow::operators::core::Map;
    c.inner.flat_map(move |((key, val), t_in, d_in)| {
        use timely::progress::Timestamp;
        let iter_at_level: i64 = level
            .checked_sub(1)
            .and_then(|idx| t_in.inner.get(idx).copied())
            .unwrap_or(0) as i64;
        let mut results: smallvec::SmallVec<[((Row, Row), Time, Diff); 2]> = svec![((key, val), Time::minimum(), 1)];
        for op in &ops {
            let mut next = smallvec::SmallVec::new();
            for ((k, v), t, d) in results {
                match op {
                    LinearOp::Project(proj) => {
                        let mut env = vec![k, v];
                        let nk = eval(&proj.key, &mut env);
                        let nv = eval(&proj.val, &mut env);
                        next.push(((nk, nv), t, d));
                    },
                    LinearOp::Filter(cond) => {
                        let keep = match eval(cond, &mut vec![k.clone(), v.clone()]) {
                            Value::Int(n) => n != 0,
                            other => panic!("a filter predicate must be an Int, got {other:?}"),
                        };
                        if keep { next.push(((k, v), t, d)); }
                    },
                    LinearOp::Negate => {
                        next.push(((k, v), t, -d));
                    },
                    LinearOp::EnterAt(field) => {
                        let delay = {
                            let mut env = vec![k.clone(), v.clone()];
                            let raw = eval(field, &mut env).as_int() as u64;
                            256 * (64 - raw.leading_zeros() as u64)
                        };
                        let mut coords = smallvec::SmallVec::<[u64; 1]>::new();
                        for _ in 0..level.saturating_sub(1) { coords.push(0); }
                        coords.push(delay);
                        next.push(((k, v), Product::new(0u64, PointStamp::new(coords)), d));
                    },
                    LinearOp::LiftIter => {
                        next.push(((k, append_iter(v, iter_at_level)), t, d));
                    },
                    LinearOp::FlatMap(list_term) => {
                        let elems = {
                            let mut env = vec![k.clone(), v.clone()];
                            match eval(list_term, &mut env) {
                                Value::List(xs) => xs,
                                other => panic!("flatmap: expected a List, got {:?}", other),
                            }
                        };
                        // One row per element: (key, tuple(pos, element)),
                        // position first so `collect` restores order.
                        for (pos, elem) in elems.into_iter().enumerate() {
                            next.push(((k.clone(), Value::Tuple(vec![Value::Int(pos as i64), elem])), t.clone(), d));
                        }
                    },
                }
            }
            results = next;
        }
        results.into_iter().map(move |((k, v), t_delta, d)| ((k, v), t_in.join(&t_delta), d_in * d))
    }).as_collection()
}

/// The vec rendering substrate.
pub enum VecBackend {}

impl Backend for VecBackend {
    type Container = Vec<((Row, Row), Time, Diff)>;
    type Arr<'scope> = Arr<'scope>;

    fn linear<'s>(c: Collection<'s, Time, Self::Container>, ops: Vec<LinearOp>, level: usize) -> Collection<'s, Time, Self::Container> {
        render_linear(c, ops, level)
    }
    fn arrange<'s>(c: Collection<'s, Time, Self::Container>) -> Self::Arr<'s> {
        Arr::Local(c.arrange_by_key())
    }
    fn as_collection<'s>(a: Self::Arr<'s>) -> Collection<'s, Time, Self::Container> {
        match a {
            Arr::Local(a) => a.as_collection(|k, v| (k.clone(), v.clone())),
            Arr::Imported(a) => a.as_collection(|k, v| (k.clone(), v.clone())),
        }
    }
    fn join<'s>(l: Self::Arr<'s>, r: Self::Arr<'s>, projection: &Projection) -> Collection<'s, Time, Self::Container> {
        let proj = projection.clone();
        let f: Arc<dyn Fn(&Row, &Row, &Row) -> SmallVec<[(Row, Row); 2]> + Send + Sync> =
            Arc::new(move |key, left, right| {
                let mut env = vec![key.clone(), left.clone(), right.clone()];
                let k = eval(&proj.key, &mut env);
                let v = eval(&proj.val, &mut env);
                svec![(k, v)]
            });
        match (l, r) {
            (Arr::Local(l), Arr::Local(r)) => l.join_core(r, move |k, v1, v2| f(k, v1, v2)),
            (Arr::Local(l), Arr::Imported(r)) => l.join_core(r, move |k, v1, v2| f(k, v1, v2)),
            (Arr::Imported(l), Arr::Local(r)) => l.join_core(r, move |k, v1, v2| f(k, v1, v2)),
            (Arr::Imported(l), Arr::Imported(r)) => l.join_core(r, move |k, v1, v2| f(k, v1, v2)),
        }
    }
    fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer) -> Self::Arr<'s> {
        let f: Arc<dyn Fn(&Row, &[(&Row, Diff)], &mut Vec<(Row, Diff)>) + Send + Sync> = match reducer {
            Reducer::Min => Arc::new(|_key, vals, output| { if let Some(min) = vals.iter().map(|(v, _)| (*v).clone()).min() { output.push((min, 1)); } }),
            Reducer::Distinct => Arc::new(|_key, _vals, output| { output.push((Value::unit(), 1)); }),
            // Count yields a one-field tuple `(count)`, keeping the convention
            // that a value is a tuple (so `$1[0]` and the explain envelope work).
            Reducer::Count => Arc::new(|_key, vals, output| { let count: Diff = vals.iter().map(|(_, d)| *d).sum(); if count > 0 { output.push((Value::Tuple(vec![Value::Int(count)]), 1)); } }),
            // NEST: collect the key's values into a List, in value order (DD
            // hands them sorted), each repeated per its multiplicity.
            Reducer::Collect => Arc::new(|_key, vals, output| {
                let mut items: Vec<Value> = Vec::new();
                for (v, d) in vals { for _ in 0..(*d).max(0) { items.push((*v).clone()); } }
                output.push((Value::List(items), 1));
            }),
        };
        // The same reduction over either arrangement; its output is always local.
        macro_rules! reduce {
            ($a:expr) => {
                $a.reduce_abelian::<_, ValBuilder<_, _, _, _>, ValSpine<_, _, _, _>, _, _>(
                    "Reduce",
                    move |k, v, o| f(k, v, o),
                    |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v, t, r)| ((key.clone(), v), t, r))); },
                )
            };
        }
        Arr::Local(match a {
            Arr::Local(a) => reduce!(a),
            Arr::Imported(a) => reduce!(a),
        })
    }
    fn inspect<'s>(c: Collection<'s, Time, Self::Container>, label: String) -> Collection<'s, Time, Self::Container> {
        use std::fmt::Write;
        let mut line = String::new();
        c.inspect(move |x| {
            // Keep formatting outside stderr's lock, and write one complete record.
            line.clear();
            writeln!(&mut line, "  [{label}] {x:?}").unwrap();
            eprint!("{line}");
        })
    }
    fn leave_dynamic<'s>(c: Collection<'s, Time, Self::Container>, depth: usize) -> Collection<'s, Time, Self::Container> {
        c.leave_dynamic(depth)
    }
    /// An imported trace may: its handles report compaction at the host time (`TraceEnter`
    /// keeps only the outer coordinate), so no reader can compact it to iteration times.
    fn shares_into_regions<'s>(a: &Self::Arr<'s>) -> bool {
        matches!(a, Arr::Imported(_))
    }
    fn enter_region<'s, 'r>(a: Self::Arr<'s>, region: Scope<'r, Time>) -> Self::Arr<'r> {
        match a {
            Arr::Local(a) => Arr::Local(a.enter_region(region)),
            Arr::Imported(a) => Arr::Imported(a.enter_region(region)),
        }
    }
}

/// Render `s` with the vec substrate. See [`crate::backend::render_tree`].
pub fn render_tree<'s>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<Col<'s>>,
) -> Vec<Col<'s>> {
    let imports = check_import_shapes(s, imports).into_iter().map(Rendered::Collection).collect();
    crate::backend::render_tree::<VecBackend>(s, scope, depth, imports)
}

/// As [`render_tree`], but an import may arrive already arranged (a published trace), in
/// which case the program's `arrange`s and joins of it read the producer's arrangement
/// instead of building their own. Shape ascriptions are checked here for imports that
/// arrive as rows; checking an arranged import's ascription is the driver's job.
pub fn render_tree_arranged<'s>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<Rendered<'s, VecBackend>>,
) -> Vec<Col<'s>> {
    assert_eq!(s.imports.len(), imports.len());
    let imports = imports.into_iter().zip(&s.imports).map(|(rendered, import)| match rendered {
        Rendered::Collection(c) if import.shape.is_some() => Rendered::Collection(check_shape(c, import)),
        other => other,
    }).collect();
    crate::backend::render_tree::<VecBackend>(s, scope, depth, imports)
}
