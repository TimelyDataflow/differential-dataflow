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

/// A time as data: the epoch, then each of the `level` scope coordinates, zero where the
/// `PointStamp` has dropped it.
fn time_value(t: &Time, level: usize) -> Row {
    let coords = (0..level).map(|i| t.inner.get(i).copied().unwrap_or(0));
    Value::Tuple(std::iter::once(t.outer).chain(coords).map(|x| Value::Int(x as i64)).collect())
}

/// The time a tuple of `Int`s names, read as `time_value` writes it. A negative coordinate is
/// below every time, so it is zero, the minimum.
fn value_time(v: &Row) -> Time {
    let Value::Tuple(xs) = v else { panic!("join_function: expected a time tuple, got {v:?}") };
    let mut coords = xs.iter().map(|x| x.as_int().max(0) as u64);
    let outer = coords.next().unwrap_or(0);
    Product::new(outer, PointStamp::new(coords.collect()))
}

/// Render a Linear chain: one flat_map applying the ops in sequence. Each row carries its
/// time through the chain, so an op that reads the time sees the delays before it. `level` is
/// the op's scope depth — it locates the iteration coord for LiftIter, the
/// coordinate position EnterAt's delay lands in, and the width of a time as data.
fn render_linear<'scope>(c: Col<'scope>, ops: Vec<LinearOp>, level: usize) -> Col<'scope> {
    use differential_dataflow::AsCollection;
    use differential_dataflow::lattice::Lattice;
    use timely::dataflow::operators::core::Map;
    c.inner.flat_map(move |((key, val), t_in, d_in)| {
        let mut results: smallvec::SmallVec<[((Row, Row), Time, Diff); 2]> = svec![((key, val), t_in, d_in)];
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
                        next.push(((k, v), t.join(&Product::new(0u64, PointStamp::new(coords))), d));
                    },
                    LinearOp::LiftIter => {
                        let iter = level.checked_sub(1).and_then(|idx| t.inner.get(idx).copied()).unwrap_or(0) as i64;
                        next.push(((k, append_iter(v, iter)), t, d));
                    },
                    LinearOp::Lift => {
                        let time = time_value(&t, level);
                        next.push(((k, Value::Tuple(vec![v, time])), t, d));
                    },
                    LinearOp::JoinFunction(term) => {
                        let elems = match eval(term, &mut vec![k, v]) {
                            Value::List(xs) => xs,
                            other => panic!("join_function: expected a List, got {other:?}"),
                        };
                        for elem in elems {
                            let Value::Tuple(fs) = elem else { panic!("join_function: expected ((key, val), time, diff)") };
                            let Ok::<[Value; 3], _>([Value::Tuple(row), time, Value::Int(d2)]) = fs.try_into()
                            else { panic!("join_function: expected ((key, val), time, diff)") };
                            let Ok::<[Value; 2], _>([k2, v2]) = row.try_into()
                            else { panic!("join_function: expected a (key, val) pair") };
                            next.push(((k2, v2), t.join(&value_time(&time)), d * d2));
                        }
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
        results
    }).as_collection()
}

/// The vec rendering substrate.
pub enum VecBackend {}

impl Backend for VecBackend {
    type Container = Vec<((Row, Row), Time, Diff)>;
    type Arr<'scope> = Arr<'scope>;

    fn linear<'s>(c: Collection<'s, Time, Self::Container>, ops: Vec<LinearOp>, level: usize, _shape: Option<st::RowShape>) -> Collection<'s, Time, Self::Container> {
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
    fn join<'s>(l: Self::Arr<'s>, r: Self::Arr<'s>, projection: &Projection, _shapes: Option<(st::RowShape, st::RowShape)>) -> Collection<'s, Time, Self::Container> {
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
    fn half_join<'s>(_stream: Collection<'s, Time, Self::Container>, _arrangement: Self::Arr<'s>, _strict: bool, _projection: &Projection, _level: usize, _shapes: Option<(st::RowShape, st::RowShape)>) -> Collection<'s, Time, Self::Container> {
        panic!("half_join runs on the corgi backend only")
    }
    fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer) -> Self::Arr<'s> {
        let f: Arc<dyn Fn(&Row, &[(&Row, Diff)], &mut Vec<(Row, Diff)>) + Send + Sync> = match reducer {
            Reducer::Min => Arc::new(|_key, vals, output| { if let Some(min) = vals.iter().map(|(v, _)| (*v).clone()).min() { output.push((min, 1)); } }),
            Reducer::Distinct => Arc::new(|_key, _vals, output| { output.push((Value::unit(), 1)); }),
            // The count is the sum of the diffs, of either sign, as DD's `count`.
            Reducer::Count => Arc::new(|_key, vals, output| { let count: Diff = vals.iter().map(|(_, d)| *d).sum(); if count != 0 { output.push((Value::Int(count), 1)); } }),
            // NEST: collect the key's values into a List, in value order (DD
            // hands them sorted), each repeated per its multiplicity.
            Reducer::Collect => Arc::new(|_key, vals, output| {
                let mut items: Vec<Value> = Vec::new();
                for (v, d) in vals { for _ in 0..(*d).max(0) { items.push((*v).clone()); } }
                output.push((Value::List(items), 1));
            }),
            // The term reads the key and its values with their diffs, in value order (DD hands
            // them sorted, and only those with non-zero diffs), and lists the output with diffs.
            Reducer::Expr(term) => {
                let term = term.clone();
                Arc::new(move |key, vals, output| {
                    let vals = vals.iter().map(|(v, d)| Value::Tuple(vec![(*v).clone(), Value::Int(*d)])).collect();
                    match eval(&term, &mut vec![key.clone(), Value::List(vals)]) {
                        Value::List(outs) => output.extend(outs.into_iter().map(|out| match out {
                            Value::Tuple(mut pair) if pair.len() == 2 && matches!(pair[1], Value::Int(_)) => {
                                let diff = pair.pop().unwrap().as_int();
                                (pair.pop().unwrap(), diff)
                            }
                            other => panic!("reduce: expected an output (val, diff), got {other:?}"),
                        })),
                        other => panic!("reduce: expected a List, got {other:?}"),
                    }
                })
            }
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
    shapes: Option<&crate::shapes::ScopeShapes>,
) -> Vec<Col<'s>> {
    let imports = check_import_shapes(s, imports).into_iter().map(Rendered::Collection).collect();
    crate::backend::render_tree::<VecBackend>(s, scope, depth, imports, shapes)
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
    shapes: Option<&crate::shapes::ScopeShapes>,
) -> Vec<Col<'s>> {
    assert_eq!(s.imports.len(), imports.len());
    let imports = imports.into_iter().zip(&s.imports).map(|(rendered, import)| match rendered {
        Rendered::Collection(c) if import.shape.is_some() => Rendered::Collection(check_shape(c, import)),
        other => other,
    }).collect();
    crate::backend::render_tree::<VecBackend>(s, scope, depth, imports, shapes)
}
