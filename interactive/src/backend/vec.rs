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
use smallvec::SmallVec;
use smallvec::smallvec as svec;

use crate::backend::Backend;
use crate::parse::{Projection, Reducer};
use crate::scope_ir as st;
use crate::ir::{LinearOp, Diff, Time, Value, eval};

/// The row type: a single `Value` (an `Int`, or a `Tuple`/`List`/`Variant`).
pub type Row = Value;
/// A rendered collection at the renderer's (inner, dynamic) time.
pub type Col<'scope> = VecCollection<'scope, Time, (Row, Row), Diff>;
type Arr<'scope> = Arranged<'scope, TraceAgent<ValSpine<Row, Row, Time, Diff>>>;

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
                        let keep = { let mut env = vec![k.clone(), v.clone()]; eval(cond, &mut env).truthy() };
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
        c.arrange_by_key()
    }
    fn as_collection<'s>(a: Self::Arr<'s>) -> Collection<'s, Time, Self::Container> {
        a.as_collection(|k, v| (k.clone(), v.clone()))
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
        l.join_core(r, move |k, v1, v2| f(k, v1, v2))
    }
    fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer) -> Self::Arr<'s> {
        let f: Arc<dyn Fn(&Row, &[(&Row, Diff)], &mut Vec<(Row, Diff)>) + Send + Sync> = match reducer {
            Reducer::Min => Arc::new(|_key, vals, output| { if let Some(min) = vals.iter().map(|(v, _)| (*v).clone()).min() { output.push((min, 1)); } }),
            Reducer::Distinct => Arc::new(|_key, _vals, output| { output.push((Value::unit(), 1)); }),
            Reducer::Count => Arc::new(|_key, vals, output| { let count: Diff = vals.iter().map(|(_, d)| *d).sum(); if count > 0 { output.push((Value::Int(count), 1)); } }),
            // NEST: collect the key's values into a List, in value order (DD
            // hands them sorted), each repeated per its multiplicity.
            Reducer::Collect => Arc::new(|_key, vals, output| {
                let mut items: Vec<Value> = Vec::new();
                for (v, d) in vals { for _ in 0..(*d).max(0) { items.push((*v).clone()); } }
                output.push((Value::List(items), 1));
            }),
        };
        a.reduce_abelian::<_, ValBuilder<_, _, _, _>, ValSpine<_, _, _, _>, _>(
            "Reduce",
            move |k, v, o| f(k, v, o),
            |vec, key, upds| { vec.clear(); vec.extend(upds.drain(..).map(|(v, t, r)| ((key.clone(), v), t, r))); },
        )
    }
    fn inspect<'s>(c: Collection<'s, Time, Self::Container>, label: String) -> Collection<'s, Time, Self::Container> {
        c.inspect(move |x| eprintln!("  [{}] {:?}", label, x.clone()))
    }
    fn leave_dynamic<'s>(c: Collection<'s, Time, Self::Container>, depth: usize) -> Collection<'s, Time, Self::Container> {
        c.leave_dynamic(depth)
    }
}

/// Render `s` with the vec substrate. See [`crate::backend::render_tree`].
pub fn render_tree<'s>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<Col<'s>>,
) -> Vec<Col<'s>> {
    crate::backend::render_tree::<VecBackend>(s, scope, depth, imports)
}

/// Evaluate `program` on explicit inputs: single worker, in-process, every
/// export returned as its consolidated final contents (rows with non-zero
/// net multiplicity).
///
/// `inputs[i]` provides the rows of positional input `i`, each with
/// multiplicity +1; all rows are introduced at time 0 and the computation
/// runs to quiescence. This is the data-in/data-out entry point that tests
/// and tools build on — e.g. feeding an explanation's demand-set back
/// through the original program to check the queried output reproduces.
pub fn evaluate(
    program: &st::Program,
    inputs: &[Vec<(Row, Row)>],
) -> std::collections::BTreeMap<String, Vec<((Row, Row), Diff)>> {
    use std::sync::mpsc::channel;
    use timely::dataflow::operators::core::capture::{Capture, Event};
    use differential_dataflow::input::Input;

    let names: Vec<String> = program.root.exports.iter().map(|e| e.name.clone()).collect();
    let mut txs = Vec::with_capacity(names.len());
    let mut rxs = Vec::with_capacity(names.len());
    for _ in &names {
        let (tx, rx) = channel::<Event<u64, Vec<((Row, Row), u64, Diff)>>>();
        txs.push(tx);
        rxs.push(rx);
    }

    let program = program.clone();
    let inputs: Vec<Vec<(Row, Row)>> = inputs.to_vec();
    timely::execute_directly(move |worker| {
        let mut handles = worker.dataflow::<u64, _, _>(|scope| {
            let mut handles = Vec::new();
            let mut collections = Vec::new();
            for _ in 0..inputs.len() {
                let (h, c) = scope.new_collection::<(Row, Row), Diff>();
                handles.push(h);
                collections.push(c);
            }
            let exports = scope.iterative::<PointStamp<u64>, _, _>(|inner| {
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                let root_imports: Vec<_> = program.root.imports.iter().map(|imp| match &imp.from {
                    st::Source::Input(n) => entered[*n].clone(),
                    other => panic!("evaluate: unsupported source {:?}", other),
                }).collect();
                let exports = render_tree(&program.root, inner.clone(), 0, root_imports);
                exports.into_iter().map(|c| c.leave(scope)).collect::<Vec<_>>()
            });
            for (col, tx) in exports.into_iter().zip(txs) {
                col.inner.capture_into(tx);
            }
            handles
        });
        for (i, rows) in inputs.iter().enumerate() {
            for r in rows {
                handles[i].update(r.clone(), 1);
            }
        }
        // Dropping the handles closes the inputs; `execute_directly` then
        // steps the worker until the dataflow completes.
    });

    names
        .into_iter()
        .zip(rxs)
        .map(|(name, rx)| {
            let mut acc: std::collections::BTreeMap<(Row, Row), Diff> = std::collections::BTreeMap::new();
            for event in rx {
                if let Event::Messages(_, data) = event {
                    for ((k, v), _, d) in data {
                        *acc.entry((k, v)).or_insert(0) += d;
                    }
                }
            }
            (name, acc.into_iter().filter(|(_, d)| *d != 0).collect())
        })
        .collect()
}
