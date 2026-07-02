//! corgi-native rendering substrate (Route B): corgi columns are the native representation, the
//! arrangement is `Spine<Rc<CorgiBatch>>` (cursor-less), and the scalar logic is `eval_graph`.
//! Parallels `backend::vec`. Linear/arrange/join are corgi-native; `reduce` awaits Frank's
//! cursor-less `retire` design (the operator's interesting-times machinery).
//!
//! This iteration: the `Backend` impl SHAPE compiles (arrange + leave_dynamic real; linear/join/
//! as_collection/reduce/inspect = `todo!()`), validating the trait wiring + `render_tree::<CorgiBackend>`.

use timely::container::CapacityContainerBuilder;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;

use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use differential_dataflow::operators::join::join_with_tactic;
use differential_dataflow::operators::reduce::reduce_with_tactic;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::chunker::ContainerChunker;
use differential_dataflow::trace::implementations::merge_batcher::vec::VecMerger;
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;

use corgi::arrange::gather;
use corgi::Value as CValue;

use crate::backend::Backend;
use crate::corgi_chunk::{batch_to_rows, CorgiChunk, CorgiChunkBuilder};
use crate::corgi_backend::CorgiContainer;
use crate::corgi_join::CorgiJoinTactic;
use crate::corgi_reduce::CorgiReduceTactic;
use crate::corgi_logic::{compilable, compile_predicate, compile_projection};
use crate::ir::{Diff, LinearOp, Time, Value as DValue};
use crate::parse::{Projection, Reducer};
use crate::scope_ir as st;

/// Apply a `LinearOp` chain to one corgi container (the corgi-native row-wise compute per batch).
/// Project = corgi `eval_graph`; Filter = corgi mask + `gather`; Negate = Rust — all columnar.
/// The time/list-shaping ops (EnterAt/LiftIter/FlatMap) take a correctness-first row-wise path
/// (untranscode → vec-style transform → `from_updates`), matching `backend::vec` exactly; a columnar
/// fast-path is future work. `level` is the scope depth (locates the iteration coordinate).
fn apply_ops(mut c: CC, ops: &[LinearOp], level: usize) -> CC {
    use timely::order::Product;
    use differential_dataflow::lattice::Lattice;
    use differential_dataflow::dynamic::pointstamp::PointStamp;

    for op in ops {
        c = match op {
            LinearOp::Project(p) if compilable(&p.key) && compilable(&p.val) => {
                let (kshape, vshape) = (corgi::shape_of_value(&c.keys), corgi::shape_of_value(&c.vals));
                let g = compile_projection(&p.key, &p.val, &kshape, &vshape);
                let mut cols = corgi::eval_graph(&g, CValue::Prod(vec![c.keys, c.vals])).into_prod("linear project");
                let vals = cols.pop().unwrap();
                let keys = cols.pop().unwrap();
                CorgiContainer { keys, vals, times: c.times, diffs: c.diffs }
            }
            LinearOp::Filter(cond) if compilable(cond) => {
                let (kshape, vshape) = (corgi::shape_of_value(&c.keys), corgi::shape_of_value(&c.vals));
                let g = compile_predicate(cond, &kshape, &vshape);
                let mask = corgi::eval_graph(&g, CValue::Prod(vec![c.keys.clone(), c.vals.clone()])).into_u64("filter mask");
                let keep: Vec<usize> = (0..mask.len()).filter(|&i| mask[i] != 0).collect();
                let keys = gather(&c.keys, &keep);
                let vals = gather(&c.vals, &keep);
                let times = keep.iter().map(|&i| c.times[i].clone()).collect();
                let diffs = keep.iter().map(|&i| c.diffs[i]).collect();
                CorgiContainer { keys, vals, times, diffs }
            }
            // Row-wise fallback for projections/predicates the corgi compiler can't lower (List,
            // Case/Inject, Unary, Hash) — `ir::eval`, parity with `backend::vec`.
            LinearOp::Project(p) => {
                let mut out: Vec<Upd> = Vec::new();
                for ((k, v), t, d) in c.into_updates() {
                    let mut env = vec![k, v];
                    let nk = crate::ir::eval(&p.key, &mut env);
                    let nv = crate::ir::eval(&p.val, &mut env);
                    out.push(((nk, nv), t, d));
                }
                CorgiContainer::from_updates(out)
            }
            LinearOp::Filter(cond) => {
                let mut out: Vec<Upd> = Vec::new();
                for ((k, v), t, d) in c.into_updates() {
                    let keep = { let mut env = vec![k.clone(), v.clone()]; crate::ir::eval(cond, &mut env).truthy() };
                    if keep { out.push(((k, v), t, d)); }
                }
                CorgiContainer::from_updates(out)
            }
            LinearOp::Negate => {
                for d in c.diffs.iter_mut() {
                    *d = -*d;
                }
                c
            }
            // Row-wise ops (parity with `backend::vec::render_linear`).
            LinearOp::EnterAt(field) => {
                let mut out: Vec<Upd> = Vec::new();
                for ((k, v), t, d) in c.into_updates() {
                    let delay = {
                        let mut env = vec![k.clone(), v.clone()];
                        let raw = crate::ir::eval(field, &mut env).as_int() as u64;
                        256 * (64 - raw.leading_zeros() as u64)
                    };
                    let mut coords = smallvec::SmallVec::<[u64; 1]>::new();
                    for _ in 0..level.saturating_sub(1) { coords.push(0); }
                    coords.push(delay);
                    let delta = Product::new(0u64, PointStamp::new(coords));
                    out.push(((k, v), t.join(&delta), d));
                }
                CorgiContainer::from_updates(out)
            }
            LinearOp::LiftIter => {
                let mut out: Vec<Upd> = Vec::new();
                for ((k, v), t, d) in c.into_updates() {
                    let iter = level
                        .checked_sub(1)
                        .and_then(|idx| t.inner.get(idx).copied())
                        .unwrap_or(0) as i64;
                    out.push(((k, append_iter(v, iter)), t, d));
                }
                CorgiContainer::from_updates(out)
            }
            LinearOp::FlatMap(list_term) => {
                let mut out: Vec<Upd> = Vec::new();
                for ((k, v), t, d) in c.into_updates() {
                    let elems = {
                        let mut env = vec![k.clone(), v.clone()];
                        match crate::ir::eval(list_term, &mut env) {
                            DValue::List(xs) => xs,
                            other => panic!("flatmap: expected a List, got {other:?}"),
                        }
                    };
                    for (pos, elem) in elems.into_iter().enumerate() {
                        out.push(((k.clone(), DValue::Tuple(vec![DValue::Int(pos as i64), elem])), t.clone(), d));
                    }
                }
                CorgiContainer::from_updates(out)
            }
        };
    }
    c
}

/// Append the user-iter coordinate to a value (mirrors `backend::vec::append_iter`): extend a `Tuple`,
/// or wrap any other value as `(value, iter)`.
fn append_iter(val: DValue, iter: i64) -> DValue {
    match val {
        DValue::Tuple(mut xs) => { xs.push(DValue::Int(iter)); DValue::Tuple(xs) }
        other => DValue::Tuple(vec![other, DValue::Int(iter)]),
    }
}

type Row = DValue;
type Upd = ((Row, Row), Time, Diff);
type CC = CorgiContainer<Time, Diff>;
type CTrace = differential_dataflow::trace::chunk::ChunkSpine<CorgiChunk<Time, Diff>>;

/// The corgi rendering substrate.
pub enum CorgiBackend {}

impl Backend for CorgiBackend {
    type Container = CC;
    type Arr<'scope> = Arranged<'scope, TraceAgent<CTrace>>;

    fn linear<'s>(c: Collection<'s, Time, CC>, ops: Vec<LinearOp>, level: usize) -> Collection<'s, Time, CC> {
        // Container-level: fold the LinearOp chain over each corgi batch (no inter-op transcode).
        // `level` is the scope depth (locates the iteration coordinate for LiftIter/EnterAt).
        c.inner
            .unary(Pipeline, "CorgiLinear", move |_, _| {
                move |input, output| {
                    input.for_each(|cap, data| {
                        let mut out = apply_ops(std::mem::take(data), &ops, level);
                        output.session(&cap).give_container(&mut out);
                    });
                }
            })
            .as_collection()
    }

    fn arrange<'s>(c: Collection<'s, Time, CC>) -> Self::Arr<'s> {
        arrange_core::<_, CC, ContainerChunker<Vec<Upd>>, MergeBatcher<VecMerger<(Row, Row), Time, Diff>>, RcBuilder<CorgiChunkBuilder<Time, Diff>>, CTrace>(
            c.inner,
            Pipeline,
            "CorgiArrange",
        )
    }

    fn as_collection<'s>(a: Self::Arr<'s>) -> Collection<'s, Time, CC> {
        // Cursor-free: the arrangement's stream carries `Vec<Rc<CorgiBatch>>`; read each batch's
        // updates (corgi columns → rows) and reassemble a corgi container of those updates.
        a.stream
            .unary(Pipeline, "CorgiAsCollection", |_, _| {
                |input, output| {
                    input.for_each(|cap, data| {
                        let mut rows = Vec::new();
                        for batch in data.iter() {
                            rows.extend(batch_to_rows(batch));
                        }
                        let mut c = CorgiContainer::from_updates(rows);
                        output.session(&cap).give_container(&mut c);
                    });
                }
            })
            .as_collection()
    }

    fn join<'s>(l: Self::Arr<'s>, r: Self::Arr<'s>, projection: &Projection) -> Collection<'s, Time, CC> {
        // The tactic compiles the projection per work-unit (shape-directed, for `Spread`).
        let tactic = CorgiJoinTactic::new(projection.key.clone(), projection.val.clone());
        // join_with_tactic emits Vec-rows (via the output session); a ToCorgi unary rebuilds the
        // corgi container at the operator output boundary.
        let rows = join_with_tactic::<_, _, _, CapacityContainerBuilder<Vec<Upd>>>(l, r, tactic);
        rows.unary(Pipeline, "JoinToCorgi", |_, _| {
            |input, output| {
                input.for_each(|cap, data| {
                    let mut c = CorgiContainer::from_updates(std::mem::take(data));
                    output.session(&cap).give_container(&mut c);
                });
            }
        })
        .as_collection()
    }

    fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer) -> Self::Arr<'s> {
        reduce_with_tactic::<_, CTrace, _>(a, "CorgiReduce", CorgiReduceTactic::new(reducer.clone()))
    }

    fn inspect<'s>(c: Collection<'s, Time, CC>, label: String) -> Collection<'s, Time, CC> {
        c.inner
            .unary(Pipeline, "CorgiInspect", move |_, _| {
                move |input, output| {
                    input.for_each(|cap, data| {
                        let mut cont = std::mem::take(data);
                        for ((k, v), t, d) in cont.clone().into_updates() {
                            eprintln!("  [{label}] (({k:?}, {v:?}), {t:?}, {d})");
                        }
                        output.session(&cap).give_container(&mut cont);
                    });
                }
            })
            .as_collection()
    }

    fn leave_dynamic<'s>(c: Collection<'s, Time, CC>, level: usize) -> Collection<'s, Time, CC> {
        // Mirror DD's `Collection::leave_dynamic` (dynamic/mod.rs:40), but over a `CorgiContainer`:
        // strip all but `level-1` PointStamp coordinates from the capability AND from each row's time
        // (stored columnar in `CorgiContainer.times`, not inline in the data tuples). The input
        // connection summary advertises the `retain` so timely's progress tracking stays correct.
        use timely::dataflow::operators::generic::{builder_rc::OperatorBuilder, OutputBuilder};
        use timely::order::Product;
        use timely::progress::Antichain;
        use differential_dataflow::dynamic::pointstamp::{PointStamp, PointStampSummary};

        let mut builder = OperatorBuilder::new("CorgiLeaveDynamic".to_string(), c.inner.scope());
        let (output, stream) = builder.new_output();
        let mut output = OutputBuilder::from(output);
        let summary = Product { outer: Default::default(), inner: PointStampSummary { retain: Some(level - 1), actions: Vec::new() } };
        let mut input = builder.new_input_connection(c.inner, Pipeline, [(0, Antichain::from_elem(summary))]);

        builder.build(move |_capability| move |_frontier| {
            let mut output = output.activate();
            input.for_each(|cap, data| {
                let mut new_time = cap.time().clone();
                let mut v = std::mem::take(&mut new_time.inner).into_inner();
                v.truncate(level - 1);
                new_time.inner = PointStamp::new(v);
                let new_cap = cap.delayed(&new_time, 0);
                for t in data.times.iter_mut() {
                    let mut v = std::mem::take(&mut t.inner).into_inner();
                    v.truncate(level - 1);
                    t.inner = PointStamp::new(v);
                }
                output.session(&new_cap).give_container(data);
            });
        });

        stream.as_collection()
    }
}

/// Render `s` with the corgi substrate. See [`crate::backend::render_tree`].
pub fn render_tree<'s>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<Collection<'s, Time, CC>>,
) -> Vec<Collection<'s, Time, CC>> {
    crate::backend::render_tree::<CorgiBackend>(s, scope, depth, imports)
}

/// Evaluate `program` on explicit inputs via the **corgi** backend (mirrors [`crate::backend::vec::evaluate`]).
///
/// Inputs/exports cross the iterative-scope boundary as Vec rows (which support refinement
/// enter/leave); corgi containers exist only INSIDE the dynamic scope, where `Enter`/`Leave` are the
/// same-Time identity. The `ToCorgi`/`FromCorgi` unaries are the only row↔corgi conversions.
pub fn evaluate(
    program: &st::Program,
    inputs: &[Vec<(Row, Row)>],
) -> std::collections::BTreeMap<String, Vec<((Row, Row), Diff)>> {
    use std::collections::BTreeMap;
    use std::sync::mpsc::channel;
    use timely::dataflow::operators::core::capture::{Capture, Event};
    use timely::dataflow::operators::generic::Operator;
    use differential_dataflow::input::Input;
    use differential_dataflow::AsCollection;
    use differential_dataflow::dynamic::pointstamp::PointStamp;
    use crate::corgi_backend::CorgiContainer;

    let names: Vec<String> = program.root.exports.iter().map(|e| e.name.clone()).collect();
    let mut txs = Vec::new();
    let mut rxs = Vec::new();
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
                // Enter Vec collections (refinement), then convert each to a corgi container.
                let mut corgi_imports = Vec::new();
                for c in collections.iter().map(|c| c.clone().enter(inner)) {
                    let cs = c
                        .inner
                        .unary(Pipeline, "ToCorgi", |_, _| {
                            |input, output| {
                                input.for_each(|cap, data| {
                                    let mut cc = CorgiContainer::from_updates(std::mem::take(data));
                                    output.session(&cap).give_container(&mut cc);
                                });
                            }
                        })
                        .as_collection();
                    corgi_imports.push(cs);
                }
                let root_imports: Vec<_> = program
                    .root
                    .imports
                    .iter()
                    .map(|imp| match &imp.from {
                        st::Source::Input(n) => corgi_imports[*n].clone(),
                        other => panic!("corgi evaluate: unsupported source {other:?}"),
                    })
                    .collect();
                let exports = render_tree(&program.root, inner.clone(), 0, root_imports);
                // Convert corgi exports back to Vec rows, then leave the scope.
                let mut leaved = Vec::new();
                for c in exports {
                    let rows = c
                        .inner
                        .unary(Pipeline, "FromCorgi", |_, _| {
                            |input, output| {
                                input.for_each(|cap, data| {
                                    let mut rows = std::mem::take(data).into_updates();
                                    output.session(&cap).give_container(&mut rows);
                                });
                            }
                        })
                        .as_collection();
                    leaved.push(rows.leave(scope));
                }
                leaved
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
    });

    names
        .into_iter()
        .zip(rxs)
        .map(|(name, rx)| {
            let mut acc: BTreeMap<(Row, Row), Diff> = BTreeMap::new();
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
