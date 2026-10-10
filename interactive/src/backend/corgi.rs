//! The corgi rendering substrate: corgi columns are the native representation on dataflow edges,
//! arrangements are chains of sorted columnar chunks (`ChunkSpine<CorgiChunk>`, cursor-less), and
//! scalar logic runs columnar via `eval_graph`. The row-wise `backend::vec` remains useful for
//! comparison, but its representation choices do not define corgi's physical semantics.
//!
//! All `Backend` methods are corgi-native: `linear` folds a `LinearOp` chain over each container
//! ([`apply_ops`]); `arrange` ingests columns without
//! a row round-trip; `join`/`reduce` run through the int-proxy tactics ([`CorgiJoinBackend`],
//! [`CorgiReduceBackend`]) over the columnar chunks.

use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;

use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use differential_dataflow::operators::join::join_with_tactic;
use differential_dataflow::operators::reduce::reduce_with_tactic;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::chunk::{Chunk, ChunkBatcher};

use corgi::arrange::gather;
use corgi::Value as CValue;

use crate::backend::Backend;
use crate::corgi::chunk::{recover_key, CorgiChunk, CorgiChunker};
use crate::corgi::container::CorgiContainer;
use crate::corgi::exchange::CorgiPact;
use crate::corgi::join::CorgiJoinBackend;
use crate::corgi::reduce::CorgiReduceBackend;
use differential_dataflow::operators::int_proxy::{ProxyJoinTactic, ProxyReduceTactic};
use crate::corgi::logic::{compile_flatmap, compile_predicate, compile_projection, compile_scalar, shape_of_row};
use corgi::{Graph, NumOp, Shape};
use crate::ir::{Diff, LinearOp, Projection, Reducer, Time, Value as DValue};
use crate::scope_ir as st;

/// A DDIR row, an update, the corgi container on dataflow edges, and the columnar trace —
/// the shorthands the `Backend` methods below are written in terms of.
type Row = DValue;
type CC = CorgiContainer<Time, Diff>;
type CTrace = differential_dataflow::trace::chunk::ChunkSpine<CorgiChunk<Time, Diff>>;

/// The compiled form of one `LinearOp`, pinned to the shapes it was compiled against. Shapes are
/// static per collection, so a chain compiles ONCE: when the operator is built, if its input's
/// shape was inferred at install, and otherwise on the first non-empty batch. Every later batch
/// reuses the graph; a batch of a different shape is the invariant violation, not a recompile.
#[derive(Default)]
pub struct Plan {
    compiled: Option<(Shape, Shape, Graph<NumOp>)>,
}

impl Plan {
    /// The graph of `op` for a container of these shapes, compiling on first use. A type error
    /// is a panic with corgi's message: a program that typechecks never reaches it.
    fn graph(&mut self, op: &LinearOp, kshape: Shape, vshape: Shape) -> &Graph<NumOp> {
        let what = match op {
            LinearOp::Project(_) => "map",
            LinearOp::Filter(_) => "filter",
            LinearOp::EnterAt(_) => "enter_at",
            LinearOp::FlatMap(_) => "flatmap",
            LinearOp::Negate | LinearOp::LiftIter => unreachable!("{op:?} compiles no graph"),
        };
        if self.compiled.is_none() {
            let (k, v) = (&kshape, &vshape);
            let g = match op {
                LinearOp::Project(p) => compile_projection(&p.key, &p.val, k, v),
                LinearOp::Filter(cond) => compile_predicate(cond, k, v),
                LinearOp::EnterAt(field) => compile_scalar(field, k, v),
                LinearOp::FlatMap(list) => compile_flatmap(list, k, v),
                LinearOp::Negate | LinearOp::LiftIter => unreachable!(),
            };
            let g = g.unwrap_or_else(|e| panic!("{what}: type error at shapes ({kshape}, {vshape}): {e}"));
            self.compiled = Some((kshape.clone(), vshape.clone(), g));
        }
        let (k, v, g) = self.compiled.as_ref().unwrap();
        assert!(*k == kshape && *v == vshape, "{what}: a batch of shape ({kshape}, {vshape}) reached an operator pinned at ({k}, {v})");
        g
    }
}

/// Apply a `LinearOp` chain to one corgi container (the corgi-native compute per batch).
/// Project = corgi `eval_graph`; Filter = corgi mask + `gather`; FlatMap = `eval_graph` to a list
/// column + a structural explode; Negate = Rust. Every term is columnar — there is no row-wise
/// path inside the dataflow — and each op's graph is compiled once (`plans`). An empty batch
/// passes through untouched: it carries no shape to compile against and no rows to compute.
/// The two data<->time ops are columnar and total: EnterAt reads its delay field as a column and
/// joins it into `times` in place; LiftIter reads the iteration coordinate out of `times` and
/// appends it to `vals`. `level` is the scope depth (it locates that coordinate).
fn apply_ops(mut c: CC, ops: &[LinearOp], level: usize, plans: &mut [Plan]) -> CC {
    // A container with no rows has no SHAPE either, and the ops below are shape-directed: an
    // empty batch passes through untouched (every `LinearOp` maps zero rows to zero rows), and
    // nothing downstream reads its shape — `CorgiChunker::push_into` drops empty containers
    // before they reach `concat_blocks`, the one place shapes must agree. The check belongs at
    // the top of the LOOP, not before it: a filter can empty a batch mid-chain, and the next op
    // in the same `ops` slice must not compile against the erased container.
    for (op, plan) in ops.iter().zip(plans.iter_mut()) {
        if c.times.is_empty() {
            return c;
        }
        let (kshape, vshape) = (corgi::shape_of_value(&c.keys), corgi::shape_of_value(&c.vals));
        c = match op {
            LinearOp::Project(_) => {
                let g = plan.graph(op, kshape, vshape);
                let mut cols = corgi::eval_graph(g, CValue::Prod(vec![c.keys, c.vals])).into_prod("linear project").unwrap();
                let vals = cols.pop().unwrap();
                let keys = cols.pop().unwrap();
                CorgiContainer { keys, vals, times: c.times, diffs: c.diffs }
            }
            LinearOp::Filter(_) => {
                let g = plan.graph(op, kshape, vshape);
                let mask = corgi::eval_graph(g, CValue::Prod(vec![c.keys.clone(), c.vals.clone()]));
                let mask = mask.as_i64("filter mask").unwrap();
                let keep: Vec<usize> = (0..mask.len()).filter(|&i| mask[i] != 0).collect();
                let keys = gather(&c.keys, &keep);
                let vals = gather(&c.vals, &keep);
                let times = c.times.gather(&keep);
                let diffs = keep.iter().map(|&i| c.diffs[i]).collect();
                CorgiContainer { keys, vals, times, diffs }
            }
            LinearOp::Negate => {
                for d in c.diffs.iter_mut() {
                    *d = -*d;
                }
                c
            }
            LinearOp::EnterAt(_) => {
                let g = plan.graph(op, kshape, vshape);
                // The key and val columns are IDENTITY here — only times change. Evaluate the
                // delay field to an `Int` column and join it into each time in place. Joining
                // `Product(0, PointStamp([0,..,0, delay]))` is, coordinate-wise, `max` at index
                // `level-1` and identity everywhere else (u64's minimum is 0), so the delta
                // never has to be built. The epoch is lane 0, so PointStamp index `level-1` is
                // lane `level`, and the join is one lane-wise max.
                let raw = corgi::eval_graph(g, CValue::Prod(vec![c.keys.clone(), c.vals.clone()]))
                    .into_i64("enter_at delay")
                    .unwrap();
                let delays: Vec<u64> = raw.iter().map(|&r| 256 * (64 - (r as u64).leading_zeros() as u64)).collect();
                c.times.lane_max(level.saturating_sub(1) + 1, &delays);
                c
            }
            // The inverse of `EnterAt`: a value read OUT of each row's time. Vals gain one
            // integer field; keys, times and diffs are untouched, and no term is compiled.
            //
            // It mirrors [`append_iter`] shape for shape, and the empty product is where the two
            // representations part company: DDIR unit IS `Tuple([])`, which `append_iter` extends
            // to `Tuple([iter])`, but columnar it arrives as `CValue::Unit`, not an empty `Prod`.
            // So `Unit` must become `Prod([iter])` — `Prod([Unit, iter])` would be a silent
            // one-field-too-many divergence from `backend::vec`.
            LinearOp::LiftIter => {
                // PointStamp index `level-1` is lane `level`; at the root there is no iteration
                // coordinate and the value is zero.
                let iters: Vec<u64> = if level == 0 { vec![0; c.times.len()] } else { c.times.lane(level) };
                let lane = CValue::i64(iters.into_iter().map(|i| i as i64).collect());
                let vals = match c.vals {
                    CValue::Prod(mut fields) => { fields.push(lane); CValue::Prod(fields) }
                    CValue::Unit(_) => CValue::Prod(vec![lane]),
                    other => CValue::Prod(vec![other, lane]),
                };
                CorgiContainer { keys: c.keys, vals, times: c.times, diffs: c.diffs }
            }
            LinearOp::FlatMap(_) => {
                let g = plan.graph(op, kshape, vshape);
                // Structural explode: the evaluated list column's FLAT element storage already
                // IS the new value column, so the elements never move. Each row's span in the
                // bounds gives both the within-row position (DDIR's `$1[0]`) and a repeat map
                // carrying key/time/diff across. No per-row eval, no transcode.
                let (bounds, elems) =
                    corgi::eval_graph(g, CValue::Prod(vec![c.keys.clone(), c.vals])).into_list("flatmap list").unwrap();
                let ends: Vec<usize> = bounds.to_vec();
                let total = ends.last().copied().unwrap_or(0);
                let (mut reps, mut pos) = (Vec::with_capacity(total), Vec::with_capacity(total));
                let mut start = 0usize;
                for (row, end) in ends.into_iter().enumerate() {
                    for p in 0..(end - start) {
                        reps.push(row);
                        pos.push(p as i64);
                    }
                    start = end;
                }
                CorgiContainer {
                    keys: gather(&c.keys, &reps),
                    vals: CValue::Prod(vec![CValue::i64(pos), elems]),
                    times: c.times.gather(&reps),
                    diffs: reps.iter().map(|&r| c.diffs[r]).collect(),
                }
            }
        };
    }
    c
}

/// The corgi rendering substrate. An uninhabited type used only as a type-level tag: it
/// carries the [`Backend`] impl (a namespace of rendering functions selected by type) and is
/// never a value — rendering goes through `render_tree::<CorgiBackend>`. The empty enum (vs a
/// unit struct) makes constructing one impossible, signalling "type only". Mirrors `VecBackend`.
pub enum CorgiBackend {}

impl Backend for CorgiBackend {
    type Container = CC;
    type Arr<'scope> = Arranged<'scope, TraceAgent<CTrace>>;

    fn linear<'s>(c: Collection<'s, Time, CC>, ops: Vec<LinearOp>, level: usize, shape: Option<st::RowShape>) -> Collection<'s, Time, CC> {
        // Container-level: fold the LinearOp chain over each corgi batch (no inter-op transcode).
        // `level` is the scope depth (locates the iteration coordinate for LiftIter/EnterAt).
        // With the input's shape known, every op compiles now, at the shape it will see.
        let mut plans: Vec<Plan> = ops.iter().map(|_| Plan::default()).collect();
        if let Some(Ok(steps)) = shape.map(|(k, v)| crate::shapes::linear_shapes(&ops, k, v)) {
            for ((op, plan), (k, v)) in ops.iter().zip(plans.iter_mut()).zip(steps) {
                if !matches!(op, LinearOp::Negate | LinearOp::LiftIter) { plan.graph(op, k, v); }
            }
        }
        c.inner
            .unary(Pipeline, "CorgiLinear", move |_, _| {
                move |input, output| {
                    input.for_each(|cap, data| {
                        let mut out = apply_ops(std::mem::take(data), &ops, level, &mut plans);
                        output.session(&cap).give_container(&mut out);
                    });
                }
            })
            .as_collection()
    }

    fn arrange<'s>(c: Collection<'s, Time, CC>) -> Self::Arr<'s> {
        // This is the backend's ONE inter-worker data movement. `CorgiPact` partitions each
        // container by the structural hash of its key column and gathers each destination's rows
        // into fresh contiguous columns, so every update for a key reaches one worker and both
        // sides of a join agree which. Every other operator here is key-local given correctly
        // placed arrangements, which is why substituting this for `Pipeline` is the whole of
        // multi-worker support. At one worker timely's `Exchange` short-circuits to a direct
        // push, so the single-worker path costs nothing.
        //
        // Column-native ingest: `CorgiChunker` sort-consolidates each input `CorgiContainer`'s
        // columns straight into a `CorgiChunk` (no drain-to-rows), then the standard chunk batcher +
        // builder. No columns→rows→columns round-trip at the arrangement boundary.
        arrange_core::<_, CC, _, CTrace>(
            c.inner,
            CorgiPact,
            "CorgiArrange",
            ChunkBatcher::<CorgiChunker<Time, Diff>, _>::new,
        )
    }

    fn as_collection<'s>(a: Self::Arr<'s>) -> Collection<'s, Time, CC> {
        // Each chunk already IS a columnar container: its key/val columns clone by Arc bump,
        // so a chunk becomes a `CorgiContainer` for the price of copying its time lanes and
        // its diffs. One container per chunk — no concatenation, no gather, no
        // columns→rows→columns round-trip.
        a.stream
            .unary(Pipeline, "CorgiAsCollection", |_, _| {
                |input, output| {
                    input.for_each(|cap, data| {
                        let mut session = output.session(&cap);
                        for batch in data.iter() {
                            let Some(payload) = batch.inner.as_ref() else { continue };
                            for ch in payload.chunks.iter().filter(|c| c.len() > 0) {
                                let mut c = CorgiContainer {
                                    // Drop the arrangement's leading identifier lane: edges carry
                                    // the key the program wrote, so `$0` indexes what it always did.
                                    keys: recover_key(ch.keys()),
                                    vals: ch.vals().clone(),
                                    times: ch.times().clone(),
                                    diffs: ch.diffs().to_vec(),
                                };
                                session.give_container(&mut c);
                            }
                        }
                    });
                }
            })
            .as_collection()
    }

    fn join<'s>(l: Self::Arr<'s>, r: Self::Arr<'s>, projection: &Projection, shapes: Option<(st::RowShape, st::RowShape)>) -> Collection<'s, Time, CC> {
        // The proxy-join seam drives the backend blockwise under the driver's fuel; the backend
        // compiles the projection once (shape-directed, for `Spread`), now if both inputs' shapes
        // are known and otherwise on its first output, and emits corgi columns directly as
        // `CorgiContainer`s — column-native, no row round-trip.
        let shapes = shapes.map(|((k, v0), (_, v1))| (k, v0, v1));
        let tactic = ProxyJoinTactic::new(CorgiJoinBackend::new(projection.key.clone(), projection.val.clone(), shapes));
        join_with_tactic::<_, _, _, CC>(l, r, "Join", tactic).as_collection()
    }

    fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer) -> Self::Arr<'s> {
        // Amortize columnar corrections while reusing sweep scratch across wide presentations.
        let tactic = ProxyReduceTactic::new(CorgiReduceBackend::new(reducer.clone())).with_key_batch_size(256);
        reduce_with_tactic::<_, CTrace, _>(a, "CorgiReduce", tactic)
    }

    fn inspect<'s>(c: Collection<'s, Time, CC>, label: String) -> Collection<'s, Time, CC> {
        use std::fmt::Write;
        c.inner
            .unary(Pipeline, "CorgiInspect", move |_, _| {
                let mut line = String::new();
                move |input, output| {
                    input.for_each(|cap, data| {
                        let mut cont = std::mem::take(data);
                        for ((k, v), t, d) in cont.clone().into_updates() {
                            // Format before taking stderr's lock: nested Debug output
                            // otherwise writes many small fragments while holding it.
                            line.clear();
                            writeln!(&mut line, "  [{label}] (({k:?}, {v:?}), {t:?}, {d})").unwrap();
                            eprint!("{line}");
                        }
                        output.session(&cap).give_container(&mut cont);
                    });
                }
            })
            .as_collection()
    }

    fn leave_dynamic<'s>(c: Collection<'s, Time, CC>, level: usize) -> Collection<'s, Time, CC> {
        // Mirror DD's `Collection::leave_dynamic`, but over a `CorgiContainer`:
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
                // A message may carry several timestamps (a multi-element stamp): hold a
                // capability for each, truncated exactly as the rows are.
                let new_cap: timely::dataflow::operators::CapabilitySet<_> = cap
                    .stamp()
                    .iter()
                    .map(|t| {
                        let mut new_time = t.clone();
                        let mut v = std::mem::take(&mut new_time.inner).into_inner();
                        v.truncate(level - 1);
                        new_time.inner = PointStamp::new(v);
                        cap.delayed(&new_time, 0)
                    })
                    .collect();
                // `level - 1` PointStamp coordinates after the epoch are `level` lanes.
                data.times.truncate(level);
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
    shapes: Option<&crate::shapes::ScopeShapes>,
) -> Vec<Collection<'s, Time, CC>> {
    crate::backend::render_tree::<CorgiBackend>(s, scope, depth, imports, shapes)
}

/// Render `s` with the corgi substrate over ROW collections: each import converts to corgi
/// containers at the boundary (`ToCorgi`), the tree renders columnar, and each export
/// converts back (`FromCorgi`). Signature-compatible with
/// [`vec::render_tree`](crate::backend::vec::render_tree) (hence the `vec::Col` alias), so a
/// row-speaking driver switches backends by switching this one call.
pub fn render_tree_corgi<'s>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<crate::backend::vec::Col<'s>>,
    shapes: Option<&crate::shapes::ScopeShapes>,
) -> Vec<Collection<'s, Time, CC>> {
    let corgi_imports: Vec<Collection<'s, Time, CC>> = crate::backend::vec::check_import_shapes(s, imports)
        .into_iter()
        .zip(&s.imports)
        .enumerate()
        .map(|(k, (c, import))| {
            // The declared shape, or the one inferred at install (an imported trace's).
            let shape = import.shape.clone()
                .or_else(|| shapes.and_then(|sh| sh.imports[k].clone()).map(|c| (c.key, c.val)));
            c.inner
                .unary(Pipeline, "ToCorgi", move |_, _| {
                    // Known shapes describe empty lists and inactive sum lanes;
                    // imports without one retain first-row inference.
                    let mut pinned = shape;
                    move |input, output| {
                        input.for_each(|cap, data| {
                            let rows = std::mem::take(data);
                            let mut cc = match rows.first() {
                                None => CorgiContainer::default(),
                                Some(((k, v), _, _)) => {
                                    let (ks, vs) = pinned.get_or_insert_with(|| {
                                        let pin = |r: &Row, what: &str| shape_of_row(r).unwrap_or_else(|e| panic!("input {what}: {e}"));
                                        (pin(k, "key"), pin(v, "value"))
                                    });
                                    CorgiContainer::from_updates(rows, ks, vs)
                                }
                            };
                            output.session(&cap).give_container(&mut cc);
                        });
                    }
                })
                .as_collection()
        })
        .collect();
    render_tree(s, scope, depth, corgi_imports, shapes)
}

/// A columnar export: the program's corgi collection, left to the host time and arranged as
/// corgi chunks. Readers convert to rows only when they read ([`export_rows`]).
pub type ExportTrace = TraceAgent<differential_dataflow::trace::chunk::ChunkSpine<CorgiChunk<u64, Diff>>>;

/// Arrange an export (already at host time) as a columnar trace.
pub fn arrange_export<'s>(c: Collection<'s, u64, CorgiContainer<u64, Diff>>) -> Arranged<'s, ExportTrace> {
    arrange_core::<_, CorgiContainer<u64, Diff>, _, differential_dataflow::trace::chunk::ChunkSpine<CorgiChunk<u64, Diff>>>(
        c.inner,
        CorgiPact,
        "CorgiExport",
        ChunkBatcher::<CorgiChunker<u64, Diff>, _>::new,
    )
}

/// The batches an export's arrangement has produced on this worker, not yet taken.
pub type ExportTap = std::rc::Rc<std::cell::RefCell<Vec<<ExportTrace as differential_dataflow::trace::TraceReader>::Batch>>>;

/// The updates in `batches`, as rows.
pub fn batch_rows(batches: Vec<<ExportTrace as differential_dataflow::trace::TraceReader>::Batch>) -> Vec<((Row, Row), u64, Diff)> {
    let mut out = Vec::new();
    for batch in batches {
        for ch in batch.chunks.iter().filter(|c| c.len() > 0) {
            let c = CorgiContainer {
                keys: recover_key(ch.keys()),
                vals: ch.vals().clone(),
                times: ch.times().clone(),
                diffs: ch.diffs().to_vec(),
            };
            out.extend(c.into_updates());
        }
    }
    out
}

/// Decode and consume at most `rows_per_batch` export rows at a time.
/// Row payloads (including nested lists) can still vary in size.
pub fn for_each_batch_rows(
    batches: Vec<<ExportTrace as differential_dataflow::trace::TraceReader>::Batch>,
    rows_per_batch: usize,
    mut consume: impl FnMut(Vec<((Row, Row), u64, Diff)>),
) {
    assert!(rows_per_batch > 0, "export row batch size must be positive");
    let mut indices = Vec::new();
    for batch in batches {
        for ch in batch.chunks.iter().filter(|c| c.len() > 0) {
            let keys = recover_key(ch.keys());
            for start in (0..ch.len()).step_by(rows_per_batch) {
                let end = start.saturating_add(rows_per_batch).min(ch.len());
                indices.clear();
                indices.extend(start..end);
                let c = CorgiContainer {
                    keys: corgi::arrange::gather(&keys, &indices),
                    vals: corgi::arrange::gather(ch.vals(), &indices),
                    times: ch.times().gather(&indices),
                    diffs: ch.diffs()[start..end].to_vec(),
                };
                consume(c.into_updates());
            }
        }
    }
}

/// Record each batch the arrangement emits into `tap`, passing the stream through.
pub fn tap_export<'s>(a: &Arranged<'s, ExportTrace>, tap: ExportTap) -> timely::dataflow::Stream<'s, u64, Vec<differential_dataflow::trace::Span<u64, <ExportTrace as differential_dataflow::trace::TraceReader>::Batch>>> {
    a.stream.clone().unary(Pipeline, "ExportTap", move |_, _| {
        move |input, output| {
            input.for_each(|cap, data| {
                tap.borrow_mut().extend(data.iter().filter_map(|b| b.inner.clone()));
                output.session(&cap).give_container(data);
            });
        }
    })
}

/// The rows of an imported columnar export, as `((key, val), time, diff)` updates.
pub fn export_rows<'s>(
    a: Arranged<'s, ExportTrace>,
) -> timely::dataflow::Stream<'s, u64, Vec<((Row, Row), u64, Diff)>> {
    a.stream.unary(Pipeline, "ExportRows", |_, _| {
        |input, output| {
            input.for_each(|cap, data| {
                let mut session = output.session(&cap);
                for batch in data.iter() {
                    let Some(payload) = batch.inner.as_ref() else { continue };
                    for ch in payload.chunks.iter().filter(|c| c.len() > 0) {
                        let c = CorgiContainer {
                            keys: recover_key(ch.keys()),
                            vals: ch.vals().clone(),
                            times: ch.times().clone(),
                            diffs: ch.diffs().to_vec(),
                        };
                        session.give_container(&mut c.into_updates());
                    }
                }
            });
        }
    })
}

/// [`render_tree_corgi`] with each export converted back to rows (`FromCorgi`).
pub fn render_tree_rows<'s>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<crate::backend::vec::Col<'s>>,
    shapes: Option<&crate::shapes::ScopeShapes>,
) -> Vec<crate::backend::vec::Col<'s>> {
    render_tree_corgi(s, scope, depth, imports, shapes)
        .into_iter()
        .map(|c| {
            c.inner
                .unary(Pipeline, "FromCorgi", |_, _| {
                    |input, output| {
                        input.for_each(|cap, data| {
                            let mut rows = std::mem::take(data).into_updates();
                            output.session(&cap).give_container(&mut rows);
                        });
                    }
                })
                .as_collection()
        })
        .collect()
}
