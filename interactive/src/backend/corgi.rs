//! The corgi rendering substrate: corgi columns are the native representation on dataflow edges,
//! arrangements are chains of sorted columnar chunks (`ChunkSpine<CorgiChunk>`, cursor-less), and
//! scalar logic runs columnar via `eval_graph`.
//!
//! All `Backend` methods are corgi-native: `linear` folds a `LinearOp` chain over each container
//! ([`apply_ops`]); `arrange` ingests columns without
//! a row round-trip; `join`/`reduce` run through the int-proxy tactics ([`CorgiJoinBackend`],
//! [`CorgiReduceBackend`]) over the columnar chunks.

use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::progress::Antichain;

use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use differential_dataflow::collection::containers::Enter;
use differential_dataflow::operators::join::join_with_tactic;
use differential_dataflow::operators::reduce::reduce_with_tactic;
use differential_dataflow::trace::Span;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::wrappers::enter::{BatchEnter, TraceEnter};
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::chunk::{Chunk, ChunkBatcher};

use corgi::arrange::gather;
use corgi::Value as CValue;

use crate::backend::Backend;
use crate::corgi::chunk::{recover_key, CorgiChunk, CorgiChunker};
use crate::corgi::col_times::ColTime;
use crate::corgi::container::CorgiContainer;
use crate::corgi::exchange::CorgiPact;
use crate::corgi::half_join::{CorgiHalfJoin, HalfJoinBatcher};
use crate::corgi::join::CorgiJoinBackend;
use differential_dogs3::operators::half_join::half_join_with_tactic;
use crate::corgi::reduce::CorgiReduceBackend;
use differential_dataflow::operators::int_proxy::{BackendEnter, ProxyJoinTactic, ProxyReduceTactic};
use crate::corgi::logic::{compile_flatmap, compile_join_function, compile_predicate, compile_projection, compile_scalar};
use corgi::{Graph, NumOp, Shape};
use crate::ir::{Diff, Lambda, LinearOp, Reducer, Time};
use crate::scope_ir as st;

/// The corgi container on dataflow edges, and the columnar trace — the shorthands the `Backend`
/// methods below are written in terms of.
type CC = CorgiContainer<Time, Diff>;
type CTrace = differential_dataflow::trace::chunk::ChunkSpine<CorgiChunk<Time, Diff>>;

/// The compiled form of one `LinearOp`, at the shapes of the batches it reads, which install
/// inferred. A chain compiles once, when the operator is built; every batch reuses the graph, and
/// a batch of other shapes is the invariant violation, not a recompile.
pub struct Plan {
    shape: (Shape, Shape),
    /// The op's graph; `None` for the ops that compile none.
    graph: Option<Graph<NumOp>>,
}

impl Plan {
    /// `op` compiled at the shapes `kshape` and `vshape` it reads. A type error is a panic with
    /// corgi's message: install type-checked the program, so it is not reached. `level` is the
    /// op's scope depth, which fixes the width of a time as data.
    fn new(op: &LinearOp, kshape: Shape, vshape: Shape, level: usize) -> Plan {
        let (k, v) = (&kshape, &vshape);
        let (what, graph) = match op {
            LinearOp::Project(p) => ("map", Some(compile_projection(p, &[kshape.clone(), vshape.clone()]))),
            LinearOp::Filter(cond) => ("filter", Some(compile_predicate(cond, k, v))),
            LinearOp::EnterAt(field) => ("enter_at", Some(compile_scalar(field, k, v))),
            LinearOp::FlatMap(list) => ("flatmap", Some(compile_flatmap(list, k, v))),
            LinearOp::JoinFunction(lambda) => ("join_function", Some(compile_join_function(lambda, k, v, level + 1))),
            LinearOp::Negate | LinearOp::LiftIter | LinearOp::Lift => ("", None),
        };
        let graph = graph.map(|g| g.unwrap_or_else(|e| panic!("{what}: type error at shapes ({kshape}, {vshape}): {e}")));
        Plan { shape: (kshape, vshape), graph }
    }

    /// The graph, for a batch of these shapes.
    fn graph(&self, kshape: &Shape, vshape: &Shape) -> &Graph<NumOp> {
        let (k, v) = &self.shape;
        assert!(k == kshape && v == vshape, "a batch of shape ({kshape}, {vshape}) reached an operator compiled at ({k}, {v})");
        self.graph.as_ref().expect("an op that compiles a graph")
    }
}

/// Apply a `LinearOp` chain to one corgi container (the corgi-native compute per batch).
/// Project = corgi `eval_graph`; Filter = corgi mask + `gather`; FlatMap = `eval_graph` to a list
/// column + a structural explode; Negate = Rust. Every slot is columnar — there is no row-wise
/// path inside the dataflow — and each op's graph was compiled when the operator was built
/// (`plans`). An empty batch passes through untouched: it has no rows to compute.
/// The data<->time ops are columnar and total: EnterAt reads its delay field as a column and
/// joins it into `times` in place; LiftIter reads the iteration coordinate out of `times` and
/// appends it to `vals`; Lift reads every lane of `times` into `vals`; JoinFunction explodes
/// its list column as FlatMap does and joins each element's time lanes into `times`. `level`
/// is the scope depth (it locates that coordinate, and fixes the number of lanes as data).
fn apply_ops(mut c: CC, ops: &[LinearOp], level: usize, plans: &[Plan]) -> CC {
    // A container with no rows may have no SHAPE either, and each op below checks its batch's
    // shapes: an empty batch passes through untouched (every `LinearOp` maps zero rows to zero
    // rows), and nothing downstream reads its shape — `CorgiChunker::push_into` drops empty
    // containers before they reach `concat_blocks`, the one place shapes must agree. The check
    // belongs at the top of the LOOP, not before it: a filter can empty a batch mid-chain, and
    // the next op in the same `ops` slice must not check the erased container.
    for (op, plan) in ops.iter().zip(plans) {
        if c.times.is_empty() {
            return c;
        }
        let (kshape, vshape) = (corgi::shape_of_value(&c.keys), corgi::shape_of_value(&c.vals));
        c = match op {
            LinearOp::Project(_) => {
                let g = plan.graph(&kshape, &vshape);
                let mut cols = corgi::eval_graph(g, CValue::Prod(vec![c.keys, c.vals])).into_prod("linear project").unwrap();
                let vals = cols.pop().unwrap();
                let keys = cols.pop().unwrap();
                CorgiContainer { keys, vals, times: c.times, diffs: c.diffs }
            }
            LinearOp::Filter(_) => {
                let g = plan.graph(&kshape, &vshape);
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
                let g = plan.graph(&kshape, &vshape);
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
            // integer field; keys, times and diffs are untouched, and no slot is compiled.
            //
            // A tuple gains a field, and unit is the empty tuple: columnar it arrives as
            // `CValue::Unit`, not an empty `Prod`, so `Unit` must become `Prod([iter])`.
            // `Prod([Unit, iter])` would be a silent one-field-too-many divergence from the
            // shape `shapes` infers.
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
            // A time as data is every lane, the epoch and one per scope: `level + 1` lanes, zero
            // where the column is narrower. Keys, times and diffs are untouched.
            LinearOp::Lift => {
                let lanes = (0..=level).map(|j| CValue::i64(c.times.lane(j).into_iter().map(|x| x as i64).collect())).collect();
                let vals = CValue::Prod(vec![c.vals, CValue::Prod(lanes)]);
                CorgiContainer { keys: c.keys, vals, times: c.times, diffs: c.diffs }
            }
            LinearOp::JoinFunction(_) => {
                let g = plan.graph(&kshape, &vshape);
                // The explode is FlatMap's: the list's flat element storage holds the new key,
                // val, time and diff columns, and each row's span in the bounds is a repeat map
                // carrying its time and diff across. Each time lane then joins in by `lane_max`,
                // a negative coordinate clamping to zero, the minimum, which the max leaves alone.
                let (bounds, elems) =
                    corgi::eval_graph(g, CValue::Prod(vec![c.keys, c.vals])).into_list("join_function list").unwrap();
                let mut elem = elems.into_prod("join_function element").unwrap();
                let d2 = elem.pop().unwrap().into_i64("join_function diff").unwrap();
                let lanes = elem.pop().unwrap().into_prod("join_function time").unwrap();
                let mut row = elem.pop().unwrap().into_prod("join_function row").unwrap();
                let vals = row.pop().unwrap();
                let keys = row.pop().unwrap();
                let mut reps = Vec::with_capacity(d2.len());
                let mut start = 0usize;
                for (r, &end) in bounds.to_vec().iter().enumerate() {
                    reps.extend(std::iter::repeat_n(r, end - start));
                    start = end;
                }
                let mut times = c.times.gather(&reps);
                for (j, lane) in lanes.into_iter().enumerate() {
                    let lane: Vec<u64> = lane.into_i64("join_function time lane").unwrap().into_iter().map(|x| x.max(0) as u64).collect();
                    times.lane_max(j, &lane);
                }
                let diffs = reps.iter().zip(d2).map(|(&r, d)| c.diffs[r] * d).collect();
                CorgiContainer { keys, vals, times, diffs }
            }
            LinearOp::FlatMap(_) => {
                let g = plan.graph(&kshape, &vshape);
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

/// A batch of a published (host-time) corgi export (`ExportTrace`'s batch).
type HostBatch = std::rc::Rc<ChunkBatch<CorgiChunk<u64, Diff>>>;
/// A published corgi export entered into a program's scope: the producer's own chunks, which hold
/// host epochs. Operators read them through DD's backend wrappers (`BackendEnter`), and the half
/// join through [`ArrangedBatch`](crate::corgi::half_join::ArrangedBatch).
pub type ImportArr<'scope> = Arranged<'scope, TraceEnter<ExportTrace, Time>>;

/// The corgi backend's arrangement: one it built, or a published export it imported.
#[derive(Clone)]
pub enum Arr<'scope> {
    Local(Arranged<'scope, TraceAgent<CTrace>>),
    Imported(ImportArr<'scope>),
}

/// The time each coordinate of `t` less one, at least zero: the half join's compaction of its
/// arrangement. Every later stream time is at or beyond some such frontier element, and advancing
/// arranged times to it keeps their order against those stream times, strict and not.
fn step_back(t: &Time, out: &mut Antichain<Time>) {
    use differential_dataflow::dynamic::pointstamp::PointStamp;
    let inner = PointStamp::new(t.inner.iter().map(|x| x.saturating_sub(1)).collect());
    out.insert(timely::order::Product::new(t.outer.saturating_sub(1), inner));
}

/// A chunk as a corgi container. Each chunk already IS a columnar container: its key/val columns
/// clone by Arc bump, so a chunk becomes a `CorgiContainer` for the price of copying its time
/// lanes and its diffs, with no gather and no columns→rows→columns round-trip.
fn chunk_container<T: ColTime>(chunk: &CorgiChunk<T, Diff>) -> CorgiContainer<T, Diff> {
    CorgiContainer {
        // Drop the arrangement's leading identifier lane: edges carry the key the program wrote,
        // so `$0` indexes what it always did.
        keys: recover_key(chunk.keys()),
        vals: chunk.vals().clone(),
        times: chunk.times().clone(),
        diffs: chunk.diffs().to_vec(),
    }
}

/// The updates of a stream of columnar batches, as corgi containers, one per non-empty chunk:
/// `containers` reads a batch's chunks at the stream's times.
fn chunk_containers<'s, T: ColTime, B: Clone + 'static>(
    stream: timely::dataflow::Stream<'s, T, Vec<Span<T, B>>>,
    name: &str,
    containers: impl Fn(&B, &mut dyn FnMut(CorgiContainer<T, Diff>)) + 'static,
) -> Collection<'s, T, CorgiContainer<T, Diff>> {
    stream
        .unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for batch in data.iter() {
                        let Some(payload) = batch.inner.as_ref() else { continue };
                        containers(payload, &mut |mut c| session.give_container(&mut c));
                    }
                });
            }
        })
        .as_collection()
}

/// A batch's non-empty chunks, as containers.
fn containers_of<T: ColTime>(batch: &std::rc::Rc<ChunkBatch<CorgiChunk<T, Diff>>>, give: &mut dyn FnMut(CorgiContainer<T, Diff>)) {
    for chunk in batch.chunks.iter().filter(|c| c.len() > 0) { give(chunk_container(chunk)); }
}

/// The corgi rendering substrate. An uninhabited type used only as a type-level tag: it
/// carries the [`Backend`] impl (a namespace of rendering functions selected by type) and is
/// never a value — rendering goes through `render_tree::<CorgiBackend>`. The empty enum (vs a
/// unit struct) makes constructing one impossible, signalling "type only". Mirrors `VecBackend`.
pub enum CorgiBackend {}

impl Backend for CorgiBackend {
    type Container = CC;
    type Arr<'scope> = Arr<'scope>;

    fn linear<'s>(c: Collection<'s, Time, CC>, ops: Vec<LinearOp>, level: usize, (k, v): st::RowShape) -> Collection<'s, Time, CC> {
        // Container-level: fold the LinearOp chain over each corgi batch (no inter-op transcode).
        // `level` is the scope depth (locates the iteration coordinate for LiftIter/EnterAt).
        // Every op compiles now, at the shape it will see.
        let steps = crate::shapes::linear_shapes(&ops, k, v, level).expect("install inferred these shapes");
        let plans: Vec<Plan> = ops.iter().zip(steps).map(|(op, (key, val))| Plan::new(op, key, val, level)).collect();
        c.inner
            .unary(Pipeline, "CorgiLinear", move |_, _| {
                move |input, output| {
                    input.for_each(|cap, data| {
                        let mut out = apply_ops(std::mem::take(data), &ops, level, &plans);
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
        Arr::Local(arrange_core::<_, CC, _, CTrace>(
            c.inner,
            CorgiPact,
            "CorgiArrange",
            ChunkBatcher::<CorgiChunker<Time, Diff>, _>::new,
        ))
    }

    fn as_collection<'s>(a: Self::Arr<'s>) -> Collection<'s, Time, CC> {
        match a {
            Arr::Local(a) => chunk_containers(a.stream, "CorgiAsCollection", containers_of),
            // An import's containers hold host epochs, which enter the program's scope.
            Arr::Imported(a) => chunk_containers(a.stream, "CorgiAsCollection", |batch: &BatchEnter<HostBatch, Time>, give| {
                containers_of(batch.inner(), &mut |c| give(Enter::<u64, Time>::enter(c)));
            }),
        }
    }

    fn join<'s>(l: Self::Arr<'s>, r: Self::Arr<'s>, projection: &Lambda, ((k, v0), (_, v1)): (st::RowShape, st::RowShape)) -> Collection<'s, Time, CC> {
        // The proxy-join seam drives the backend blockwise under the driver's fuel; the backend
        // compiles the projection once, now, at both inputs' shapes, and emits corgi columns
        // directly as `CorgiContainer`s — column-native, no row round-trip.
        let backend = CorgiJoinBackend::new(projection, [k, v0, v1]);
        // An imported input's batches hold host epochs: `BackendEnter` hands the backend the
        // batches as they are, and enters the times it draws into the program's scope.
        match (l, r) {
            (Arr::Local(l), Arr::Local(r)) => join_with_tactic::<_, _, _, CC>(l, r, "Join", ProxyJoinTactic::new(backend)).as_collection(),
            (Arr::Local(l), Arr::Imported(r)) => {
                let backend = BackendEnter::<_, u64, _>::right(backend);
                join_with_tactic::<_, _, _, CC>(l, r, "Join", ProxyJoinTactic::new(backend)).as_collection()
            }
            (Arr::Imported(l), Arr::Local(r)) => {
                let backend = BackendEnter::<_, u64>::new(backend);
                join_with_tactic::<_, _, _, CC>(l, r, "Join", ProxyJoinTactic::new(backend)).as_collection()
            }
            (Arr::Imported(l), Arr::Imported(r)) => {
                let backend = BackendEnter::<_, u64>::new(BackendEnter::<_, u64, _>::right(backend));
                join_with_tactic::<_, _, _, CC>(l, r, "Join", ProxyJoinTactic::new(backend)).as_collection()
            }
        }
    }

    fn half_join<'s>(stream: Collection<'s, Time, CC>, arrangement: Self::Arr<'s>, strict: bool, projection: &Lambda, level: usize, ((k, v0), (_, v1)): (st::RowShape, st::RowShape)) -> Collection<'s, Time, CC> {
        // dogs3's driver owns progress and yielding; corgi supplies when an update is unblocked
        // (`HalfJoinBatcher`) and how released updates meet the arrangement (`CorgiHalfJoin`).
        // `CorgiPact` places each update on the worker holding its key, as `arrange` placed the rows.
        // The stream's value is `(val, time)`, as `lift` makes it; the projection reads the val.
        let Shape::Prod(mut v0) = v0 else { panic!("half_join: a stream value of shape {v0}, not (val, time)") };
        let half_join = CorgiHalfJoin::new(projection, strict, level + 1, [k, v0.swap_remove(0), v1]);
        let yielding = |_timer: std::time::Instant, count: usize| count > 1_000_000;
        match arrangement {
            Arr::Local(a) => half_join_with_tactic(stream.inner, a, CorgiPact, step_back, yielding, HalfJoinBatcher::new(strict, level + 1), half_join).as_collection(),
            Arr::Imported(a) => half_join_with_tactic(stream.inner, a, CorgiPact, step_back, yielding, HalfJoinBatcher::new(strict, level + 1), half_join).as_collection(),
        }
    }
    fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer, shape: st::RowShape) -> Self::Arr<'s> {
        // Amortize columnar corrections while reusing sweep scratch across wide presentations.
        let backend = CorgiReduceBackend::new(reducer.clone(), shape);
        Arr::Local(match a {
            Arr::Local(a) => reduce_with_tactic::<_, CTrace, _>(a, "CorgiReduce", ProxyReduceTactic::new(backend).with_key_batch_size(256)),
            // An imported input's times enter the program's scope as the backend draws them.
            Arr::Imported(a) => {
                let backend = BackendEnter::<_, u64>::new(backend);
                reduce_with_tactic::<_, CTrace, _>(a, "CorgiReduce", ProxyReduceTactic::new(backend).with_key_batch_size(256))
            }
        })
    }

    fn inspect<'s>(c: Collection<'s, Time, CC>, label: String) -> Collection<'s, Time, CC> {
        use std::fmt::Write;
        c.inner
            .unary(Pipeline, "CorgiInspect", move |_, _| {
                let mut line = String::new();
                move |input, output| {
                    input.for_each(|cap, data| {
                        let mut cont = std::mem::take(data);
                        if !cont.times.is_empty() {
                            // The batch's columns, as corgi shows them. Formatted before taking
                            // stderr's lock: nested Debug output otherwise writes many small
                            // fragments while holding it.
                            line.clear();
                            writeln!(&mut line, "  [{label}] keys={:?} vals={:?} times={:?} diffs={:?}", cont.keys, cont.vals, cont.times.to_vec(), cont.diffs).unwrap();
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
    fn enter_region<'s, 'r>(a: Self::Arr<'s>, region: Scope<'r, Time>) -> Self::Arr<'r> {
        match a {
            Arr::Local(a) => Arr::Local(a.enter_region(region)),
            Arr::Imported(a) => Arr::Imported(a.enter_region(region)),
        }
    }
}

/// A root import for [`render_tree_corgi_arranged`]: corgi containers, or a published columnar
/// export.
#[allow(clippy::large_enum_variant)] // One per import, consumed as the program renders.
pub enum CorgiImport<'s> {
    Columns(Collection<'s, Time, CC>),
    Export(ImportArr<'s>),
}

/// Render `s` with the corgi substrate ([`crate::backend::render_tree`]) at the `shapes` install
/// inferred. A published columnar export arrives as the producer's arrangement, which the
/// program's `arrange`s, joins, and reduces of it read in place. Shape ascriptions are the
/// driver's to check, before it renders.
pub fn render_tree_corgi_arranged<'s>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<CorgiImport<'s>>,
    shapes: &crate::shapes::ScopeShapes,
) -> Vec<Collection<'s, Time, CC>> {
    assert_eq!(s.imports.len(), imports.len());
    let imports = imports.into_iter().map(|imp| match imp {
        CorgiImport::Columns(c) => crate::backend::Rendered::Collection(c),
        CorgiImport::Export(a) => crate::backend::Rendered::Arrangement(Arr::Imported(a)),
    }).collect();
    crate::backend::render_tree::<CorgiBackend>(s, scope, depth, imports, shapes)
}

/// A columnar export: the program's corgi collection, left to the host time and arranged as
/// corgi chunks.
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

/// The updates in `batches`, one container per non-empty chunk. A chunk's key and value columns
/// are shared with the container, not copied.
pub fn batch_containers(batches: Vec<<ExportTrace as differential_dataflow::trace::TraceReader>::Batch>) -> Vec<CorgiContainer<u64, Diff>> {
    batches.iter().flat_map(|batch| batch.chunks.iter().filter(|c| c.len() > 0)).map(|ch| CorgiContainer {
        keys: recover_key(ch.keys()),
        vals: ch.vals().clone(),
        times: ch.times().clone(),
        diffs: ch.diffs().to_vec(),
    }).collect()
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

/// The updates of an imported columnar export, as corgi containers.
pub fn export_containers<'s>(a: Arranged<'s, ExportTrace>) -> Collection<'s, u64, CorgiContainer<u64, Diff>> {
    chunk_containers(a.stream, "ExportContainers", containers_of)
}
