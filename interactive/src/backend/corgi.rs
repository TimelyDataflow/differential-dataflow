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
use timely::progress::{Antichain, Timestamp};

use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use differential_dataflow::operators::join::join_with_tactic;
use differential_dataflow::operators::reduce::reduce_with_tactic;
use differential_dataflow::trace::Span;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::operators::arrange::arrangement::arrange_core;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::chunk::{Chunk, ChunkBatcher};

use corgi::arrange::gather;
use corgi::Value as CValue;

use crate::backend::Backend;
use crate::runtime::chunk::{recover_key, CorgiChunk, CorgiChunker};
use crate::runtime::col_times::ColTime;
use crate::runtime::container::CorgiContainer;
use crate::runtime::exchange::CorgiPact;
use crate::runtime::half_join::{CorgiHalfJoinBackend, HalfJoinBatcher};
use crate::runtime::join::CorgiJoinBackend;
use differential_dogs3::operators::half_join::half_join_with_tactic;
use crate::runtime::reduce::CorgiReduceBackend;
use differential_dataflow::operators::int_proxy::{BackendFrontier, ProxyJoinTactic, ProxyReduceTactic};
use differential_dogs3::operators::half_join::proxy::ProxyHalfJoinTactic;
use crate::lambda::{compile_flatmap, compile_join_function, compile_predicate, compile_projection, compile_scalar};
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
pub(crate) struct Plan {
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

/// The corgi backend's arrangement: one a program built, or a published export it imported.
/// Both are the same trace type at the same time, so every operator reads either in place.
///
/// An import is read as of `since`, the time its reader started: a published trace has compacted
/// to that time, from which on its accumulations are correct, and before which its batches can
/// hold counts no time had. Every reader advances the times it draws to `since`: a collection as
/// it flattens the chunks, a join, reduce or half join through DD's `BackendFrontier`. An
/// arrangement the program built is read as it is, which `since` at the minimum says.
///
/// One time, not a frontier: the server compacts every published trace by the one-element
/// frontier `{(epoch - 1, [])}` and a reader starts at that same element, so the frontier a
/// reader reads as of is `{since}`, which is beyond the frontier its trace compacted by, as DD's
/// `BackendFrontier` requires.
#[derive(Clone)]
pub(crate) struct Arr<'scope> {
    pub(crate) arranged: Arranged<'scope, TraceAgent<CTrace>>,
    pub(crate) since: Time,
}

impl<'scope> Arr<'scope> {
    /// An arrangement the program built, read as it is.
    fn built(arranged: Arranged<'scope, TraceAgent<CTrace>>) -> Self {
        Arr { arranged, since: Time::minimum() }
    }
}

/// The time each coordinate of `t` less one, at least zero: the half join's compaction of its
/// arrangement. Every later stream time is at or beyond some such frontier element, and advancing
/// arranged times to it keeps their order against those stream times, strict and not.
fn step_back(t: &Time, out: &mut Antichain<Time>) {
    use differential_dataflow::dynamic::pointstamp::PointStamp;
    let inner = PointStamp::new(t.inner.iter().map(|x| x.saturating_sub(1)).collect());
    out.insert(timely::order::Product::new(t.outer.saturating_sub(1), inner));
}

/// The updates of a stream of columnar batches, as corgi containers, read as of `since`.
///
/// Each chunk already IS a columnar container: its key/val columns clone by Arc bump,
/// so a chunk becomes a `CorgiContainer` for the price of copying its time lanes and
/// its diffs. One container per chunk — no concatenation, no gather, no
/// columns→rows→columns round-trip. The copied time lanes are advanced to `since` in
/// place, one lane-wise max; a container's rows may then coincide, which the arrange
/// that reads them consolidates.
fn chunk_containers<'s, T: ColTime>(
    stream: timely::dataflow::Stream<'s, T, Vec<Span<T, std::rc::Rc<ChunkBatch<CorgiChunk<T, Diff>>>>>>,
    name: &str,
    since: T,
) -> Collection<'s, T, CorgiContainer<T, Diff>> {
    let since = (since != T::minimum()).then(|| Antichain::from_elem(since));
    stream
        .unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for batch in data.iter() {
                        let Some(payload) = batch.inner.clone() else { continue };
                        for ch in payload.chunks.iter().filter(|c| c.len() > 0) {
                            let mut times = ch.times().clone();
                            if let Some(since) = &since {
                                times.advance_by(since.borrow(), times.len());
                            }
                            let mut c = CorgiContainer {
                                // Drop the arrangement's leading identifier lane: edges carry
                                // the key the program wrote, so `$0` indexes what it always did.
                                keys: recover_key(ch.keys()),
                                vals: ch.vals().clone(),
                                times,
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

/// The corgi rendering substrate. An uninhabited type used only as a type-level tag: it
/// carries the [`Backend`] impl (a namespace of rendering functions selected by type) and is
/// never a value — rendering goes through `render_tree::<CorgiBackend>`. The empty enum (vs a
/// unit struct) makes constructing one impossible, signalling "type only".
pub(crate) enum CorgiBackend {}

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
        Arr::built(arrange_core::<_, CC, _, CTrace>(
            c.inner,
            CorgiPact,
            "CorgiArrange",
            ChunkBatcher::<CorgiChunker<Time, Diff>, _>::new,
        ))
    }

    fn as_collection<'s>(a: Self::Arr<'s>) -> Collection<'s, Time, CC> {
        chunk_containers(a.arranged.stream, "CorgiAsCollection", a.since)
    }

    fn join<'s>(l: Self::Arr<'s>, r: Self::Arr<'s>, projection: &Lambda, ((k, v0), (_, v1)): (st::RowShape, st::RowShape)) -> Collection<'s, Time, CC> {
        // The proxy-join seam drives the backend blockwise under the driver's fuel; the backend
        // compiles the projection once, now, at both inputs' shapes, and emits corgi columns
        // directly as `CorgiContainer`s — column-native, no row round-trip.
        let backend = CorgiJoinBackend::new(projection, [k, v0, v1]);
        // An import is read as of its time. Reading one input as of a time `s` or both gives the
        // same output when the other's times are at or beyond `s` (`(t0 ∨ s) ∨ t1 = t0 ∨ t1`
        // then), as every program time is; two imports share one time. So the join reads both
        // inputs as of the join of the two times, a one-element frontier as `Arr` says.
        // Arrangements the program built have `since` at the minimum, where the wrapper returns
        // after one comparison per unit, so every join is wrapped and compiles to one operator.
        let since = differential_dataflow::lattice::Lattice::join(&l.since, &r.since);
        let backend = BackendFrontier::new(backend, Antichain::from_elem(since));
        join_with_tactic::<_, _, _, CC>(l.arranged, r.arranged, "Join", ProxyJoinTactic::new(backend)).as_collection()
    }

    fn half_join<'s>(stream: Collection<'s, Time, CC>, arrangement: Self::Arr<'s>, strict: bool, projection: &Lambda, level: usize, ((k, v0), (_, v1)): (st::RowShape, st::RowShape)) -> Collection<'s, Time, CC> {
        // dogs3's driver owns progress and yielding; corgi supplies when an update is unblocked
        // (`HalfJoinBatcher`) and the data of released updates and arranged rows
        // (`CorgiHalfJoinBackend`); dogs3's proxy half-join tactic decides which pairs meet.
        // `CorgiPact` places each update on the worker holding its key, as `arrange` placed the rows.
        // The stream's value is `(val, time)`, as `lift` makes it; the projection reads the val.
        //
        // An imported arrangement is read as of `since`, as a join's or reduce's is: the import's
        // rows also reach the program as a stream read as of `since` (through `as_collection`),
        // and a delta join meets each pair exactly once only if both paths compare the same times.
        // The wrapper advances every arranged time before `since`, in every unit, since the total
        // order the half join compares in is not preserved by advancing even against later stream
        // times; what is at or beyond `since` costs one comparison per row, and an arrangement the
        // program built has `since` at the minimum, so every half join is wrapped and compiles to
        // one operator.
        let Shape::Prod(mut v0) = v0 else { panic!("half_join: a stream value of shape {v0}, not (val, time)") };
        let Arr { arranged, since } = arrangement;
        let backend = CorgiHalfJoinBackend::new(projection, level + 1, [k, v0.swap_remove(0), v1]);
        let backend = BackendFrontier::new(backend, Antichain::from_elem(since));
        let yielding = |_timer: std::time::Instant, count: usize| count > 1_000_000;
        half_join_with_tactic(stream.inner, arranged, CorgiPact, step_back, yielding, HalfJoinBatcher::new(strict, level + 1), ProxyHalfJoinTactic::new(backend, strict)).as_collection()
    }
    fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer, shape: st::RowShape) -> Self::Arr<'s> {
        // Amortize columnar corrections while reusing sweep scratch across wide presentations.
        let backend = CorgiReduceBackend::new(reducer.clone(), shape);
        // An import is read as of its time; the output is the program's own, read as it is.
        // As for the join, the wrapper at the minimum is one comparison per unit.
        let Arr { arranged, since } = a;
        let backend = BackendFrontier::new(backend, Antichain::from_elem(since));
        Arr::built(reduce_with_tactic::<_, CTrace, _>(arranged, "CorgiReduce", ProxyReduceTactic::new(backend).with_key_batch_size(256)))
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
        Arr { arranged: a.arranged.enter_region(region), since: a.since }
    }
}

/// A root import for [`render_program`]: corgi containers, or a published export read as of the
/// time the program starts.
#[allow(clippy::large_enum_variant)] // One per import, consumed as the program renders.
pub(crate) enum CorgiImport<'s> {
    Columns(Collection<'s, Time, CC>),
    Export(Arr<'s>),
}

/// Render the root scope `s` with the corgi substrate ([`crate::backend::render_scope`]) at the
/// `shapes` install inferred, and arrange each export. A published export arrives as its
/// producer's arrangement, which the program's `arrange`s, joins, and reduces read in place; an
/// export that is already an arrangement is returned as it is. Shape ascriptions are the
/// driver's to check, before it renders.
pub(crate) fn render_program<'s>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    imports: Vec<CorgiImport<'s>>,
    shapes: &crate::shapes::ScopeShapes,
) -> Vec<Arranged<'s, ExportTrace>> {
    assert_eq!(s.imports.len(), imports.len());
    let imports = imports.into_iter().map(|imp| match imp {
        CorgiImport::Columns(c) => crate::backend::Rendered::Collection(c),
        CorgiImport::Export(a) => crate::backend::Rendered::Arrangement(a),
    }).collect();
    let rendered = crate::backend::render_scope::<CorgiBackend>(s, scope, 0, imports, shapes);
    // An export that is not already an arrangement is arranged here, after its statement has
    // rendered: label those operators with the export, for diagnostics.
    let logger = scope.worker().logger_for::<crate::backend::LabelEventBuilder>("ddir/labels");
    let arranged = rendered.iter().zip(&s.exports).map(|(r, export)| {
        let first = scope.worker().peek_identifier();
        // A re-exported import is the producer's arrangement; its own readers import it as of
        // the times they start.
        let arranged = r.arrange().arranged;
        if let Some(logger) = &logger {
            let end = scope.worker().peek_identifier();
            logger.log(crate::backend::LabelEvent { first, end, label: format!("export {}", export.name) });
        }
        arranged
    }).collect();
    if let Some(logger) = &logger { logger.flush(); }
    arranged
}

/// A published export: a trace of corgi chunks at the program time, as the program built it.
pub(crate) type ExportTrace = TraceAgent<CTrace>;

/// Arrange a collection as a published trace.
pub(crate) fn arrange_export<'s>(c: Collection<'s, Time, CC>) -> Arranged<'s, ExportTrace> {
    CorgiBackend::arrange(c).arranged
}

/// The batches an export's arrangement has produced on this worker, not yet taken.
pub(crate) type ExportTap = std::rc::Rc<std::cell::RefCell<Vec<<ExportTrace as differential_dataflow::trace::TraceReader>::Batch>>>;

/// The updates in `batches`, one container per non-empty chunk. A chunk's key and value columns
/// are shared with the container, not copied.
pub(crate) fn batch_containers(batches: Vec<<ExportTrace as differential_dataflow::trace::TraceReader>::Batch>) -> Vec<CC> {
    batches.iter().flat_map(|batch| batch.chunks.iter().filter(|c| c.len() > 0)).map(|ch| CorgiContainer {
        keys: recover_key(ch.keys()),
        vals: ch.vals().clone(),
        times: ch.times().clone(),
        diffs: ch.diffs().to_vec(),
    }).collect()
}

/// Record each batch the arrangement emits into `tap`, passing the stream through.
pub(crate) fn tap_export<'s>(a: &Arranged<'s, ExportTrace>, tap: ExportTap) -> timely::dataflow::Stream<'s, Time, Vec<Span<Time, <ExportTrace as differential_dataflow::trace::TraceReader>::Batch>>> {
    a.stream.clone().unary(Pipeline, "ExportTap", move |_, _| {
        move |input, output| {
            input.for_each(|cap, data| {
                tap.borrow_mut().extend(data.iter().filter_map(|b| b.inner.clone()));
                output.session(&cap).give_container(data);
            });
        }
    })
}

/// The updates of an imported columnar export, as corgi containers, read as of `since` (see
/// [`Arr`]).
pub(crate) fn export_containers<'s>(a: Arranged<'s, ExportTrace>, since: Time) -> Collection<'s, Time, CC> {
    chunk_containers(a.stream, "ExportContainers", since)
}
