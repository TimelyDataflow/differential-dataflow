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
use timely::progress::Antichain;

use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use differential_dataflow::operators::join::{Fresh, JoinTactic, join_with_tactic};
use differential_dataflow::operators::reduce::{ReduceTactic, reduce_with_tactic};
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
use differential_dogs3::operators::half_join::{half_join_with_tactic, HalfJoinTactic};
use crate::corgi::reduce::CorgiReduceBackend;
use differential_dataflow::operators::int_proxy::{ProxyJoinTactic, ProxyReduceTactic};
use crate::corgi::logic::{compile_flatmap, compile_join_function, compile_predicate, compile_projection, compile_scalar, shape_of_row};
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
    /// is a panic with corgi's message: a program that typechecks never reaches it. `level` is
    /// the op's scope depth, which fixes the width of a time as data.
    fn graph(&mut self, op: &LinearOp, kshape: Shape, vshape: Shape, level: usize) -> &Graph<NumOp> {
        let what = match op {
            LinearOp::Project(_) => "map",
            LinearOp::Filter(_) => "filter",
            LinearOp::EnterAt(_) => "enter_at",
            LinearOp::FlatMap(_) => "flatmap",
            LinearOp::JoinFunction(_) => "join_function",
            LinearOp::Negate | LinearOp::LiftIter | LinearOp::Lift => unreachable!("{op:?} compiles no graph"),
        };
        if self.compiled.is_none() {
            let (k, v) = (&kshape, &vshape);
            let g = match op {
                LinearOp::Project(p) => compile_projection(&p.key, &p.val, k, v),
                LinearOp::Filter(cond) => compile_predicate(cond, k, v),
                LinearOp::EnterAt(field) => compile_scalar(field, k, v),
                LinearOp::FlatMap(list) => compile_flatmap(list, k, v),
                LinearOp::JoinFunction(term) => compile_join_function(term, k, v, level + 1),
                LinearOp::Negate | LinearOp::LiftIter | LinearOp::Lift => unreachable!(),
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
/// The data<->time ops are columnar and total: EnterAt reads its delay field as a column and
/// joins it into `times` in place; LiftIter reads the iteration coordinate out of `times` and
/// appends it to `vals`; Lift reads every lane of `times` into `vals`; JoinFunction explodes
/// its list column as FlatMap does and joins each element's time lanes into `times`. `level`
/// is the scope depth (it locates that coordinate, and fixes the number of lanes as data).
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
                let g = plan.graph(op, kshape, vshape, level);
                let mut cols = corgi::eval_graph(g, CValue::Prod(vec![c.keys, c.vals])).into_prod("linear project").unwrap();
                let vals = cols.pop().unwrap();
                let keys = cols.pop().unwrap();
                CorgiContainer { keys, vals, times: c.times, diffs: c.diffs }
            }
            LinearOp::Filter(_) => {
                let g = plan.graph(op, kshape, vshape, level);
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
                let g = plan.graph(op, kshape, vshape, level);
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
            // A time as data is every lane, the epoch and one per scope: `level + 1` lanes, zero
            // where the column is narrower. Keys, times and diffs are untouched.
            LinearOp::Lift => {
                let lanes = (0..=level).map(|j| CValue::i64(c.times.lane(j).into_iter().map(|x| x as i64).collect())).collect();
                let vals = CValue::Prod(vec![c.vals, CValue::Prod(lanes)]);
                CorgiContainer { keys: c.keys, vals, times: c.times, diffs: c.diffs }
            }
            LinearOp::JoinFunction(_) => {
                let g = plan.graph(op, kshape, vshape, level);
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
                let g = plan.graph(op, kshape, vshape, level);
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

/// A batch of the program's own corgi arrangements (`CTrace`'s batch).
type NativeBatch = std::rc::Rc<ChunkBatch<CorgiChunk<Time, Diff>>>;
/// A batch of a published (host-time) corgi export (`ExportTrace`'s batch).
type HostBatch = std::rc::Rc<ChunkBatch<CorgiChunk<u64, Diff>>>;
/// A published corgi export entered into a program's scope: the producer's own chunks.
pub type ImportArr<'scope> = Arranged<'scope, TraceEnter<ExportTrace, Time>>;

/// The corgi backend's arrangement: one it built, or a published export it imported.
#[derive(Clone)]
pub enum Arr<'scope> {
    Local(Arranged<'scope, TraceAgent<CTrace>>),
    Imported(ImportArr<'scope>),
}

/// A batch as the corgi tactics read it. An imported batch crosses into the program as its
/// producer's chunks *retimed*: the same `Rc`s, read with each host epoch `e` as `(e, [])`. The
/// lanes are identical, so the crossing costs one `Rc` per chunk and copies no data.
trait Native {
    fn native(self) -> NativeBatch;
}
impl Native for NativeBatch {
    fn native(self) -> NativeBatch { self }
}
impl Native for BatchEnter<HostBatch, Time> {
    fn native(self) -> NativeBatch {
        std::rc::Rc::new(ChunkBatch { chunks: self.inner().chunks.iter().map(CorgiChunk::retime).collect() })
    }
}

/// A join tactic over native batches, accepting either input imported: the bridge where an
/// imported batch's times change to the program's.
struct RetimeJoin<J>(J);
impl<B0: Native, B1: Native, J: JoinTactic<Time, NativeBatch, NativeBatch, CC>> JoinTactic<Time, B0, B1, CC> for RetimeJoin<J> {
    fn prep(&mut self, input0: Vec<B0>, input1: Vec<B1>, fresh: Fresh, meet: Time) -> Box<dyn Iterator<Item = CC>> {
        let input0 = input0.into_iter().map(Native::native).collect();
        let input1 = input1.into_iter().map(Native::native).collect();
        self.0.prep(input0, input1, fresh, meet)
    }
}

/// A half-join tactic over native batches, accepting an imported arrangement.
struct RetimeHalfJoin<J>(J);
impl<B: Native, M, J: HalfJoinTactic<Time, NativeBatch, M, CC>> HalfJoinTactic<Time, B, M, CC> for RetimeHalfJoin<J> {
    fn prep(&mut self, chunks: Vec<M>, batches: Vec<B>, lower: Antichain<Time>) -> Box<dyn Iterator<Item = CC>> {
        self.0.prep(chunks, batches.into_iter().map(Native::native).collect(), lower)
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

/// A reduce tactic over native batches, accepting an imported input. (Its output is its own.)
struct RetimeReduce<R>(R);
impl<B1: Native, R: ReduceTactic<Time, NativeBatch, NativeBatch>> ReduceTactic<Time, B1, NativeBatch> for RetimeReduce<R> {
    fn retire(
        &mut self,
        source_batches: Vec<B1>,
        output_batches: Vec<NativeBatch>,
        input_batches: Vec<B1>,
        lower: &Antichain<Time>,
        upper: &Antichain<Time>,
        held: &Antichain<Time>,
    ) -> (Option<Span<Time, NativeBatch>>, Antichain<Time>) {
        let source_batches = source_batches.into_iter().map(Native::native).collect();
        let input_batches = input_batches.into_iter().map(Native::native).collect();
        self.0.retire(source_batches, output_batches, input_batches, lower, upper, held)
    }
}

/// The updates of a stream of columnar batches, as corgi containers.
///
/// Each chunk already IS a columnar container: its key/val columns clone by Arc bump,
/// so a chunk becomes a `CorgiContainer` for the price of copying its time lanes and
/// its diffs. One container per chunk — no concatenation, no gather, no
/// columns→rows→columns round-trip. `chunks` presents a batch's chunks at the stream's time,
/// which retimes an imported batch on the way.
fn chunk_containers<'s, T: ColTime, B: Clone + 'static>(
    stream: timely::dataflow::Stream<'s, T, Vec<Span<T, B>>>,
    name: &str,
    chunks: impl Fn(B) -> std::rc::Rc<ChunkBatch<CorgiChunk<T, Diff>>> + 'static,
) -> Collection<'s, T, CorgiContainer<T, Diff>> {
    stream
        .unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each(|cap, data| {
                    let mut session = output.session(&cap);
                    for batch in data.iter() {
                        let Some(payload) = batch.inner.clone() else { continue };
                        for ch in chunks(payload).chunks.iter().filter(|c| c.len() > 0) {
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

/// The corgi rendering substrate. An uninhabited type used only as a type-level tag: it
/// carries the [`Backend`] impl (a namespace of rendering functions selected by type) and is
/// never a value — rendering goes through `render_tree::<CorgiBackend>`. The empty enum (vs a
/// unit struct) makes constructing one impossible, signalling "type only". Mirrors `VecBackend`.
pub enum CorgiBackend {}

impl Backend for CorgiBackend {
    type Container = CC;
    type Arr<'scope> = Arr<'scope>;

    fn linear<'s>(c: Collection<'s, Time, CC>, ops: Vec<LinearOp>, level: usize, shape: Option<st::RowShape>) -> Collection<'s, Time, CC> {
        // Container-level: fold the LinearOp chain over each corgi batch (no inter-op transcode).
        // `level` is the scope depth (locates the iteration coordinate for LiftIter/EnterAt).
        // With the input's shape known, every op compiles now, at the shape it will see.
        let mut plans: Vec<Plan> = ops.iter().map(|_| Plan::default()).collect();
        if let Some(Ok(steps)) = shape.map(|(k, v)| crate::shapes::linear_shapes(&ops, k, v, level)) {
            for ((op, plan), (k, v)) in ops.iter().zip(plans.iter_mut()).zip(steps) {
                if !matches!(op, LinearOp::Negate | LinearOp::LiftIter | LinearOp::Lift) { plan.graph(op, k, v, level); }
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
        Arr::Local(arrange_core::<_, CC, _, CTrace>(
            c.inner,
            CorgiPact,
            "CorgiArrange",
            ChunkBatcher::<CorgiChunker<Time, Diff>, _>::new,
        ))
    }

    fn as_collection<'s>(a: Self::Arr<'s>) -> Collection<'s, Time, CC> {
        match a {
            Arr::Local(a) => chunk_containers(a.stream, "CorgiAsCollection", Native::native),
            Arr::Imported(a) => chunk_containers(a.stream, "CorgiAsCollection", Native::native),
        }
    }

    fn join<'s>(l: Self::Arr<'s>, r: Self::Arr<'s>, projection: &Projection, shapes: Option<(st::RowShape, st::RowShape)>) -> Collection<'s, Time, CC> {
        // The proxy-join seam drives the backend blockwise under the driver's fuel; the backend
        // compiles the projection once (shape-directed, for `Spread`), now if both inputs' shapes
        // are known and otherwise on its first output, and emits corgi columns directly as
        // `CorgiContainer`s — column-native, no row round-trip.
        let shapes = shapes.map(|((k, v0), (_, v1))| (k, v0, v1));
        let tactic = || RetimeJoin(ProxyJoinTactic::new(CorgiJoinBackend::new(projection.key.clone(), projection.val.clone(), shapes.clone())));
        match (l, r) {
            (Arr::Local(l), Arr::Local(r)) => join_with_tactic::<_, _, _, CC>(l, r, "Join", tactic()).as_collection(),
            (Arr::Local(l), Arr::Imported(r)) => join_with_tactic::<_, _, _, CC>(l, r, "Join", tactic()).as_collection(),
            (Arr::Imported(l), Arr::Local(r)) => join_with_tactic::<_, _, _, CC>(l, r, "Join", tactic()).as_collection(),
            (Arr::Imported(l), Arr::Imported(r)) => join_with_tactic::<_, _, _, CC>(l, r, "Join", tactic()).as_collection(),
        }
    }

    fn half_join<'s>(stream: Collection<'s, Time, CC>, arrangement: Self::Arr<'s>, strict: bool, projection: &Projection, level: usize, _shapes: Option<(st::RowShape, st::RowShape)>) -> Collection<'s, Time, CC> {
        // dogs3's driver owns progress and yielding; corgi supplies when an update is unblocked
        // (`HalfJoinBatcher`) and how released updates meet the arrangement (`CorgiHalfJoin`).
        // `CorgiPact` places each update on the worker holding its key, as `arrange` placed the rows.
        let tactic = || RetimeHalfJoin(CorgiHalfJoin::new(projection.key.clone(), projection.val.clone(), strict, level + 1));
        let yielding = |_timer: std::time::Instant, count: usize| count > 1_000_000;
        match arrangement {
            Arr::Local(a) => half_join_with_tactic(stream.inner, a, CorgiPact, step_back, yielding, HalfJoinBatcher::new(strict, level + 1), tactic()).as_collection(),
            Arr::Imported(a) => half_join_with_tactic(stream.inner, a, CorgiPact, step_back, yielding, HalfJoinBatcher::new(strict, level + 1), tactic()).as_collection(),
        }
    }
    fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer) -> Self::Arr<'s> {
        // Amortize columnar corrections while reusing sweep scratch across wide presentations.
        let tactic = RetimeReduce(ProxyReduceTactic::new(CorgiReduceBackend::new(reducer.clone())).with_key_batch_size(256));
        Arr::Local(match a {
            Arr::Local(a) => reduce_with_tactic::<_, CTrace, _>(a, "CorgiReduce", tactic),
            Arr::Imported(a) => reduce_with_tactic::<_, CTrace, _>(a, "CorgiReduce", tactic),
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
    fn enter_region<'s, 'r>(a: Self::Arr<'s>, region: Scope<'r, Time>) -> Self::Arr<'r> {
        match a {
            Arr::Local(a) => Arr::Local(a.enter_region(region)),
            Arr::Imported(a) => Arr::Imported(a.enter_region(region)),
        }
    }
}

/// Convert a collection of rows to corgi containers, at `shape` if it is known and otherwise at
/// the shape of the first row seen. Known shapes describe empty lists and inactive sum lanes.
pub fn rows_to_corgi<'s, T: ColTime>(
    c: differential_dataflow::VecCollection<'s, T, (Row, Row), Diff>,
    shape: Option<st::RowShape>,
) -> Collection<'s, T, CorgiContainer<T, Diff>> {
    c.inner
        .unary(Pipeline, "ToCorgi", move |_, _| {
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
}

/// Assert that every container of `c` has `shape`: an import's shape ascription, checked as data
/// arrives when the shape of the trace it reads was not known at install.
pub fn assert_shape<'s, T: ColTime>(
    c: Collection<'s, T, CorgiContainer<T, Diff>>,
    shape: st::RowShape,
) -> Collection<'s, T, CorgiContainer<T, Diff>> {
    c.inner
        .unary(Pipeline, "AssertShape", move |_, _| {
            move |input, output| {
                input.for_each(|cap, data| {
                    if !data.times.is_empty() {
                        let (k, v) = (corgi::shape_of_value(&data.keys), corgi::shape_of_value(&data.vals));
                        assert!(k == shape.0 && v == shape.1, "input does not match its shape ascription");
                    }
                    output.session(&cap).give_container(data);
                });
            }
        })
        .as_collection()
}

/// Render `s` with the corgi substrate. See [`crate::backend::render_tree`].
pub fn render_tree<'s>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<Collection<'s, Time, CC>>,
    shapes: Option<&crate::shapes::ScopeShapes>,
) -> Vec<Collection<'s, Time, CC>> {
    let imports = imports.into_iter().map(crate::backend::Rendered::Collection).collect();
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
        .map(|(k, (c, import))| rows_to_corgi(c, import_shape(k, import, shapes)))
        .collect();
    render_tree(s, scope, depth, corgi_imports, shapes)
}

/// Import `k`'s declared shape, or the one inferred at install (an imported trace's).
fn import_shape(k: usize, import: &st::Import, shapes: Option<&crate::shapes::ScopeShapes>) -> Option<st::RowShape> {
    import.shape.clone().or_else(|| shapes.and_then(|sh| sh.imports[k].clone()).map(|c| (c.key, c.val)))
}

/// A root import for [`render_tree_corgi_arranged`]: corgi containers, or a published columnar
/// export.
#[allow(clippy::large_enum_variant)] // One per import, consumed as the program renders.
pub enum CorgiImport<'s> {
    Columns(Collection<'s, Time, CC>),
    Export(ImportArr<'s>),
}

/// As [`render_tree`], but a published columnar export arrives as the producer's arrangement,
/// which the program's `arrange`s, joins, and reduces of it read in place. Shape ascriptions are
/// the driver's to check ([`assert_shape`], [`assert_export_shape`]).
pub fn render_tree_corgi_arranged<'s>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<CorgiImport<'s>>,
    shapes: Option<&crate::shapes::ScopeShapes>,
) -> Vec<Collection<'s, Time, CC>> {
    assert_eq!(s.imports.len(), imports.len());
    let imports = imports.into_iter().map(|imp| match imp {
        CorgiImport::Columns(c) => crate::backend::Rendered::Collection(c),
        CorgiImport::Export(a) => crate::backend::Rendered::Arrangement(Arr::Imported(a)),
    }).collect();
    crate::backend::render_tree::<CorgiBackend>(s, scope, depth, imports, shapes)
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

/// The updates of an imported columnar export, as corgi containers.
pub fn export_containers<'s>(a: Arranged<'s, ExportTrace>) -> Collection<'s, u64, CorgiContainer<u64, Diff>> {
    chunk_containers(a.stream, "ExportContainers", |batch| batch)
}

/// Assert that every chunk of an imported export has `shape`, an import's ascription, when the
/// export's shape was not known at install. A side operator over the batches: the program reads
/// the arrangement itself, and nothing is copied.
pub fn assert_export_shape(a: &Arranged<'_, ExportTrace>, shape: st::RowShape) {
    use timely::dataflow::operators::generic::Operator;
    a.stream.clone().sink(Pipeline, "AssertShape", move |(input, _frontier)| {
        input.for_each(|_cap, data| {
            for batch in data.iter() {
                let Some(payload) = batch.inner.as_ref() else { continue };
                for ch in payload.chunks.iter().filter(|c| c.len() > 0) {
                    let (k, v) = (corgi::shape_of_value(&recover_key(ch.keys())), corgi::shape_of_value(ch.vals()));
                    assert!(k == shape.0 && v == shape.1, "input does not match its shape ascription");
                }
            }
        });
    });
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
