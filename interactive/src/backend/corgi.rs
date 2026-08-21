//! The corgi rendering substrate: corgi columns are the native representation on dataflow edges,
//! arrangements are chains of sorted columnar chunks (`ChunkSpine<CorgiChunk>`, cursor-less), and
//! scalar logic runs columnar via `eval_graph`. Parallels the row-wise `backend::vec`, which stays
//! the correctness reference.
//!
//! All `Backend` methods are corgi-native: `linear` folds a `LinearOp` chain over each container
//! ([`apply_ops`], columnar fast paths with row-wise fallbacks); `arrange` ingests columns without
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
use differential_dataflow::trace::chunk::{Chunk, ChunkBatcher, ChunkBuilder};

use corgi::arrange::gather;
use corgi::Value as CValue;

use crate::backend::{no_compaction, stage_extend, stage_probe, Backend, Prefix};
use crate::corgi::chunk::{recover_key, CorgiChunk, CorgiChunker};
use crate::corgi::container::CorgiContainer;
use crate::corgi::join::CorgiJoinBackend;
use crate::corgi::reduce::CorgiReduceBackend;
use differential_dataflow::operators::int_proxy::{ProxyJoinTactic, ProxyReduceTactic};
use crate::corgi::logic::{compilable, compile_flatmap, compile_predicate, compile_projection, compile_scalar};
use crate::ir::{Diff, LinearOp, Time, Value as DValue};
use crate::parse::{Projection, Reducer};
use crate::scope_ir as st;

/// A DDIR row, an update, the corgi container on dataflow edges, and the columnar trace —
/// the shorthands the `Backend` methods below are written in terms of.
type Row = DValue;
type Upd = ((Row, Row), Time, Diff);
type CC = CorgiContainer<Time, Diff>;
type CTrace = differential_dataflow::trace::chunk::ChunkSpine<CorgiChunk<Time, Diff>>;

/// Rebase a join-projection term from the join environment (`$0`=key, `$1`=left val,
/// `$2`=right val) onto the row environment of the identity join's output
/// (`$0`=key, `$1`=(left val, right val)): `$1 -> $1[0]`, `$2 -> $1[1]`, structurally
/// everywhere. `Bound` binders are scope-relative and pass through untouched.
fn rebase_join_term(t: &crate::parse::Term) -> crate::parse::Term {
    use crate::parse::Term::*;
    match t {
        Var(0) => Var(0),
        Var(1) => Proj(Box::new(Var(1)), 0),
        Var(2) => Proj(Box::new(Var(1)), 1),
        Var(n) => panic!("join projection references ${n}"),
        Bound(k) => Bound(*k),
        Int(n) => Int(*n),
        Tuple(fs) => Tuple(fs.iter().map(rebase_join_term).collect()),
        List(fs) => List(fs.iter().map(rebase_join_term).collect()),
        Spread(inner) => Spread(Box::new(rebase_join_term(inner))),
        Proj(inner, i) => Proj(Box::new(rebase_join_term(inner)), *i),
        Inject(tag, payload) => Inject(Box::new(rebase_join_term(tag)), Box::new(rebase_join_term(payload))),
        Case { scrutinee, arms, default } => Case {
            scrutinee: Box::new(rebase_join_term(scrutinee)),
            arms: arms.iter().map(rebase_join_term).collect(),
            default: default.as_ref().map(|d| Box::new(rebase_join_term(d))),
        },
        Fold { list, init, step } => Fold {
            list: Box::new(rebase_join_term(list)),
            init: Box::new(rebase_join_term(init)),
            step: Box::new(rebase_join_term(step)),
        },
        If { cond, then, els } => If {
            cond: Box::new(rebase_join_term(cond)),
            then: Box::new(rebase_join_term(then)),
            els: Box::new(rebase_join_term(els)),
        },
        Binary(op, l, r) => Binary(*op, Box::new(rebase_join_term(l)), Box::new(rebase_join_term(r))),
        Unary(op, inner) => Unary(*op, Box::new(rebase_join_term(inner))),
        Hash(args) => Hash(args.iter().map(rebase_join_term).collect()),
    }
}

/// Apply a `LinearOp` chain to one corgi container (the corgi-native compute per batch).
/// Project = corgi `eval_graph`; Filter = corgi mask + `gather`; FlatMap = `eval_graph` to a list
/// column + a structural explode; Negate = Rust. Each falls back to rows when the term has no
/// lowering with this container's shapes, so capability never depends on the compiler's coverage.
/// The two data<->time ops are columnar and total: EnterAt reads its delay field as a column and
/// joins it into `times` in place; LiftIter reads the iteration coordinate out of `times` and
/// appends it to `vals`. `level` is the scope depth (it locates that coordinate).
fn apply_ops(mut c: CC, ops: &[LinearOp], level: usize) -> CC {
    use timely::order::Product;
    use differential_dataflow::lattice::Lattice;
    use differential_dataflow::dynamic::pointstamp::PointStamp;

    for op in ops {
        c = match op {
            LinearOp::Project(p) => {
                let (kshape, vshape) = (corgi::shape_of_value(&c.keys), corgi::shape_of_value(&c.vals));
                // The shape-aware gate: attempt the lowering with this container's shapes and
                // fall back to rows only when it declines — a heterogeneous list literal, a
                // `Case` whose arms disagree, a data-driven tag.
                if let Some(g) = compile_projection(&p.key, &p.val, &kshape, &vshape) {
                    let mut cols = corgi::eval_graph(&g, CValue::Prod(vec![c.keys, c.vals])).into_prod("linear project");
                    let vals = cols.pop().unwrap();
                    let keys = cols.pop().unwrap();
                    CorgiContainer { keys, vals, times: c.times, diffs: c.diffs }
                } else {
                    let mut out: Vec<Upd> = Vec::new();
                    for ((k, v), t, d) in c.into_updates() {
                        let mut env = vec![k, v];
                        let nk = crate::ir::eval(&p.key, &mut env);
                        let nv = crate::ir::eval(&p.val, &mut env);
                        out.push(((nk, nv), t, d));
                    }
                    CorgiContainer::from_updates(out)
                }
            }
            LinearOp::Filter(cond) => {
                let (kshape, vshape) = (corgi::shape_of_value(&c.keys), corgi::shape_of_value(&c.vals));
                if let Some(g) = compile_predicate(cond, &kshape, &vshape) {
                    let mask = corgi::eval_graph(&g, CValue::Prod(vec![c.keys.clone(), c.vals.clone()])).into_u64("filter mask");
                    let keep: Vec<usize> = (0..mask.len()).filter(|&i| mask[i] != 0).collect();
                    let keys = gather(&c.keys, &keep);
                    let vals = gather(&c.vals, &keep);
                    let times = keep.iter().map(|&i| c.times[i].clone()).collect();
                    let diffs = keep.iter().map(|&i| c.diffs[i]).collect();
                    CorgiContainer { keys, vals, times, diffs }
                } else {
                    let mut out: Vec<Upd> = Vec::new();
                    for ((k, v), t, d) in c.into_updates() {
                        let keep = { let mut env = vec![k.clone(), v.clone()]; crate::ir::eval(cond, &mut env).truthy() };
                        if keep { out.push(((k, v), t, d)); }
                    }
                    CorgiContainer::from_updates(out)
                }
            }
            LinearOp::Negate => {
                for d in c.diffs.iter_mut() {
                    *d = -*d;
                }
                c
            }
            LinearOp::EnterAt(field) => {
                let (kshape, vshape) = (corgi::shape_of_value(&c.keys), corgi::shape_of_value(&c.vals));
                if let Some(g) = compile_scalar(field, &kshape, &vshape) {
                    // The key and val columns are IDENTITY here — only times change. Evaluate the
                    // delay field to a `U64` column and join it into each time in place. Joining
                    // `Product(0, PointStamp([0,..,0, delay]))` is, coordinate-wise, `max` at index
                    // `level-1` and identity everywhere else (u64's minimum is 0), so the delta
                    // never has to be built. `PointStamp::new` re-strips the trailing minimums the
                    // resize may add, keeping the representation canonical for a zero delay.
                    let raw = corgi::eval_graph(&g, CValue::Prod(vec![c.keys.clone(), c.vals.clone()]))
                        .into_u64("enter_at delay");
                    let idx = level.saturating_sub(1);
                    for (t, &r) in c.times.iter_mut().zip(raw.iter()) {
                        let delay = 256 * (64 - r.leading_zeros() as u64);
                        let mut coords = std::mem::take(&mut t.inner).into_inner();
                        if coords.len() <= idx {
                            coords.resize(idx + 1, 0);
                        }
                        coords[idx] = coords[idx].max(delay);
                        t.inner = PointStamp::new(coords);
                    }
                    c
                } else {
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
            }
            // The inverse of `EnterAt`: a value read OUT of each row's time. Vals gain one
            // integer field; keys, times and diffs are untouched, and no term is compiled, so
            // this path is total — there is no fallback to fall back to.
            //
            // It mirrors [`append_iter`] shape for shape, and the empty product is where the two
            // representations part company: DDIR unit IS `Tuple([])`, which `append_iter` extends
            // to `Tuple([iter])`, but columnar it arrives as `CValue::Unit`, not an empty `Prod`.
            // So `Unit` must become `Prod([iter])` — `Prod([Unit, iter])` would be a silent
            // one-field-too-many divergence from `backend::vec`.
            LinearOp::LiftIter => {
                let iters: Vec<u64> = c
                    .times
                    .iter()
                    .map(|t| level.checked_sub(1).and_then(|idx| t.inner.get(idx).copied()).unwrap_or(0))
                    .collect();
                let lane = CValue::u64(iters);
                let vals = match c.vals {
                    CValue::Prod(mut fields) => { fields.push(lane); CValue::Prod(fields) }
                    CValue::Unit(_) => CValue::Prod(vec![lane]),
                    other => CValue::Prod(vec![other, lane]),
                };
                CorgiContainer { keys: c.keys, vals, times: c.times, diffs: c.diffs }
            }
            // Row-wise ops (parity with `backend::vec::render_linear`).
            LinearOp::FlatMap(list_term) => {
                let (kshape, vshape) = (corgi::shape_of_value(&c.keys), corgi::shape_of_value(&c.vals));
                if let Some(g) = compile_flatmap(list_term, &kshape, &vshape) {
                    // Structural explode: the evaluated list column's FLAT element storage already
                    // IS the new value column, so the elements never move. Each row's span in the
                    // bounds gives both the within-row position (DDIR's `$1[0]`) and a repeat map
                    // carrying key/time/diff across. No per-row eval, no transcode.
                    let (bounds, elems) =
                        corgi::eval_graph(&g, CValue::Prod(vec![c.keys.clone(), c.vals])).into_list("flatmap list");
                    let ends: Vec<usize> = match &bounds {
                        corgi::Bounds::Offsets(v) => v.clone(),
                        corgi::Bounds::Stride(k, rows) => (1..=*rows).map(|i| i * k).collect(),
                    };
                    let total = ends.last().copied().unwrap_or(0);
                    let (mut reps, mut pos) = (Vec::with_capacity(total), Vec::with_capacity(total));
                    let mut start = 0usize;
                    for (row, end) in ends.into_iter().enumerate() {
                        for p in 0..(end - start) {
                            reps.push(row);
                            pos.push(p as u64);
                        }
                        start = end;
                    }
                    CorgiContainer {
                        keys: gather(&c.keys, &reps),
                        vals: CValue::Prod(vec![CValue::u64(pos), elems]),
                        times: reps.iter().map(|&r| c.times[r].clone()).collect(),
                        diffs: reps.iter().map(|&r| c.diffs[r]).collect(),
                    }
                } else {
                    apply_flatmap_rows(c, list_term)
                }
            }
        };
    }
    c
}

/// The row-wise `FlatMap`: untranscode, `ir::eval` the list term per row, explode, re-transcode.
/// Parity with `backend::vec::render_linear`, and the fallback when the list term has no columnar
/// lowering with this container's shapes.
fn apply_flatmap_rows(c: CC, list_term: &crate::parse::Term) -> CC {
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


/// The corgi rendering substrate. An uninhabited type used only as a type-level tag: it
/// carries the [`Backend`] impl (a namespace of rendering functions selected by type) and is
/// never a value — rendering goes through `render_tree::<CorgiBackend>`. The empty enum (vs a
/// unit struct) makes constructing one impossible, signalling "type only". Mirrors `VecBackend`.
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
        // Single-worker guard: this arrange is `Pipeline` (no key exchange), so multi-worker
        // execution would MIS-PLACE keys — silently wrong, not slow. A columnar exchange
        // (radix partition by key hash) lifts this; it pairs with the stored-hash-column work.
        assert_eq!(c.inner.scope().peers(), 1, "the corgi backend is single-worker: arrange does not exchange keys");
        // Column-native ingest: `CorgiChunker` sort-consolidates each input `CorgiContainer`'s
        // columns straight into a `CorgiChunk` (no drain-to-rows), then the standard chunk batcher +
        // builder. No columns→rows→columns round-trip at the arrangement boundary.
        arrange_core::<_, CC, CorgiChunker<Time, Diff>, ChunkBatcher<CorgiChunk<Time, Diff>>, ChunkBuilder<CorgiChunk<Time, Diff>>, CTrace>(
            c.inner,
            Pipeline,
            "CorgiArrange",
        )
    }

    fn as_collection<'s>(a: Self::Arr<'s>) -> Collection<'s, Time, CC> {
        // Each chunk already IS a columnar container: its key/val columns clone by Arc bump,
        // so a chunk becomes a `CorgiContainer` for the price of materializing its times
        // (`ColTimes` → `Vec<T>`, the owned-time egress) and a diffs memcpy. One container
        // per chunk — no concatenation, no gather, no columns→rows→columns round-trip.
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
                                    times: ch.times().to_vec(),
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

    fn join<'s>(l: Self::Arr<'s>, r: Self::Arr<'s>, projection: &Projection) -> Collection<'s, Time, CC> {
        // The proxy-join seam drives the backend blockwise under the driver's fuel; the backend
        // compiles the projection per container (shape-directed, for `Spread`) and emits corgi
        // columns directly as `CorgiContainer`s — column-native, no row round-trip.
        if compilable(&projection.key) && compilable(&projection.val) {
            let tactic = ProxyJoinTactic::new(CorgiJoinBackend::new(projection.key.clone(), projection.val.clone()));
            join_with_tactic::<_, _, _, CC>(l, r, "Join", tactic).as_collection()
        } else {
            // Projections the lowering can't compile take the same shape as `linear`'s gate:
            // join with the identity projection (compilable by construction), then apply the
            // original terms as a row-wise `Project`, rebased from the join env
            // `[$0=key, $1=left val, $2=right val]` onto the row env `[$0=key, $1=(lv, rv)]`.
            // Capability never depends on the lowering's coverage; only speed does.
            use crate::parse::Term;
            let key = Term::Var(0);
            let val = Term::Tuple(vec![Term::Var(1), Term::Var(2)]);
            let tactic = ProxyJoinTactic::new(CorgiJoinBackend::new(key, val));
            let joined = join_with_tactic::<_, _, _, CC>(l, r, "Join", tactic).as_collection();
            let rebased = Projection { key: rebase_join_term(&projection.key), val: rebase_join_term(&projection.val) };
            Self::linear(joined, vec![LinearOp::Project(rebased)], 0)
        }
    }

    /// Rows leave and re-enter corgi columns at the delta join's boundary, because a partial
    /// binding has no columnar form; `CorgiContainer`'s own transcode is that boundary.
    fn to_rows<'s>(c: Collection<'s, Time, CC>) -> differential_dataflow::VecCollection<'s, Time, (Row, Row), Diff> {
        c.inner
            .unary(Pipeline, "CorgiDeltaRows", |_, _| {
                |input, output| {
                    input.for_each(|cap, data| {
                        output.session(&cap).give_container(&mut std::mem::take(data).into_updates());
                    });
                }
            })
            .as_collection()
    }

    fn from_rows<'s>(c: differential_dataflow::VecCollection<'s, Time, (Row, Row), Diff>) -> Collection<'s, Time, CC> {
        c.inner
            .unary(Pipeline, "CorgiDeltaColumns", |_, _| {
                |input, output| {
                    input.for_each(|cap, data| {
                        let mut cc = CorgiContainer::from_updates(std::mem::take(data));
                        output.session(&cap).give_container(&mut cc);
                    });
                }
            })
            .as_collection()
    }

    fn delta_stage<'s>(
        stream: differential_dataflow::VecCollection<'s, Time, (Prefix, Time), Diff>,
        index: Self::Arr<'s>,
        kind: &st::StageKind,
        strict: bool,
    ) -> differential_dataflow::VecCollection<'s, Time, (Prefix, Time), Diff> {
        use differential_dogs3::operators::half_join::{cursors::BlobList, half_join_with_tactic};
        use crate::corgi::half_join::CorgiHalfJoinTactic;

        let (probe, bind) = stage_probe(kind);
        let requests = stream.map(move |(prefix, payload)| (probe(&prefix), prefix, payload));
        // `Pipeline`: the corgi backend is single-worker and its arrange does not exchange, so
        // the deltas are already where the arrangement they probe is.
        half_join_with_tactic(
            requests.inner,
            index,
            Pipeline,
            no_compaction,
            |_timer, _count| false,
            BlobList::new(strict),
            CorgiHalfJoinTactic::new(move |_k: &Row, prefix: &Prefix, val: &Row| stage_extend(prefix, bind, val), strict),
        )
        .as_collection()
    }

    fn reduce<'s>(a: Self::Arr<'s>, reducer: &Reducer) -> Self::Arr<'s> {
        reduce_with_tactic::<_, CTrace, _>(a, "CorgiReduce", ProxyReduceTactic::new(CorgiReduceBackend::new(reducer.clone())))
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

/// Render `s` with the corgi substrate over ROW collections: each import converts to corgi
/// containers at the boundary (`ToCorgi`), the tree renders columnar, and each export
/// converts back (`FromCorgi`). Signature-compatible with
/// [`vec::render_tree`](crate::backend::vec::render_tree) (hence the `vec::Col` alias), so a
/// row-speaking driver switches backends by switching this one call.
pub fn render_tree_rows<'s>(
    s: &st::Scope,
    scope: Scope<'s, Time>,
    depth: usize,
    imports: Vec<crate::backend::vec::Col<'s>>,
) -> Vec<crate::backend::vec::Col<'s>> {
    let corgi_imports: Vec<Collection<'s, Time, CC>> = imports
        .into_iter()
        .map(|c| {
            c.inner
                .unary(Pipeline, "ToCorgi", |_, _| {
                    |input, output| {
                        input.for_each(|cap, data| {
                            let mut cc = CorgiContainer::from_updates(std::mem::take(data));
                            output.session(&cap).give_container(&mut cc);
                        });
                    }
                })
                .as_collection()
        })
        .collect();
    render_tree(s, scope, depth, corgi_imports)
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
    use differential_dataflow::input::Input;
    use differential_dataflow::dynamic::pointstamp::PointStamp;

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
                // Enter row collections (refinement); rows convert to corgi containers and
                // back inside `render_tree_rows`.
                let entered: Vec<_> = collections.iter().map(|c| c.clone().enter(inner)).collect();
                let root_imports: Vec<_> = program
                    .root
                    .imports
                    .iter()
                    .map(|imp| match &imp.from {
                        st::Source::Input(n) => entered[*n].clone(),
                        other => panic!("corgi evaluate: unsupported source {other:?}"),
                    })
                    .collect();
                render_tree_rows(&program.root, inner.clone(), 0, root_imports)
                    .into_iter()
                    .map(|rows| rows.leave(scope))
                    .collect::<Vec<_>>()
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
