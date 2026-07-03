# Spike: corgi as columnar scalar logic inside differential-dataflow

**Goal.** Run corgi (`frankmcsherry/wip:corgi`) — or a light fork — as the scalar logic DD's
join/reduce/linear operators invoke *where they would normally call a closure*, applied to **many
rows at once** (columnar). Host = the DDIR crate (`interactive/`). Drive to a **measured viability
conclusion**: does corgi-over-columns beat DDIR's per-row `ir::eval` interpreter, and from what
column width? How much existing structure can we reuse; do we need a corgi-data CHUNK variant?

This file is the durable log across loop iterations. **Read it first each iteration.**

---

## PHASE 2 (2026-06-29): BUILD THE CORGI BACKEND (no transcode, corgi-native)

Goal: corgi's columnar `Value` is DDIR's native row representation end-to-end; operators run
`eval_graph`; conversion only at I/O boundaries. Frank: consult `chunk/vec.rs` + the `columnar/`
stack as templates (don't copy literally). Phase-1 verdict (above) stands; this builds the real thing.

### Design surface — much smaller than feared (verified this iteration)
- `timely::Container` = just `Accountable + Default + 'static` (blanket impl, timely lib.rs:104). So
  a custom container needs only `record_count` + `Default` + `'static`.
- Backend container bound (`backend/mod.rs:39-42`) = `Container + Clone + ResultsIn<Summary> +
  Enter<Time,Time,Inner=Self> + Leave<Time,Time,Outer=Self>`. **Enter/Leave are SAME-time =
  IDENTITY** for DDIR's dynamic-timestamp model (comment `backend/mod.rs:38`). No `ContainerBytes`
  in the bound → **not needed for single-worker** (`evaluate` uses `execute_directly`).
- Container trait defs: `Negate::negate(self)->Self`; `Enter<T1,T2>{type InnerContainer; enter}`;
  `Leave<T1,T2>{type OuterContainer; leave}`; `ResultsIn<TS>::results_in(self,&TS)->Self`
  (`collection.rs:1314-1339`; `Vec<(D,T,R)>` reference impls at :1350-1376 — the template).
- NOTE: `backend::col` is **commented out in this branch** (`backend/mod.rs:14`, "deferred on the
  Value-model port") — so this is a fresh backend, not a col revival. `col.rs` readable for ideas.

### The container (M1 foundation)
`CorgiContainer<T, R>` = `{ keys: corgi::Value, vals: corgi::Value, times: Vec<T>, diffs: Vec<R> }`.
**Payload (key/val) corgi-columnar; time/diff plain Rust Vecs** — corgi never touches time/lattice.
Trait impls (all easy, time/diff in Rust, payload columns pass through or gather):
- `Accountable::record_count` = `times.len()`. `Default`/`Clone`.
- `Negate` = negate `diffs`. `Enter<T1,T2>`/`Leave<T1,T2>` = map times (identity for T1=T2=Time).
- `ResultsIn` = map each time via `step.results_in`; on `None`, drop that row → `gather` the corgi
  key/val columns by surviving indices (corgi `engine::gather`). Keep all four columns aligned.

### Staged milestones
- **M1 — container + corgi-native Linear, in a real dataflow (NO arrangement).** Linear is a
  container-level unary: input `CorgiContainer` block → `eval_graph` on keys/vals → output block.
  Ingest transcodes rows→block ONCE (boundary); egress block→rows ONCE. Between ops: zero
  transcode. Example mirrors `corgi_linear_dataflow`/`corgi_chain` but with the native container →
  should hit the eval-only ceiling. **This is the headline "no transcode" proof; do first.**
- **M2 — corgi arrangement (for join/reduce).** Route A: a `Chunk` impl over corgi columns
  (`chunk/vec.rs` template) → free `ChunkBatcher/Builder/Spine`; needs a cursor (navigates KEYS)
  + merge/extract/advance/settle (corgi sort/merge). The scalar logic (join projection / reducer)
  runs `eval_graph` over each key's VALUE-column slice (cursor gives the key run; corgi bulk-processes
  its values). Pragmatic start: hybrid (existing `ValSpine` storage + corgi logic over per-key
  slices) to prove reduce/join correctness, then move storage to corgi columns to kill transcode.
- **M3 — wire `impl Backend for CorgiBackend`** (linear/arrange/join/reduce/as_collection/inspect/
  leave_dynamic) + `render_tree::<CorgiBackend>`; run a real DDIR program end-to-end vs `evaluate`.

### Boundary transcode is expected & fine (the only conversions):
ingest (input rows→corgi block), egress (→rows), and any not-yet-ported op (interpreter fallback).
Operator-to-operator flow is transcode-free. Reuse `corgi_logic::{transcode,untranscode,compile*}`.

---

## ★ VERDICT (2026-06-28): VIABLE — strong win, recommended. ★

corgi as DDIR's columnar scalar logic works, integrates soundly with differential-dataflow, and
is a large measured win. Every question posed is answered:

- **corgi as scalar logic over many rows** — YES. Compiles DDIR `Term` → corgi `Graph<NumOp>`,
  runs columnar. Measured **50–135×** vs the per-row `ir::eval` interpreter on the map/filter/arith
  core (eval-only), **5.4×** on list/fold aggregation. Crossover at **~10 rows** (below that,
  `eval_graph` per-call setup loses). Examples: `corgi_bench`, `corgi_fold`.
- **Inside a real DD dataflow** — YES, SOUND. `corgi_linear_dataflow` runs corgi as a DDIR Linear
  map in a real differential computation; output matches the interpreter exactly incl. net-zero
  retractions vanishing. corgi never touches time; the operator keeps (time,diff)/lattice in Rust.
- **The cost lever** — the AoS↔SoA **transcode dominates** the per-call model (~85–95% at scale).
  Storing data corgi-columnar amortizes it: a K-op chain stays ~flat (`corgi_chain`), so the win
  GROWS with program depth (9×→22.5× over interp as K=1→16; 2–3.6× over per-op transcode).
- **Reuse / CHUNK variant** — to START, nothing new is needed: the scalar-logic path runs over any
  existing arrangement via per-call transcode. The corgi-columnar **CHUNK is the optimization**
  that removes the transcode, and pays more the deeper the program. Two routes (see "ARCHITECTURE
  FINDING"): via the `Chunk` trait (requires writing a cursor — it mandates `Navigable`), or
  cursor-less via a corgi container + custom `Merger` + tactic (the true use of the #771–773 seam).
- **Dynamic typing** — SOLVED. `infer_shape` forms the corgi `Shape` by observing the data; the
  `Term` drives ops, the inferred shape drives layout. For sum types, scan the column for all arms
  (the one genuinely data-dependent case; not yet built).
- **corgi modifications needed** — NONE so far. Map/filter/arith/list/fold all hit existing ops.

**Coverage:** DONE + measured — Var/Bound/Int/Tuple/Proj/Binary(arith,cmp,logical)/If/List/Fold.
MAPPED but not built — Variant/Inject/Case (→ MapSum/Unwrap/CapSum; needs column-scan Sum shape
inference + lane partition), Hash (compose or a light corgi op), signed/i64 (corgi `enc_i64`/
`ToSigned`). None are blockers; all have a clear path.

**Recommendation:** pursue it. Order: (1) land the `Term→Graph` compiler + transcoder as a real
DDIR module [DONE here as `corgi_logic.rs`]; (2) wire corgi into the Linear operator behind a
Backend flag [prototyped]; (3) corgi reduce tactic (closure already gets a per-key column slice);
(4) corgi-columnar CHUNK to kill the transcode — biggest payoff, do once 1–3 prove out; (5) extend
coverage to Variant/Case for the eqsat/datalog corpus.

Artifacts (all GREEN, `~/.cargo/bin/cargo run --release -p interactive --example <name>`):
`corgi_smoke`, `corgi_bench`, `corgi_linear_dataflow`, `corgi_chain`, `corgi_fold`;
library module `interactive/src/corgi_logic.rs`.

---

## Environment (durable)

- Worktree: `/Users/mcsherry/Projects/dd-corgi-spike`, branch `corgi-scalar-spike`, base
  `4eff165a` (= join-tactics: master-next `#772` + navigation-free join/reduce tactic drivers).
- corgi clone (read-only study, light mods allowed): `/Users/mcsherry/Projects/wip/corgi`
  (separate `wip` repo, `corgi/` dir, no-deps, ~2.1k LoC). Wired as path dep in
  `interactive/Cargo.toml`: `corgi = { path = "../../wip/corgi" }`.
- cargo NOT on PATH → use `~/.cargo/bin/cargo`. Build cmd: `cd <worktree> && ~/.cargo/bin/cargo build -p interactive`.
  Cold build measured **~10s** (fast iteration available). Run long builds in background.

## The plan: corgi = the scalar closure, NOT the tactic

DD `master-next` made navigation optional and join/reduce drive over a pluggable **tactic**
(`operators/join.rs:56 JoinTactic`, `operators/reduce.rs:28 ReduceTactic`; minimal batch =
`BatchReader{len,description}` only, `trace/mod.rs:215`; `Navigable` opt-in `cursor/mod.rs:25`).
corgi does NOT implement the tactic/merge engine — the tactic keeps navigation + all `(time,diff)`
lattice algebra in Rust. corgi replaces the **scalar closure**: DDIR's `Term` programs
(`interactive/src/parse/mod.rs:28`; `Projection`=`:86`, `Reducer`=`:89`, `LinearOp`=`ir.rs:37`)
compile to a corgi `Graph<NumOp>`, run by one compiled `eval_graph` over columns. "No lattice in
corgi" is therefore fine — corgi never sees time.

**Why columns matter:** corgi only beats the inline interpreter when handed *wide* columns
(`eval_graph` has per-call setup, `graph.rs:72`). Per-row corgi loses. The whole experiment is
"how wide before corgi wins."

**KEY INSIGHT (rung 0, verified):** corgi ops are *inherently columnar* — a `Prim` column IS the
batch of rows, and each op is the SIMD/vectorized map. So a per-row scalar `Term` compiles to a
**flat** corgi graph over the column (`Input` = the column); there is NO top-level `map` wrapper.
`MapList`/`Fold` appear only when the `Term` itself maps/folds over an *inner* `List` (DDIR `Fold`,
`FlatMap`). This makes the `Term → Graph` compile more direct than expected: binders are columns,
`Field` projects a Prod-column's component (itself a column), arithmetic/compare ops are columnar.
Pitfall found: `parse_ml("input map (x -> x add 100)")` panics ("MapList: expected a list") on a
`Prim` — use `input add_u64 100`.

### Staging ladder
- **Rung 0 (DONE):** env up, corgi builds inside DDIR, API mapped, smoke test.
- **Rung 1:** `Term → Graph<NumOp>` compiler (the load-bearing artifact) + `&[ir::Value] →
  corgi::Value` columnar transcoder, both **shape-directed**. Standalone bench: corgi vs per-row
  `ir::eval` on a map/filter `Term`, sweep column width → find crossover. (Lives as a module +
  example *inside* `interactive/`, reusing real `Term`/`Value`/`ir::eval`.)
- **Rung 2:** wire into DDIR's **Linear** operator (pure row-wise map/filter; no tactic needed —
  `backend/mod.rs:46`, `vec.rs:114`) so it runs inside a real DD dataflow.
- **Rung 3:** **Reduce** — its closure already gets a per-key `&[(val,diff)]` slice
  (`reduce.rs:651`), a natural column; corgi reducer (Min/Distinct/Count/Collect →
  Min/DedupList/Len/SortList+group). Big-group win first; key-batching for small groups later.
- **Rung 4 (stretch):** corgi-columnar **CHUNK** batch (`Chunk` impl à la `trace/chunk/vec.rs`)
  storing values as corgi columns so the column handed to corgi is a slice/Gather, not a
  per-call transcode. This is where the "batch without cursor" feature earns its keep.
- **Join** last (closure is per-`(val0,val1)` pair, `join.rs:99` → needs match-buffering).

## Dynamic-typing approach (Frank's key concern)

DDIR `Value = Int|Tuple|Variant|List` (`ir.rs:18`) is dynamically typed; corgi columns need a
fixed `Shape` (`shape.rs`, `Prim(w)|Prod|Sum|List`). Resolution: **infer the corgi `Shape` by
observing the data** (corgi `shape_of_value`) — i.e. the row type is formed from what actually
flows. The `Term` drives the ops; the input `Shape` drives shape-dependent choices (e.g. `Proj` →
`Field` on a Prod vs `Get`/`Gather` on a List, `Inject` arity). Output shape = `shape_of(graph,
input_shape)`. Compiler signature: `compile(term: &Term, input_shape: &Shape) -> Graph<NumOp>`.

## corgi API (locked, for the compiler)

- Build: `corgi::Builder::<NumOp>::default()` → `.input()` / `.tuple(Vec<id>)` /
  `.add(op: impl Into<NumOp>, vec![id]) ` / `.finish(out_id) -> Graph<NumOp>`. (`graph.rs:113`)
- Run: `corgi::eval_graph(&g, value) -> Value` (CONSUMES arg, moves to last use). `graph.rs:72`.
- Typecheck: `corgi::shape_of(&g, &input_shape) -> Result<Shape,String>`; `shape_of_value(&v)`.
- Template for the compiler: corgi's own `frontend/ml.rs:479 lower(e, env, b)` — env maps
  binders→node ids; near-identical shape to what I need.
- `Value` ctors: `Value::u64(Vec<u64>)`, `Value::u8`, `Value::List(bounds.into(), Box<Value>)`,
  `Value::Prod(Vec<Value>)`, sum via `Value::sum`/`sum_from_prim`. Lit broadcasts vs an input node.
- i64: `corgi::{enc_i64, dec_i64}`; signed order via `ArithOp::ToSigned` ("signed"). Spike v1 can
  assume small non-negative ints (plain u64) and add enc/dec when needed.

### Term → corgi op map (from `frontend/mod.rs:83 resolve` + `ops/`)
| DDIR `Term`/op | corgi |
|---|---|
| `Var(i)`/`Bound(k)` | env lookup (node id); Var absolute, Bound from innermost |
| `Int(n)` | `Op::Lit(Value::u64(vec![n as u64]))` (broadcast vs input node) |
| `Tuple` / `Spread` | `b.tuple(ids)` / splice fields |
| `Proj(t,i)` | `Op::Field(i)` (Prod) **or** `get`/`Gather` (List) — shape-directed |
| `Inject(tag,pay)` | `Op::Inject(tag,arity)` — const tag only (data-driven tag = stretch) |
| `Case{arms,default}` | `Op::MapSum(arms)` + `Op::Unwrap` (cf ml.rs Match); default = stretch |
| `Fold{list,init,step}` | pair `(init,list)` then `Op::Fold(body)`; mind binder order |
| `If{c,t,e}` | masks + `Op::Select` (mask,then,else) — evaluates both arms (SIMD predication) |
| `Unary(Neg)` | `ArithOp::Neg(U,64)`; `Len`→`Op::Len`; `Not`/`IsTag` = compose |
| `Binary(Add/Sub/Mul)` | `ArithOp::Bin(op,U,64)` |
| `Binary(Eq/Ne/Lt/Le)` | `CmpOp::Rel(Pred::_)`; `Gt/Ge` = swapped; `And/Or` = compose (mul/max) |
| `Hash` | NO corgi op → light corgi mod, or compose mul/shr/and (stretch) |
| `Reducer::{Min,Distinct,Count,Collect}` | `Min` / `DedupList` / `Len` / `SortList`+group |

Most of LinearOp's map/filter core (Proj/Tuple/Int/Binary/If) maps 1:1 → start there.

---

## Decision log
- 2026-06-28: isolated worktree (not Frank's checkouts); corgi as path dep; spike code hosted in
  `interactive/` reusing real `Term`/`ir::eval` (no duplication, stays honest, "in DDIR" per ask).
- 2026-06-28: corgi-as-closure (rungs 1–3) works on existing arrangements via per-call transcode;
  corgi-columnar CHUNK (rung 4) removes the transcode and is where the cursor-less batch matters.

## Status / next
- [x] Rung 0: env, build (cold ~10s / incremental ~0.1s), corgi API mapped, op table.
- [x] Rung 0: smoke test `interactive/examples/corgi_smoke.rs` GREEN.
- [x] Rung 1: `examples/corgi_bench.rs` — `infer_shape` (data→Shape), `transcode` (AoS→SoA),
      `compile(Term→Graph<NumOp>)` for the map/filter core (Var/Bound/Int/Tuple/Proj/Binary/If),
      crossover bench. **GREEN + correctness verified vs `ir::eval`.**
- [x] Rung 2: `examples/corgi_linear_dataflow.rs` — corgi computes a DDIR Linear map **inside a
      real DD dataflow** (container-level unary operator: transcode batch→corgi cols, `eval_graph`,
      transcode back; (time,diff) pass through). Shared compiler/transcoder promoted to lib module
      `src/corgi_logic.rs`. **VERIFIED == `ir::eval` reference incl. net-zero retractions vanishing.**
      → Integration is SOUND; corgi composes with DD's incremental/differential semantics.
      (This operator pays both transcodes = the rung-1 "floor"; the ceiling needs rung 4.)
- [x] Rung 4 (measurement): `examples/corgi_chain.rs` — K-op chain, interp vs per-op-transcode
      vs corgi-columnar. **GREEN + correctness.** corgi-columnar storage scales ~flat with depth.
- [x] Q3 (partial, sufficient for verdict): List + Fold built & validated (`examples/corgi_fold.rs`,
      5.4× over interp, correctness OK). Variant/Case/Hash mapped but not built (see VERDICT).
- [x] **Phase-1 VERDICT written (top of file). Measured viability reached.**

### Phase 2 status (build the corgi backend; spec at top under "PHASE 2")
- [x] Studied references: `Backend` trait, `chunk/vec.rs` (Chunk+Cursor), `RecordedUpdates`
      (`columnar/collection/container.rs`), container trait surface. Design locked (small surface).
- [x] M1: `src/corgi_backend.rs` (`CorgiContainer<T,R>` = corgi key/val cols + Rust time/diff;
      `Accountable/Default/Clone`, `from_updates`/`into_updates` boundary transcode) +
      `examples/corgi_backend_linear.rs` (a real DD dataflow: ingest→K×corgi-Project→egress).
      **GREEN, == interpreter.** 8 chained Linear ops, **2 transcodes total (ingest+egress), ZERO
      between operators** (vs 2K in the per-op model). Container surface confirmed minimal:
      `CapacityContainerBuilder<C>` needs only `C: Accountable+Default`; give_container ships whole
      prebuilt blocks through `unary`/`Pipeline`. corgi `Value` self-describes (`shape_of_value`),
      so the container needn't carry shapes. (Inline the K-chain in the dataflow closure — a generic
      `fn over Stream<G,_>` trips timely's Stream-lifetime / `Scope` struct-vs-trait.)
- [x] M2(a): `examples/corgi_reduce.rs` — corgi EXPRESSES reduce (`GroupKey` + per-group fold via
      `MapList`/`ArithOp::Reduce`) — sum-per-key & min-per-key == reference. GREEN. The per-key
      reduce logic is validated; M2(b) makes it incremental.
- [ ] M2(b): incremental corgi arrangement — **DECISION: Route B (cursor-less + tactic)** (Frank,
      2026-06-29). ← **NEXT**. Sub-plan:
      - M2b-1: `CorgiBatch` (corgi key/val cols + Rust time/diff, sorted/consolidated, +
        `Description`) impl `BatchReader` (NO Cursor) + `Batch`/`Merger` (merge two batches & advance
        times to compaction frontier, fuel-bounded) — the intricate core; test in isolation
        mirroring `chunk/vec.rs`'s merger tests.
        - [x] **Corgi light mod DONE** (branch `dd-arrange-api` in `wip/corgi`): exposed
          `corgi::arrange::{gather, gather_lanes, compare_at}`. `compare_at` = scalar wrapper over
          `compare_idx` (new, `ops/cmp/order.rs`); `mod order` → `pub(crate)`. **All 45 corgi tests
          pass; interactive builds against it.** NOTE: `sort_perm`/`concat` are `#[cfg(test)]`-gated
          → NOT used. Build a sort via Rust `indices.sort_by(|i,j| compare_at(kv,i,kv,j))`; build
          merge output via `gather_lanes(&[Some(&kv1),Some(&kv2)], tags, off)` (avoids `concat`).
          corgi structural order = leaf, Prod field-wise, List LENGTH-FIRST, Sum tag-then-payload —
          a total order; consistent use is all correctness needs (compare as multisets, not vs DDIR Ord).
        - [x] **`CorgiBatch` + merge DONE & TESTED** (`src/corgi_arrange.rs`): `CorgiBatch<T,R>`
          (corgi key/val cols + Rust time/diff + `Description`) impl `BatchReader` (no Cursor);
          `from_unsorted` (sort via `compare_at` + `gather`, consolidate); `merge` (two-pointer over
          (k,v) groups via `compare_at`, per-group advance-to-frontier + consolidate in Rust, output
          via `gather_lanes`). **Property test: 300 random merges == reference (union+advance+
          consolidate); sort/consolidate test passes.** The intricate incremental core works over
          corgi-native columns. (Eager merge — fuel/grading sophistication deferred; correctness first.)
- [~] M2b-2: wire `CorgiBatch` into the DD trace machinery.
      - [x] `Batch` + `Merger` (`CorgiMerger`, eager wrapper over `merge`) DONE & TESTED
        (`merger_trait_drives` passes). KEY: `Batch`/`BatchReader`/`Builder` are blanket-impl'd for
        `Rc<B>` (`trace/mod.rs:327` rc_blanket_impls), so `Spine<Rc<CorgiBatch>>` exists for free.
      - [ ] `Builder` (DD trait) + `Batcher` + `arrange_core` wiring — the fiddliest plumbing
        (Batcher/Chunker generics; `arrange_core` at `operators/arrange/arrangement.rs:346`).
- [x] M2b-3 (mechanism, de-risk): `examples/corgi_reduce_trace.rs` — Route-B reduce MECHANISM
      verified end-to-end over a manual corgi trace. Accumulate `CorgiBatch`es via `merge`
      (consolidated per (key,val)); corgi **Count** reducer (GroupKey + `fold_add` over the diff
      column — retractions just subtract; i64-as-u64 add is bit-correct). 4 steps incl. retraction
      to zero (key vanishes) + multi-val key — **each step == reference. GREEN.** The full Route-B
      reduce mechanism (trace-maintenance + corgi reduce) works; only the live-operator wiring remains.
- [ ] M2b-2-finish + live tactics: **DECISION (Frank 2026-06-29): keep looping, do the FULL tactic
      wiring** (arrange_core + reduce_with_tactic/join_with_tactic — the proper Route-B Backend). ← **NEXT**
      Sub-plan (test each piece; expect several iterations; all mechanisms already proven):
      - [x] **`Builder` DONE & TESTED** (`CorgiBatchBuilder`, `builder_builds_corgi_batch` passes):
        `Input = Vec<((key,val),T,R)>` (rows), `Output = CorgiBatch`; `done`/`seal` transcode the
        sorted chain → corgi columns via `build_batch` (`infer_shape`+`transcode`+`from_unsorted`).
        `RcBuilder<CorgiBatchBuilder>` → `Rc<CorgiBatch>` (blanket).
      - **DESIGN DECISION:** `Ba::Output = Vec<((key,val),T,R)>` (rows) → **reuse the existing
        `MergeBatcher` + its vec-row `Merger`** (`merge_batcher.rs:283`) as the Batcher (big
        simplification). Cost: one transcode rows→corgi at the Builder (arrangement-ingest boundary,
        once per batch) — NOT operator-to-operator. The reduce/join tactics read corgi columns from
        the stored `CorgiBatch`, so the corgi-native compute property holds. (A corgi batcher-side
        Merger to drop even the ingest round-trip is a later optimization.)
      - [x] **Batcher + chunker + `arrange_core` DONE — COMPILES & RUNS**
        (`examples/corgi_arrange_smoke.rs`). `impl DrainContainer for CorgiContainer` (drain =
        untranscode to rows) lets the stock `ContainerChunker<Vec<Upd>>` chunk a corgi-container
        stream; Batcher = `MergeBatcher<VecMerger<(Value,Value),T,R>>` (reused, like ord_neu);
        Builder = `RcBuilder<CorgiBatchBuilder>`; Trace = `Spine<Rc<CorgiBatch>>`. Full 6-generic
        `arrange_core` wiring compiles and a 1000-update arrange runs clean. **The arrangement is wired.**
      - [!] `CorgiReduceTactic::retire` — **WALL HIT (2026-06-29), surfaced to Frank.** DD's
        incremental reduce core is `HistoryReplayer` (interesting-times algorithm, `reduce.rs:448-758`,
        ~300 lines of DD's subtlest logic) and it is **cursor-coupled**: `compute` takes
        `C1/C2/C3: Cursor`, `replay_key` walks cursors, and the default `CursorTactic` even *requires*
        `B1: Navigable` (`reduce.rs:258`). A cursor-less Route-B reduce ⇒ reimplement that
        interesting-times machinery over corgi columns — DD's hardest code, very high-risk to get
        right unattended (partially-ordered iterative times). M2b-3 proved the SINGLE-TIME reduce
        mechanism; the general multi-time incremental `retire` is the gap. Options for Frank: (a)
        simplified single-time/totally-ordered CorgiReduceTactic (tractable, covers many DDIR reduces,
        documents the limit); (b) HYBRID — give CorgiBatch a thin cursor, reuse the proven
        `HistoryReplayer`/CursorTactic with corgi as the per-key `logic` (correct + general, but
        reintroduces a cursor + per-key-slice transcode); (c) Frank designs the cursor-less `retire`.
        NOTE: JOIN is likely more tractable cursor-less (bilinear, no interesting-times).
        **DECISION (Frank 2026-06-29): option (c) — Frank designs the cursor-less `retire`.** Reduce
        operator PAUSED pending his design; the M2b-3 mechanism + the arrangement are ready for it.
      - [x] **JOIN MECHANISM DONE & TESTED** (`examples/corgi_join_mechanism.rs`): cursor-less
        equijoin compute — merge-join two corgi batches' key columns via `compare_at`, cross-product
        matched val runs via `gather`, multiply diffs (Rust). 300 randomized cases + a directed case
        == reference hash-join, incl. retractions (negative product diffs). GREEN.
      - [~] `CorgiJoinTactic::{defer,work}` + `join_with_tactic` — wrap the mechanism in the tactic:
        - [x] **Skeleton COMPILES** (`src/corgi_join.rs`): `CorgiJoinTactic<T>` + `CorgiDeferred<T>`
          + `defer` (queues bilinear units to todo0/todo1 by `Fresh`, sets advance flags) + stub
          `work`, as a valid `impl JoinTactic<Rc<CorgiBatch>, Rc<CorgiBatch>, CB>`. Bound gotchas
          resolved: `CB` is the **composite** `timely::ContainerBuilder` (not `timely::container::`),
          + `CB: PushInto<((Row,Row),T,Diff)>` for `session.give`; structs need `T: Timestamp`
          (hold `Capability<T>`). The generic-bound wiring is validated.
        - [x] **`work` DONE + JOIN LIVE IN A REAL DATAFLOW** (`examples/corgi_join_dataflow.rs`):
          `work` merges each side's batch list (`merge_one`), merge-joins by key (`compare_at`),
          cross-products matched val runs (`gather`) with `t0.join(t1)` + `d0*d1`, runs the projection
          corgi program (`compile_join_projection`, Var0=key/Var1=val0/Var2=val1), and emits via
          `session_with_builder(&cap).give(((k,v),t,d))`. Wired with `join_with_tactic` over two
          `arrange_core` corgi traces. **Output == reference hash-join incl. a retraction** (net-zero
          `(2,6)` excluded → `{(1,110),(1,120),(2,205),(2,305)}`). **The cursor-less corgi JOIN
          operator works end-to-end over corgi-native storage + compute via the #771–773 seam.**
      - [ ] (old) `CorgiJoinTactic::{defer,work}` + `join_with_tactic` — wrap the mechanism in the tactic:
        `defer` queues bilinear units (Vec<B0>×Vec<B1>, by `Fresh`) + capability + advance flags;
        `work` drains under fuel, runs the join mechanism per unit, joins times via the lattice +
        multiplies diffs, emits via the `OutputBuilderSession`/`JoinSession` (capability). Bilinear:
        accumulate each side's batch list (merge to one run first). Test join in a real dataflow vs
        reference. The output-session/capability + batch-list mgmt is fiddly timely plumbing —
        surface if too subtle. ← **NEXT**
      - M2b-2: `Batcher`/`Builder` + a `Spine<Rc<CorgiBatch>>`; `arrange` a collection.
      - M2b-3: `CorgiReduceTactic::retire` runs the M2(a) reduce program in bulk over batches;
        `reduce_with_tactic`; test incremental reduce (with retractions) vs reference.
      - M2b-4: `CorgiJoinTactic` (defer/work); `join_with_tactic`; test vs reference.
- [ ] M3: `impl Backend for CorgiBackend` + `render_tree` a real program vs `evaluate`.
  - PARTIAL BACKEND (Frank's post-join step): Linear (M1) + arrange (M2b-2) + join (M2b-4) all work
    live; reduce = Frank's design. A `Backend` impl with `reduce` stubbed (`todo!()`) + `render_tree`
    on a reduce-free DDIR program (map/filter/join) would show corgi composing through the real DDIR
    backend path. Full M3 (with reduce) awaits Frank's `retire`.
    - [x] **Container bounds DONE** (`src/corgi_backend.rs`): `Negate` (negate diffs), `Enter<T,T>`/
      `Leave<T,T>` (identity — DDIR same-Time dynamic model), `ResultsIn<T::Summary>` (map times via
      `step.results_in`, drop `None` rows via `gather` of the corgi cols) — all generic, time/diff in
      Rust. `CorgiContainer` now satisfies the `Backend::Container` bound. COMPILES.
    - [x] **`Backend` impl SHAPE COMPILES** (`src/backend/corgi.rs`, `pub mod corgi` in backend/mod.rs):
      `enum CorgiBackend` with `Container = CorgiContainer<Time,Diff>`, `Arr = Arranged<TraceAgent<
      Spine<Rc<CorgiBatch<Time,Diff>>>>>`. `arrange` real (the arrange_core stack); linear/join/
      as_collection/reduce/inspect/leave_dynamic = `todo!()`. A `render_tree::<CorgiBackend>` wrapper
      compiles → **the full Backend wiring + render_tree bounds are validated.** Note: `leave_dynamic`
      is Vec-specific in DD (`dynamic/mod.rs:40`, truncates per-record times) → corgi needs a custom
      unary over `CorgiContainer.times`.
    - [x] **`linear` + `join` bodies DONE (compile).** `linear` = container-level fold of LinearOps
      (`apply_ops`: Project=corgi `eval_graph`, Filter=corgi mask+`gather`, Negate=Rust;
      EnterAt/LiftIter/FlatMap=todo!). `join` = `join_with_tactic`+`CorgiJoinTactic` → Vec-rows →
      ToCorgi unary → `Collection<Time,CorgiContainer>`. **CorgiBackend now implements ALL
      non-reduce operators (linear/arrange/join real); reduce/as_collection/inspect/leave_dynamic =
      todo!.** Compiles.
    - [ ] REMAINING for an end-to-end `render_tree::<CorgiBackend>` run (pre-reduce PLUMBING):
      `leave_dynamic` (custom unary truncating `CorgiContainer.times`), a corgi `evaluate` harness
      (mirror `vec.rs:182` — ingest/iterative/render_tree/leave/capture/convert), `as_collection`
      (cursor-less, if exports hit it), + a test program. NOTE: this harness will integrate with
      Frank's reduce design, so best coordinated with it.

## ★ PERF (2026-07-01): END-TO-END corgi vs vec — nuanced; compute wins, arrange loses ★
`examples/corgi_perf.rs` — `corgi::evaluate` vs `vec::evaluate`, median wall-clock, single worker,
release. (Phase-1's 50–135× was EVAL-ONLY microbench; end-to-end the scalar eval is a small slice.)
```
scenario              n         vec        corgi      verdict
linear-compute-8x     100k     157.6ms     48.6ms     corgi 3.2x FASTER   (8 wide maps, NO arrange)
linear-compute-8x      20k      31.0ms      8.7ms     corgi 3.6x FASTER
linear-heavy          100k     113.4ms    136.0ms     corgi 1.2x slower   (3 maps+filter+ARRANGE)
linear-heavy           20k      21.4ms     23.3ms     corgi 1.1x slower
reach                 4000      13.9ms     43.6ms     corgi 3.1x slower   (recursion: join+distinct)
reach                 1000       3.6ms     11.6ms     corgi 3.2x slower
reach                  200       0.86ms     2.0ms     corgi 2.4x slower
```
**Read:** (1) corgi's columnar scalar logic is a REAL end-to-end win (~3.2–3.6×) when scalar compute
dominates and data stays columnar between ops (deep linear chain, no arrange) — even paying transcode
in/out. (2) But the ARRANGEMENT machinery dominates real programs: add ONE arrange to the linear chain
and the 3× win collapses to ~parity (the arrange is the swing factor — same chain, ±arrange). (3)
Recursive/join-heavy programs are 2–3× SLOWER. WHY corgi loses on arrange-heavy/recursive:
- **corgi's arrange is correctness-first, unoptimized:** `CorgiMerger` does the whole merge on the first
  `work` call (no fuel/batching), the sort is a discrimination sort over corgi columns, and every merge
  re-materializes columns via `gather_lanes`. DD's `ValSpine` (vec backend) is a mature fueled spine.
- **join/reduce run ROW-WISE anyway:** the join tactic emits via `session.give` per row; reduce runs
  the DDIR reducer in Rust over per-key slices + `active_times` OVER-derives interesting times. So corgi
  pays transcode/untranscode at these boundaries with NO columnar compute win to offset it.
- **eval isn't the bottleneck** in these programs (narrow rows, few arith ops/key), so accelerating it
  (corgi's strength) barely moves end-to-end wall-clock, while the transcode tax is pure overhead.
**Takeaway:** the corgi-as-scalar-logic thesis is validated where it should hold (compute-bound wide
linear work: 3×+), but a corgi backend only beats vec end-to-end if the arrangement layer is also made
competitive (fueled merge, keep join/reduce columnar) — otherwise arrange/transcode overhead dominates.
Next perf levers, in impact order: fueled/batched `CorgiMerger`; columnar join emit (build corgi cols
directly, skip `session.give` row round-trip); columnar reduce (avoid untranscode per key). ----

## ★★★★ M5 DONE (2026-06-30): ALL 6 CANONICAL PROGRAMS RUN == vec (Sum rung landed) ★★★★
binders now passes — columnar Variant (the Sum rung) is implemented. The corgi backend renders every
canonical `.ddp` (`reach, scc, stable, unnest, adt, binders`) identically to vec. 32 interactive tests
(+4 Sum round-trip), all 5 corgi dataflow examples, 27 corgi tests — all green.

**The Sum rung (DDIR `Variant` ↔ corgi `Value::Sum`), all in `corgi_logic.rs`:**
- `infer_shape_cols(rows)` — NEW all-rows column scan (vs `infer_shape`'s single sample): a `Variant`
  column's `Shape::Sum` is the union of every arm that appears (a sample shows only one tag), so it
  scans, building one lane per `0..=max_tag` (absent tag → `None` = ⊥). `from_updates` now uses it.
- `transcode` `Sum` arm: per-row tag column + one packed lane per committed variant (its rows in row
  order); `Value::sum_opt(tags, lanes)` derives each row's within-lane offset; absent arms stay ⊥.
- `untranscode` `Sum` arm: untranscode each committed lane, then per row pull its payload from its lane
  at the recorded OFFSET (robust to gather/merge reordering — not a sequential cursor). Uses the public
  `Value::into_sum`/`sum_opt` — NO corgi mod needed (engine already had Sum gather/compare/merge).
- transcode/untranscode are now TOTAL over `Shape` (Prim/Unit/Prod/List/Sum) — dead catch-alls removed.
In binders the Sum column is created by the row-wise `key` Project (`Node(list ...)`) and consumed one
op later by the row-wise `map`'s `case` (into_updates→ir::eval), so its full lifecycle is transcode+
untranscode; merge/gather over Sum columns aren't exercised by these programs (corgi engine supports
them, but that path is unverified here). `LiftIter` is implemented but untested (no program uses it).
**No remaining backend todo!s; Variant no longer a gap.** ---- previous milestones below ----

## ★★★ M4 (2026-06-30): REAL CANONICAL PROGRAMS RUN — 5/6 == vec backend ★★★
All backend `todo!`s cleared; the corgi backend now runs real `.ddp` programs (incl. RECURSIVE ones)
end-to-end and matches vec. `examples/corgi_progs.rs` (PROG=<name> or all) + `examples/corgi_reach.rs`:
```
reach   OK   (recursion: feedback var + join + distinct + concat + leave_dynamic)
scc     OK   (recursion + enter_at + min + nested scopes + negate)
stable  OK   (recursion + min + arrange)
unnest  OK   (flatmap + collect: List round-trip through the relational layer)
adt     OK   (con/case/Inject — variant is transient, eliminated within one ir::eval)
binders FAIL (persists a Variant IN a collection → needs columnar Sum; the known "later rung")
```
**Backend todo!s cleared this milestone:**
1. **`leave_dynamic`** — custom `OperatorBuilder` unary mirroring DD's `dynamic/mod.rs:40`: strips all
   but `level-1` PointStamp coords from the capability AND each `CorgiContainer.times` entry; sets the
   input connection summary `retain: Some(level-1)` so progress tracking stays correct. UNLOCKS all
   recursive programs.
2. **`EnterAt`/`LiftIter`/`FlatMap`** — correctness-first ROW-WISE path in `apply_ops` (untranscode →
   vec-style transform → `from_updates`), parity with `backend::vec::render_linear`; `level` threaded
   in. (Columnar fast-path is future; these are corner ops.)
3. **Row-wise Project/Filter fallback** — `corgi_logic::compilable(term)` gates: terms the corgi
   compiler can't lower (List, Case/Inject, Unary, Hash) fall back to `ir::eval` (parity with vec),
   keeping the columnar fast-path for the common case. UNLOCKS unnest's `list(...)` projection + adt's
   `case`. `untranscode` gained a `List` arm (inverse of transcode: ends + flat → per-row Lists).
4. **Collect / as_collection / inspect** — already worked.

**THE BIG BUG fixed en route (affected ALL unit-valued data):** the compiler emitted an empty tuple
`Term::Tuple([])` (DDIR unit) as corgi `Prod([])` — an empty product with NO row-count witness — so
any unit key/val silently produced ZERO rows downstream (`key($0[0];)`, `key(;)`, distinct/count
outputs, concat of unit collections). Fix: `compile` emits `Op::Unit` over the anchor (a length-
carrying `Unit` column) for an empty field list. This is why `reach`/`distinct` initially returned
empty. NOTE: ill-typed CONCAT of heterogeneous val shapes (e.g. `Prod[Prim]` + `Unit`) still can't be
columnar (vec tolerates it via dynamic typing); real programs keep concat operands shape-uniform.

REMAINING (the Sum rung, clearly scoped, NOT a backend todo!): columnar Variant — `Shape::Sum` in
`infer_shape`(needs to SCAN all rows for arms, not just sample row[0])/`transcode`/`untranscode`. corgi's
engine already has Sum `gather`/`compare`/merge arms; only the DDIR transcode layer lacks them. Only
`binders` needs it (persists a variant in a collection). `LiftIter` is implemented but untested (no
canonical program uses it). ---- previous milestones below ----

## ★★ M3 DONE (2026-06-30): END-TO-END render_tree::<CorgiBackend> == vec backend ★★
A whole flat DDIR program runs through the corgi backend and matches the vec backend exactly:
```
let a = input 0 | key($0[0] ; $0[1]);
let b = input 1 | key($0[0] ; $0[1]);
let j = a | join(b, ($0 ; tuple($1, $2)));
export "join" = j;  export "count" = j | count;  export "filtered" = j | filter($0 != 2);
```
`examples/corgi_program.rs`: `corgi::evaluate(&prog,&inputs) == vec::evaluate(&prog,&inputs)` for all
three exports (join 3 rows, count 2 rows, filtered 3 rows). Exercises Linear (key/filter), join, AND
reduce(count) composed via the real DDIR `render_tree` path. All 5 corgi examples + 28 interactive +
27 corgi tests pass.

**Three issues found & fixed making the end-to-end work (all in `corgi_logic.rs`/`corgi_arrange.rs`):**
1. **`Spread` in projections** — DDIR's parser makes EVERY projection key/val a `Term::Tuple`, and a
   bare `$n` field becomes `Term::Spread(Var n)` (flat-row splice). The map/filter-core compiler didn't
   handle `Spread`. Fix: shape-directed compile — `compile`/`compile_projection`/`compile_predicate`/
   `compile_join_projection` now take `env_shapes: &[Shape]`; the `Tuple` arm splices a `Spread`'s
   `Prod` fields via `shape_of_place`. Tactics/`apply_ops` pass shapes from `shape_of_value` of the
   actual columns (so the join tactic now holds key/val `Term`s and compiles per work-unit).
2. **Empty-batch shape mismatch in `merge`** — `Batch::empty` carries a `Unit(0)` placeholder shape;
   the spine merges empties with real batches → `gather_lanes`' cross-source shape check panicked. Fix:
   substitute the non-empty side's `kv` for any empty source (never indexed, since no tag points to it).
3. **Cross-shape `Eq`/`Ne`** — `filter($0 != 2)` compares the 1-tuple key (`Prod[Prim]`) to `Int 2`
   (`Prim`); vec does structural `Value` compare (always-unequal), but corgi's `CmpOp::Rel` needs equal
   shapes. Fix: `infer_term_shape` + fold cross-shape `Eq`→0 / `Ne`→1 to a constant column.

NOTE: keys/vals are ALWAYS tuples (`Prod`) in real DDIR programs (parser wraps every projection field
list in `Term::Tuple`). The earlier hand-built examples used scalar keys; the backend handles both.
Remaining backend `todo!`: `leave_dynamic` (only needed for SUB-SCOPE/iterative programs — flat
programs never reach it), and `EnterAt`/`LiftIter`/`FlatMap` linear ops + `Collect` reducer + Variant.

## ★ PHASE 2 STATUS (2026-06-29): corgi backend — operators LIVE except reduce ★
Built & tested, all over corgi-native storage + `eval_graph` compute, cursor-less (Route B):
- **Linear** — corgi-native container + map/filter in a real dataflow, no inter-op transcode (M1).
- **arrange** — `arrange_core` → `Spine<Rc<CorgiBatch>>` (CorgiBatch: BatchReader/Batch/Merger/Builder;
  merge 300-case property test; reused `MergeBatcher`; one rows→corgi transcode at ingest-boundary).
- **join** — `CorgiJoinTactic` + `join_with_tactic`, LIVE in a real dataflow == reference (incl.
  retraction); merge-join + cross-product + projection all over corgi columns via the #771–773 seam.
- **reduce** — ✅ **DONE (cursor-less, live)** via Frank's `active_times`: `CorgiReduceTactic`
  (`src/corgi_reduce.rs`) + `reduce_with_tactic`, wired in `Backend::reduce`. retire = per changed
  key → `active_times` (over-deriving join-closure) → at each active time accumulate input-as-of-t,
  run the reducer (Rust DDIR reducers, matching vec), diff vs (output + emitted-so-far), route to
  held buckets; pended→held frontier. Test `examples/corgi_reduce_dataflow.rs`: directed Count
  (retraction-to-zero) + **120 randomized Count == reference** + Min/Distinct (min changes under
  retraction). Needed a 1-line corgi mod: `Unit` arm in `compare_idx` (Distinct's unit payload).
- corgi light mod: `corgi::arrange::{gather, gather_lanes, compare_at}` (branch `dd-arrange-api`; 45 tests pass).
Verdict: Route B (cursor-less corgi batch + custom tactics) is VALIDATED for **all four operators —
Linear, arrange, join, AND reduce** — each live in a real dataflow over corgi-native storage via the
#771–773 seam, tested vs reference. `Backend for CorgiBackend` implements linear/arrange/join/reduce
(as_collection/inspect/leave_dynamic still todo!, only needed for an end-to-end render_tree harness).
Files: `interactive/src/corgi_{logic,backend,arrange,join,reduce}.rs` + `backend/corgi.rs`; corgi
mods on branch `dd-arrange-api` (`arrange` facade + `compare_idx` Unit arm).

### M2(b) FORK — the incremental arrangement (the big build); routes:
- **Route A (Chunk trait, à la `chunk/vec.rs`):** impl `Chunk` over corgi columns → free
  Batcher/Builder/Spine + reuses DD's fueled merge/advance/settle harness. BUT requires a `Cursor`
  (Navigable), and `Cursor`'s assoc types are `BatchContainer`/`Layout`-bound (`chunk/vec.rs` leans
  entirely on `Vector<((K,V),T,R)>`). corgi columns are NOT `BatchContainer`s → heavy glue
  (impl BatchContainer for corgi leaves, or store a parallel DD-container copy for navigation).
- **Route B (cursor-less, the #771–773 seam):** corgi batch = `BatchReader` only (len/description,
  NO Cursor) + a custom `ReduceTactic`/`JoinTactic` that reads corgi columns in BULK (eval_graph,
  GroupKey) + `Batch`/`Merger` over corgi columns (merge/advance via corgi sort). Avoids the
  BatchContainer glue entirely (plays to corgi's bulk nature) BUT reimplements the incremental
  merge/advance + the tactic defer/work/retire protocols.
- Both are large + intricate (fueled merge, frontier/capability mgmt) — the high-risk core where
  Frank's review matters. Recommendation: **Route B** (genuine use of the cursor-less tactic seam;
  corgi is bulk-columnar, not step-navigated; sidesteps the Cursor↔BatchContainer impedance).
  Trade: reimplement incremental merge vs fight the container glue. Awaiting Frank's routing.

### RUNG 4 RESULTS (chain of K Linear `x->x+c` ops, n=100k, ns/row over whole chain)
| K | interp | per-op xcode | columnar | col vs interp | col vs per-op |
|---|---|---|---|---|---|
| 1 | 25.7 | 5.8 | 2.9 | 9.0× | 2.0× |
| 4 | 75.0 | 14.2 | 5.0 | 15.1× | 2.9× |
| 16 | 282.9 | 44.8 | 12.6 | 22.5× | 3.6× |

**Conclusion:** columnar ≈ `2·xcode + K·eval` (flat-ish); per-op ≈ `K·(2·xcode + eval)`; interp
steepest. So the **deeper the program, the bigger the corgi win**, and storing data corgi-columnar
(amortizing transcode across ops) beats per-op transcode 2–3.6× and widening. Strong case for a
corgi-data CHUNK/container once the scalar-logic path exists.

### ARCHITECTURE FINDING — where corgi-columnar data should live
- The `Chunk` trait (arrangement storage, #744) **requires `Navigable<Cursor: Cursor>`**
  (`trace/chunk/mod.rs:73`). So a corgi-columnar *arrangement via `Chunk`* must still implement a
  cursor — it is NOT the "cursor-less batch." Route A (via `Chunk`): write a corgi cursor + the 5
  chunk ops (merge/extract/advance/settle/bounds), get Batcher/Builder/Spine free; add a bulk
  columnar accessor for the corgi fast path. Most reuse; you write a cursor.
- The genuinely **cursor-less** path is the operator layer (#771–773): `join_with_tactic`/
  `reduce_with_tactic` need only `BatchReader` (`len`/`description`), not `Navigable`. Route B: a
  corgi container holding columns + `Batch`/`Merger` over corgi's own sort/merge + a custom tactic
  that bulk-applies corgi. Maximal "no cursor"; you reimplement merge/advance + the tactic.
- For Frank's stated goal (corgi = the *scalar logic*, not the navigator), neither is needed to
  START: rungs 1–3 run over ANY existing arrangement via per-call transcode. corgi-columnar storage
  (A or B) is the *optimization* that removes the transcode the rung-4 bench shows dominates.
- Rung 3 (reduce) DEPRIORITIZED: the reduce closure already receives a per-key column slice
  `&[(val,diff)]` (`reduce.rs:651`), so corgi-as-reducer is the *same* mechanism proven in rungs
  1–2; building it adds marginal evidence vs the CHUNK question Frank explicitly asked.

### RUNG 1 RESULTS (release, non-negative ints, rows = Tuple([a,b]))
| metric | map (a+b, a*3) | filter (a<b) |
|---|---|---|
| crossover (corgi eval beats interp) | ~10 rows | ~10 rows |
| corgi eval-only peak speedup | **79× @10k** | **135× @100k** |
| corgi eval ns/row (≥1k) | 0.8–1.6 | 0.3–0.6 |
| interp ns/row | 61–73 | 37–40 |
| **with per-call transcode** | 6–11× | 4–7× |
| transcode share of corgi-total @1M | ~85% (1.6 vs 12 ns/row) | ~95% (0.6 vs 10.6) |

**Conclusions (measured):**
1. corgi-as-columnar-scalar-logic is a **large win** — 50–135× on eval past the ~10-row crossover.
   Any real batch (100s–1000s of rows) is far past crossover. Below ~10 rows corgi loses to
   `eval_graph` per-call setup (n=1: 0.23–0.27×).
2. **The transcode (`&[ir::Value]`→corgi columns) dominates** the corgi-as-closure model — ~10
   ns/row, ~85–95% of total at scale. So in the naive per-call-transcode form the win is capped at
   ~4–11×. **To unlock the full 50–135×, data must already be corgi-columnar → motivates the
   corgi-data CHUNK (rung 4); the per-call transcode is exactly what storing columns removes.**
3. Compile is direct: the map/filter core is ~1:1 onto corgi ops; `infer_shape` handles the
   dynamic typing (Shape from data). No corgi modification needed for this subset.

### Open questions
1. ~~Crossover width~~ → ~10 rows. (ANSWERED)
2. ~~Transcode cost~~ → dominates (~85–95% at scale). (ANSWERED — drives rung-4 case)
3. How rich a `Term` subset for real programs (datalog/eqsat need Variant/List/Fold/Hash)? (OPEN)
4. Does it run inside a real DD dataflow, and does corgi-columnar storage actually remove the
   transcode in that setting? (rung 2 → rung 4)

---

## ★ CHUNK BACKEND + TRANSCODE REMOVAL + BOUNDARY MODEL (2026-07-02)

Branch `corgi-chunk` off `master-next` (has #778 Chunk/NavigableChunk split). corgi on
`dd-arrange-api`. Both pushed to frankmcsherry forks. Correctness gate: `corgi_progs` (all 6
canonical programs vs `vec`) after EVERY change; `corgi_scorecard` = per-operator triage.

### Result — measured, per operator (corgi/vec, scorecard)
`CorgiChunk: Chunk` (cursor-less) is the arrangement; the fueled `ChunkBatchMerger` comes free.
Removing the columns↔rows transcode boundaries (they were self-inflicted, not inherent):
- `as_collection` reads chunk columns straight into a CorgiContainer (no untranscode).
- join emits corgi columns via `give_container` into a `CapacityContainerBuilder<CorgiContainer>`
  (no untranscode + per-row give + re-transcode; no `JoinToCorgi` unary).
- arrange ingest: `CorgiChunker` sort-consolidates each input container's columns directly into
  CorgiChunks (replacing `ContainerChunker`'s drain-to-rows), ACCUMULATING to TARGET so it emits
  few large chunks (else columnar per-chunk set-up dominates on many small batches).

| operator | before | after |
|---|---|---|
| map8 / filter (no arrange) | 0.30x / 0.75x | unchanged — columnar eval wins |
| arrange | 1.5x | 0.90x (BEATS vec) |
| join | 1.5x | 0.70x (BEATS vec) |
| reduce_distinct/count | 2.0x | 1.30-1.35x (reduce still row-wise) |
| reach | 1.9x | 1.45x |

Thesis validated: vec spends ~22% of reach in `Value::cmp`/`partial_cmp` (pointer-chasing nested
enums); corgi ~5% (columnar `compare_idx`). corgi beats vec wherever it stays columnar; it only
loses where it drops back to `Value` rows — now just the reduce.

### Multi-record primitives exposed in corgi `arrange` (dd-arrange-api)
`sort_perm` (discrimination argsort), `compare_idx` (batched compare), `find_ranges` (single-row
`find` = equal-range probe → multi-record merge-join/semijoin). Fixed a real corgi bug:
`Reduce(Red::Add)` used checked `.sum()` while its Scan sibling wraps — made it wrapping so raw
two's-complement diff sums are correct.

### Reduce: proven building blocks, NOT yet wired (row-wise reduce still runs)
- `columnar_sum_by_key` (Count) and `columnar_distinct_keys` (two-level (key,val) consolidate) —
  pure corgi ML (`group -> map(fold_add) -> filter`) via `parse_ml`+`eval_graph`, tested.
- Count fast-path landed (skip needless per-value consolidation): reduce_count 1.39x->1.30x.
- Diffs cross as RAW two's-complement u64 bits (`ne 0` drop = sign-agnostic). "present" simplified
  to net!=0 (ignore signed sign) -> keeps everything on the raw-bit path, no signed encoding yet.
- `group_offsets(key_col) -> (perm, ends)` — integers-only boundary primitive, tested.

### ★ DESIGN CAPTURE — a data-blind reduce tactic (the reusable DD asset)
A framework that presents outward as (int-id, time, diff) can supply a reduce tactic that owns ALL
the time navigation, backend-agnostic. Only integers cross the boundary; the value never becomes a
`Value` in DD.

Tactic (DD) owns — the hard part, written once: interesting-times (`active_times`), wave/interval
navigation, per-(key,interesting-time) delta (desired vs current output), `pending`, held
capabilities, output-trace maintenance — all over `(key-id: u64, val-id: u64, time: T, diff: R)`.

Backend (corgi) supplies:
1. present-as-ints (input): each record -> (key-id, val-id) as u64 hashes; times/diffs DD-native.
2. value callback (consolidate): given input record indices (a key's rows <= t), return reduced
   output as `[(out-val-id, diff)]` — backend runs its columnar value logic. Reducer semantics live
   here. (`group_offsets` + `gather` + the consolidate blocks are the pieces.)
3. mint output ids: Min/Distinct REUSE ids (min = an input val-id; distinct = the unit-id) — easy.
   Count/Collect create NEW objects -> mint an id. HASHING is the id function: same output value ->
   same id, globally consistent across the output->input boundary (a reduce output is a downstream
   input). Collisions = accepted risk. This is why ids should be HASHES not per-batch indices —
   indices are stable only within a batch; hashes are stable everywhere without a registry.
4. materialize (egress): id -> value to build the output arrangement (or build it columnar by id).

The one structural cost: cross-retire `pending` must be keyed by the stable hash-id, not a
per-retire group index — this ripples through the delta logic, so the columnar reduce is a REDESIGN
of `retire` into id/hash space, not a patch. Also carries the workload trade-off: columnar
consolidate wins on wide/batch reduction but must PRESERVE the changed-key restriction (columnar
semijoin) so small-delta recursion (reach) doesn't regress to O(accumulated x rounds).

Next effort (well-scoped, primitives ready): rewrite `retire` in id/hash space — read changed keys'
input as columns, `group_offsets` -> per-group `active_times` -> per (group,wave-time) hand corgi
the include-index list -> consolidate -> delta/emit keyed by hash -> columnar output.
