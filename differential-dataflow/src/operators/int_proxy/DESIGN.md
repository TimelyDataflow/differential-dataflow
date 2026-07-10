# `int_proxy`: settled design and follow-ups (#781)

The settled design of the integer-proxy seam, and the non-blocking follow-ups that
remain. The correctness gates and landing items that blocked #781 have closed — their
conclusions graduated into the module docs (`mod.rs`, trait docs) and the code, and their
history lives in git. This is the living record of what is settled and what follows, in the
manner of `formal/DESIGN.md`. File/line references are to this branch unless noted.

## 1. What this is

A boundary where only integers cross between DD's operator logic and a storage
backend: records present as `((key_hash, value_id), time, diff)`, DD runs all
lattice/time reasoning over the integers, and the backend supplies value semantics
through callbacks (`present*`, `reduce_many`, `materialize`, `cross`). The full
contracts for the two integers live in `mod.rs`; this document does not repeat them.

The work publishes **two extension tiers**, and implementors should self-select:

* **Backend tier** (`ProxyReduceBackend`, `ProxyJoinBackend`): you own value
  semantics — hashing, the value function, building real columns from ids. DD owns
  the operator algebra. The safe tier; a working backend is a few hundred lines.
* **Tactic tier** (`ReduceTactic`, `JoinTactic`, now `pub`): you own the operator
  algebra — interesting-time completeness, capability discipline, description
  tiling. Publication is intentional (we want others to be able to implement
  tactics), which is precisely why its contract is written down (in the trait docs and
  the shared-driver asserts, done as #789) and why its formal spec
  (`formal/Differential/Model.lean`) is the document of record as it sharpens.

## 2. Settled semantics

* **S1 — `key_hash` is an independence marker, not identity.** "Key = key" is
  deliberately downgraded to "key = coarse partition of keys": equal keys always
  share a cell (the partition is sound for scheduling, the changed-key restriction,
  and `pending` bookkeeping), but a cell may contain colliding keys. Both operators
  inherit this identically. **Exactness is recoverable backend-side**, because the
  backend always sees real records: a join backend can verify real-key equality per
  matched pair at `cross` (dropping fabricated pairs is sound — their times/diffs
  are simply never emitted); a reduce backend can partition brackets by real key
  and mint key-qualified value ids (`vid = hash(key, value)` or per-`(key, value)`
  ordinals), after which the `(key_hash, vid)` bookkeeping tracks `(key, value)`
  exactly. The unrecoverable residue is granularity only: a cell's keys share
  pending times and the changed-key restriction — extra work, never wrong answers.
  The reference backend accepts the birthday bound for simplicity; production
  backends choose their precision. *DONE (2026-07-05): the recovery recipe (verify-at-`cross`,
  partition-by-real-key + key-qualified vids) is now in `mod.rs`'s collision-risk docs, with
  the unrecoverable granularity residue; `vec_backend` is documented as deliberately not
  implementing it.*
* **S2 — `value_id` is not order-preserving.** Already documented in `mod.rs`
  (min/max fall to the value callback); recorded here as settled.
* **S3 — the seam is public.** See tiers above. Consequence: contract
  documentation and driver-side assertions are landing requirements, not niceties.
* **S4 — one implementation of each hard algorithm; divergence is a bug surface.**
  The interesting-times machinery is the hardest, most correctness-critical code in
  DD, and it currently exists more than once: the cursor `ReduceTactic`, and this
  proxy port — whose replay machinery (`history.rs`) is a near-verbatim second copy
  and whose reduce driver (`reduce.rs`) re-expresses the same key-bundled-forward
  skeleton (bundle by key, proceed forwards, do per-key-group work). Every copy is a
  place a careless edit can drift from the model — the output-support drift that #781
  fixed was exactly such a case, introduced by the second copy. The presumption is therefore *verbatim reuse*:
  `EditList`/`ValueHistory` are already generic over the value `V` (owned or
  lifetimed) and `u64` is a valid instance, so the existing machinery is expected to
  serve directly; any part that resists unification must carry an in-comment,
  benchmarked reason it cannot — absent that, the fork is debt to retire, not
  structure to keep. New code is not credited as "new mechanism" merely for being
  new; overlap with existing tactics is a cost to drive down. The replay-machinery
  unification was the first instance; F5 is the limit (one interesting-times driver).
  This is the organizing goal, not a someday.
* **S5 — the reference backend is test-and-doc grade, and lives in test code.**
  `vec_backend` (and the identity backend in `tests/`) exist to exercise and
  illustrate the seam without a columnar engine; nothing should depend on them.
  Sharper: a *blessed in-repo* implementation does not test the property that
  matters — that the seam is implementable *out-of-repo* — it only adds a maintained
  impl on the shipped surface. The seam is tested by being consumed *from test /
  example code, as an external crate would consume it*. So the reference backends,
  and the old `ProxyChunk`'s `Chunk` surface, belong in test code, not `src/`. **Both
  are now relocated (2026-07-05):** `vec_backend` → `tests/support/vec_backend.rs`,
  and the `Chunk` surface → a test-local `IdentityChunk`
  (`tests/support/identity_chunk.rs`); lib and the full `int_proxy` suite pass. The
  presentation type itself (now `ProxyBridge`, S6) stays shipped seam API. Same status
  as the conformance kit (F4): referred to, never depended on.
  **Validated (2026-07-07):** re-porting the out-of-repo `corgi` backend atop #781 surfaced a
  contract gap the in-repo reference structurally *could not* — `present_*` **must** return a
  sorted, consolidated bridge, but the doc said only "may consolidate" and (unlike `seed_times`)
  no assert guarded it; `vec_backend` sorts internally, so it never hit it. Now tightened in the
  trait docs and caught by `debug_assert_sorted_bridge`. Exactly the property S5 exists to find.
* **S6 — the seam presentation is exchange, not storage; ephemeral ids must not be
  arranged.** What a backend `present`s (`ProxyBridge`) is an integer projection of
  records that crosses the DD↔backend boundary for one work unit, named across it by
  their full `(key_hash, value_id)` bridge key — the harness hands the ids back unchanged
  (never renumbering to positions), and a `value_id` is intra-hash, so the pair is what pins
  a record (which lets ids stay intra-hash rather than be forced globally unique); the backend
  resolves them however it minted them. Those ids are meaningful only within that one
  presentation (S2). Giving it a
  `Chunk` impl — letting it be batched, merged, graded, persisted
  as an arrangement — would grant those ids a lifetime they do not have: a
  correctness-compromising antipattern, not a convenience. So `ProxyBridge` is a
  plain owned value (no `Rc` — no shipped path clones it; no `Chunk`), lives in the
  seam (`operators/int_proxy/mod.rs`), and the operator's own state is never a
  "proxy trace" of arranged proxy records. A test that wants to store proxy records
  as an arrangement wraps them in its own `Chunk` (`IdentityChunk`), in test code.
* **S7 — the reduce seam differences *inside the backend*; it is no longer Abelian by name.**
  The crossing `reduce_corrections(keys, in_ends, input, out_ends, output)` receives both the input
  accumulation **and** the current output, and returns the output *corrections* — so `desired −
  current` is computed by the backend, not in framework code. The differencing therefore no longer
  requires `ROut` to be a group *at the seam* (the reference backends happen to subtract; the
  `ROut: Abelian` bound survives as a convenience, not a seam commitment), and the trait is named
  `ProxyReduceBackend` — the `Abelian*` prefix, added 2026-07-05 when framework code did the
  subtraction, was dropped when the differencing moved into the backend (2026-07-11). Both relaxation
  axes this point once flagged as future work are now realized:
  * **Batching granularity** — the tactic crosses per *round* (all a window's keys at their current
    moment), one moment deep, capping peak intermediate state at O(window presentation) rather than
    O(times × values). A tactic property, never a trait one.
  * **Non-Abelian** — presenting current output to the backend (the change this point predicted as
    "a trait change") is exactly what `reduce_corrections` does. `ProxyJoin*` never carried the
    constraint (join differences nothing), so the earlier `AbelianReduce*` asymmetry is gone; both
    seams are now `Proxy*`.
* **S8 — the presentation is a plain `Vec`, not a bespoke type.** `ProxyBridge<T, R>` is a
  *type alias* for `Vec<((key_hash, value_id), time, diff)>` — DD's standard `(data, time, diff)`
  update shape — so [`consolidate_updates`](crate::consolidation::consolidate_updates) sorts and
  consolidates it directly. There is **no struct, no accessors, no `from_unsorted`, no
  `sort_perm`**: the columnar (struct-of-arrays) layout and its sort/consolidate apparatus were a
  `Chunk` vestige (bulk merge/compaction wants SoA) the un-fusing (S6) left unexamined. As a
  transient presentation — read one record at a time, consolidated once, then dropped — none of
  it earns its place, and the one forward-looking rationale (vectorized column processing) does
  not even hold: the in-tree columnar backend (`columnar/`) is trie-based and doesn't match SoA.
  How a backend resolves the ids it presents is its own affair, not the seam's: the harness hands
  each `(key_hash, value_id)` back exactly as minted (never a position), so the reference `vec_backend`
  keys `(key_hash, value_id) → (key, value)` maps, and the identity backend needs nothing
  (ids *are* values). Being a plain `Vec`, the bridge also can't be arranged or persisted — S6's
  ephemeral-id guarantee now holds *structurally*, by type. The alias lives in `mod.rs`
  (`bridge.rs` is gone).
* **S9 — the seam shares the retire/unit context with the backend, via an `Instance`.**
  `ReduceInstance`/`JoinInstance` bundle the batches the tactic already holds plus the `lower`
  compaction frontier, and are passed to the read/value methods (`present_*`, `cross`, `reduce*`);
  `materialize` is exempt — its values are *produced*, not drawn from a batch. This is not
  something the seam hides: the backend *is* the arrangement — it produced those batches — so
  withholding them bought nothing. The seam's job is to supply the *implementation* of the hard,
  value-agnostic part (interesting times, lattice logic), not to wall off data the backend owns.
  Two uses the `Instance` unblocks: (a) a backend may resolve `value_id`s by *offset* into the
  batches instead of copying `(key, value)` out at present (F6); (b) `present_*` may advance loaded
  times by `lower` and consolidate, collapsing the historical tail — sound because output is read
  only at times `>= lower` (the join frontier is the unit's capability time: every output is
  produced under it, so `output ∨ cap = output`). `seed_times` receives the `Instance` but must
  **not** apply the frontier: seeds are raw novel support (`b.support ∪ pending`), and
  `lower` is inert there anyway (novel/pending times are already `>= lower`). Advancing is a
  backend *option*, not a requirement — the identity backend ignores `lower` (correct, less
  compact); the reference advances (correct, and demonstrates the intent).

## 3. Follow-ups (non-blocking)

* [ ] **F1 — pivot `columnar/` reduce onto the seam (first in-tree consumer).**
  `ColChunk` already implements `Chunk`; the pivot replaces cursor-driven
  consumption with a `ProxyReduceBackend` over its columns (vectorized hash
  presents, brackets served from column slices, `materialize` building the trie).
  Buys: the `for<'a> Ref` bounds leave the operator paths (the reference-type
  complexity localizes in one backend file); the reduce half gains a CI-covered,
  benchmarked consumer; DD's static-columnar and dynamic-columnar stories converge
  on one operator core. On user logic: `reduce_many` *is* the batched-closure
  seam — per-key Rust closures keep working through its default adapter, and
  columnar execution is available to named/algebraic reducers or IR-compiled
  logic (the corgi `Term → Graph` path is the existence proof). Vectorizing
  *arbitrary* closures is not a goal; the framework's job is to make bulk
  execution possible, which `reduce_many`/`cross` do.
* [ ] **F2 — time genericity in `ProxyBridge`.** Each record carries an owned `T` (the
  `time` field of every `(key_hash, value_id, time, diff)` row), re-materialized at every
  presentation — for nested times (`Product<u64, PointStamp<u64>>`) this is the measured
  dominant cost for columnar backends (the corgi field report), and the cursor path pays the
  same tax in `EditList`. Post-F1 there is, for the first time, a single fix site. Until then,
  backends should not bake in assumptions about the time representation.
* [ ] **F3 — chunk transducer/payload factoring.** `VecChunk`, `ColChunk` (and the
  out-of-tree `CorgiChunk`, and the test-only `IdentityChunk`) each hand-roll
  `merge`/`extract`/`advance`/`settle` over the identical shape
  `{payload, times, diffs}`, differing only in ~5 payload primitives (length,
  compare-at-index, gather, concat, split). `pack()` already shows the
  generic-driver pattern; extend it before a fifth copy appears. A new backend's
  chunk then costs ~100 lines of index plumbing instead of ~450 subtle ones.
* [ ] **F4 — conformance kit.** Extract the grid oracle, compaction adversary,
  and delta-proportionality harness from `tests/int_proxy.rs` so any backend —
  in-tree or out — can certify itself without re-deriving the test strategy.
  **Gap it should close (review):** the join has no analog of the reduce's
  compaction-cancellation regression — nothing exercises the `join.rs` early return where
  advancing a fresh presentation by the capability consolidates it to empty (owed outputs
  cancelling pairwise). Sound by the same join-preservation lemma, but *argued, not tested* —
  and a sound-looking cancellation is exactly the shape that produced the drift #781 fixed.
* [ ] **F5 — long view: one interesting-times implementation.** Trade study on
  making the cursor reduce path a proxy backend over `Ord` types (dense-ordinal
  vids; the hashing tax on native keys is the open question), leaving DD with a
  single implementation of its hardest algorithm, specified by the Lean model and
  certified by F4. Not scheduled; every step above is chosen to keep it reachable.
  **Note (review):** the *join* tactic's per-key replay (`join_key`) is likewise a second copy
  — of the cursor join's `JoinThinker` — so the unification target is both drivers, and the
  fuzz/model safety net currently covers only the reduce.
* [ ] **F6 — resolve value ids by offset, not by copy.** The reference copies `(key, value)`
  into `(key_hash, value_id) → (key, value)` maps at `present`, so it can answer `cross`/`reduce`
  later. With the batches now on the `Instance` (S9), a backend can instead keep
  `(key_hash, value_id) → offset` and dereference the batch at the callback — no data copy, which
  is what a large-value columnar backend needs. Blocked *for the reference* only by `VecChunk`
  exposing no positional record access; a hash-native store (F7) makes it natural. The seam is
  ready; this is the backend's to take.
* [~] **F7 — hash-native storage (`HashChunk`): the actual performance path.** The reference
  stores Ord-ordered (`VecChunk`), so every `present` re-hashes and re-sorts the input into the
  seam's `(key_hash, value_id)` order — and *that transform* is the measured ~4× reduce-load
  overhead (profiled 2026-07-06), **not** the ~13% SipHash slice (which `ahash` would trim). A

  **Spiked 2026-07-09 (the boring version; `tests/int_proxy.rs`).** Rather than build `HashChunk`,
  pre-map `(k,v) → (hash(k), v)` and store in the *existing* `VecChunk<u64,u64>` — now the key IS
  the `key_hash` and (identity model) the value IS the `value_id`, so the store is already in seam
  order. Boring `HashReduce`/`HashJoin` backends then `present` by an in-order scan (`read_hash`),
  drop the real key, and skip value resolution entirely (`value_id` *is* the value). Measured wall
  clock (1M keys, `bench_wall_clock_vs_row`, `mode=2`):
  * **join load 3.0× → 1.3×; join steady ~2× → ~1×** — hash-native brings join to *stock parity* on
    both axes. The `present` re-sort of the accumulated side was the entire join overhead.
  * **reduce load 6.4× → 2.9×** — the re-sort penalty (which *grew* with n) is more than halved and
    no longer the dominant scaling term. F7's thesis holds: hash-native storage removes the `present`
    re-sort, and the two integers are all the operator logic needs (the boring backends drop reals).
  * **The reduce residual (~2.9×) is allocation churn, not algorithm.** Profiled (samply/pollard,
    `prof_reduce_load`): the tactic spends **54% of load in `malloc`** (43% self) vs the cursor
    reduce's **10%**; `grow_one`/`finish_grow` are ~25% (un-reserved `push` loops). Pure-compute
    delta over the cursor is only ~1.5× — the tactic re-implements the same per-moment reduce loop as
    the reference but with fresh per-key/per-window `Vec`s (`KeyState` fields, `IdHistory` buffers,
    window input/output, corrections) where the cursor reduce reuses scratch. So it is **not** an
    inherent overhead: buffer reuse / capacity reservation is the follow-up, independent of `HashChunk`.
  * **Buffer reuse (2026-07-09): reduce load 2.9× → 1.37×; the tactic is now compute-bound.** Two
    rounds, both behavior-preserving (grid-oracle fuzz + SCC green throughout):
    * *Round 1 (→ 2.2×):* retire-wide reusable scratch — a `DiscoverScratch` (the `discover_times`
      replays + time buffers, mirroring the reference `HistoryReplayer`'s field-held scratch) plus
      hoisted phase-2 round/moment buffers (`batch_keys`/`in_*`/`out_*`/`active`/`in_accum`/`cur_out`)
      and `tile_deltas`, all cleared not reallocated. `malloc` 54% → 43% of load. Residual then
      dominated by `drop_in_place<KeyState>` (~18% — per-key `moments`/`meets`/histories/`produced`
      freed on `states.clear()`).
    * *Round 2 (→ 1.37×):* made `states` a **long-lived buffer reloaded slot-by-slot** — index
      `[0..n_states)`, reload each slot in place (`clear`+refill keeps capacity; `drain` moves
      discovered moments in), grow by one only when a window is wider than any before. No pool /
      free-list — a slot's `Vec`s and replays are allocated once and reused, so keys cost no per-key
      alloc/free. `malloc` 43% → **12.8%** (≈ the cursor reduce's own 10.5%); `int_proxy` self-time
      41.5% → **82%**. The remaining 1.37× is genuine compute (cursor walk + consolidate), not
      allocation — the "not inherent" claim above, cashed.
    * *Join (same pattern):* the join tactic was already allocation-lean at 1×1 (`malloc` ~9%, tactic
      self ~3%; the load is `present` + arrangement `merge`, not tactic allocation). Its one real
      per-key allocation is `h0`/`h1` in `join_key`'s replay wave (fired only when *both* sides have
      ≥16 records for a key). Hoisting those into reusable scratch held across the unit cut a
      **high-fanout** join (fanout 32) ~9% (`malloc` 17% → 12%); 1×1 is unchanged (it never enters the
      wave). Both operators' residual is now `present`, the seam's intrinsic cost (below).
  * **What the residual ~0.3–0.4× actually is: `present`.** Both tactics re-materialize each input from
    the arrangement into a consolidated `(key_hash, value_id, time, diff)` bridge before the operator
    logic runs, where the stock operator reads its trace cursor directly. That re-read + consolidate is
    the abstraction boundary's cost — the integers-only seam made concrete.
    * *Read by slice, not cursor (2026-07-09): join load 1.31× → 1.17×, reduce 1.37× → 1.32×.* The
      reference `VecChunk` is a flat sorted `Vec<((key,value),time,diff)>` — already the bridge tuple
      in seam order — so the backend reads `ChunkBatch.chunks[..].as_slice()` directly (full scan =
      `extend_from_slice` memcpy; filtered = two-pointer *galloping both* pointers) instead of walking
      the generic galloping cursor. That deleted the `Cursor` stepping (`seek_key`/`step_val`/
      `val_spills`, ~17% of load). Needs no tactic change — `present` still returns an owned bridge.
    * *What remains, and why:* (1) the **copy** into the owned bridge (`read_hash` ~15%) — inherent
      unless the tactic accepts borrowed slices (a signature change, deferred); (2) the **`consolidate`
      sort** (~16% of join load) — the bridge is a union of *already-sorted* runs (each chunk; source ∪
      novel), so a k-way merge would be linear where the current general `consolidate_updates`
      quicksorts. Replacing sort→merge is the next lever and also needs no tactic change.
    (`VecChunk::merge` ~30% of join load is arrangement *build*, shared with the stock path — *not*
    part of the residual.)
  Corollary: the boring pre-map is a *bench/validation* device (identity-model values, dropped reals,
  collision risk); the real `HashChunk` below is still what a production backend with arbitrary values
  wants. The elaborate real-value store stays on the shelf.

  A
  `HashChunk` — a `Chunk` ordered by `(key_hash, value_id)`, carrying the reals alongside — makes
  `present` an in-order scan (no re-sort; the hash *is* the stored sort key) and folds in F6 (reals
  resolved by offset); merge/compaction is a hash-order merge, streaming with bounded footprint.
  Caveat: as things stand it is a *fifth* hand-rolled `Chunk` (~450 lines, mostly a re-sorted
  `VecChunk`) — do **F3 first** so it costs ~100 lines of payload instead. Corollary: `VecChunk` is
  an *anti-example* as a backend template — Ord-ordered, it performs exactly the impedance-match a
  real backend must avoid — so a `HashChunk`-backed reference would be the honest (and fast) exemplar.
  **Design note (review):** the *system-wide* stability of `key_hash` (mod.rs) is load-bearing
  precisely here — once arrangements are stored in hash order and shared across operators without
  re-hashing, operator B reads operator A's hash-ordered output directly. Within one operator (today
  each backend re-hashes on present) per-backend consistency would suffice, so the system-wide claim
  is a deliberate commitment *toward this storage*, not a current requirement — which makes F7 the
  most consequential follow-up. It follows that S1's recovery-cost story (verify-at-`cross`,
  key-qualified vids) should be weighed against hash-native storage, not against `VecChunk`.
* [ ] **F8 — windowed (bounded-footprint) processing for ~100M-key scale. Mandatory before long.**
  Both tactics `present_*` **a whole second integer copy — the bridge — of the snapshot** (all
  changed/matched keys) before processing, so peak memory is O(keyspace). Even where the driver
  yields output (join, done — see below), that materialization is the floor: making whole integer
  copies of the entire snapshot exhausts us at scale. The fix is to process the keyspace in
  **windows**, diverting a *bounded chunk* of value work to the backend per window. This stays true
  to the seam's purpose — a bulk crossing (`reduce_many`/`cross` over a whole window) amortizes the
  boundary once per window — and is the reason **not** to stream through a cursor: per-record backend
  calls would reintroduce exactly the overhead the bulk seam removes. It is a **trait change per
  operator** — both window the keyspace into bounded key subsets, but the interface differs by driver:
  reduce's synchronous `retire` takes a **session** (`begin → {next_window, reduce_many, emit}* →
  finish`, landed — see Reduce), while join's interleaved driver needs the chunked/lazy `prep` (see
  Join). The early guess that *one* trait serves both was wrong: the two drivers pull opposite ways on
  ownership (§ session type, under Reduce). (The reduce changed-key restriction *looks* like windowing
  but isn't — at an initial load every key is "changed," so the restriction is the whole keyspace.)
  The *efficient* form — bounded metadata, O(range) presentation — is F7-adjacent.
  * **Window by key-hash *range* `[lo, hi)`, not a subset — and keep the metadata bounded too.**
    Present should take a hash *range*, not an explicit key subset: a subset of 100M keys is itself
    O(keyspace) to name, whereas a range is O(1) and is what "align batches on key-hash boundaries"
    wants — the backend seeks each batch to the range bound and merges (O(range) on hash-ordered
    storage, F7; on Ord storage it degrades to scan-and-filter, O(batch) per range, so the *interface*
    can precede F7 but is only *cheap* with it). Bracketing picks the range boundaries from an
    **elicited coarse size distribution** — a histogram over hash *prefixes* (~256 buckets by top
    byte), not per-key `(hash, size)`, or the bracketing metadata is itself O(keyspace). Reduce's
    `seed_times` is the elicit hook (hand back the histogram); join needs the analog over its fresh
    side. Two independent axes: *output* emission (join chunking, done) and *input* materialization
    (range-present) — this bullet is the second.
  * **Reduce — DONE (2026-07-09; Abelian oracle retired 2026-07-11).** The reduce tactic
    (`ProxyReduceTactic`) mirrors the reference cursor reduce's bounded application (`operators/reduce.rs`
    :715–776). Determination is `discover_times` (interesting times only, so O(times), no cliff).
    Application walks all a window's keys' moments in **rounds**, keeping each key's input / output /
    `output_produced` accumulations **one moment deep** and crossing a round's active keys via
    `reduce_corrections` — which takes the input accumulation **and** the current output and returns the
    *corrections*, so the differencing lives in the backend and needs no group (also relaxing the fragile
    vid-must-collide-to-cancel minting contract, since the backend differences real values). Peak memory
    is O(window presentation); the O(times × values) cliff is gone by construction. `output_produced` is
    meet-collapsed each round, exactly like the reference. The backend drives a session: `begin(tiles)` /
    per-window `next_window` / `reduce_corrections` / `emit(tile, deltas)` / `finish` — resolving inside
    the backend so no value round-trips, and tiling without a spine change (`finish` builds once per tile
    over key-disjoint cross-window contributions). Validated against the brute-force grid oracle and —
    the exemplar — the stock row reduce through SCC's doubly-nested product-timed fixpoint (`scc_proxy_*`).
    * **An earlier Abelian batched-crossing tactic** did one bulk `reduce_many` per window, which
      materialized *every* moment's input and output accumulation at once — the O(times × values) memory
      cliff (wide time antichains × large per-key outputs, i.e. iterative dataflow). It was never
      shippable, served only as a development oracle (and earned its keep — it caught the cross-key
      batching bug), and has been **retired**; its two backend traits folded into one `ProxyReduceBackend`.
    * **Tiling complication resolved without touching the spine.** The tactic computes the time-only
      tiles from `(lower, upper, held)` and hands them to `begin`; `emit` routes per tile; `finish`
      builds **once per tile** at the end (real records accumulated across windows, which are
      key-disjoint — hash-contiguous windows — so one consolidation suffices). No per-window
      `materialize`, hence no duplicate-description batches and no chain-tolerant-spine change.
    * **The reference demonstrates the bound, not just the interface.** `next_window` seeks only the
      window's keys, so it materializes a *window's* history, not the retire's — the win reduce gets
      and join doesn't, because `seed_times` decouples key-discovery from presentation (no full-present
      floor). The cost is a per-window re-seek of source/output; F7's hash-native storage removes it.
      Windows are a fixed `WINDOW_KEYS` count for now; hash-range + coarse-histogram sizing (above) is
      the F7 refinement. Parity verified by the int_proxy suite — row-reduce matches, the identity fuzz
      grid oracle, and SCC — with one key per window forced.
    * **Cross-key round batching — DONE.** The application walks all a window's keys' moments in
      *rounds*: each round crosses every active key's current moment in a single `reduce_corrections`,
      cutting backend calls from O(Σ moments) to O(max moments over keys) — the concurrency the batched
      Abelian tactic had, without its cliff (peak materialization stays one moment deep per key). A key's
      own moments stay sequential (each sees its earlier corrections). One subtlety cost a bug worth
      recording: the round loop must terminate only when every key is *exhausted*, not when a round
      produces no crossing — an all-empty-gated round can precede later non-empty moments, and breaking
      early drops them. Found by tracing the identity backend's per-key crossings against the per-moment
      version (they now match byte-for-byte) after reasoning wrongly "proved" they must already agree.
    * **Deferred axes.** (b) *Incremental batch formation for huge outputs* (not histories):
      `finish` accumulates then builds; a backend wanting a bounded output footprint could trickle into
      sorted runs inside `emit` — the session already accommodates it. Pre-existing (the row reduce also
      builds all-at-once), on its own axis. (c) *A session type*: the lifecycle is a documented
      convention with state inlined as backend fields. A borrowing GAT `Session` would make
      begin/finish RAII-paired and split cross-retire state (`keys`) from per-retire state — worth
      doing on a trigger (a second driver, a state-leak bug), but it does **not** unify with join
      (whose interleaved driver needs an *owning* session), so it's a reduce-local ergonomics choice,
      not a shared abstraction.
  * **Join — output chunking DONE (incl. mid-wave 2026-07-09); the rest is F7-gated.** *(Subsumes the
    old F8 join-fuel item; footgun removed by the #790 port.)* `prep` **chunks the unit's output** — a
    container per ~`JOIN_CHUNK` matches — so a large unit ships in bounded pieces the driver meters fuel
    against, and delta-sized units keep a small working set. The flush fires **wherever the batch
    fills, including *mid-key* inside a high-fanout key's replay wave** (a match is an independent
    output record, so splitting anywhere is sound), so no single key materializes more than a chunk of
    its cross product — the join echo of reduce's per-moment emit, but far easier: join's matches carry
    no dependency, so it's one size knob applied continuously, no relocation. This needs **no**
    relocation because `prep` stays synchronous (present + all crosses finish before it returns), so
    the shared resolution state never spans a yield. (Parity verified by forcing the replay path and a
    flush per pair.) What it does **not** bound is a single huge unit's **presentation** — `prep` still
    presents *both sides in full* up front. That, not the crossing, is join's cliff, and windowing the
    fresh side by hash needs hash-native storage (F7). The fuller *lazy* `prep` (yield mid-cross,
    relocating resolution onto the iterator) is what would stream the output rather than hold the
    container `Vec`, and it's the same relocation the fresh-side windowing wants — so both wait on and
    compose with F7 for a fully bounded join.
  Sequencing: reduce tactic done — bounded, cross-key-batched, SCC-validated (the Abelian oracle
  retired) — and join output-chunking done. Remaining: join single-unit load, and both operators'
  *efficient* hash-range/coarse-histogram form — both compose with F7.
