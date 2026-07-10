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
* **S7 — the presentation is a plain `Vec`, not a bespoke type.** `ProxyBridge<T, R>` is a
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
* **S8 — the seam shares the retire/unit context with the backend, via an `Instance`.**
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
  later. With the batches now on the `Instance` (S8), a backend can instead keep
  `(key_hash, value_id) → offset` and dereference the batch at the callback — no data copy, which
  is what a large-value columnar backend needs. Blocked *for the reference* only by `VecChunk`
  exposing no positional record access; a hash-native store (F7) makes it natural. The seam is
  ready; this is the backend's to take.
* [~] **F7 — hash-native storage (`HashChunk`).** The reference stores Ord-ordered (`VecChunk`), so
  every `present` re-derives the seam's `(key_hash, value_id)` order from the stored `(key, value)`
  order — the measured reduce-load overhead (profiled 2026-07-06), not the ~13% SipHash slice. **The
  work: build a `HashChunk`** — a `Chunk` natively ordered by `(key_hash, value_id)`, carrying the
  reals alongside and resolving ids by *offset* (subsumes F6). Then `present` is an in-order scan, and
  merge/compaction a hash-order merge with bounded footprint. Do **F3 first**, or it is a fifth
  hand-rolled `Chunk` (~450 lines, vs ~100 of payload plumbing). It is the honest fast exemplar;
  `VecChunk` is the anti-example (Ord-ordered, it performs exactly the impedance-match a real backend
  must avoid). **Load-bearing:** the *system-wide* stability of `key_hash` (mod.rs) pays off here —
  arrangements stored in hash order let a downstream operator read an upstream's output directly, no
  re-hash and no re-present; within one operator per-backend consistency would suffice, so the
  system-wide claim is a commitment *toward* this storage, which makes F7 the most consequential
  follow-up. (Weigh S1's collision-recovery cost against hash-native storage, not `VecChunk`.)
  * *Thesis validated, not built (git; `tests/int_proxy.rs`).* A boring pre-map (`(k,v) → (hash(k), v)`,
    values as ids) puts the existing `VecChunk` in seam order; with backend-only present tuning already
    landed (buffer reuse, slice-based reads) it reaches stock parity for join and ~1.3× for reduce — a
    bench/validation device (identity-model values, dropped reals, collision risk), not the real thing.
    One backend-only present win remains (no tactic change): `present` still `consolidate`-sorts a
    union of *already-sorted* runs, so a k-way **merge** would be linear.
* [ ] **F8 — bounded-footprint (windowed) presentation for ~100M-key scale.** Both tactics `present`
  a whole second integer copy of the snapshot before processing, so peak memory is O(keyspace). The
  fix: window the keyspace and divert a *bounded chunk* of value work to the backend per window — a
  bulk crossing amortizes the boundary once per window (the reason **not** to stream per-record, which
  would reintroduce the overhead the bulk seam removes). It is a **trait change per operator**, not one
  shared trait: the two drivers pull opposite ways on ownership — reduce's synchronous `retire` wants a
  session, join's interleaved `prep` wants a chunked/lazy iterator. *Landed:* reduce windowing (the
  `begin → {next_window, reduce_corrections, emit}* → finish` session, one key per window forced in the
  suite) and join output-chunking (a container per `JOIN_CHUNK` matches, flushed mid-wave). **Remaining:**
  * **Join single-unit load.** `prep` still presents *both sides in full* up front — that, not the
    crossing, is join's cliff. Window the fresh side by hash range; the fuller *lazy* `prep` (yield
    mid-cross, relocating resolution onto the iterator) also streams the output rather than holding the
    container `Vec`. Both need F7.
  * **The *efficient* form for both: window by hash *range* `[lo, hi)`, not a key subset.** A subset of
    100M keys is itself O(keyspace) to name; a range is O(1) and is what "align on key-hash boundaries"
    wants (seek each batch to the bound and merge — O(range) on hash-ordered storage F7; O(batch)
    scan-and-filter on Ord, so the interface can precede F7 but is cheap only with it). Size the ranges
    from an **elicited coarse histogram** — hash-*prefix* buckets (~256 by top byte), not per-key
    `(hash, size)`, or the bracketing metadata is itself O(keyspace). `seed_times` is reduce's elicit
    hook; join needs the analog over its fresh side. (This replaces reduce's placeholder fixed
    `WINDOW_KEYS` count.)
