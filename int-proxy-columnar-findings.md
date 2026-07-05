# A columnar backend on `int_proxy`: validation + gaps

A field report from wiring a **dynamic-shape columnar backend** (corgi) into the `int_proxy`
reduce/join framework. Two audiences: (1) reviewers of the `int_proxy` PR, who get a second,
independent consumer that validates the trait surface and drove a correctness fix; (2) anyone
thinking about columnar / non-`Ord`-keyed storage in differential, who gets a concrete map of
what the framework already gives and what it's missing.

## The pieces

| Layer | Location |
|---|---|
| `int_proxy` framework (reduce/join over `(key_hash, value_id)`) | `TimelyDataflow/differential-dataflow#781` |
| corgi DDIR backend (the consumer; `interactive` crate, `SPIKE.md`, benches) | `frankmcsherry/differential-dataflow @ corgi-chunk` |
| corgi kernel crate (the `arrange` API: `find_ranges`, `sort_blocks`, content-hash, …) | `frankmcsherry/wip @ dd-arrange-api` |

The DD-core work is #781; the corgi backend stacked on top lives entirely in the `interactive`
crate. One caveat for reviewers: `corgi-chunk` currently carries **~148 lines of framework
improvements ahead of #781** — an MSD-bucket sort in `ProxyChunk::from_unsorted`, reduce
buffer-reuse, and test extensions — that were developed against the backend and should be pushed
back onto the `int-proxy` branch so the PR carries its own improvements. So diff `pr781..corgi-chunk`
for the true backend delta, but don't assume the DD subtree equals the PR verbatim.

## Why `int_proxy` is the right (and only) extension point here

DD's native operators (`reduce_abelian`, `join_core`) are monomorphized over **static** key/value
types: the cursor yields `&K`/`&V`, the reduce closure is `Fn(&K, &[(&V, R)], …)`. corgi has no
such types — keys and values are `corgi::Value`, a **runtime** shape (`corgi::Shape`) determined
by the DDIR program, stored columnar (`Prim = Arc<Vec<uN>>`). There is no compile-time `K`/`V`.

`int_proxy` is the bridge: it runs reduce/join over `(key_hash: u64, value_id: u64)` integer
proxies, so the *framework* never needs the key/value types, and the *backend* owns the
dynamic-shape materialization behind the proxies. This is the essential **static-vs-dynamic
columnar-shape divide** — `columnar/`/`ColChunk` serve compile-time shapes; a corgi-style backend
serves runtime shapes; `int_proxy` connects them. Native cursor tactics are structurally
unavailable to such a backend, which is exactly the gap `int_proxy` fills.

## What got validated

- All 6 canonical DDIR programs (map/filter/join/reduce/distinct/nested-recursion) match the
  vec backend exactly, including **SCC** — the sharp case for interesting-times.
- The corgi backend surfaced and drove the **`seed_times` correctness fix**: seed interesting
  times from the *batch's own support*, not the consolidated/merged view. Accumulation-preserving
  compaction can advance a stored record onto a novel retraction's exact time and cancel it in the
  merged view, erasing a seed the reduce needed. Formalized in Lean (`Model.lean`,
  `scenario1_cancels`, `seedSet = support ∪ pending`). This is a framework-level result #781 now
  carries — a second backend independently exercised the interesting-times machinery hard enough
  to find it.

## Empirical findings (SCC, `e = 2n`, single worker, from-scratch to fixpoint)

Three-way: hand-written native DD `strongly_connected` vs DDIR-vec vs DDIR-corgi (identical graph).

| n | native | DDIR-vec | DDIR-corgi | corgi/vec |
|---|---|---|---|---|
| 1,000 | 26 ms | 37 ms | 83 ms | 2.24× |
| 100,000 | 4.00 s | 2.92 s | 7.18 s | 2.46× |
| 500,000 | 25.9 s | 20.3 s | 52.5 s | 2.59× |
| 1,000,000 | 56.2 s | 44.5 s | 112.8 s | 2.54× |

corgi holds a flat ~2.5× vs vec to 1M (no superlinear blow-up). Two apples-to-apples notes:

- **DDIR-vec beats native above ~10k nodes** (0.7–0.8× native). The DDIR SCC uses
  `enter_at($1[0])` — log-bucketed prioritized iteration (`256·(64−leading_zeros)`); native
  `strongly_connected` uses plain `propagate` (all labels at inner round 0). So the `native`
  column runs a *less prioritized algorithm* and is only a loose reference; **corgi-vs-vec is the
  fair comparison** (identical DDIR program).
- On linear/map-heavy programs corgi is **3× faster** than vec (columnar `eval_graph` vs per-tuple
  `ir::eval`). SCC is corgi's *worst* case — control-heavy, compute-light.

### Where the 2.5× goes (self-time, SCC n=800, both backends 200 identical iters → samples comparable)

| bucket | DDIR-vec | DDIR-corgi |
|---|---|---|
| comparison / ordering | ~515 samples | ~600 |
| framework + glue | ~925 | ~1,020 |
| **allocation** (malloc + SmallVec + raw_vec) | **~700** | **~2,900** |
| total | 3,703 | 7,923 (2.14×) |

**The entire gap is allocation.** Comparison is a tie (vec's single biggest cost is compiled
`Value::cmp`, 13%; corgi's structural `compare_idx` is comparable). Framework is a tie —
`int_proxy` costs corgi nothing over DD's native cursor reduce. corgi's engine kernels are only
~16% self; the `interactive` glue ~3.4% self.

The allocation is **not** the value columns — those are already columnar (`Prim = Arc<Vec>`,
refcount clones). It's the **per-row timestamp**: `times: Vec<Product<u64, PointStamp<u64>>>`,
where `PointStamp` is a per-row heap `SmallVec` (spills under nested scopes), cloned on every merge
`emit`/`to_vec` and allocated on every lattice `Product::join`.

## What the framework is "missing" for columnar backends

1. **A columnar `Time`/`Diff` container path a dynamic-shape backend can reuse.** DD's own
   `columnar/` module already has it: `ColChunk`/`UpdatesTyped` store `times: Lists<ContainerOf<Time>>`,
   and `Time = Product<u64, PointStamp<u64>>` derives `Columnar`, so the time column is SoA (outer
   `u64` column + inner `Vecs` = flat coords + bounds) — **the per-row `PointStamp` heap object
   never arises**. The catch: `ColChunk<U>` requires `U: ColumnarUpdate` = *static* `(K,V,T,R)`, so
   a dynamic-shape backend can't adopt it wholesale for keys/vals. The reusable piece is the
   `Time`/`Diff` container specifically. Making that path usable independent of static key/value
   types would delete the single largest cost (the ~37% bucket) for any such backend.

2. **Hash-ordered comparison + O(1) probe (optional).** Since `int_proxy` already keys on
   `key_hash`, comparisons could be `u64`-hash rather than structural, and a Robin-Hood table
   sorted by hash with a rank/select side index gives sorted + immutable + mergeable + **O(1)
   lookup** — attacking both the join probe and the merge-scan compares. Caveat: it's a join/merge
   win, not a reduce-`min` win (min is over values, not hashes), which is why a naive hash-sort
   regressed reduce/reach in an earlier experiment.

3. **A general columnar *kernel* surface.** A backend like this wants segmented sort / argmin
   returning positions, batched range-find (`find_ranges`), and content-hash — not ad-hoc "arrange"
   helpers. The corgi `arrange` API (`frankmcsherry/wip @ dd-arrange-api`) is a first cut.

## Repro

```
# corgi backend branch (path-deps the corgi kernel crate at ../../wip/corgi):
cargo run --release -p interactive --example corgi_progs      # correctness gate (6 programs vs vec)
cargo run --release -p interactive --example corgi_perf       # linear / reach / three-way SCC
N=100000,500000,1000000 cargo run --release -p interactive --example corgi_scc_big   # large-N SCC
```

Net: `int_proxy` is a working, correctness-validated extension point for non-`Ord`-keyed backends;
the remaining cost for a columnar consumer is timestamp allocation, and DD already contains the
columnar timestamp representation that fixes it — it just isn't reachable without static K/V today.
