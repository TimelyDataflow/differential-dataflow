# Corgi kernel findings — SCC on the DDIR corgi backend (2026-07-11)

Profile-grounded asks for the corgi kernel, from an `samply` capture of `interactive`'s SCC
(`corgi_scc_prof`, N=1000, ITERS=200, corgi backend, baseline `corgi-on-mn @ 8681e193`, arm64).
19,537 samples @ 1kHz. The corgi backend is a cursor-less `Chunk` arrangement: keys/vals are corgi
`Value` columns (`Prod([keys, vals])`), and merge/advance/sort drive ordering through corgi's
structural-compare + discrimination-sort kernels.

**Headline:** on SCC (corgi's worst case — control-heavy, compute-light, tiny integer keys, unit
values), the dominant *corgi* cost is **structural comparison + discrimination sort**, not compute
and not (as I'd earlier guessed) column allocation. Numbers below are the honest split.

---

## 1. Structural compare / discrimination sort — the big corgi lever (~20% of total)

| corgi fn | self % | total % | driven from |
|---|---|---|---|
| `ops::cmp::order::compare::compare_idx` | 4.0 | **14.7** | `CorgiChunk::merge` (merge_batcher) + `CorgiChunk::advance` (spine compaction) |
| `ops::cmp::order::discriminate::sort_blocks` | 0.1 | 6.0 | `sort_consolidate` → `sort_perm` |
| `ops::cmp::order::discriminate::sort_leaf_blocks` | 3.2 | 5.4 | (under `sort_blocks`) |
| `value::Prim::cmp_idx` | 1.4 | 3.2 | (leaf under `compare_idx`) |
| `ops::cmp::CmpOp::eval` | 1.4 | 3.5 | |

Call context (from stacks): `compare_idx` is almost entirely **arrangement maintenance** —
`CorgiChunk::merge` (the two-pointer merge's per-pair `(key,val)` compare) and `CorgiChunk::advance`
(the compaction's group-boundary scan). It **recurses through the `Prod([key,val])` structure per
comparison** (`compare_idx → compare_idx → Prim::cmp_idx`), i.e. it re-walks the product shape and
re-dispatches to the leaf on every comparison. For SCC the keys are a single `Prim(64)` (a graph
node id), so this is a lot of shape-walking to reach one `u64` compare.

**Ask — radix / flat sort-key at the leaves.** This is the known "compare_idx interpreted-comparison
tax" axis. Precompute a **flat, radix-friendly sort-key column once** per chunk (or expose a kernel
that does), so that:
- merge run-head compares read a `u64`/byte directly instead of re-walking `Prod` + dispatching per
  comparison, and
- `from_unsorted` / `sort_perm` can **radix-sort** (O(n), skip comparison) instead of
  comparison-sort (`sort_leaf_blocks` + `compare_idx`).

For primitive keys this could go from ~20% to near-nothing (a `u64` radix), and it's a general win
(not SCC-specific): every arrangement merge/consolidate in every DDIR program pays this tax. The
backend already leans on `compare_idx`/`sort_perm`/`sort_blocks` as the ordering primitives, so a
flat-sort-key variant (or a `radix_sort_perm(col) -> perm` + `compare_prekey(prekey, ia, ib)`)
slots in without a backend redesign.

**Confirmed to GROW with scale (N=100000 profile, /tmp/scc_corgi_bigN.json.gz, 42.8k samples):**
`compare_idx` rises **14.7% → 21.9%** total (5.2% self), now driven overwhelmingly by
**`CorgiChunk::merge` = 22% total** (the arrangement merge, which was not even top-10 at N=1000).
`sort_leaf_blocks` 5.4% → 6.3%. So the structural-order machinery is ~28% of total at N=100k and
climbing — this is *the* corgi lever, and it gets bigger at the sizes Frank actually runs (n=1m).

## 2. Column re-gather — real but smaller (~4–5% of total)

| corgi fn | self % | total % |
|---|---|---|
| `engine::gather_lanes` | 0.1 | 3.0 |
| `value::Prim::gather_lanes` | 1.0 | 1.3 |
| `engine::gather` / `Prim::gather` | 0.5 | 0.6 |

Every `merge`/`advance`/`present`/`settle` builds a **new** column via `gather`/`gather_lanes`
(fresh `Arc<Vec>`), re-materializing the repeated key column. This is the "flat SoA re-gathered per
merge" axis. It's a smaller bucket than I'd initially estimated — **not** the dominant allocation
(see the correction below) — but it's pure churn: the merge output's key column is a permutation/
concatenation of inputs we already hold.

**Ask (secondary):** cheap column *views*/*slices* and/or in-place gather, so the merge/advance
paths can reorder/concatenate without allocating a fresh backing `Vec` each time. Lower priority
than (1).

## Allocation attribution (malloc ≈ 43% of samples)

An earlier note of mine attributed ~32% of runtime to "corgi column churn." **The profile does not
support that** — corgi's own `gather*` is only ~4–5%. The `malloc` is not diffuse once attributed;
it's a handful of identifiable sites (percentages are *total* samples; they overlap in their
descendant `malloc`, so they don't sum to 43%):

| allocating site | total % | owner | notes |
|---|---|---|---|
| `SmallVec::extend` (PointStamp materialization) | 10.7 | backend/framework | timestamps; partly addressed by `ColTimes`, rest is reduce/compaction |
| `corgi_chunk::sort_consolidate` | 7.7 | **backend (ours)** | fresh perm + reordered `times_s`/`diffs_s`/`keep` per consolidate; **same code that pays item-1 compare/sort CPU** |
| `consolidate_updates_slice_slow` (reduce **bridge**) | 7.7 | **tactic-facing** | the `Vec<((u64,u64),T,R)>` consolidation — replaced by the columnar `ProxyBridgeBuilder`/`from_unsorted` |
| `ValueHistory::build` (replay) | 3.4 | **tactic** | columnarized in `fm/int-proxy-columnar` `history.rs` |
| `gather` / `gather_lanes` (corgi columns) | ~4–5 | **corgi** | item 2 above |
| `register_keys`/`register_vals` + hashbrown rehash | <1 | backend | IdMaps |

`RawVec::finish_grow` (6.8%) + `grow_one` (5.9%) are the `malloc`-under-Vec-growth *inside* the rows
above, not a separate bucket.

**Reading for each owner:**
- **corgi:** biggest lever is compare/sort CPU (item 1), not allocation (gather is ~4–5%). A radix
  sort-key (item 1) also removes `sort_consolidate`'s permutation allocation — the one place the
  corgi ask and a backend cleanup compound.
- **tactic (int_proxy):** the reduce-bridge `consolidate_updates` (7.7%) and `ValueHistory` (3.4%)
  are *exactly* what the columnar bridge (`fm/int-proxy-columnar`) already targets. Nothing new to
  build — but the payoff is **N- and workload-dependent**: at N=1000 (this profile) they're ~11%
  combined; at large N the arrangement dominates and they shrink; on shallow-time programs the
  columnar machinery's overhead outweighs the alloc removed. A shallow-time fast-path is the open
  item there. **Confirmed at N=100000:** the bridge `consolidate_updates` shrinks 7.7% → 5.4% and
  `ValueHistory::build` 3.4% → 1.9% (combined 11% → 7.3%) as the arrangement merge (22%) takes over
  — so the columnar bridge's payoff genuinely fades with scale, and a shallow-time fast-path (not
  more columnar machinery) is the right investment for the tactic. Also new at scale:
  `collect_present` (OUR reduce present's changed-key scan over accumulated history) rises to 10.8%
  total / **8.3% self** — a backend O(history) semijoin cost worth its own look.
- **backend (ours):** `sort_consolidate` (7.7%) is ours to cut via buffer reuse (and it rides the
  corgi radix-key win).

## What NOT to change (backend-side, not corgi's problem)

- Timestamp materialization (`PointStamp` `try_grow`, ~11%) is the DDIR backend's job — already
  partly addressed by columnar times (`ColTimes`), the rest is reduce-tactic/compaction work.
- `CorgiChunk::advance` (13.3% total) and the reduce `retire` (50% total) are backend/framework;
  corgi appears inside them only via items 1 & 2 above.

## Reference

- Backend: `interactive/src/corgi_chunk.rs` (merge/advance/sort_consolidate — the `compare_idx` /
  `sort_perm` callers), `interactive/src/corgi_reduce_backend.rs`.
- Kernels of interest: `compare_idx`, `sort_perm`, `sort_blocks`, `sort_leaf_blocks`, `gather`,
  `gather_lanes` in the corgi crate.
- Profile: `/tmp/scc_corgi.json.gz` (Firefox format; `samply load` or pollard).
