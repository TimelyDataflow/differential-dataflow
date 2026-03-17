# Update Containers: A Unified Abstraction

## Problem

Differential dataflow manipulates bundles of updates at five distinct sites, each currently implemented with different types and traits. The result is a complex stack of generic traits (`Layout`, `LayoutExt`, `WithLayout`, `BatchContainer`, `Batcher`, `Builder`, `Merger`, `Cursor`, etc.) with dozens of associated types. These sites are structurally similar and should share a single abstraction.

## The Five Sites

1. **Collection streams** — updates flow through dataflow operators as containers. Containers are sorted at the point of creation (by the producing operator) and remain sorted throughout the stream. Containers accumulate and benefit from consolidation.

2. **Batchers** — incoming containers (already sorted) are merged using a merge-sort stacking discipline. The batcher is purely chain management — it never sees unsorted data.

3. **Batcher seal** — when a frontier advances, updates not beyond the frontier are extracted from the batcher's chain to form trace batches. Updates beyond the frontier remain for future extraction. A batch *is* a chain (or wraps one), so extraction directly produces batch contents.

4. **Trace batch merging** — the trace spine merges batches to maintain compaction. This is done incrementally (bounded work per step) to avoid stalling the system. During merging, times are advanced by a compaction frontier, which may reorder entries within each (key, value) group and requires re-sorting and consolidation.

5. **Operator bulk loading** — operators (e.g., `reduce`) that know their keys ahead of time can request bulk extraction from the trace. Given a set of keys and multiple trace batches (chains), the system extracts all matching updates, merges them across batches, and advances times by a frontier. This provides bounded, pre-fetched work units to the operator, replacing cursor-based one-at-a-time exploration.

## Core Observation

All five sites:
- Work with containers of sorted updates
- Benefit from consolidated updates (coalesced diffs)
- Involve merging chains of bounded-size containers
- Can be driven by the same merge-sort stacking discipline

## The Abstraction

### Container

An opaque, bounded-size collection of sorted updates. The container's internal representation is not prescribed — it could be a sorted `Vec<(K, V, T, R)>`, a prefix-deduplicated trie, a columnar encoding, or anything else. The trait describes what the container can do, not how it is structured.

Containers in a trace are **shared** — multiple readers (operators holding references, the trace itself during merging) may hold references to the same container via `Rc` (or `Arc`). A shared container is immutable; it cannot track consumption internally.

Instead, consumption is tracked by a thin wrapper:

```rust
struct ChainEntry<C> {
    container: Rc<C>,  // shared, immutable container
    offset: usize,     // per-reader consumption position
}
```

Each reader has its own `ChainEntry` with its own offset. The trait methods operate on `ChainEntry<C>`, advancing the offset without mutating the underlying container. When a `ChainEntry` is dropped and no other readers hold the `Rc`, the container's memory is released.

Containers that are not yet shared (e.g., freshly produced by an operator, or output from a merge step) are owned directly and can be wrapped in `Rc` when installed into a trace batch.

### Chain

A `Vec<Container>` maintained by a generic merge-sort stacking discipline:
- When adjacent containers in the chain have similar sizes, they are merged.
- Containers are bounded in size (configurable target), preventing memory spikes from large allocations.
- Empty containers are dropped from the chain, eagerly releasing resources.
- Merging respects a fuel budget for incremental progress, avoiding stalls.

The chain management code is fully generic over the container type. It does not inspect container contents.

### Traits

The container functionality is split into two traits: one for core merging and extraction, one for key-filtered extraction.

```rust
/// Tracks a read position into a shared (or owned) container.
struct ChainEntry<C> {
    container: Rc<C>,
    offset: usize,
}

/// Core trait for sorted update containers. Supports merging and
/// frontier-based extraction. Agnostic to internal representation.
///
/// Methods take `ChainEntry<Self>` rather than `&mut Self`, because
/// containers in a trace are shared via `Rc`. The offset in the
/// `ChainEntry` tracks per-reader consumption without mutating the
/// underlying container.
trait MergeContainer: Sized {
    type Time: Timestamp + Lattice;

    /// Merge from the heads of two peer containers into a new (owned)
    /// container of approximately `target_size` logical updates.
    ///
    /// Both entries advance their offsets to reflect consumed input.
    /// If `advance` is provided, times are advanced by the frontier
    /// and entries are re-sorted and consolidated within each
    /// (key, value) group.
    ///
    /// Used by: batcher chain maintenance (no advance),
    ///          trace compaction (with advance).
    fn merge_step(
        left: &mut ChainEntry<Self>,
        right: &mut ChainEntry<Self>,
        advance: Option<&Antichain<Self::Time>>,
        target_size: usize,
    ) -> Self;

    /// Partition by time frontier: returns (extracted, kept) where
    /// `extracted` contains entries not beyond `frontier`, and `kept`
    /// contains the rest.
    ///
    /// Takes ownership — this is used by the batcher at seal time,
    /// before containers are shared via Rc.
    ///
    /// Used by: batcher seal, to partition updates by time frontier.
    fn extract_frontier(
        self,
        frontier: &Antichain<Self::Time>,
    ) -> (Self, Self);

    /// Total number of logical updates in the container.
    fn total_len(&self) -> usize;

    /// Number of logical updates remaining (after offset).
    fn remaining(entry: &ChainEntry<Self>) -> usize {
        entry.container.total_len() - entry.offset
    }

    /// Whether all entries have been consumed.
    fn exhausted(entry: &ChainEntry<Self>) -> bool {
        entry.offset >= entry.container.total_len()
    }
}

/// Key-filtered extraction with time advancement. Separated from
/// MergeContainer because not all consumers need it, and the key
/// set representation may vary.
trait ExtractVia<K: ?Sized>: MergeContainer {
    /// Extract entries matching `keys` into a new (owned) container.
    /// Times are advanced by `frontier` and consolidated.
    ///
    /// Used by: operator bulk loading from trace batches.
    fn extract_keys(
        source: &mut ChainEntry<Self>,
        keys: &K,
        advance: &Antichain<Self::Time>,
        target_size: usize,
    ) -> Self;
}
```

### How the Sites Use the Traits

| Site | Operation | Trait | Advance? | Filter? |
|------|-----------|-------|----------|---------|
| Collection stream | chain stacking | `MergeContainer` | no | no |
| Batcher chain | `merge_step` | `MergeContainer` | no | no |
| Batcher seal | `extract_frontier` | `MergeContainer` | no | by frontier |
| Trace compaction | `merge_step` | `MergeContainer` | yes | no |
| Operator bulk load | `extract_keys` | `ExtractVia<K>` | yes | by key set |

### Chain Management (Generic)

```rust
fn merge_chains<C: MergeContainer>(
    left: &mut Vec<ChainEntry<C>>,
    right: &mut Vec<ChainEntry<C>>,
    advance: Option<&Antichain<C::Time>>,
    target_size: usize,
    fuel: &mut usize,
) -> Vec<C> {
    let mut output = Vec::new();
    while *fuel > 0 {
        drop_empty_prefix(left);
        drop_empty_prefix(right);
        match (left.first_mut(), right.first_mut()) {
            (Some(l), Some(r)) => {
                let merged = C::merge_step(l, r, advance, target_size);
                *fuel -= merged.len();
                push_with_stacking(&mut output, merged, advance, target_size);
            }
            (Some(_), None) | (None, Some(_)) => {
                // drain remaining chain (advancing if needed)
            }
            (None, None) => break,
        }
    }
    output  // owned containers, not yet shared
}
```

Note that `merge_chains` takes `Vec<ChainEntry<C>>` (shared inputs from trace batches) but produces `Vec<C>` (owned outputs, not yet shared). The outputs become shared when the resulting batch is installed in the trace and wrapped in `Rc`.

The stacking discipline in `push_with_stacking` merges the new container with the last output container if both are undersized. This handles runts from exhausted inputs without special cases.

### Downstream Consumers

**Batcher** — receives sorted containers from the stream, maintains a chain using the stacking discipline, seals by calling `extract_frontier` across the chain. Requires only `MergeContainer`. No sorting, no construction — the batcher is purely chain management.

**Batch** — a frozen, shared chain. Wraps `Vec<Rc<C>>` (or equivalently, a `Vec<ChainEntry<C>>` can be created from it with zero offsets). Multiple readers can hold references to the same batch; each creates its own `ChainEntry` wrappers with independent offsets. Produced directly by batcher seal (which wraps the owned output containers in `Rc`) — no separate builder type needed.

**Trace** — maintains batches in a spine, merges them incrementally via `merge_chains` with advancement. The chain merger is generic; the trace manages the spine structure (which pairs of batches to merge, fuel allocation).

**Operators producing updates** (`reduce`, `upsert`, `map`, `join`, etc.) — these are producers. They construct sorted containers directly (using the concrete container type's own construction API) and emit them into the stream. This construction interface is type-specific and does not need to be abstracted by a trait — each container type knows how to be built.

**Sorting** happens at the producer, not the consumer. Operators sort their output into containers before emitting. A boundary operator can accept unsorted user input and produce sorted containers. The sorting mechanism is specific to the container type's construction path.

## Design Decisions

**Pairs, not slices.** `merge_step` takes two containers, not slices of chains. This keeps the contract simple. Undersized outputs from exhausted inputs are absorbed by the stacking discipline, which already handles this.

**Offset-based consumption via ChainEntry.** Containers are immutable once shared (via `Rc`). Consumption is tracked externally by `ChainEntry`, which pairs an `Rc<C>` with a mutable offset. Each reader has its own entries with independent offsets. This supports sharing batches across multiple operators and the trace's own merge machinery without conflicts.

**Representation-agnostic.** The trait does not prescribe sorted vecs, tries, or columnar layouts. Implementations choose their own representation. A sorted `Vec<(K,V,T,R)>` is a valid (if unoptimized) implementation.

**Advance is fused with merge/extraction.** Time advancement is not a standalone mutation. It happens during `merge_step` (trace compaction) and `extract_keys` (bulk loading), where the data is already being touched. This avoids a separate re-sort pass and allows consolidation to happen as entries are produced.

**Value transformation is separate.** Operators that need to project or transform values do so as a second pass after extraction. The cost of revisiting entries is small relative to the cost of extracting them (especially from cold/remote storage). This keeps the container trait free of operator-specific concerns.

**Stream containers are sorted containers.** The same container type flows through the dataflow stream and is maintained in trace batches. Sorting is the producer's responsibility. The batcher receives already-sorted containers and only manages the chain. This eliminates a type conversion boundary and a separate sorting step at the batcher.

**Two traits, not one.** `MergeContainer` handles the core merging and frontier extraction needed by all five sites. `ExtractVia<K>` adds key-filtered extraction for operator bulk loading. This separation keeps the core trait minimal and allows the key set representation to vary via the type parameter `K`.

**Container construction is not part of the trait.** Building a container from raw updates is specific to each container type's representation. Producers (operators) use the concrete type's API directly. This keeps the trait focused on the operations that the chain management and trace infrastructure need to be generic over.

## Open Questions

1. **Key set representation.** The type parameter `K` in `ExtractVia<K>` leaves the key set representation open. Options include a sorted slice of owned keys, a key-only container, or another structure. Explicit keys (not a predicate) seem right for seek-based access into large batch chains with small key sets.

2. **`(K, T, R)` updates.** Some use cases have no value component (e.g., `distinct`). Is this a separate container type, the same type with a unit value, or handled by a type parameter on the trait?

3. **Target size semantics.** Is `target_size` a count of logical updates, a byte budget, or something else? Byte budget is more useful for memory management but harder for the container to estimate cheaply.

4. **Interaction with timely containers.** Collection streams currently use timely dataflow's container infrastructure. Using update containers as the stream container requires integration with timely's `push`/`pull` protocol. This may mean implementing timely's `Container` trait for update containers, or adapting the boundary between timely and differential.

5. **Cursor interface.** Does a cursor-based read path still exist alongside bulk loading? Some use cases (user exploration, ad-hoc queries) may still want cursor-style access. If so, the cursor could be a method on the container or chain, separate from the traits above.
