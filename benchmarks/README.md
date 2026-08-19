# Differential dataflow benchmarks

This project measures the cost of expressing and executing computations through DDIR.

Each benchmark provides four implementations.

1. `compiled` is an optimized statically typed differential dataflow, in the root scope at a `u64` timestamp.
2. `compiled-ddir` is a statically typed transcription of the DDIR operator plan, rendered where DDIR must render it.
3. `ddir-vec` interprets the DDIR program over row-oriented `Value` records.
4. `ddir-corgi` interprets the same DDIR program over the Corgi columnar backend.

The first ratio identifies costs imposed by DDIR's plan and by DDIR's runtime embedding.

The second ratio identifies dynamic representation and interpretation costs.

The third ratio identifies the effect of the columnar backend.

### Why `compiled-ddir` exists even for atomic operators

DDIR renders every program inside an iterative scope, so its operators run at
`interactive::ir::Time`, which is `Product<u64, PointStamp<u64>>`, rather than at
`u64`. That is a larger timestamp, a vector-valued lattice, and an `enter` operator
per input. A typed program writing the same map or join would not pay any of it.

That cost is real and it belongs to DDIR, but it is a cost of the runtime embedding
rather than of interpretation. Charging it to the `ddir-vec` and `ddir-corgi` rows
would silently inflate every interpretation ratio in this project, and it would grow
with any future change to DDIR's timestamp without any interpreter getting slower.

So even the atomic operator cases, where the operator plan really is identical, keep
a `compiled-ddir` rung: the same typed plan, in the iterative scope, at DDIR's
timestamp. `compiled-ddir / compiled` is the embedding tax and `ddir-vec /
compiled-ddir` is interpretation.

## SCC benchmark

The initial benchmark computes the subset of input edges whose endpoints are in the same strongly connected component.

The `compiled` variant uses `strongly_connected_at` with the node identifier.

The `compiled-ddir` variant transcribes DDIR's nested joins, reductions, and variables into Rust types.

Both compiled variants introduce labels in the same rounds as DDIR's `enter_at($1[0])`.

Run a small validation with:

```text
cargo run -p benchmarks --bin scc -- --nodes 100 --edges 200 --runs 4
```

Run an optimized measurement with:

```text
cargo run --release -p benchmarks --bin scc -- --nodes 100000 --edges 200000 --warmup 2 --runs 8
```

The program writes one JSON object per measured implementation run to standard output.

Progress and a ratio summary go to standard error.

All four output multisets must agree before the program emits measurements.

The implementation order rotates between runs to remove fixed ordering bias.

The implementations of one benchmark share a process, and therefore an allocator
heap, so the order within a repetition is not neutral. Rotating only removes that
bias if the rotation completes a whole number of times, so `--runs` must be a
multiple of the implementation count. The programs refuse other values.

This first benchmark is a single-worker, single-batch measurement.

It does not yet measure update streams, scale-out, peak memory, or allocation counts.

The [preliminary SCC report](reports/2026-08-17-scc.md) shows the initial report format and results.

## Operator scorecard

The `operators` binary measures atomic operators, whose optimized and DDIR-matched
operator plans coincide. The two compiled implementations therefore render the same
plan, and differ only in the scope and timestamp they render it at.

V0 contains these load-time rows.

| Family | Cases |
| --- | --- |
| Baseline | Identity |
| Map | One map; eight chained maps |
| Filter | Retain none; retain half; retain all |
| Arrange | Sorted unique keys; randomized unique keys; duplicate keys |
| Join | One-to-one matches; partial misses; fanout |
| Reduce | Distinct; count; min |

Run the scorecard with:

```text
cargo run --release -p benchmarks --bin operators -- --case all --rows 100000 --keys 1000 --fanout 4 --warmup 2 --runs 8
```

The timed output is probed in its native container without materialization.

An untimed validation runs all four implementations, materializes each output, and
requires exact agreement with a hand-written oracle before measurement. The compiled
implementations are executed there rather than assumed to agree: `compiled-ddir`
renders at a coarser timestamp lattice, which selects different code paths inside the
reduce and join operators. The capture operators exist only in the validation run.

The [preliminary operator report](reports/2026-08-17-operators.md) records the first results.

The `arrange_updates` binary measures a second batch after a populated arrangement reaches time one.

The batch retracts existing rows and inserts the same number of new rows.

Run it with:

```text
cargo run --release -p benchmarks --bin arrange_updates -- --rows 100000 --update-rows 1000 --warmup 2 --runs 8
```

Its `measured_ns` includes only update submission and stabilization.

Parsing, construction, and initial arrangement formation are recorded separately.

An untimed two-batch run consolidates all output changes and checks the final arrangement exactly before measurement.

## Measurement boundaries

`prepare_ns` measures DDIR parsing, lowering, and optimization.

It is zero for the compiled variants.

`build_ns` measures timely and differential dataflow construction.

`ingest_ns` measures submission and flushing of the input updates without stepping the worker.

Every implementation consumes rows that were prepared before the timed region, and
consumes them by value. Cloning at the input handle would charge the DDIR paths a
deep copy of every `Value` that the typed paths, whose rows are `Copy`, never pay.

`stabilize_ns` measures worker execution until the output frontier reaches the end of the input batch.

For load-time rows, `measured_ns` is the sum of these phases.

For the arrangement-update row, `measured_ns` is `update_ingest_ns + update_stabilize_ns`.

Worker creation, output collection, consolidation, correctness checking, digesting, and JSON serialization are outside the measured region.

The `ddir-corgi` conversion of DDIR input rows into a Corgi container is inside
`stabilize_ns`, because it is an operator in the timed dataflow. It dominates the
cheapest cases, where no backend does useful operator work.

All three binaries install mimalloc as the global allocator. These workloads are
allocation-bound, so the allocator is a first-order term and a reproduction under the
system allocator will not match. Report it alongside the compiler and the machine.

## Adding workloads

A workload should own its deterministic input generator and canonical output representation.

Each implementation should consume identical logical input and produce the same consolidated output.

The compiled DDIR-matched implementation should remain visibly paired with the DDIR source so that changes in either plan are reviewed together.

New measurements should extend the versioned JSON record rather than changing the meaning of existing fields.

## The result schema

`schema` is currently 2. The `scc` and `operators` records carry different workload
parameters and are distinguished by `benchmark`; the version tracks the shared fields.

`revision` and `dirty` identify the source. A revision alone does not: if `dirty` is
true then uncommitted changes were present and the measurement belongs to no commit,
so a longitudinal series must discard the point. An unreadable tree reports `dirty`.

`checked_against` names the oracle a run was validated against, which is not the same
choice in every benchmark. `operators` and `arrange_updates` check against a
hand-written expected output. `scc` checks against `compiled-ddir`, which is in turn
cross-checked by the independent `strongly_connected_at` implementation.
