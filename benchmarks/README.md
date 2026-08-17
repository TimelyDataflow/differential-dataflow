# Differential dataflow benchmarks

This project measures the cost of expressing and executing computations through DDIR.

Each benchmark should provide four implementations.

1. `compiled` is an optimized statically typed differential dataflow.
2. `compiled-ddir` is a statically typed transcription of the DDIR operator plan.
3. `ddir-vec` interprets the DDIR program over row-oriented `Value` records.
4. `ddir-corgi` interprets the same DDIR program over the Corgi columnar backend.

The first ratio identifies costs imposed by the DDIR-expressible plan.

The second ratio identifies dynamic representation and interpretation costs.

The third ratio identifies the effect of the columnar backend.

## SCC benchmark

The initial benchmark computes the subset of input edges whose endpoints are in the same strongly connected component.

The `compiled` variant uses `strongly_connected_at` with the node identifier.

The `compiled-ddir` variant transcribes DDIR's nested joins, reductions, and variables into Rust types.

Both compiled variants introduce labels in the same rounds as DDIR's `enter_at($1[0])`.

Run a small validation with:

```text
cargo run -p benchmarks --bin scc -- --nodes 100 --edges 200 --runs 1
```

Run an optimized measurement with:

```text
cargo run --release -p benchmarks --bin scc -- --nodes 100000 --edges 200000 --warmup 1 --runs 5
```

The program writes one JSON object per measured implementation run to standard output.

Progress and a ratio summary go to standard error.

All four output multisets must agree before the program emits measurements.

The implementation order rotates between runs to reduce fixed ordering bias.

This first benchmark is a single-worker, single-batch measurement.

It does not yet measure update streams, scale-out, peak memory, or allocation counts.

The [preliminary SCC report](reports/2026-08-17-scc.md) shows the initial report format and results.

## Operator scorecard

The `operators` binary measures atomic operators whose optimized and DDIR-matched compiled plans coincide.

It therefore has three implementations: typed compiled, DDIR Vec, and DDIR Corgi.

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
cargo run --release -p benchmarks --bin operators -- --case all --rows 100000 --keys 1000 --fanout 4 --warmup 2 --runs 5
```

The timed output is probed in its native container without materialization.

An untimed validation materializes each output and requires exact agreement before measurement.

The [preliminary operator report](reports/2026-08-17-operators.md) records the first results.

The `arrange_updates` binary measures a second batch after a populated arrangement reaches time one.

The batch retracts existing rows and inserts the same number of new rows.

Run it with:

```text
cargo run --release -p benchmarks --bin arrange_updates -- --rows 100000 --update-rows 1000 --warmup 2 --runs 5
```

Its `measured_ns` includes only update submission and stabilization.

Parsing, construction, and initial arrangement formation are recorded separately.

An untimed two-batch run consolidates all output changes and checks the final arrangement exactly before measurement.

## Measurement boundaries

`prepare_ns` measures DDIR parsing, lowering, and optimization.

It is zero for the compiled variants.

`build_ns` measures timely and differential dataflow construction.

`ingest_ns` measures submission and flushing of the input updates without stepping the worker.

`stabilize_ns` measures worker execution until the output frontier reaches the end of the input batch.

For load-time rows, `measured_ns` is the sum of these phases.

For the arrangement-update row, `measured_ns` is `update_ingest_ns + update_stabilize_ns`.

Worker creation, output collection, consolidation, correctness checking, digesting, and JSON serialization are outside the measured region.

## Adding workloads

A workload should own its deterministic input generator and canonical output representation.

Each implementation should consume identical logical input and produce the same consolidated output.

The compiled DDIR-matched implementation should remain visibly paired with the DDIR source so that changes in either plan are reviewed together.

New measurements should extend the versioned JSON record rather than changing the meaning of existing fields.
