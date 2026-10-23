# V0 operator scorecard

This report supersedes the preliminary 2026-08-17 scorecard, which measured a dirty
working tree on a different machine and attributed DDIR's runtime embedding cost to
interpretation. Its numbers should not be compared against these.

## Configuration

The benchmark ran on 2026-08-19 on an Apple M4 Mac mini with four performance cores, six efficiency cores, and 16 GB of memory.

The operating system was macOS 15.6 (build 24G84), Darwin 24.6.0, on `arm64`.

The compiler was `rustc 1.95.0 (59807616e 2026-04-14)`.

The build used the workspace release profile, including LTO and one codegen unit.

All three binaries install mimalloc as the global allocator. These workloads are allocation-bound, so a reproduction under the system allocator will not match.

The revision was `1d02e4d515cf629d2f685134ddf5b5221f278c9b` with a clean working tree, and every record in this report carries `"dirty": false`.

Each load-time point used one timely worker, 100,000 input rows, seed `3237998146`, two warmup runs, and eight measured runs.

Eight runs is two complete rotations of the four implementations, so each implementation leads a repetition exactly twice.

Reducer and duplicate-arrangement cases used 1,000 keys.

The fanout join used fanout four.

The update case formed an arrangement of 100,000 rows, then retracted 1,000 existing rows and inserted 1,000 new rows.

The reported times are medians of `measured_ns` across the eight runs.

The [raw JSONL records](2026-08-19-operators.jsonl) contain all 512 measured implementation runs.

## The two compiled implementations

Atomic operators have one typed operator plan: DDIR does not force a different map or a different join. But DDIR renders every program inside an iterative scope, so its operators run at `Product<u64, PointStamp<u64>>` where a typed program runs at `u64`.

`compiled` renders the plan in the root scope at `u64`, which is what a typed program would write.

`compiled-ddir` renders the identical plan in the iterative scope at DDIR's timestamp.

The ratio between them is the cost of DDIR's runtime embedding. The previous report folded it into the interpretation ratio, where it inflated every row and would have grown with any future change to DDIR's timestamp without any interpreter getting slower.

Both compiled implementations are executed during validation, not assumed to agree: rendering at a coarser timestamp lattice selects different code paths inside the reduce and join operators.

## Absolute time

| Case | Compiled | Compiled, DDIR scope | DDIR Vec | DDIR Corgi |
| --- | ---: | ---: | ---: | ---: |
| Identity | 0.258 ms | 0.526 ms | 1.284 ms | 8.088 ms |
| One map | 0.990 ms | 1.827 ms | 22.997 ms | 8.413 ms |
| Eight maps | 3.555 ms | 6.649 ms | 97.523 ms | 10.038 ms |
| Filter: retain none | 0.729 ms | 1.057 ms | 14.949 ms | 8.412 ms |
| Filter: retain half | 0.913 ms | 1.303 ms | 17.044 ms | 8.903 ms |
| Filter: retain all | 0.738 ms | 1.160 ms | 18.858 ms | 9.061 ms |
| Arrange: sorted unique | 4.098 ms | 5.928 ms | 24.250 ms | 13.139 ms |
| Arrange: random unique | 9.343 ms | 12.423 ms | 35.640 ms | 13.758 ms |
| Arrange: duplicates | 1.701 ms | 2.647 ms | 15.394 ms | 11.483 ms |
| Join: one-to-one | 22.695 ms | 30.344 ms | 81.850 ms | 35.663 ms |
| Join: partial miss | 20.236 ms | 26.806 ms | 72.103 ms | 33.169 ms |
| Join: fanout four | 21.862 ms | 33.718 ms | 121.975 ms | 36.787 ms |
| Distinct | 1.594 ms | 2.850 ms | 32.472 ms | 21.003 ms |
| Count | 1.589 ms | 2.874 ms | 32.792 ms | 21.166 ms |
| Min | 7.427 ms | 11.064 ms | 33.849 ms | 22.102 ms |
| Arrange: update 1,000 | 0.060 ms | 0.104 ms | 0.436 ms | 0.273 ms |

## Ratios

| Case | Embedding | Vec / embedded | Corgi / Vec | Corgi / embedded |
| --- | ---: | ---: | ---: | ---: |
| Identity | 2.04x | 2.44x | 6.301x | 15.37x |
| One map | 1.85x | 12.59x | 0.366x | 4.60x |
| Eight maps | 1.87x | 14.67x | 0.103x | 1.51x |
| Filter: retain none | 1.45x | 14.14x | 0.563x | 7.96x |
| Filter: retain half | 1.43x | 13.08x | 0.522x | 6.83x |
| Filter: retain all | 1.57x | 16.26x | 0.480x | 7.81x |
| Arrange: sorted unique | 1.45x | 4.09x | 0.542x | 2.22x |
| Arrange: random unique | 1.33x | 2.87x | 0.386x | 1.11x |
| Arrange: duplicates | 1.56x | 5.82x | 0.746x | 4.34x |
| Join: one-to-one | 1.34x | 2.70x | 0.436x | 1.18x |
| Join: partial miss | 1.32x | 2.69x | 0.460x | 1.24x |
| Join: fanout four | 1.54x | 3.62x | 0.302x | 1.09x |
| Distinct | 1.79x | 11.39x | 0.647x | 7.37x |
| Count | 1.81x | 11.41x | 0.645x | 7.37x |
| Min | 1.49x | 3.06x | 0.653x | 2.00x |
| Arrange: update 1,000 | 1.73x | 4.19x | 0.626x | 2.62x |

## Observations

Rendering the same typed plan in DDIR's iterative scope costs between 1.32x and 2.04x. That is a large fraction of what the previous report called interpretation, and it is paid before any interpreter runs.

The embedding cost is largest where operator work is smallest. Identity pays 2.04x for an `enter` and a wider timestamp and nothing else; the joins pay 1.32x to 1.54x because real operator work dominates.

Corgi does not outperform compiled Rust on any V0 row at this scale, in either scope.

Corgi outperforms Vec on every row except identity, from 0.75x down to 0.10x.

The identity row measures conversion, not operators. Corgi spends 7.599 ms of its 8.088 ms in stabilization, which for this case is the `ToCorgi` conversion of 100,000 rows into a container, with no operator downstream to amortize it.

Eight maps is Corgi's best row: 0.103x of Vec, and only 1.51x the same plan compiled into the same scope. Chained maps amortize the fixed conversion across eight columnar passes.

The joins leave the smallest remaining gap after the conversion is amortized, at 1.09x to 1.24x of the embedded compiled plan.

Random input order raises compiled arrangement time from 4.098 ms to 9.343 ms, while Corgi moves only from 13.139 ms to 13.758 ms. Corgi is largely insensitive to input order here; the typed path is not.

The 1,000-row arrangement update takes a median 0.060 ms compiled, 0.104 ms compiled in DDIR's scope, 0.436 ms with Vec, and 0.273 ms with Corgi. These are short enough that a larger batch or more repetitions would be appropriate before drawing fine-grained conclusions.

## Variation

Max-minus-min as a percentage of the median, over the eight measured runs.

| Case | Compiled | Compiled, DDIR scope | DDIR Vec | DDIR Corgi |
| --- | ---: | ---: | ---: | ---: |
| Identity | 21.0% | 17.4% | 4.1% | 2.2% |
| One map | 8.3% | 3.2% | 1.1% | 2.8% |
| Eight maps | 9.1% | 17.8% | 2.0% | 2.1% |
| Filter: retain none | 14.2% | 9.2% | 2.4% | 3.0% |
| Filter: retain half | 6.4% | 6.6% | 0.9% | 1.5% |
| Filter: retain all | 19.0% | 4.4% | 0.8% | 3.4% |
| Arrange: sorted unique | 5.2% | 3.5% | 7.1% | 2.3% |
| Arrange: random unique | 2.6% | 2.6% | 5.1% | 3.1% |
| Arrange: duplicates | 5.9% | 7.5% | 1.0% | 3.3% |
| Join: one-to-one | 2.2% | 2.6% | 4.0% | 5.5% |
| Join: partial miss | 2.1% | 4.2% | 3.8% | 3.0% |
| Join: fanout four | 2.9% | 8.7% | 2.6% | 2.7% |
| Distinct | 7.0% | 14.2% | 1.8% | 3.2% |
| Count | 3.9% | 5.0% | 2.4% | 2.3% |
| Min | 2.9% | 2.7% | 2.1% | 2.1% |
| Arrange: update 1,000 | 11.6% | 9.1% | 1.8% | 5.6% |

The dispersed rows are the sub-millisecond ones, where fixed cost rather than throughput is being measured. Every ratio drawn from a row above roughly 10% should be read as one significant figure.

Balancing the rotation did not measurably reduce dispersion on this machine. It was adopted because an unbalanced rotation biases the median, not because it was observed to reduce variance, and this report should not be read as evidence that it does.

## Measurement boundaries

Load-time `measured_ns` sums parsing and lowering, dataflow construction, input submission, and stabilization.

The compiled rows report zero parsing and lowering time.

The update row excludes parsing, construction, and initial arrangement formation from `measured_ns`. It includes only submission and stabilization of the second batch. The excluded phases remain present as separate raw fields.

Every implementation consumes rows prepared before the timed region, by value. The previous report's harness cloned each `Value` row at the DDIR input handle, which charged the DDIR paths a deep copy the typed paths never paid; it showed as a flat 2.4 ms against 0.27 ms in every case. Ingest is now 0.50 ms for DDIR against 0.25 ms compiled, and the remainder is the real cost of submitting `Value` rows.

Timed dataflows probe their native output containers without capturing or materializing the result.

Conversion of DDIR input rows into a Corgi container is inside stabilization, because it is an operator in the timed dataflow.

Before timing each load case, an untimed run materializes all four outputs and compares them exactly against a hand-written expected result.

Before timing the update case, an untimed two-batch run consolidates all emitted changes and checks the final arrangement exactly across all four implementations.

The implementation order rotates between repetitions, and the run count is required to be a multiple of the implementation count so that the rotation completes.

## Limits

V0 is single-worker and in-memory.

It does not measure scale-out, peak memory, allocation counts, skew sweeps, row-width sweeps, or long update sequences.

Each case has one fixed scale point, so fixed overhead and throughput are not yet separated by a fitted curve. The sub-millisecond rows in particular are dominated by fixed cost.

The embedding ratio is measured only at the root of the iterative scope, where `PointStamp` is empty. A DDIR program that actually iterates carries longer pointstamps, and this scorecard says nothing about how the embedding cost grows with depth.
