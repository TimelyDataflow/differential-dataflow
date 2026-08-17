# Preliminary V0 operator scorecard

This report is exploratory and is not yet a longitudinal baseline.

## Configuration

The benchmark ran on 2026-08-17 on an Apple M2 MacBook Air with four performance cores, four efficiency cores, and 24 GB of memory.

The operating system was macOS 26.5.1 on `arm64`.

The compiler was `rustc 1.95.0 (59807616e 2026-04-14)`.

The build used the workspace release profile, including LTO and one codegen unit.

The base revision was `78e6f1ea78dad1798dc7d09cd034cf201bda1845`.

The working tree contained the uncommitted benchmark project, so the revision alone does not identify the tested source.

Each load-time point used one timely worker, 100,000 input rows, seed `3237998146`, two warmup runs, and five measured runs.

Reducer and duplicate-arrangement cases used 1,000 keys.

The fanout join used fanout four.

The update case formed an arrangement of 100,000 rows, then retracted 1,000 existing rows and inserted 1,000 new rows.

The reported times are medians of `measured_ns` across the five runs.

The typed implementation is both the optimized and plan-matched compiled implementation for these atomic cases.

The [raw JSONL records](2026-08-17-operators.jsonl) contain all 240 measured implementation runs.

## Results

| Case | Compiled | DDIR Vec | DDIR Corgi | Vec / compiled | Corgi / compiled | Corgi / Vec |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Identity | 0.322 ms | 3.583 ms | 11.044 ms | 11.13x | 34.31x | 3.082x |
| One map | 1.378 ms | 33.296 ms | 11.541 ms | 24.16x | 8.37x | 0.347x |
| Eight maps | 4.399 ms | 130.219 ms | 13.416 ms | 29.60x | 3.05x | 0.103x |
| Filter: retain none | 0.881 ms | 19.822 ms | 11.478 ms | 22.49x | 13.02x | 0.579x |
| Filter: retain half | 1.159 ms | 22.322 ms | 12.157 ms | 19.25x | 10.49x | 0.545x |
| Filter: retain all | 0.910 ms | 24.250 ms | 12.232 ms | 26.65x | 13.44x | 0.504x |
| Arrange: sorted unique | 5.303 ms | 32.237 ms | 17.471 ms | 6.08x | 3.29x | 0.542x |
| Arrange: random unique | 11.938 ms | 46.397 ms | 18.202 ms | 3.89x | 1.52x | 0.392x |
| Arrange: duplicates | 2.224 ms | 20.605 ms | 15.725 ms | 9.26x | 7.07x | 0.763x |
| Join: one-to-one | 30.064 ms | 110.679 ms | 48.969 ms | 3.68x | 1.63x | 0.442x |
| Join: partial miss | 26.132 ms | 94.468 ms | 45.350 ms | 3.62x | 1.74x | 0.480x |
| Join: fanout four | 29.135 ms | 172.729 ms | 53.468 ms | 5.93x | 1.84x | 0.310x |
| Distinct | 1.941 ms | 43.766 ms | 27.505 ms | 22.55x | 14.17x | 0.628x |
| Count | 1.894 ms | 43.674 ms | 28.130 ms | 23.05x | 14.85x | 0.644x |
| Min | 9.959 ms | 45.103 ms | 29.308 ms | 4.53x | 2.94x | 0.650x |
| Arrange: update 1,000 | 0.073 ms | 0.570 ms | 0.334 ms | 7.78x | 4.56x | 0.586x |

Corgi does not outperform compiled Rust on any V0 row at this scale.

Corgi outperforms Vec on every non-identity row.

The identity row exposes fixed conversion and rendering costs: Corgi is 3.08x slower than Vec when neither backend performs useful operator work.

Eight maps amortize that fixed cost and make Corgi 9.71x faster than Vec, but Corgi remains 3.05x slower than compiled Rust.

Random input order increases compiled arrangement time from 5.303 ms to 11.938 ms, while Corgi changes from 17.471 ms to 18.202 ms.

The joins leave the smallest Corgi-to-compiled load-time gaps, ranging from 1.63x to 1.84x.

The 1,000-row arrangement update takes a median 0.073 ms compiled, 0.570 ms with Vec, and 0.334 ms with Corgi.

These update measurements are short enough that a larger batch or more repetitions would be appropriate before drawing fine-grained conclusions.

## Measurement boundaries

Load-time `measured_ns` sums parsing and lowering, dataflow construction, input submission, and stabilization.

The compiled rows report zero parsing and lowering time.

The update row excludes parsing, construction, and initial arrangement formation from `measured_ns`.

It includes only submission and stabilization of the second batch.

Those excluded startup phases remain present as separate raw fields.

Timed dataflows probe their native output containers without capturing or materializing the result.

Conversion of DDIR input rows into a Corgi container is included in stabilization time.

Before timing each load case, an untimed run materializes Vec and Corgi outputs and compares them exactly with the typed result.

Before timing the update case, an untimed two-batch run consolidates all emitted changes and compares the final arrangement exactly across all implementations.

The implementation order rotates between repetitions.

## Limits

V0 is single-worker and in-memory.

It does not measure scale-out, peak memory, allocation counts, skew sweeps, row-width sweeps, or long update sequences.

Each case has one fixed scale point, so fixed overhead and throughput are not yet separated by a fitted curve.

The dirty working tree should be replaced by a committed source revision before adopting a longitudinal baseline.
