# Observed SF1 additional process memory

These are single observations from 2026-10-21, intended to choose the next
investigations. They are not timing targets or an official LDBC benchmark.
The machine was mini-01, Apple M4 (10 CPU cores), 16 GiB RAM, macOS 26.1
(build 25B78). No other SF1 trial ran concurrently.

The complete measurements are in [the CSV](observations/sf1-memory-2026-10-21.csv),
with [provenance](observations/sf1-memory-2026-10-21.json). Requests are pinned in
[bindings/sf1-memory.json](bindings/sf1-memory.json); these are smoke requests,
not draws from the official parameter distribution.

## What was measured

Every case starts a fresh server with one worker. First the harness loads and
settles all 17 shared input tables. Then it installs the query and binds one
request, replays all 521 prepared epochs (one update class per epoch), captures
the final answer, and retracts the request. It runs three extra empty ticks after
loading, binding, replay and retirement. These are fixed observation points,
not proof that every background allocation has reached its minimum.

A separate input-only server loads the same 11,519,991 projected initial rows
and replays the same 2,579,178 projected inserts/retractions, including cascades.
For an initial/final active measurement, **additional RSS = query process RSS
minus input-only process RSS at the corresponding state**. Thus the input
storage is not charged to every query. Each report also records its own
before-query input baseline.

RSS is sampled every 100 ms and at phase boundaries. The CSV retains both
query-active RSS and the largest sampled RSS across the entire job. Its peak
extra column subtracts the input-only job's largest sampled RSS; it is a
difference of maxima, not a simultaneous allocation measurement. Failure peaks
are censored. Allocator retention and query-induced input/history retention are
included: this does not attribute live bytes to individual operators. Small
negative differences are preserved. macOS physical footprint is also recorded
in raw reports and the peak summary, separately from RSS.

Limits were 6 GiB server RSS, 10 GiB combined Python/server RSS or physical
footprint, 120 seconds per command, and 300 seconds per case. Python answers
were not recomputed during profiling. Initial/final captures were compared
with saved, previously Python-validated answers where available. Execution,
answer validation and resource failure remain separate report fields.

IC2 uses a flat bounded top-twenty reduction; IC5 and IC9 use nested
1024/32/1 buckets, hashing forum identity for IC5 and the full sort value for
IC9. Other queries use the ordinary compiler. These choices are part of the
measurement, not the default compiler configuration.

## Observations

The input-only control used **2.141 GiB initially and 2.180 GiB finally**.
A separate repeat ended 0.625 MiB lower. Every query's own before-query
RSS was 2.141 GiB, matching the initial control at the recorded precision.
This supports broad triage; the query cases themselves were not repeated.

**39/41 queries completed the full lifecycle.** IC9 reached both active points
and matched its saved answers, then exceeded 6 GiB during post-retirement
settling. BI17 exceeded 6 GiB while binding, before answer capture. All 40
queries that reached capture matched both saved, previously Python-validated
endpoints. No new answer mismatch was observed; this does not check every epoch.

For active memory, BI15 is the largest observed case, followed by BI4, IS7
and IS2. For resource failures, investigate BI17 binding and IC9 retirement.
IC3 is about 0.511 GiB above the final input-only control while active, but
2.283 GiB above it after retirement; IC6 and IC5 also grow substantially during
retirement. Those later process footprints need a separate investigation
from steady query-active storage. Three settling ticks do not establish
whether the retained RSS is live operator state or reusable allocator memory.

All memory columns below are **additional server RSS in GiB**. Replay seconds
include journal decoding, encoding and transport, and exclude loading, binding,
answer checking and retirement. A censored peak is a lower bound. The CSV also
contains absolute RSS, initial active RSS, post-retirement RSS, whole-job time
and per-query plan hashes. The 42 primary cases took 1,223.6 seconds in total
(about 20.4 minutes), excluding this table's summarization and the control repeat.

| Query | Final active extra | Sampled peak extra | Replay (s) | Lifecycle |
| --- | ---: | ---: | ---: | --- |
| BI15 | 2.213 | 3.534 | 32.18 | completed |
| BI4 | 1.294 | 2.354 | 10.82 | completed |
| IS7 | 1.277 | 1.277 | 4.21 | completed |
| IS2 | 1.205 | 1.205 | 5.08 | completed |
| IC9 | 1.169 | ≥ 3.866 | 35.60 | limit: post-retirement settling |
| BI16 | 1.126 | 1.126 | 4.57 | completed |
| BI10 | 1.100 | 3.622 | 10.96 | completed |
| BI3 | 1.088 | 1.088 | 4.86 | completed |
| BI9 | 1.067 | 2.287 | 7.16 | completed |
| BI2 | 1.047 | 1.055 | 3.59 | completed |
| BI18 | 1.045 | 2.854 | 19.31 | completed |
| IC7 | 1.026 | 1.026 | 4.55 | completed |
| BI12 | 0.967 | 2.535 | 5.07 | completed |
| BI7 | 0.909 | 0.918 | 5.08 | completed |
| BI5 | 0.815 | 0.815 | 5.24 | completed |
| IS6 | 0.702 | 0.702 | 4.13 | completed |
| BI6 | 0.623 | 0.623 | 5.40 | completed |
| BI14 | 0.597 | 0.597 | 4.71 | completed |
| IC5 | 0.522 | 2.042 | 4.28 | completed |
| IC3 | 0.511 | 2.283 | 5.83 | completed |
| BI8 | 0.457 | 0.458 | 5.84 | completed |
| BI1 | 0.418 | 0.858 | 3.22 | completed |
| IC4 | 0.412 | 0.422 | 4.30 | completed |
| BI13 | 0.400 | 0.434 | 3.75 | completed |
| IC6 | 0.377 | 2.800 | 3.64 | completed |
| IS5 | 0.364 | 0.364 | 3.01 | completed |
| IS4 | 0.358 | 0.358 | 3.00 | completed |
| BI19 | 0.347 | 0.458 | 8.45 | completed |
| IC14 | 0.330 | 0.339 | 8.37 | completed |
| IC2 | 0.178 | 0.522 | 10.54 | completed |
| IC8 | 0.153 | 0.153 | 3.63 | completed |
| IC10 | 0.134 | 0.139 | 3.39 | completed |
| IC12 | 0.131 | 0.159 | 3.75 | completed |
| BI11 | 0.060 | 0.087 | 3.29 | completed |
| BI20 | 0.037 | 0.037 | 3.02 | completed |
| IC1 | 0.036 | 0.041 | 3.44 | completed |
| IS3 | 0.033 | 0.033 | 2.94 | completed |
| IC11 | 0.028 | 0.028 | 3.21 | completed |
| IC13 | 0.018 | 0.020 | 3.14 | completed |
| IS1 | -0.001 | -0.001 | 2.73 | completed |
| BI17 | unavailable | ≥ 3.981 | — | limit: bind |

The historical harness captured retirement's empty answer and then exited.
This profile advances three more ticks. IC9's larger peak under this lifecycle
does not by itself establish a regression in its query plan. The measured
prototype grouped all settling ticks under one phase label in its raw peak
map; the committed profiler now labels each settling phase separately.

## Reproduction and correctness

Follow [UPDATES.md](UPDATES.md) to prepare the official BI composite-merged-fk
SF1 initial snapshot and insert/delete batches. The profiler verifies journal
and adapter hashes before starting. Then run:

```sh
python3 interactive/server/bench/ldbc/profile_memory.py \
  --prepared /tmp/ldbc-prepared/journal --server /tmp/ldbc-target/release/ddir_server \
  --parameters interactive/server/bench/ldbc/bindings/sf1-memory.json \
  --bounded-topk --output /tmp/ldbc-memory
```

The committed CSV is a compact observation record; raw phase reports, plans,
captures, logs, journal and the measured binary are retained as investigation
artifacts outside Git. `summarize_memory.py DIR` regenerates the CSV/JSON from
the raw reports, including after copying the artifact directory to another host.

The measured source was clean commit `6801cb3a`; subsequent profiler cleanup
labels the settling phases separately and adds the summary/CI checks without
changing query plans or the measured operations. The measured release server
was built from `854ade89`; the provenance file pins its SHA-256. A new binary,
request, worker count or lifecycle needs its own observations.
All 41 final-branch physical plans were checked against the recorded plan hashes.

For correctness, CI checks the full small witness catalogue at one/four workers,
weighted integer aggregates against direct arithmetic, and bounded ranking
against the Python reference through all 49 update-fixture states. It also checks
late query installation, saved-answer matching and retirement on a small fixture.
The SF1 profile's saved-answer comparison does not replace those tests or the
full replay checker.
