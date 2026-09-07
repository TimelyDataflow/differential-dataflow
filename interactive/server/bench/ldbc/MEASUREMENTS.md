# Measurement record: M4 mini, 2026-09-07

Observations to guide [gap investigations](GAPS.md), not an official LDBC score,
a backend leaderboard, or a current-master qualification. The foundation merged
as `75fba2b9` (#864). **The SF1 timings below predate that merge and its v2
harness.** Establish a fresh baseline before claiming an improvement to merged
code. Equivalent M4 minis are useful comparison hosts; they do not make
different plans, bindings, builds, or swap states equal.

## Machine, builds, and timing epochs

Apple M4 Mac mini (`Mac16,10`), 10 logical CPUs, 16 GiB memory, macOS 26.1 /
build 25B78, Python 3.9.6, `rustc 1.95.0 (59807616e 2026-04-14)`.
Hardware/OS were rechecked when preparing this record. Trials used private real
TCP servers, normally four workers. No two campaign timing jobs intentionally
overlapped; the overnight host was not otherwise proven idle.

| Image / experiment | Native source / Corgi | Release build |
| --- | --- | --- |
| Original overnight; fresh IC11 backend sweep | DDIR `8da53dfc`, Corgi `f6acd14` | opt3, LTO, one codegen unit, debug info on, incremental off |
| BI1 count pair | Forwarding prototype `18e281f4`, same Corgi | Same settings on both sides; only authored count lowering changes |
| Fresh IC11 survey A/B/B/A | DDIR `302c45ea`, Corgi `be00398` / candidate `d4e9f5c` | Matched opt3/LTO/one codegen unit, debug info off, incremental off; candidate used a local dependency override |

The overnight epoch had substantial pre-existing swap and ended in a
compressed-memory watchdog restart during an IC12 retry. Its broad timings
are historical leads, not evidence for small percentage changes. Post-reboot
IC11 sweep/survey runs had normal observed pressure, zero swap, and a 2-GiB
**combined process-group footprint** cap. They are a separate epoch even when
the binary is unchanged. Sampled footprint is not RSS, cumulative allocation
traffic, or a hard OS memory limit.

The historical harness had experimental selective loading, complete supplied
parameters, digest-only measurement, and separate independent checkers. These
options and diagnostic drivers are not merged. Native source is identified
separately from workload checkout `revision`: that field alone does not
identify the server binary.

## Preserved evidence and its limits

- [2026-09-07.json](measurements/2026-09-07.json): image/source hashes,
  workload/plan/data fingerprints, per-cycle IC11 wire samples, matched BI1
  samples and exact deltas, IC13 diagnostics, original report/certificate
  hashes. IC11 cycles were re-summed from raw events; applicable report hashes
  were checked against saved certificates.
- [2026-09-07-census.csv](measurements/2026-09-07-census.csv): all 41
  original-native SF1 outcomes, phase medians, input/delta sizes, failure
  classes. Partial timings remain failed lifecycles. Blank is not zero.
  Initial settlement and final retirement are one-off observations, included
  despite the old harness marking them outside measured rounds.
- [ic11-sf1-parameters.json](measurements/ic11-sf1-parameters.json): all 32
  effective tuples used in the fresh diversity/survey comparisons.
- [DATA.md](DATA.md): pinned archive identities and a small-data run recipe.

These are compact archival extracts, **not** v2 reports for `compare.py`.
Raw reports/logs, oracle programs, profiles, native images, and experimental
branches are not bundled or publicly archived here. Hashes identify original
artifacts; they are not download locations or substitutes for independent
validation. Some revisions name unpublished prototypes. Another checkout
cannot yet rerun every historical experiment verbatim.

Times below are milliseconds unless noted. `wire_ms` is client-observed
request/response time including transport/protocol handling, not server CPU.
Old headline totals included Python preparation and sometimes full-graph set
differences; these tables use recorded **wire** samples instead. That does not
make old and v2 reports comparable across changed loaders and workload code.

## M1: IC11 request cycles

Post-reboot, original preserved image; four workers; SF1 IC11 only, 214,768
selected rows (person, knows, place, organisation, employment). Four measured
cycles follow two warmups. Each cycle contains **two** request batches, reads,
releases, graph update/restoration, and all empty checks. Derived
`batch_answers` is excluded to avoid double counting. These are neither
individual-request latency nor QPS.

| IDs / distinct tuples per batch | Corgi median cycle | Vec median cycle | Sampled group footprint GiB, Corgi / Vec |
| --- | ---: | ---: | ---: |
| 1 / 1 | 25.72 | 41.72 | 1.14 / 0.66 |
| 3 / 2 | 61.87 | 109.92 | 1.15 / 0.75 |
| 12 / 6 | 225.65 | 422.39 | 1.18 / 1.06 |
| 48 / 24 | Not attempted | Cap crossed during warmup release | No completed measurement |

Each completed run had 37 independently checked snapshots using a separate
raw-CSV two-hop employment/top-ten evaluator, not official conformance checks.
The bank selects eight people per friendship-degree quartile, interleaved,
with country/year predicates derived from input statistics, not answers. Each
tuple is assigned to two IDs. Across measured cycles the sizes cover 8/9/13
distinct tuples and have 1/8, 2/24, and 2/96 empty answers. Different sizes
are **not** pure batching ablations with identical marginal parameters;
each backend pair does have matching schedules and answers.

The complete-cycle Corgi/Vec gain is about 1.6–1.9x here, not every phase/query.
Empty-check subtotals remain included: Corgi cycle medians 3.70/7.52/19.70 ms
versus Vec 0.52/0.73/1.91 ms. An older two-binding smoke bank gave roughly
192-ms Corgi three-ID cycles with the same engine. Switching banks is **not**
a 3x optimization.

For new baselines reuse the bank/configuration, but first address
[LDBC-008](GAPS.md#ldbc-008-portable-scale-runs-and-representative-workload-banks).
Current `--queries ic11 --snapshot ...` does not recreate the selective
substrate or historical validation schedule.

## M2: BI1 count lowering

Overnight swap-active epoch; four workers; standing BI1; same forwarding
prototype image on both sides. Written queries, bindings and deltas match
within each pair; emitted DDP changes to use native counts. This measures an
authoring/compiler experiment, not the automatic DDIR collect/length optimizer.

| Affected year/kind/length group | Measured rounds / warmups | Control update / restore | Count lowering update / restore |
| --- | ---: | ---: | ---: |
| 8,189 short 2010 comments | 5 / 1 | 482.41 / 512.75 | 4.60 / 3.67 |
| 1,012,596 short 2012 comments | 3 / 1 | 1,032.40 / 948.39 | 518.68 / 428.50 |

The delta removes/restores one leaf comment. The large-group probe selects a
different comment, not more changed rows. About half a second disappears in
both cases; residual mixed count/sum work scales with the affected group.
Thus the gain is roughly 100x in one case and 2x in the other. All 28 small-group
and 20 large-group snapshots were checked against independently computed CSV
statistics and exact deltas. Merged edge-only churn does not recreate these
affected BI1 changes. The automatic optimizer's SF1 comparison remains unrun;
do not assign these gains to it. See [LDBC-001](GAPS.md#ldbc-001-aggregate-observations-materialize-whole-groups).

A separate original-native BI6 affected-result control had update/restore
2.60 / 2.31 ms, eight independently checked snapshots, and two changed displayed
scores. Maintained aggregation is not uniformly slow.

## M3: Lifetime and recursion diagnostics

Overnight historical observations, not current-master timings:

- Original-native IC6 bind/release: about **2,790 / 1,754 ms** for three-ID
  batches, 19 snapshots independently checked. A separate idle-history probe
  found roughly 1-ms graph updates before any request, **1.1–1.4 seconds** on
  the first update after release, then roughly 1 ms again. This is deferred
  work, not permanent idle-maintenance cost.
- BI9: roughly **33-ms** maintenance, then a **90-second retirement timeout**.
  BI20 completed but retirement took about **7,780 ms**. Update medians alone
  miss these lifecycle costs.
- Synthetic IC13: one edge `(0,1)`, request `0 -> 1`, all other people isolated.
  At 32 people Corgi bind/release was **2.11 / 2.04 ms**; at 10,295 people bind
  was **573.76 ms**, with the same distance-one answer. The extract retains
  intermediate sizes and Vec at 32/128/512. Each size is one sample, not a
  repeated latency estimate or SF1's actual path distribution.

Separate saved operator profiles provide leads, not CPU percentages: BI1 had
717 ms Linear / 281 ms Reduce summed worker activity over two updates; IC6
had 7.48 s Join / 7.46 s Arrange / 1.66 s Linear over two initial binds.
Worker activity is not critical-path latency; a Join can advance traces without
new input. The profiles are not bundled. Reproduce attribution before choosing
a kernel intervention. See LDBC-002 through LDBC-004.

## M4: Survey prototype negative control

A synthetic microprobe compared one pair of 131,072-element lists of singleton
tuples. Public survey equality took **33.031 ms**; a bounded-block prototype
took **0.367 ms**. Requested allocation traffic dropped from about 126.9 MB
to 0.26 MB (cumulative bytes, not peak memory). Three timing samples followed
a warmup, with allocation counting measured separately. This is not an LDBC
query or a pair of otherwise-identical full DDIR builds.

The subsequent IC11 comparison used corrected newer DDIR merging on both
sides; only the single-pair survey implementation changed. It used M1's
three-ID bank/configuration and matched nodebug release images, normal pressure
and zero swap. Execution order was A/B/B/A:

| Run | Median complete wire cycle | Sampled group footprint GiB |
| --- | ---: | ---: |
| Control 1 | 57.96 | 1.16 |
| Candidate 1 | 59.45 | 1.18 |
| Candidate 2 | 59.68 | 1.19 |
| Control 2 | 59.69 | 1.17 |

Means over eight cycles per side are **60.69 / 60.73 ms**. All 37 snapshots
per run independently checked; data/plans/schedules agree. No material
whole-cycle improvement is established. No specialized-shape frequency/CPU
profile was collected, leaving rarity versus offsetting costs unresolved.
BI1/wide-query effects were not measured. Keep this contrary result attached
to [LDBC-007](GAPS.md#ldbc-007-kernel-wins-need-whole-query-evidence).

## M5: Review measurements

The prerequisite reviewer supplied this 100,000-row list-ordering probe:
old length-first sort versus building lexicographic ranks and sorting them.
**Not independently rerun here; machine/build/raw samples were not supplied.
These do not inherit the M4 machine description above.**

| List length | Alphabet | Old ms | New ms | New / old |
| --- | ---: | ---: | ---: | ---: |
| 4 | 1,000 | 4.3 | 13.4 | 3.1x |
| 16 | 1,000 | 4.3 | 34.6 | 8.1x |
| 4 | 4 | 5.0 | 11.5 | 2.3x |
| 16 | 4 | 16.5 | 49.1 | 3.0x |

The reviewer attributed nearly all additional time to rank construction and
reported ordering/equality agreement on 6,000 nested/tie-heavy fuzz cases.
Non-list values do not pay this list-ranking cost. This is a lead for
LDBC-005, not a whole-query slowdown or permission to restore the wrong order.

## M6: Capacity and coverage

The original-native isolated SF1 census has **30 completed lifecycles**: two
same-plan Python checked, 28 digest-only. Later separate certificates for
selected queries do not upgrade every census row to independently validated.

Six failures were two-second `ps` monitor timeouts: IC3, IC5, IC9, IC12, IC14,
BI17. Five were query-command timeouts: IC13 bind; BI10/BI15/BI19 initial
settlement; BI9 retirement. IC12's host-restart retry was not a successful
replacement. Monitor failure does not prove engine capacity requirements.
The [census CSV](measurements/2026-09-07-census.csv) retains all outcomes.

Fresh IC11 Vec48 crossed the 2-GiB group-footprint cap during warmup release
(2,196,343,136 bytes); Corgi48 was left untried. A fresh one-worker concurrent
IS1/IS3/IC11 + standing BI11/BI18 attempt crossed the same cap during initial
settlement: 2,015,004,544 native bytes plus 162,267,592 Python bytes at that
sample. Pressure was normal, swap zero, Vec unattempted. No measured-cycle
result exists for that panel. This is a limit under a conservative budget,
not proof all 16 GiB are insufficient. There is no all-query concurrent SF1
result.

## Making a new comparable record

Start with the tiny fixture, then pinned SF0.003; keep SF1 opt-in with
whole-process memory controls. Record CPU model/memory explicitly: reports
currently include platform/logical CPUs, not a full hardware inventory. On a
mini capture `sysctl -n machdep.cpu.brand_string hw.model hw.memsize hw.logicalcpu`,
`sw_vers`, `rustc --version`, `python3 --version`, and pressure/swap observations.

A common release convention for future M4 records, from the repository root:

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  cargo build --release -p ddir-server
LDBC_RESULTS_DIR=$(mktemp -d /tmp/ddir-ldbc-results.XXXXXX)
python3 interactive/server/bench/ldbc/suite.py --server target/release/ddir_server \
  --queries ic11 bi11 --workers 4 --rounds 5 --warmup 2 \
  --output "$LDBC_RESULTS_DIR/before"
```

This uses the tiny fixture, **not** SF1 or M1's parameters: a portable
command/answer gate. For timing supply the chosen snapshot and parameter bank
and keep them unchanged. Preserve Cargo.lock; use `--locked` on subsequent
builds. Build both sides before alternating trials, retaining binaries outside
a target path the next build overwrites. Record local patches, debug-info
choice, workers, and the resource envelope.

Repeat with the candidate and a new `after` directory, then use
`compare.py before/report.json after/report.json`. It checks v2 compatibility
and flags changed plans; it does not establish quiet hosts, equivalent CPUs,
or statistical significance. Sum actual per-round events for cycle statistics,
excluding warmups/derived `batch_answers`; a sum of medians is not a cycle median.

Publish raw v2 reports (or stable artifact link/checksum), build/hardware
metadata, repeat order, and validation method with each new entry. Report
median/range and raw samples; distinguish parameter windows from identical
repeats. Do not pool swap epochs or raise caps automatically after failure.
Update the gap with its fix or negative finding, preserving historical results.
