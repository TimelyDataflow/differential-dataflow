# Refresh the current measurements

[CURRENT.md](CURRENT.md) is replaceable. Anyone can refresh it independently of
an engine PR or its author. New validated observations take precedence over an
old explanation, whether faster or slower. No causal diagnosis is required:
**not reproduced at revision X** is different from **fixed by commit X**.

Keep the current reproduction and raw reports. Obsolete experiments and data
may be removed with their references; there is no requirement to maintain an
ever-growing archive. Git preserves the previous version. [GAPS.md](GAPS.md)
tracks bounded investigations and is updated when new evidence contradicts it.

## Workload contracts

Use the standard `suite.py`, natural query definitions, full common input
schema and same-plan Python answer validation. No custom driver, selective
loader, digest-only path or handwritten query rewrite is needed. Use the
[DATA.md](DATA.md) archive/hash, setting `LDBC_SNAPSHOT` to its extracted
`initial_snapshot` directory.

Every timing recipe uses four workers, mixed mode, three request IDs per
interactive query, one standing binding per BI query, two warmup and five
measured rounds. Parameters come from the default recorded bank, without
overrides, with `PYTHONHASHSEED=0` for repeatable set iteration. Two IDs may have
equal parameters: this is a smoke workload, not three distinct bindings or the
official request distribution.

Recipes differ only in the explicitly named query grouping:

- **tiny-catalogue-v1:** all 41 queries concurrently, no downloaded data,
  one round and no warmup. Correctness/overhead gate, not scale performance.
- **sf0003-isolated-v1:** all 41 queries, each in a fresh server, both backends.
  All 35,588 common input rows are loaded for each query.
- **sf0003-panel-v1:** IS1/IS3/IC11 and BI11/BI18 concurrently, both backends.
  Measures shared-workload execution, not a sum of isolated timings.

From the repository root, after building the server:

```sh
LDBC_RESULTS_DIR=$(mktemp -d /tmp/ddir-ldbc-evidence.XXXXXX)
export PYTHONHASHSEED=0
python3 interactive/server/bench/ldbc/suite.py \
  --server target/release/ddir_server --queries all --backend both \
  --mode mixed --workers 4 --rounds 1 --warmup 0 --batch-size 3 --changes 1 \
  --timeout 120 --max-rss-gib 1 --output "$LDBC_RESULTS_DIR/tiny"
```

For each of three fresh timing trials, choose a new output directory:

```sh
python3 interactive/server/bench/ldbc/suite.py \
  --server target/release/ddir_server --snapshot "$LDBC_SNAPSHOT" \
  --queries all --isolated --backend both --mode mixed \
  --workers 4 --rounds 5 --warmup 2 --batch-size 3 --changes 1 \
  --timeout 120 --max-rss-gib 1 --output "$LDBC_RESULTS_DIR/isolated-1"

python3 interactive/server/bench/ldbc/suite.py \
  --server target/release/ddir_server --snapshot "$LDBC_SNAPSHOT" \
  --queries is1 is3 ic11 bi11 bi18 --backend both --mode mixed \
  --workers 4 --rounds 5 --warmup 2 --batch-size 3 --changes 1 \
  --timeout 120 --max-rss-gib 1 --output "$LDBC_RESULTS_DIR/panel-1"
```

Repeat as `isolated-2`/`isolated-3` and `panel-2`/`panel-3`, serially. Do
not select only favorable trials. This is fixed-snapshot bounded churn, not
growth or saturation throughput: retract one fact each from knows/member/likes,
then restore them. Many queries do not depend on those facts or their answers
do not change; these timings do not establish affected-result maintenance cost.
The effective data, bank, signed deltas, request schedule, plans and checked
answers are in each report.

Keep the hash seed set for every trial. Current `parameters.py` chooses a
default company from an unordered set; without this setting, even unused bank
fields can differ across processes and the comparison correctly refuses to
pool the reports. Stable parameter selection is a harness follow-up in GAPS.

The 1-GiB flag limits sampled **server RSS**, not total host memory. Monitor
Python plus the server, compression/swap and host headroom; stop on pressure.
An external safety monitor may cap the whole process group but must not change
the workload or timer. Record its limits and observations. Do not raise a failed
cap automatically. These small-data recipes make no SF1 capacity claim.

## Build receipt

For a timing run, build a clean identified engine checkout without unrecorded
Cargo overrides. The current M4 convention is:

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  cargo build --release -p ddir-server
shasum -a 256 target/release/ddir_server Cargo.lock
rustc -Vv
cargo --version
sysctl -n machdep.cpu.brand_string hw.model hw.memsize hw.logicalcpu
sw_vers
python3 --version
```

Record the full engine revision, Corgi/dependency revisions, exact command and
effective release profile, target, all build environment overrides, machine and
OS, Python, resource limits, and observed pressure/swap. Keep the resolved
Cargo.lock; use `--locked` for its reproduction. Preserve each binary outside a
target path the next build would overwrite, or use separate worktrees.

The report's `revision` and copied Cargo.lock describe the **workload checkout**,
not necessarily the source of `--server`. Link `binary_sha256` to the actual
engine build receipt. Equivalent M4 minis are a useful comparison class, not a
guarantee of equivalent background load or thermals.

## Accounting and validation

```sh
python3 interactive/server/bench/ldbc/compare.py BEFORE/report.json AFTER/report.json
python3 interactive/server/bench/ldbc/readout.py --summary TRIAL1/report.json TRIAL2/report.json TRIAL3/report.json
python3 interactive/server/bench/ldbc/readout.py TRIAL1/report.json
```

Paths above are placeholders. Both tools also accept `.json.gz`. Compression
is storage only; the report hash in a readout refers to decompressed JSON bytes.

`compare.py` rejects incompatible v2 data/banks/schedules/answers, selected
measurement-source changes and configuration/environment differences. It flags
changed plans. It does not certify build provenance, quiet hosts, every source
change or statistical significance. Audit those in the receipt. A rejected
comparison requires a fresh control or a new series, not bypassing the check.

`readout.py` consumes successful `snb-suite-2` raw events. Full output shows
each measured cycle plus setup, warmup and final retirement separately. Summary
output requires repeated trials of the same binary, workload and environment;
its headline is the **median of fresh-trial cycle medians**, with the range of
those trial medians. Raw per-cycle and per-phase values remain in the reports.

A cycle sums recorded, non-warmup phases, excluding derived `batch_answers`
to avoid double counting. For an interactive query it includes two batches
(initial/changed graph), complete answer reads, request retractions and empty
checks, graph mutation/restoration and checks. A BI cycle includes graph
mutation/restoration and maintained-answer reads. Final BI retirement is
separate. It is not one query's latency or an input-update throughput score.

`wire_ms` includes transport/protocol work, not just engine CPU. `client_ms`
also includes phase preparation, encoding and answer decoding. Neither cycle
sum includes intervening Python oracle/validation work. Setup sums recorded
install/load/settle/read commands, not uninstrumented Python data preparation
or process startup/shutdown. Never sum phase medians to obtain a cycle median.

Same-plan checks validate execution and lifecycle, not independent specification
conformance. Failures, timeouts and cap stops remain labeled failures; partial
timings must not be promoted to successful cycles.

## Replacing the readout

Publish current raw reports (compressed is fine), Cargo.lock, build/workload
receipt, generated tables and interpretation in the repository or at stable
artifact URLs with hashes. No session-local driver or inaccessible path should
be necessary to rerun it. Replace CURRENT.md and the relevant gap assessments.
Superseded files can be removed; do not silently combine unlike workloads.

For a claimed engine speedup, rebuild both revisions and alternate fresh A/B/B/A
runs on the same host under one workload checkout. For an updated baseline,
three validated fresh runs suffice without identifying why results changed.
Keep disagreement/noise visible as **contested** or **no material change**.

Only the named engine revision is measured. Engine/dependency changes require
remeasurement before claiming current performance; workload/timing changes
require a new series/control. Docs-only changes need no rerun. CI checks
semantics/accounting, not timing thresholds; there is no automatic benchmark
service or stale-result detector.
