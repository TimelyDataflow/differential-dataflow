# Refreshing the evidence

The contract is: **new reproducible evidence can supersede the current readout;
old measurements cannot veto it**. This applies equally to improvements,
regressions, negative findings, and corrected explanations. Anyone can submit
the refresh independently of the engine PR or its author. No causal diagnosis
is required to report what the current engine does.

There are three separate records:

- [CURRENT.md](CURRENT.md): replaceable pointers to the latest qualifying runs,
  explicitly scoped to tested revisions and workload recipes.
- Dated measurement records: retained evidence, including negative/failed runs.
  [MEASUREMENTS.md](MEASUREMENTS.md) is the initial historical archive. Correct
  errors with a visible correction; do not silently rewrite old samples.
- [GAPS.md](GAPS.md): hypotheses and bounded tasks. Supersede its conclusions
  when new evidence warrants it; a gap's old author is not a gatekeeper.

## Portable recipes, version 1

Run from the repository root, with Python 3.9+ and the existing suite. These
recipes use the **natural query definitions**, no optimizer prototype, private
TCP servers, and the full common input schema. Every selected relation's rows
are retained. Do not substitute selective inputs, digest-only validation,
handwritten query rewrites, a different bank, or different churn under the same
recipe name.

Both recipes explicitly fix mixed mode, three request IDs per interactive
query, and one standing binding per BI query. Two IDs can have equal parameters;
this is not a diverse three-query population or 30 BI bindings. All answers,
including empty results, are checked by the suite's same-plan Python evaluator.
That validates execution/lifecycle, not independent specification conformance.

### tiny-catalogue-v1

No downloads. The complete 41-query fixture, all concurrently installed, both
backends in separate servers. This is a semantic/overhead gate, **not evidence
of performance on large inputs**. Debug builds are allowed for the semantic
gate but cannot supply release timing baselines.

```sh
LDBC_RESULTS_DIR=$(mktemp -d /tmp/ddir-ldbc-evidence.XXXXXX)
python3 interactive/server/bench/ldbc/suite.py \
  --server target/release/ddir_server --queries all --backend both \
  --mode mixed --workers 4 --rounds 1 --warmup 0 --batch-size 3 --changes 1 \
  --timeout 120 --max-rss-gib 1 --output "$LDBC_RESULTS_DIR/tiny"
```

### sf0003-panel-v1

Use the pinned SF0.003 archive/hash and extraction recipe in [DATA.md](DATA.md),
setting `LDBC_SNAPSHOT` to its `initial_snapshot` directory. This panel combines
transient IS1/IS3/IC11 requests with maintained BI11/BI18. It is a small
generated-data measurement, not a replacement for SF1 or all-query coverage.

```sh
python3 interactive/server/bench/ldbc/suite.py \
  --server target/release/ddir_server --snapshot "$LDBC_SNAPSHOT" \
  --queries is1 is3 ic11 bi11 bi18 --backend both --mode mixed \
  --workers 4 --rounds 5 --warmup 2 --batch-size 3 --changes 1 \
  --timeout 120 --max-rss-gib 1 --output "$LDBC_RESULTS_DIR/sf0003-1"
```

Do not pass a parameter override: version 1 uses the recorded data-derived
bank. Data, effective bank, signed delta, request schedule and plan hashes in
the reports must match across comparable runs. Changed parameter-generation or
loader code requires a fresh series/control, even if the CLI stayed the same.
No snapshot grows: the driver retracts bounded facts and restores them. See
[SNB.md](SNB.md) for the exact lifecycle.

The 1-GiB flag is a sampled **server RSS** ceiling, not whole-host protection.
Run serially, observe host headroom/compression/swap, and stop on memory pressure.
Use external whole-process controls where available and record them. Do not
increase a failed limit automatically. Neither recipe authorizes SF1; portable
safe scale execution and wider representative banks remain LDBC-008.

## Build and provenance receipt

For timings, build an identified checkout with no unrecorded source changes or
Cargo overrides. One M4 release convention is:

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  cargo build --release -p ddir-server
```

This uses the checked-in release profile with debug information disabled. Save
the resolved Cargo.lock; use `--locked` for subsequent builds from it. Record
the exact build command/environment, `rustc -Vv`, `cargo --version`, target and
effective profile, full engine commit, Corgi revision, and any patches/local
dependencies. Dependency or toolchain changes are allowed experimental subjects,
but must not be mistaken for a change to DDIR alone. Use a separate worktree or
copy each binary outside a target path that the next build would overwrite.

Save a `BUILD.md` beside the reports linking each **binary SHA-256** to that
receipt. The runner hashes the executable, but its `revision` identifies the
**workload checkout**, not necessarily the source that built `--server`.
Likewise its copied Cargo.lock comes from the workload checkout. Neither field
alone proves the binary's engine/dependency provenance. Hash the actual binary
with `shasum -a 256` (or `sha256sum`) and check it against `binary_sha256`.

Record CPU/model, memory, OS, Python, worker count, build settings, profiling
state, resource limits and observed pressure/swap. On a mini:

```sh
sysctl -n machdep.cpu.brand_string hw.model hw.memsize hw.logicalcpu
sw_vers
python3 --version
```

Equivalent M4/16-GiB minis are the initial comparison class, not a guarantee
of identical clocks, thermals or background load. Rebuild/repeat the old
control on the new host before claiming a speedup attributable to the engine.
Retain the new-host observation even when a direct ratio is not justified.

## Repeat, compare, and render

For a new current baseline, run three fresh server trials with identical
arguments and distinct output directories. Keep every trial. For a before/after
claim, build both binaries first, then run A/B/B/A serially on the same host,
with the same workload checkout and recipe. Each recipe's warmup is performed
anew by each fresh server. Do not choose a best run or pool unlike workloads.

Use the existing checks before interpreting ratios:

```sh
python3 interactive/server/bench/ldbc/compare.py BEFORE/report.json AFTER/report.json
python3 interactive/server/bench/ldbc/compare.py BEFORE/report.json AFTER/report.json --metric client_ms
python3 interactive/server/bench/ldbc/readout.py TRIAL1/report.json TRIAL2/report.json TRIAL3/report.json
```

The uppercase report paths are placeholders for the saved trial directories.
`compare.py` rejects incompatible v2 reports, changed data/bindings/answers,
workers, measured schedule and selected timing/reference sources. It flags
changed DDP plans, which are legitimate optimization subjects when logical
queries/answers remain fixed. It does **not** certify binary provenance,
machine equivalence, all source changes, resource conditions, or significance.
Audit those using the receipt. Do not bypass an incompatibility to get a ratio:
start a new series, or run both engines under the new contract.

`readout.py` is a read-only postprocessor, not a runner or promotion service.
It emits deterministic Markdown from successful `snb-suite-2` raw events:

- Sum each measured round's actual timed phases, omitting derived
  `batch_answers` to avoid double counting. Include reads, releases, graph
  updates/restoration and empty checks. Publish every cycle and its median/range.
- Keep all five timing fields separate. `wire_ms` includes transport/protocol
  work, not just engine CPU. `client_ms` includes phase preparation, encoding
  and answer decoding. Neither includes intervening oracle/validation work;
  the cycle sum is not elapsed wall time including the oracle.
- Show setup, warmup and final retirement separately, even though the runner
  marks final retirement as warmup. These are sums of recorded commands, not
  process startup/shutdown or the uninstrumented Python loader/reference work.
- Never derive a full-cycle median by adding phase medians. Reject failed or
  incomplete reports instead of rendering partial cycles as successful times.

Retain per-trial results; there is no automatic statistical-significance claim.
Overlapping/noisy repetitions warrant **no material change** or **contested**,
not a speedup selected from one favorable sample.

## Promote a result without its engine author's involvement

1. Add a dated record under `measurements/DATE-RECIPE/` containing the recipe
   ID, full run/build commands and provenance, generated readout, validation and
   resource outcome, interpretation, and the record it supersedes. Include all
   raw reports/plans/lockfiles, or durable public artifact URLs and SHA-256s.
   Another session's absolute paths, unpublished commits, or hashes without
   retrievable artifacts are not sufficient for a reproducible current record.
2. Check the contract above. A later valid result can replace the observation
   in [CURRENT.md](CURRENT.md), even when worse, or when an old bottleneck is
   absent for unexplained reasons. Make the tested revision explicit. A
   workload change starts a new series; it does not overwrite the old series
   as an engine speedup.
3. Update affected gap assessments and links. **Not reproduced at X** is enough
   to retire a performance hypothesis; do not require a bisect or invent a
   fix attribution. A faster timing cannot establish that a correctness bug
   is fixed. Keep unresolved replication disagreements visible.
4. Preserve the earlier dated record and point to its successor. Refreshes
   can be ordinary small docs/evidence PRs, independent of engine changes.

Performance qualifications do not automatically transfer across engine or
dependency revisions. Recheck after such changes before describing them as
current; docs-only commits need no rerun. On a new workload/timing contract,
label **needs fresh baseline** until qualifying data exists. CI checks semantics
and the postprocessor's accounting, not M4 timing thresholds; there is no
scheduled benchmark or automatic stale-result detector in this follow-on.

The initial historical SF1 records explicitly **do not meet this replay
contract**: their experimental drivers/oracles are not yet public. They remain
leads, not current baselines. To refresh one, first land its exact driver/recipe
or publish a new portable experiment with its differences called out. Do not
claim that either portable recipe above recreates those older numbers.
