# Current M4 measurement receipt

Isolated catalogue measured 2026-09-10; concurrent panel and tiny fixture
measured 2026-09-12 with the same binary. The engine and workload checkout are unmodified public
master-next `229508dd6f7d046c5d1ac1b96da4be2f7b100124`, also verified against
the remote during the run. No engine/query optimization is supplied by this
evidence change.

## Engine

- Corgi: `be003988f17e3998a8abba3e168a2520a46d10e4` (the stock dependency pin).
- Target: `aarch64-apple-darwin`.
- Rust: `rustc 1.95.0 (59807616e 2026-04-14)`, commit
  `59807616e1fa2540724bfbac14d7976d7e4a3860`, LLVM 22.1.2.
- Cargo: `cargo 1.95.0 (f2d3ce0bd 2026-03-21)`.
- Release: opt-level 3, LTO, one codegen unit, debug assertions off, debug
  information disabled; one build job, incremental compilation disabled.
- No local/global Cargo config, dependency override, Rust flags, target
  override or compiler wrapper was present.
- Binary SHA-256:
  `3ef184745f2fedefa78e255b7dc4ca55feb4ff0f7110f9ac2bc1118648b5e8c8`.
- [Cargo.lock](Cargo.lock) SHA-256:
  `60e1637590bb68d3d9cac03bb210e04cbd9ee5b85400e951551434441e441232`.

The build command in that clean checkout was:

```sh
CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0 CARGO_PROFILE_RELEASE_DEBUG=0 \
  cargo build --release -p ddir-server
```

For exact dependency reproduction, put the saved lockfile in a fresh engine
checkout before building and add `--locked`. Future builds may produce different
binary bytes (for example from checkout paths); record the new hash. The
receipt links this measured binary to its source, rather than claiming a
workload checkout's revision by itself proves executable provenance.

## Workload and hardware

The exact standard-runner recipes are in [REFRESH.md](../../REFRESH.md):
`tiny-catalogue-v1`, `sf0003-isolated-v1`, `sf0003-panel-v1`. No custom query
driver, parameter override, selective loader or digest-only validation.
All recorded runs use `PYTHONHASHSEED=0` to make the current unordered
default-company choice repeatable across processes.
Three fresh trials per timing recipe; two warmup and five measured rounds,
four workers, both backends run serially. The complete tiny fixture has one
correctness/overhead run, not three performance repetitions.

Apple M4 Mac mini `Mac16,10`, 10 logical CPUs, 17,179,869,184 bytes RAM,
macOS 26.1 build 25B78, Python 3.9.6. Native macOS server, no container.
No server profiling; the runner's unchanged one-second server RSS monitor
remains enabled with its 1-GiB ceiling and 120-second command timeout.

SF0.003 archive: 2,024,762 bytes, SHA-256
`c66014cd90ae71f98f78e4be925fffad8a8d88558ad6ced173477fa318a46a6a`.
It was verified and freshly extracted before timing. [DATA.md](../../DATA.md)
provides its pinned public download and layout.
Full projected input: 35,588 rows, 50 people, 3,660 messages, 492 likes.
Input fingerprint:
`8b2f87bb5f2243dc1a566b9f7152177493ebf6b6da3d6fca8ed6c0d144ac52d8`.
The delta retracts one row each from knows/member/likes, then restores them.

## Resource protection

An external Darwin safety monitor sampled the Python/server process group
every 100 ms, with a 2-GiB physical-footprint ceiling (4 GiB for compilation),
3-GiB host reserve, 1-GiB compressor ceiling, 0.25-GiB swap ceiling and
0.0625-GiB swap-growth ceiling. It aborts on abnormal pressure, insufficient
disk, deadline or monitor failure. It observes execution; it does not alter
requests, data, answers or timers. Whole-cycle numbers exclude reference work
as specified by the standard runner, not by this monitor.

The monitor is safety infrastructure, not a special benchmark driver. The
standard commands in REFRESH.md suffice to reproduce the query workload;
use suitable whole-process protection on the host running them. The stored
reports retain the standard runner's RSS samples and every checked event.
Matching `.guard.json` receipts record the monitor policy and implementation
hashes, command, completion, combined peak footprint, and host state before
and after each run. All seven runs passed; sampled peak footprint was at most
400.2 MiB, pressure remained normal, and swap use was zero. See
[CURRENT.md](../../CURRENT.md) for per-run outcomes.

## Files and interpretation

Reports are verbatim `snb-suite-2` JSON, gzip-compressed for size, not extracts.
Their absolute temporary paths are provenance, not required reproduction paths.
There are seven reports: three `isolated-N`, three `panel-N`, and `tiny`.
SHA-256 hashes of the stored reports, guard receipts and lockfile are in
[SHA256SUMS](SHA256SUMS); verify them from this directory with
`shasum -a 256 -c SHA256SUMS`. The manifest hashes compressed bytes, while
`readout.py` reports the hash of decompressed JSON bytes.
The current postprocessors accept compressed reports directly:

```sh
python3 interactive/server/bench/ldbc/readout.py --summary \
  interactive/server/bench/ldbc/measurements/current/isolated-1.json.gz \
  interactive/server/bench/ldbc/measurements/current/isolated-2.json.gz \
  interactive/server/bench/ldbc/measurements/current/isolated-3.json.gz
```

Use `panel-1/2/3.json.gz` for the concurrent panel, or omit `--summary` to
inspect raw cycle totals, setup, warmup and retirement. To compare engines
before/after a change, use `compare.py` and separate repeated trials; do not
pool different binaries in one summary.

All generated plans can be reproduced at the public workload revision with
`suite.py --queries all --emit`; hashes are recorded in each report. The
resolved lockfile and raw inputs' public archive provide the remaining build
and data identities.
