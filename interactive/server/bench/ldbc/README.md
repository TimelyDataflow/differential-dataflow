# LDBC-derived server workload

A small performance/correctness baseline for DDIR, through the real
`ddir_server` TCP interface. No server extensions, external database, generator,
or Python packages are required. Python 3.9+ and `ps` are required by the runner.

This is **not an official LDBC benchmark result**. It implements a four-query
panel with a controlled update/request schedule, not the LDBC driver's arrival
distribution, update stream, conformance checks, or throughput scoring.

## Run

From the repository root:

```sh
cargo build --release -p ddir-server
python3 interactive/server/bench/ldbc/run.py --server target/release/ddir_server
```

The default uses the 50-row hand-authored fixture, both backends (separate
processes), one worker, one warmup round and five measured rounds. It prints
phase medians and the location of a JSON report and server logs. This fixture
is for correctness and overhead, not evidence of columnar throughput.

Use an existing SNB BI **CSV composite-merged-fk** initial snapshot for timing:

```sh
python3 interactive/server/bench/ldbc/run.py --server target/release/ddir_server \
  --snapshot /path/to/graphs/csv/bi/composite-merged-fk/initial_snapshot \
  --backend corgi --workers 4 --rounds 10 --batch-size 8 --changes 32 \
  --output /tmp/ldbc-baseline
```

`--output` must be a new directory. The adapter reads existing files only; it
does not acquire data or modify a database. It accepts the same layout at small
and large scale factors. Missing table directories fail rather than silently
becoming empty tables. Only seven relations/needed columns are loaded; the
reported projected row counts and data hash define the actual input.

`--queries ic6` isolates one query over the same common graph substrate;
`--queries bi5 bi11` measures BI maintenance without interactive consumers.
Compare those controls with the default concurrent panel. `--batch-size 1`
measures single bindings **per interactive query**; in the default panel that
still means two simultaneous requests per tick. Increase batch size separately
from data size to study dispatch amortization. Defaults enforce a 120-second
command deadline and a sampled 6-GiB **server** RSS ceiling. The latter is not
a hard memory limit and excludes the Python loader/reference process.

## Queries and lifecycle

| Plan | Work exercised | Binding lifetime |
| --- | --- | --- |
| IS3 | Full friend list, names, descending creation-date order | One request batch |
| IC6 | Distinct one/two-hop authors, post/tag joins, grouped counts, top ten | One request batch |
| BI5 | Tagged posts/comments, immediate replies and likes, score aggregation, top 100 | Whole run |
| BI11 | Country/date-filtered unordered friendship triangles, including a zero result | Whole run |

The readable `.ddp` files specify the plans; there is no hidden query compiler.
The query semantics follow the SNB read definitions. In particular BI5 counts
comments as well as posts, and all three BI11 friendship dates must fall in the
inclusive interval. Reference implementations:
[BI5](https://github.com/ldbc/ldbc_snb_bi/blob/47dd38b40844ecdb0e42e5a610c369535304786d/umbra/queries/bi-5.sql),
[BI11](https://github.com/ldbc/ldbc_snb_bi/blob/47dd38b40844ecdb0e42e5a610c369535304786d/umbra/queries/bi-11.sql).

All selected plans are installed once and import a shared graph program.
Their common substrate symmetrizes friendships, attaches tag names, and maps
people to countries; its load and maintenance costs are included. Even isolated
controls retain this fixed substrate. This uses existing named-import semantics;
it does not introduce native Corgi trace sharing or a new prepared-query feature.

Each round runs requests on the initial graph, retracts those bindings, removes
a deterministic bounded set of friendship/tag/like edges, runs new requests on
the changed graph, retracts them, and restores the exact removed edges. The
dataset oscillates between two states: **bounded churn, not growth or a
time-bounded saturation run**. No node deletion/cascade semantics are assumed.
Every `feed` group ends with `tick`; acknowledgement of that tick, not of feed
admission, establishes completion.

BI parameters remain bound throughout; their maintenance is paid even when
unread. Interactive bindings are absent during graph mutations. Their parameter
bank spans degree quantiles and an absent person. IC6 tags are chosen from
reachable multi-tag posts when possible, to exercise joins and ranking rather
than mostly empty lookups. It is a data-derived bank, not the official parameter
distribution; the runner prints its nonempty-answer coverage.
Bindings vary by round and state; request IDs are reused after retraction.
They are not maintained answers to future requests. Batch size controls how
many bindings coexist. The report records the exact bank, bindings and deltas.
This is a closed-loop single-client workload, not a maximum-QPS claim.

An independent Python graph traversal/counting oracle checks every returned
row, order, rank and multiplicity, including empty results after release and BI
results after mutation/restoration. Oracle construction/validation is untimed.
The tiny fixture also has hand-counted reference answers. Larger snapshots use
the same oracle and plans, not a row-count-only check. This is still not a full
LDBC conformance suite.

## Read the measurements

- Setup records plan install/parse, input admission and initial `tick` separately.
- `bind`: feed all interactive bindings and wait for `tick`.
- `read:*`: `peek` and decode the full answer. No top-k work is done by the client.
- `batch_answers`: sum of bind and read phases, excluding reference checks;
  this is the complete batch-answer latency, **not the bind time alone**.
- `release`: retract all bindings and wait for `tick`. Do not omit this from
  a sustained-work accounting. Subsequent `empty:*` reads check cleanup.
- `update` / `restore`: graph feeds and `tick`, including maintenance of both
  standing BI results and unbound interactive plans. `maintained:*` reads are
  separate; subtracting them does not remove the maintenance cost.

Events split command preparation, encoding, wire wait/receipt and Value decoding.
Wire time includes server work and TCP overhead; it is not a kernel profile.
Medians/min/max exclude warmup; raw samples, result bytes, reference answers,
sampled server RSS, machine metadata, binary/source/data hashes, repository
revision/status and the available Cargo lockfile accompany the report. Keep the
same data, plans, workers and schedule when comparing binaries. Use release
builds and an otherwise quiet machine. The repository's pinned dependencies are
used; no patch to an external Corgi checkout is required.

After running a candidate binary with the same arguments, compare the reports:

```sh
python3 interactive/server/bench/ldbc/compare.py \
  /tmp/ldbc-baseline/report.json /tmp/ldbc-candidate/report.json
```

The comparison rejects failed runs, changed data/parameters/schedules/answers,
different worker counts or environments, and changed measurement code. Changed
DDP plans are allowed and called out: plan improvements are benchmark subjects
too. Ratios above one mean faster. Keep setup and release costs in view, and
repeat runs before treating small differences as improvements.

## CI and follow-ups

CI runs only the fixture, both backends, at one and four workers, with the whole
parameter bank. It asserts answers, not timing thresholds, and uses this runner
rather than a separate simulation. The equivalent local command is:

```sh
python3 interactive/server/bench/ldbc/run.py --server target/debug/ddir_server \
  --workers 4 --rounds 1 --warmup 0 --batch-size 9 --changes 1
```

First optimization controls: IC6's expand-before-tag-filter plan, BI5's
`collect`/`fold` sums, repeated scalar compilation/dispatch, and shared-import
row/column conversion. Keep their patches independent and compare against this
baseline before stacking them. Add the remaining queries in small panels,
especially recursive, optional/SUM, temporal and large-output cases. This panel
does not yet measure ad-hoc re-planning, parameterized BI, simultaneous bound-IC
updates, or the official update stream. Empty strings are rejected by this narrow
adapter: general nullable/message-content queries need typed server admission,
not padding the data to evade the current shape-inference limitation.
