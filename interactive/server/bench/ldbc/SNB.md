# Complete SNB read suite

This is a runnable DDIR benchmark catalogue, not an official LDBC score or a
conformance claim. It contains every read in the pinned specification:
[IS1–7](https://github.com/ldbc/ldbc_snb_docs/blob/b2269610f433da72e7c97041f01680aae369a903/interactive-short-reads.tex),
[IC1–14 v2](https://github.com/ldbc/ldbc_snb_docs/blob/b2269610f433da72e7c97041f01680aae369a903/interactive-v2-complex-reads.tex),
and [BI1–20](https://github.com/ldbc/ldbc_snb_docs/blob/b2269610f433da72e7c97041f01680aae369a903/bi-reads.tex).
There are **41 reads**, not 25 per family. No selected query is silently skipped.
IC14 is the v2 cheapest-interaction-path query, not v1's all-shortest-path query.

Follow-up work is tracked in [GAPS.md](GAPS.md), with a caveated
[M4 measurement record](MEASUREMENTS.md) and [pinned generated-data recipes](DATA.md).

## Run

From the repository root, with Python 3.9+ and `ps`:

```sh
cargo build --release -p ddir-server
python3 interactive/server/bench/ldbc/suite.py --server target/release/ddir_server
```

The default starts a private real TCP server for each backend, installs all 41
queries concurrently, loads the same shared graph, and checks answers throughout
a bounded update/request cycle. It uses a hand-authored 199-row witness graph
(8 people, 35 messages), one worker, one warmup round, and three measured rounds.
No generator, external database, downloaded data, Python packages, or custom
Rust benchmark executable is needed.

The query definitions are in [snb/queries.py](snb/queries.py), with named fields
and relational steps. [snb/rel.py](snb/rel.py) is a small authoring notation that
emits ordinary DDP. It is not another engine backend: the server does all joins,
recursion, aggregation, nested collection construction, and ranking. Each run
saves the emitted `.ddp` plans and a catalogue of parameters/output fields. To
inspect them without starting a server:

```sh
python3 interactive/server/bench/ldbc/suite.py --list
python3 interactive/server/bench/ldbc/suite.py --emit --output /tmp/snb-plans
```

Use a subset or a different lifecycle without editing query definitions:

```sh
python3 interactive/server/bench/ldbc/suite.py --server target/release/ddir_server \
  --queries ic1 ic6 ic14 bi1 bi15 --workers 4 --rounds 10
python3 interactive/server/bench/ldbc/suite.py --server target/release/ddir_server \
  --queries bi --mode maintained --isolated
```

`--queries` accepts `all`, `is`, `ic`, `bi`, and individual names. `--isolated`
starts a fresh server for each query, retaining the same raw graph substrate;
this is an attribution control, not concurrent throughput. `--output` must be
empty or new. Server logs and a JSON report survive failures.

## Data and parameters

To use existing generated data:

```sh
python3 interactive/server/bench/ldbc/suite.py --server target/release/ddir_server \
  --snapshot /path/to/graphs/csv/bi/composite-merged-fk/initial_snapshot \
  --backend corgi --workers 4 --batch-size 3 --rounds 5
```

The adapter reads all 18 required tables in SNB BI CSV `composite-merged-fk`
layout, including `.csv.gz` partitions, through the same
[snapshot dialect reader](README.md#run) as the small panel
(double-quoted fields, backslash escaping within quotes; not raw-generator CSV).
A plain partition takes precedence over its `.csv.gz` copy. It projects 17 relations: people,
posts/comments, forums, friendships, memberships, tags/classes, places,
organisations, interests, message/forum tags, likes, education, employment,
email, and languages. Missing files fail explicitly. All reads have their
required source fields; BI data is used for Interactive semantics too, not the
official Interactive generator/driver workload. No files are downloaded or
modified. Calendar fields are derived in UTC; query results are not precomputed
by the adapter.

Strings are UTF-8 byte lists, including genuinely empty strings. The current
leaves are 64-bit integers, not byte-packed or dictionary-encoded strings.
Post and Comment share a tuple with a kind field; they are **not** an enum-layout
optimization experiment. Source shape ascriptions supply the element encoding
of empty lists and inactive sum lanes. They are execution-time contracts, not
transactional validation at `feed` admission. A mismatched row can panic a
dataflow worker and take down the shared server on either backend, disconnecting
other clients. There is no per-program failure isolation; use trusted programs
and shape-correct data. The benchmark starts private server processes.

Default bindings are deterministic smoke parameters selected from input facts,
not LDBC's parameter generator. The fixture has nonempty witnesses for all 41
reads; generated datasets may legitimately give empty answers. Actual values
(people, messages, tags, dates) vary between interactive request batches, not
just request IDs. Equal bindings under distinct IDs are also exercised at the
default batch size. The full bank and exact bindings are recorded.
The tiny IC3 bank includes a requester outside the qualifying person's two-hop
neighborhood, so varying bindings changes the answer, not just request IDs.

Override bindings with `--parameters /path/to/bindings.json`. Its format is a
query-name object containing a nonempty list of parameter objects; omitted
fields use the smoke defaults. Integer dates are milliseconds since the epoch:

```json
{"ic6": [{"pid": 1, "tagname": "Topic"}, {"pid": 2, "tagname": "Other"}],
 "bi12": [{"language": ["en", "fr"]}]}
```

BI12's language list is represented by rows sharing a request ID. An empty set
uses a disabled row, so the request still exists and returns the zero-count
distribution. BI13's calendar-month parameter is derived from its end date.
In maintained mode the first binding for each query remains standing; later
bank entries are for transient request batches, not simultaneous BI bindings.

## Lifecycle and accounting

| Mode | BI bindings | IS/IC bindings | Graph changes |
| --- | --- | --- | --- |
| `mixed` (default) | Standing until final retirement | Bound per batch, then retracted | Paid with BI live and interactive parameter inputs empty |
| `maintained` | Standing | Standing | All bound answers maintained and checked |

All query programs are installed once. Every query imports the shared raw
graph program and has a separate request input. Parameter-independent parts
of an installed query remain as dataflows even when its request input is empty;
their maintenance is included. Named imports currently cross the server's row
boundary: this is not native Corgi arrangement sharing. Derived relations are
not factored across different query programs.

Each round visits the initial graph, changes it, and restores it. The witness
removes a friendship and leaf post (including tags) and renames a person. CSV
mode retracts a bounded number of friendship, membership, and like rows, then
restores them. This is **churn between two fixed snapshots**, not growing data,
an official update stream, or a time-bounded saturation run. It does not claim
general entity-deletion/cascade semantics. Interactive request IDs are reused
after retraction with new values; they do not name pre-maintained future answers.

Each feed group is completed by `tick`; the feed acknowledgement alone means
admission. An empty `peek` after that tick is a completed empty result. The
report separates install/parse, graph admission, initial settlement, bind/tick,
answer retrieval, release/tick, graph update/tick, and restoration. `read:*`
includes full-result wire transfer and client decoding, not just a handle to an
answer. Ranking/top-k work stays in DDIR. `batch_answers` sums binding and answer
retrieval, excluding reference checks between them.
Standing-result reads are `maintained:*`; cleanup checks are `empty:*`, so empty
reads do not dilute the latency summary for bound requests.

Per-event command preparation, encoding, wire wait/receipt, and decoding are
recorded and summarized separately as `prepare_ms`, `encode_ms`, `wire_ms`,
`decode_ms` and `client_ms`. Comparisons default to client-observed request/response
time (`wire_ms`), which includes server work, transport and Python protocol
handling; it is not server CPU time. `client_ms` includes command preparation,
encoding and answer decoding, but excludes parameter selection and oracle work.
`batch_answers` sums each component over bind/read phases separately.
The snapshot deltas are computed once during setup (`delta_prepare_ms`) and
reused for every update/restore; these phases do work proportional to the delta,
not repeated full-graph differences in the client. Version `snb-suite-2` reports
require a fresh baseline; the comparison tool rejects the old summary format.
Shared update/bind time cannot be attributed to one query by inspecting
its later `peek`. Use `--isolated` and selected concurrent panels for attribution.
Parsing/planning is setup cost here: **this is not an ad-hoc re-planning test**.
Do not omit release or standing maintenance when assessing sustained work.

The JSON report contains raw timings and non-warmup medians, result hashes/row
counts, source/data/binary/plan hashes, exact deltas, machine/revision metadata,
sampled server RSS, and a copy/hash of the available Cargo lockfile. Compare
matching reports using the same `compare.py` as the four-query panel:

```sh
python3 interactive/server/bench/ldbc/compare.py /tmp/snb-before/report.json /tmp/snb-after/report.json
```

Changed logical/DDP plans are permitted but called out; returned answers and
schedules must still match. Changed measurement/data-adapter code requires a
fresh baseline. Four-query and full-suite reports are not interchangeable.

## Validation, limits, and optimization controls

Every returned row, rank, multiplicity, and empty result is checked against
fresh Python evaluation of the **same logical plan**, outside the timers. This
checks DDP lowering and incremental execution; it is not an independent query
specification oracle. All 41 have nonempty default witnesses; **14 queries**
also have independent hand-checked expectations in [test_snb.py](test_snb.py),
not necessarily a complete oracle for each query. IC3 checks two-hop requester
sensitivity and loss of a required-country message. The other 27 have no
independent semantic oracle here. The older four-query panel
retains independent traversal/counting oracles. Official conformance validation
is still separate work.

Floating results use explicit IEEE f64 newtypes with total ordering, not the
specification's Float32 API representation. They are preserved as tagged values
in reports. Shortest-path maintenance uses a hop-indexed bound of `|V|-1` so
deletions terminate; this can be expensive on large connected graphs. General
aggregates currently use `collect`/`fold`; nested and ranked outputs are fully
materialized. These are deliberate visible baseline costs, not tuned plans.

The complete suite is verified on the 199-row fixture and the generated
SF0.003 snapshot (35,588 projected rows) on both backends. CI runs only the
fixture: mixed at one/four workers and maintained at four workers. It asserts
answers, not timing thresholds, and does not fetch data. The local equivalent is:

```sh
python3 interactive/server/bench/ldbc/test_snb.py
python3 interactive/server/bench/ldbc/suite.py --server target/debug/ddir_server \
  --workers 4 --rounds 1 --warmup 0
python3 interactive/server/bench/ldbc/suite.py --server target/debug/ddir_server \
  --mode maintained --workers 4 --rounds 1 --warmup 0
```

Start scaling with selected queries. Neither the Python snapshot evaluator nor
the hop-indexed path plans are intended to make all-query SF1 runs cheap. The
default 120-second command deadline and sampled 6-GiB server RSS ceiling exclude
Python and are not a hard OS memory limit. Small debug runs establish correctness,
not competitive performance; use release builds and a quiet machine for timing.
RSS can fall under compression or swapping while memory pressure grows. For
larger runs, apply external limits or monitoring to the whole process group,
including the Python loader/reference process, and retain host headroom. The
built-in RSS ceiling alone cannot make an all-query SF1 run safe on a
memory-constrained machine.

Useful initial attribution groups (not official choke-point classifications):
IC1/IC12 for optional/nested output; IC2/IC6 for selective joins and ranking;
BI1/BI5/BI12/BI13 for aggregation; BI11/BI18 for graph motifs; IS2/IS6/BI9 for
thread recursion; IC13/IC14/BI10/BI15/BI19/BI20 for distance/path maintenance;
BI17 for broad temporal joins. Keep optimizer and kernel changes separate from
these definitions so each improvement can be measured against an unchanged
workload. In particular, the IC6 baseline has not been hand-rewritten to push
the tag filter ahead of friendship/message expansion.
BI19 retains an unbounded rank whose result is discarded by its next projection;
eliminating that unused ranking operation is an optimizer opportunity, not a
query-semantics correction. BI13's current message-count interval includes the
end date while profiles use a strict upper bound. The specification's endpoint
wording remains an interpretation question; this harness correction does not
change that policy or its inclusive calendar-month count.
