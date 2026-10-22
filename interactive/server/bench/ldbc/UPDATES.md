# Maintained SNB reads over BI updates

`update_stream.py` maintains selected reads while replaying the BI dataset's
daily inserts and deletes. Each query runs in a separate server with one fixed
request. The Python evaluator checks the complete answer using the same logical
query and exactly the projected changes sent to DDIR. This checks lowering and
incremental maintenance; it is not independent LDBC conformance or a concurrent
41-query throughput benchmark. Query definitions are in [snb/queries.py](snb/queries.py).

Run from the repository root with Python 3.9+, `ps`, and a server binary:

```sh
CARGO_TARGET_DIR=/tmp/ldbc-target cargo build --release -p ddir-server
python3 interactive/server/bench/ldbc/test_updates.py
python3 interactive/server/bench/ldbc/test_update_stream.py \
  --server /tmp/ldbc-target/release/ddir_server --workers 1 \
  --output /tmp/ldbc-update-fixture
```

The fixture requires no downloaded data. It checks all 41 reads after every
epoch, including cascades through other authors, repeated explicit deletes,
ID reuse, deletion of bound people, empty relations, and restoration. CI runs
it at one/four workers and also exercises deferred checking, saved-answer reuse,
and automatic mismatch investigations in replay mode.

## Prepare the update stream once

Supply an existing SNB BI CSV `composite-merged-fk` export with
`initial_snapshot/`, `inserts/dynamic/`, and `deletes/dynamic/`. See
[SNB.md](SNB.md#data-and-parameters) for the projection and CSV dialect. The
runner reads existing files; it does not download or modify the dataset.

```sh
python3 interactive/server/bench/ldbc/update_stream.py \
  --data /path/to/graphs/csv/bi/composite-merged-fk \
  --prepare-only --output /tmp/ldbc-prepared
```

Preparation saves the initial projected graph and one checksummed delta file
per epoch in `/tmp/ldbc-prepared/journal/`. Share this journal to give another
investigator exactly the same input. The manifest records source partitions,
their hashes, explicit/absent delete targets, and projected row counts. Reuse
checks the snapshot, delta files, and adapter source hashes.

Each epoch is one update class on one day. Within a day, inserts run in this
order: person, forum, post, comment, post-like, comment-like, friendship, and
membership; deletes follow in the same order. The next day follows. This
exposes states between classes rather than applying a whole day atomically.
Both `.csv` and `.csv.gz` partitions work; a plain copy takes precedence.

Delete files specify explicit targets. The adapter additionally removes
incident edges, deleted forums' posts, authored messages, and recursive reply
descendants as required. Deleting a person removes their Walls/Albums and
clears their Groups' moderator. Already-cascaded targets are not retracted
again. The journal supplies the same exact delta to the server and reference.

`--classes insert-person insert-forum`, `--first-day`, and `--last-day` select
raw batches during preparation only; omitted classes are never applied.
`--batch-limit N` selects a prefix during preparation or replay. To shorten a
reproducer, keep its preceding epochs: selecting only a slow class changes
the graph and its cascades. Every output directory must be empty or new.

## Run and check one query

```sh
python3 interactive/server/bench/ldbc/update_stream.py \
  --prepared /tmp/ldbc-prepared/journal \
  --server /tmp/ldbc-target/release/ddir_server --queries ic3 --workers 1 \
  --check-mode replay --check-every 128 --no-check-first-class \
  --output /tmp/ldbc-ic3
python3 interactive/server/bench/ldbc/summarize_updates.py /tmp/ldbc-ic3 \
  --query ic3
```

The default `--check-mode strict` recomputes Python after every epoch.
For real datasets, `replay` separates update timing from expensive checking:
it feeds every epoch, captures selected answers, retires the request and checks
the empty answer, closes the server, then replays the journal in Python to
validate the captures. Initial/final states are always captured. Periodic
checks use `--check-every`; the default also checks the first epoch of every
class. `--check-every 0 --no-check-first-class` checks just initial/final states.
Passing checkpoints do not certify intervening answers.

`--queries` accepts individual names and `all`, `is`, `ic`, or `bi`. Defaults
are data-derived smoke bindings, not the official parameter distribution.
`--parameters file.json` takes one override object per query, for example
`{"ic3": {"pid": 910}}`; actual request rows are recorded in each result.

To test a rewritten physical plan, add `--plan /path/to/candidate.ddp` with
exactly one selected query. The usual Python logical query remains the oracle.
The candidate must keep that query's input/export interface; use the saved
`ic3.ddp` as a starting point. The harness copies and hashes the candidate.

An answer mismatch saves actual/expected answers and automatically retries
the suspect interval with strict checking, retaining the complete preceding
history and the same candidate plan. A passing retry does not erase the
original mismatch. Timeouts and resource failures have separate classifications.

For repeated runs of the same plan, journal, binding, and reference code,
`--reference-from /tmp/ldbc-ic3` reuses previously validated captures, including
across worker counts. It still compares every new answer in full. Missing or
incompatible checks recompute Python. Omit the flag for fresh oracle evaluation.

## IC3 investigation recipes

IC3 finds friends within two hops who live outside two specified countries,
counts their messages in each country during a date window, requires positive
counts in both, and ranks the top twenty people by combined count. Committed
bindings pin the requests from the initial investigation:

| Dataset | Parameter file | Requesting person |
| --- | --- | ---: |
| SF0.1 | [ic3-sf01.json](bindings/ic3-sf01.json) | 910 |
| SF1 | [ic3-sf1.json](bindings/ic3-sf1.json) | 2199023264850 |

Both use Azerbaijan/Bosnia_and_Herzegovina and a 1,500-day window starting
2010-01-01. The window covers the 2012 data. Prepare a journal for the matching
dataset, then add the corresponding file to the run command above:

```sh
# SF0.1: start with a ten-epoch prefix and fresh initial/final Python checks.
python3 interactive/server/bench/ldbc/update_stream.py \
  --prepared /tmp/ldbc-prepared/journal \
  --server /tmp/ldbc-target/release/ddir_server --queries ic3 --workers 1 \
  --parameters interactive/server/bench/ldbc/bindings/ic3-sf01.json \
  --batch-limit 10 --check-mode replay --check-every 0 --no-check-first-class \
  --output /tmp/ldbc-ic3-prefix
```

For SF1, prepare from the SF1 export, use `bindings/ic3-sf1.json`, and omit
`--batch-limit` to exercise the complete stream and request retirement.
At server revision `b137520a`, this request completed all 521 update epochs,
then exceeded the 6-GiB sampled server limit during retirement, before deferred
Python checking. Those captured SF1 answers were not validated. This is a
historical investigation lead, not a failure assertion for newer binaries.

The current query projects unused residence, message and country fields before
the large joins, reducing message-expanded rows from 46 fields to 17. Integer
counts and sums use differential weights rather than collected lists. Different
messages that project to equal rows retain their multiplicities. The memory
observations in [MEMORY.md](MEMORY.md) separate query-active storage from later
transient peaks; a process peak does not identify which operator owns the bytes.

## Query plans and reference memory

Groups containing only integer sums and counts use native `count` over keyed
unit values. Sums multiply each input diff by the integer contribution through
`join_function`; the zero timestamp leaves input time unchanged. A separate
presence relation preserves zero-sum results for nonempty groups. When the last
input disappears, the group disappears. Other aggregate combinations retain the
existing lowering. The Python grouping evaluator remains the logical oracle.

The query definitions project away unused fields before large joins. IC12
restricts tag classes before expanding ancestry, BI15 restricts scored
interactions to friendship edges, and BI17 applies forum membership before
joining initiators to discussions. These rewrites preserve request identity,
output shapes and bag multiplicities. They do not constitute a general optimizer
pass or claim to resolve every memory limit.

Private Python reference evaluations release intermediate row lists after their
last consumer. Shared children remain available until all parents consume them;
an explicitly supplied cache retains the existing multi-export behavior. This
changes checker memory use, without changing generated DDIR.

## Bounded ranking plans

The ordinary compiler uses the Corgi reducer in [snb/topk.py](snb/topk.py) for
every finite rank limit. It keeps the complete original sort key and row, caps
output multiplicities at the limit, and assigns ranks only at the last stage.
Inputs remain maintained by DD so a deletion can expose a previously unselected
candidate. Unbounded rankings and nested collection outputs still use `collect`.

IC2 uses a flat top-twenty reduction. IC5 and IC9 use nested 1024/32/1 hash
buckets; each count divides its predecessor. IC5 hashes its stable forum ID
(the second sort field), keeping a forum in the same bucket when its score
changes. IC9 hashes the full sort value. Other finite rankings use one stage.
The normal suite, update runner and memory profiler all use these defaults:

```sh
python3 interactive/server/bench/ldbc/update_stream.py \
  --prepared /tmp/ldbc-prepared/journal --server /tmp/ldbc-target/release/ddir_server \
  --queries ic9 --check-mode replay --check-every 0 --no-check-first-class \
  --output /tmp/ic9-bounded
```

`bounded_topk.py` overrides bucket/hash choices for IC2/IC5/IC9 experiments,
using the same compiler and reducer. For example, try a flat IC9 plan:

```sh
python3 interactive/server/bench/ldbc/bounded_topk.py --query ic9 \
  --buckets 1 --hash-key value --output /tmp/ic9-flat.ddp
python3 interactive/server/bench/ldbc/update_stream.py \
  --prepared /tmp/ldbc-prepared/journal --server /tmp/ldbc-target/release/ddir_server \
  --queries ic9 --plan /tmp/ic9-flat.ddp --check-mode replay \
  --check-every 0 --no-check-first-class --output /tmp/ic9-flat
```

These overrides change physical ranking; the ordinary logical query remains the
Python reference. `test_topk.py` checks positive/negative diffs, duplicates,
retractions, ties, empty groups, limits 0/1/10/20/100, varying sort-key arities
and composite groups. It checks both the defaults and experimental IC2/IC5/IC9
plans through all 49 update-fixture states at one/four workers.
`test_aggregates.py` checks native integer aggregation independently.

## Additional process memory

`profile_memory.py` loads and settles the shared inputs before installing a query.
A separate fresh server replays the same journal without a query as the input-only
control. Queries also run in fresh servers, with one fixed request each. Compare
query-active RSS with the control at the corresponding initial/final state; keep
installation and retirement peaks separate. The three empty settling ticks after
retirement can expose a later compaction peak that an immediately exiting server
would miss.

```sh
python3 interactive/server/bench/ldbc/profile_memory.py \
  --prepared /tmp/ldbc-prepared/journal --server /tmp/ldbc-target/release/ddir_server \
  --parameters interactive/server/bench/ldbc/bindings/sf1-memory.json \
  --output /tmp/ldbc-memory
python3 interactive/server/bench/ldbc/summarize_memory.py /tmp/ldbc-memory
```

The parameter file pins the observed SF1 smoke requests, not official LDBC
parameter-distribution samples. Profiling captures initial/final answers and
checks that retirement empties the output; it **does not recompute Python**.
Optional `--expected DIR` compares with saved `NAME.json` files containing
`initial`, `final` and `validated` fields. This comparison is recorded separately
from execution and does not replace `update_stream.py` correctness checking.

Memory is sampled every 100 ms and at phase boundaries. On macOS, both resident
size and physical footprint are recorded; other platforms record RSS through
`ps`. Per-command and whole-job limits default to 120/300 seconds, server RSS to
6 GiB, combined Python/server RSS or physical footprint to 10 GiB. Failures stay
in the reports and censor peak measurements. `memory.csv` and `memory.json` rank
query-active additional RSS and retain the overall peak, status and provenance.
The differences include allocator retention and are not query-owned live bytes;
small negative differences are preserved.

## Results and handoff

Keep the journal, complete run directory, source revision, and server build
revision/toolchain. Reports record commands, request rows, worker count, limits,
host, source/plan/binary hashes, and each update's latency. `coverage.csv`
distinguishes completed updates, captured answers awaiting validation, and
passed checks. A captured answer is not a pass. `summarize_updates.py` reads
saved reports without rerunning the workload or reference.

Replay wall time includes decoding, command preparation, encoding, transport,
acknowledgements, and checkpoint capture. Update wire time is the client's
observed wait, not server CPU time. Python reference timings are separate.
Projected changes count every inserted/retracted tuple, including cascades;
they are not transaction counts. The standing request input advances with
each `tick`, allowing logical/physical compaction of completed engine history.

Defaults are 120-second command/reference deadlines, a sampled 6-GiB server
limit, and a sampled 10-GiB combined Python/server limit (also physical footprint
on macOS). Sampling can miss short peaks. Resource failures remain in the
reports; they do not establish an answer mismatch.
