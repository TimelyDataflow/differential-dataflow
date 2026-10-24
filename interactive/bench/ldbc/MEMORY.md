# Profiling the memory a query adds

`profile_memory.py` measures how much memory each query adds to a server that already holds
the shared SF1 inputs, over the full BI update stream. Requests are pinned in
[bindings/sf1-memory.json](bindings/sf1-memory.json); these are smoke requests, not draws from
the official parameter distribution. Results are single observations for choosing what to
investigate, not timing targets or an official LDBC benchmark. They aren't committed: rerun
the profiler on the binary and machine in question.

## What it measures

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

Limits are 6 GiB server RSS, 10 GiB combined Python/server RSS or physical
footprint, 120 seconds per command, and 300 seconds per case. Python answers
are not recomputed during profiling. Initial and final captures are compared
with saved, previously Python-validated answers where available. Execution,
answer validation and resource failure remain separate report fields.

## Reproduction and correctness

Follow [UPDATES.md](UPDATES.md) to prepare the official BI composite-merged-fk
SF1 initial snapshot and insert/delete batches. The profiler verifies journal
and adapter hashes before starting. Then run:

```sh
python3 interactive/bench/ldbc/profile_memory.py \
  --prepared /tmp/ldbc-prepared/journal --server /tmp/ldbc-target/release/ddir_server \
  --parameters interactive/bench/ldbc/bindings/sf1-memory.json \
  --output /tmp/ldbc-memory
```

`summarize_memory.py DIR` regenerates the summary CSV and JSON from the raw reports in an
output directory, including after copying the directory to another host.

For correctness, CI checks the full small witness catalogue at one/four workers,
weighted integer aggregates against direct arithmetic, and bounded ranking
against the Python reference through all 49 update-fixture states. It also checks
late query installation, saved-answer matching and retirement on a small fixture.
The SF1 profile's saved-answer comparison does not replace those tests or the
full replay checker.
