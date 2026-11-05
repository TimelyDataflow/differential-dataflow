# SNB Interactive v2 driver

`driver.py` runs the LDBC SNB Interactive v2 workload against the DDIR server, or
against DuckDB as a baseline. It reimplements the official Java driver
([ldbc_snb_interactive_v2_driver](https://github.com/ldbc/ldbc_snb_interactive_v2_driver))
from its source, because the Java driver cannot talk to the DDIR server.

Interactive v2 replays the last 3% of the simulated network's history (about 33
days at SF1, 1.75M inserts and deletes) one operation at a time, on a simulation
clock compressed by the time compression ratio (TCR). Complex reads (IC1–IC14,
17 variants) are scheduled at fixed intervals of that clock; each is followed by
a random walk of short reads (IS1–IS7) on ids taken from earlier results. A run
passes the schedule audit when at most 5% of each operation type starts more
than one second late. The score is the throughput at the lowest passing TCR.

## Inputs

All three are built from the BI SF1 CSV export (`composite-merged-fk`), the same
data the rest of this suite uses. The official v2 update streams and parameter
files live on SURF's repository, which could not be reached when this was
written.

1. **The initial graph** is read from `initial_snapshot/` in chunks (`stream.py`).
2. **Updates:** every insert and delete in `inserts/dynamic` and
   `deletes/dynamic` is one operation at its own timestamp, read one day at a
   time (`stream.py`). A person comes with its interests, studies and jobs; a
   forum, post or comment with its tags. A delete is its explicit target; the
   cascade is the system's to work out (below).
3. **Parameters:** `factors.py` rebuilds Datagen's raw tables and the paramgen
   factor tables from the CSV export. The official paramgen then runs on them
   unchanged, and `parameters.py` converts its output to one JSON file:
```sh
B=/path/to/bi-sf1-composite-merged-fk/graphs/csv/bi/composite-merged-fk
python3 interactive_v2/factors.py $B /tmp/v2-sf1          # duckdb
cp -R /tmp/v2-sf1/factors /tmp/v2-factors                 # paramgen rewrites people4Hops
cd ldbc_snb_interactive_v2_driver/paramgen && mkdir -p scratch && \
  TZ=UTC python3 paramgen.py --raw_parquet_dir /tmp/v2-sf1/raw --factor_tables_dir /tmp/v2-factors \
    --time_bucket_size_in_days 1 --threshold_values_path paramgen_window_values.json
python3 interactive_v2/parameters.py ../parameters /tmp/v2-sf1/parameters.json
```

The paramgen needs Python 3.10+, `duckdb==0.10.2`, `pandas` 2.x (3.x breaks its
group-by) and `networkit`. It must run with `TZ=UTC`, or the path parameters'
validity windows shift by the local offset. For SF below 1, use
`paramgen_window_values_test.json`, as the official scripts do.

## Running

```sh
python3 interactive_v2/driver.py --system ddir --server target/release/ddir_server \
  --data $B --parameters /tmp/v2-sf1/parameters.json \
  --tcr 0.008 --threads 16 --warmup 10000 --operations 60000 --output /tmp/v2-run
```

`--system duckdb` runs the baseline: the `baselines/sql/duckdb` schema and SQL,
updates through `baselines/sqlsystems.duckdb_apply`. It needs DuckDB 2.0,
Python 3.11+, and `--snapshot`, the projected initial graph that
`update_stream.py --prepare-only` writes, because its cascades are computed in
the driver (`snb.operations.Cascade`). `--cascade client` does the same for DDIR.

`--sf 0.1` runs SF0.1's data with SF1's query mix; SF0.1's own frequency file
sets every frequency to 1, a test setting.

Output: `results_log.csv` in the Java driver's format, and `results.json` with
the audit, throughput, and per-type count, late count and latency percentiles.

`--tick-log` (DDIR) writes what each tick held to `ticks.jsonl`, for `replay.py`
and `costs.py`. `--batch-ms M` (DDIR) holds each tick until its oldest operation
has waited M ms, so ticks are fuller; it needs enough `--threads` to fill them.

`--validate --operations N` runs the first N operations one at a time in a fixed
order, ignoring the clock, and writes every read's answer to `answers.jsonl`.
Two systems' files can be compared line by line; the first 3,000 operations
(2,473 reads) agree between DDIR and DuckDB.

## How the systems execute

One update thread submits each update as it falls due; `--threads` read workers
each run one complex read and then its walk, one read at a time, as the Java
driver's thread pool does.

- **DDIR:** one loop ticks the server. A tick applies every update submitted
  since the previous tick began, answers every read submitted in that time, and
  retracts the previous tick's reads. Every operation in a tick starts when the
  tick starts and ends when it ends.
- **DuckDB:** the update thread applies each group of due updates as one
  transaction, and each read worker runs its reads on its own connection, so
  reads run concurrently with each other and with updates.

**Delete cascades.** DDIR computes them in its graph program (`graph.py`): it
takes inserted rows and explicit delete targets, and derives each live relation
by the rules of `snb.updates.State` (a deleted person takes their messages,
Walls, Albums and edges, and their Groups lose their moderator; a deleted forum
takes its posts; a deleted message takes the replies below it). The rest of the
graph program is unchanged. Over the whole SF1 stream the live relations equal
the BI daily journal (checked at three days), and `test_cascade.py` checks the
rules on a fixture. The driver then holds no graph: 181 MB at SF1, against
8 GB when it computed cascades itself. The server holds 7.5 GB instead of 4.7 at
peak in the same setup, for arrangements of the inserted rows by the keys the
cascades look them up by.
DuckDB still gets exact deltas from the driver, computed inside the timed update.

## Results (SF1, Apple M4 with 4 performance and 6 efficiency cores, 16 GB)

**The time compression ratio (TCR)** is wall time per unit of simulated time.
The update stream covers 33 simulated days; at TCR 0.006 the driver replays it
167 times faster than it happened, asking for about 440 operations per second.
Lower is harder. The score is the throughput at the lowest TCR that passes the
audit. The spec's floor is 0.001 (about 2,600 operations per second at SF1).

10,000 warmup and 60,000 measured operations; DDIR with 4 timely workers (more
did not help), DuckDB 2.0 with its default 10 threads.

| | lowest passing TCR | throughput | first failing TCR |
| --- | ---: | ---: | ---: |
| DuckDB, 16 read threads | 0.006 | 434 ops/s | 0.005 |
| DDIR, 16 read threads, plans before IC9 and IC14 were rewritten | 0.008 | 330 ops/s | 0.007 |
| DDIR, 16 read threads | 0.007 | 377 ops/s | 0.006 |
| DDIR, 64 read threads, `--batch-ms 50` | 0.006 | 441 ops/s | 0.005 |
| DDIR, all complex reads one-off (`--oneoff`), 192 threads, `--batch-ms 50` | 0.0025 | 1,059 ops/s | 0.002 |

At 0.005 DDIR's server is saturated with standing plans: about 3.2 of the 4
workers' cores are busy. One-off complex reads (`oneoff/`, below) halve their
work; the 0.0025 run was 81% one-off complex reads, 12% short reads and 3%
updates. A one-off is installed while updates flow. Since #1024 and #1025 it
reads every import, including the arrangements its half joins read, as of the
tick it starts, so its answer is that tick's. The 0.0025 row was measured before
those changes; on master-next with them (7eb7da8d) it passes again, at 1,049
ops/s, and 0.002 still fails.

At SF0.1, with SF1's query mix (`--sf 0.1`), 80,000 operations:

| | lowest passing TCR | throughput | first failing TCR |
| --- | ---: | ---: | ---: |
| DuckDB, 16 read threads | 0.0001 | 2,521 ops/s | 0.00007 |
| DDIR, one-off complex reads, 512 threads, `--batch-ms 50` | 0.00007 | 3,586 ops/s | 0.00005 |

From SF0.1 to SF1, DuckDB's throughput falls 5.8 times and DDIR's 3.4 times.

**Where DDIR's time goes.** `driver.py --tick-log` records each tick, and
`replay.py` runs those ticks again back to back. For the first 8,000 ticks of a
TCR 0.008 run (26,000 operations), with 4 workers and the earlier plans:

- 71.1 s in all, matching the live run's server time.
- Reads take 68.4 s, about half of it retracting the previous tick's reads.
  Retractions already share a tick with the next reads; giving them ticks of
  their own costs 83.8 s.
- Updates take about 2 s: keeping the shared indexes and all 21 plans' standing
  state current is cheap.
- Every tick costs about 1 ms even when empty. Each tick advances every input
  of every program, so all 1,122 operators of the graph program and the 21
  plans are scheduled, about 0.6 µs each.
- The rewritten IC9 and IC14 bring the slice to 61.4 s. Merging every 16 ticks
  into one (`--merge 16`, what `--batch-ms` does live) brings it to 44.3 s.
- By workers: 117.7 s with 1, 78.1 s with 2, 61.4 s with 4. CPU grows from 116
  to 212 s. Work is spread evenly across workers (per tick, the busiest worker
  does 1.09 times the mean), except in ranking steps keyed by request, which
  is what the IC9 rewrite addresses.

**One-off complex reads.** `oneoff/split.py` splits each authored IC plan into
a standing program (its statements that do not depend on the request,
published) and a one-off program (the request and everything that depends on
it, importing those). `--oneoff ic1,...` makes each such read install its own
copy of the one-off program, answer, and drop it at the next tick instead of
retracting. Answered one at a time on the initial graph, all complex reads take
17.2 s with 4 workers this way against 40.7 s standing, and the answers agree.

**Work per read, against DuckDB.** `costs.py` answers every read of that slice
on its own, against the initial graph, on one core (earlier plans):

| | reads | DDIR answer + retract | DuckDB |
| --- | ---: | ---: | ---: |
| IS1–IS7 | 19,677 | 6.4 s | 94.8 s |
| IC1–IC12 | 1,775 | 75.4 s | 46.3 s |
| IC13, IC14 (paths) | 446 | 54.8 s | 144.1 s |

DDIR does far less work on short reads and paths; DuckDB's short reads are slow
because it has no point-lookup indexes. On the other complex reads DDIR does
1.6 times DuckDB's work, IC9 (6.5 times) and IC12 (2.3 times) most of all. DuckDB
passes lower TCRs because it runs its 16 connections' queries at once on 10
cores, while DDIR runs one tick at a time.

## Departures from the Java driver

- **Update stream.** Built from the BI CSV, it starts at the BI snapshot
  (2012-11-29 00:00) rather than 02:52:48, and it keeps two kinds of operation
  the official stream builder drops by accident: untagged posts and comments
  (an inner join on tags), and likes of comments created on an earlier day.
- **Driver bugs not copied.** The Java driver drops the first update, uses the
  parameter after the first valid one, and orders ties nondeterministically.
  Here no update is dropped, the first valid parameter is used, and ties follow
  operation kind order (inserts before deletes, updates before reads).
- **Random numbers.** The walk uses Python's `random.Random(42)`, not the Java
  driver's Well19937c streams, so individual coin tosses differ.
- **Phases.** Warmup and measurement are one continuous run: the first
  `--warmup` completed operations are not measured. The Java driver restarts its
  clock between phases. `--operations` counts executed operations, including
  short reads, rather than estimating them in advance.
- **Paramgen inputs.** Rebuilt factor tables, not Datagen's. people4Hops takes
  the same number of people in China as Datagen's seeded sample, chosen by md5
  order instead; and the raw graph lacks entities deleted before the BI
  snapshot was cut.
