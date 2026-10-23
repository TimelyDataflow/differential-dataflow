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

1. **The initial graph:** `update_stream.py --prepare-only` writes
   `journal/initial.pickle.gz`, the projected BI initial snapshot.
2. **Updates:** `snb/operations.py` reads every insert and delete in
   `inserts/dynamic` and `deletes/dynamic` as one operation at its own timestamp.
   A person comes with its interests, studies and jobs; a forum, post or comment
   with its tags. Cascades are resolved when the operation runs, by
   `operations.Cascade`. Applied in order, the operations reproduce the BI daily
   journal exactly at the end of every one of the 33 days.
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
validity windows shift by the local offset.

## Running

```sh
python3 interactive_v2/driver.py --system ddir --server target/release/ddir_server \
  --data $B --snapshot journal/initial.pickle.gz --parameters /tmp/v2-sf1/parameters.json \
  --tcr 0.008 --threads 16 --warmup 10000 --operations 60000 --output /tmp/v2-run
```

`--system duckdb` runs the baseline: the `baselines/sql/duckdb` schema and SQL,
updates through `baselines/sqlsystems.duckdb_apply`. It needs DuckDB 2.0 and
Python 3.11+.

Output: `results_log.csv` in the Java driver's format, and `results.json` with
the audit, throughput, and per-type count, late count and latency percentiles.

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

Both receive the same deltas: the cascades are computed in the driver process
for both, inside the timed update.

## First results (SF1, Apple M4, 16 GB)

10,000 warmup and 60,000 measured operations, 16 read threads; DDIR with 4
timely workers (8 did not help), DuckDB 2.0 with its default threads.

| | lowest passing TCR | throughput | first failing TCR |
| --- | ---: | ---: | ---: |
| DDIR | 0.008 | 330 ops/s | 0.007 (about 10% of each complex read late) |
| DuckDB | 0.006 | 434 ops/s | 0.005 |

At its passing TCR, DDIR's median latency is lower than DuckDB's for 13 of the
17 complex-read variants (IC14 100 ms against 1.3 s), but its p99 is 300–600 ms
for every read type, because every operation in a tick waits for the slowest.
The cost is in the complex reads, and retracting one costs as much as answering
it (install / retract in an otherwise idle server, ms): IC9 120 / 112,
IC14b 57 / 81, IC13a 17 / 19, IC1 12 / 10. A retraction runs in the next tick,
so a tick holding only a short read can take hundreds of milliseconds. An empty
tick costs 1 ms, and a short read adds well under one.

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
