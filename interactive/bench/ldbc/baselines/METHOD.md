# SNB reads on DuckDB and Umbra: method

This directory holds two SQL baselines for the 41 SNB reads in `../snb/queries.py`, and
the method used to compare them with DDIR on one machine. It is written so that the same study can be repeated on
another system, such as Materialize, and the comparison kept honest.

## What is compared

- **The queries are the suite's.** Each SQL query in `sql/<system>/<query>.sql` (two-digit numbers: `bi01.sql`) was
  written from the logical definition in `snb/queries.py`, not from the official LDBC
  SQL, and follows it where the two differ: IC14 is v2's cheapest path, with ties broken
  by the smallest list of people; BI2 lists only tags with a message in its window; BI13
  counts a message on the end date. The Umbra SQL is a port of the DuckDB SQL.
- **The data is the same.** The BI `composite-merged-fk` export, projected to the suite's
  17 relations. DuckDB loads the CSVs itself (`sqlsystems.duckdb_load`); its load was
  checked equal to the suite's Python projection (`snb/data.py`), relation by relation,
  at SF0.1 and SF1. Umbra copies CSVs that DuckDB writes from that projection.
- **The updates are the same.** `update_stream.py --prepare-only` turns the BI inserts
  and deletes into 521 epochs at SF1 (one update class on one day), with LDBC's delete
  cascades. All systems apply exactly those projected changes.
- **Precomputed tables.** The SQL builds two derived tables after loading, as LDBC's own
  SQL implementations do: `thread` (each message's root post, forum, author and language)
  and `ancestry` (the tag-class hierarchy). `thread` is maintained on every update batch,
  inside the timed update. Everything else (`edges`, `residents`, `replies`, the
  interaction weights) is a view, recomputed per query.

## Same answers first

A number is only reported for a query once every system returns the same answer.

1. **The Python reference.** `reference.py` evaluates each query's logical plan in
   Python (`snb.parameters.reference`) for its bindings. Both SQL systems matched it
   exactly on all 142 SF0.1 bindings, and on every SF0.1 maintenance checkpoint after
   replaying the update stream. At SF1 the reference is slow (up to six minutes for one
   IC12 binding), so it was computed for 100 of the 142 bindings.
2. **Three-way agreement.** For every SF1 binding, DDIR, DuckDB and Umbra return the same
   answer. For maintenance, all three agree at every captured epoch (every 16th, and the
   last).
3. **Rewrites.** Every rewritten or authored DDIR plan returns exactly the generated
   plan's answer on every binding (`../authored/check.py`), and at every update-stream
   capture.
4. **Repeat each run.** Each check runs a binding at least twice. DuckDB 1.4.5 returned
   nondeterministic, wrong shortest paths from `WITH RECURSIVE … USING KEY` with more
   than one thread; a single run would not have shown it. All numbers use DuckDB 2.0.0,
   which was deterministic in 60 of 60 repeats.

## Bindings

- **Suite bindings:** `suite-0` and `suite-1` are the suite's deterministic smoke
  bindings (`snb.parameters.parameters` and `alternate`), the same ones `suite.py` and
  `update_stream.py` use. Tables report the median over these two.
- **Official BI bindings:** `../bindings/bi-official-sf1.json` holds the first three of
  each BI query's official SF1 parameters, converted by `ldbc_parameters.py` from LDBC's
  `parameters-sf1` files. Interactive parameters are not part of the BI export, so IS and
  IC use the suite bindings only. Several official bindings have empty answers.

## One-off reads

A one-off read is a question asked of loaded data, answered from scratch.

- **DuckDB and Umbra:** load once, then for each binding one warmup and five runs; report
  the median. Every result is checked against the reference or the other systems.
- **DDIR:** one server per query. The server installs the shared graph program and the
  query, loads the snapshot, and settles; then each binding is fed, ticked and peeked,
  then released. Latency is the bind-plus-peek wire time, median of three runs after one
  warmup (`../authored/check.py` measures the generated and authored plans this way).
- **Standing state:** DDIR computes each query's parameter-independent parts once, when
  the query is installed (0.5–10 s per query at SF1), and maintains them. The SQL systems
  recompute those parts on every read. That favours DDIR on reads like IC14, BI14, BI20,
  BI6 and IS7, and is reported, not hidden.

## Maintenance

Per update batch, what does it cost to have every answer current again?

- **DDIR:** `update_stream.py --check-mode replay` over all 521 epochs, one server per
  query with its suite-0 binding standing. The per-batch cost is the update's wire time
  (feed plus tick), during which the standing answer is brought up to date. Answers are
  captured every 16 epochs and compared across systems afterwards.
- **DuckDB and Umbra:** apply each epoch's deletions and insertions in one transaction,
  maintain `thread`, then re-run every query with its suite-0 binding
  (`run_sql.py maintain`). The per-batch cost of a query is the update plus its re-run.
  Runs cover at least the first BI day (16 epochs, every update class) and then stop
  after 60 seconds: re-running 41 queries after every epoch for all 521 epochs takes
  about an hour per configuration and tells little more.
- **Granularity:** an epoch is one update class on one day. A system that re-runs once
  per day instead would pay about 16× less per day; on cheap queries that would bring the
  SQL systems level with DDIR.

## Writing the SQL fairly

- **Rank with `ORDER BY … LIMIT k`.** The suite keys answers by `(request, rank)`. A first
  version computed rank with `row_number() OVER (ORDER BY …)` and filtered on it, which
  made both engines number every candidate before filtering: IC9 cost 3.8× (DuckDB) and
  8.8× (Umbra) more, IC2 2.3× and 4.3×, BI20 4.7× on DuckDB. The SQL now orders and
  limits, and the runner numbers the returned rows.
- **Shortest paths** are recursive CTEs: DuckDB uses `USING KEY` (Bellman-Ford rounds that
  expand only improved rows); Umbra, which has no `USING KEY`, carries the whole state in
  each round, as LDBC's Umbra queries do.
- **No hand tuning beyond that.** The DDIR side has 17 rewritten strategies (below); the
  SQL was not rewritten the same way. Several of the same ideas (semijoins first, keeping
  per-day or per-forum aggregates) would help the SQL systems too.

## DDIR plan variants

Results distinguish:

- **as shipped:** plans generated at master-next `bdc950a5`;
- **#996:** plans generated after #996 (bounded top-k, fewer carried columns);
- **#996 + projection:** the same with the field-demand pass of #1001 (then opt-in with
  `DDIR_PROJECTION=1`; projection is off by default, and `DDIR_PASSES=projection`
  enables it now);
- **authored:** `../authored/plans/`, hand-written. Seventeen follow rewritten strategies,
  each found by looking at what a query's plan does with its request:
  - *apply the request's selective part first* (a semijoin or join on a key): IC3 (the two
    countries' messages by country id), IC6 (the tag's posts), BI10 (the class's messages),
    BI17 (memberships of tagged forums), BI18 (both hops restricted to interested people),
    BI12 (messages by thread language);
  - *keep request-independent aggregates*: BI1 (per-day totals), BI4 (forum popularity),
    BI9 (thread sizes), BI2 (tagged messages by class and day), IC5 (posts per person and
    forum), IC2 and IC9 (each person's 20 newest messages);
  - *rank on narrow rows, fetch wide fields for the winners*: IS2, IC8, IC2, IC9;
  - *search from both ends*: IC13, and BI15 with friendship costs computed only for the
    edges the search reaches;
  - *remove redundant `distinct`s* where a uniqueness fact holds (BI12: a message has one
    thread language).

  One measured variant is not in `plans/`: BI11 with edges oriented from the
  lower-degree endpoint before listing wedges (37 → 23 ms at SF1).

## Running it

```sh
# DDIR: generated and authored plans, one-off (see ../authored/README.md).
python3 interactive/bench/ldbc/authored/check.py --server target/release/ddir_server \
    --prepared JOURNAL --parameters interactive/bench/ldbc/bindings/bi-official-sf1.json \
    --workers 4 --output OUT
# DDIR maintenance (Python validation is slow at SF1; compare captures across systems instead).
python3 interactive/bench/ldbc/update_stream.py --prepared JOURNAL --server target/release/ddir_server \
    --queries all --workers 4 --check-mode replay --check-every 16 --no-check-first-class --output OUT

# Bindings and reference answers (the suite's Python; slow at SF1).
python3 interactive/bench/ldbc/baselines/reference.py --prepared JOURNAL \
    --parameters interactive/bench/ldbc/bindings/bi-official-sf1.json --output ref.json

# DuckDB (Python >= 3.11: pip install duckdb psycopg[binary] pyarrow), from baselines/.
python run_sql.py check    --system duckdb --snapshot SNAP --bindings ref.json
python run_sql.py oneoff   --system duckdb --snapshot SNAP --bindings ref.json --threads 4 --output duck.json
python run_sql.py maintain --system duckdb --snapshot SNAP --journal JOURNAL --bindings ref.json --threads 4 --output duck-maint

# Umbra (Docker image umbradb/umbra, arm64 and amd64).
python run_sql.py export --snapshot SNAP --output /path/to/csv
DATA_DIR=/path/to/csv ./umbra_server.sh 0-3       # CPU set 0-3: four threads
python run_sql.py oneoff --system umbra --csv-dir /data --bindings ref.json --output umbra.json
```

## Practical notes

- **Memory.** On a 16 GB machine, the as-shipped DDIR plans ran out of memory on BI17
  (one-off and update stream) and on IC2 and IC9 in the update stream; #996 fixed all three.
  `update_stream.py` keeps the Python snapshot and its server under `--max-rss-gib` and
  `--max-total-rss-gib`.
- **Python checking dominates SF1 runs.** The reference and `update_stream.py`'s
  validation can take hours at SF1; capture answers and compare systems instead.
- **Umbra in Docker** creates a database volume per container; start it with `--rm`
  (`umbra_server.sh` does) or they accumulate. Umbra's per-batch cost here includes the
  client copying rows into the container (tens of milliseconds), which sets a floor on
  cheap queries.
