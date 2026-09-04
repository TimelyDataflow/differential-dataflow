# DDIR benchmarks

Every workload here runs as a **session on the one server binary**: `install`
a program, `load` its inputs in bulk (sharded across the workers), `tick` once
for the initial epoch, then `tick R` for R epochs of standing change. The
server times each `tick`; `bench.py` collects the times across backends,
worker counts and repetitions, and writes a JSONL log plus a markdown summary
under `reports/`, named by date and git revision.

```
cargo build --release -p interactive --example ddir_server
./bench.py --scale small --runs 3                       # sanity pass, seconds
./bench.py --scale medium --workers 1,4 --runs 3        # the standing report
./bench.py --aoc --backends vec,corgi                   # the 33 AoC parts, one epoch each
./bench.py --profile scc --backend corgi --scale medium # samply -> reports/profiles/
```

## Workloads

| name | program | input |
|---|---|---|
| scc, cc, triangles, kcore, adt, ast, unnest | `examples/programs/` | a seeded random graph, `random:nodes=N,edges=2N,churn=C` |
| reach, tour | same | the graph plus one root, `feed p 1 0` |
| stable | same | random 4-field rows (`arity=4`), the preference edges |
| aocDDpP | `examples/aoc2023/dayDD/partP.ddp` | the transcribed fact file, one epoch, no churn |

Scales (`--scale`): small = 10k nodes / 20k edges / churn 10 / 20 rounds;
medium = 100k / 200k / 100 / 50; large = 1M / 2M / 1000 / 20. The graph is
deterministic (`seed=0`), so two runs at one revision see the same rows.

## What the numbers mean

- **initial epoch**: the first `tick` — the bulk load reaching the dataflow
  and the whole computation running to its fixed point.
- **per churn epoch**: the `tick R` time divided by R — each epoch retracts C
  rows of the window and inserts C fresh ones, and the tick waits for every
  export to catch up. Closed-loop: one epoch is fully retired before the next
  opens (the old harness's `--sync=K` open-loop regime is not reproduced).
- Not included: parse/lower/optimize (the intake thread), process start, and
  the server's own bookkeeping outside `tick`. The export arrangement the
  server maintains for `peek`/`import` **is** included; it is part of running
  on the server.

- The graph workloads run **without their `| inspect(..)` taps** (a stripped
  copy of each program is written to `bench/.tmp/`). The taps print every row
  they see; unnest's tap on 200k rows was 90% of its "load" time, one
  unbuffered stderr write per row. They are for reading, not for timing.

Medians over `--runs` are reported; the JSONL keeps every run. Single runs
are not comparable across machines or with other things running.

## Profiling

`--profile` wraps one session in `samply record --save-only` and writes a
Firefox-format profile to `reports/profiles/`. Load it with `samply load` or
with the pollard tools; `timely:work-0` is the thread to look at.
