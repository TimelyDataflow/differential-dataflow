# Current standard-runner measurements

Measured on an Apple M4 mini with 16 GiB RAM and four workers, using unmodified
master-next `229508dd6f7d046c5d1ac1b96da4be2f7b100124`. Isolated trials ran on
2026-09-10; the concurrent panel and tiny check ran on 2026-09-12. The remote
master-next still named this revision on 2026-09-12.

All 41 queries passed on both Vec and Corgi in three fresh isolated trials.
The five-query concurrent panel passed three trials on both backends; the tiny
fixture passed all 41 queries concurrently on both backends. There were 8,818
checked answer snapshots across the seven reports, including setup and warmup.
These are same-plan reference checks, not 8,818 independent specification tests.

This is a reproducible **small-data baseline**, not an SF1 or official LDBC
score. SF0.003 projects 35,588 input rows (50 people, 3,660 messages, 492 likes).
The suite loads every common relation and uses the natural query definitions.
The default parameter bank is a smoke bank. Each interactive query has three
request IDs, which need not have distinct parameter values; each BI query has
one standing binding. It does not measure 30 distinct maintained settings.

[DATA.md](DATA.md) pins the public data download. [REFRESH.md](REFRESH.md)
contains exact commands, timing definitions and replacement rules.
[BUILD.md](measurements/current/BUILD.md) records the engine, dependencies,
compiler, hardware and resource limits. [GAPS.md](GAPS.md) identifies follow-up
tasks without attributing elapsed time to an unmeasured cause.

## Reading the times

Each row reports the median of three fresh-trial cycle medians, in milliseconds.
Each trial has two warmup and five measured cycles. The range is the smallest
and largest **trial median**, not the spread of individual samples or a
confidence interval.

For an interactive query, a cycle includes initial and changed-graph request
batches, complete answer reads, request retractions and empty checks, and graph
mutation/restoration. For BI it includes mutation/restoration and maintained
answer reads. The graph change retracts one knows, member and like fact, then
restores them; many answers are unaffected. Setup and final standing-query
retirement are separate and available in the full readout.

**Cycle wire** sums recorded request/response times, including transport and
protocol work. **Cycle client** also includes preparation, encoding and decoding.
Both exclude intervening Python reference/validation work. These are complete
recorded lifecycle costs, not single-request latency, pure server CPU time,
or input-update throughput. Comparing another engine or workload requires the
same data, parameter schedule, update stream and timing contract.

## All queries, isolated

Each query gets a fresh server with the full common input. The three raw
reports are [trial 1](measurements/current/isolated-1.json.gz),
[trial 2](measurements/current/isolated-2.json.gz) and
[trial 3](measurements/current/isolated-3.json.gz).

| Query / panel | Backend | Cycle wire | Trial-median wire range | Cycle client |
| --- | --- | ---: | ---: | ---: |
| is1 | vec | 1.743 | 1.705–1.744 | 2.616 |
| is2 | vec | 6.956 | 6.891–7.049 | 14.121 |
| is3 | vec | 2.218 | 2.210–2.240 | 5.115 |
| is4 | vec | 1.954 | 1.923–2.005 | 2.984 |
| is5 | vec | 1.932 | 1.920–1.983 | 2.273 |
| is6 | vec | 2.258 | 2.205–2.263 | 2.873 |
| is7 | vec | 3.269 | 3.254–3.271 | 7.243 |
| ic1 | vec | 5.941 | 5.939–5.989 | 9.175 |
| ic2 | vec | 35.263 | 35.223–35.282 | 60.096 |
| ic3 | vec | 60.917 | 60.591–61.074 | 61.234 |
| ic4 | vec | 13.493 | 13.274–13.516 | 16.795 |
| ic5 | vec | 14.713 | 14.670–14.778 | 24.689 |
| ic6 | vec | 23.509 | 23.499–23.666 | 25.410 |
| ic7 | vec | 4.537 | 4.503–4.559 | 12.613 |
| ic8 | vec | 3.367 | 3.359–3.389 | 13.281 |
| ic9 | vec | 76.714 | 76.399–76.840 | 91.278 |
| ic10 | vec | 3.957 | 3.912–3.970 | 4.923 |
| ic11 | vec | 4.926 | 4.920–4.945 | 8.465 |
| ic12 | vec | 50.000 | 49.624–50.036 | 56.196 |
| ic13 | vec | 55.975 | 55.923–55.980 | 56.225 |
| ic14 | vec | 143.177 | 142.663–147.202 | 143.509 |
| bi1 | vec | 0.867 | 0.831–0.881 | 1.981 |
| bi2 | vec | 1.124 | 1.074–1.124 | 2.993 |
| bi3 | vec | 1.195 | 1.182–1.199 | 1.469 |
| bi4 | vec | 1.527 | 1.499–1.541 | 6.803 |
| bi5 | vec | 1.201 | 1.195–1.207 | 1.613 |
| bi6 | vec | 1.233 | 1.228–1.254 | 1.544 |
| bi7 | vec | 1.169 | 1.153–1.172 | 3.304 |
| bi8 | vec | 1.479 | 1.442–1.495 | 1.906 |
| bi9 | vec | 1.245 | 1.223–1.268 | 5.083 |
| bi10 | vec | 4.263 | 4.254–4.273 | 6.367 |
| bi11 | vec | 0.893 | 0.872–0.900 | 0.978 |
| bi12 | vec | 1.074 | 1.066–1.091 | 1.488 |
| bi13 | vec | 1.002 | 0.996–1.010 | 1.214 |
| bi14 | vec | 1.255 | 1.215–1.262 | 1.353 |
| bi15 | vec | 5.363 | 5.272–5.427 | 5.476 |
| bi16 | vec | 1.362 | 1.357–1.391 | 1.522 |
| bi17 | vec | 1.188 | 1.168–1.201 | 1.233 |
| bi18 | vec | 1.220 | 1.196–1.241 | 1.528 |
| bi19 | vec | 21.066 | 20.960–21.218 | 21.185 |
| bi20 | vec | 1.184 | 1.139–1.188 | 1.278 |
| is1 | corgi | 1.958 | 1.919–1.959 | 2.952 |
| is2 | corgi | 6.724 | 6.684–6.779 | 13.821 |
| is3 | corgi | 2.498 | 2.491–2.544 | 5.364 |
| is4 | corgi | 2.181 | 2.155–2.237 | 3.254 |
| is5 | corgi | 2.438 | 2.372–2.444 | 2.792 |
| is6 | corgi | 2.638 | 2.606–2.704 | 3.310 |
| is7 | corgi | 4.970 | 4.936–5.010 | 9.036 |
| ic1 | corgi | 11.624 | 11.590–11.717 | 15.257 |
| ic2 | corgi | 21.688 | 21.584–21.769 | 38.492 |
| ic3 | corgi | 32.742 | 32.725–32.884 | 33.072 |
| ic4 | corgi | 8.302 | 8.261–8.306 | 11.457 |
| ic5 | corgi | 10.840 | 10.791–10.860 | 20.685 |
| ic6 | corgi | 13.972 | 13.872–13.986 | 15.841 |
| ic7 | corgi | 5.780 | 5.675–5.848 | 13.764 |
| ic8 | corgi | 4.017 | 3.889–4.036 | 13.522 |
| ic9 | corgi | 50.318 | 49.942–50.500 | 64.367 |
| ic10 | corgi | 4.977 | 4.893–5.003 | 5.944 |
| ic11 | corgi | 5.580 | 5.567–5.619 | 9.079 |
| ic12 | corgi | 10.826 | 10.802–11.238 | 16.418 |
| ic13 | corgi | 53.981 | 53.419–54.052 | 54.229 |
| ic14 | corgi | 174.001 | 173.318–174.648 | 174.340 |
| bi1 | corgi | 0.868 | 0.852–0.875 | 1.984 |
| bi2 | corgi | 1.179 | 1.167–1.184 | 3.071 |
| bi3 | corgi | 1.246 | 1.236–1.250 | 1.528 |
| bi4 | corgi | 1.809 | 1.770–1.812 | 7.091 |
| bi5 | corgi | 1.277 | 1.260–1.278 | 1.695 |
| bi6 | corgi | 1.379 | 1.371–1.381 | 1.700 |
| bi7 | corgi | 1.231 | 1.224–1.290 | 3.410 |
| bi8 | corgi | 1.719 | 1.697–1.725 | 2.146 |
| bi9 | corgi | 1.318 | 1.311–1.369 | 5.159 |
| bi10 | corgi | 5.080 | 5.079–5.105 | 7.224 |
| bi11 | corgi | 0.963 | 0.889–1.006 | 1.037 |
| bi12 | corgi | 1.108 | 1.085–1.119 | 1.544 |
| bi13 | corgi | 1.053 | 1.035–1.060 | 1.250 |
| bi14 | corgi | 1.964 | 1.955–2.014 | 2.062 |
| bi15 | corgi | 10.156 | 10.123–10.572 | 10.278 |
| bi16 | corgi | 1.512 | 1.473–1.527 | 1.675 |
| bi17 | corgi | 1.260 | 1.249–1.274 | 1.305 |
| bi18 | corgi | 1.401 | 1.373–1.406 | 1.707 |
| bi19 | corgi | 25.673 | 25.547–25.695 | 25.799 |
| bi20 | corgi | 1.303 | 1.267–1.307 | 1.402 |

At this scale Corgi has lower cycle times for IC2–6, IC9 and IC12, while Vec
has lower times for several short interactive queries, IC14 and BI15/19.
IC12 is the largest ratio in Corgi's favor (50.000 / 10.826, about 4.6x).
These are backend comparisons of this workload; they do not locate the
responsible operators. The many roughly 1-ms BI cycles should not be read as
evidence of cheap maintenance under changes that substantially affect results.

## Five queries concurrently

IS1, IS3, IC11, BI11 and BI18 are installed together and share the common
substrate. A row is one whole panel cycle, not a per-query average or a sum of
isolated measurements. Requests and reads are driven in the standard runner's
order; this is not a concurrent-client saturation test.
Raw reports: [trial 1](measurements/current/panel-1.json.gz),
[trial 2](measurements/current/panel-2.json.gz),
[trial 3](measurements/current/panel-3.json.gz).

| Query / panel | Backend | Cycle wire | Trial-median wire range | Cycle client |
| --- | --- | ---: | ---: | ---: |
| is1, is3, ic11, bi11, bi18 | vec | 10.093 | 10.049–10.272 | 17.418 |
| is1, is3, ic11, bi11, bi18 | corgi | 11.777 | 11.775–12.022 | 19.145 |

## Run completion and memory

Whole-command duration below includes reference evaluation and server startup,
unlike cycle sums above. Footprint is the sampled combined Python/server
physical footprint from the external monitor, not server RSS.

| Recipe | Trial | Duration (s) | Peak footprint (MiB) | Checked snapshots |
| --- | ---: | ---: | ---: | ---: |
| Isolated catalogue | 1 | 72.55 | 400.1 | 2,446 |
| Isolated catalogue | 2 | 73.11 | 398.4 | 2,446 |
| Isolated catalogue | 3 | 72.95 | 385.7 | 2,446 |
| Concurrent panel | 1 | 2.29 | 189.3 | 328 |
| Concurrent panel | 2 | 2.30 | 187.5 | 328 |
| Concurrent panel | 3 | 2.27 | 200.9 | 328 |
| Tiny catalogue | 1 | 0.95 | 247.9 | 496 |

Every run completed below the 2-GiB process-group cap, with normal memory
pressure and zero swap use. The [tiny report](measurements/current/tiny.json.gz)
is a correctness/overhead check using one round and no warmup.
Each report has a matching `.guard.json` receipt in
[measurements/current](measurements/current/) with observed resource outcomes.

## Reproduce or replace

Regenerate either timing table from the checked-in reports:

```sh
python3 interactive/server/bench/ldbc/readout.py --summary \
  interactive/server/bench/ldbc/measurements/current/isolated-{1,2,3}.json.gz
python3 interactive/server/bench/ldbc/readout.py --summary \
  interactive/server/bench/ldbc/measurements/current/panel-{1,2,3}.json.gz
```

Omit `--summary` for each cycle's timings, setup, warmup and final retirement.
`compare.py` accepted trial 1 versus trials 2 and 3 for both timing recipes,
including matching checked answers. The summary also checks matching binary,
workload and environment identities before combining trials.

Anyone can replace these observations by following [REFRESH.md](REFRESH.md).
Record the measured revision, replace the raw reports and tables, and update
affected gap assessments. A contradictory rerun can be promoted without
permission from the original author or a causal explanation. Git retains the
previous version; no historical experiment archive needs to grow here.
