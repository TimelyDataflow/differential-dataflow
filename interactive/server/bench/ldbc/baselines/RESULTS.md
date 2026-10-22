# Results on one machine (SF1)

These are the numbers from one run of the method in [METHOD.md](METHOD.md). They are
for comparison on this machine only: the SQL engines, DDIR and the hardware all
matter, and other systems measured elsewhere will not be directly comparable.

**Machine.** Apple M4 (4 performance and 6 efficiency cores), 16 GB, macOS 15.
DuckDB 2.0.0 (in process); Umbra 26.10 (`umbradb/umbra`, arm64, in an OrbStack Linux
VM, CPU set pinned); DDIR `ddir_server` release builds. LDBC SNB BI SF1
`composite-merged-fk` (12M projected rows; 521 update epochs).

**Columns.** One-off read latency in milliseconds, median over the suite's two bindings
(`suite-0`, `suite-1`), each the median of several warm runs. DuckDB at 1 and 4 threads,
Umbra on 1 and 4 CPUs, DDIR at 4 workers:
*as shipped* is master-next `bdc950a5` (OOM: out of memory on 16 GB);
*generated* is master-next `71e37290` (#996, projection pass off);
*authored* is the authored plans as committed: 26 timed on the same build, and the 15
whose sums were last moved into multiplicities (BI1, BI2, BI4, BI5, BI6, BI8, BI9,
BI12, BI13, BI14, BI15, BI16, IC3, IC5, IC10) re-timed on master-next `59e700f4`. Batch columns are the mean cost per update
epoch over the first BI day (16 epochs), at 4 threads, CPUs or workers: for DuckDB and
Umbra, applying the update and re-running the query; for DDIR, applying the update to
its standing answer, with the best of the measured plans (as shipped, #996, the
rewrites, and 15 authored plans re-measured on `59e700f4`; builds `bdc950a5` to `59e700f4`).

**Answers.** Every number is for a query whose answers agree: DuckDB, Umbra and DDIR on
all 142 SF1 bindings; the authored plans as committed match the generated plans on every
SF1 binding and on the fixture.

**Sums in the multiplicity.** Moving those 15 plans' sums from `collect` and `fold` into
multiplicities made one-off reads 0–33% faster (BI5 15.4 → 10.4 ms). It matters most
under updates: over all 521 epochs BI1's mean cost per batch fell from 3.88 to 1.33 ms
(worst batch 51 → 16 ms, peak memory 2.36 → 2.61 GB), and BI9's from 7.99 to 6.73 ms.
Across those 15 authored plans, the all-epoch mean per batch improved on the best earlier
plan for 10 (BI6 9.31 → 2.32 ms, BI8 5.56 → 2.58, BI5 4.35 → 2.27, BI13 3.31 → 1.40,
BI16 2.76 → 1.34), was level for 2 (BI2, BI15), and was worse for 3 (BI4 4.80 → 10.28,
BI9 3.74 → 6.66, IC5 2.60 → 17.55). Those three rank large candidate sets with `collect`,
where #996's generated plans use a bounded top-k that maintains only the first k.

**Summary** (geometric means of DDIR ÷ the faster of DuckDB and Umbra at 4 threads):

| | Ratio |
| --- | ---: |
| One-off, DDIR as shipped (40 queries; BI17 ran out of memory) | 4.22 |
| One-off, DDIR generated (#996) | 3.29 |
| One-off, DDIR authored | 0.76 |
| Per update batch, DDIR best plan | 0.07 |

Below 1 favours DDIR. Read the caveats in METHOD.md with these: DDIR's one-off reads
benefit from state it builds at install, the SQL systems re-run once per epoch rather
than once per day, and the DDIR rewrites have no SQL counterpart.

| Query | DuckDB 1t / 4t | Umbra 1c / 4c | DDIR as shipped | DDIR generated | DDIR authored | Batch: DuckDB / Umbra | Batch: DDIR |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| IS1 | 0.3 / 0.4 | 0.9 / 0.7 | 0.3 | 0.3 | 0.3 | 7.9 / 24.1 | 2.3 |
| IS2 | 8.3 / 3.4 | 3.8 / 2.3 | 13.8 | 7.7 | 2.5 | 12.6 / 26.0 | 1.6 |
| IS3 | 1.3 / 1.5 | 1.5 / 1.6 | 1.4 | 7.3 | 2.9 | 10.5 / 26.5 | 1.0 |
| IS4 | 0.9 / 0.5 | 1.0 / 0.5 | 0.3 | 0.3 | 0.3 | 8.3 / 24.3 | 1.6 |
| IS5 | 0.9 / 0.6 | 1.0 / 0.9 | 0.3 | 0.3 | 0.3 | 8.2 / 24.3 | 0.8 |
| IS6 | 1.1 / 0.8 | 1.1 / 1.1 | 0.4 | 0.3 | 0.3 | 8.6 / 24.7 | 1.5 |
| IS7 | 15.5 / 5.3 | 7.8 / 4.2 | 0.5 | 0.4 | 0.4 | 14.7 / 30.9 | 1.2 |
| IC1 | 40.5 / 22.2 | 23.2 / 15.1 | 14.5 | 14.5 | 12.0 | 39.4 / 39.7 | 1.0 |
| IC2 | 14.7 / 5.3 | 5.1 / 3.0 | 653 | 323 | 4.4 | 16.1 / 27.4 | 5.9 |
| IC3 | 110 / 64.7 | 70.0 / 24.3 | 2539 | 1328 | 2.4 | 95.0 / 53.7 | 1.5 |
| IC4 | 11.7 / 6.9 | 9.8 / 6.7 | 46.4 | 38.3 | 24.6 | 18.6 / 33.0 | 2.8 |
| IC5 | 125 / 49.4 | 109 / 39.2 | 567 | 451 | 36.4 | 82.3 / 83.6 | 1.6 |
| IC6 | 36.1 / 18.0 | 79.6 / 29.2 | 173 | 169 | 6.6 | 34.1 / 53.8 | 1.3 |
| IC7 | 13.7 / 5.4 | 6.2 / 4.8 | 7.8 | 5.5 | 5.8 | 14.9 / 29.9 | 2.4 |
| IC8 | 6.8 / 2.9 | 2.5 / 2.1 | 8.8 | 4.4 | 1.4 | 11.9 / 25.9 | 1.6 |
| IC9 | 64.9 / 21.6 | 23.7 / 11.4 | 6884 | 1246 | 43.1 | 38.6 / 36.5 | 8.5 |
| IC10 | 25.0 / 11.8 | 25.9 / 16.6 | 31.2 | 30.0 | 9.2 | 22.2 / 41.4 | 1.4 |
| IC11 | 17.5 / 8.1 | 17.2 / 9.6 | 18.0 | 13.1 | 8.0 | 17.8 / 34.1 | 1.7 |
| IC12 | 59.8 / 26.6 | 17.0 / 9.3 | 78.9 | 33.0 | 17.5 | 44.7 / 36.5 | 1.4 |
| IC13 | 104 / 68.8 | 29.2 / 17.3 | 19.7 | 19.3 | 2.6 | 80.1 / 41.8 | 1.8 |
| IC14 | 1063 / 646 | 235 / 115 | 62.6 | 61.8 | 58.0 | 654 / 140 | 2.1 |
| BI1 | 14.1 / 4.6 | 21.8 / 6.9 | 1909 | 951 | 1.8 | 12.7 / 30.8 | 1.1 |
| BI2 | 7.3 / 3.7 | 5.3 / 3.4 | 15.3 | 21.4 | 0.6 | 11.9 / 27.0 | 1.2 |
| BI3 | 24.1 / 12.7 | 22.7 / 10.5 | 36.3 | 34.6 | 17.2 | 20.3 / 34.3 | 1.9 |
| BI4 | 115 / 42.4 | 102 / 35.7 | 1107 | 547 | 60.1 | 51.1 / 60.9 | 3.4 |
| BI5 | 16.9 / 7.1 | 14.4 / 11.4 | 14.1 | 10.1 | 6.1 | 16.9 / 37.4 | 2.7 |
| BI6 | 69.5 / 28.2 | 54.3 / 27.0 | 12.9 | 10.3 | 7.0 | 40.6 / 56.1 | 2.5 |
| BI7 | 19.3 / 8.8 | 64.5 / 23.0 | 31.9 | 27.1 | 10.1 | 18.8 / 48.8 | 2.9 |
| BI8 | 29.4 / 14.1 | 21.1 / 13.0 | 34.6 | 30.2 | 13.8 | 27.2 / 38.4 | 2.4 |
| BI9 | 169 / 62.1 | 187 / 65.4 | 2142 | 529 | 202 | 72.5 / 88.5 | 2.5 |
| BI10 | 138 / 84.2 | 116 / 46.4 | 1362 | 1327 | 87.2 | 92.2 / 70.7 | 2.9 |
| BI11 | 23.0 / 11.5 | 14.1 / 8.1 | 55.8 | 51.6 | 27.9 | 19.7 / 32.9 | 1.5 |
| BI12 | 108 / 38.0 | 148 / 50.2 | 1589 | 512 | 55.7 | 48.1 / 74.6 | 1.8 |
| BI13 | 19.2 / 8.4 | 18.8 / 10.6 | 52.3 | 37.2 | 13.9 | 17.0 / 34.8 | 2.3 |
| BI14 | 34.9 / 14.0 | 20.0 / 12.0 | 2.0 | 1.3 | 1.0 | 23.3 / 35.4 | 1.2 |
| BI15 | 1898 / 1216 | 459 / 184 | 2681 | 1075 | 14.3 | 1244 / 225 | 2.2 |
| BI16 | 20.7 / 8.9 | 13.6 / 9.1 | 16.7 | 16.5 | 6.4 | 17.2 / 33.1 | 1.4 |
| BI17 | 2275 / 807 | 151 / 74.6 | OOM | 901 | 170 | 1612 / 162 | 2.2 |
| BI18 | 110 / 46.8 | 43.1 / 18.1 | 700 | 568 | 179 | 87.6 / 49.7 | 4.8 |
| BI19 | 1099 / 636 | 286 / 113 | 119 | 118 | 88.5 | 654 / 139 | 3.0 |
| BI20 | 27.9 / 17.8 | 18.0 / 10.5 | 0.9 | 0.8 | 0.8 | 26.5 / 37.3 | 1.1 |

The same data is in [results-mini03-sf1.csv](results-mini03-sf1.csv).
