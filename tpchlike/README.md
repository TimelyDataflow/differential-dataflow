## tpchlike: a TPCH-like evaluation of differential dataflow

[TPC-H](http://www.tpc.org/tpch/) is a database benchmark, used by serious people to evaluate things that make money. [Differential dataflow](https://github.com/frankmcsherry/differential-dataflow) is less serious, but it is still interesting to see how it performs on this class of tasks.

The evaluation here is meant to mimic the evaluation done in ["How to Win a Hot Dog Eating Contest"](https://infoscience.epfl.ch/record/218203/files/sigmod2016-cr.pdf?version=1), in which they also evaluate a TPC-H like workload adapted to a streaming setting. The idea is that instead of loading all base relations, we introduce tuples to the base relations in a streaming fashion. Specifically, they (and we) round-robin through the relations adding one tuple to each at a time. 

Our plan is to eventually implement all of the queries, each a few different ways to demonstrate the awesome flexibility of differential dataflow, and to get measurements from each reflecting the amount of time taken. 

---

The program runs with the command line

    cargo run --release -- <path> <logical_batch> <physical_batch> <query number>

and looks in `<path>` for the various TPC-H files (e.g. `lineitem.tbl`). If you don't have these files, you can grab the generator at the TPC-H link up above. The `logical_batch` argument merges rounds of input and changes the output of the computation; we try to use `1` for the most part, which acts as if each tuple were introduced independently. The `physical_batch` argument indicates how many logical rounds should be introduced concurrently; increasing this argument can increase the throughput at the expense of latency, but will not change the output of the computation.

Here are some initial measurements on the scale factor 1 dataset (about 1GB of data, and six million tuples in the `lineitem` relation), as we vary the physical batching (varying the concurrent work). We also report the number of tuples in the base relations used by the query, the throughput at batch size 100,000, and the reported numbers for the single-threaded implementation from the hot dog eating paper. These latter numbers are on a scale factor 10 dataset, which my laptop decides to page out, and are intended for *qualitative* comparison; so that we can see where things appear to be much improved (e.g. `q15`, `q19`, `q20`, `q22`), and where there is space to improve ourselves (e.g. `q04`, `q06`, `q14`). 

|                                     |  1,000 | 10,000 | 100,000 | tuples touched | rate @ 100,000 | [Hot Dog](https://infoscience.epfl.ch/record/218203/files/sigmod2016-cr.pdf?version=1) |
|------------------------------------:|-------:|-------:|--------:|---------------:|---------------:|----------:|
| [query01](./src/queries/query01.rs) |  4.04s |  4.24s |   5.10s |      6,001,215 |        1.18M/s |   1.27M/s |
| [query02](./src/queries/query02.rs) |  0.51s |  0.40s |   0.40s |      1,010,030 |        2.55M/s | 756.61K/s |
| [query03](./src/queries/query03.rs) |  1.84s |  1.32s |   1.10s |      7,651,215 |        6.98M/s |   3.74M/s |
| [query04](./src/queries/query04.rs) |  2.54s |  2.09s |   1.91s |      7,501,215 |        3.93M/s |  10.08M/s |
| [query05](./src/queries/query05.rs) |  2.90s |  2.32s |   1.77s |      7,661,245 |        4.32M/s | 584.26K/s |
| [query06](./src/queries/query06.rs) |  0.32s |  0.17s |   0.16s |      6,001,215 |       36.88M/s | 138.33M/s |
| [query07](./src/queries/query07.rs) |  2.87s |  1.85s |   1.45s |      7,661,240 |        5.28M/s | 650.65K/s |
| [query08](./src/queries/query08.rs) |  4.74s |  4.01s |   3.04s |      7,861,240 |        2.52M/s |  91.22K/s |
| [query09](./src/queries/query09.rs) | 10.65s |  7.16s |   4.75s |      8,511,240 |        1.79M/s | 104.37K/s |
| [query10](./src/queries/query10.rs) |  7.07s |  4.52s |   2.70s |      7,651,240 |        2.84M/s |   2.89M/s |
| [query11](./src/queries/query11.rs) |  9.98s |  9.08s |   9.73s |        810,025 |       83.25K/s |     768/s |
| [query12](./src/queries/query12.rs) |  1.08s |  0.79s |   0.61s |      7,501,215 |       12.37M/s |   8.68M/s |
| [query13](./src/queries/query13.rs) |  6.18s |  4.47s |   3.83s |      1,650,000 |      430.86K/s | 779.52K/s |
| [query14](./src/queries/query14.rs) |  0.87s |  0.51s |   0.36s |      6,201,215 |       17.12M/s |  33.04M/s |
| [query15](./src/queries/query15.rs) |  1.54s |  0.81s |   0.45s |      6,011,215 |       13.44M/s |      17/s |
| [query16](./src/queries/query16.rs) |  1.17s |  0.85s |   0.71s |      1,010,000 |        1.43M/s | 123.94K/s |
| [query17](./src/queries/query17.rs) |  3.96s |  3.29s |   2.75s |      6,201,215 |        2.25M/s | 379.30K/s |
| [query18](./src/queries/query18.rs) | 12.46s | 10.93s |   9.77s |      7,651,215 |      782.91K/s |   1.13M/s |
| [query19](./src/queries/query19.rs) |  0.43s |  0.23s |   0.22s |      6,201,215 |       28.22M/s |   1.95M/s |
| [query20](./src/queries/query20.rs) |  1.45s |  0.86s |   0.72s |      6,811,215 |        9.43M/s |     977/s |
| [query21](./src/queries/query21.rs) |  8.81s |  6.97s |   5.82s |      7,511,240 |        1.29M/s | 836.80K/s |
| [query22](./src/queries/query22.rs) |  7.67s |  8.27s |   9.50s |      1,650,000 |      173.66K/s |     189/s |

**PLEASE NOTE**: These times are the reported running times of the code in the repository, which may or may not compute the intended quantities. It is very possible (likely, even) that I have botched some or all of the query implementations. I'm not validating the results at the moment, as I don't have much to validate against, but if you think you see bugs (or want to help validating) drop me a line! Please don't just go and use these measurements as "truth" until we find out if I am actually computing the correct answers.
