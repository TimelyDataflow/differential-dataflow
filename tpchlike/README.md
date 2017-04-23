## tpchlike: a TPCH-like evaluation of differential dataflow

[TPC-H](http://www.tpc.org/tpch/) is a database benchmark, used by serious people to evaluate things that make money. [Differential dataflow](https://github.com/frankmcsherry/differential-dataflow) is less serious, but it is still interesting to see how it performs on this class of tasks.

The evaluation here is meant to mimic the evaluation done in ["How to Win a Hot Dog Eating Contest"](https://infoscience.epfl.ch/record/218203/files/sigmod2016-cr.pdf?version=1), in which they also evaluate a TPC-H like workload adapted to a streaming setting. The idea is that instead of loading all base relations, we introduce tuples to the base relations in a streaming fashion. Specifically, they (and we) round-robin through the relations adding one tuple to each at a time. 

Our plan is to eventually implement all of the queries, each a few different ways to demonstrate the awesome flexibility of differential dataflow, and to get measurements from each reflecting the amount of time taken. 

---

The program runs with the command line

    cargo run --release -- <path> <logical_batch> <physical_batch>

and looks in `<path>` for the various TPC-H files (e.g. `lineitem.tbl`). If you don't have these files, you can grab the generator at the TPC-H link up above. The `logical_batch` argument merges rounds of input and changes the output of the computation; we try to use `1` for the most part, which acts as if each tuple were introduced independently. The `physical_batch` argument indicates how many logical rounds should be introduced concurrently; increasing this argument can increase the throughput at the expense of latency, but will not change the output of the computation.

Here are some initial measurements on the scale factor 1 dataset (about 1GB of data, and six million tuples in the `lineitem` relation), as we vary the physical batching (varying the concurrent work). We also report the number of tuples in the base relations used by the query, the throughput at batch size 100,000, and the reported numbers for the single-threaded implementation from the hot dog eating paper. These latter numbers are on a scale factor 10 dataset, which my laptop decides to page out, and are intended for *qualitative* comparison; so that we can see where things appear to be much improved (e.g. `q15`, `q19`, `q20`, `q22`), and where there is clearly space to improve (e.g. `q04`, `q06`, `q14`). 

|                                     |  1,000 | 10,000 | 100,000 | tuples touched | rate @ 100,000 | [Hot Dog](https://infoscience.epfl.ch/record/218203/files/sigmod2016-cr.pdf?version=1) |
|------------------------------------:|-------:|-------:|--------:|---------------:|---------------:|----------:|
| [query01](./src/queries/query01.rs) |  4.24s |  4.76s |   5.72s |      6,001,215 |        1.05M/s |   1.27M/s |
| [query02](./src/queries/query02.rs) |  1.09s |  0.58s |   0.46s |      1,010,030 |        2.19M/s | 756.61K/s |
| [query03](./src/queries/query03.rs) |  4.59s |  3.16s |   2.29s |      7,651,215 |        3.35M/s |   3.74M/s |
| [query04](./src/queries/query04.rs) | 12.07s |  7.88s |   5.21s |      7,501,215 |        1.44M/s |  10.08M/s |
| [query05](./src/queries/query05.rs) |  6.17s |  3.99s |   3.12s |      7,661,245 |        2.45M/s | 584.26K/s |
| [query06](./src/queries/query06.rs) |  0.32s |  0.17s |   0.16s |      6,001,215 |       36.88M/s | 138.33M/s |
| [query07](./src/queries/query07.rs) |  4.98s |  2.65s |   1.88s |      7,661,240 |        4.08M/s | 650.65K/s |
| [query08](./src/queries/query08.rs) |  7.85s |  5.38s |   4.06s |      7,861,240 |        1.89M/s |  91.22K/s |
| [query09](./src/queries/query09.rs) | 15.45s |  9.57s |   6.12s |      8,511,240 |        1.39M/s | 104.37K/s |
| [query10](./src/queries/query10.rs) |  8.02s |  5.20s |   3.22s |      7,651,240 |        2.38M/s |   2.89M/s |
| [query11](./src/queries/query11.rs) | 10.81s |  9.40s |   9.82s |        810,025 |       82.49K/s |     768/s |
| [query12](./src/queries/query12.rs) |  2.71s |  1.72s |   1.10s |      7,501,215 |        6.84M/s |   8.68M/s |
| [query13](./src/queries/query13.rs) |  8.66s |  6.06s |   4.96s |      1,650,000 |      332.63K/s | 779.52K/s |
| [query14](./src/queries/query14.rs) |  1.19s |  0.72s |   0.50s |      6,201,215 |       12.52M/s |  33.04M/s |
| [query15](./src/queries/query15.rs) |  4.29s |  2.11s |   1.28s |      6,011,215 |        4.69M/s |      17/s |
| [query16](./src/queries/query16.rs) |  1.92s |  1.22s |   0.98s |      1,010,000 |        1.03M/s | 123.94K/s |
| [query17](./src/queries/query17.rs) |  6.71s |  4.96s |   3.76s |      6,201,215 |        1.65M/s | 379.30K/s |
| [query18](./src/queries/query18.rs) | 30.52s | 22.32s |  18.13s |      7,651,215 |      422.10K/s |   1.13M/s |
| [query19](./src/queries/query19.rs) |  0.51s |  0.27s |   0.24s |      6,201,215 |       25.40M/s |   1.95M/s |
| [query20](./src/queries/query20.rs) |  2.11s |  1.28s |   1.06s |      6,811,215 |        6.41M/s |     977/s |
| [query21](./src/queries/query21.rs) | 23.85s | 16.08s |  10.55s |      7,511,240 |      712.09K/s | 836.80K/s |
| [query22](./src/queries/query22.rs) | 25.67s | 29.30s |  47.69s |      1,650,000 |       56.32K/s |     189/s |

**PLEASE NOTE**: These times are the reported running times of the code in the repository, which may or may not compute the intended quantities. It is very possible (likely, even) that I have botched some or all of the query implementations. I'm not validating the results at the moment, as I don't have much to validate against, but if you think you see bugs (or want to help validating) drop me a line! Please don't just go and use these measurements as "truth" until we find out if I am actually computing the correct answers.
