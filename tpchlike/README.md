## tpchlike: a TPCH-like evaluation of differential dataflow

[TPC-H](http://www.tpc.org/tpch/) is a database benchmark, used by serious people to evaluate things that make money. [Differential dataflow](https://github.com/frankmcsherry/differential-dataflow) is less serious, but it is still interesting to see how it performs on this class of tasks.

The evaluation here is meant to mimic the evaluation done in ["How to Win a Hot Dog Eating Contest"](https://infoscience.epfl.ch/record/218203/files/sigmod2016-cr.pdf?version=1), in which they also evaluate a TPC-H like workload adapted to a streaming setting. The idea is that instead of loading all base relations, we introduce tuples to the base relations in a streaming fashion. Specifically, they (and we) round-robin through the relations adding one tuple to each at a time. 

Our plan is to eventually implement all of the queries, each a few different ways to demonstrate the awesome flexibility of differential dataflow, and to get measurements from each reflecting the amount of time taken. 

---

The program runs with the command line

    cargo run --release -- <path> <logical_batch> <physical_batch>

and looks in `<path>` for the various TPC-H files (e.g. `lineitem.tbl`). If you don't have these files, you can grab the generator at the TPC-H link up above. The `logical_batch` argument merges rounds of input and changes the output of the computation; we try to use `1` for the most part, which acts as if each tuple were introduced independently. The `physical_batch` argument indicates how many logical rounds should be introduced concurrently; increasing this argument can increase the throughput at the expense of latency, but will not change the output of the computation.

Here are some initial measurements on the scale factor 1 dataset* (about 1GB of data, and six million tuples in the `lineitem` relation), as we vary the physical batching (varying the concurrent work). We also report the number of tuples in the base relations used by the query, and the throughput at batch size 100,000.**

|                                     |  1,000 | 10,000 | 100,000 | tuples touched | rate @ 100,000 |
|------------------------------------:|-------:|-------:|--------:|---------------:|---------------:|
| [query01](./src/queries/query01.rs) |  0.35s |  0.32s |   0.30s |      6,001,215 |       19.84M/s |
| [query02](./src/queries/query02.rs) |  1.15s |  0.65s |   0.48s |      1,010,030 |        2.10M/s |
| [query03](./src/queries/query03.rs) |  1.88s |  1.43s |   1.28s |      7,651,215 |        5.96M/s |
| [query04](./src/queries/query04.rs) |  5.89s |  4.10s |   2.66s |      7,501,215 |        2.82M/s |
| [query05](./src/queries/query05.rs) |  4.51s |  3.50s |   2.86s |      7,661,245 |        2.68M/s |
| [query06](./src/queries/query06.rs) |  0.35s |  0.33s |   0.32s |      6,001,215 |       18.83M/s |
| [query07](./src/queries/query07.rs) |  2.32s |  1.46s |   1.07s |      7,661,240 |        7.17M/s |
| [query08](./src/queries/query08.rs) |  6.82s |  5.28s |   3.87s |      7,861,240 |        1.98M/s |
| [query15](./src/queries/query15.rs) | 70.19s | 65.75s |  81.80s |      6,011,215 |       74.48K/s |
| [query17](./src/queries/query17.rs) |  6.56s |  4.79s |   3.75s |      6,201,215 |        1.66M/s |
| [query19](./src/queries/query19.rs) |  0.63s |  0.50s |   0.46s |      6,201,215 |       13.48M/s |
| [query20](./src/queries/query20.rs) |  1.05s |  0.70s |   0.61s |      6,811,215 |       11.20M/s |

*: I have a copy of the scale factor 10 dataset, which is what the hot-dog eating paper uses, but OSX annoyingly sees the sequential data loading as a great indication that it should page everything out to disk. It ends up at 16GB in memory, reading the data back in mostly measures my SSD (which gives 800MB/s reads, and indeed most queries take just over 20 seconds).

**: It is very possible that I have botched some of the query implementations. I'm not validating the results at the moment, as I don't have much to validate against, but if you think you see bugs (or want to help validating) drop me a line! Please don't just go and use these measurements as "truth" until we find out if I am actually computing the correct answers.

---

The only comparisons I have at the moment are for those found at [dbtoaster.org](http://www.dbtoaster.org/index.php?page=home&subpage=performance) and in the [hot-dog eating paper](https://infoscience.epfl.ch/record/218203/files/sigmod2016-cr.pdf?version=1). However, I'm still trying to understand their results; for example, they report 100m+ tuples/second for `query06` and just 17 tuples/second for `query15`. Once I learn more about whether I am reading their results correctly, I'll try and post some comparisons.