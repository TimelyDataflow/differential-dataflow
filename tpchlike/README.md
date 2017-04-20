## tpchlike: a TPCH-like evaluation of differential dataflow

[TPC-H](http://www.tpc.org/tpch/) is a database benchmark, used by serious people to evaluate things that make money. [Differential dataflow](https://github.com/frankmcsherry/differential-dataflow) is less serious, but it is still interesting to see how it performs on this class of tasks.

The evaluation here is meant to mimic the evaluation done in ["How to Win a Hot Dog Eating Contest"](https://infoscience.epfl.ch/record/218203/files/sigmod2016-cr.pdf?version=1), in which they also evaluate a TPC-H like workload adapted to a streaming setting. The idea is that instead of loading all base relations, we introduce tuples to the base relations in a streaming fashion. Specifically, they (and we) round-robin through the relations adding one tuple to each at a time. 

Our plan is to eventually implement all of the queries, each a few different ways to demonstrate the awesome flexibility of differential dataflow, and to get measurements from each reflecting the amount of time taken. 

---

The program runs with the command line

    cargo run --release -- <path> <logical_batch> <physical_batch>

and looks in `<path>` for the various TPC-H files (e.g. `lineitem.tbl`). If you don't have these files, you can grab the generator at the TPC-H link up above. The `logical_batch` argument merges rounds of input and changes the output of the computation; we try to use `1` for the most part, which acts as if each tuple were introduced independently. The `physical_batch` argument indicates how many logical rounds should be introduced concurrently; increasing this argument can increase the throughput at the expense of latency.

Here are some initial measurements on the scale factor 1 dataset (about 1GB of data, and six million tuples in the `lineitem` relation), as we vary the physical batching (varying the concurrent work). We also report the number of tuples in the base relations used by the query (though our harness currently inserts all tuples into all relations anyhow; oops).*

|   Query |  1,000 | 10,000 | 100,000 | tuples touched |
|--------:|-------:|-------:|--------:|---------------:|
| [query01](https://github.com/frankmcsherry/differential-dataflow/blob/master/tpchlike/src/queries/query01.rs) |  1.26s |  1.33s |   1.43s |      6,001,215 |
| [query02](https://github.com/frankmcsherry/differential-dataflow/blob/master/tpchlike/src/queries/query02.rs) |  2.29s |  1.72s |   1.61s |      1,010,030 |
| [query06](https://github.com/frankmcsherry/differential-dataflow/blob/master/tpchlike/src/queries/query06.rs) |  1.29s |  1.35s |   1.49s |      6,001,215 |
| [query15](https://github.com/frankmcsherry/differential-dataflow/blob/master/tpchlike/src/queries/query15.rs) | 73.12s | 67.15s |  84.63s |      6,011,215 |
| [query17](https://github.com/frankmcsherry/differential-dataflow/blob/master/tpchlike/src/queries/query17.rs) |  7.42s |  5.72s |   4.78s |      6,201,215 |
| all      |  82.86s   |  74.77s | 90.55s |  7,011,245 |

*: It is very possible that I have botched some of the query implementations. I'm not validating the results at the moment, as I don't have much to validate against, but if you think you see bugs (or want to help validating) drop me a line! Please don't just go and use these measurements as "truth" until we find out if I am actually computing the correct answers.

---

The only comparisons I have at the moment are for those found at [dbtoaster.org](http://www.dbtoaster.org/index.php?page=home&subpage=performance) and in the [hot-dog eating paper](https://infoscience.epfl.ch/record/218203/files/sigmod2016-cr.pdf?version=1). However, I'm still trying to understand their results; for example, they report 100m+ tuples/second for `query06` and just 17 tuples/second for `query15`. Once I learn more about whether I am reading their results correctly, I'll try and post some comparisons.