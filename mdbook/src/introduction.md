# Differential Dataflow

In this book we will work through the motivation and technical details behind [differential dataflow](https://github.com/frankmcsherry/differential-dataflow), a computational framework build on top of [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) intended for efficiently performing computations on large amounts of data and *maintaining* the computations as the data change.

Differential dataflow arose from [work at Microsoft Research](https://www.microsoft.com/en-us/research/wp-content/uploads/2013/11/naiad_sosp2013.pdf), where we aimed to build a high-level framework that could both compute and incrementally maintain non-trivial algoithms. The work has since progressed, and its incremental recomputation core supports a surprising breadth of functionality.

