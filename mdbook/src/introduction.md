# Differential Dataflow

In this book we will work through the motivation and technical details behind [differential dataflow](https://github.com/frankmcsherry/differential-dataflow), a computational framework build on top of [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) intended for efficiently performing computations on large amounts of data and *maintaining* the computations as the data change.

Differential dataflow programs are meant to look like many standard "big data" computations, borrowing idioms from frameworks like MapReduce and SQL. However, once you write and run your program, you can start *changing* the data inputs to the computation, and differential dataflow will promptly show you the changes in its output. Everything you write can be efficiently *maintained* as well as computed.

Differential dataflow arose from [work at Microsoft Research](https://www.microsoft.com/en-us/research/wp-content/uploads/2013/11/naiad_sosp2013.pdf), where we aimed to build a high-level framework that could both compute and incrementally maintain non-trivial algoithms. The work has since progressed, and its incremental recomputation core supports a surprising breadth of functionality.

