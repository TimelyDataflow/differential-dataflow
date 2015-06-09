# Differential Dataflow
An implementation of [differential dataflow](http://www.cidrdb.org/cidr2013/Papers/CIDR13_Paper111.pdf) using [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) on [Rust](http://www.rust-lang.org).

Differential dataflow is a fully incrementalized data-parallel programming framework. Rather than process large collections of data once, Differential dataflow maintains enough information about the processed data to be able to react quickly to changes in the inputs, producing any corresponding changes in the computation's outputs.

Importantly, differential dataflow supports more than the common data-parallel operators like `group_by` and `join`, including additionally a fixed-point operator `iterate` which repeatedly applies data-parallel logic to a collection until it converges. Iterative computations are also fully incrementalized, and differential dataflow can often quickly update the results of iterative computations.
