## Worst-case optimal joins in differential dataflow

A re-implementation of the worst-case optimal join framework presented in the [`dataflow-join`](https://github.com/frankmcsherry/dataflow-join) project, in the framework of [differential dataflow](https://github.com/frankmcsherry/differential-dataflow).

---

This project transports the dataflow structured implementation of generic join, a worst-case optimal join algorithm, for relations that are the accumulation of differences at *partially ordered* times. This makes it appropriate for, *e.g.* incrementally updated Datalog computations, whose inner joins are between collection that vary both in epoch of input updates and rounds of derivation.

Our approach mirrors that of `dataflow-join`, in which we implement three operators: `count`, `propose`, and `intersect`. Unline `dataflow-join`, these operators must be implemented to act on streams and indices over updates at partially ordered times. This introduces a non-trivial complexity in the maintenance of indexed updates, which is done by hand in `dataflow-join` and relies on the assumption that times are totally ordered.

---

Our approach here is to augment the timestamps `T` with an integer, corresponding to a relation index, and using the *lexicographic* order over the pair `(T, usize)`: 

    (a1, b1) <= (a2, b2)   when   (a1 < a2) or ((a1 == a2) and (b1 <= b2))

this ordering allows two updates to be naturally ordered by their `T` when distinct, but when equal we order times by the associated relation index. This allows us to "pretend" to perform updates in the order of relation index, to avoid double counting updates, without actually maintaining multiple independent indices.