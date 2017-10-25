## Worst-case optimal joins in differential dataflow

This project is an implementation of the worst-case optimal join dataflow framework presented in the [`dataflow-join`](https://github.com/frankmcsherry/dataflow-join) project, in the framework of [differential dataflow](https://github.com/frankmcsherry/differential-dataflow).

---

Recent work on so called "worst-case optimal" join computation demonstrated new ways to compute relational joins (and more), based on extending candidate tuples by entire attributes at a time (rather than "relation at a time" as in traditional binary joins). These algorithms can be substantially more efficient than existing algorithms, but the work is still relatively young and not all implementations are well understood.

Our goal in this project is to transport [an existing dataflow implementation](https://github.com/frankmcsherry/dataflow-join) with support for incrementally changing input relations, to the framework of [differential dataflow](https://github.com/frankmcsherry/differential-dataflow) in which collections can change in more general ways, suitable for updating iterative computation.

### An outline

A recent approach to worst-case optimal join computation operates by first ordering all attributes of the involved relations, and then attribute-by-attribute computing the relational join of all relations restricted to these attributes, in a special way. From the tuples in the join of the relations on the first k attributes, each tuple is extended by 

1. identifying the relation that would propose the fewest distinct values for the k+1st attribute,
2. having that relation propose those few distinct values as extensions, 
3. having other relations validate the results by intersecting with their proposals.

This scheme does work proportional to the fewest distinct values extending each tuple, and can be shown (by others) to do no more work than could be required to produce the output, for adversarially arranged input relations of the same size.

### A start

One can naively implement this algorithm in differential dataflow, as is done in the [`examples/ngo.rs`]() example. This example just does triangles (a join `t(x,y,z) := g(x,y), g(x,z), g(y,z)`), but the approach generalizes.

First, the example forms a few indices from the source relation `edges` (which is `g` just above):

```rust
    // arrange the edge relation three ways.
    let as_self = edges.arrange_by_self();
    let forward = edges.arrange_by_key();
    let reverse = edges.map_in_place(|x| ::std::mem::swap(&mut x.0, &mut x.1))
                       .arrange_by_key();

    // arrange the count of extensions from each source.
    let counts = edges.map(|(src, _dst)| src)
                      .arrange_by_self();
```

These are the edge as pairs themselves, then indexed by their source, then by their destination, and then an index of the number of candidates from each source. These indices are defined so that they can be re-used as appropriate, rather than having identical copies of the same indices constructed throughout the computation.

Second, we take the input edge tuples `(src, dst)` and for each field we weight the tuple by the number of extensions that would be proposed using that field as a key:

```rust
    // extract ((src, dst), idx) tuples with weights equal to the number of extensions.
    let cand_count1 = forward.join_core(&counts, |&src, &dst, &()| Some(((src, dst), 1)));
    let cand_count2 = reverse.join_core(&counts, |&dst, &src, &()| Some(((src, dst), 2)));
```

Notice that we also tag the records with an identifier that distinguishes the two weightings, so that we can decide which of the two relations would propose fewer extensions.

Third, we determine for each tuple the relation with smallest weight, and therefore fewest distinct values for extension:

```rust
    // determine for each (src, dst) tuple which index would propose the fewest extensions.
    let winners = cand_count1.concat(&cand_count2)
                             .group(|_srcdst, counts, output| {
                                 let mut min_cnt = isize::max_value();
                                 let mut min_idx = usize::max_value();
                                 for &(&idx, cnt) in counts.iter() {
                                     if min_cnt > cnt {
                                         min_idx = idx;
                                         min_cnt = cnt;
                                     }
                                 }
                                 output.push((min_idx, 1));
                             });
```

This is no more complicated than concatenating all of the proposals, and then using `group` to determine which of values `1` and `2` have the least weight.

Finally, for each relation (here, each of `1` and `2`) we select out those elements for which the relation had the smallest weight, propose the extensions using `join`, and then filter the extensions by the other relations with a semijoin. Here is the fragment for the forward index, identified by `1`:

```rust
    // select tuples with the first relation minimizing the proposals, join, then intersect.
    let winners1 = winners.flat_map(|((src, dst), index)| if index == 1 { Some((src, dst)) } else { None })
                          .join_core(&forward, |&src, &dst, &ext| Some(((dst, ext), src)))
                          .join_core(&as_self, |&(dst, ext), &src, &()| Some(((dst, ext), src)))
                          .map(|((dst, ext), src)| (src, dst, ext));
```

The reverse index, identified by `2`, undergoes a similar computation using `index == 2` and `&reverse`, as well as slightly different field manipulation, and the results are concatenated together.

### An improvement

While the first approach above works, and has the "worst-case optimal" bound, it has a major limitation: it require random access to data whose size is linear in the amount of computation done, the worst-case optimal bound, rather than linear in the size of the input relations. This limits the approach to computations whose total work performed is bounded by the memory of the system it runs on.

We can improve the memory requirements to linear in the sizes of the input relations (with factors depending on the query complexity), following recent work (linked above) on dataflow implementations of the worst-case optimal join algorithms. Informally, the intuition is that we can describe independent update rules for each relation, rather than requiring a single computation that responds to updates to any of its inputs (the source of stress in the implementation above).

For example, in the triangle query `t(x,y,z) := g(x,y), g(x,z), g(y,z)` we can write independent update rules for `g(x,y)`, `g(x,z)`, and `g(y,z)`, using what are called "delta queries" in the databases community: the query that given a change to one relation determines the change to the whole query. The three delta queries are

    dtdxy(x,y,z) := dg(x,y), g(x,z), g(y,z)
    dtdxz(x,y,z) := dg(x,z), g(x,y), g(y,z)
    dtdyz(x,y,z) := dg(y,z), g(x,y), g(x,z)

Each of these queries can be evaluated and maintained independently, importantly using join operators that only respond to changes in their `dg` inputs, and do no work for changes in the `g` inputs. As a result, the operators can be implemented to require only the maintained indices over `g`, which have size linear in the input relations, and not require any state proportional to the volume of `dg` changes they process.

Unfortunately, for this to be correct we cannot simultaneously execute all update rules; doing so would risk either under- or double-counting updates that would show up in a second-order term. One way around this is to execute the rules "in order", so that concurrent changes are only visible for relations that have been updated. Each rule should accumulate updates using either `less_than` or `less_equal`, depending one whether the index is update after or before the rule, respectively.

### A Challenge

This is the fundamental issues with differential dataflow: while it understands and preserves `less_equal` comparisons among times, it does not understand `less_than`, and may conflate two distinct times that are nonetheless equivalent from the `less_equal` point of view. This is currently fundamental to how differential dataflow operates, and that lattice join and meet operations work with respect to `less_equal` rather than `less_than`.

Our approach will be to augment the ambient timestamps `T` with an integer, corresponding to a relation index, and using the *lexicographic* order over the pair `(T, usize)`: 

    (a1, b1) <= (a2, b2)   when   (a1 < a2) or ((a1 == a2) and (b1 <= b2))

this ordering allows two updates to be naturally ordered by their `T` when distinct, but when equal we order times by the associated relation index. This allows us to "pretend" to perform updates in the order of relation index, to avoid under- and double-counting updates, without actually maintaining multiple independent indices.