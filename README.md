# Differential Dataflow
An implementation of [differential dataflow](http://www.cidrdb.org/cidr2013/Papers/CIDR13_Paper111.pdf) using [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) on [Rust](http://www.rust-lang.org).

## Background

Differential dataflow is a data-parallel programming framework designed to efficiently process large volumes of data and to quickly respond to changes in input collections.

Like many other data-parallel platforms, differential dataflow supports a variety of data-parallel operators such as `group_by` and `join`. Unlike most other data-parallel platforms, differential dataflow also includes an `iterate` operator, which repeatedly applies a differential dataflow subcomputation to a collection until it converges.

Once you have written a differential dataflow program, you then update the input collections and the implementation will respond with the correct updates to the output collections. These updates (both input and output) have the form `(data, diff)`, where `data` is a typed record and `diff` is an integer indicating the change in the number of occurrences of `data`. A positive `diff` indicates more occurrences of `data` and a negative `diff` indicates fewer. If things are working correctly, you never see a zero `diff`.

Differential dataflow is efficient because it communicates *only* in terms of differences. At its core is a computational engine which is also based on differences, and which does no work that does not correspond to a change in the trace of the computation as a result of changes to the inputs. Achieving this property in the presence of iterative subcomputations is the main "unique" feature of differential dataflow.

## An example program:  breadth first search

Consider the graph problem of determining the distance from a set of root nodes to each other node in the graph.

One way to approach this problem is to develop a set of known minimal distances to nodes, perhaps starting from the set of roots at distance zero, and repeatedly expanding the set using the graph's edges.

Each known minimal distance (to some node) can be joined with the set of edges emanating from the node, resulting in proposals for distances to other nodes. To collect these into a set of minimal distances, we can group them by the node identifier and retain only the minimal distance.

The program to do this in differential dataflow follows exactly this pattern. Although there is a bit of syntactic guff, and there is no reason you should expect to understand the arguments of the various methods at this point, the above algorithm looks like:

```rust
let (e_in, edges) = dataflow.new_input::<((u32, u32), i32)>();
let (r_in, roots) = dataflow.new_input::<(u32, i32)>();

// initialize roots at distance 0
let start = roots.map(|(x, w)| ((x, 0), w));

// repeatedly update minimal distances to each node,
// by describing how to do one round of updates, and then repeating.
let limit = start.iterate(|dists| {

    // bring the invariant edges into the loop
    let edges = dists.builder().enter(&edges);

    // join current distances with edges to get +1 distances,
    // include the current distances in the set as well,
    // group by node id and keep minimum distance.
    dists.join_map_u(&edges, |_,d,n| (*n, d+1))
         .concat(&dists)
         .group_u(|_, s, t| t.push((*s.peek().unwrap().0, 1)) )
});
```

Having defined the computation, we can now modify the `edges` and `roots` input collections, using the `e_in` and `r_in` handles, respectively. We might start by adding all of our edges, and perhaps then exploring the set of roots. Or we might change the edges with the roots held fixed to see how the distances change for each edge.

In any case, the outputs will precisely track the computation applied to the inputs. It's just that easy!

## An example execution: breadth first search

Let's take the [BFS example](https://github.com/frankmcsherry/differential-dataflow/blob/master/examples/bfs.rs) from the differential dataflow repository. It constructs a random graph on 200M edges, performs the reachability computation defined above, starting from root nodes 0, 1, and 2, and reports the number of nodes at each distance.

The random number generator is seeded, so you should see the same output I see (up to timings):

```
Echidnatron% cargo run --release --example bfs
     Running `target/release/examples/bfs`
performing BFS on 100000000 nodes, 200000000 edges:
observed at (Root, 0):
elapsed: 264.0985786569945s
	(0, 3)
	(1, 8)
	(2, 21)
	(3, 52)
	(4, 99)
	(5, 199)
	(6, 357)
	(7, 711)
	(8, 1349)
	(9, 2721)
	(10, 5465)
	(11, 10902)
	(12, 21798)
	(13, 43391)
	(14, 86149)
	(15, 172251)
	(16, 342484)
	(17, 676404)
	(18, 1325526)
	(19, 2544386)
	(20, 4699159)
	(21, 8077892)
	(22, 12229347)
	(23, 15145774)
	(24, 14275975)
	(25, 10020299)
	(26, 5505139)
	(27, 2584731)
	(28, 1118823)
	(29, 467071)
	(30, 192398)
	(31, 78933)
	(32, 32175)
	(33, 13103)
	(34, 5162)
	(35, 2051)
	(36, 819)
	(37, 336)
	(38, 138)
	(39, 56)
	(40, 34)
	(41, 14)
	(42, 6)
	(43, 3)
	(44, 2)
```

The computation takes 264.1 seconds to get done, which is not a great (or even good) time for breadth first search on 200 million edges. The computation is single threaded, and the number would go down when more threads are brought to bear.

That isn't the cool part, though.

### Concurrent incremental updates

As soon as the computation started, it began to randomly add and remove edges, one addition and removal per second. The corresponding output updates depend on those of the initial computation, and so we can't report them until after reporting those of the first round. But, just after the first round of output emerges, we see outputs like:

```
observed at (Root, 3):
elapsed: 261.10050941698137s
	(23, 1)
	(24, 2)
	(25, -2)
	(27, -1)
observed at (Root, 4):
elapsed: 260.100518569001s
	(21, 1)
	(24, -1)
observed at (Root, 5):
elapsed: 259.1005240849918s
	(23, 1)
observed at (Root, 8):
elapsed: 256.1005283939885s
	(24, -1)
	(25, -1)
observed at (Root, 11):
elapsed: 253.109466711001s
	(25, 1)
	(26, -1)
```

Each of these outputs reports a change in the numbers of nodes at each distance from the roots. If accumulated with prior counts (the initial counts above, plus any preceding changes), they are the correct counts for the number of nodes *now* at each distance from the roots.

These outputs corresponding to inputs introduced at seconds 3, 4, 5, 8, and 11 from the start of the computation, times at which we haven't even finished introducing the initial 200M randomly generated graph edges. As a consequence, these outputs have fairly large `elapsed` measurements.

However, you may notice that the times at which they are reported, their second of introduction plus the elapsed measurement, are quite tightly concentrated, at roughly 264.1 seconds. While these results are not available until after the outputs for the first epoch are complete, they do emerge almost immediately thereafter.

### Streaming incremental updates

If we scan through the output until epoch 265, the first one not blocked by the initial 264.1s of work, we see that its elapsed time is quite small. As are those that follow it:

```
observed at (Root, 265):
elapsed: 0.0018802949925884604s
	(25, -1)
	(26, -1)
	(27, -1)
observed at (Root, 268):
elapsed: 0.0018262710073031485s
	(24, 1)
observed at (Root, 273):
elapsed: 0.0018914630054496229s
	(25, -1)
observed at (Root, 274):
elapsed: 0.00016224500723183155s
	(22, -1)
	(24, 1)
	(29, 1)
...
```
The `elapsed` measurements from this point on are single-digit milliseconds, or less.

## Data parallelism

Differential dataflow is built on [timely dataflow](https://github.com/frankmcsherry/timely-dataflow), a distributed data-parallel dataflow platform. Consequently, it distributes over multiple threads, processes, and computers. The additional resources allow larger computations to be processed more efficiently, but may limit the minimal latency for small updates, as more computations must coordinate.

## To-do list

There are a few things that stand out as good, meaty bits of work to do. If you are interested, let me know. Otherwise I'll take a stab at them.

### Garbage collection

The current store for differential dataflow tuples is logically equivalent to an append-only list. It only grows. In the Naiad work we had some garbage collection, because you can use progress information to form equivalance classes of "indistinguishable" times; ones that would either all be used in an aggregation on none of which would be used. Differences at indistinguishable times can be consolidated, and cancelled records can be discarded.

There are [the beginnings of a garbage-collecting trace](https://github.com/frankmcsherry/differential-dataflow/blob/master/src/collection/tier.rs), but putting this in place involves some more thinking about how to progressively stage the traces, to avoid constantly recollecting. The trace has a `step` method to progressively collect, but it's all very un-tested at this point.

### Generic storage

A "trace", a map from keys to times to vals to weights, can have many representations. The most general representation requires a lot more structure than some of the simpler ones. Examples of traces with simpler representations include:

* Traces containing differences for only one time.
* Traces containing only positive differences (where the values can just be listed, without weights).
* Traces containing differences where the value has type `()`.

In each of these cases, and many weird combinations of them, there are substantial gains to be had by specialization. For example, it is very common to have a large static (or slowly changing) collection of background data. This could easily be represented as a one-level trie, where we have a list of `(key, count)` and a list of `vals`. This has very little overhead, and is very efficient to navigate. Changes to this collection could be maintained in a general representation, separately.

Given that no one of these representations are sufficiently general and concise, it seems appealing to describe them generically with a trait, and allow implementations to override the representation (perhaps defaulting to the most general representation). We could take the methods from the current `Trace`, but some of them involve lifetimes in what appear to be higher-kinded fashions (mainly, we need to describe the lifetime of value references, unless we are ok cloning them).

There are some details about how to work around this in [Rust's associated items RFC](https://github.com/aturon/rfcs/blob/associated-items/active/0000-associated-items.md#encoding-higher-kinded-types), which would probably involve ripping up a bunch of things, and putting them back down differently.

### Re-using storage

It is not uncommon for the same set of `(key, val)` tuples to be used by multiple operators. At the moment each operator maintains its own indexed copy of the tuples. This is pretty clearly wasteful, both in terms of memory and computation. However, sharing the state is a bit complicated, because it interacts weirdly with dataflow semantics. It seems like it could be done, in the sense that there are no data races or weird sharing that we have to worry about, so much as how to communicate the correct information.

### Half-joins

There is the potential to implement multiple joins in a manner like that of Koch et al, where the join is logically differentiated with respect to each of its inputs, and each form is instantiated to respond to changes in the corresponding input. This seems very pleasant, and avoids materializing (and storing) intermediate data, but seems to require a new operator, like a join but which only responds to changes on one of its inputs.
