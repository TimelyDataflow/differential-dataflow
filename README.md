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
	// imagine nodes and edges contain (node, dist) and (src, dst) pairs, respectively
    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());

        inner.join_map_u(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .group_u(|_, s, t| t.push((*s.peek().unwrap().0, 1)))
     })
}```



The [BFS example](https://github.com/frankmcsherry/differential-dataflow/blob/master/examples/bfs.rs) from the differential dataflow repository [wraps this up as a method](https://github.com/frankmcsherry/differential-dataflow/blob/master/examples/bfs.rs#L98-L115).

Once you've set up a differential dataflow graph, you are then able to start interacting with it by adding "differences" to the inputs. The example in the repository has inputs for the roots of the computation and the edges in the graph. We can repeatedly add and remove edges, for example, and differential dataflow magically updates the outputs of the computation!

## An example execution: breadth first search

Let's take the [BFS example](https://github.com/frankmcsherry/differential-dataflow/blob/master/examples/bfs.rs) from the differential dataflow repository. It takes four arguments: 

    cargo run --release --example bfs nodes edges batch inspect

where `nodes` and `edges` are numbers of nodes and edges in your random graph of choice, `batch` is "how many edges should we change at a time?" and `inspect` should be `inspect` if you want to see any output. Not observing the output may let it go a bit faster in a low-latency environment.

Let's try 1M nodes, 10M edges:

```
Echidnatron% cargo run --release --example bfs 10000000 100000000 1000 inspect
     Running `target/release/examples/bfs 10000000 100000000 1000 inspect`
performing BFS on 10000000 nodes, 100000000 edges:
loaded; elapsed: 4.963626955002837s
	(0, 1)
	(1, 9)
	(2, 84)
	(3, 881)
	(4, 8834)
	(5, 87849)
	(6, 833495)
	(7, 5128633)
	(8, 3917057)
	(9, 22710)
	(10, 10)
stable; elapsed: 29.791500767001708s
```

This tells us how many nodes in the graph are reachable at each distance (mostly distance `7` and `8` it seems), as well as how long it took to determine this (`29.79s` which isn't great). This improves with more threads, but it isn't the coolest part of differential dataflow.

### Concurrent incremental updates

As soon as the computation has produced the output above, we start introduce batches of `batch` edge changes at a time. We add that many and remove that many. In the example above, we are removing 1000 existing edges and adding 1000 new ones.

```
	(7, 1)
	(8, -2)
	(9, 1)
wave 0: avg 0.00012186273099359823
	(6, 4)
	(7, -2)
	(8, -3)
	(9, 1)
wave 1: avg 0.000010136429002159274
	(5, -3)
	(6, -25)
	(7, -45)
	(8, 70)
	(9, 3)
wave 2: avg 0.000016634063002129552
	(6, -3)
	(7, -4)
	(8, 7)
wave 3: avg 0.000010830363004060928
	(5, -1)
	(6, -6)
	(7, -43)
	(8, 49)
	(9, 1)
wave 4: avg 0.00001652919500338612
```

We are now seeing the changes to the node distances for each batch of 1000 changes, and the amount of time taken *divided by the batch size*. It's a pretty small time. The number of changes are also pretty small, which makes me wonder a bit. The dynamics of these sorts of experiments are often a bit weird. 

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

**Update**: There is a first attempt at this in the [`arrangement` branch](https://github.com/frankmcsherry/differential-dataflow/tree/arrangement), which also does the "Re-using storage" thing below. At the moment, it ICEs Rust stable and nightly (in different ways) and may be more horrible than is worth it. 

### Re-using storage

It is not uncommon for the same set of `(key, val)` tuples to be used by multiple operators. At the moment each operator maintains its own indexed copy of the tuples. This is pretty clearly wasteful, both in terms of memory and computation. However, sharing the state is a bit complicated, because it interacts weirdly with dataflow semantics. It seems like it could be done, in the sense that there are no data races or weird sharing that we have to worry about, so much as how to communicate the correct information.

**Update**: There is a first attempt at this in the [`arrangement` branch](https://github.com/frankmcsherry/differential-dataflow/tree/arrangement), which also does the "Generic storage" thing above. At the moment, it ICEs Rust stable and nightly (in different ways) and may be more horrible than is worth it. 
### Half-joins

There is the potential to implement multiple joins in a manner like that of Koch et al, where the join is logically differentiated with respect to each of its inputs, and each form is instantiated to respond to changes in the corresponding input. This seems very pleasant, and avoids materializing (and storing) intermediate data, but seems to require a new operator, like a join but which only responds to changes on one of its inputs.
