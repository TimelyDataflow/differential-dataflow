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

        // join dists with edges, keeps old dists, keep min.
        inner.join_map_u(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .group_u(|_, s, t| t.push((*s.peek().unwrap().0, 1)))
    })
```



The [BFS example](https://github.com/frankmcsherry/differential-dataflow/blob/master/examples/bfs.rs) from the differential dataflow repository [wraps this up as a method](https://github.com/frankmcsherry/differential-dataflow/blob/master/examples/bfs.rs#L102-L119).

Once you've set up a differential dataflow graph, you are then able to start interacting with it by adding "differences" to the inputs. The example in the repository has inputs for the roots of the computation and the edges in the graph. We can repeatedly add and remove edges, for example, and differential dataflow magically updates the outputs of the computation!

## An example execution: breadth first search

Let's take the [BFS example](https://github.com/frankmcsherry/differential-dataflow/blob/master/examples/bfs.rs) from the differential dataflow repository. It takes four arguments: 

    cargo run --release --example bfs -- nodes edges batch inspect

where `nodes` and `edges` are numbers of nodes and edges in your random graph of choice, `batch` is "how many edges should we change at a time?" and `inspect` should be `inspect` if you want to see any output. Not observing the output may let it go a bit faster in a low-latency environment.

Let's try 10M nodes, 100M edges:

```
Echidnatron% cargo run --release --example bfs -- 10000000 100000000 1000 inspect
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

### Incremental updates

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

Differential dataflow is built on [timely dataflow](https://github.com/frankmcsherry/timely-dataflow), a distributed data-parallel dataflow platform. Consequently, it distributes over multiple threads, processes, and computers. The additional resources increase the throughput as more workers contribute, but may reduce the minimal latency for small updates as more workers must coordinate.

## Roadmap

There are several bits of work in progress, and some next steps that remain to be done. I will describe them in an attempt to put them in some order.

### Arrangement as an operator

The main change underway in the arrangement branch is a new class of operators `arrange_by_*` that take various collections and key functions and group the data by the key function. Each differential dataflow operator (e.g. `group`, `join`) used to do this on their own, but for several reasons it seems that factoring this out could simplify our lives and improve performance.

Users of methods like `group` and `join` should see no (or, few) changes, but they will also be able to tap in to the arrangement infrastructure to do some interesting things:

#### Generic storage

The per-operator state in differential dataflow is morally equivalent to a collection of `(K, T, V, W)` for keys `K` timestamps `T` values `V` and integer weights `W`. However, specific operators may have structural constraints that do not require this full generality:

* Operators without values, for example set-theoretic operators like `distinct`, `union`, `intersect`, and `semijoin`, use the value type `()` which has zero size. This means that rather than providing for an offset into a list of possible `((), W)` pairs, we can instead simply keep the `W` value.

* Operators whose inputs are closed and so do not change with time will shut down and retain no state. However, some operators take multiple inputs, only some of which correspond to closed inputs that do not vary with time: a standard example is the join in a graph algorithm where the edges may be static while the node values evolve. In this case, there is no need for the complexity associated with multiple timestamps. There may be no need for negative weights either, nor weights at all if we simply record the values `V` that occur.

Other examples include storage that may have different strategies for sharing (to be discussed), compaction (to be discussed), and fault-tolerance (not to be discussed).

To support these, there is a `Trace` trait and many of the operators have been re-written to be generic in the type of trace. 

This change allows us to have one implementation of core `join` logic that can be used for `equijoin`, `semijoin`, and `intersect`, without having to sacrifice performance for the simpler instances. At least, we provide rustc and llvm with enough information to perform appropriate specialization; whether it helps a great deal or not is to be seen. Likewise, we should be able to have one implementation of core `group` logic, but an ICE in Rust currently prevents this (more on this as it evolves). 

#### Sharing

In many cases, the same set of data will be partitioned the same way multiple times. Examples include `(key, val)` relations that are joined with multiple datasets. It is a waste to have each instance of `join` perform identical work and spend memory maintaining identical collections. 

The `arrange_by_*` operators present their input data both as a stream, but also with a shared read-only reference `Rc<SharedTrace<K,T,V,W>>` (not its real name) that can be handed to multiple consumers of the data. The arrange operator will perform the arrangement once, and downstream operators can take notifications on the stream to determine which subset of times it has logically accepted.

The arrangements are generic with respect to an implementor of `Trace`, which may require some wrappers to allow the same stream to be used in different scopes (for example, the same set of edges in different graph subcomputations).

#### Compaction

The state maintained by current differential dataflow operators, and by current `Trace` implementors, is an append-only list of differences. This has good properties with respect to sharing, and sanity generally, but it does grow without bound as the computation proceeds. As each user of the trace must filter through prior differences associated with each key, computation times also increase with the number of differences.

As the computation proceeds, progress information can reveal that differences at some times are no longer possible. From this information, we can determine that some times in the logs are effectively indistinguishable: two times `t1` and `t2` may have the property that for all future times `t`, `t1 <= t iff t2 <= t`. In such a case, the two times can be coalesced, which can cause differences to collapse. In many common cases like windowed streams, the result is a bounded memory footprint.

Naiad's differential dataflow implementation performed compaction in place. Our current design wants to avoid this, but we can simply compact data into a second location and swap it in once we know it is safe to do so. Some archeaology must be done to recover the compaction logic (I opted not to exfiltrate the research when MSR SVC was shut down), and to check whether it is still correct for the more general setting here (Naiad had relatively more restricted timestamps).

Another detail is that sharing as described above makes reasoning about equivalent times more sensitive. As multiple readers may be tracking a given collection, we need to track the lower envelope of times they may need to differentiate between. This may require changing the simple reference-counted pointer to be something a bit more thoughtful (perhaps a RAII wrapper that advances a frontier, and abandons it on drop?). 

### Motivation

An excellent and interesting motivator for a lot of this work is a combination of the worst-case optimal join work of Ngo et al with the flattened stream join work of Koch et al. Taken together, and implemented in differential, they appear to provide incrementally updateable worst-case join computation with a memory footprint proportional to the size of the input relations (or their difference traces, if they are not static collections). This has the potential to be very exciting in the context of something like Datalog, where somewhat exotic joins are performed incrementally, as part of semi-naive evaluation. 

A successful implementation, experiment, and write-up for a Datalog-like join using these techinques would probably make for a pretty neat paper. 

## Acknowledgements

In addition to contributions to this repository, differential dataflow is based on work at the now defunct Microsoft Research lab in Silicon Valley. Collaborators have included: Martin Abadi, Paul Barham, Rebecca Isaacs, Michael Isard, Derek Murray, and Gordon Plotkin. Work on this project continued at the Systems Group of ETH Zürich, and was informed by discussions with Zaheer Chothia, Andrea Lattuada, John Liagouris, and Darko Makreshanski.
