# Differential Dataflow
An implementation of [differential dataflow](http://www.cidrdb.org/cidr2013/Papers/CIDR13_Paper111.pdf) using [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) on [Rust](http://www.rust-lang.org).

## Background

Differential dataflow is a data-parallel programming framework designed to efficiently process large volumes of data and to quickly respond to arbitrary changes in input collections. 

Differential dataflow programs are written as transformations of collections of data, using familiar operators like `map`, `filter`, `join`, and `group`. Differential dataflow includes perhaps less familiar operators like `iterate`, which repeatedly applies a differential dataflow fragment to a collection. The programs are compiled down to timely dataflow computations.

Once written, a differential dataflow responds to arbitrary changes to its initially empty input collections, reporting the corresponding changes to each of its output collections. Differential dataflow can react quickly because it only acts where changes in collections occur, and does no work elsewhere.

## An example: counting degrees in a graph.

A graph is a collection of pairs `(Node, Node)`, and one standard analysis is to determine the number of times each `Node` occurs in the first position, its "degree". The number of nodes with each degree is a helpful graph statistic.

```rust
// create a a degree counting differential dataflow
let (mut input, probe) = computation.scoped(|scope| {

	// create edge input, count a few ways.
	let (input, edges) = scope.new_input();

	// pull off source and count them.
	let degrs = edges.as_collection()	 // /-- this is a lie --\
					 .flat_map(|(src, dst)| [src,dst].into_iter())
					 .count();

	// pull of count and count those.
    let distr = degrs.map(|(_, cnt)| cnt as u32)
					 .count();

	// show us something about the collection, notice when done.
	let probe = distr.inspect(|x| println!("observed: {:?}", x))
					 .probe().0;

    (input, probe)
});
```

If we feed this with some random graph data, say fifty random edges among ten nodes, we get output like

		Running `target/release/examples/degrees 10 50 1`
	observed: ((6, 2), 1)
	observed: ((5, 4), 1)
	observed: ((7, 1), 1)
	observed: ((4, 2), 1)
	observed: ((3, 1), 1)
	Loading finished after Duration { secs: 0, nanos: 122363 }

This shows us the records that passed the `inspect` operator, revealing the contents of the collection: there are five distinct degrees, three through seven. The records have the form `((degree, count), delta)` where the `delta` field tells us that each record is coming into existence. If it were leaving, it would be a negative number.

Let's update the input by removing one edge and adding a new random edge:

	observed: ((8, 1), 1)
	observed: ((7, 1), -1)
	observed: ((2, 1), 1)
	observed: ((3, 1), -1)
	Round 1 finished after Duration { secs: 0, nanos: 53880 }

We see here some changes! Those degree three and seven nodes have been replaced by degree two and eight nodes; looks like one lost an edge and gave it to the other!

How about a few more changes?

	Round 2 finished after Duration { secs: 0, nanos: 19312 }
	Round 3 finished after Duration { secs: 0, nanos: 16572 }
	Round 4 finished after Duration { secs: 0, nanos: 17950 }
	observed: ((6, 2), -1)
	observed: ((6, 3), 1)
	observed: ((5, 3), 1)
	observed: ((5, 4), -1)
	observed: ((8, 1), -1)
	observed: ((7, 1), 1)
	Round 5 finished after Duration { secs: 0, nanos: 37817 }

Well a few weird things happen here. First, rounds 2, 3, and 4 don't print anything. Seriously? It turns out that the random changes we made didn't affect any of the degree counts, we moved edges between nodes, preserving degrees. It can happen. 

The second weird thing is that with only two edge changes we have six changes in the output! It turns out we can have up to eight. The eight gets turned back into a seven, and a five gets turned into a six. But: going from five to six changes the count for each, and each change requires two record differences.

### Scaling up

The appealing thing about differential dataflow is that it only does work where things change, so even if there is a lot of data, if not much changes it goes quite fast. Let's scale our 10 nodes and 50 edges up by a factor of one million:

		Running `target/release/examples/degrees 10000000 50000000 1`
	...
	Loading finished after Duration { secs: 14, nanos: 887066000 }
	observed: ((6, 1459805), -1)
	observed: ((6, 1459807), 1)
	observed: ((5, 1757098), 1)
	observed: ((5, 1757099), -1)
	observed: ((7, 1042893), 1)
	observed: ((7, 1042894), -1)
	Round 1 finished after Duration { secs: 0, nanos: 251857841 }

After 15 seconds or so, we get our counts (which I elided because there are lots of them). Then it takes just 250ms to update these counts. This is faster than 15 seconds, but differential is actually still just getting warmed up (cleaning up after the previous round, mainly). If we keep watching, we see

	observed: ((6, 1459806), 1)
	observed: ((6, 1459807), -1)
	observed: ((5, 1757097), 1)
	observed: ((5, 1757098), -1)
	observed: ((7, 1042893), -1)
	observed: ((7, 1042894), 1)
	observed: ((4, 1751921), -1)
	observed: ((4, 1751922), 1)
	Round 2 finished after Duration { secs: 0, nanos: 71098 }

This is what the performance looks like for the remaining rounds: about 50-70 microseconds to update the counts, similar to the times on the 10 node 50 edge graph, which makes some sense because no matter how large the graph, randomly rewiring one edge can change at most four counts, corresponding to at most eight changes in the collection.

### Scaling out

Differential dataflow is built on top of [timely dataflow](https://github.com/frankmcsherry/timely-dataflow), a distributed data-parallel runtime. Timely dataflow scales out to multiple independent workers, increasing the capacity of the system (at the cost of some coordination that cuts into latency).

If we bring two workers to bear, the 10 million node, 50 million edge computation drops down from 14.887s to 

		Running `target/release/examples/degrees 10000000 50000000 1 -w2
	Loading finished after Duration { secs: 7, nanos: 323694414 }
	...
	Round 1 finished after Duration { secs: 0, nanos: 145373140 }
	...
	Round 2 finished after Duration { secs: 0, nanos: 171744 }
	...
	Round 3 finished after Duration { secs: 0, nanos: 119206 }

The initial computation got pretty much 2x faster! You might have noticed that the time has actually *increased* for the later rounds, which makes sense because there are only like at most eight records. We don't need two threads for that; they just get in the way. It's a trade-off. More workers make bulk work faster and increase the throughput, but cut in to the minimal latency.

### Other stuff

You might say: "15s is a long time to update 50 million counts." It's true, with 100ns RAM, you might expect to do 10 million updates per second (at least), since all you need to do is increment some random entry in an array (maybe four of them, for each endpoint that changed). 10 million updates per second would be 5 seconds for 50 million edges.

Fortunately, there are implementations of each operator when the keys are unsigned integers that just probe in arrays and do a light amount of logic (`count_u` in the case of count).

		Running `target/release/examples/degrees 10000000 50000000 1`
	...
	Loading finished after Duration { secs: 4, nanos: 768145153 }
	...
	Round 1 finished after Duration { secs: 0, nanos: 217417146 }
	...
	Round 2 finished after Duration { secs: 0, nanos: 49914 }

There you go, five seconds. Now, this is still slower than you would get with an array, because differential dataflow is leaving everything in a state where it can be efficiently updated. You might have thought an array does a just-fine job of this, so let's talk about something cooler.

## A second example: k-core computation

The k-core of a graph is the largest subset of its edges so that all vertices with an edges have degree at least k. This means that we need to throw away edges incident on vertices with degree less than k. Those edges going away might lower some other degrees, so we need to *keep* throwing away edges on vertices with degree less than k until we stop. Maybe we throw away all the edges, maybe we stop with some left over. 

```rust
let k = 5;
edges.iterate(|inner| {

	// determine which vertices remain
	let active = edges.flat_map(|(src,dst)| [src,dst].into_iter())
					  .threshold_u(|cnt| if cnt > k { 1 } else { 0 });

	// restrict edges to those pointing to active vertices
	edges.semijoin_u(active)
		 .map(|(src,dst)| (dst,src))
		 .semijoin_u(active)
		 .map(|(dst,src)| (src,dst))
});
```

To be totally clear, the syntax with `into_iter` doesn't work, because Rust, and instead there is a more horrible syntax needed to get a non-heap allocated iterator over two elements. But, it works, and

	     Running `target/release/examples/degrees 10000000 50000000 1 5`
	observed: ((5, 400565), 1)
	observed: ((6, 659693), 1)
	observed: ((7, 930734), 1)
	observed: ((8, 1152892), 1)
	...
	observed: ((30, 3), 1)
	Loading finished after Duration { secs: 31, nanos: 240040855 }

Well that is a thing. Who knows if 31 seconds is any good. Let's start messing around with the data!

	observed: ((6, 659692), 1)
	observed: ((6, 659693), -1)
	observed: ((7, 930734), -1)
	observed: ((7, 930735), 1)
	observed: ((8, 1152891), 1)
	observed: ((8, 1152892), -1)
	observed: ((9, 1263344), -1)
	observed: ((9, 1263346), 1)
	observed: ((10, 1250532), 1)
	observed: ((10, 1250533), -1)
	observed: ((11, 1122179), -1)
	observed: ((11, 1122180), 1)
	observed: ((12, 925999), 1)
	observed: ((12, 926000), -1)
	Round 1 finished after Duration { secs: 0, nanos: 301988072 }
	observed: ((6, 659692), -1)
	observed: ((6, 659693), 1)
	observed: ((7, 930734), 1)
	observed: ((7, 930735), -1)
	observed: ((9, 1263345), 1)
	observed: ((9, 1263346), -1)
	observed: ((10, 1250532), -1)
	observed: ((10, 1250534), 1)
	observed: ((11, 1122179), 1)
	observed: ((11, 1122180), -1)
	observed: ((13, 704217), 1)
	observed: ((13, 704218), -1)
	observed: ((14, 497956), -1)
	observed: ((14, 497957), 1)
	Round 2 finished after Duration { secs: 0, nanos: 401968 }

Ok, the 300 milliseconds to clean up some mess, and then we are down to about 400 microseconds to *update* the degree distribution of the k-core of our graph. And amazing, we have enough work now that adding a second worker improves things

    	Running `target/release/examples/degrees 10000000 50000000 1 5 -w2`
	Loading finished after Duration { secs: 16, nanos: 808444276 }
    ...
    Round 2 finished after Duration { secs: 0, nanos: 401113 }

Even round 2 goes faster! Like, a teensy bit. 

Of course, round 2 isn't why we used multiple workers. Multiple workers do increase the *throughput* of a computation, though. Let's see what happens if we adjust 1000 edges each iteration rather than just one.

	     Running `target/release/examples/degrees 10000000 50000000 1000 5`
	...
	Loading finished after Duration { secs: 30, nanos: 115939220 }
	...
	Round 1 finished after Duration { secs: 0, nanos: 316472394 }
	...
	Round 2 finished after Duration { secs: 0, nanos: 11244112 }

Looks like about 11 milliseconds for one worker to do 1,000 updates. That is much better than 1,000 x the 50-70 microsecond latency up above! Also, if we spin up a second worker, we see

	     Running `target/release/examples/degrees 10000000 50000000 1000 5 -w2`
	...
	Loading finished after Duration { secs: 17, nanos: 181791387 }
	...
	Round 1 finished after Duration { secs: 0, nanos: 198020474 }
	...
	Round 2 finished after Duration { secs: 0, nanos: 6525667 }

We cut the latency by almost a factor of two even for round 2. Thanks, second worker!

That's about 6.5ms to update 1,000 edges, which means we should be able to handle about 150,000 edge changes each second. And the number goes up if we batch more edges together, or use more workers!

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
