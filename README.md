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
	let degrs = edges.as_collection()
					 .map(|(src,_dst)| src)
					 .count();

	// pull off count and count those.
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

The k-core of a graph is the largest subset of its edges so that all vertices with any edges have degree at least k. The way you find the k-core is to throw away all edges incident on vertices with degree less than k. Those edges going away might lower some other degrees, so we need to *keep* throwing away edges on vertices with degree less than k until we stop. Maybe we throw away all the edges, maybe we stop with some left over.

```rust
let k = 5;
edges.iterate(|inner| {

	// determine the active vertices     // /-- this is a lie --\
	let active = inner.flat_map(|(src,dst)| [src,dst].into_iter())
					  .threshold_u(|cnt| if cnt >= k { 1 } else { 0 });

	// keep edges between active vertices
	edges.semijoin_u(active)
		 .map(|(src,dst)| (dst,src))
		 .semijoin_u(active)
		 .map(|(dst,src)| (src,dst))
});
```

To be totally clear, the syntax with `into_iter()` doesn't work, because Rust, and instead there is a more horrible syntax needed to get a non-heap allocated iterator over two elements. But, it works, and

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
	...
	observed: ((12, 926000), -1)
	Round 1 finished after Duration { secs: 0, nanos: 301988072 }
	observed: ((6, 659692), -1)
	...
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

There are several interesting things still to do with differential dataflow. Here is the roadmap of what I am hoping to do (though, holler if you'd like to stick your nose in and help out, or ever just comment):

### Compacting traces

Naiad's implementation of differential dataflow is capable of compacting traces so that as frontiers progress, indistinguishable differences are merged and possibly cancelled. Timely dataflow presents enough information to do this, and actually provides more information that allows independent compaction for bi-linear operators like `join`, which Naiad's progress tracking doesn't support.

The current `Trace` implementations are ill-suited for this, and should probably be replaced as part of a larger overhaul. For example, trace implementations (of which there can be many) will surely need to improve to support fine-grained timestamps (the next item on the list).

Independently, the `arrange` operator permits sharing of `Trace` implementations, and this sharing must gate the state compaction. This isn't complicated, but it does mean that the current `Rc` sharing will need to be upgraded to reference counted frontiers (e.g. `MutableAntichain`), so that the trace manager can understand up to what frontiers is it safe to compact the data.

### Fine-grained timestamps

Data are currently sent in batches with a single timestamp, as this is how timely dataflow current manages its messages. However, many streams of data have very fine grained updates with individual changes each having their own time. These two are in conflict, and don't need to be.

Differential could process streams of `(val, time, wgt)` where `time` is promoted to data, as long as each batch has a timestamp less or equal to the times it contains. This would permit larger numbers of changes in each batch, and each batch could potentially be retired at once (rather than serializing the work). This has the potential to increase throughput substantially (as more work gets done at once) and improve scaling as workers need to coordinate less often.

The "cost" here is mostly in complexity: differential operators would need to understand how to process collections of changes at various times and should probably avoid using time-keyed maps to store the differences before they are processed. The core `Trace` implementations would likely need to be improved to not have as much per-time overhead (perhaps flattening the `(key, time, val, wgt)` tuples into a trie, as prototyped elsewhere). Such designs and implementations would likely be sub-optimal for batch processing, but it feels like in this direction lies progress.

### Fault-tolerance

This is mostly an idle diversion. The functional nature of differential dataflow should mean that it can commit updates for various times once they are confirmed correct, and upon recovery any identified updates can simply be loaded rather than waited upon. For example, trie-based `Trace` implementations represent a collection of updates in a range of times as a flat array, which should be fairly easily serialized and stored.

As part of recovering, we might imagine that operators will be called upon to reproduce a subset of their output diffs, by time (because downstream operators didn't commit everything). The `group` operator is already designed to produce output diffs by time on demand, but operators like `join` are not so designed (in short, `join` can't afford to record its outputs, which makes it harder to see what has changed at a time from the time alone).

It would be interesting to prototype serializing this state to stable storage, and finding and loading it on start-up. There is much more to do to have "serious" fault-tolerance, but this would at least be interesting to explore.

## Acknowledgements

In addition to contributions to this repository, differential dataflow is based on work at the now defunct Microsoft Research lab in Silicon Valley. Collaborators have included: Martin Abadi, Paul Barham, Rebecca Isaacs, Michael Isard, Derek Murray, and Gordon Plotkin. Work on this project continued at the Systems Group of ETH Zürich, and was informed by discussions with Zaheer Chothia, Andrea Lattuada, John Liagouris, and Darko Makreshanski.
