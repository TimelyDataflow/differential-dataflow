# Differential Dataflow
An implementation of [differential dataflow](./differentialdataflow.pdf) over [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) on [Rust](http://www.rust-lang.org).

## Background

Differential dataflow is a data-parallel programming framework designed to efficiently process large volumes of data and to quickly respond to arbitrary changes in input collections. 

Differential dataflow programs are written as functional transformations of collections of data, using familiar operators like `map`, `filter`, `join`, and `group`. Differential dataflow also includes more exotic operators such as `iterate`, which repeatedly applies a differential dataflow fragment to a collection. The programs are compiled down to [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) computations.

For example, here is a differential dataflow fragment to compute the out-degree distribution of a directed graph (for each degree, the number of nodes with that many outgoing edges):

```rust
let out_degr_dist =
edges.map(|(src, _dst)| src)    // extract source
     .count();                  // count occurrences of source
     .map(|(_src, deg)| deg)    // extract degree
     .count();                  // count occurrences of degree
```

Alternately, here is a fragment that computes the set of nodes reachable from a set `roots` of starting nodes:

```rust
let reachable =
roots.iterate(|reach|
    edges.enter(&reach.scope())
         .semijoin(reach)
         .map(|(src, dst)| dst)
         .concat(reach)
         .distinct()
)
```

Once written, a differential dataflow responds to arbitrary changes to its initially empty input collections, reporting the corresponding changes to each of its output collections. Differential dataflow can react quickly because it only acts where changes in collections occur, and does no work elsewhere. 

In the examples above, we can add to and remove from `edges`, dynamically altering the graph, and get immediate feedback on how the results change. We could also add to and remove from `roots` altering the reachability query itself.

Be sure to check out the [differential dataflow documentation](http://www.frankmcsherry.org/differential-dataflow/differential_dataflow/index.html), which is continually improving.

## An example: counting degrees in a graph.

A graph is a collection of pairs `(Node, Node)`, and one standard analysis is to determine the number of times each `Node` occurs in the first position, its "degree". The number of nodes with each degree is a helpful graph statistic.

```rust
// create a a degree counting differential dataflow
let (mut input, probe) = worker.dataflow(|scope| {

    // create edge input, count a few ways.
    let (input, edges) = scope.new_collection();

    // pull off source and count them.
    let degrs = edges.map(|(src, _dst)| src)
                     .count();

    // pull off degree and count those.
    let distr = degrs.map(|(_src, deg)| deg)
                     .count();

    // show us something about the collection, notice when done.
    let probe = distr.inspect(|x| println!("observed: {:?}", x))
                     .probe();

    (input, probe)
});
```

If we feed this with some random graph data, say fifty random edges among ten nodes, we get output like

    Running `target/release/examples/degrees 10 50 1`
    observed: ((5, 4), (Root, 0), 1)
    observed: ((4, 2), (Root, 0), 1)
    observed: ((7, 1), (Root, 0), 1)
    observed: ((6, 2), (Root, 0), 1)
    observed: ((3, 1), (Root, 0), 1)
    Loading finished after 312139

This shows us the records that passed the `inspect` operator, revealing the contents of the collection: there are five distinct degrees, three through seven in some order. The records have the form `((degree, count), time, delta)` where the `time` field says this is the first round of data, and the `delta` field tells us that each record is coming into existence. If the corresponding record were departing the collection, it would be a negative number.

Let's update the input by removing one edge and adding a new random edge:

    observed: ((7, 1), (Root, 1), -1)
    observed: ((8, 1), (Root, 1), 1)
    observed: ((3, 1), (Root, 1), -1)
    observed: ((2, 1), (Root, 1), 1)
    worker 0, round 1 finished after Duration { secs: 0, nanos: 139268 }

We see here some changes! Those degree three and seven nodes have been replaced by degree two and eight nodes; looks like one lost an edge and gave it to the other!

How about a few more changes?

    worker 0, round 2 finished after Duration { secs: 0, nanos: 81286 }
    worker 0, round 3 finished after Duration { secs: 0, nanos: 73066 }
    worker 0, round 4 finished after Duration { secs: 0, nanos: 64023 }
    observed: ((5, 3), (Root, 5), 1)
    observed: ((5, 4), (Root, 5), -1)
    observed: ((7, 1), (Root, 5), 1)
    observed: ((6, 2), (Root, 5), -1)
    observed: ((6, 3), (Root, 5), 1)
    observed: ((8, 1), (Root, 5), -1)
    worker 0, round 5 finished after Duration { secs: 0, nanos: 104017 }

Well a few weird things happen here. First, rounds 2, 3, and 4 don't print anything. Seriously? It turns out that the random changes we made didn't affect any of the degree counts, we moved edges between nodes, preserving degrees. It can happen. 

The second weird thing is that with only two edge changes we have six changes in the output! It turns out we can have up to eight. The eight gets turned back into a seven, and a five gets turned into a six. But: going from five to six changes the count for each, and each change requires two record differences.

### Scaling up

The appealing thing about differential dataflow is that it only does work where changes occur, so even if there is a lot of data, if not much changes it can still go quite fast. Let's scale our 10 nodes and 50 edges up by a factor of one million:

    Running `target/release/examples/degrees 10000000 50000000 1`
    observed: ((1, 336908), (Root, 0), 1)
    observed: ((2, 843854), (Root, 0), 1)
    observed: ((3, 1404462), (Root, 0), 1)
    observed: ((4, 1751921), (Root, 0), 1)
    observed: ((5, 1757099), (Root, 0), 1)
    observed: ((6, 1459805), (Root, 0), 1)
    observed: ((7, 1042894), (Root, 0), 1)
    observed: ((8, 653178), (Root, 0), 1)
    observed: ((9, 363983), (Root, 0), 1)
    observed: ((10, 181423), (Root, 0), 1)
    observed: ((11, 82478), (Root, 0), 1)
    observed: ((12, 34407), (Root, 0), 1)
    observed: ((13, 13216), (Root, 0), 1)
    observed: ((14, 4842), (Root, 0), 1)
    observed: ((15, 1561), (Root, 0), 1)
    observed: ((16, 483), (Root, 0), 1)
    observed: ((17, 143), (Root, 0), 1)
    observed: ((18, 38), (Root, 0), 1)
    observed: ((19, 8), (Root, 0), 1)
    observed: ((20, 3), (Root, 0), 1)
    observed: ((22, 1), (Root, 0), 1)
    Loading finished after Duration { secs: 15, nanos: 485478768 }

There are a lot more distinct degrees here. I sorted them because it was too painful to look at the unsorted data. You would normally get to see the output unsorted, because they are just changes values in a collection.

Let's perform a single change again.

    observed: ((5, 1757098), (Root, 1), 1)
    observed: ((5, 1757099), (Root, 1), -1)
    observed: ((7, 1042893), (Root, 1), 1)
    observed: ((7, 1042894), (Root, 1), -1)
    observed: ((6, 1459805), (Root, 1), -1)
    observed: ((6, 1459807), (Root, 1), 1)
    worker 0, round 1 finished after Duration { secs: 0, nanos: 119917 }

Although the initial computation took about fifteen seconds, we get our changes in about 141 microseconds; that's about one hundred thousand times faster than re-running the computation. That's pretty nice. Actually, it is small enough that the time to print things to the screen is a bit expensive, so let's comment that part out.

```rust
    // show us something about the collection, notice when done.
    let probe = distr//.inspect(|x| println!("observed: {:?}", x))
                     .probe().0;
```

Now we can just watch as changes roll past and look at the times.

         Running `target/release/examples/degrees 10000000 50000000 1`
    Loading finished after Duration { secs: 15, nanos: 632757755 }
    worker 0, round 1 finished after Duration { secs: 0, nanos: 141124 }
    worker 0, round 2 finished after Duration { secs: 0, nanos: 105730 }
    worker 0, round 3 finished after Duration { secs: 0, nanos: 106172 }
    worker 0, round 4 finished after Duration { secs: 0, nanos: 168487 }
    worker 0, round 5 finished after Duration { secs: 0, nanos: 62194 }
    ...

Nice. This is some hundreds of microseconds per update, which means maybe ten thousand updates per second. It's not a horrible number for my laptop, but it isn't the right answer yet.

### Scaling .. "along"?

Differential dataflow is designed for throughput in addition to latency. We can increase the number of rounds of updates it works on concurrently, which can increase its effective throughput. This does not change the output of the computation, except that we see larger batches of output changes at once.

Notice that those times above are a few hundred microseconds for each single update. If we work on ten rounds of updates at once, we get times that look like this:

         Running `target/release/examples/degrees 10000000 50000000 10`
    Loading finished after Duration { secs: 15, nanos: 665081016 }
    worker 0, round 10 finished after Duration { secs: 0, nanos: 321328 }
    worker 0, round 20 finished after Duration { secs: 0, nanos: 220047 }
    worker 0, round 30 finished after Duration { secs: 0, nanos: 234313 }
    worker 0, round 40 finished after Duration { secs: 0, nanos: 311963 }
    worker 0, round 50 finished after Duration { secs: 0, nanos: 188093 }
    ...

This is appealing in that we finish the first 10 updates after 321 microseconds, whereas after the same amount of time we haven't finished the third update. The cost is that we learn about the first and second output updates later than we would have if we had fed input updates in one-by-one, but these are the only two updates for which that is the case.

As we turn up the batching, performance improves. Here we work on one hundred rounds of updates at once:

         Running `target/release/examples/degrees 10000000 50000000 100`
    Loading finished after Duration { secs: 15, nanos: 612717043 }
    worker 0, round 100 finished after Duration { secs: 0, nanos: 1237368 }
    worker 0, round 200 finished after Duration { secs: 0, nanos: 1184419 }
    worker 0, round 300 finished after Duration { secs: 0, nanos: 1169537 }
    worker 0, round 400 finished after Duration { secs: 0, nanos: 1275858 }
    worker 0, round 500 finished after Duration { secs: 0, nanos: 1187122 }
    ...

This now averages to about twelve microseconds for each update, which is getting closer to one hundred thousand updates per second. 

Actually, let's just try that. Here are the numbers for one hundred thousand rounds of updates at a time:

         Running `target/release/examples/degrees 10000000 50000000 100000`
    Loading finished after Duration { secs: 15, nanos: 554954141 }
    worker 0, round 100000 finished after Duration { secs: 0, nanos: 529935399 }
    worker 0, round 200000 finished after Duration { secs: 0, nanos: 580862749 }
    worker 0, round 300000 finished after Duration { secs: 0, nanos: 537156939 }
    worker 0, round 400000 finished after Duration { secs: 0, nanos: 709743753 }
    worker 0, round 500000 finished after Duration { secs: 0, nanos: 546691982 }
    ...

This averages to about five or six microseconds per round of update, and now that I think about it each update was actually two changes, wasn't it. Good for you, differential dataflow!

### Scaling out

Differential dataflow is built on top of [timely dataflow](https://github.com/frankmcsherry/timely-dataflow), a distributed data-parallel runtime. Timely dataflow scales out to multiple independent workers, increasing the capacity of the system (at the cost of some coordination that cuts into latency).

If we bring two workers to bear, our 10 million node, 50 million edge computation drops down from fiveteen seconds to just over eight seconds.

         Running `target/release/examples/degrees 10000000 50000000 1 -w2`
    Loading finished after Duration { secs: 8, nanos: 207375094 }
    worker 0, round 1 finished after Duration { secs: 0, nanos: 119399 }
    worker 1, round 1 finished after Duration { secs: 0, nanos: 241527 }
    worker 0, round 2 finished after Duration { secs: 0, nanos: 222533 }
    worker 1, round 2 finished after Duration { secs: 0, nanos: 154681 }
    ...

That is a so-so reduction. You might notice that the times *increased* for the subsequent rounds. It turns out that multiple workers just get in each other's way when there isn't much work to do. 

Fortunately, as we work on more and more rounds of updates at the same time, the benefit of multiple workers increases. Here are the numbers for ten rounds at a time:

         Running `target/release/examples/degrees 10000000 50000000 10 -w2`
    Loading finished after Duration { secs: 8, nanos: 575313222 }
    worker 0, round 10 finished after Duration { secs: 0, nanos: 267152 }
    worker 1, round 10 finished after Duration { secs: 0, nanos: 325377 }
    worker 1, round 20 finished after Duration { secs: 0, nanos: 201332 }
    worker 0, round 20 finished after Duration { secs: 0, nanos: 252631 }
    ...

One hundred rounds at a time:

         Running `target/release/examples/degrees 10000000 50000000 100 -w2`
    Loading finished after Duration { secs: 8, nanos: 877454322 }
    worker 0, round 100 finished after Duration { secs: 0, nanos: 764008 }
    worker 1, round 100 finished after Duration { secs: 0, nanos: 810008 }
    worker 0, round 200 finished after Duration { secs: 0, nanos: 696741 }
    worker 1, round 200 finished after Duration { secs: 0, nanos: 730942 }
    ...

One hundred thousand rounds at a time:

    Running `target/release/examples/degrees 10000000 50000000 100000 -w2`
    Loading finished after Duration { secs: 8, nanos: 789885099 }
    worker 0, round 100000 finished after Duration { secs: 0, nanos: 322398063 }
    worker 1, round 100000 finished after Duration { secs: 0, nanos: 322534039 }
    worker 0, round 200000 finished after Duration { secs: 0, nanos: 340302572 }
    worker 1, round 200000 finished after Duration { secs: 0, nanos: 340617938 }
    ...

These last numbers were about half a second with one worker, and are decently improved with the second worker.

### Going even faster

There are several performance optimizations in differential dataflow designed to make the underlying operators as close to what you would expect to write, when possible. Additionally, by building on timely dataflow, you can drop in your own implementations a la carte where you know best.

For example, we know that all of the keys involved in these two `count` methods are unsigned integers. We can use a special form of `count` called `count_u`, which just uses the values themselves to distribute and sort the records, rather than repeatedly hashing them.

         Running `target/release/examples/degrees 10000000 50000000 10 -w2`
    Loading finished after Duration { secs: 7, nanos: 462765111 }
    worker 0, round 10 finished after Duration { secs: 0, nanos: 284091 }
    worker 1, round 10 finished after Duration { secs: 0, nanos: 438691 }
    worker 0, round 20 finished after Duration { secs: 0, nanos: 257730 }
    worker 1, round 20 finished after Duration { secs: 0, nanos: 202014 }
    ...

         Running `target/release/examples/degrees 10000000 50000000 100 -w2`
    Loading finished after Duration { secs: 7, nanos: 359640948 }
    worker 0, round 100 finished after Duration { secs: 0, nanos: 726242 }
    worker 1, round 100 finished after Duration { secs: 0, nanos: 742154 }
    worker 1, round 200 finished after Duration { secs: 0, nanos: 555628 }
    worker 0, round 200 finished after Duration { secs: 0, nanos: 580650 }
    ...

         Running `target/release/examples/degrees 10000000 50000000 100000 -w2`
    Loading finished after Duration { secs: 7, nanos: 357319064 }
    worker 0, round 100000 finished after Duration { secs: 0, nanos: 222533047 }
    worker 1, round 100000 finished after Duration { secs: 0, nanos: 223707807 }
    worker 0, round 200000 finished after Duration { secs: 0, nanos: 237145000 }
    worker 1, round 200000 finished after Duration { secs: 0, nanos: 237228387 }
    ...


For another example, we also know in this case that the underlying collections go through a *sequence* of changes, meaning their timestamps are totally ordered. In this case we can use a much simpler implementation, `count_total_u`. The reduces the update times substantially, for each batch size:

         Running `target/release/examples/degrees 10000000 50000000 10 -w2`
    Loading finished after Duration { secs: 5, nanos: 866946585 }
    worker 1, round 10 finished after Duration { secs: 0, nanos: 149072 }
    worker 0, round 10 finished after Duration { secs: 0, nanos: 93686 }
    worker 0, round 20 finished after Duration { secs: 0, nanos: 134797 }
    worker 1, round 20 finished after Duration { secs: 0, nanos: 144141 }
    ...

         Running `target/release/examples/degrees 10000000 50000000 100 -w2`
    Loading finished after Duration { secs: 5, nanos: 927197087 }
    worker 0, round 100 finished after Duration { secs: 0, nanos: 382786 }
    worker 1, round 100 finished after Duration { secs: 0, nanos: 430597 }
    worker 1, round 200 finished after Duration { secs: 0, nanos: 436174 }
    worker 0, round 200 finished after Duration { secs: 0, nanos: 449252 }
    ...

         Running `target/release/examples/degrees 10000000 50000000 100000 -w2`
    Loading finished after Duration { secs: 5, nanos: 923657540 }
    worker 0, round 100000 finished after Duration { secs: 0, nanos: 111166331 }
    worker 1, round 100000 finished after Duration { secs: 0, nanos: 111733061 }
    worker 0, round 200000 finished after Duration { secs: 0, nanos: 114145529 }
    worker 1, round 200000 finished after Duration { secs: 0, nanos: 115295166 }
    ...

These times have now dropped quite a bit from where we started; we now absorb almost one million rounds of updates per second, and produce correct (not just consistent) answers even while distributed across multiple workers.

## A second example: k-core computation

The k-core of a graph is the largest subset of its edges so that all vertices with any incident edges have degree at least k. One way to find the k-core is to repeatedly delete all edges incident on vertices with degree less than k. Those edges going away might lower the degrees of other vertices, so we need to *iteratively* throwing away edges on vertices with degree less than k until we stop. Maybe we throw away all the edges, maybe we stop with some left over.

Here is a direct implementation, in which we repeatedly take determine the set of active nodes (those with at least 
`k` edges point to or from them), and restrict the set `edges` to those with both `src` and `dst` present in `active`.

```rust
let k = 5;

// iteratively thin edges.
edges.iterate(|inner| {

    // determine the active vertices        /-- this is a lie --\
    let active = inner.flat_map(|(src,dst)| [src,dst].into_iter())
                      .map(|node| (node, ()))
                      .group(|_node, s, t| if s[0].1 > k { t.push(((), 1)); })
                      .map(|(node,_)| node);

    // keep edges between active vertices
    edges.enter(&inner.scope())
         .semijoin(active)
         .map(|(src,dst)| (dst,src))
         .semijoin(active)
         .map(|(dst,src)| (src,dst))
});
```

To be totally clear, the syntax with `into_iter()` doesn't work, because Rust, and instead there is a more horrible syntax needed to get a non-heap allocated iterator over two elements. But, it works, and

    Running `target/release/examples/degrees 10000000 50000000 1 5 kcore1`
    Loading finished after 72204416910

Well that is a thing. Who knows if 72 seconds is any good? (*ed:* it is worse than the numbers in the previous version of this readme).

The amazing thing, though is what happens next:

    worker 0, round 1 finished after Duration { secs: 0, nanos: 567171 }
    worker 0, round 2 finished after Duration { secs: 0, nanos: 449687 }
    worker 0, round 3 finished after Duration { secs: 0, nanos: 467143 }
    worker 0, round 4 finished after Duration { secs: 0, nanos: 480019 }
    worker 0, round 5 finished after Duration { secs: 0, nanos: 404831 }

We are taking about half a millisecond to *update* the k-core computation. Each edge addition and deletion could cause other edges to drop out of or more confusingly *return* to the k-core, and differential dataflow is correctly updating all of that for you. And it is doing it in sub-millisecond timescales.

If we crank the batching up by one thousand, we improve the throughput a fair bit:

    Running `target/release/examples/degrees 10000000 50000000 1000 5 kcore1`
    Loading finished after Duration { secs: 73, nanos: 507094824 }
    worker 0, round 1000 finished after Duration { secs: 0, nanos: 55649900 }
    worker 0, round 2000 finished after Duration { secs: 0, nanos: 51793416 }
    worker 0, round 3000 finished after Duration { secs: 0, nanos: 57733231 }
    worker 0, round 4000 finished after Duration { secs: 0, nanos: 50438934 }
    worker 0, round 5000 finished after Duration { secs: 0, nanos: 55020469 }

Each batch is doing one thousand rounds of updates in just over 50 milliseconds, averaging out to about 50 microseconds for each update, and corresponding to roughly 20,000 distinct updates per second.

I think this is all great, both that it works at all and that it even seems to work pretty well.

## Roadmap

There are several interesting things still to do with differential dataflow. Here is the roadmap of what I am hoping to do (though, holler if you'd like to stick your nose in and help out, or ever just comment):

### Compacting traces

As computation proceeds, some older times become indistinguishable. Once we hit round 1,000, we don't really care about the difference between updates at round 500 versus round 600; all updates before round 1,000 are "done". Updates at indistinguishable times can be merged, which is an important part of differential dataflow running forever and ever.

The underlying data stores are now able to compact their representations when provided with guarantees about which future times will be encountered. This results in stable incremental update times even after millions of batches of rounds of updates.

This compaction works across shared use of the traces, in that multiple operators can use the same state and provide different opinions about which future times will be encountered. The trace implementation is bright enough to track the lower bound of these opinions, and compact conservatively to continue to permit sharing of resources.

**MERGED**

### Fine-grained timestamps

Differential dataflow used to strongly rely on the fact that all times in a batch of updates would be identical. This introduces substantial overheads for fine-grained updates, where there are perhaps just a few updates with each timestamp; each must be put in its own batch, and introduce coordination overhead for the underlying timely dataflow runtime.

The underlying streams of updates are now able to be batched arbitrarily, with timestamps promoted to data fields. Differential dataflow can now batch updates at different times, and the operator implementations can retire time *intervals* rather than individual times, often with improved performance.

There is now some complexity in the operator implementations, each of which emulates "sequential playback" of the updates on a key-by-key basis. The intent is that they should do no more work than one would do if updates were provided one at a time, but without the associated system overhead.

**MERGED**

### Fault-tolerance

The functional nature of differential dataflow computations means that it can commit to output updates for various times once they are produced, because as long as the input are also committed, the computation should always produce the same output. Internally, differential dataflow stores data as indexed collections of immutable lists, and each list is self-describing: each indicates an interval of logical time and contains exactly the updates in that interval.

The internal data stores are sufficient to quickly bring a differential dataflow computation back to life if failed or otherwise shut down. The immutable nature makes them well suited for persisting to disk and replicating as appropriate. It seems reasonable to investigate how much or little extra is required to quickly recover a stopped differential dataflow computation from persisted versions of its immutable collections.

There is much more to do to have "serious" fault-tolerance,  but this is an interesting first step to explore.

### Other issues

The [issue tracker](https://github.com/frankmcsherry/differential-dataflow/issues) has several open issues relating to current performance defects or missing features. If you are interested in contributing, that would be great! If you have other questions, don't hesitate to get in touch.

## Acknowledgements

In addition to contributions to this repository, differential dataflow is based on work at the now defunct Microsoft Research lab in Silicon Valley. Collaborators have included: Martin Abadi, Paul Barham, Rebecca Isaacs, Michael Isard, Derek Murray, and Gordon Plotkin. Work on this project continued at the Systems Group of ETH Zürich, and was informed by discussions with Zaheer Chothia, Andrea Lattuada, John Liagouris, and Darko Makreshanski.
