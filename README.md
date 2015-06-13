# Differential Dataflow
An implementation of [differential dataflow](http://www.cidrdb.org/cidr2013/Papers/CIDR13_Paper111.pdf) using [timely dataflow](https://github.com/frankmcsherry/timely-dataflow) on [Rust](http://www.rust-lang.org).

## Background

Differential dataflow is a data-parallel programming framework designed to efficiently process large volumes of data and to quickly respond to changes in input collections.

Like many other data-parallel platforms, differential dataflow supports a variety of data-parallel operators such as `group_by` and `join`. Unlike most other data-parallel platforms, differential dataflow also includes an `iterate` operator, which repeatedly applies a differential dataflow subcomputation to a collection until it converges.

Once you have written a differential dataflow program, you then update the input collections and the implementation will respond with the correct updates to the output collections. These updates (both input and output) have the form `(data, diff)`, where `data` is a typed record and `diff` is an integer indicating the change in the number of occurrences of `data`. A positive `diff` indicates more occurrences of `data` and a negative `diff` indicates fewer.

Differential dataflow is efficient because it communicates *only* in terms of differences. At its core is a computational engine which is also based on differences, and which does no work that does not correspond to a change in the trace of the computation as a result of changes to the inputs. Achieving this property in the presence of iterative subcomputations is the main "unique" feature of differential dataflow.

## An example program:  breadth first search

Consider the graph problem of determining the distance from a set of root nodes to each other node in the graph.

One way to approach this problem is to develop a set of known minimal distances to nodes, perhaps starting from the set of roots at distance zero, and repeatedly expanding the set using the graph's edges.

Each known minimal distance (to some node) can be joined with the set of edges emanating from the node, resulting in proposals for distances to nodes. To collect these into a set of minimal distances, we can group them by the node identifier and retain only the minimal distance.

The program to do this in differential dataflow follows exactly this pattern. Although there is a bit of syntactic guff, and there is no reason you should expect to understand the arguments of the various methods at this point, the above algorithm looks like:

```rust
let edges = dataflow.new_input::<((u32, u32), i32)>();
let roots = dataflow.new_input::<(u32, i32)>();

// initialize roots at distance 0
let start = roots.map(|(x, w)| ((x, 0), w));

// repeatedly update minimal distances to each node,
// by describing how to do one round of updates, and then repeating.
let limit = start.iterate(u32::max_value(), |x| x.0, |x| x.0, |dists| {

    // bring the invariant edges into the loop
    let edges = dists.builder().enter(&edges);

    // join current distances with edges to get +1 distances,
    // include the current distances in the set as well,
    // group by node id and keep minimum distance.
    dists.join_u(&edges, |d| d, |e| e, |_,d,n| (*n, d+1))
         .concat(&dists)
         .group_by_u(|x| x, |k,v| (*k, *v), |_, mut s, t| {
             t.push((*s.peek().unwrap().0, 1))
         })
});
```

Having defined the computation, we can now modify the `edges` and `roots` input collections. We might start by adding all of our edges, and perhaps then exploring the set of roots. Or we might change the edges with the roots held fixed to see how the distances change for each edge.

In any case, the outputs will precisely track the computation applied to the inputs. It's just that easy!
