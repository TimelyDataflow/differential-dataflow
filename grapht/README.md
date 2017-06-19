# Grapht

A differential dataflow server for continually changing graphs

## Overview

Grapht is a system that hosts continually changing graph datasets, and computation defined over them. Users are able to build and load shared libraries that can both define computation over existing graph datasets or define and share new graphs. The user code is able to share the same underlying representations for the graphs, reducing the overhead and streamlining the execution.

## A Sketch

Grapht is very much in progress, and its goals are mostly to exercise timely and differential dataflow as systems, and to see where this leads. At the moment, Grapht provides a small set of graph definitions and graph analyses one can load against them. This is mostly so that we can see what it is like to start up and shut down graph computations with shared state, and understand if we are doing this well. 

Several computations are defined in `./dataflows/` which give examples of defining your own computation. These projects have no special status, and could each be re-implemented by users such as yourself. The intent is that once your project is compiled, Grapht can load the associated shared libraries and bring your computation into the same shared address space as the other graph computations.

## Examples

The main binary is `bin/server.rs`, which can be invoked by running

    cargo run --bin server

This .. doesn't appear to do anything:

    Echidnatron% cargo run --bin server
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
         Running `target/debug/server`
    >

What has happened is that the server is up and running and waiting for some input! Pretty exciting. 

Before getting ahead of ourselves, we'll need to build some code to run. Let's quit out of the server for now and do the following:

    Echidnatron% cd dataflows/random_graph
    Echidnatron% cargo build
       Compiling random_graph v0.1.0 (file:///Users/mcsherry/Projects/differential-dataflow/grapht/dataflows/random_graph)
        Finished dev [unoptimized + debuginfo] target(s) in 5.90 secs
    Echidnatron%

and then 

    Echidnatron% cd ../degr_dist
    Echidnatron% cargo build
       Compiling degr_dist v0.1.0 (file:///Users/mcsherry/Projects/differential-dataflow/grapht/dataflows/degr_dist)
        Finished dev [unoptimized + debuginfo] target(s) in 8.14 secs
    Echidnatron%

These commands will build two shared libraries, `librandom_graph.dylib` and `libdegr_dist.dylib`, which we will use in our server!

Ok, back to the server now. Load that puppy up again and 

    Echidnatron% cargo run --bin server
        Finished dev [unoptimized + debuginfo] target(s) in 0.0 secs
         Running `target/debug/server`
    > ./dataflows/random_graph/target/debug/librandom_graph.dylib build <graph_name> 1000 2000 10
    >

Ok. We have now bound to `<graph_name>` a random graph on 1,000 nodes comprising a sliding window over 2,000 edges, advanced ten edges at a time. If you would like to, you can change any of the arguments passed.

Up next, let's attach the `degr_dist` computation to `<graph_name>` and see what we get:

    > ./dataflows/degr_dist/target/debug/libdegr_dist.dylib build <graph_name>
    >

You may have a moment now where nothing much happens. In fact, lots is happening behind the scenes; we are taking all of the produced history of `<graph_name>` and feeding it in to the `degr_dist` computation. Here we go:

    > count: ((1, 266), (Root, 0), 1)
    count: ((1, 266), (Root, 1), -1)
    count: ((1, 267), (Root, 1), 1)
    count: ((1, 267), (Root, 2), -1)
    count: ((1, 266), (Root, 2), 1)
    count: ((1, 266), (Root, 4), -1)
    count: ((1, 267), (Root, 4), 1)
    ...

Here we learn a few things: First, I just print `>` to make it look like a console, and this is a mess when we also try and print out the changes to the degree distribution. More importantly, we start to get the history for count of `1`: initially there are 266 nodes with out-degree one, which increases, then decreases, then increases, then ...

    ...
    count: ((1, 269), (Root, 999), -1)
    count: ((1, 270), (Root, 999), 1)
    count: ((1, 270), (Root, 1003), -1)
    count: ((1, 269), (Root, 1003), 1)
    ...

It just keeps going talking about degree one, doesn't it? In fact, it probably goes on for quite a while unless you typed that second line really quickly. The reason being the graph starts changing, and does so pretty quickly. And, all of those changes are a part of the history that gets analyzed by the degree distribution computation. I did a quick copy/paste, in a fraction of a second, and even for me it managed some thirty thousand iterations:

    ...
    count: ((1, 273), (Root, 33109), -1)
    count: ((1, 272), (Root, 33109), 1)
    count: ((2, 283), (Root, 0), 1)
    count: ((2, 283), (Root, 1), -1)
    count: ((2, 281), (Root, 1), 1)
    count: ((2, 281), (Root, 2), -1)
    count: ((2, 282), (Root, 2), 1)
    count: ((2, 282), (Root, 4), -1)
    count: ((2, 281), (Root, 4), 1)
    ...

If we look further down we get the degree three counts:

    ...
    count: ((2, 264), (Root, 33108), 1)
    count: ((2, 264), (Root, 33109), -1)
    count: ((2, 265), (Root, 33109), 1)
    count: ((3, 164), (Root, 0), 1)
    count: ((3, 164), (Root, 1), -1)
    count: ((3, 165), (Root, 1), 1)
    count: ((3, 165), (Root, 2), -1)
    count: ((3, 166), (Root, 2), 1)

And so it goes. 