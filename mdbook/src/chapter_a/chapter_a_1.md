## Increase the scale.

Ten people was a pretty small organization. Let's do ten million instead.

We are going to have to turn off the output printing here. We'll also break it down two ways, just loading up the initial computation, and then doing that plus all of the changes to the reporting structure. We haven't learned how to interactively load all of the input and await results yet (in just a moment), so we will only see elapsed times measuring the throughput, not the latency.

First, we produce the skip-level management.

```ignore
        Echidnatron% time cargo run --release --example hello 10000000
        cargo run --release --example hello 10000000 -w1  2.74s user 1.00s system 98% cpu 3.786 total
        Echidnatron%
```

Four seconds. We have no clue if this is a good or bad time.

Second, we produce the skip-level management and then modify it 10 million times.

```ignore
        Echidnatron% time cargo run --release --example hello 10000000
        cargo run --release --example hello 10000000  10.64s user 2.22s system 99% cpu 12.939 total
        Echidnatron%
```

About thirteen seconds now.

That's less than a microsecond per modification (subtracting the loading time). Importantly, these are throughput measurements rather than latency numbers; we aren't actually doing the 10 million updates one after the other. But, if you compare this to a sequence of 10 million updates to a database, we would be pretty pleased with a microsecond per operation.