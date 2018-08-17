## Example 3: Stable computation

Let's now change the computation just a bit, to show off how differential dataflow might hope to lead us to more efficient computation.

Instead of just counting the words in the input, let's track the number of times each *count* occurs. Rather than report the words themselves, we will just report how words occur once, twice, three times, etc. To do this, we replace

```rust,no_run
    words.count()
         .inspect(|x| println!("seen: {:?}", x));
```

with

```rust,no_run
    words.count()
         .map(|(_word, count)| count)
         .count()
         .inspect(|x| println!("seen: {:?}", x));
```

The two new lines extract the count from each `(word, count)` pair, and then we just `count()` the results again. This shows us whether we have many common words, lots of unique words, or some mix of the two. It doesn't actually show us the words themselves, though.

Let's run our computation again, with this new computation!

        Echidnatron% cargo run -- 3 10
           Compiling differential-dataflow v0.6.0 (file:///Users/mcsherry/Projects/differential-dataflow)
            Finished dev [unoptimized + debuginfo] target(s) in 11.59s
             Running `target/debug/examples/test 3 10`
        seen: ((3, 2), (Root, 0), 1)
        seen: ((4, 1), (Root, 0), 1)
        Echidnatron%

See how although there are changes through the first three rounds of input, the changes stop then. At each time we remove an element with count four and introduce an element with count three. The counts remain at one with four and two with three, never changing.

Let's increase the number of rounds from ten to ten million:

        Echidnatron% cargo run -- 3 10000000
           Compiling differential-dataflow v0.6.0 (file:///Users/mcsherry/Projects/differential-dataflow)
            Finished dev [unoptimized + debuginfo] target(s) in 11.59s
             Running `target/debug/examples/test 3 10000000`
        seen: ((3333333, 2), (Root, 0), 1)
        seen: ((3333334, 1), (Root, 0), 1)
        Echidnatron%

Now, although the computation is stable, and produces no output changes, there is still a lot of work to do. For each input change, we need to confirm that there are no output changes, and this involves taking each insert and remove and seeing how they change the number of times each string appears. When we then see how these changes affect the numbers of times each count appears, we will notice that no output change results. But we still have to do the work to get there.

Differential dataflow computation isn't free, but it does do a good job of noticing when collections change and only doing work when this happens. When collections do not change, we don't need to do any work to update computation that depends on them. Any computation downstream from the second `count()`, for example the `inspect()` operator, will not be invoked unless there are changes in the counts. If there were more complicated computation, it would only be invoked when there were changes, on only with those changes that occur (allowing it to focus its attention on those changes).