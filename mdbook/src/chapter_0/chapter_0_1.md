## Example 1: Static data

Differential dataflow programs are written against what may appear (intentionally) to be static datasets, using operations that look a bit like database (SQL) or big data (MapReduce) idioms. It is all a bit more complicated than that, but this is a good place to start from.

The following complete program creates an input collection of strings, and then reports the number of times each string occurs in the collection.

```rust,no_run
    extern crate timely;
    extern crate differential_dataflow;

    use differential_dataflow::input::Input;
    use differential_dataflow::operators::Count;

    fn main() {

        // define a new computational scope, in which to run BFS
        timely::execute_from_args(std::env::args(), move |worker| {

            let values: usize = std::env::args().nth(1).unwrap().parse().unwrap();
            let rounds: usize = std::env::args().nth(2).unwrap().parse().unwrap();

            // create a counting differential dataflow.
            let mut input = worker.dataflow(|scope| {

                // create paired input and collection.
                let (input, words) = scope.new_collection();

                words.count()
                     .inspect(|x| println!("seen: {:?}", x));

                // return the input for manipulation.
                input
            });

            // load some initial data.
            input.advance_to(0);
            for value in 0 .. rounds {
                input.insert((value % values).to_string());
            }

        }).unwrap();
    }
```

When we execute this program, we should see the changes the output of the `count` operator undergoes (as this is where we have placed the `inspect` operator which prints tuples that flow past it). Each update has the form

        (data, time, diff)

Here `data` will be pairs `(String, isize)` reporting pairs of words and their counts; this is determined by the computation we have written, and the type of data in the stream we inspect. The `time` type will essentially be an integer, but due to how timely dataflow works it will say this as `(Root, i)` for values of `i`. The `diff` type will be an `isize`, a signed integer, telling us how many times each `data` is observed.

        Echidnatron% cargo run -- 3 10
           Compiling differential-dataflow v0.6.0 (file:///Users/mcsherry/Projects/differential-dataflow)
            Finished dev [unoptimized + debuginfo] target(s) in 9.17s
             Running `target/debug/examples/test`
        seen: (("0", 4), (Root, 0), 1)
        seen: (("1", 3), (Root, 0), 1)
        seen: (("2", 3), (Root, 0), 1)
        Echidnatron%

Here we see three records, which makes sense as we only introduce three distinct values. And indeed, their reported counts are as they should be (zero occurs four times, one and two occur three times).

What might make less sense is why we bother to report things like `(Root, 0)` and `1`. These are the `time` and `diff` fields from above, and they aren't very interesting at the moment because we introduced all of our data at once then don't change anything. They will become more interesting in just a moment, when we start changing the data.

Of course, we can also increase the amount of data like so

        Echidnatron% cargo run -- 3 10000000
           Compiling differential-dataflow v0.6.0 (file:///Users/mcsherry/Projects/differential-dataflow)
            Finished dev [unoptimized + debuginfo] target(s) in 9.17s
             Running `target/debug/examples/test`
        seen: (("0", 3333334), (Root, 0), 1)
        seen: (("1", 3333333), (Root, 0), 1)
        seen: (("2", 3333333), (Root, 0), 1)
        Echidnatron%

and if we have the time to watch the output, we can increase the number of distinct values like so:

        Echidnatron% cargo run -- 3333333 10000000
           Compiling differential-dataflow v0.6.0 (file:///Users/mcsherry/Projects/differential-dataflow)
            Finished dev [unoptimized + debuginfo] target(s) in 9.17s
             Running `target/debug/examples/test`
        seen: (("0", 4), (Root, 0), 1)
        seen: (("1", 3), (Root, 0), 1)
        seen: (("10", 3), (Root, 0), 1)
        ...
        seen: (("999997", 3), (Root, 0), 1)
        seen: (("999998", 3), (Root, 0), 1)
        seen: (("999999", 3), (Root, 0), 1)
        Echidnatron%

At this point, we have something like a traditional big data batch processing experience! Let's fix that.