## Example 2: Dynamic data

Let's keep the same differential dataflow computation, word counting, from the previous example. The only thing we are going to change here is how we introduce the data. After loading our initial data, we will start to supply *changes* to the input collection, by inserting and removing records at subsequent times.s

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
            let mut input = worker.dataflow::<usize,_,_>(|scope| {

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

            // ... and now let's change some data!
            for round in 1 .. rounds {
                input.advance_to(round);
                input.insert(((round) % values).to_string());
                input.remove(((round - 1) % values).to_string());
            }
        }).unwrap();
    }
```

When we execute this program, we should see the *changes* the output of the `count` operator undergoes.

        Echidnatron% cargo run -- 3 10
           Compiling differential-dataflow v0.6.0 (file:///Users/mcsherry/Projects/differential-dataflow)
            Finished dev [unoptimized + debuginfo] target(s) in 9.41s
             Running `target/debug/examples/test 3 10`
        seen: (("0", 3), (Root, 1), 1)
        seen: (("0", 3), (Root, 3), -1)
        seen: (("0", 3), (Root, 4), 1)
        seen: (("0", 3), (Root, 6), -1)
        seen: (("0", 3), (Root, 7), 1)
        seen: (("0", 3), (Root, 9), -1)
        seen: (("0", 4), (Root, 0), 1)
        seen: (("0", 4), (Root, 1), -1)
        seen: (("0", 4), (Root, 3), 1)
        seen: (("0", 4), (Root, 4), -1)
        seen: (("0", 4), (Root, 6), 1)
        seen: (("0", 4), (Root, 7), -1)
        seen: (("0", 4), (Root, 9), 1)
        seen: (("1", 3), (Root, 0), 1)
        seen: (("1", 3), (Root, 1), -1)
        seen: (("1", 3), (Root, 2), 1)
        seen: (("1", 3), (Root, 4), -1)
        seen: (("1", 3), (Root, 5), 1)
        seen: (("1", 3), (Root, 7), -1)
        seen: (("1", 3), (Root, 8), 1)
        seen: (("1", 4), (Root, 1), 1)
        seen: (("1", 4), (Root, 2), -1)
        seen: (("1", 4), (Root, 4), 1)
        seen: (("1", 4), (Root, 5), -1)
        seen: (("1", 4), (Root, 7), 1)
        seen: (("1", 4), (Root, 8), -1)
        seen: (("2", 3), (Root, 0), 1)
        seen: (("2", 3), (Root, 2), -1)
        seen: (("2", 3), (Root, 3), 1)
        seen: (("2", 3), (Root, 5), -1)
        seen: (("2", 3), (Root, 6), 1)
        seen: (("2", 3), (Root, 8), -1)
        seen: (("2", 3), (Root, 9), 1)
        seen: (("2", 4), (Root, 2), 1)
        seen: (("2", 4), (Root, 3), -1)
        seen: (("2", 4), (Root, 5), 1)
        seen: (("2", 4), (Root, 6), -1)
        seen: (("2", 4), (Root, 8), 1)
        seen: (("2", 4), (Root, 9), -1)
        Echidnatron%

This is a bit of a mess, so let's pick out the changes at specific times. We still have our initial "changes" in the frst round of computation, where we load up our data.

        seen: (("0", 4), (Root, 0), 1)
        seen: (("1", 3), (Root, 0), 1)
        seen: (("2", 3), (Root, 0), 1)

From this point on we see *actual changes*, where records are removed and new records added in. For example, in the very next round we see these paired subtractions and additions:

        seen: (("0", 3), (Root, 1), 1)
        seen: (("0", 4), (Root, 1), -1)
        seen: (("1", 3), (Root, 1), -1)
        seen: (("1", 4), (Root, 1), 1)

These indicate that the count for `"0"` we down by one (removing a count of four and inserting a count of three), and the count for `"1"` went up by one (removing a count of three and inserting a count of four). At each of the other times, we see similar paired interactions resulting from the data we have inserted and removed.

        seen: (("1", 3), (Root, 8), 1)
        seen: (("1", 4), (Root, 8), -1)
        seen: (("2", 3), (Root, 8), -1)
        seen: (("2", 4), (Root, 8), 1)

We see changes for each round up through `(Root, 9)`, at which point we stop changing the input collection, and so the output collection stops changing as well.