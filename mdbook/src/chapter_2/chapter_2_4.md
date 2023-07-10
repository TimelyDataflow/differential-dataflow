## The Consolidate Operator

The `consolidate` operator takes an input collection, and does nothing other than possibly changing its physical representation. It leaves the same sets of elements at the same times with the same logical counts.

What `consolidate` does do is ensure that each element at each time has at most one physical tuple. Generally, we might have multiple updates to the same element at the same time, expressed as independent updates. The `consolidate` operator adds all of these updates together before moving the update along.

As an example, if we were to inspect

```rust
# extern crate timely;
# extern crate differential_dataflow;
# use timely::dataflow::Scope;
# use differential_dataflow::Collection;
# use differential_dataflow::lattice::Lattice;
# use differential_dataflow::operators::Reduce;
# fn example<G: Scope>(manages: &Collection<G, (u64, u64)>)
# where G::Timestamp: Lattice
# {
    manages
        .map(|(m2, m1)| (m1, m2))
        .concat(&manages)
        .inspect(|x| println!("{:?}", x));
# }
```

we might see two copies of the same element:

```ignore
((0, 0), 0, 1)
((0, 0), 0, 1)
```

However, by introducing `consolidate`

```rust
# extern crate timely;
# extern crate differential_dataflow;
# use timely::dataflow::Scope;
# use differential_dataflow::Collection;
# use differential_dataflow::lattice::Lattice;
# fn example<G: Scope>(manages: &Collection<G, (u64, u64)>)
# where G::Timestamp: Lattice
# {
    manages
        .map(|(m2, m1)| (m1, m2))
        .concat(&manages)
        .consolidate()
        .inspect(|x| println!("{:?}", x));
# }
```

we are guaranteed to see at most one `(0,0)` update at each time:

```ignore
((0, 0), 0, 2)
```

The `consolidate` function is mostly useful before `inspect`ing data, but it can also be important for efficiency; knowing when to spend the additional computation to consolidate the representation of your data is an advanced topic!
