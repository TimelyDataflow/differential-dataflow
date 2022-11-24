## The Map Operator

The `map` operator applies a supplied function to each element of a collection, the results of which are accumulated into a new collection. The map operator preserves the counts of elements, and any elements that are made equal by the map operator will have their counts accumulate.

As an example, our example program used `map` to reverse the pairs of identifiers in the `manages` collection, to place the second element first.

```rust
# extern crate timely;
# extern crate differential_dataflow;
# use timely::dataflow::Scope;
# use differential_dataflow::Collection;
# use differential_dataflow::lattice::Lattice;
# use differential_dataflow::operators::Join;
# fn example<G: Scope>(manages: &Collection<G, (u64, u64)>)
# where G::Timestamp: Lattice
# {
    manages
        .map(|(m2, m1)| (m1, m2))
        .join(&manages)
        .inspect(|x| println!("{:?}", x));
# }
```

If instead we had just written

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
        .map(|(m2, m1)| m2);
# }
```

we would have a collection containing each manager with a multiplicity equal to the number of individuals they manage.
