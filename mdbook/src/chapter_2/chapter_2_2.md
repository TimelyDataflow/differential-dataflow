## The Filter Operator

The `filter` operator applies a supplied predicate to each element of a collection, and retains only those for which the predicate returns `true`.

As an example, we might select out those management relation where the manager has greater employee id than the managee, by writing

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
        .filter(|&(m2, m1)| m2 > m1);
# }
```

Rust makes it very clear when a method is provided with data, or only the ability to look at the data. The filter operator is only allowed to look at the data, which is where the `&` glyph comes from. This allows us to be more efficient in execution, but it is a subtle concept that further Rust reading may illuminate.
