## The Consolidate Operator

The `consolidate` operator takes an input collection, and does nothing to the collection except ensure that each element occurs with only one count. As an example, if we were to inspect

```rust,no_run
    manages
        .map(|(m2, m1)| (m1, m2))
        .concat(&manages)
        .inspect(|x| println!("{:?}", x));
```

we might see two copies of the same element:

        ((0,0), (Root, 0), 1)
        ((0,0), (Root, 0), 1)

This is because for reasons of efficiency, operators like `map` and `concat` do not work too hard to "consolidate" the changes that they produce. This is rarely a problem, but it can nonetheless be helpful to consolidate a collection before inspecting it, to ensure that you see the most concise version.