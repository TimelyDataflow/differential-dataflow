## The Concat Operator

The `concat` operator takes two collections whose element have the same type, and produces the collection in which the counts of each element are added together.

For example, we might form the symmetric "management relation" by concatenating the `manages` collection with the same collection with its fields flipped:

```rust,no_run
    manages
        .map(|(m2, m1)| (m1, m2))
        .concat(&manages);
```

This collection likely has at most one copy of each record, unless perhaps any manager manages itself. In fact, zero manages itself, and the element `(0, 0)` would have count two.