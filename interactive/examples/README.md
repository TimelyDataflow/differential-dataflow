# Examples

[`access.txt`](access.txt) is a whole session: it loads [`access.ddp`](access.ddp), feeds it, and reads the results.
Run it from `interactive/`, and see [the README](../README.md#operation) for a walk through it:

```sh
cargo run --release -p ddir-server < examples/access.txt
```

The other programs each show one idiom, explained in their comments:
[`top_k`](top_k.ddp), [`retention`](retention.ddp), [`upsert`](upsert.ddp), [`components`](components.ddp), [`shortest_paths`](shortest_paths.ddp), [`triangles`](triangles.ddp).

[`aoc2023`](aoc2023/README.md) is Advent of Code 2023 in DDIR, with its own runner.
