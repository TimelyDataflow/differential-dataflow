# Recipes

Each recipe is a small program for one idiom, with a session that runs it on a few rows.
The idioms are the ones that make DDIR programs fast or correct, and that are hard to find from the operator list alone.

Run a recipe from `interactive/`, and compare with its recorded transcript:

```sh
cargo run --release -p ddir-server < examples/recipes/upsert.txt
```

`NAME.ddp` is the program and explains the idiom in its comments.
`NAME.txt` feeds it, seals its inputs, and peeks the results.
`NAME.out` is the transcript, checked at one and three workers by `server/tests/recipes.rs`.
After an intended change, `UPDATE_RECIPES=1 cargo test --release -p ddir-server --test recipes` records the transcripts again.

| Recipe | Idiom | Use it for |
|---|---|---|
| [`sum`](sum.ddp) | `sum` adds each key's values, weighted by their diffs | sums of integers, incrementally |
| [`top_k`](top_k.ddp) | a tree of `reduce(limit k)` groups | top k over many rows, with small updates |
| [`retention`](retention.ddp) | `lift` and `delay` retract each row a fixed time after it arrives | sliding windows, expiry |
| [`upsert`](upsert.ddp) | a root `var` holds the table one time ago; writes count at their own time only | a keyed table with put and delete |
| [`left_join`](left_join.ddp) | the unmatched rows are the input minus the matched ones; a declared `enum` for the missing value | outer joins, without NULL |
| [`connected_components`](connected_components.ddp) | `delay` holds back large labels, so small ones spread first | label propagation, prioritized iteration |
| [`shortest_paths`](shortest_paths.ddp) | `delay` to bucket `d / Δ`, spaced several rounds apart | weighted shortest paths, delta stepping |
| [`triangles`](triangles.ddp) | `lift` and `half_join` along each use of a relation | multiway joins without large intermediate results |

Two facts the recipes lean on:
- **Inputs are multisets.** A row's count is the sum of its diffs, so a second put of a key adds a second row; replacing a value is a program's job, as in `upsert`.
- **`(e)` is a one-field tuple, not `e`.** Parentheses build a tuple. Write `t` where you mean the value itself; `(t)` is a different shape.
