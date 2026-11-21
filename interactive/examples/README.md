# Examples

Each folder here is one setting: a few programs, one session that runs them in a server, and the session's recorded transcript.
Start with the tour.

| Folder | Programs | What the session shows |
|---|---|---|
| [`tour`](tour/tour.txt) | [`tour`](tour/tour.ddp) | loading a program, feeding rows at times, sealing, reading results by key and by time, retractions, an inline program, batched commands, errors, and dropping programs |
| [`tables`](tables/tables.txt) | [`shop`](tables/shop.ddp), [`upsert`](tables/upsert.ddp), [`sum`](tables/sum.ddp), [`top_k`](tables/top_k.ddp), [`left_join`](tables/left_join.ddp), [`retention`](tables/retention.ddp) | a shop's orders and customer table, read by programs that keep totals, the top customers, orders joined to customers, and a sliding window up to date |
| [`graphs`](graphs/graphs.txt) | [`graph`](graphs/graph.ddp), [`reach`](graphs/reach.ddp), [`shortest_paths`](graphs/shortest_paths.ddp), [`components`](graphs/components.ddp), [`scc`](graphs/scc.ddp), [`triangles`](graphs/triangles.ddp) | one program holding a graph, five programs reading it, and edge changes flowing through all of them |
| [`apps`](apps/apps.txt) | [`access`](apps/access.ddp), [`egraph`](apps/egraph.ddp) | access control through nested groups with blocks, and an e-graph rewrite rule saturating as nodes arrive |

The comments in each program say which idiom it uses and why.
In `tables` and `graphs`, one program takes the fed inputs and the others bind their inputs to its outputs at load, as `load reach from examples/graphs/reach.ddp edges=graph::edges`.
They share its arrangements rather than each keeping a copy.

## Running

Run a session from `interactive/`:

```sh
cargo run --release -p ddir-server < examples/tour/tour.txt
```

Each response line starts with its request's number: `_7 data ...` lines are results, and each request ends in `_7 ok ...` or `_7 err ...`.
Set `DDIR_WORKERS=4` to run on four workers; the results are the same.
Without a session on stdin, the server reads commands as you type them.
The [server reference](../server/README.md#server-command-reference) has every command, and the [language reference](../LANGUAGE.md) has every operator.

## Checking

`server/tests/examples.rs` runs each session at one worker and at three, and compares the output with the recorded `NAME.out`.
After an intended change, record the transcripts again:

```sh
UPDATE_EXAMPLES=1 cargo test --release -p ddir-server --test examples
```

## Elsewhere

- [`aoc2023`](aoc2023/README.md) is Advent of Code 2023 in DDIR, checked against known answers by its own runner.
- `dump_explain.rs` prints a program's IR before and after the explanation rewrite: `cargo run --example dump_explain <program>`.
- [`../bench`](../bench) holds the benchmarks: LDBC and a transaction workload.
- `../tests/programs` holds programs that pin the language's lowerings, checked in batch.
