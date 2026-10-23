# Field demand as a DDIR program

`demand_rules.ddp` computes demand dependencies, seeds, and their least fixed
point. Run `cargo run --release --example demand_rules -- PATH...` from
`interactive/` to compare its live facts with `projection::demand_graph(...).closure()`.
The example installs the rules once, feeds each program, compares the result,
then retracts it before the next program.

The first two inputs reuse `optimize.ddp`'s `nodes` and `edges` rows. Rust's
`graph.rs` exports those rows for both examples. Rust additionally supplies
shape widths, site and reference IDs, linear stage IDs and kinds, declared
shape/root-export marks, and lambda field reads from the same Corgi probe as
the installed Rust pass. These relations describe the program; all demand
edges and seeds are built in DDIR. Fact IDs use the Rust pass's allocation
order solely so the comparator can check the two live sets exactly.

The corpus command uses every repository `.ddp` below `interactive/`, the
naive and curated generated LDBC plans, and programs rendered by the
`interactive/tests/difftest.rs` generator. No DDIR analysis runs in the
install path. The example performs no masking.

## Language friction

- The key/value/presence sides and field positions are integers throughout.
  Named records would make the joins and lambda-read rules easier to audit.
- A collection row appears as `(key, value)`, so a key-only rule still carries
  `(k, ())`-shaped values through intermediate relations. A direct key/value
  split in the row model would simplify these rules.
- Resolving `Ref` requires four integer fields and several joins between the
  existing graph rows and typed sites. Named records would clarify the tags,
  positions, and scope IDs; the separate typed-site relation remains necessary.
- `input` is a reserved expression in lambdas, so using it as a pattern binder
  silently referred to the lambda input. The rules use `src` instead.
