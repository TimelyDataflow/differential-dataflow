# Preparation plan pairs

The manifest fixes 60 cases, their providers, the one-off request export names,
and the one registered host function. Inputs cover the graph and cascade
emitters, LDBC simple and interactive queries, transaction plans, server
examples, and focused transform cases.

Each valid case has a whole final `expected/NAME.ddp`. A parsed whole program
and four unified diffs record the successive `--emit-ddp-stage` views:
parsed, lowered, optimized, projected, and as-of. Empty diff files mean no
change. Apply the diffs to the parsed file to recover every whole stage.
The final recovered stage must match `NAME.ddp` byte for byte.

The explanation rewrite starts from IR, so its base is `explain.lowered.ddp`
and it has three diffs. The deliberately invalid export has parsed DDP and
an expected `.error`; the emitter must refuse its final plan. Diagnostic
reports remain available through `ddir-plan` without `--emit-ddp`.

Run the corpus in the default build:

```sh
CARGO_NET_OFFLINE=true PATH="$HOME/.cargo/bin:$PATH" cargo test -p interactive --test plan_pairs
```

Accept intended changes, review their DDP diffs, then verify two regenerations:

```sh
CARGO_NET_OFFLINE=true PATH="$HOME/.cargo/bin:$PATH" UPDATE_PLAN_PAIRS=1 cargo test -p interactive --test plan_pairs
python3 -B interactive/tests/plan_pairs/regenerate.py
```

Python 3 is required, without extra packages or benchmark data. Every run
compares generated graph programs with the benchmark emitters. Update mode
refreshes those inputs. It checks all cases before writing expected outputs.

Every run checks shapes, exports, references, dependency
order, scope boundaries, feedback bindings, source labels, optimizer
idempotence, and repeated planning. Projection must report zero further changes at both the projected and as-of
stages, and the full IR must remain identical.
Every view is compared, including during regeneration.

The optimizer composes adjacent maps, cancels adjacent negations, drops identity
maps, and removes dead imports and aliases. Projection uses the same cleanup
schedule after introducing masks. Unit tests in `src/passes/tests.rs` evaluate
each accepted map rewrite on generated batches of 0, 1, 17, and 64 rows. They
cover this manifest and the 81 other checked-in review programs, including BI.

`hoist-arranges` shares explicit arrangements of the same binding across scopes,
following unchanged parent imports. It keeps a lone child arrangement in place.
`collapse-parent-arranges` removes an arrange of an already arranged parent import,
including trace imports and reduction results. Both rules are independently
switchable. Hoisted statement labels retain their source name in the ancestor scope.

Every emitted reduction uses `reduce(...)`, including the parsed view. Parsed
`distinct` explicitly keys by the whole record because that view has no shape
inference. Its extra maps can differ from the optimized unit-valued form.
Both forms spell the resulting unit values as `()` in the reducer.

Composed reducers use a graph-derived spelling only when it reparses to the exact
optimized graph. In particular, a map over an enlisted value remains a map when
folding it would change that graph.

The test repeats planning and compares emitted bytes. The regeneration script
also checks every corpus file against its starting bytes after each of two
updates. New benchmark files require manifest entries. Keep semantic tests
alongside these snapshots: exact graph and plan comparison is stricter than
semantic equivalence, and no equivalence is claimed for a failed round trip.
