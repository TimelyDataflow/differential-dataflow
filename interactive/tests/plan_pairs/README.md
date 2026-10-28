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
idempotence, and repeated planning. Projection repetition counts are explicit
in the manifest until the reduction change removes those repeat masks.
Every view is compared, including during regeneration.

The test repeats planning and compares emitted bytes. The regeneration script
also checks every corpus file against its starting bytes after each of two
updates. New benchmark files require manifest entries. Keep semantic tests
alongside these snapshots: exact graph and plan comparison is stricter than
semantic equivalence, and no equivalence is claimed for a failed round trip.
