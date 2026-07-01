# Formal developments

Machine-checked arguments about differential dataflow, in Lean 4 with Mathlib.

## Contents

`Differential/Coverage.lean` concerns which times an incremental computation must revisit.
A collection is a difference trace: updates at lattice-valued times, accumulated over all times less or equal to a query time.
The file proves that for any function `f` of the accumulated input, the output is again a difference trace, with updates only at joins of input update times (`exists_diff_trace`, `exists_diff_trace_comp`).
Per round — an old input plus a batch of novel updates — the output change is covered by the joins involving at least one novel time (`round_coverage`), and that set coincides exactly with what an implementation enumerates by seeding on the novel times and closing under join against novel and prior times (`cl_inter_above_eq_novelJoins`; `active_times` in `reduce.rs`).
Emitting corrections at those times keeps the output correct up to the frontier (`round_advances_invariant`).
Joins landing at or beyond the frontier are deferred as pending obligations; the pending set covers exactly the staleness of the stored output (`round_needed_set_decomposition`), and a round with no new input shows it cannot be dropped (`draining_round_needs_pending`).
All statements use exact times.
Extending them to logically compacted input — stored update times advanced, merged, or cancelled — is the intended next step.

`Differential/Compaction.lean` concerns `Lattice::advance_by`.
It proves soundness (`advance_le_iff`: at or beyond the frontier, the representative compares exactly as the original does) and canonicity (`advance_eq_iff`: two times share a representative precisely when they are indistinguishable beyond the frontier).
These are pointwise facts about one time and one frontier; consolidation, and the correctness of computations reading compacted state across rounds, are outside its scope.

## Building

```
export PATH="$HOME/.elan/bin:$PATH"
cd formal
lake build
```

A fresh checkout fetches Mathlib; `lake exe cache get` downloads prebuilt artifacts, which is much faster than compiling it.

To confirm a theorem is fully proved, check its axioms: in a scratch file importing `Differential.Coverage`, `#print axioms Coverage.round_coverage` should report only `propext`, `Classical.choice`, and `Quot.sound` — in particular no `sorryAx`.

Nothing in the Rust workspace depends on this directory, and no CI gates on it.
