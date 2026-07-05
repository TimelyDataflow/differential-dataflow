# Formal developments

Machine-checked arguments about differential dataflow, in Lean 4 with Mathlib.

These files split along one line.
`Differential/Compositional.lean` argues that differential computation *abstractly* produces the right thing: each operator, as a transformation of difference traces, computes what re-evaluating its mathematics moment by moment would, and this composes to whole programs.
The other four refine that intent into a *specific, efficient implementation* of the `reduce` operator: `Coverage.lean` states the general coverage keystone (any function of the accumulated input changes only at join-closure times of its edits), and `RoundCoverage.lean`, `Compaction.lean`, and `Model.lean` build the incremental schedule on it — interesting-time enumeration, pending obligations, logical compaction.

## Contents

`Differential/Compositional.lean` concerns compositional adequacy: that each operator's difference-trace implementation computes its mathematics applied pointwise to the accumulated data, and that operators compose.
A collection is a difference trace; `Model.acc` accumulates it, and `acc_injective` shows a trace is determined by its accumulation.
An operator is then a commuting square `Adequate D impl := acc ∘ impl = D ∘ acc`, and these squares compose (`Adequate.comp`): correctness of a whole program reduces to correctness of its pieces.
Each generator is a square — LINEAR/SUM (`linImpl_adequate`), `enter` and temporal time-actions (`timeImpl_adequate`), REDUCE (`reduce_adequate`, whose existence half is exactly `Coverage.exists_diff_trace_comp`), and JOIN as a bilinear convolution over the lattice (`join_adequate`, which is bilinearity plus `sup_le_iff`) — and a deep-embedded `Program` (`id`/`linear`/`reduce`/`par`/`join`/`seq`) is adequate by induction (`Program.adequate`).
The batch square is only a specification; the content of differential dataflow is that `impl` runs *incrementally*, as a per-batch increment (`Refines`) that composes by a discrete chain rule (`Refines.comp`) — this is why difference traces are the composable interface between operators, and the locality that makes the increment cheap for the nonlinear REDUCE is precisely the interesting-times argument of `Coverage.lean` and `Model.lean`.
The square also *carries* a coverage bound: `AdequateCov` enriches it with the fact — from `Coverage.exists_diff_trace_comp` — that the output edits lie in the join-closure of the input's edits, and `AdequateCov.comp` shows that bound composes (closure idempotence), so `Program.adequateCov` maintains the right answer AND keeps a whole program's edits in the join-closure of its input's edits — bounded work is compositional, not just correctness.
A second, orthogonal bound is on state: `acc_compact` shows that advancing every update time to its frontier representative (`Compaction.advance`) is invisible beyond the frontier (`Compaction.Beyond`), so a pointwise operator fed the *compacted* input produces the same output there and needs no pre-frontier history (`adequate_compact_sufficient`, and `_comp` for the composite) — a first cut at the state axis; the frontier-strength gradient (delta joins) and pipeline state-demand composition are deferred (see `DESIGN.md`).
ITERATE adds an iteration coordinate `T × ℕ`: the loop's accumulated output is the from-scratch iteration of the body's denotation (`round_matches`, `iterate_program`), stabilization is free from finite support (`leave_stabilizes`, no termination hypothesis needed), and a DELAY-ing (`Causal`) body still has a unique accumulated fixpoint (`acc_unique`) — evaluation-order independence *is* the adequacy statement.
Everything is axiom-clean and monomorphic in the group (one type `A` per program); the deferred extensions are heterogeneous typing (typed `Program X Y`, the foundation for *verified program transformations* — e.g. moving computation in and out of loops — and for key/value-changing joins) and the confluence question of when two different arrival schedules compute the same value (distinct from the adequacy proved here).

`Differential/Coverage.lean` is the general keystone: which times an incremental computation must revisit, for *any* operator.
A collection is a difference trace: updates at lattice-valued times, accumulated over all times less or equal to a query time.
The file proves that for any function `f` of the accumulated input (assuming only `f 0 = 0`), the output is again a difference trace, with updates only at joins of input update times (`exists_diff_trace`, `exists_diff_trace_comp`) — a statement about differential computation as such, of which LINEAR, JOIN, and REDUCE are specializations.

`Differential/RoundCoverage.lean` refines the keystone into the `reduce` operator's per-round schedule.
Per round — an old input plus a batch of novel updates — the output change is covered by the joins involving at least one novel time (`round_coverage`), and that set coincides exactly with what an implementation enumerates by seeding on the novel times and closing under join against novel and prior times (`cl_inter_above_eq_novelJoins`; `active_times` in `reduce.rs`).
Emitting corrections at those times keeps the output correct up to the frontier (`round_advances_invariant`).
Joins landing at or beyond the frontier are deferred as pending obligations; the pending set covers exactly the staleness of the stored output (`round_needed_set_decomposition`), and a round with no new input shows it cannot be dropped (`draining_round_needs_pending`).
All statements use exact times; `Differential/Model.lean` extends them to compacted state across many rounds.

`Differential/Compaction.lean` concerns `Lattice::advance_by`.
It proves soundness (`advance_le_iff`: at or beyond the frontier, the representative compares exactly as the original does) and canonicity (`advance_eq_iff`: two times share a representative precisely when they are indistinguishable beyond the frontier).
These are pointwise facts about one time and one frontier; `Differential/Model.lean` handles consolidation and the correctness of computations reading compacted state across rounds.

`Differential/Model.lean` proves end-to-end stream correctness for the incremental operator of `TARGETS2.md`: rounds delimited by antichain frontiers, a truncated join-closure enumeration with a carried pending set, and logical compaction modeled as an ADVERSARY that may re-represent the stored input and output arbitrarily, subject only to preserving accumulations at or beyond the frontier.
The goal theorem (`Model.Run.stream_correct`, quantified as `Model.streamCorrectness_holds`) is unconditional: every run, under every such adversary, emits corrections that accumulate to the reduction of the true input at every finalized time; once the frontier empties the emitted stream is THE difference trace of the output (`update_ext`, `Run.emitted_unique`).
The enumeration's ground set includes the stored output support (TARGETS2's step 2, as amended there); whether the OUTPUT-FREE enumeration also suffices is the open question recorded in TARGETS2's amendment note.
TARGETS2.md states the model in prose; the file header of `Model.lean` has a sentence-by-sentence correspondence table, and `scenario1Run` checks TARGETS2's cancellation scenario end to end.

## The algorithm, in prose

The proof organizes around four observations; they are worth having separately from the Lean, as a way to study the algorithm.

**What is owed has no memory.**
At any moment the debt at a time `t` is the difference of two readable quantities: `f` of the input accumulated at or below `t`, minus the corrections emitted at or below `t`.
Compaction of either collection may change anything except accumulations at or beyond the frontier — and everything the operator will ever again read is an accumulation at such a time.
So the compacted input and compacted output together determine the debt at every non-finalized time exactly, and each round can recompute what it owes from scratch rather than carry anything across the compaction (`Run.staleness_rederived`).
This is where keeping the output earns its keep: the input testifies to what SHOULD have been said, the output to what WAS said, and the debt is the gap between the two.
Without the stored output, "what was said" is knowable only by attributing past emissions to joins of input times as they stood at emission; input compaction can orphan that attribution when the constituents later merge or cancel, and re-basing debts onto the surviving times is exactly the open output-free question.

**Debt is created only above the seeds.**
Values are not enough; the operator must also know the ADDRESSES at which to re-evaluate.
Track the debt across a round and ask what can change it at a time `t`: the input accumulation moves only if a batch update sits at or below `t`, and the emitted accumulation moves only if the round emits at or below `t` — which happens only at active times, all above the seed set `N` (batch times plus pending times now due).
So at a `t` above nothing in `N`, both ingredients are frozen: whatever debt `t` has after the round it already had before, and inductively it had none — pre-existing debt sits above pending times (the invariant's pending-coverage clause), a pending time at or below an in-interval `t` is due and hence in `N`, and a pending time not yet due is beyond `upper`, putting everything above it outside the interval.
Debt never appears at a time nothing touched; `N` is by construction the complete list of a round's creation sites.

**Seeding on `N` is the full closure, restricted to where debt can live.**
Within `Above(N)` the restriction to "joins containing at least one seed" is vacuous, by absorption: if a join `x` of purely stored times sits above a seed `n`, then `x = x ⊔ n`, a join containing the seed after all (`cl_inter_above_eq_novelJoins`, `Reached.absorb`).
So seeding on the new times and closing against stored input and output times enumerates exactly the full join-closure of everything, intersected with the only region where debt can exist; what the full closure would add — joins of old times above no new time — is precisely the region the accounting above shows was paid and left untouched.
Old times still matter as join partners because `f` is opaque: a batch update at `b` changes the collection everywhere above `b`, but `f` may respond only where the new update first co-habits with an old one, at `s ⊔ b`.
New times are ignition; old input times are fuel; old output times are where past statements sit and may need retraction.
The output times cannot replace the pending set, though: for updates whose values satisfy `f(a) = f(b) = 0` but `f(a + b) ≠ 0`, the constituent evaluations emit nothing, so no output evidence exists anywhere, while the join of their times still owes a first emission — and the adversary may re-represent the input (say, as one update at a common lower bound) so that no visible join structure points there either.
Only the pended time knows.

**Pends are resumption tokens.**
Within a round the closure is not even completed: a join crossing `upper` is recorded at its first crossing and closed no further.
When the frontier passes it, it enters `N`, and closing it against the stored times regenerates the deeper joins — the pend resumes a suspended closure computation.
Resuming from the CURRENT representatives is sound by the first observation: the debt above a due pend is re-derived from the compacted input and output, so the resumption needs nothing that compaction could have destroyed.
A pend carries no other content — the implementation's own comment, "we know nothing about why we were warned", is the honest reading — which is also why over-warning is harmless and the model asks only that the recorded pends be CONTAINED in the new pending set (`Step.pending_super`).

## Building

```
export PATH="$HOME/.elan/bin:$PATH"
cd formal
lake build
```

A fresh checkout fetches Mathlib; `lake exe cache get` downloads prebuilt artifacts, which is much faster than compiling it.

To confirm a theorem is fully proved, check its axioms: in a scratch file importing `Differential.Coverage`, `#print axioms Coverage.exists_diff_trace_comp` (or `Coverage.round_coverage`, importing `Differential.RoundCoverage`; or `Model.streamCorrectness_holds`, importing `Differential.Model`; or `Compositional.Program.adequate`, importing `Differential.Compositional`) should report only `propext`, `Classical.choice`, and `Quot.sound` — in particular no `sorryAx`.

Nothing in the Rust workspace depends on this directory, and no CI gates on it.
