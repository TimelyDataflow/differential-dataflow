# Proof targets: end-to-end correctness of an incremental reduce

This document accompanies `Coverage.lean` and is self-contained with it; no other files are assumed.
It states the theorem that, once proved, gives confidence in a reference implementation of differential dataflow's `reduce` operator.
`Coverage.lean` proves per-round lemmas with exact times; the target below generalizes them to many rounds and to logically compacted state.
Getting this statement right matters more than extending the current proofs; they should be bent to fit it, not the reverse.

## Background: the computation being modeled

A `reduce` operator maintains, per key, the invariant that its output equals a function `f` of its accumulated input.
Input arrives in batches of updates at lattice-valued times; the output can change at times that are *joins* of input times, not only at input times themselves (`Coverage.lean`'s subject).
The operator works in rounds: each round covers an interval `[lower, upper)` of times, evaluates `f` at the "interesting" times in the interval, emits corrections, and defers interesting times at or beyond `upper` to later rounds as a *pending set*.
Between rounds, the stored input and output are *logically compacted*: their update times may be advanced, merged, and cancelled, constrained only to preserve accumulations at times at or beyond the current frontier.
The question is whether the emitted corrections are, in total, exactly right.

## The model

Fix a lattice `T` (join and meet), an abelian group `A`, and a reduction `f : A → A` with `f 0 = 0`.
Work is per key; a single key suffices.

An *update set* is a finitely supported `d : T → A`; its *accumulation* is `acc d t = ∑_{x ≤ t} d x` (the `Represents` sum of `Coverage.lean`).

*Frontiers* `u_0 ≤ u_1 ≤ … ≤ u_n` are finite antichains, ordered by upper-set containment.
`Beyond u` is the upper set of `u` (every `t` with `x ≤ t` for some `x ∈ u`).

*Rounds* `r = 1 … n`: round `r` delivers a batch `b_r`, an update set whose support lies in the interval — every support point is in `Beyond u_{r-1}` and not in `Beyond u_r`.
The true input after round `r` is `I_r = b_1 + … + b_r`.

## Compaction, modeled adversarially

Do not model the compaction mechanism.
Instead, quantify over representatives: after round `r`, the stored input is *any* update set `S_r` with

    ∀ t ∈ Beyond u_r,  acc S_r t = acc I_r t

and the stored output is any `O_r` agreeing likewise with the sum of corrections emitted so far.
The adversary may move times, merge them, and cancel updates outright, so long as accumulations beyond the frontier are preserved.
This makes the theorem stronger and the statement simpler, and it is the step a fresh reader is least likely to find alone.

For the record, the mechanism being generalized replaces a time `t` by `advance t F = ⨅_{x ∈ F} (t ⊔ x)` for a frontier `F` at or below the round's `lower`, then consolidates equal times and drops cancelled updates.
Two short facts make such a replacement an instance of the adversary, and are worth having as lemmas in any case:

* soundness: for `s ∈ Beyond F`, `advance t F ≤ s ↔ t ≤ s`;
* canonicity: `advance t₁ F = advance t₂ F` iff `t₁` and `t₂` compare identically to every `s ∈ Beyond F`.

A consequence used below (*join preservation*): if `t'` stands for `t` and `s ∈ Beyond F`, then `s ⊔ t' = s ⊔ t`, by two applications of soundness.

## The operator, abstracted

State after round `r`: the stored update sets `S_r`, `O_r`, and a *pending set* `P_r ⊆ Beyond u_r` of deferred times.
Initially all are empty, with `u_0` the minimal frontier.

Round `r+1` computes:

1. Seeds: `N = supp b_{r+1} ∪ { p ∈ P_r : p ∉ Beyond u_{r+1} }`.
2. Interesting times: the joins of finite subsets of `supp S_r ∪ supp O_r ∪ N` containing at least one element of `N` — the set `cl (supp S_r ∪ supp O_r ∪ N) ∩ Above(N)` in `Coverage.lean`'s terms (`cl_inter_above_eq_novelJoins` characterizes it).
   (Amended 2026-07-02: the stored-output support participates in the ground set.  This is what the original implementation does, and it is what makes the round invariant re-derivable from current representatives under any adversary — the proven, unconditional path.  The output-free variant remains an open question; see PLAN.md.)
   Split it: `active` is the part not in `Beyond u_{r+1}`; `pended` is the part in `Beyond u_{r+1}` that the enumeration reaches before crossing `upper` (a join beyond `upper` is deferred and closed no further).
3. Emission: an update set `e_{r+1}` supported on `active`, chosen so that for every `t ∈ active`, `acc (O_r + e_{r+1}) t = f (acc (S_r + b_{r+1}) t)`.
   (The greedy `dtrace` construction of `Coverage.lean` builds it.)
4. `P_{r+1} = pended ∪ (P_r ∩ Beyond u_{r+1})`, and the adversary chooses fresh representatives `S_{r+1}`, `O_{r+1}`.
   (Amended 2026-07-02: the model requires only `P_{r+1} ⊇ pended ∪ (P_r ∩ Beyond u_{r+1})` with `P_{r+1} ⊆ Beyond u_{r+1}` finite — the implementation warns about MORE times than the enumeration defers, e.g. joins with already-finalized times it keeps around "to scare future times", and over-warning is harmless: a warned time owing nothing evaluates to an empty correction when due.  The exact choice above is one instance, `Step.of_pending_eq` in the Lean.)

## The goal theorem

**Stream correctness.**
For every choice of rounds, frontiers, and adversary:

    ∀ t ∉ Beyond u_n,   acc (e_1 + … + e_n) t = f (acc I_n t)

At every finalized time, the emitted corrections accumulate to the reduction of the true input.
Corollary, when the frontier eventually empties: the consolidated emitted stream is the unique difference trace of `f ∘ I` (uniqueness because a nonzero consolidated update set has a minimal support point, where its accumulation is nonzero).

## The round invariant (proof skeleton)

`INV r`, two clauses:

1. *Finalized correctness*: `∀ t ∉ Beyond u_r, acc (e_1 + … + e_r) t = f (acc I_r t)`.
2. *Pending coverage*: the staleness `s_r t = f (acc I_r t) − acc (e_1 + … + e_r) t`, restricted to `Beyond u_r`, is representable as a difference trace whose every support point lies in `cl (P_r ∪ supp S_r)` and above some element of `P_r`.

Clause 2 is the load-bearing one: it is what survives the adversary cancelling the very input updates whose joins still owe corrections, because the pended time itself carries the obligation.
Clause 2 is stated against the current representative `S_r`; its stability under re-representation rests on join preservation above.

## How `Coverage.lean` maps on

Its theorems are the exact-time, single-round instances of the target: `round_coverage` is the change term, `round_needed_set_decomposition` is the change/staleness split (its two disjuncts foreshadow the two clauses), `round_advances_invariant` is the emission step, and `draining_round_needs_pending` shows clause 2 cannot be dropped.
The `dtrace` machinery and `exists_diff_trace_vanish'` are reusable as they stand.
The generalization needed throughout: statements relative to `Beyond u` rather than global, and stored supports in place of true supports.

## Sanity scenarios the statement must express

If the formal model cannot express these, the model is wrong; if the proof cannot survive them, the proof is wrong.
Times are pairs under the product order.

1. *Cancellation*: `b_1 = {+a at (1,0), −a at (0,1)}`, `u_1 = {(1,1)}`.
   The join `(1,1)` is pended; the adversary may then represent `S_1` as the empty set, since both updates advance to `(1,1)` and cancel.
   Round 2 (empty batch, `u_2` past `(1,1)`) must still emit the retraction at `(1,1)`, and nothing in the stored input reaches it: only `P_1` does.
2. *A join coinciding with a stored time*: seed `(2,1)`, stored prior time `(3,1)`; their join is `(3,1)` itself, so a stored time at or above an interesting time is itself interesting.
3. *A stored time beyond `upper`*: the adversary may place a stored time in `Beyond u_{r+1}`; if the closure reaches it, it must be deferred, not evaluated.

## Out of scope (deliberately)

The theorem is about the model above; the implementation's fidelity to the model is checked by differential testing against an oracle, not by this proof.
In particular: that the implementation's incremental enumeration computes the same interesting-time sets as the declarative closure, that its internal buffer compaction is faithful (the same soundness lemma, applied within a round), that its evaluation order is a linear extension of the partial order, and the dataflow plumbing around it.
Model the closure and the pending set; do not model cursors, buffers, or evaluation order.
