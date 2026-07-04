# Design note: differential operators as refinements of coverage

Status: living roadmap for the reorganization that follows the compositional-adequacy
work (`Basic.lean`, `Compositional.lean`), updated as each step lands. It states the
intended *shape* of the directory and tracks how far the files have been moved toward it
(see **Progress** at the end). As each piece is realized in code its durable description
graduates into `README.md` and drops out of the aspirational prose here; the burndown
shrinks to nothing and this file is then deleted. It exists so the reorganization is a
mechanical realization of a decided structure rather than an improvisation.

## The claim

`Coverage.lean` is not "the reduce file." Its keystone (`exists_diff_trace_comp`) is
quantified over an *arbitrary* function of the accumulated input, with the sole
hypothesis that the empty collection maps to the empty collection. Read plainly, it
says:

> Any operator that computes a function of the accumulated collection emits its
> corrections only at join-closure times of the input's edits.

This is a statement about differential computation as such. REDUCE is not the subject
of the theorem; it is the *instance that uses the theorem at full strength* — it
supplies no extra structure, so the generic bound is the final bound. LINEAR and JOIN
look easy only because they supply structure that *collapses* the generic bound. The
organizing principle is therefore to prove the general operator result once, and obtain
each operator class as a tightening — not to prove three standalone squares.

## Ground

- **Accumulation** (`Trace.acc`) and **difference traces**. A collection is a difference
  trace; `acc` sums it up to a query time; `acc` is injective, so a trace is determined
  by its accumulation. Everything downstream is a property of `acc`, or of operators
  expressed through it.

## Three general properties — each stated for *any operator that is a function of accumulation*, and each *composes*

These are the roots. None mentions linear/join/reduce; all three are proven once,
generically, and each has a composition law.

1. **Adequacy — correctness.** Implementing on traces, then accumulating, equals
   accumulating, then applying the operator's mathematics. Composes (squares compose),
   so program correctness reduces to per-operator correctness. *The value axis.*
   (Today: `Compositional.Adequate`, `Adequate.comp`.)

2. **Spatial coverage — bounded work.** Output edits lie in the join-closure of input
   edits; corrections are local to the change. Composes by closure-idempotence plus
   transitivity, so a program's edits stay near its inputs' edits. *The work axis.*
   (Today: `Coverage.exists_diff_trace_comp`, but the bound is *discarded* at the seam
   in `Compositional.exists_acc_eq_comp` — see #1 below.)

3. **Compaction sufficiency — bounded state.** The correct output beyond a frontier is a
   function of the *compacted* input and the frontier; state need not grow with history.
   Frontier-parameterized, and composes (a chain's state demand is set by its strongest
   frontier requirement). *The state axis.* (Today: scattered — `acc_mapDomain` in
   `Basic`, `Compaction.advance` / `advance_le_iff`, and Model's "compaction is
   invisible" section.)

Correctness, plus two *independent* resource guarantees — spatial (work) and temporal
(state) — each holding generally and each composing. That is the trunk. The two axes are
orthogonal: an operator can be cheap on one and demanding on the other (DELTA JOIN,
below, is the witness).

## The four theorems to build generically

Two on each resource axis. All four are stated for a *general* data operator; the
operator classes inherit them with tighter constants.

- **#1 — carry the spatial bound through the square.** Prove not just "correct answer,
  finitely maintained" but "correct answer, maintained with edits confined to the
  join-closure of the input's edits." This is re-exposing a clause `Coverage` already
  proves and `Compositional` currently drops (the discarded `↑S ⊆ cl E` in
  `exists_acc_eq_comp`). Nearly plumbing; upgrades REDUCE from a near-tautology to the
  actual locality statement.

- **#2 — the spatial bound composes.** Wiring G after F keeps G's edits near the
  *original* input's edits. Holds by closure-idempotence (neighborhoods do not inflate
  under nesting) and transitivity (the "above a genuinely new time" property chains).
  Operator-agnostic; consumes #1. *This is the statement that bounded work is
  compositional* — the reason the system is worth building.

- **#1' — carry the compaction frontier through the square.** Prove "correct beyond the
  frontier *from the compacted input at that frontier*," with the frontier explicit. The
  frontier is the "additional information" the property needs.

- **#2' — bounded state composes.** The compacted-state requirement of a chain is
  governed by its *strongest* frontier demand (a DELTA JOIN downstream forces its
  predecessors to hold finer arrangements). The state-analogue of #2, and likely the
  more operationally important one, since state is what blows up in practice.

Built this way, LINEAR/JOIN/REDUCE stop being three proofs and become tags on two
theorems.

## Two independent refinement gradients

Each named operator is a *point in a two-dimensional refinement space* — data structure ×
compaction strength — not a monolith.

- **Data-domain structure** (tightens the *work* axis; fixes the increment form):
  - **LINEAR** (additive): closure collapses to the input edits exactly (an additive map
    manufactures no new join times); state-free increment.
  - **BILINEAR / JOIN** (two inputs): closure to pairwise joins; product-rule increment.
    Not literally an instance of the single-input keystone — wants a two-input coverage
    lemma to become a true corollary.
  - **DATA-PARALLEL / REDUCE** (a direct sum over keys of independent per-key functions):
    no algebraic help — the general spatial bound unchanged — plus the *orthogonal* fact
    that keys do not interact, so per-key work is independently bounded. Coverage confines
    work in time; data-parallelism confines it across keys; the two multiply.

- **Compaction strength** (tightens the *state* axis):
  - **stateless**: tolerates aggressive compaction (any old frontier).
  - **normal**: compact behind the frontier.
  - **DELTA JOIN** (dogsdogsdogs): compact only *up to just behind* the current frontier.
    Spatially unremarkable (a monotone index probe per delta) yet temporally demanding —
    the proof that the two axes are independent, and that compaction strength is a
    parameter of the state axis, not a corollary of which operator you are.

The reduce **schedule** — interesting-times enumeration, the pending/TODO set,
novel-joins, `draining_round_needs_pending` — sits *below* both gradients as the
operator-specific realization of DATA-PARALLEL + normal compaction. It is the algorithm,
not the property. This is the only genuinely near-Model material, and it is the second
piece of "additional information" the implementation carries: the frontier parameterizes
the general compaction keystone (any operator); the pending set is this schedule's
work-list (this operator).

## Two constructs outside the batch mold

- **Time-relocation operators** (`enter`, `leave`, `feedback`, `delay`): a parallel root —
  data-agnostic relocations along the time lattice, governed by monotone maps and their
  adjoints (`timeImpl`, the Galois-adjoint story), not by coverage. Support here is the
  *image* of the relocation, not a join-closure.

- **ITERATE**: couples a time operator (`feedback`) with a data operator (the body) over
  the extended lattice (outer time × round). Its correctness is **not** a coverage
  argument — it is a *causality argument on the timestamp*, a strong induction on the
  round (`acc_unique`). ITERATE is the operator where the timestamp stops being passive
  bookkeeping and becomes the thing that makes the answer well-defined; abstract the
  timestamp away and the theorem is false, because nothing pins the fixpoint. On the state
  axis, compaction *along the iteration coordinate* is exactly **stabilization**
  (`leave_stabilizes`: past the last populated round, all rounds compact to one), which
  is why ITERATE is not orphaned from the compaction keystone — it is that keystone read
  along the round.

### Confluence lives with ITERATE, and only there

Adequacy says the maintained value equals the from-scratch correct answer at every
frontier, independent of how the frontier was reached. So for every *non-recursive*
operator, two frontier-sequences both equal the correct answer at their shared query
times, hence equal each other: confluence collapses into adequacy and is trivial. The
*only* place it does not is where "the correct answer" is not a fixed target but a
**fixpoint the schedule participates in reaching** — ITERATE. There, different frontier
orders are different iteration schedules, and confluence is a genuine theorem rather than
a corollary. This is why confluence is the residual open question, and why it belongs
with the one timestamp-native operator: it is the sole operator whose answer depends on
the schedule.

## File spine reflecting the tower

1. `Basic` — `acc`, its group-hom API, injectivity.
2. `Coverage` (spatial keystone only) — function-of-accumulation ⇒ finite trace in
   join-closure; the adequacy square; the composition laws for correctness (#1) and work
   (#2). *(Split landed: the Stage-2/3 reduce schedule now lives in `RoundCoverage.lean`,
   described in `README.md`; #1/#2 remain to be added.)*
3. `Compaction` (temporal keystone, pulled up) — `acc_mapDomain` / `advance` /
   "compaction is invisible," gathered out of where they are scattered; frontier as an
   explicit parameter; the state-composition law (#1'/#2').
4. `Operators` — LINEAR / BILINEAR / DATA-PARALLEL as tightenings of the work axis, and
   stateless / normal / DELTA JOIN as tightenings of the state axis. Short corollaries,
   tagged by structure.
5. `Time` — the adjoint family; the product order; a single progress/causality notion.
6. `Iterate` — built on `Time` + `Operators`; stabilization as iteration-axis compaction;
   the home of the confluence question.
7. `RoundCoverage` + `Model` (the reduce schedule) — the deepest refinement:
   DATA-PARALLEL-REDUCE + normal compaction realized as an executable per-round schedule
   with pending obligations. `Coverage`'s former Stage-2/3 material (round coverage,
   frontier `≥ lower`, the novel-joins/pending decomposition) now lives in
   `RoundCoverage.lean` — implementation-side reasoning about a per-round schedule, not
   part of the general keystone — and `Model` builds the executable run on top of it.
   (Whether `RoundCoverage` eventually folds into `Model` is a later call; keeping it a
   distinct module first makes the keystone/schedule boundary explicit.)

Reading the tower top-down: correctness and two composing resource bounds at the trunk
(2–3); two orthogonal structural gradients that tighten them per operator (4); a separate
time-relocation root (5); ITERATE as the timestamp-coupling exception where confluence
lives (6); and the concrete reduce schedule as the bottom realization (7).

## Migration notes

- The reorganization is mostly *moving and re-exposing*, not reproving — the mathematics
  already exists; it is stacked in the wrong order (specializations proved before the
  general result they instantiate).
- The genuinely new content is the four composition theorems (#1/#2 on work, #1'/#2' on
  state). Everything else is relocation.
- Each landed step graduates its durable description into `README.md` (present tense, what
  the files are) and leaves only a checked box here — so `README` stays accurate and this
  note never competes with it as a second description.

## Progress

- [x] Land the compositional-adequacy work (#783, merged to `master-next`).
- [x] Land this design note (#784).
- [x] **Split `Coverage` into spatial-keystone vs. reduce-schedule** — the move every
  other step hangs off. `Coverage.lean` keeps the general keystone through
  `exists_diff_trace_comp`; Stage 2/3 moved to `RoundCoverage.lean` (`namespace Coverage`,
  imports `Coverage`), imported by `Model` and the umbrella. Pure relocation; verified by
  `lake build` and axiom-clean capstones (`exists_diff_trace_comp`, `round_coverage`,
  `Model.streamCorrectness_holds`, `Compositional.Program.adequate`). Graduated into
  `README.md`.
- [~] **State axis, first cut done.** `compact` + `acc_compact` (compaction invisible
  beyond the frontier, an instance of `Trace.acc_mapDomain` via `Compaction.advance_le_iff`);
  `adequate_compact_sufficient` (pointwise operators tolerate input compaction beyond the
  frontier) + `_comp` (composes). Graduated into `README.md`. Still open on this axis: the
  frontier-STRENGTH gradient (delta joins compact only up to just behind the frontier) and
  the composition of state *demand* across a pipeline (#2' proper) — the genuinely harder part.
- [ ] Pull the compaction lemmas (`acc_mapDomain` / `Compaction` / "invisible") into their
  own temporal-keystone root (spine #3) — note: `acc_mapDomain` stays foundational in `Basic`;
  the bridge is the currently-scattered `acc`-preservation lemmas. Low-value relocation; do it
  only when the frontier-strength `#1'/#2'` work gives it a purpose.
- [x] **#1 — carry the join-closure bound through the adequacy square.** Re-exposed the
  `↑S ⊆ cl E` clause `exists_acc_eq_comp` discarded; `AdequateCov` bundles it onto the
  square; `reduce_adequateCov` (general operator) and `linImpl_adequateCov` (linear
  tightening) supply it. Graduated into `README.md`.
- [x] **#2 — the join-closure bound composes.** `AdequateCov.comp` (supClosure idempotence
  + transitivity), lifted to whole programs by `Program.adequateCov` (with `AdequateCov.add`
  for par and `joinImpl_support_cl`/`AdequateCov.join` for join). Verified axiom-clean.
- [ ] #1'/#2' — the compaction-frontier analogues on the state axis.
- [ ] Factor the operator tightenings (`Operators`) and the time/iterate roots
  (`Time`, `Iterate`); confluence is the residual open question.
