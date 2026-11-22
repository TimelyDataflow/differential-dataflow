import Differential.Basic
import Differential.Coverage
import Differential.RoundCoverage
import Differential.Compaction

/-!
# The incremental model of `reduce` (TARGETS2.md, "The model" onward)

This file states, in Lean, the model and goal theorem of `TARGETS2.md` — and PROVES the
goal theorem, unconditionally (`Run.stream_correct`, `streamCorrectness_holds`): every
run, under every accumulation-preserving adversary, is stream correct.  One deliberate
amendment to TARGETS2 (see its step 2): the enumeration's ground set includes the stored
(post-consolidation) OUTPUT support, matching what the implementation does; the pending
coverage is then re-derived each round from current representatives and never crosses
the adversary boundary.  The OUTPUT-FREE enumeration (TARGETS2's original step 2)
remains an open question, described in the amendment note there.
Nothing here is `sorry`ed.

Correspondence with TARGETS2.md:

* "update set", "accumulation"        → `T →₀ A`, `acc`
* "Beyond u"                          → `Beyond`
* "Compaction, modeled adversarially" → the `stored_agrees`/`output_agrees` fields of `Step`
  (any representative preserving accumulations on `Beyond upper`); the recorded `advance`
  mechanism and its two facts live in `Differential/Compaction.lean`
  (`Compaction.advance_le_iff`, `Compaction.advance_eq_iff`); `acc_mapDomain` here shows
  advance-and-consolidate is one instance of the adversary
* "The operator, abstracted", steps 1–4 → `seedSet`, `Reached`/`activeSet`/`pendedSet`
  (step 2, with the truncated closure: a time reached in `Beyond upper` is closed no
  further; the enumeration may join against a VIEW of the state, `viewReached` and
  `Enumerates`, such as the state cancelled by a time below every seed,
  `meet_view_agrees`), the `emit_*` fields of `Enumerates` (step 3), the `pending_*`
  fields of `Enumerates` and `Step` (step 4, relaxed: the implementation may warn about
  MORE times than the enumeration defers, so long as they stay beyond the frontier, and
  may warn about a LOWER time in place of a deferred one; `Step.of_pending_eq` recovers
  the exact choice)
* "The goal theorem"                  → `Run.StreamCorrect`, `StreamCorrectness`
* "The round invariant"               → `Run.INV` (clause 1 = finalized correctness,
  clause 2 = pending coverage); `inv_zero` is the base case
* "Sanity scenarios"                  → `scenario1_cancels` (the adversary may store the
  empty set while the pending join owes a retraction), `Reached.absorb` (a stored time
  at/above an interesting time is itself interesting), `Enumerates.reached_beyond_pended` and
  `active_not_beyond` (a reached time beyond `upper` is deferred, never evaluated)
-/

set_option linter.style.openClassical false
set_option linter.style.show false

open Finset
open scoped Classical

namespace Model

open Coverage
open Trace

variable {T : Type*} [SemilatticeSup T]
variable {A : Type*} [AddCommGroup A]

/-- `Beyond u` is the upper set of the finite antichain `u`: every `t` with `x ≤ t` for
    some `x ∈ u`. -/
def Beyond (u : Finset T) : Set T := {t | ∃ x ∈ u, x ≤ t}

theorem beyond_isUpperSet (u : Finset T) : IsUpperSet (Beyond u) := by
  rintro a b hab ⟨x, hx, hxa⟩
  exact ⟨x, hx, hxa.trans hab⟩

/-! ## Update sets and accumulation

`acc` and its group-hom API live in `Differential.Basic` (`namespace Trace`), reached here via
`open Trace`.  Only the bridge to `Coverage.Represents` — which needs both in scope — stays here. -/

/-- The bridge to `Coverage.lean`: an update set is a difference trace of its own
    accumulation, supported on its support. -/
theorem acc_represents (d : T →₀ A) : Represents d.support ⇑d (acc d) := by
  intro t; rfl

/-! ### Intra-round compaction is invisible

The implementation also compacts WITHIN a round: moving through the interesting times in
order, it advances its input and output buffers by joining their times with the meet `m`
of every time still to be examined, and consolidates.  Because `m` is at or below
everything the round still reads, this needs no adversary move at all: the two lemmas
below (TARGETS2.md's *join preservation*, specialized to a one-element frontier) say the
compacted buffer yields the same accumulation at every remaining evaluation point and the
same join against every remaining seed as the uncompacted history would.

Consolidation can also CANCEL: updates that land on the same time and sum to zero vanish,
and their times stop being join partners.  That is licensed by reading the round through a
view (`Enumerates`, `meet_view_agrees`): a round may enumerate against its input cancelled
by any time below its own seeds.  Cancelling by a meet that rises during the round is the
same as running a finer schedule, split at intermediate frontiers, each piece cancelling by
a time below its own seeds; every such run is correct at every frontier it passes
(`Run.correct_at`). -/

/-- Advancing by a meet below `t` and consolidating leaves the accumulation at `t`
    unchanged. -/
theorem acc_mapDomain_sup (d : T →₀ A) {m t : T} (hmt : m ≤ t) :
    acc (Finsupp.mapDomain (· ⊔ m) d) t = acc d t :=
  acc_mapDomain _ d t fun _ _ => ⟨fun h => le_trans le_sup_left h, fun h => sup_le h hmt⟩

/-- Joins against a seed above the meet are unchanged by advancing the joinee, so the
    enumeration discovers the same interesting times from the compacted buffer. -/
theorem sup_advance_eq {m s : T} (x : T) (hms : m ≤ s) : s ⊔ (x ⊔ m) = s ⊔ x := by
  rw [sup_comm x m, ← sup_assoc, sup_eq_left.mpr hms]

/-! ## The operator, abstracted (`TARGETS2.md`, steps 1–4) -/

/-- Operator state after a round: the stored input and output representatives and the
    pending set of deferred times. -/
structure State (T : Type*) (A : Type*) [SemilatticeSup T] [AddCommGroup A] where
  stored : T →₀ A
  output : T →₀ A
  pending : Set T

/-- Step 1: seeds are the batch's support plus the pending times now inside the interval. -/
def seedSet (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  ↑b.support ∪ (σ.pending \ Beyond upper)

/-- The times available to join against, read through a VIEW `(vS, vO)` of the stored input
    and output: the view's input times, its (post-consolidation) OUTPUT times, and seeds.
    A view may be the state itself (`baseSet`), or the state compacted by a time below every
    seed, with whatever cancels gone (`meet_view_agrees`). -/
def viewBase (vS vO : T →₀ A) (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  ↑vS.support ∪ ↑vO.support ∪ seedSet σ b upper

/-- The times available to join against: stored input times, stored (post-consolidation)
    OUTPUT times, and seeds.  Including the output support is what lets the pending
    coverage be re-derived from current representatives each round (`staleness_rederived`),
    dissolving the re-representation question for the fully general adversary. -/
def baseSet (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  viewBase σ.stored σ.output σ b upper

/-- Step 2, with the truncated closure: the enumeration seeds on `N`, and joins a reached
    time that is NOT beyond `upper` against base times and other such reached times.  A
    time reached in `Beyond upper` is deferred and closed no further. -/
inductive Reached (base : Set T) (N : Set T) (upper : Finset T) : T → Prop
  | seed {x : T} : x ∈ N → Reached base N upper x
  | joinBase {x y : T} : Reached base N upper x → x ∉ Beyond upper → y ∈ base →
      Reached base N upper (x ⊔ y)
  | joinActive {x y : T} : Reached base N upper x → x ∉ Beyond upper →
      Reached base N upper y → y ∉ Beyond upper → Reached base N upper (x ⊔ y)

/-- The times the enumeration reaches, joining against a view. -/
def viewReached (vS vO : T →₀ A) (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  {x | Reached (viewBase vS vO σ b upper) (seedSet σ b upper) upper x}

/-- The interesting times evaluated this round, enumerating against a view. -/
def viewActive (vS vO : T →₀ A) (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  viewReached vS vO σ b upper \ Beyond upper

/-- The interesting times deferred to later rounds, enumerating against a view. -/
def viewPended (vS vO : T →₀ A) (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  viewReached vS vO σ b upper ∩ Beyond upper

def reachedSet (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  viewReached σ.stored σ.output σ b upper

/-- The interesting times evaluated this round. -/
def activeSet (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  viewActive σ.stored σ.output σ b upper

/-- The interesting times deferred to later rounds. -/
def pendedSet (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  viewPended σ.stored σ.output σ b upper

/-- What a round's enumeration owes, when it joins against the view `(vS, vO)` (TARGETS2
    steps 2–4).  The view must agree with the stored input and output on some upper set
    holding every seed; everything the round reads is at or above a seed, so the view
    accumulates the same there, and its cancellations are invisible to the round.  The
    emission is supported on the view's active times and is correct there; every time the
    view's enumeration defers, and every carried pending time, is at or above some time of
    the new pending set. -/
structure Enumerates (f : A → A) (upper : Finset T) (b : T →₀ A) (σ : State T A)
    (vS vO : T →₀ A) (e : T →₀ A) (σ' : State T A) : Prop where
  view_agrees : ∃ U : Set T, IsUpperSet U ∧ seedSet σ b upper ⊆ U ∧
    (∀ t ∈ U, acc vS t = acc σ.stored t) ∧ (∀ t ∈ U, acc vO t = acc σ.output t)
  emit_support : ↑e.support ⊆ viewActive vS vO σ b upper
  emit_correct : ∀ t ∈ viewActive vS vO σ b upper,
    acc (σ.output + e) t = f (acc (σ.stored + b) t)
  pending_below : ∀ x ∈ viewPended vS vO σ b upper ∪ (σ.pending ∩ Beyond upper),
    ∃ p ∈ σ'.pending, p ≤ x

/-- One round of the operator (TARGETS2 steps 3–4), from the state before the round to the
    state after, delivering batch `b` and advancing the frontier to `upper`.

    The enumeration joins against some view of the state (`Enumerates`); the state itself
    is one such view (`Step.of_pending_eq`).
    The emission `e` is supported on the active times and makes the output correct there
    (step 3).  Every deferred time, and every carried one, is at or above some time of
    the new pending set, which is confined beyond the frontier (step 4, relaxed): the
    implementation warns about more times than the enumeration defers — e.g. joins with
    already-finalized times it kept around "to scare future times" — and over-warning is
    harmless, since a warned time that owes nothing is evaluated to an empty correction
    when it comes due.  It may also warn about a time in place of the deferred joins above
    it: a reached time beyond `upper` is pended without its joins against the times the
    sweep has already passed.  Only the upper set the pending times generate matters,
    because a pend is a witness ("some pending time lies below"), not an address.  The
    new stored input and output are ADVERSARIAL: any update sets whose accumulations
    agree with the true ones on `Beyond upper` ("Compaction, modeled adversarially"). -/
structure Step (f : A → A) (upper : Finset T) (b : T →₀ A)
    (σ : State T A) (e : T →₀ A) (σ' : State T A) : Prop where
  enumerates : ∃ vS vO : T →₀ A, Enumerates f upper b σ vS vO e σ'
  pending_confined : σ'.pending ⊆ Beyond upper
  pending_finite : σ'.pending.Finite
  stored_agrees : ∀ t ∈ Beyond upper, acc σ'.stored t = acc (σ.stored + b) t
  output_agrees : ∀ t ∈ Beyond upper, acc σ'.output t = acc (σ.output + e) t

/-- A run of `n` rounds: frontiers `u_0 … u_n` ordered by upper-set containment with
    `u_0` covering everything, batches supported in their intervals, and a `Step` between
    consecutive states. -/
structure Run (T : Type*) (A : Type*) [SemilatticeSup T] [AddCommGroup A]
    (f : A → A) (n : ℕ) where
  frontier : ℕ → Finset T
  batch : ℕ → T →₀ A
  state : ℕ → State T A
  emit : ℕ → T →₀ A
  frontier_zero : Beyond (frontier 0) = Set.univ
  frontier_mono : ∀ r < n, Beyond (frontier (r + 1)) ⊆ Beyond (frontier r)
  state_zero : state 0 = ⟨0, 0, ∅⟩
  batch_support : ∀ r < n,
      ↑(batch (r + 1)).support ⊆ Beyond (frontier r) \ Beyond (frontier (r + 1))
  step : ∀ r < n, Step f (frontier (r + 1)) (batch (r + 1)) (state r) (emit (r + 1))
      (state (r + 1))

variable {f : A → A} {n : ℕ}

/-- The true input after round `r`: the sum of the delivered batches. -/
noncomputable def Run.input (R : Run T A f n) (r : ℕ) : T →₀ A :=
  ∑ k ∈ Finset.range r, R.batch (k + 1)

/-- The corrections emitted through round `r`. -/
noncomputable def Run.emitted (R : Run T A f n) (r : ℕ) : T →₀ A :=
  ∑ k ∈ Finset.range r, R.emit (k + 1)

/-! ## The goal theorem and the round invariant (`TARGETS2.md`) -/

/-- **Stream correctness**: at every finalized time, the emitted corrections accumulate to
    the reduction of the true input. -/
def Run.StreamCorrect (R : Run T A f n) : Prop :=
  ∀ t, t ∉ Beyond (R.frontier n) → acc (R.emitted n) t = f (acc (R.input n) t)

/-- THE TARGET: every run of the operator, for every choice of rounds, frontiers, and
    adversary, is stream correct. -/
def StreamCorrectness (T A : Type*) [SemilatticeSup T] [AddCommGroup A]
    (f : A → A) : Prop :=
  ∀ (n : ℕ) (R : Run T A f n), R.StreamCorrect

/-- What the emitted corrections still owe: `f` of the true input, minus what was emitted. -/
noncomputable def Run.staleness (R : Run T A f n) (r : ℕ) : T → A :=
  fun t => f (acc (R.input r) t) - acc (R.emitted r) t

/-- The round invariant `INV r`.

    Clause 1 (*finalized correctness*): nothing is owed at times not beyond `u_r`.
    Clause 2 (*pending coverage*): what is owed beyond `u_r` is representable as a
    difference trace whose every support point is a join of pending and stored times, at
    or above some pending time.  Clause 2 is the load-bearing one: the pended time itself
    carries the obligation, surviving the adversary cancelling the input evidence. -/
def Run.INV (R : Run T A f n) (r : ℕ) : Prop :=
  (∀ t, t ∉ Beyond (R.frontier r) → acc (R.emitted r) t = f (acc (R.input r) t)) ∧
  ∃ (SP : Finset T) (dP : T → A),
    (∀ t ∈ Beyond (R.frontier r), R.staleness r t = ∑ s ∈ Cut SP t, dP s) ∧
    ∀ x, dP x ≠ 0 →
      x ∈ supClosure ((R.state r).pending ∪ ↑(R.state r).stored.support
          ∪ ↑(R.state r).output.support) ∧
      ∃ p ∈ (R.state r).pending, p ≤ x

/-- The base case of the induction: `INV 0` (before any round, nothing is finalized,
    nothing is owed). -/
theorem inv_zero (hf0 : f 0 = 0) (R : Run T A f n) : R.INV 0 := by
  constructor
  · intro t ht
    exact absurd (R.frontier_zero ▸ Set.mem_univ t) ht
  · refine ⟨∅, fun _ => 0, ?_, by simp⟩
    intro t _
    simp [Run.staleness, Run.input, Run.emitted, hf0, Cut]

/-! ## Sanity scenarios (`TARGETS2.md`) -/

/-- Scenario 2: a stored (or seed) time at or above a reached active time is itself
    reached — the join is the stored time itself. -/
theorem Reached.absorb {base N : Set T} {upper : Finset T} {x y : T}
    (hx : Reached base N upper x) (hact : x ∉ Beyond upper) (hy : y ∈ base) (hxy : x ≤ y) :
    Reached base N upper y := by
  have h := Reached.joinBase hx hact hy
  rwa [sup_eq_right.mpr hxy] at h

/-- Scenario 3, half one: active times are never beyond `upper`, so a stored time the
    adversary placed beyond `upper` is never evaluated. -/
theorem active_not_beyond {σ : State T A} {b : T →₀ A} {upper : Finset T} {x : T}
    (hx : x ∈ activeSet σ b upper) : x ∉ Beyond upper := hx.2

/-- Scenario 3, half two: a reached time beyond `upper` is deferred: it is at or above a
    time of the next pending set. -/
theorem Enumerates.reached_beyond_pended {f : A → A} {upper : Finset T} {b : T →₀ A}
    {σ σ' : State T A} {vS vO e : T →₀ A} (hs : Enumerates f upper b σ vS vO e σ')
    {x : T} (hr : x ∈ viewReached vS vO σ b upper) (hb : x ∈ Beyond upper) :
    ∃ p ∈ σ'.pending, p ≤ x :=
  hs.pending_below x (Or.inl ⟨hr, hb⟩)

/-- **Cancelling by the meet is a view.**  Compacting the stored input and output by any
    time `m` at or below every seed — advancing each time to its join with `m` and
    consolidating, so updates that land together and cancel are gone — agrees with the
    state on `Above m`, which holds every seed.  A round whose seeds all lie above `m` may
    therefore enumerate against its input cancelled by `m`.  Splitting a round at more
    frontiers raises the seeds of each piece, hence the `m` each piece may cancel by: the
    finer schedule cancels more. -/
theorem meet_view_agrees {σ : State T A} {b : T →₀ A} {upper : Finset T} {m : T}
    (hm : ∀ s ∈ seedSet σ b upper, m ≤ s) :
    ∃ U : Set T, IsUpperSet U ∧ seedSet σ b upper ⊆ U ∧
      (∀ t ∈ U, acc (Finsupp.mapDomain (· ⊔ m) σ.stored) t = acc σ.stored t) ∧
      (∀ t ∈ U, acc (Finsupp.mapDomain (· ⊔ m) σ.output) t = acc σ.output t) :=
  ⟨{t | m ≤ t}, fun _ _ hab ha => le_trans ha hab, hm,
    fun _ ht => acc_mapDomain_sup _ ht, fun _ ht => acc_mapDomain_sup _ ht⟩

/-- Scenario 1 (cancellation), concretely over `ℕ × ℕ` and `ℤ`: the batch
    `{+1 at (1,0), −1 at (0,1)}` accumulates to zero at every point of `Beyond {(1,1)}`,
    so `Step.stored_agrees` admits the EMPTY stored set — while the pended join `(1,1)`
    still owes a retraction reachable only through the pending set. -/
theorem scenario1_cancels :
    ∀ t ∈ Beyond ({((1 : ℕ), (1 : ℕ))} : Finset (ℕ × ℕ)),
      acc (Finsupp.single ((1 : ℕ), (0 : ℕ)) (1 : ℤ)
         + Finsupp.single ((0 : ℕ), (1 : ℕ)) (-1 : ℤ)) t = 0 := by
  rintro t ⟨x, hx, hxt⟩
  simp only [Finset.mem_singleton] at hx
  subst hx
  have h10 : ((1 : ℕ), (0 : ℕ)) ≤ t :=
    le_trans (Prod.mk_le_mk.mpr ⟨le_refl 1, Nat.zero_le 1⟩) hxt
  have h01 : ((0 : ℕ), (1 : ℕ)) ≤ t :=
    le_trans (Prod.mk_le_mk.mpr ⟨Nat.zero_le 1, le_refl 1⟩) hxt
  rw [acc_add, acc_single, acc_single, if_pos h10, if_pos h01]
  ring

/-! ## Agreeing cuts restrict -/

/-- Agreeing cuts over a larger set agree over a smaller one. -/
theorem cut_mono_congr {E C : Finset T} (hEC : E ⊆ C) {s t : T}
    (h : Cut C s = Cut C t) : Cut E s = Cut E t := by
  ext x
  simp only [Cut, Finset.mem_filter]
  constructor
  · rintro ⟨hxE, hxs⟩
    have hx : x ∈ Cut C s := by
      simp only [Cut, Finset.mem_filter]; exact ⟨hEC hxE, hxs⟩
    rw [h] at hx
    exact ⟨hxE, (Finset.mem_filter.mp hx).2⟩
  · rintro ⟨hxE, hxt⟩
    have hx : x ∈ Cut C t := by
      simp only [Cut, Finset.mem_filter]; exact ⟨hEC hxE, hxt⟩
    rw [← h] at hx
    exact ⟨hxE, (Finset.mem_filter.mp hx).2⟩

/-- The round's change term, over the STORED trace: `f∘acc(S+b) − f∘acc S` is a difference
    trace supported on joins of stored ∪ batch times involving at least one batch time.
    This is `Coverage.lean`'s `round_coverage` applied to the stored representative —
    the true input never enters, which is where cancellation soundness dissolves. -/
theorem change_term_coverage (S b : T →₀ A) (f : A → A) :
    ∃ (SP : Finset T) (d : T → A),
      (∀ x, d x ≠ 0 → x ∈ cl (S.support ∪ b.support) ∧ ∃ nu ∈ b.support, nu ≤ x) ∧
      Represents SP d (fun t => f (acc (S + b) t) - f (acc S t)) := by
  obtain ⟨SP, d, hsupp, hrep⟩ :=
    round_coverage (acc_represents S) (acc_represents b) f
  exact ⟨SP, d, hsupp, hrep.congr (fun t => by rw [acc_add])⟩

/-! ## The compaction bridge

The change term over the TRUE input equals the change term over any STORED representative,
globally: they agree on `Beyond u` because accumulations do, and both vanish elsewhere
because the batch is invisible there.  So the change coverage computed from the stored
support covers the true change — cancellation costs nothing. -/

/-- An update set supported in `Beyond u` accumulates to zero outside it. -/
theorem acc_zero_off_beyond {u : Finset T} {d : T →₀ A} (hd : ↑d.support ⊆ Beyond u)
    {t : T} (ht : t ∉ Beyond u) : acc d t = 0 := by
  unfold acc
  apply Finset.sum_eq_zero
  intro x hx
  simp only [Finset.mem_filter] at hx
  exact absurd (beyond_isUpperSet u hx.2 (hd hx.1)) ht

/-- The true and stored change terms coincide everywhere. -/
theorem change_term_eq_stored {u : Finset T} {S I b : T →₀ A} (f : A → A)
    (hagree : ∀ t ∈ Beyond u, acc S t = acc I t)
    (hb : ↑b.support ⊆ Beyond u) (t : T) :
    f (acc (I + b) t) - f (acc I t) = f (acc (S + b) t) - f (acc S t) := by
  by_cases ht : t ∈ Beyond u
  · rw [acc_add, acc_add, hagree t ht]
  · rw [acc_add, acc_add, acc_zero_off_beyond hb ht, add_zero, add_zero,
      sub_self, sub_self]

/-- **The compaction bridge.**  The TRUE change term is a difference trace supported on
    the joins the operator computes from its STORED representative — `cl(supp S ∪ supp b)`
    above a batch time.  The adversary's cancellations are invisible here: the theorem
    quantifies over the representative. -/
theorem true_change_coverage {u : Finset T} {S I b : T →₀ A} (f : A → A)
    (hagree : ∀ t ∈ Beyond u, acc S t = acc I t) (hb : ↑b.support ⊆ Beyond u) :
    ∃ (SP : Finset T) (d : T → A),
      (∀ x, d x ≠ 0 → x ∈ cl (S.support ∪ b.support) ∧ ∃ nu ∈ b.support, nu ≤ x) ∧
      Represents SP d (fun t => f (acc (I + b) t) - f (acc I t)) := by
  obtain ⟨SP, d, hsupp, hrep⟩ := change_term_coverage S b f
  exact ⟨SP, d, hsupp, hrep.congr (fun t => (change_term_eq_stored f hagree hb t).symm)⟩

/-! ## Coverage through a view

The enumeration need not read the stored state.  Any VIEW agreeing with the true input and
output on an upper set `U` holding every seed will do: everything the round must touch is
in the view's join-closure, above a seed, or above a pending time not yet due.  The change
term is covered from the view's support (`view_change_coverage`).  The round-start
staleness is RE-DERIVED over the view (`view_staleness`): the trace the invariant carries is
not reused, only the fact that staleness vanishes where no pending time reaches. -/

/-- An update set supported in an upper set accumulates to zero outside it. -/
theorem acc_zero_off_upper {U : Set T} (hU : IsUpperSet U) {d : T →₀ A}
    (hd : ↑d.support ⊆ U) {t : T} (ht : t ∉ U) : acc d t = 0 := by
  unfold acc
  apply Finset.sum_eq_zero
  intro x hx
  simp only [Finset.mem_filter] at hx
  exact absurd (hU hx.2 (hd hx.1)) ht

/-- Accumulations depend only on the cut over any superset of the support. -/
theorem acc_cut_congr (d : T →₀ A) {E : Finset T} (hdE : d.support ⊆ E) {s t : T}
    (hst : Cut E s = Cut E t) : acc d s = acc d t := by
  have h : Cut d.support s = Cut d.support t := cut_mono_congr hdE hst
  unfold acc
  exact congrArg (fun F => ∑ x ∈ F, d x) h

/-- **Change term over a view.**  A view `S` agreeing with the input `I` on an upper set
    holding the batch support covers the change term `f∘acc(I+b) − f∘acc I`, from its own
    support.  `true_change_coverage` is the case `U = Beyond u`. -/
theorem view_change_coverage {U : Set T} (hU : IsUpperSet U) {S I b : T →₀ A} (f : A → A)
    (hagree : ∀ t ∈ U, acc S t = acc I t) (hb : ↑b.support ⊆ U) :
    ∃ (SP : Finset T) (d : T → A),
      (∀ x, d x ≠ 0 → x ∈ cl (S.support ∪ b.support) ∧ ∃ nu ∈ b.support, nu ≤ x) ∧
      Represents SP d (fun t => f (acc (I + b) t) - f (acc I t)) := by
  obtain ⟨SP, d, hsupp, hrep⟩ := change_term_coverage S b f
  refine ⟨SP, d, hsupp, hrep.congr (fun t => ?_)⟩
  by_cases ht : t ∈ U
  · simp only [acc_add, hagree t ht]
  · simp only [acc_add, acc_zero_off_upper hU hb ht, add_zero, sub_self]

/-- **Staleness over a view.**  Let `P` be the pending times, `due ⊆ P` those coming due, and
    `S'`, `O'` a view agreeing with the input `I` and output `O` on an upper set `U` holding
    `due`.  If the staleness `f∘acc I − acc O` vanishes where no pending time reaches, it has a
    trace whose every live point is above a carried time of `P \ due`, or is in the closure of
    `due` and the VIEW's supports, above a due time. -/
theorem view_staleness {P due : Finset T} {U : Set T} (hU : IsUpperSet U) (hdueU : ↑due ⊆ U)
    {I O S' O' : T →₀ A} (f : A → A)
    (hS : ∀ t ∈ U, acc S' t = acc I t) (hO : ∀ t ∈ U, acc O' t = acc O t)
    (hvan : ∀ t, (∀ p ∈ P, ¬ p ≤ t) → f (acc I t) - acc O t = 0) :
    ∃ (SQ : Finset T) (dQ : T → A),
      Represents SQ dQ (fun t => f (acc I t) - acc O t) ∧
      ∀ x, dQ x ≠ 0 →
        (∃ p ∈ P \ due, p ≤ x) ∨
        (x ∈ cl (due ∪ S'.support ∪ O'.support) ∧ ∃ p ∈ due, p ≤ x) := by
  classical
  set g : T → A := fun t => f (acc I t) - acc O t with hg
  -- The part above a due time, read through the view.
  set gd : T → A := fun t => if ∃ p ∈ due, p ≤ t then g t else 0 with hgd
  set E1 : Finset T := due ∪ S'.support ∪ O'.support with hE1
  have hdueE1 : due ⊆ E1 := fun x hx => by simp [hE1, hx]
  have hS'E1 : S'.support ⊆ E1 := fun x hx => by simp [hE1, hx]
  have hO'E1 : O'.support ⊆ E1 := fun x hx => by simp [hE1, hx]
  have hdue_cut : ∀ s t, Cut E1 s = Cut E1 t →
      ((∃ p ∈ due, p ≤ s) ↔ (∃ p ∈ due, p ≤ t)) := by
    intro s t hst
    have hc : Cut due s = Cut due t := cut_mono_congr hdueE1 hst
    constructor
    · rintro ⟨p, hp, hps⟩
      have hmem : p ∈ Cut due s := by simp [Cut, hp, hps]
      rw [hc] at hmem
      exact ⟨p, hp, (Finset.mem_filter.mp hmem).2⟩
    · rintro ⟨p, hp, hpt⟩
      have hmem : p ∈ Cut due t := by simp [Cut, hp, hpt]
      rw [← hc] at hmem
      exact ⟨p, hp, (Finset.mem_filter.mp hmem).2⟩
  have hsimple1 : Simple E1 gd := by
    intro s t hst
    by_cases hs : ∃ p ∈ due, p ≤ s
    · have ht := (hdue_cut s t hst).mp hs
      have hsU : s ∈ U := by obtain ⟨p, hp, hps⟩ := hs; exact hU hps (hdueU hp)
      have htU : t ∈ U := by obtain ⟨p, hp, hpt⟩ := ht; exact hU hpt (hdueU hp)
      simp only [hgd, if_pos hs, if_pos ht, hg]
      rw [← hS s hsU, ← hO s hsU, ← hS t htU, ← hO t htU,
        acc_cut_congr S' hS'E1 hst, acc_cut_congr O' hO'E1 hst]
    · have ht : ¬ ∃ p ∈ due, p ≤ t := fun h => hs ((hdue_cut s t hst).mpr h)
      simp only [hgd, if_neg hs, if_neg ht]
  have hbase1 : ∀ t, (∀ c ∈ E1, ¬ c ≤ t) → gd t = 0 := by
    intro t ht
    have hn : ¬ ∃ p ∈ due, p ≤ t := fun ⟨p, hp, hpt⟩ => ht p (hdueE1 hp) hpt
    simp only [hgd, if_neg hn]
  have hD1 : IsLowerSet {t : T | ¬ ∃ p ∈ due, p ≤ t} := by
    intro a b hba ha ⟨p, hp, hpb⟩
    exact ha ⟨p, hp, le_trans hpb hba⟩
  obtain ⟨S1, d1, -, hsupp1, hrep1⟩ :=
    exists_diff_trace_vanish' hsimple1 hbase1 hD1 (fun t ht => by
      simp only [Set.mem_setOf_eq] at ht
      simp only [hgd, if_neg ht])
  -- The rest, above carried times only.
  set gr : T → A := fun t => g t - gd t with hgr
  set E2 : Finset T := P ∪ I.support ∪ O.support ∪ E1 with hE2
  have hsimple2 : Simple E2 gr := by
    have hI : Simple E2 (fun t => f (acc I t)) :=
      (((acc_represents I).simple).comp f).mono (fun x hx => by simp [hE2, hx])
    have hOs : Simple E2 (acc O) :=
      ((acc_represents O).simple).mono (fun x hx => by simp [hE2, hx])
    have hgS : Simple E2 g := hI.sub hOs
    exact hgS.sub (hsimple1.mono (fun x hx => by simp [hE2, hx]))
  have hbase2 : ∀ t, (∀ c ∈ E2, ¬ c ≤ t) → gr t = 0 := by
    intro t ht
    have hg0 : g t = 0 := hvan t (fun p hp => ht p (by simp [hE2, hp]))
    have hgd0 : gd t = 0 := hbase1 t (fun c hc => ht c (by simp [hE2, hc]))
    simp only [hgr, hg0, hgd0, sub_self]
  have hD2 : IsLowerSet {t : T | ¬ ∃ p ∈ P \ due, p ≤ t} := by
    intro a b hba ha ⟨p, hp, hpb⟩
    exact ha ⟨p, hp, le_trans hpb hba⟩
  obtain ⟨S2, d2, -, hsupp2, hrep2⟩ :=
    exists_diff_trace_vanish' hsimple2 hbase2 hD2 (fun t ht => by
      simp only [Set.mem_setOf_eq] at ht
      by_cases hd : ∃ p ∈ due, p ≤ t
      · simp only [hgr, hgd, if_pos hd, sub_self]
      · have hnone : ∀ p ∈ P, ¬ p ≤ t := by
          intro p hp hpt
          by_cases hpd : p ∈ due
          · exact hd ⟨p, hpd, hpt⟩
          · exact ht ⟨p, Finset.mem_sdiff.mpr ⟨hp, hpd⟩, hpt⟩
        have hg0 : g t = 0 := hvan t hnone
        simp only [hgr, hgd, if_neg hd, hg0, sub_self])
  -- Together: `gd + gr = g`.
  refine ⟨S1 ∪ S2, fun s => (if s ∈ S1 then d1 s else 0) + (if s ∈ S2 then d2 s else 0),
    (hrep1.add hrep2).congr (fun t => by simp only [hgr]; abel), ?_⟩
  intro x hx
  rcases Represents.add_support hx with h | h
  · obtain ⟨hcl, hxD⟩ := hsupp1 x h
    simp only [Set.mem_setOf_eq, not_not] at hxD
    exact Or.inr ⟨hcl, hxD⟩
  · obtain ⟨-, hxD⟩ := hsupp2 x h
    simp only [Set.mem_setOf_eq, not_not] at hxD
    exact Or.inl hxD

/-! ## Cut toolkit -/

/-- Splitting a cut-sum of an indicator-combined trace over a union of supports. -/
theorem cut_sum_union (S1 S2 : Finset T) (d1 d2 : T → A) (t : T) :
    ∑ s ∈ Cut (S1 ∪ S2) t, ((if s ∈ S1 then d1 s else 0) + (if s ∈ S2 then d2 s else 0))
      = (∑ s ∈ Cut S1 t, d1 s) + ∑ s ∈ Cut S2 t, d2 s := by
  rw [Finset.sum_add_distrib]
  congr 1
  · rw [← Finset.sum_filter]
    congr 1
    ext s; simp only [Cut, Finset.mem_filter, Finset.mem_union]; tauto
  · rw [← Finset.sum_filter]
    congr 1
    ext s; simp only [Cut, Finset.mem_filter, Finset.mem_union]; tauto

/-! ## The enumeration reaches the closure

Two directions of TARGETS2's step 2.  A closure point `x` (a join of base times with a
seed below it) below `upper` is REACHED: build it by joining one element at a time from
the seed; every partial join is `≤ x`, hence also below `upper`, so each step is licensed.
A closure point beyond `upper` is at or above a PENDED time: walk the same chain and stop
at the first partial join that crosses `upper` — it is reached, beyond, and `≤ x`. -/

theorem le_foldl_sup (l : List T) (x : T) : x ≤ l.foldl (· ⊔ ·) x := by
  induction l generalizing x with
  | nil => exact le_refl x
  | cons y l' ih => exact le_trans le_sup_left (by simpa using ih (x ⊔ y))

theorem foldl_sup_le {l : List T} {x z : T} (hx : x ≤ z) (hl : ∀ y ∈ l, y ≤ z) :
    l.foldl (· ⊔ ·) x ≤ z := by
  induction l generalizing x with
  | nil => exact hx
  | cons y l' ih =>
    simp only [List.foldl_cons]
    exact ih (sup_le hx (hl y (by simp))) (fun w hw => hl w (by simp [hw]))

theorem mem_le_foldl_sup {l : List T} {y : T} (hy : y ∈ l) (x : T) :
    y ≤ l.foldl (· ⊔ ·) x := by
  induction l generalizing x with
  | nil => simp at hy
  | cons a l' ih =>
    simp only [List.foldl_cons]
    rcases List.mem_cons.mp hy with rfl | h
    · exact le_trans le_sup_right (le_foldl_sup l' (x ⊔ y))
    · exact ih h (x ⊔ a)

/-- A nonempty join equals the fold from any point below it over the elements. -/
theorem foldl_toList_eq {Y : Finset T} (hne : Y.Nonempty) {n x : T}
    (hsup : Y.sup' hne id = x) (hnx : n ≤ x) : Y.toList.foldl (· ⊔ ·) n = x := by
  apply le_antisymm
  · refine foldl_sup_le hnx (fun y hy => ?_)
    rw [← hsup]
    exact Finset.le_sup' id (Finset.mem_toList.mp hy)
  · rw [← hsup]
    exact Finset.sup'_le hne id (fun y hy => mem_le_foldl_sup (Finset.mem_toList.mpr hy) n)

/-- Joining a reached time with base elements, staying below `upper`, stays reached. -/
theorem Reached.foldl {base N : Set T} {upper : Finset T} {x : T}
    (hx : Reached base N upper x) (l : List T) (hl : ∀ y ∈ l, y ∈ base)
    (hact : l.foldl (· ⊔ ·) x ∉ Beyond upper) :
    Reached base N upper (l.foldl (· ⊔ ·) x) := by
  induction l generalizing x with
  | nil => exact hx
  | cons y l' ih =>
    simp only [List.foldl_cons] at hact ⊢
    have hxact : x ∉ Beyond upper := by
      intro hc
      exact hact (beyond_isUpperSet upper
        (le_trans le_sup_left (le_foldl_sup l' (x ⊔ y))) hc)
    exact ih (Reached.joinBase hx hxact (hl y (by simp)))
      (fun w hw => hl w (by simp [hw])) hact

/-- Walking a join chain that ends beyond `upper`: some prefix is reached, beyond
    `upper`, and below the total — the FIRST CROSSING is a pended witness. -/
theorem exists_crossing_of_foldl {base N : Set T} {upper : Finset T} {x : T}
    (hx : Reached base N upper x) (l : List T) (hl : ∀ y ∈ l, y ∈ base)
    (hbey : l.foldl (· ⊔ ·) x ∈ Beyond upper) :
    ∃ p, Reached base N upper p ∧ p ∈ Beyond upper ∧ p ≤ l.foldl (· ⊔ ·) x := by
  induction l generalizing x with
  | nil => exact ⟨x, hx, by simpa using hbey, le_refl x⟩
  | cons y l' ih =>
    simp only [List.foldl_cons] at hbey ⊢
    by_cases hxb : x ∈ Beyond upper
    · exact ⟨x, hx, hxb, le_trans le_sup_left (le_foldl_sup l' (x ⊔ y))⟩
    · exact ih (Reached.joinBase hx hxb (hl y (by simp)))
        (fun w hw => hl w (by simp [hw])) hbey

/-- **In-band closure points are reached** (hence active, hence emittable): any `x` below
    `upper` that is a join of ground-set elements with a seed below it.  The ground set
    need only be in `base` BELOW `x` — pending times above `upper` never participate in a
    decomposition of an in-band point. -/
theorem reached_of_mem_cl {base N : Set T} {upper : Finset T} {X : Set T} {n x : T}
    (hX : ∀ y ∈ X, y ≤ x → y ∈ base) (hnN : n ∈ N) (hnx : n ≤ x) (hx : x ∈ supClosure X)
    (hact : x ∉ Beyond upper) : Reached base N upper x := by
  obtain ⟨Y, hne, hYX, hsup⟩ := hx
  have hfold : Y.toList.foldl (· ⊔ ·) n = x := foldl_toList_eq hne hsup hnx
  have hl : ∀ y ∈ Y.toList, y ∈ base := by
    intro y hy
    have hyY : y ∈ Y := Finset.mem_toList.mp hy
    exact hX y (hYX (Finset.mem_coe.mpr hyY)) (hsup ▸ Finset.le_sup' id hyY)
  rw [← hfold] at hact ⊢
  exact (Reached.seed hnN).foldl Y.toList hl hact

/-- **Beyond-`upper` closure points sit above a pended time**: the first crossing of the
    join chain is reached, beyond `upper`, and below `x`. -/
theorem exists_pended_le_of_mem_cl {base N : Set T} {upper : Finset T} {X : Set T} {n x : T}
    (hX : ∀ y ∈ X, y ≤ x → y ∈ base) (hnN : n ∈ N) (hnx : n ≤ x) (hx : x ∈ supClosure X)
    (hbey : x ∈ Beyond upper) :
    ∃ p, Reached base N upper p ∧ p ∈ Beyond upper ∧ p ≤ x := by
  obtain ⟨Y, hne, hYX, hsup⟩ := hx
  have hfold : Y.toList.foldl (· ⊔ ·) n = x := foldl_toList_eq hne hsup hnx
  have hl : ∀ y ∈ Y.toList, y ∈ base := by
    intro y hy
    have hyY : y ∈ Y := Finset.mem_toList.mp hy
    exact hX y (hYX (Finset.mem_coe.mpr hyY)) (hsup ▸ Finset.le_sup' id hyY)
  rw [← hfold] at hbey ⊢
  exact exists_crossing_of_foldl (Reached.seed hnN) Y.toList hl hbey

/-- **The view enumeration covers the round.**  Seed on `N` (holding the batch support and
    the due pending times); join against `base` (holding the VIEW's input and output
    supports and the seeds).  Every live point of the change term and of the re-derived
    staleness is reached if it is below `upper`, and is at or above a pended time of that
    enumeration, or a carried pending time, if it is beyond `upper`.  These are the two facts
    `Run.round_discrepancy` takes from the enumeration. -/
theorem view_round_coverage {P due : Finset T} {upper : Finset T}
    (hcarried : ↑(P \ due) ⊆ Beyond upper)
    {U : Set T} (hU : IsUpperSet U) (hdueU : ↑due ⊆ U)
    {I O b S' O' : T →₀ A} (hbU : ↑b.support ⊆ U) (f : A → A)
    (hS : ∀ t ∈ U, acc S' t = acc I t) (hO : ∀ t ∈ U, acc O' t = acc O t)
    (hvan : ∀ t, (∀ p ∈ P, ¬ p ≤ t) → f (acc I t) - acc O t = 0)
    {N base : Set T} (hbN : ↑b.support ⊆ N) (hdN : ↑due ⊆ N) (hNb : N ⊆ base)
    (hSb : ↑S'.support ⊆ base) (hOb : ↑O'.support ⊆ base) :
    ∃ (SC : Finset T) (dC : T → A) (SQ : Finset T) (dQ : T → A),
      Represents SC dC (fun t => f (acc (I + b) t) - f (acc I t)) ∧
      Represents SQ dQ (fun t => f (acc I t) - acc O t) ∧
      ∀ x, (dC x ≠ 0 ∨ dQ x ≠ 0) →
        (x ∉ Beyond upper → Reached base N upper x) ∧
        (x ∈ Beyond upper →
          (∃ p, Reached base N upper p ∧ p ∈ Beyond upper ∧ p ≤ x) ∨
          (∃ p ∈ P \ due, p ≤ x)) := by
  obtain ⟨SC, dC, hsuppC, hrepC⟩ := view_change_coverage hU f hS hbU
  obtain ⟨SQ, dQ, hrepQ, hsuppQ⟩ := view_staleness (P := P) hU hdueU f hS hO hvan
  refine ⟨SC, dC, SQ, dQ, hrepC, hrepQ, ?_⟩
  intro x hx
  -- Each live point is a closure point over a subset of `base`, above a seed in `N`,
  -- or sits above a carried pending time.
  have hcases : (∃ (X : Finset T) (n : T), ↑X ⊆ base ∧ n ∈ N ∧ n ≤ x ∧ x ∈ cl X) ∨
      (∃ p ∈ P \ due, p ≤ x) := by
    rcases hx with h | h
    · obtain ⟨hcl, nu, hnu, hnux⟩ := hsuppC x h
      refine Or.inl ⟨S'.support ∪ b.support, nu, ?_, hbN hnu, hnux, hcl⟩
      intro y hy
      rcases Finset.mem_union.mp hy with hy | hy
      · exact hSb hy
      · exact hNb (hbN hy)
    · rcases hsuppQ x h with hc | ⟨hcl, p, hp, hpx⟩
      · exact Or.inr hc
      · refine Or.inl ⟨due ∪ S'.support ∪ O'.support, p, ?_, hdN hp, hpx, hcl⟩
        intro y hy
        rcases Finset.mem_union.mp hy with hy | hy
        · rcases Finset.mem_union.mp hy with hy | hy
          · exact hNb (hdN hy)
          · exact hSb hy
        · exact hOb hy
  rcases hcases with ⟨X, n, hXb, hn, hnx, hcl⟩ | ⟨p, hp, hpx⟩
  · exact ⟨fun hxu => reached_of_mem_cl (fun y hy _ => hXb hy) hn hnx hcl hxu,
      fun hxb => Or.inl (exists_pended_le_of_mem_cl (fun y hy _ => hXb hy) hn hnx hcl hxb)⟩
  · exact ⟨fun hxu => absurd (beyond_isUpperSet upper hpx (hcarried hp)) hxu,
      fun _ => Or.inr ⟨p, hp, hpx⟩⟩

/-- Reached times stay in any upper set containing the seeds. -/
theorem Reached.mem_of_upperSet {base N : Set T} {upper : Finset T} {U : Set T}
    (hU : IsUpperSet U) (hN : N ⊆ U) {x : T} (hx : Reached base N upper x) : x ∈ U := by
  induction hx with
  | seed h => exact hN h
  | joinBase _ _ _ ih => exact hU le_sup_left ih
  | joinActive _ _ _ _ ih _ => exact hU le_sup_left ih

/-! ## A trace live only where its function vanishes is zero

The tool for proving that the RUN'S OWN emission (any `Step`-compliant one) achieves
finalized correctness: the discrepancy between what was needed and what was emitted has a
trace whose live points are all active — where `Step.emit_correct` says the discrepancy
vanishes — so by induction up the order the whole trace is zero. -/

theorem cut_eq_insert_filter {S : Finset T} {x : T} (hxS : x ∈ S) :
    Cut S x = insert x (S.filter (· < x)) := by
  ext y
  simp only [Cut, Finset.mem_filter, Finset.mem_insert]
  constructor
  · rintro ⟨hyS, hyx⟩
    rcases lt_or_eq_of_le hyx with h | h
    · exact Or.inr ⟨hyS, h⟩
    · exact Or.inl h
  · rintro (rfl | ⟨hyS, hyx⟩)
    · exact ⟨hxS, le_refl _⟩
    · exact ⟨hyS, le_of_lt hyx⟩

/-- If `g` is represented by `(S, d)` at every point of `W`, vanishes on `W`, and every
    live point of the trace lies in `W`, then the trace is zero outright. -/
theorem live_eq_zero_of_vanish {S : Finset T} {d : T → A} {g : T → A} {W : Set T}
    (hrepW : ∀ w ∈ W, g w = ∑ s ∈ Cut S w, d s)
    (hgW : ∀ w ∈ W, g w = 0)
    (hliveW : ∀ x ∈ S, d x ≠ 0 → x ∈ W) :
    ∀ x ∈ S, d x = 0 := by
  suffices h : ∀ (m : ℕ) (x : T), x ∈ S → (S.filter (· < x)).card ≤ m → d x = 0 by
    intro x hx
    exact h (S.filter (· < x)).card x hx le_rfl
  intro m
  induction m with
  | zero =>
    intro x hxS hcard
    by_contra hdx
    have hxW := hliveW x hxS hdx
    have hemp : S.filter (· < x) = ∅ := Finset.card_eq_zero.mp (Nat.le_zero.mp hcard)
    have hz := (hgW x hxW).symm.trans (hrepW x hxW)
    rw [cut_eq_insert_filter hxS, hemp] at hz
    simp only [Finset.insert_empty, Finset.sum_singleton] at hz
    exact hdx hz.symm
  | succ m ih =>
    intro x hxS hcard
    by_contra hdx
    have hxW := hliveW x hxS hdx
    have hz := (hgW x hxW).symm.trans (hrepW x hxW)
    rw [cut_eq_insert_filter hxS,
      Finset.sum_insert (by simp [Finset.mem_filter])] at hz
    have hrest : ∑ s ∈ S.filter (· < x), d s = 0 := by
      apply Finset.sum_eq_zero
      intro s hs
      simp only [Finset.mem_filter] at hs
      have hlt : (S.filter (· < s)).card < (S.filter (· < x)).card := by
        apply Finset.card_lt_card
        refine Finset.ssubset_iff_of_subset (fun a ha => ?_) |>.2 ⟨s, ?_, ?_⟩
        · simp only [Finset.mem_filter] at ha ⊢
          exact ⟨ha.1, lt_trans ha.2 hs.2⟩
        · simp only [Finset.mem_filter]
          exact ⟨hs.1, hs.2⟩
        · simp only [Finset.mem_filter, lt_self_iff_false, and_false, not_false_eq_true]
      exact ih s hs.1 (by omega)
    rw [hrest, add_zero] at hz
    exact hdx hz.symm

/-- Hence the represented function vanishes wherever the representation holds. -/
theorem rep_zero_of_live_vanish {S : Finset T} {d : T → A} {g : T → A} {W : Set T}
    (hrepW : ∀ w ∈ W, g w = ∑ s ∈ Cut S w, d s)
    (hgW : ∀ w ∈ W, g w = 0)
    (hliveW : ∀ x ∈ S, d x ≠ 0 → x ∈ W)
    {t : T} (hrep : g t = ∑ s ∈ Cut S t, d s) : g t = 0 := by
  rw [hrep]
  apply Finset.sum_eq_zero
  intro s hs
  simp only [Cut, Finset.mem_filter] at hs
  exact live_eq_zero_of_vanish hrepW hgW hliveW s hs.1

/-! ## Telescoping the run: stored state agrees with the truth -/

theorem Run.input_succ {f : A → A} {n : ℕ} (R : Run T A f n) (r : ℕ) :
    R.input (r + 1) = R.input r + R.batch (r + 1) :=
  Finset.sum_range_succ (fun k => R.batch (k + 1)) r

theorem Run.emitted_succ {f : A → A} {n : ℕ} (R : Run T A f n) (r : ℕ) :
    R.emitted (r + 1) = R.emitted r + R.emit (r + 1) :=
  Finset.sum_range_succ (fun k => R.emit (k + 1)) r

/-- The stored input representative agrees with the true input at or beyond the round's
    frontier, at every round. -/
theorem Run.stored_agrees_input {f : A → A} {n : ℕ} (R : Run T A f n) :
    ∀ r ≤ n, ∀ t ∈ Beyond (R.frontier r), acc (R.state r).stored t = acc (R.input r) t := by
  intro r
  induction r with
  | zero =>
    intro _ t _
    rw [R.state_zero]
    simp [Run.input]
  | succ r ih =>
    intro hr t ht
    rw [(R.step r (by omega)).stored_agrees t ht, acc_add,
      ih (by omega) t (R.frontier_mono r (by omega) ht), R.input_succ, acc_add]

/-- The stored output representative agrees with the emitted stream at or beyond the
    round's frontier, at every round. -/
theorem Run.output_agrees_emitted {f : A → A} {n : ℕ} (R : Run T A f n) :
    ∀ r ≤ n, ∀ t ∈ Beyond (R.frontier r),
      acc (R.state r).output t = acc (R.emitted r) t := by
  intro r
  induction r with
  | zero =>
    intro _ t _
    rw [R.state_zero]
    simp [Run.emitted]
  | succ r ih =>
    intro hr t ht
    rw [(R.step r (by omega)).output_agrees t ht, acc_add,
      ih (by omega) t (R.frontier_mono r (by omega) ht), R.emitted_succ, acc_add]

/-- Pending times always sit at or beyond the current frontier. -/
theorem Run.pending_beyond {f : A → A} {n : ℕ} (R : Run T A f n) :
    ∀ r ≤ n, (R.state r).pending ⊆ Beyond (R.frontier r) := by
  intro r hr
  cases r with
  | zero =>
    rw [R.state_zero]
    exact Set.empty_subset _
  | succ r => exact (R.step r (by omega)).pending_confined

/-- Pending sets are finite. -/
theorem Run.pending_finite {f : A → A} {n : ℕ} (R : Run T A f n) :
    ∀ r ≤ n, ((R.state r).pending).Finite := by
  intro r hr
  cases r with
  | zero =>
    rw [R.state_zero]
    exact Set.finite_empty
  | succ r => exact (R.step r (by omega)).pending_finite

/-- The three-part discrepancy trace of a round: the change term, the carried staleness,
    minus the emission. -/
noncomputable def roundTrace (dC dP : T → A) (SC SP : Finset T) (e : T →₀ A) : T → A :=
  fun s => (if s ∈ SC then dC s else 0) + (if s ∈ SP then dP s else 0)
    + (if s ∈ e.support then -(e s) else 0)

/-- **The round-discrepancy package.**  Under the invariant at round `r`, the discrepancy
    `f∘input − emitted` after round `r + 1` (with the run's own emission) is represented
    by `roundTrace` — the change term over the round's VIEW, the staleness re-derived over
    the view, minus the emission — at every time, and in band-gated form below the new
    frontier.  Its live in-band points are all ACTIVE for the view's enumeration (change
    points reach from batch seeds, staleness points from due pending seeds, emission points
    by `emit_support`); it vanishes at active times, by `emit_correct` through the
    stored-representative agreements; and its live change and staleness points beyond the
    new frontier are at or above a new pending time (a first crossing, or a carried pending
    time, through `pending_below`).  `step_clause1` and `step_residual` are corollaries. -/
theorem Run.round_discrepancy {f : A → A} {n : ℕ} (R : Run T A f n) {r : ℕ} (hr : r < n)
    (h1 : ∀ t, t ∉ Beyond (R.frontier r) → acc (R.emitted r) t = f (acc (R.input r) t))
    {SP : Finset T} {dP : T → A}
    (h2rep : ∀ t ∈ Beyond (R.frontier r),
      f (acc (R.input r) t) - acc (R.emitted r) t = ∑ s ∈ Cut SP t, dP s)
    (h2supp : ∀ x, dP x ≠ 0 →
      x ∈ supClosure ((R.state r).pending ∪ ↑(R.state r).stored.support
          ∪ ↑(R.state r).output.support) ∧
      ∃ p ∈ (R.state r).pending, p ≤ x) :
    ∃ (SC : Finset T) (dC : T → A) (SQ : Finset T) (dQ : T → A) (W : Set T),
      (∀ w ∈ W, w ∉ Beyond (R.frontier (r + 1))) ∧
      ↑(R.emit (r + 1)).support ⊆ W ∧
      (∀ t, f (acc (R.input r + R.batch (r + 1)) t) - acc (R.emitted r) t
          - acc (R.emit (r + 1)) t
        = ∑ s ∈ Cut (SC ∪ SQ ∪ (R.emit (r + 1)).support) t,
            roundTrace dC dQ SC SQ (R.emit (r + 1)) s) ∧
      (∀ t, t ∉ Beyond (R.frontier (r + 1)) →
        f (acc (R.input r + R.batch (r + 1)) t) - acc (R.emitted r) t
          - acc (R.emit (r + 1)) t
        = ∑ s ∈ Cut (SC ∪ SQ ∪ (R.emit (r + 1)).support) t,
            (if s ∈ Beyond (R.frontier (r + 1)) then 0
              else roundTrace dC dQ SC SQ (R.emit (r + 1)) s)) ∧
      (∀ x, roundTrace dC dQ SC SQ (R.emit (r + 1)) x ≠ 0 →
        x ∉ Beyond (R.frontier (r + 1)) → x ∈ W) ∧
      (∀ w ∈ W,
        f (acc (R.input r + R.batch (r + 1)) w) - acc (R.emitted r) w
          - acc (R.emit (r + 1)) w = 0) ∧
      (∀ x, (dC x ≠ 0 ∨ dQ x ≠ 0) → x ∈ Beyond (R.frontier (r + 1)) →
        ∃ p ∈ (R.state (r + 1)).pending, p ≤ x) := by
  classical
  have hrn : r ≤ n := le_of_lt hr
  have hstep := R.step r hr
  obtain ⟨vS, vO, hen⟩ := hstep.enumerates
  obtain ⟨U, hU, hNU, hvS, hvO⟩ := hen.view_agrees
  have hbsupp : ↑(R.batch (r + 1)).support ⊆ Beyond (R.frontier r) :=
    fun x hx => (R.batch_support r hr hx).1
  have hpend : (R.state r).pending ⊆ Beyond (R.frontier r) := R.pending_beyond r hrn
  have hPfin : ((R.state r).pending).Finite := R.pending_finite r hrn
  set PF : Finset T := hPfin.toFinset with hPF
  set due : Finset T := PF.filter (fun p => p ∉ Beyond (R.frontier (r + 1))) with hdue
  have hdue_coe : (↑due : Set T) = (R.state r).pending \ Beyond (R.frontier (r + 1)) := by
    ext p
    simp [hdue, hPF, Set.Finite.mem_toFinset]
  have hcar : (↑(PF \ due) : Set T) ⊆ Beyond (R.frontier (r + 1)) := by
    intro p hp
    rw [Finset.coe_sdiff, Set.mem_sdiff] at hp
    by_contra hpb
    exact hp.2 (Finset.mem_coe.mpr (Finset.mem_filter.mpr ⟨Finset.mem_coe.mp hp.1, hpb⟩))
  -- The view agrees with the TRUTH where it agrees with the state and the state does.
  have hU' : IsUpperSet (U ∩ Beyond (R.frontier r)) := hU.inter (beyond_isUpperSet _)
  have hdueU' : (↑due : Set T) ⊆ U ∩ Beyond (R.frontier r) := by
    rw [hdue_coe]
    intro p hp
    exact ⟨hNU (Or.inr hp), hpend hp.1⟩
  have hbU' : ↑(R.batch (r + 1)).support ⊆ U ∩ Beyond (R.frontier r) :=
    fun x hx => ⟨hNU (Or.inl hx), hbsupp hx⟩
  have hS : ∀ t ∈ U ∩ Beyond (R.frontier r), acc vS t = acc (R.input r) t :=
    fun t ht => (hvS t ht.1).trans (R.stored_agrees_input r hrn t ht.2)
  have hO : ∀ t ∈ U ∩ Beyond (R.frontier r), acc vO t = acc (R.emitted r) t :=
    fun t ht => (hvO t ht.1).trans (R.output_agrees_emitted r hrn t ht.2)
  -- The invariant says the staleness vanishes wherever no pending time reaches.
  have hvan : ∀ t, (∀ p ∈ PF, ¬ p ≤ t) →
      f (acc (R.input r) t) - acc (R.emitted r) t = 0 := by
    intro t ht
    by_cases htb : t ∈ Beyond (R.frontier r)
    · rw [h2rep t htb]
      apply Finset.sum_eq_zero
      intro s hs
      simp only [Cut, Finset.mem_filter] at hs
      by_contra hds
      obtain ⟨-, p, hpP, hps⟩ := h2supp s hds
      exact ht p (hPfin.mem_toFinset.mpr hpP) (hps.trans hs.2)
    · rw [h1 t htb, sub_self]
  have hbN : ↑(R.batch (r + 1)).support
      ⊆ seedSet (R.state r) (R.batch (r + 1)) (R.frontier (r + 1)) := fun _ hx => Or.inl hx
  have hdN : (↑due : Set T) ⊆ seedSet (R.state r) (R.batch (r + 1)) (R.frontier (r + 1)) := by
    rw [hdue_coe]
    exact fun _ hp => Or.inr hp
  obtain ⟨SC, dC, SQ, dQ, hrepC, hrepQ, hcov⟩ :=
    view_round_coverage (P := PF) (due := due) (upper := R.frontier (r + 1))
      (N := seedSet (R.state r) (R.batch (r + 1)) (R.frontier (r + 1)))
      (base := viewBase vS vO (R.state r) (R.batch (r + 1)) (R.frontier (r + 1)))
      hcar hU' hdueU' hbU' f hS hO hvan hbN hdN Set.subset_union_right
      (fun _ hx => Or.inl (Or.inl hx)) (fun _ hx => Or.inl (Or.inr hx))
  have hArep : ∀ t',
      f (acc (R.input r + R.batch (r + 1)) t') - acc (R.emitted r) t'
        - acc (R.emit (r + 1)) t'
      = ∑ s ∈ Cut (SC ∪ SQ ∪ (R.emit (r + 1)).support) t',
          ((if s ∈ SC then dC s else 0) + (if s ∈ SQ then dQ s else 0)
            + (if s ∈ (R.emit (r + 1)).support then -(R.emit (r + 1) s) else 0)) := by
    intro t'
    have hgate : ∀ s,
        ((if s ∈ SC then dC s else 0) + (if s ∈ SQ then dQ s else 0)
          + (if s ∈ (R.emit (r + 1)).support then -(R.emit (r + 1) s) else 0))
        = ((if s ∈ SC ∪ SQ then
              ((if s ∈ SC then dC s else 0) + (if s ∈ SQ then dQ s else 0)) else 0)
            + (if s ∈ (R.emit (r + 1)).support then -(R.emit (r + 1) s) else 0)) := by
      intro s
      by_cases hs : s ∈ SC ∪ SQ
      · rw [if_pos hs]
      · rw [if_neg hs]
        simp only [Finset.mem_union, not_or] at hs
        rw [if_neg hs.1, if_neg hs.2]
        simp only [zero_add]
    have hc : f (acc (R.input r + R.batch (r + 1)) t') - f (acc (R.input r) t')
        = ∑ s ∈ Cut SC t', dC s := hrepC t'
    have hq : f (acc (R.input r) t') - acc (R.emitted r) t' = ∑ s ∈ Cut SQ t', dQ s :=
      hrepQ t'
    have hneg : ∑ s ∈ Cut (R.emit (r + 1)).support t', -(R.emit (r + 1) s)
        = -acc (R.emit (r + 1)) t' := by
      rw [Finset.sum_neg_distrib]
      rfl
    rw [Finset.sum_congr rfl (fun s _ => hgate s), cut_sum_union, cut_sum_union,
      ← hc, ← hq, hneg]
    abel
  refine ⟨SC, dC, SQ, dQ, viewActive vS vO (R.state r) (R.batch (r + 1)) (R.frontier (r + 1)),
    fun w hw => hw.2, hen.emit_support, ?_, ?_, ?_, ?_, ?_⟩
  · intro t
    simp only [roundTrace]
    exact hArep t
  · intro t ht
    simp only [roundTrace]
    rw [hArep t]
    refine Finset.sum_congr rfl (fun s hs => ?_)
    simp only [Cut, Finset.mem_filter] at hs
    exact (if_neg (fun hc => ht (beyond_isUpperSet _ hs.2 hc))).symm
  · intro x hx hxu
    simp only [roundTrace] at hx
    have h3 : dC x ≠ 0 ∨ dQ x ≠ 0 ∨ x ∈ (R.emit (r + 1)).support := by
      by_contra hcon
      simp only [not_or, ne_eq, not_not] at hcon
      obtain ⟨h₁, h₂, h₃⟩ := hcon
      apply hx
      rw [if_neg h₃]
      by_cases hxC : x ∈ SC <;> by_cases hxQ : x ∈ SQ <;>
        simp [hxC, hxQ, h₁, h₂]
    rcases h3 with h | h | h
    · exact ⟨(hcov x (Or.inl h)).1 hxu, hxu⟩
    · exact ⟨(hcov x (Or.inr h)).1 hxu, hxu⟩
    · exact hen.emit_support (Finset.mem_coe.mpr h)
  · intro w hw
    have hwl : w ∈ Beyond (R.frontier r) := by
      refine Reached.mem_of_upperSet (beyond_isUpperSet _) ?_ hw.1
      intro y hy
      rcases hy with hy | hy
      · exact hbsupp hy
      · exact hpend hy.1
    have hec := hen.emit_correct w hw
    rw [acc_add, acc_add, R.output_agrees_emitted r hrn w hwl,
      R.stored_agrees_input r hrn w hwl] at hec
    rw [acc_add, ← hec]
    abel
  · intro x h hxb
    rcases (hcov x h).2 hxb with ⟨p, hpr, hpb, hple⟩ | ⟨p, hp, hpx⟩
    · obtain ⟨q, hq, hqp⟩ := hen.pending_below p (Or.inl ⟨hpr, hpb⟩)
      exact ⟨q, hq, hqp.trans hple⟩
    · have hpP : p ∈ (R.state r).pending :=
        hPfin.mem_toFinset.mp (Finset.mem_sdiff.mp hp).1
      obtain ⟨q, hq, hqp⟩ := hen.pending_below p (Or.inr ⟨hpP, hcar (Finset.mem_coe.mpr hp)⟩)
      exact ⟨q, hq, hqp.trans hpx⟩

/-- **Clause 1 advances for the run's own emission**: given the invariant at round `r`,
    ANY `Step`-compliant emission achieves finalized correctness below the next frontier —
    the gated round trace is live only at active times, where the discrepancy vanishes. -/
theorem Run.step_clause1 {f : A → A} {n : ℕ} (R : Run T A f n) {r : ℕ} (hr : r < n)
    (h1 : ∀ t, t ∉ Beyond (R.frontier r) → acc (R.emitted r) t = f (acc (R.input r) t))
    {SP : Finset T} {dP : T → A}
    (h2rep : ∀ t ∈ Beyond (R.frontier r),
      f (acc (R.input r) t) - acc (R.emitted r) t = ∑ s ∈ Cut SP t, dP s)
    (h2supp : ∀ x, dP x ≠ 0 →
      x ∈ supClosure ((R.state r).pending ∪ ↑(R.state r).stored.support
          ∪ ↑(R.state r).output.support) ∧
      ∃ p ∈ (R.state r).pending, p ≤ x) :
    ∀ t, t ∉ Beyond (R.frontier (r + 1)) →
      acc (R.emitted (r + 1)) t = f (acc (R.input (r + 1)) t) := by
  obtain ⟨SC, dC, SQ, dQ, W, hW, -, -, hArep', hlive, hgW, -⟩ :=
    R.round_discrepancy hr h1 h2rep h2supp
  intro t htu
  have hδ := rep_zero_of_live_vanish (fun w hw => hArep' w (hW w hw)) hgW
    (fun x _ hx => by
      have hx' : (if x ∈ Beyond (R.frontier (r + 1)) then (0 : A)
          else roundTrace dC dQ SC SQ (R.emit (r + 1)) x) ≠ 0 := hx
      have hxu : x ∉ Beyond (R.frontier (r + 1)) := fun hc => hx' (if_pos hc)
      rw [if_neg hxu] at hx'
      exact hlive x hx' hxu)
    (hArep' t htu)
  rw [R.emitted_succ, acc_add, R.input_succ]
  rw [sub_sub] at hδ
  exact (sub_eq_zero.mp hδ).symm

/-- **The residual staleness trace.**  Under the invariant at round `r`, the staleness
    after round `r + 1` is a difference trace at EVERY time, whose live points are beyond
    the new frontier and at or above some NEW pending time — by the first-crossing argument
    for change and due staleness points, and still-pending witnesses for carried points.
    In-band live points are impossible: the round trace vanishes there outright. -/
theorem Run.step_residual {f : A → A} {n : ℕ} (R : Run T A f n) {r : ℕ} (hr : r < n)
    (h1 : ∀ t, t ∉ Beyond (R.frontier r) → acc (R.emitted r) t = f (acc (R.input r) t))
    {SP : Finset T} {dP : T → A}
    (h2rep : ∀ t ∈ Beyond (R.frontier r),
      f (acc (R.input r) t) - acc (R.emitted r) t = ∑ s ∈ Cut SP t, dP s)
    (h2supp : ∀ x, dP x ≠ 0 →
      x ∈ supClosure ((R.state r).pending ∪ ↑(R.state r).stored.support
          ∪ ↑(R.state r).output.support) ∧
      ∃ p ∈ (R.state r).pending, p ≤ x) :
    ∃ (SR : Finset T) (dR : T → A),
      (∀ t, f (acc (R.input (r + 1)) t) - acc (R.emitted (r + 1)) t
        = ∑ s ∈ Cut SR t, dR s) ∧
      ∀ x, dR x ≠ 0 →
        x ∈ Beyond (R.frontier (r + 1)) ∧
        ∃ p ∈ (R.state (r + 1)).pending, p ≤ x := by
  classical
  obtain ⟨SC, dC, SQ, dQ, W, hW, hWe, hArep, hArep', hlive, hgW, hbey⟩ :=
    R.round_discrepancy hr h1 h2rep h2supp
  have hzero := live_eq_zero_of_vanish (fun w hw => hArep' w (hW w hw)) hgW
    (fun x _ hx => by
      have hx' : (if x ∈ Beyond (R.frontier (r + 1)) then (0 : A)
          else roundTrace dC dQ SC SQ (R.emit (r + 1)) x) ≠ 0 := hx
      have hxu : x ∉ Beyond (R.frontier (r + 1)) := fun hc => hx' (if_pos hc)
      rw [if_neg hxu] at hx'
      exact hlive x hx' hxu)
  refine ⟨SC ∪ SQ ∪ (R.emit (r + 1)).support,
    roundTrace dC dQ SC SQ (R.emit (r + 1)), ?_, ?_⟩
  · intro t
    rw [R.input_succ, R.emitted_succ, acc_add (R.emitted r) (R.emit (r + 1)), ← sub_sub]
    exact hArep t
  · intro x hx
    replace hx : ((if x ∈ SC then dC x else 0) + (if x ∈ SQ then dQ x else 0)
        + (if x ∈ (R.emit (r + 1)).support then -(R.emit (r + 1) x) else 0)) ≠ 0 := by
      simpa only [roundTrace] using hx
    have hxSA : x ∈ SC ∪ SQ ∪ (R.emit (r + 1)).support := by
      by_contra hcon
      simp only [Finset.mem_union, not_or] at hcon
      apply hx
      rw [if_neg hcon.1.1, if_neg hcon.1.2, if_neg hcon.2]
      simp
    have hxb : x ∈ Beyond (R.frontier (r + 1)) := by
      by_contra hxu
      have h0 := hzero x hxSA
      rw [if_neg hxu] at h0
      simp only [roundTrace] at h0
      exact hx h0
    have hxe : x ∉ (R.emit (r + 1)).support :=
      fun hc => hW x (hWe (Finset.mem_coe.mpr hc)) hxb
    rw [if_neg hxe, add_zero] at hx
    have h2 : dC x ≠ 0 ∨ dQ x ≠ 0 := by
      by_contra hcon
      simp only [not_or, ne_eq, not_not] at hcon
      apply hx
      rw [hcon.1, hcon.2]
      simp
    exact ⟨hxb, hbey x h2 hxb⟩

/-- **The transfer property**: the residual staleness trace, supported beyond the new
    frontier above a new pending time, can be re-based onto the NEW representatives'
    closure after the adversary re-picks them.  This HOLDS for every run
    (`transfer_always` below): with the output support in the ground set, the trace is
    re-derived from the current representatives rather than carried, so nothing crosses
    the adversary boundary.  It is stated separately because it isolates what the
    OUTPUT-FREE variant of the model would still owe — there, re-derivation is
    unavailable and re-basing across cancellation remains an open question (see the
    amendment note in TARGETS2.md). -/
def Run.Transfer {f : A → A} {n : ℕ} (R : Run T A f n) : Prop :=
  ∀ r, r < n →
    ∀ (SR : Finset T) (dR : T → A),
      (∀ t, f (acc (R.input (r + 1)) t) - acc (R.emitted (r + 1)) t
        = ∑ s ∈ Cut SR t, dR s) →
      (∀ x, dR x ≠ 0 →
        x ∈ Beyond (R.frontier (r + 1)) ∧
        ∃ p ∈ (R.state (r + 1)).pending, p ≤ x) →
      ∃ (SP' : Finset T) (dP' : T → A),
        (∀ t ∈ Beyond (R.frontier (r + 1)),
          f (acc (R.input (r + 1)) t) - acc (R.emitted (r + 1)) t
            = ∑ s ∈ Cut SP' t, dP' s) ∧
        ∀ x, dP' x ≠ 0 →
          x ∈ supClosure ((R.state (r + 1)).pending
              ∪ ↑(R.state (r + 1)).stored.support
              ∪ ↑(R.state (r + 1)).output.support) ∧
          ∃ p ∈ (R.state (r + 1)).pending, p ≤ x

/-- The round invariant advances, given the transfer. -/
theorem Run.inv_step {f : A → A} {n : ℕ} (R : Run T A f n) (htr : R.Transfer)
    {r : ℕ} (hr : r < n) (hinv : R.INV r) : R.INV (r + 1) := by
  obtain ⟨h1, SP, dP, h2rep, h2supp⟩ := hinv
  refine ⟨R.step_clause1 hr h1 h2rep h2supp, ?_⟩
  obtain ⟨SR, dR, hRrep, hRsupp⟩ := R.step_residual hr h1 h2rep h2supp
  obtain ⟨SP', dP', hrep', hsupp'⟩ := htr r hr SR dR hRrep hRsupp
  exact ⟨SP', dP', hrep', hsupp'⟩

/-- The invariant holds at every round, given the transfer. -/
theorem Run.inv_of_transfer {f : A → A} {n : ℕ} (R : Run T A f n)
    (hf0 : f 0 = 0) (htr : R.Transfer) : ∀ r, r ≤ n → R.INV r := by
  intro r
  induction r with
  | zero => exact fun _ => inv_zero hf0 R
  | succ r ih => exact fun hr => R.inv_step htr (by omega) (ih (by omega))

/-- **Stream correctness, modulo the transfer.**  TARGETS2.md's goal theorem: for every
    run — every choice of rounds, frontiers, emissions, and adversary — satisfying the
    re-representation transfer, the emitted corrections accumulate to the reduction of
    the true input at every finalized time. -/
theorem Run.stream_correct_of_transfer {f : A → A} {n : ℕ} (R : Run T A f n)
    (hf0 : f 0 = 0) (htr : R.Transfer) : R.StreamCorrect :=
  fun t ht => (R.inv_of_transfer hf0 htr n le_rfl).1 t ht

/-! ## Uniqueness: the emitted stream is THE difference trace (`TARGETS2.md` corollary) -/

/-- Update sets with equal accumulations everywhere are equal: a nonzero difference has a
    minimal support point, where its accumulation is its (nonzero) value. -/
theorem update_ext {d1 d2 : T →₀ A} (h : ∀ t, acc d1 t = acc d2 t) : d1 = d2 := by
  by_contra hne
  have hsupp : (d1 - d2).support.Nonempty :=
    Finsupp.support_nonempty_iff.mpr (sub_ne_zero.mpr hne)
  obtain ⟨x, hxmin⟩ := Finset.exists_minimal hsupp
  have hx : x ∈ (d1 - d2).support := hxmin.1
  have hacc : acc (d1 - d2) x = (d1 - d2) x := by
    unfold acc
    have hfilter : (d1 - d2).support.filter (· ≤ x) = {x} := by
      ext y
      simp only [Finset.mem_filter, Finset.mem_singleton]
      constructor
      · rintro ⟨hyS, hyx⟩
        exact le_antisymm hyx (hxmin.2 hyS hyx)
      · intro hyx
        rw [hyx]
        exact ⟨hx, le_refl x⟩
    rw [hfilter, Finset.sum_singleton]
  have h0 : acc (d1 - d2) x = 0 := by
    rw [acc_sub, h x, sub_self]
  rw [hacc] at h0
  exact Finsupp.mem_support_iff.mp hx h0

/-- **The corollary of stream correctness**: once the final frontier is past everything,
    any update set realizing `f ∘ (accumulated input)` — in particular any consolidation
    of the emitted stream — IS the emitted stream: the difference trace is unique. -/
theorem Run.emitted_unique {f : A → A} {n : ℕ} (R : Run T A f n)
    (hsc : R.StreamCorrect) (hempty : Beyond (R.frontier n) = ∅)
    {D : T →₀ A} (hD : ∀ t, acc D t = f (acc (R.input n) t)) : D = R.emitted n :=
  update_ext (fun t => by
    rw [hD t, ← hsc t (by rw [hempty]; exact Set.notMem_empty t)])

/-! ## Reached times are joins of base times

The other half of TARGETS2's step 2: the enumeration never invents times outside the
declarative closure.  With `reached_of_mem_cl` / `exists_pended_le_of_mem_cl` this pins
the operational enumeration between the two halves of the closure. -/

theorem Reached.mem_supClosure {base N : Set T} {upper : Finset T} (hN : N ⊆ base)
    {x : T} (hx : Reached base N upper x) : x ∈ supClosure base := by
  induction hx with
  | seed h => exact subset_supClosure (hN h)
  | joinBase _ _ hy ih => exact supClosed_supClosure ih (subset_supClosure hy)
  | joinActive _ _ _ _ ih₁ ih₂ => exact supClosed_supClosure ih₁ ih₂

theorem pendedSet_subset_supClosure (σ : State T A) (b : T →₀ A) (upper : Finset T) :
    pendedSet σ b upper ⊆ supClosure (baseSet σ b upper) :=
  fun _ hx => Reached.mem_supClosure Set.subset_union_right hx.1

/-- The deferred set is finite: it lives in the sup-closure of a finite base. -/
theorem pendedSet_finite (σ : State T A) (b : T →₀ A) (upper : Finset T)
    (hP : σ.pending.Finite) : (pendedSet σ b upper).Finite := by
  refine Set.Finite.subset ?_ (pendedSet_subset_supClosure σ b upper)
  refine Set.Finite.supClosure ?_
  exact Set.Finite.union
    (Set.Finite.union (Finset.finite_toSet _) (Finset.finite_toSet _))
    (Set.Finite.union (Finset.finite_toSet _)
      (Set.Finite.subset hP Set.sdiff_subset))

/-- Build a `Step` from the EXACT pending choice — the deferred times plus the carried
    ones, nothing more — enumerating against the state itself.  This is TARGETS2.md's
    step 4 as originally written; the relaxed `pending_*` fields follow from exactness. -/
theorem Step.of_pending_eq {f : A → A} {upper : Finset T} {b : T →₀ A}
    {σ σ' : State T A} {e : T →₀ A} (hP : σ.pending.Finite)
    (emit_support : ↑e.support ⊆ activeSet σ b upper)
    (emit_correct : ∀ t ∈ activeSet σ b upper,
      acc (σ.output + e) t = f (acc (σ.stored + b) t))
    (pending_eq : σ'.pending = pendedSet σ b upper ∪ (σ.pending ∩ Beyond upper))
    (stored_agrees : ∀ t ∈ Beyond upper, acc σ'.stored t = acc (σ.stored + b) t)
    (output_agrees : ∀ t ∈ Beyond upper, acc σ'.output t = acc (σ.output + e) t) :
    Step f upper b σ e σ' where
  enumerates := ⟨σ.stored, σ.output,
    { view_agrees := ⟨Set.univ, isUpperSet_univ, Set.subset_univ _,
        fun _ _ => rfl, fun _ _ => rfl⟩
      emit_support := emit_support
      emit_correct := emit_correct
      pending_below := fun x hx => ⟨x, by rw [pending_eq]; exact hx, le_rfl⟩ }⟩
  pending_confined := by
    rw [pending_eq]
    exact Set.union_subset (fun _ hx => hx.2) (fun _ hx => hx.2)
  pending_finite := by
    rw [pending_eq]
    exact Set.Finite.union (pendedSet_finite σ b upper hP)
      (Set.Finite.subset hP Set.inter_subset_left)
  stored_agrees := stored_agrees
  output_agrees := output_agrees

/-! ## Consumption-time re-derivation: the with-output trace, for ANY adversary

Rather than carrying the staleness trace across the adversary (the `Transfer`), rebuild
it each round from the CURRENT representatives.  On `Above(pending)` the staleness is
determined by the stored input and stored OUTPUT cuts (through the agreement lemmas), and
it vanishes elsewhere; with the pending times as edit points, `Coverage.lean`'s keystone
manufactures a trace supported in `supClosure(pending ∪ stored ∪ output)` above a pending
time — for the FULLY GENERAL adversary.  This is why the output support is in the
closure, and why `baseSet` includes it (matching an enumeration that also joins against
stored output times, as the implementation does): it is exactly what makes
`Run.stream_correct` unconditional.  An output-free variant of the model would still
need `Transfer` proved by other means. -/

/-- **The with-output re-derivation.**  If the staleness at round `r` vanishes wherever no
    pending time reaches (which `step_residual` supplies), then it has a trace supported
    in the closure of the CURRENT pending, stored-input, and stored-OUTPUT supports, above
    a pending time — with no reference to earlier representatives, hence adversary-proof. -/
theorem Run.staleness_rederived {f : A → A} {n : ℕ} (R : Run T A f n) {r : ℕ} (hrn : r ≤ n)
    (hvan : ∀ t, (¬ ∃ p ∈ (R.state r).pending, p ≤ t) →
      f (acc (R.input r) t) - acc (R.emitted r) t = 0) :
    ∃ (SP : Finset T) (dP : T → A),
      (∀ t, f (acc (R.input r) t) - acc (R.emitted r) t = ∑ s ∈ Cut SP t, dP s) ∧
      ∀ x, dP x ≠ 0 →
        x ∈ supClosure ((R.state r).pending ∪ ↑(R.state r).stored.support
            ∪ ↑(R.state r).output.support) ∧
        ∃ p ∈ (R.state r).pending, p ≤ x := by
  classical
  have hPfin : ((R.state r).pending).Finite := R.pending_finite r hrn
  set PF : Finset T := hPfin.toFinset with hPF
  have hPFcoe : (↑PF : Set T) = (R.state r).pending := hPfin.coe_toFinset
  set E : Finset T := PF ∪ (R.state r).stored.support ∪ (R.state r).output.support with hE
  set g : T → A := fun t => f (acc (R.input r) t) - acc (R.emitted r) t with hg
  have habove : ∀ {t : T}, (∃ p ∈ (R.state r).pending, p ≤ t) →
      t ∈ Beyond (R.frontier r) := by
    rintro t ⟨p, hpP, hpt⟩
    exact beyond_isUpperSet _ hpt (R.pending_beyond r hrn hpP)
  have hsimple : Simple E g := by
    intro s t hst
    have hcutP : Cut PF s = Cut PF t :=
      cut_mono_congr (Finset.subset_union_left.trans Finset.subset_union_left) hst
    have hcutS : Cut (R.state r).stored.support s = Cut (R.state r).stored.support t :=
      cut_mono_congr (Finset.subset_union_right.trans Finset.subset_union_left) hst
    have hcutO : Cut (R.state r).output.support s = Cut (R.state r).output.support t :=
      cut_mono_congr Finset.subset_union_right hst
    by_cases hs : ∃ p ∈ (R.state r).pending, p ≤ s
    · have hsb : s ∈ Beyond (R.frontier r) := habove hs
      have ht : ∃ p ∈ (R.state r).pending, p ≤ t := by
        obtain ⟨p, hpP, hps⟩ := hs
        have hp : p ∈ Cut PF s := by
          simp only [Cut, Finset.mem_filter]
          exact ⟨hPfin.mem_toFinset.mpr hpP, hps⟩
        rw [hcutP] at hp
        simp only [Cut, Finset.mem_filter] at hp
        exact ⟨p, hPfin.mem_toFinset.mp hp.1, hp.2⟩
      have htb : t ∈ Beyond (R.frontier r) := habove ht
      show f (acc (R.input r) s) - acc (R.emitted r) s
        = f (acc (R.input r) t) - acc (R.emitted r) t
      rw [← R.stored_agrees_input r hrn s hsb, ← R.stored_agrees_input r hrn t htb,
        ← R.output_agrees_emitted r hrn s hsb, ← R.output_agrees_emitted r hrn t htb]
      have hS : acc (R.state r).stored s = acc (R.state r).stored t := by
        unfold acc
        rw [show (R.state r).stored.support.filter (· ≤ s)
            = Cut (R.state r).stored.support s from rfl, hcutS]
        rfl
      have hO : acc (R.state r).output s = acc (R.state r).output t := by
        unfold acc
        rw [show (R.state r).output.support.filter (· ≤ s)
            = Cut (R.state r).output.support s from rfl, hcutO]
        rfl
      rw [hS, hO]
    · have ht : ¬ ∃ p ∈ (R.state r).pending, p ≤ t := by
        rintro ⟨p, hpP, hpt⟩
        apply hs
        have hp : p ∈ Cut PF t := by
          simp only [Cut, Finset.mem_filter]
          exact ⟨hPfin.mem_toFinset.mpr hpP, hpt⟩
        rw [← hcutP] at hp
        simp only [Cut, Finset.mem_filter] at hp
        exact ⟨p, hPfin.mem_toFinset.mp hp.1, hp.2⟩
      show f (acc (R.input r) s) - acc (R.emitted r) s
        = f (acc (R.input r) t) - acc (R.emitted r) t
      rw [hvan s hs, hvan t ht]
  have hbase : ∀ t, (∀ c ∈ E, ¬ c ≤ t) → g t = 0 := by
    intro t ht
    apply hvan
    rintro ⟨p, hpP, hpt⟩
    exact ht p (Finset.mem_union_left _ (Finset.mem_union_left _
      (hPfin.mem_toFinset.mpr hpP))) hpt
  have hD : IsLowerSet {t : T | ¬ ∃ p ∈ (R.state r).pending, p ≤ t} := by
    intro a b hba ha hb
    obtain ⟨p, hpP, hpb⟩ := hb
    exact ha ⟨p, hpP, hpb.trans hba⟩
  obtain ⟨SP, dP, _hcl, hsupp, hrep⟩ :=
    exists_diff_trace_vanish' hsimple hbase hD (fun t ht => hvan t ht)
  refine ⟨SP, dP, hrep, ?_⟩
  intro x hx
  obtain ⟨hxcl, hxD⟩ := hsupp x hx
  constructor
  · have hgr : (↑E : Set T) = (R.state r).pending ∪ ↑(R.state r).stored.support
        ∪ ↑(R.state r).output.support := by
      rw [hE, Finset.coe_union, Finset.coe_union, hPFcoe]
    rw [show cl E = supClosure (↑E : Set T) from rfl, hgr] at hxcl
    exact hxcl
  · simpa using hxD

/-- **The transfer holds for every run.**  The pending coverage is re-derived each round
    from the current representatives (`staleness_rederived`), so nothing is carried across
    the adversary boundary. -/
theorem Run.transfer_always {f : A → A} {n : ℕ} (R : Run T A f n) : R.Transfer := by
  intro r hr SR dR hrep hsupp
  have hvan : ∀ t, (¬ ∃ p ∈ (R.state (r + 1)).pending, p ≤ t) →
      f (acc (R.input (r + 1)) t) - acc (R.emitted (r + 1)) t = 0 := by
    intro t ht
    rw [hrep t]
    apply Finset.sum_eq_zero
    intro s hs
    simp only [Cut, Finset.mem_filter] at hs
    by_contra hds
    obtain ⟨-, p, hpP, hps⟩ := hsupp s hds
    exact ht ⟨p, hpP, hps.trans hs.2⟩
  obtain ⟨SP, dP, hrep', hsupp'⟩ := R.staleness_rederived (by omega) hvan
  exact ⟨SP, dP, fun t _ => hrep' t, hsupp'⟩

/-- **STREAM CORRECTNESS, unconditionally** (`TARGETS2.md`'s goal theorem): every run of
    the operator — every choice of rounds, frontiers, compliant emissions, and adversary —
    emits corrections that accumulate to the reduction of the true input at every
    finalized time. -/
theorem Run.stream_correct {f : A → A} {n : ℕ} (R : Run T A f n) (hf0 : f 0 = 0) :
    R.StreamCorrect :=
  R.stream_correct_of_transfer hf0 R.transfer_always

/-- **Correct at every frontier the run passes**, not only the last.  A coarse schedule run
    as a finer one — each coarse round split at intermediate frontiers, each piece
    enumerating against its input cancelled by a time below its own seeds
    (`meet_view_agrees`) — is correct at every coarse frontier, since those are frontiers of
    the finer run. -/
theorem Run.correct_at {f : A → A} {n : ℕ} (R : Run T A f n) (hf0 : f 0 = 0)
    {r : ℕ} (hr : r ≤ n) :
    ∀ t, t ∉ Beyond (R.frontier r) → acc (R.emitted r) t = f (acc (R.input r) t) :=
  (R.inv_of_transfer hf0 R.transfer_always r hr).1

/-- The quantified target, discharged. -/
theorem streamCorrectness_holds (T A : Type*) [SemilatticeSup T] [AddCommGroup A]
    (f : A → A) (hf0 : f 0 = 0) : StreamCorrectness T A f :=
  fun _ R => R.stream_correct hf0

/-! ## Nonvacuity

The run-level theorems quantify over `Run`s; this witness confirms the structure is
satisfiable (its conditions are not contradictory), so those theorems have content. -/

/-- The empty run over pairs of naturals: no rounds, minimal frontier, empty state. -/
noncomputable def trivialRun (f : ℤ → ℤ) : Run (ℕ × ℕ) ℤ f 0 where
  frontier _ := {((0 : ℕ), (0 : ℕ))}
  batch _ := 0
  state _ := ⟨0, 0, ∅⟩
  emit _ := 0
  frontier_zero := by
    ext t
    simp only [Set.mem_univ, iff_true]
    exact ⟨(0, 0), Finset.mem_singleton_self _, Prod.mk_le_mk.mpr ⟨Nat.zero_le _, Nat.zero_le _⟩⟩
  frontier_mono := fun r hr => absurd hr (Nat.not_lt_zero r)
  state_zero := rfl
  batch_support := fun r hr => absurd hr (Nat.not_lt_zero r)
  step := fun r hr => absurd hr (Nat.not_lt_zero r)

theorem run_nonempty (f : ℤ → ℤ) : Nonempty (Run (ℕ × ℕ) ℤ f 0) :=
  ⟨trivialRun f⟩

/-! ## Scenario 1, end to end

TARGETS2.md's cancellation scenario as a complete two-round run: the batch
`{+1 at (1,0), −1 at (0,1)}` under the nonempty-indicator reduction; the join `(1,1)` is
pended; the adversary then stores the EMPTY input (the pair cancels beyond `{(1,1)}`) and
relocates the output onto `(1,1)`; round two, seeded only by the pending time, emits the
retraction.  The run is stream correct — checked pointwise — demonstrating both that the
model expresses the scenario and that a CANCELLING adversary can be stream correct, as
the general conjecture demands. -/

/-- The nonempty-indicator reduction. -/
noncomputable def s1f : ℤ → ℤ := fun z => if z = 0 then 0 else 1

noncomputable def s1b : (ℕ × ℕ) →₀ ℤ :=
  Finsupp.single (1, 0) 1 + Finsupp.single (0, 1) (-1)

noncomputable def s1e1 : (ℕ × ℕ) →₀ ℤ :=
  Finsupp.single (1, 0) 1 + Finsupp.single (0, 1) 1

noncomputable def s1st0 : State (ℕ × ℕ) ℤ := ⟨0, 0, ∅⟩

noncomputable def s1st1 : State (ℕ × ℕ) ℤ :=
  ⟨0, Finsupp.single (1, 1) 2,
    pendedSet s1st0 s1b {(1, 1)} ∪ (s1st0.pending ∩ Beyond {(1, 1)})⟩

noncomputable def s1st2 : State (ℕ × ℕ) ℤ :=
  ⟨0, 0, pendedSet s1st1 0 ∅ ∪ (s1st1.pending ∩ Beyond ∅)⟩

theorem s1b_apply_10 : s1b (1, 0) = 1 := by
  simp [s1b, Finsupp.add_apply]

theorem s1b_apply_01 : s1b (0, 1) = -1 := by
  simp [s1b, Finsupp.add_apply]

theorem s1b_support : ↑s1b.support ⊆ ({(1, 0), (0, 1)} : Set (ℕ × ℕ)) := by
  intro x hx
  have h : x ∈ (Finsupp.single ((1 : ℕ), (0 : ℕ)) (1 : ℤ)).support
      ∪ (Finsupp.single ((0 : ℕ), (1 : ℕ)) (-1 : ℤ)).support :=
    Finsupp.support_add (Finset.mem_coe.mp hx)
  rw [Finset.mem_union] at h
  rcases h with h | h
  · exact Or.inl (by simpa using Finsupp.support_single_subset h)
  · exact Or.inr (by simpa using Finsupp.support_single_subset h)

theorem s1_not_beyond_10 : (1, 0) ∉ Beyond ({(1, 1)} : Finset (ℕ × ℕ)) := by
  rintro ⟨y, hy, hle⟩
  simp only [Finset.mem_singleton] at hy
  subst hy
  exact absurd hle.2 (by omega)

theorem s1_not_beyond_01 : (0, 1) ∉ Beyond ({(1, 1)} : Finset (ℕ × ℕ)) := by
  rintro ⟨y, hy, hle⟩
  simp only [Finset.mem_singleton] at hy
  subst hy
  exact absurd hle.1 (by omega)

/-- The two-round cancellation run. -/
noncomputable def scenario1Run : Run (ℕ × ℕ) ℤ s1f 2 where
  frontier r := match r with | 0 => {(0, 0)} | 1 => {(1, 1)} | _ => ∅
  batch r := match r with | 1 => s1b | _ => 0
  state r := match r with | 0 => s1st0 | 1 => s1st1 | _ => s1st2
  emit r := match r with
    | 1 => s1e1
    | 2 => Finsupp.single (1, 1) (-2)
    | _ => 0
  frontier_zero := by
    ext t
    simp only [Set.mem_univ, iff_true]
    exact ⟨(0, 0), Finset.mem_singleton_self _,
      Prod.mk_le_mk.mpr ⟨Nat.zero_le _, Nat.zero_le _⟩⟩
  frontier_mono := by
    intro r hr
    interval_cases r
    · intro t _
      exact ⟨(0, 0), Finset.mem_singleton_self _,
        Prod.mk_le_mk.mpr ⟨Nat.zero_le _, Nat.zero_le _⟩⟩
    · rintro t ⟨y, hy, -⟩
      exact absurd hy (Finset.notMem_empty y)
  state_zero := rfl
  batch_support := by
    intro r hr
    interval_cases r
    · intro x hx
      rcases s1b_support hx with h | h <;> subst h
      · exact ⟨⟨(0, 0), Finset.mem_singleton_self _,
          Prod.mk_le_mk.mpr ⟨Nat.zero_le _, Nat.zero_le _⟩⟩, s1_not_beyond_10⟩
      · exact ⟨⟨(0, 0), Finset.mem_singleton_self _,
          Prod.mk_le_mk.mpr ⟨Nat.zero_le _, Nat.zero_le _⟩⟩, s1_not_beyond_01⟩
    · intro x hx
      have hx0 : x ∈ ((0 : (ℕ × ℕ) →₀ ℤ)).support := hx
      simp at hx0
  step := by
    intro r hr
    interval_cases r
    · -- Round 1: seeds `(1,0)` and `(0,1)`, join pended, adversary cancels the input.
      show Step s1f {(1, 1)} s1b s1st0 s1e1 s1st1
      refine Step.of_pending_eq (by exact Set.finite_empty) ?_ ?_ rfl ?_ ?_
      · -- emissions at the two seeds, both active
        intro x hx
        have hmem : x ∈ ({(1, 0), (0, 1)} : Set (ℕ × ℕ)) := by
          have := Finsupp.support_add (Finset.mem_coe.mp hx)
          simp only [Finset.mem_union] at this
          rcases this with h | h
          · exact Or.inl (by simpa using Finsupp.support_single_subset h)
          · exact Or.inr (by simpa using Finsupp.support_single_subset h)
        rcases hmem with h | h <;> subst h
        · exact ⟨Reached.seed (Or.inl (Finsupp.mem_support_iff.mpr
            (by simp [s1b_apply_10]))), s1_not_beyond_10⟩
        · exact ⟨Reached.seed (Or.inl (Finsupp.mem_support_iff.mpr
            (by simp [s1b_apply_01]))), s1_not_beyond_01⟩
      · -- correctness at every in-band time (a superset of the active times)
        intro t ht
        have htu : ¬ ((1, 1) : ℕ × ℕ) ≤ t :=
          fun hc => ht.2 ⟨(1, 1), Finset.mem_singleton_self _, hc⟩
        show acc ((0 : (ℕ × ℕ) →₀ ℤ)
            + (Finsupp.single (1, 0) 1 + Finsupp.single (0, 1) 1)) t
          = s1f (acc ((0 : (ℕ × ℕ) →₀ ℤ)
            + (Finsupp.single (1, 0) 1 + Finsupp.single (0, 1) (-1))) t)
        rw [zero_add, zero_add]
        simp only [acc_add, acc_single]
        by_cases h1 : ((1, 0) : ℕ × ℕ) ≤ t <;> by_cases h2 : ((0, 1) : ℕ × ℕ) ≤ t
        · exact absurd (Prod.mk_le_mk.mpr ⟨h1.1, h2.2⟩) htu
        · simp [h1, h2, s1f]
        · simp [h1, h2, s1f]
        · simp [h1, h2, s1f]
      · -- the adversary stores the empty input: the pair cancels beyond `(1,1)`
        rintro t ⟨y, hy, hle⟩
        simp only [Finset.mem_singleton] at hy
        subst hy
        have h1 : ((1, 0) : ℕ × ℕ) ≤ t :=
          le_trans (Prod.mk_le_mk.mpr (by omega)) hle
        have h2 : ((0, 1) : ℕ × ℕ) ≤ t :=
          le_trans (Prod.mk_le_mk.mpr (by omega)) hle
        show acc (0 : (ℕ × ℕ) →₀ ℤ) t
          = acc ((0 : (ℕ × ℕ) →₀ ℤ)
            + (Finsupp.single (1, 0) 1 + Finsupp.single (0, 1) (-1))) t
        rw [zero_add, acc_zero, acc_add, acc_single, acc_single,
          if_pos h1, if_pos h2]
        ring
      · -- and relocates the output onto `(1,1)`
        rintro t ⟨y, hy, hle⟩
        simp only [Finset.mem_singleton] at hy
        subst hy
        have h1 : ((1, 0) : ℕ × ℕ) ≤ t :=
          le_trans (Prod.mk_le_mk.mpr (by omega)) hle
        have h2 : ((0, 1) : ℕ × ℕ) ≤ t :=
          le_trans (Prod.mk_le_mk.mpr (by omega)) hle
        show acc (Finsupp.single (1, 1) 2) t
          = acc ((0 : (ℕ × ℕ) →₀ ℤ)
            + (Finsupp.single (1, 0) 1 + Finsupp.single (0, 1) 1)) t
        rw [zero_add, acc_add, acc_single, acc_single, acc_single,
          if_pos hle, if_pos h1, if_pos h2]
        ring
    · -- Round 2: the pending `(1,1)` comes due; the retraction is emitted.
      show Step s1f ∅ 0 s1st1 (Finsupp.single (1, 1) (-2)) s1st2
      have hsup : ((1, 0) : ℕ × ℕ) ⊔ ((0, 1) : ℕ × ℕ) = (1, 1) := rfl
      have hpend11 : (1, 1) ∈ s1st1.pending := by
        refine Or.inl ⟨?_, ⟨(1, 1), Finset.mem_singleton_self _, le_refl _⟩⟩
        show Reached (baseSet s1st0 s1b {(1, 1)}) (seedSet s1st0 s1b {(1, 1)})
          {(1, 1)} (1, 1)
        rw [← hsup]
        exact Reached.joinActive
          (Reached.seed (Or.inl (Finsupp.mem_support_iff.mpr (by simp [s1b_apply_10]))))
          s1_not_beyond_10
          (Reached.seed (Or.inl (Finsupp.mem_support_iff.mpr (by simp [s1b_apply_01]))))
          s1_not_beyond_01
      have hP1 : (s1st1.pending).Finite := by
        exact Set.Finite.union
          (pendedSet_finite s1st0 s1b {(1, 1)} (by exact Set.finite_empty))
          (Set.Finite.subset (by exact Set.finite_empty) Set.inter_subset_left)
      refine Step.of_pending_eq hP1 ?_ ?_ rfl ?_ ?_
      · intro x hx
        have hmem : x = ((1, 1) : ℕ × ℕ) := by
          have := Finsupp.support_single_subset (Finset.mem_coe.mp hx)
          simpa using this
        subst hmem
        refine ⟨Reached.seed (Or.inr ⟨hpend11, fun hc => ?_⟩), fun hc => ?_⟩
        · obtain ⟨y, hy, -⟩ := hc
          exact absurd hy (Finset.notMem_empty y)
        · obtain ⟨y, hy, -⟩ := hc
          exact absurd hy (Finset.notMem_empty y)
      · intro t _
        show acc (Finsupp.single (1, 1) 2 + Finsupp.single (1, 1) (-2)) t
          = s1f (acc ((0 : (ℕ × ℕ) →₀ ℤ) + 0) t)
        have hcancel : Finsupp.single ((1 : ℕ), (1 : ℕ)) (2 : ℤ)
            + Finsupp.single (1, 1) (-2) = 0 := by
          rw [← Finsupp.single_add]
          norm_num
        rw [hcancel, zero_add, acc_zero]
        simp [s1f]
      · rintro t ⟨y, hy, -⟩
        exact absurd hy (Finset.notMem_empty y)
      · rintro t ⟨y, hy, -⟩
        exact absurd hy (Finset.notMem_empty y)

/-- Scenario 1 is stream correct: the emitted stream `+1@(1,0), +1@(0,1), −2@(1,1)`
    accumulates to the indicator of the input everywhere. -/
theorem scenario1_streamCorrect : scenario1Run.StreamCorrect := by
  rintro ⟨a, b⟩ -
  show acc (scenario1Run.emitted 2) (a, b) = s1f (acc (scenario1Run.input 2) (a, b))
  have hem : scenario1Run.emitted 2 = s1e1 + Finsupp.single (1, 1) (-2) := by
    show ∑ k ∈ Finset.range 2, scenario1Run.emit (k + 1) = _
    rw [Finset.sum_range_succ, Finset.sum_range_one]
    rfl
  have hin : scenario1Run.input 2 = s1b + 0 := by
    show ∑ k ∈ Finset.range 2, scenario1Run.batch (k + 1) = _
    rw [Finset.sum_range_succ, Finset.sum_range_one]
    rfl
  rw [hem, hin, add_zero, acc_add]
  show acc (Finsupp.single (1, 0) 1 + Finsupp.single (0, 1) 1) (a, b)
      + acc (Finsupp.single ((1 : ℕ), (1 : ℕ)) (-2 : ℤ)) (a, b)
    = s1f (acc (Finsupp.single (1, 0) 1 + Finsupp.single (0, 1) (-1)) (a, b))
  simp only [acc_add, acc_single]
  by_cases h1 : ((1, 0) : ℕ × ℕ) ≤ (a, b) <;> by_cases h2 : ((0, 1) : ℕ × ℕ) ≤ (a, b)
  · have h11 : ((1, 1) : ℕ × ℕ) ≤ (a, b) := Prod.mk_le_mk.mpr ⟨h1.1, h2.2⟩
    simp [h1, h2, h11, s1f]
  · have h11 : ¬ ((1, 1) : ℕ × ℕ) ≤ (a, b) := fun hc => h2 (Prod.mk_le_mk.mpr
      ⟨Nat.zero_le _, hc.2⟩)
    simp [h1, h2, h11, s1f]
  · have h11 : ¬ ((1, 1) : ℕ × ℕ) ≤ (a, b) := fun hc => h1 (Prod.mk_le_mk.mpr
      ⟨hc.1, Nat.zero_le _⟩)
    simp [h1, h2, h11, s1f]
  · have h11 : ¬ ((1, 1) : ℕ × ℕ) ≤ (a, b) := fun hc => h1 (Prod.mk_le_mk.mpr
      ⟨hc.1, Nat.zero_le _⟩)
    simp [h1, h2, h11, s1f]

end Model
