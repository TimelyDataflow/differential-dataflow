import Differential.Coverage
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
  further), the `emit_*` fields of `Step` (step 3), the `pending_*` fields of `Step`
  (step 4, relaxed: the implementation may warn about MORE times than the enumeration
  defers, so long as they stay beyond the frontier; `Step.of_pending_eq` recovers the
  exact choice)
* "The goal theorem"                  → `Run.StreamCorrect`, `StreamCorrectness`
* "The round invariant"               → `Run.INV` (clause 1 = finalized correctness,
  clause 2 = pending coverage); `inv_zero` is the base case
* "Sanity scenarios"                  → `scenario1_cancels` (the adversary may store the
  empty set while the pending join owes a retraction), `Reached.absorb` (a stored time
  at/above an interesting time is itself interesting), `Step.reached_beyond_pended` and
  `active_not_beyond` (a reached time beyond `upper` is deferred, never evaluated)
-/

set_option linter.style.openClassical false
set_option linter.style.show false

open Finset
open scoped Classical

namespace Model

open Coverage

variable {T : Type*} [SemilatticeSup T]
variable {A : Type*} [AddCommGroup A]

/-- `Beyond u` is the upper set of the finite antichain `u`: every `t` with `x ≤ t` for
    some `x ∈ u`. -/
def Beyond (u : Finset T) : Set T := {t | ∃ x ∈ u, x ≤ t}

theorem beyond_isUpperSet (u : Finset T) : IsUpperSet (Beyond u) := by
  rintro a b hab ⟨x, hx, hxa⟩
  exact ⟨x, hx, hxa.trans hab⟩

/-! ## Update sets and accumulation -/

/-- The accumulation of an update set at `t`: the sum of updates at times `≤ t`. -/
noncomputable def acc (d : T →₀ A) (t : T) : A := ∑ x ∈ d.support.filter (· ≤ t), d x

theorem acc_eq_sum_superset (d : T →₀ A) {E : Finset T} (h : d.support ⊆ E) (t : T) :
    acc d t = ∑ x ∈ E.filter (· ≤ t), d x := by
  refine Finset.sum_subset (Finset.filter_subset_filter _ h) ?_
  intro x hxE hxd
  simp only [Finset.mem_filter] at hxE hxd
  exact Finsupp.notMem_support_iff.mp (fun hc => hxd ⟨hc, hxE.2⟩)

@[simp] theorem acc_zero (t : T) : acc (0 : T →₀ A) t = 0 := by
  simp [acc]

theorem acc_add (d1 d2 : T →₀ A) (t : T) : acc (d1 + d2) t = acc d1 t + acc d2 t := by
  rw [acc_eq_sum_superset (d1 + d2) Finsupp.support_add t,
      acc_eq_sum_superset d1 Finset.subset_union_left t,
      acc_eq_sum_superset d2 Finset.subset_union_right t,
      ← Finset.sum_add_distrib]
  exact Finset.sum_congr rfl fun x _ => Finsupp.add_apply d1 d2 x

theorem acc_single (x : T) (a : A) (t : T) :
    acc (Finsupp.single x a) t = if x ≤ t then a else 0 := by
  rw [acc_eq_sum_superset _ Finsupp.support_single_subset t, Finset.filter_singleton]
  by_cases h : x ≤ t <;> simp [h]

/-- The bridge to `Coverage.lean`: an update set is a difference trace of its own
    accumulation, supported on its support. -/
theorem acc_represents (d : T →₀ A) : Represents d.support ⇑d (acc d) := by
  intro t; rfl

/-- `acc` as a `Finsupp.sum`, for transport along support relocations. -/
theorem acc_eq_finsupp_sum (d : T →₀ A) (t : T) :
    acc d t = d.sum fun x a => if x ≤ t then a else 0 := by
  unfold acc Finsupp.sum
  rw [Finset.sum_filter]

/-- Any support relocation `g` that preserves comparison to `t` on the support preserves
    the accumulation at `t`.  `Finsupp.mapDomain g` is relocation *with consolidation*
    (colliding times merge, cancelled updates vanish), so with `g = Compaction.advance · F hF` and
    `t ∈ Beyond F` (see `Compaction.advance_le_iff`) this shows advance-and-consolidate is one
    instance of TARGETS2's adversary. -/
theorem acc_mapDomain (g : T → T) (d : T →₀ A) (t : T)
    (hg : ∀ x ∈ d.support, (g x ≤ t ↔ x ≤ t)) :
    acc (Finsupp.mapDomain g d) t = acc d t := by
  rw [acc_eq_finsupp_sum, acc_eq_finsupp_sum,
      Finsupp.sum_mapDomain_index (by simp) (fun b m₁ m₂ => by split <;> simp)]
  exact Finsupp.sum_congr fun x hx => by simp only [hg x hx]

/-! ### Intra-round compaction is invisible

The implementation also compacts WITHIN a round: moving through the interesting times in
order, it advances its input and output buffers by joining their times with the meet `m`
of every time still to be examined, and consolidates.  Because `m` is at or below
everything the round still reads, this needs no adversary move at all: the two lemmas
below (TARGETS2.md's *join preservation*, specialized to a one-element frontier) say the
compacted buffer yields the same accumulation at every remaining evaluation point and the
same join against every remaining seed as the uncompacted history would. -/

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

/-- The times available to join against: stored input times, stored (post-consolidation)
    OUTPUT times, and seeds.  Including the output support is what lets the pending
    coverage be re-derived from current representatives each round (`staleness_rederived`),
    dissolving the re-representation question for the fully general adversary. -/
def baseSet (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  ↑σ.stored.support ∪ ↑σ.output.support ∪ seedSet σ b upper

/-- Step 2, with the truncated closure: the enumeration seeds on `N`, and joins a reached
    time that is NOT beyond `upper` against base times and other such reached times.  A
    time reached in `Beyond upper` is deferred and closed no further. -/
inductive Reached (base : Set T) (N : Set T) (upper : Finset T) : T → Prop
  | seed {x : T} : x ∈ N → Reached base N upper x
  | joinBase {x y : T} : Reached base N upper x → x ∉ Beyond upper → y ∈ base →
      Reached base N upper (x ⊔ y)
  | joinActive {x y : T} : Reached base N upper x → x ∉ Beyond upper →
      Reached base N upper y → y ∉ Beyond upper → Reached base N upper (x ⊔ y)

def reachedSet (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  {x | Reached (baseSet σ b upper) (seedSet σ b upper) upper x}

/-- The interesting times evaluated this round. -/
def activeSet (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  reachedSet σ b upper \ Beyond upper

/-- The interesting times deferred to later rounds. -/
def pendedSet (σ : State T A) (b : T →₀ A) (upper : Finset T) : Set T :=
  reachedSet σ b upper ∩ Beyond upper

/-- One round of the operator (TARGETS2 steps 3–4), from the state before the round to the
    state after, delivering batch `b` and advancing the frontier to `upper`.

    The emission `e` is supported on the active times and makes the output correct there
    (step 3).  The new pending set CONTAINS the deferred times plus the carried ones, and
    is confined beyond the frontier (step 4, relaxed): the implementation warns about
    more times than the enumeration defers — e.g. joins with already-finalized times it
    kept around "to scare future times" — and over-warning is harmless, since a warned
    time that owes nothing is evaluated to an empty correction when it comes due.  The
    new stored input and output are ADVERSARIAL: any update sets whose accumulations
    agree with the true ones on `Beyond upper` ("Compaction, modeled adversarially"). -/
structure Step (f : A → A) (upper : Finset T) (b : T →₀ A)
    (σ : State T A) (e : T →₀ A) (σ' : State T A) : Prop where
  emit_support : ↑e.support ⊆ activeSet σ b upper
  emit_correct : ∀ t ∈ activeSet σ b upper, acc (σ.output + e) t = f (acc (σ.stored + b) t)
  pending_super : pendedSet σ b upper ∪ (σ.pending ∩ Beyond upper) ⊆ σ'.pending
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

/-- Scenario 3, half two: a reached time beyond `upper` is deferred into the next pending
    set. -/
theorem Step.reached_beyond_pended {f : A → A} {upper : Finset T} {b : T →₀ A}
    {σ σ' : State T A} {e : T →₀ A} (hs : Step f upper b σ e σ')
    {x : T} (hr : x ∈ reachedSet σ b upper) (hb : x ∈ Beyond upper) : x ∈ σ'.pending :=
  hs.pending_super (Or.inl ⟨hr, hb⟩)

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

/-- The three-part discrepancy trace of a round: the change term, the carried staleness,
    minus the emission. -/
noncomputable def roundTrace (dC dP : T → A) (SC SP : Finset T) (e : T →₀ A) : T → A :=
  fun s => (if s ∈ SC then dC s else 0) + (if s ∈ SP then dP s else 0)
    + (if s ∈ e.support then -(e s) else 0)

/-- **The round-discrepancy package.**  Under the invariant at round `r`, the discrepancy
    `f∘input − emitted` after round `r + 1` (with the run's own emission) is represented
    by `roundTrace` — the change term over the STORED representative, the carried
    staleness, minus the emission — at every time, and in band-gated form below the new
    frontier; its live in-band points are all ACTIVE (change points reach from batch
    seeds, carried points from due pending seeds, emission points by `emit_support`); and
    it vanishes at active times, by `emit_correct` through the stored-representative
    agreements.  `step_clause1` and `step_residual` are corollaries. -/
theorem Run.round_discrepancy {f : A → A} {n : ℕ} (R : Run T A f n) {r : ℕ} (hr : r < n)
    (h1 : ∀ t, t ∉ Beyond (R.frontier r) → acc (R.emitted r) t = f (acc (R.input r) t))
    {SP : Finset T} {dP : T → A}
    (h2rep : ∀ t ∈ Beyond (R.frontier r),
      f (acc (R.input r) t) - acc (R.emitted r) t = ∑ s ∈ Cut SP t, dP s)
    (h2supp : ∀ x, dP x ≠ 0 →
      x ∈ supClosure ((R.state r).pending ∪ ↑(R.state r).stored.support
          ∪ ↑(R.state r).output.support) ∧
      ∃ p ∈ (R.state r).pending, p ≤ x) :
    ∃ (SC : Finset T) (dC : T → A),
      (∀ x, dC x ≠ 0 → x ∈ cl ((R.state r).stored.support ∪ (R.batch (r + 1)).support) ∧
        ∃ nu ∈ (R.batch (r + 1)).support, nu ≤ x) ∧
      (∀ t, f (acc (R.input r + R.batch (r + 1)) t) - acc (R.emitted r) t
          - acc (R.emit (r + 1)) t
        = ∑ s ∈ Cut (SC ∪ SP ∪ (R.emit (r + 1)).support) t,
            roundTrace dC dP SC SP (R.emit (r + 1)) s) ∧
      (∀ t, t ∉ Beyond (R.frontier (r + 1)) →
        f (acc (R.input r + R.batch (r + 1)) t) - acc (R.emitted r) t
          - acc (R.emit (r + 1)) t
        = ∑ s ∈ Cut (SC ∪ SP ∪ (R.emit (r + 1)).support) t,
            (if s ∈ Beyond (R.frontier (r + 1)) then 0
              else roundTrace dC dP SC SP (R.emit (r + 1)) s)) ∧
      (∀ x, roundTrace dC dP SC SP (R.emit (r + 1)) x ≠ 0 →
        x ∉ Beyond (R.frontier (r + 1)) →
        x ∈ activeSet (R.state r) (R.batch (r + 1)) (R.frontier (r + 1))) ∧
      (∀ w ∈ activeSet (R.state r) (R.batch (r + 1)) (R.frontier (r + 1)),
        f (acc (R.input r + R.batch (r + 1)) w) - acc (R.emitted r) w
          - acc (R.emit (r + 1)) w = 0) := by
  classical
  have hrn : r ≤ n := le_of_lt hr
  have hstep := R.step r hr
  have hbsupp : ↑(R.batch (r + 1)).support ⊆ Beyond (R.frontier r) :=
    fun x hx => (R.batch_support r hr hx).1
  have hpend : (R.state r).pending ⊆ Beyond (R.frontier r) := R.pending_beyond r hrn
  obtain ⟨SC, dC, hsuppC, hCrep⟩ :=
    true_change_coverage (u := R.frontier r) f
      (fun t ht => R.stored_agrees_input r hrn t ht) hbsupp
  have h2live : ∀ x, dP x ≠ 0 → x ∈ Beyond (R.frontier r) := by
    intro x hx
    obtain ⟨-, p, hpP, hpx⟩ := h2supp x hx
    exact beyond_isUpperSet _ hpx (hpend hpP)
  have h2all : ∀ t',
      f (acc (R.input r) t') - acc (R.emitted r) t' = ∑ s ∈ Cut SP t', dP s := by
    intro t'
    by_cases ht' : t' ∈ Beyond (R.frontier r)
    · exact h2rep t' ht'
    · rw [h1 t' ht', sub_self]
      symm
      apply Finset.sum_eq_zero
      intro s hs
      simp only [Cut, Finset.mem_filter] at hs
      by_contra hds
      exact ht' (beyond_isUpperSet _ hs.2 (h2live s hds))
  have hArep : ∀ t',
      f (acc (R.input r + R.batch (r + 1)) t') - acc (R.emitted r) t'
        - acc (R.emit (r + 1)) t'
      = ∑ s ∈ Cut (SC ∪ SP ∪ (R.emit (r + 1)).support) t',
          ((if s ∈ SC then dC s else 0) + (if s ∈ SP then dP s else 0)
            + (if s ∈ (R.emit (r + 1)).support then -(R.emit (r + 1) s) else 0)) := by
    intro t'
    have hgate : ∀ s,
        ((if s ∈ SC then dC s else 0) + (if s ∈ SP then dP s else 0)
          + (if s ∈ (R.emit (r + 1)).support then -(R.emit (r + 1) s) else 0))
        = ((if s ∈ SC ∪ SP then
              ((if s ∈ SC then dC s else 0) + (if s ∈ SP then dP s else 0)) else 0)
            + (if s ∈ (R.emit (r + 1)).support then -(R.emit (r + 1) s) else 0)) := by
      intro s
      by_cases hs : s ∈ SC ∪ SP
      · rw [if_pos hs]
      · rw [if_neg hs]
        simp only [Finset.mem_union, not_or] at hs
        rw [if_neg hs.1, if_neg hs.2]
        simp only [zero_add]
    have hc : f (acc (R.input r + R.batch (r + 1)) t') - f (acc (R.input r) t')
        = ∑ s ∈ Cut SC t', dC s := hCrep t'
    have hneg : ∑ s ∈ Cut (R.emit (r + 1)).support t', -(R.emit (r + 1) s)
        = -acc (R.emit (r + 1)) t' := by
      rw [Finset.sum_neg_distrib]
      rfl
    rw [Finset.sum_congr rfl (fun s _ => hgate s), cut_sum_union, cut_sum_union,
      ← hc, ← h2all t', hneg]
    abel
  have hlive0 : ∀ x,
      ((if x ∈ SC then dC x else 0) + (if x ∈ SP then dP x else 0)
        + (if x ∈ (R.emit (r + 1)).support then -(R.emit (r + 1) x) else 0)) ≠ 0 →
      x ∉ Beyond (R.frontier (r + 1)) →
      x ∈ activeSet (R.state r) (R.batch (r + 1)) (R.frontier (r + 1)) := by
    intro x hx hxu
    have h3 : dC x ≠ 0 ∨ dP x ≠ 0 ∨ x ∈ (R.emit (r + 1)).support := by
      by_contra hcon
      simp only [not_or, ne_eq, not_not] at hcon
      obtain ⟨h₁, h₂, h₃⟩ := hcon
      apply hx
      rw [if_neg h₃]
      by_cases hxC : x ∈ SC <;> by_cases hxP : x ∈ SP <;>
        simp [hxC, hxP, h₁, h₂]
    rcases h3 with h | h | h
    · obtain ⟨hcl, nu, hnu, hnux⟩ := hsuppC x h
      refine ⟨reached_of_mem_cl (base := baseSet (R.state r) (R.batch (r + 1))
          (R.frontier (r + 1))) ?_ ?_ hnux hcl hxu, hxu⟩
      · intro y hy _
        rw [Finset.coe_union] at hy
        rcases hy with hy | hy
        · exact Or.inl (Or.inl hy)
        · exact Or.inr (Or.inl hy)
      · exact Or.inl (Finset.mem_coe.mpr hnu)
    · obtain ⟨hcl, p, hpP, hpx⟩ := h2supp x h
      have hpact : p ∉ Beyond (R.frontier (r + 1)) :=
        fun hc => hxu (beyond_isUpperSet _ hpx hc)
      refine ⟨reached_of_mem_cl (base := baseSet (R.state r) (R.batch (r + 1))
          (R.frontier (r + 1))) ?_ ?_ hpx hcl hxu, hxu⟩
      · intro y hy hyx
        rcases hy with (hy | hy) | hy
        · exact Or.inr (Or.inr ⟨hy, fun hc => hxu (beyond_isUpperSet _ hyx hc)⟩)
        · exact Or.inl (Or.inl hy)
        · exact Or.inl (Or.inr hy)
      · exact Or.inr ⟨hpP, hpact⟩
    · exact hstep.emit_support (Finset.mem_coe.mpr h)
  have hgW : ∀ w ∈ activeSet (R.state r) (R.batch (r + 1)) (R.frontier (r + 1)),
      f (acc (R.input r + R.batch (r + 1)) w) - acc (R.emitted r) w
        - acc (R.emit (r + 1)) w = 0 := by
    intro w hw
    have hwl : w ∈ Beyond (R.frontier r) := by
      refine Reached.mem_of_upperSet (beyond_isUpperSet _) ?_ hw.1
      intro y hy
      rcases hy with hy | hy
      · exact hbsupp hy
      · exact hpend hy.1
    have hec := hstep.emit_correct w hw
    rw [acc_add, acc_add, R.output_agrees_emitted r hrn w hwl,
      R.stored_agrees_input r hrn w hwl] at hec
    rw [acc_add, ← hec]
    abel
  refine ⟨SC, dC, hsuppC, ?_, ?_, ?_, hgW⟩
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
    exact hlive0 x hx hxu

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
  obtain ⟨SC, dC, -, -, hArep', hlive, hgW⟩ := R.round_discrepancy hr h1 h2rep h2supp
  intro t htu
  have hδ := rep_zero_of_live_vanish (fun w hw => hArep' w hw.2) hgW
    (fun x _ hx => by
      have hx' : (if x ∈ Beyond (R.frontier (r + 1)) then (0 : A)
          else roundTrace dC dP SC SP (R.emit (r + 1)) x) ≠ 0 := hx
      have hxu : x ∉ Beyond (R.frontier (r + 1)) := fun hc => hx' (if_pos hc)
      rw [if_neg hxu] at hx'
      exact hlive x hx' hxu)
    (hArep' t htu)
  rw [R.emitted_succ, acc_add, R.input_succ]
  rw [sub_sub] at hδ
  exact (sub_eq_zero.mp hδ).symm

/-- **The residual staleness trace.**  Under the invariant at round `r`, the staleness
    after round `r + 1` is a difference trace at EVERY time, whose live points are beyond
    the new frontier, inside the closure of the OLD ground set, and at or above some NEW
    pending time — by the first-crossing argument for change points, and still-pending or
    due-seed witnesses for carried points.  In-band live points are impossible: the round
    trace vanishes there outright. -/
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
        x ∈ supClosure ((R.state r).pending ∪ ↑(R.state r).stored.support
              ∪ ↑(R.state r).output.support ∪ ↑(R.batch (r + 1)).support) ∧
        ∃ p ∈ (R.state (r + 1)).pending, p ≤ x := by
  classical
  have hstep := R.step r hr
  obtain ⟨SC, dC, hsuppC, hArep, hArep', hlive, hgW⟩ :=
    R.round_discrepancy hr h1 h2rep h2supp
  have hzero := live_eq_zero_of_vanish (fun w hw => hArep' w hw.2) hgW
    (fun x _ hx => by
      have hx' : (if x ∈ Beyond (R.frontier (r + 1)) then (0 : A)
          else roundTrace dC dP SC SP (R.emit (r + 1)) x) ≠ 0 := hx
      have hxu : x ∉ Beyond (R.frontier (r + 1)) := fun hc => hx' (if_pos hc)
      rw [if_neg hxu] at hx'
      exact hlive x hx' hxu)
  refine ⟨SC ∪ SP ∪ (R.emit (r + 1)).support,
    roundTrace dC dP SC SP (R.emit (r + 1)), ?_, ?_⟩
  · intro t
    rw [R.input_succ, R.emitted_succ, acc_add (R.emitted r) (R.emit (r + 1)), ← sub_sub]
    exact hArep t
  · intro x hx
    replace hx : ((if x ∈ SC then dC x else 0) + (if x ∈ SP then dP x else 0)
        + (if x ∈ (R.emit (r + 1)).support then -(R.emit (r + 1) x) else 0)) ≠ 0 := by
      simpa only [roundTrace] using hx
    have hxSA : x ∈ SC ∪ SP ∪ (R.emit (r + 1)).support := by
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
    have hnotact : x ∉ activeSet (R.state r) (R.batch (r + 1)) (R.frontier (r + 1)) :=
      fun hc => hc.2 hxb
    have hxe : x ∉ (R.emit (r + 1)).support :=
      fun hc => hnotact (hstep.emit_support (Finset.mem_coe.mpr hc))
    rw [if_neg hxe, add_zero] at hx
    have h2 : dC x ≠ 0 ∨ dP x ≠ 0 := by
      by_contra hcon
      simp only [not_or, ne_eq, not_not] at hcon
      apply hx
      rw [hcon.1, hcon.2]
      simp
    refine ⟨hxb, ?_, ?_⟩
    · rcases h2 with h | h
      · refine supClosure_mono ?_ (hsuppC x h).1
        rw [Finset.coe_union]
        rintro z (hz | hz)
        · exact Or.inl (Or.inl (Or.inr hz))
        · exact Or.inr hz
      · exact supClosure_mono Set.subset_union_left (h2supp x h).1
    · rcases h2 with h | h
      · obtain ⟨hcl, nu, hnu, hnux⟩ := hsuppC x h
        obtain ⟨p, hpr, hpb, hple⟩ :=
          exists_pended_le_of_mem_cl (base := baseSet (R.state r) (R.batch (r + 1))
            (R.frontier (r + 1))) (N := seedSet (R.state r) (R.batch (r + 1))
            (R.frontier (r + 1)))
            (fun y hy _ => by
              rw [Finset.coe_union] at hy
              rcases hy with hy | hy
              · exact Or.inl (Or.inl hy)
              · exact Or.inr (Or.inl hy))
            (Or.inl (Finset.mem_coe.mpr hnu)) hnux hcl hxb
        exact ⟨p, hstep.pending_super (Or.inl ⟨hpr, hpb⟩), hple⟩
      · by_cases hyp : ∃ y ∈ (R.state r).pending, y ≤ x ∧ y ∈ Beyond (R.frontier (r + 1))
        · obtain ⟨y, hyP, hyx, hyb⟩ := hyp
          exact ⟨y, hstep.pending_super (Or.inr ⟨hyP, hyb⟩), hyx⟩
        · replace hyp : ∀ y ∈ (R.state r).pending, y ≤ x →
              y ∉ Beyond (R.frontier (r + 1)) :=
            fun y hy hyx hyb => hyp ⟨y, hy, hyx, hyb⟩
          obtain ⟨hcl, p₀, hp₀P, hp₀x⟩ := h2supp x h
          have hp₀due : p₀ ∉ Beyond (R.frontier (r + 1)) := hyp p₀ hp₀P hp₀x
          obtain ⟨p, hpr, hpb, hple⟩ :=
            exists_pended_le_of_mem_cl (base := baseSet (R.state r) (R.batch (r + 1))
              (R.frontier (r + 1))) (N := seedSet (R.state r) (R.batch (r + 1))
              (R.frontier (r + 1)))
              (fun y hy hyx => by
                rcases hy with (hy | hy) | hy
                · exact Or.inr (Or.inr ⟨hy, hyp y hy hyx⟩)
                · exact Or.inl (Or.inl hy)
                · exact Or.inl (Or.inr hy))
              (Or.inr ⟨hp₀P, hp₀due⟩) hp₀x hcl hxb
          exact ⟨p, hstep.pending_super (Or.inl ⟨hpr, hpb⟩), hple⟩

/-- **The transfer property**: the residual staleness trace, supported in the OLD ground
    set's closure above a new pending time, can be re-based onto the NEW representatives'
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
        x ∈ supClosure ((R.state r).pending ∪ ↑(R.state r).stored.support
              ∪ ↑(R.state r).output.support ∪ ↑(R.batch (r + 1)).support) ∧
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

/-- **Stream correctness, modulo the transfer.**  TARGETS2.md's goal theorem: for every
    run — every choice of rounds, frontiers, emissions, and adversary — satisfying the
    re-representation transfer, the emitted corrections accumulate to the reduction of
    the true input at every finalized time. -/
theorem Run.stream_correct_of_transfer {f : A → A} {n : ℕ} (R : Run T A f n)
    (hf0 : f 0 = 0) (htr : R.Transfer) : R.StreamCorrect := by
  have hinv : ∀ r, r ≤ n → R.INV r := by
    intro r
    induction r with
    | zero => exact fun _ => inv_zero hf0 R
    | succ r ih => exact fun hr => R.inv_step htr (by omega) (ih (by omega))
  exact fun t ht => (hinv n le_rfl).1 t ht

/-! ## Uniqueness: the emitted stream is THE difference trace (`TARGETS2.md` corollary) -/

theorem acc_neg (d : T →₀ A) (t : T) : acc (-d) t = -acc d t := by
  unfold acc
  rw [Finsupp.support_neg, ← Finset.sum_neg_distrib]
  exact Finset.sum_congr rfl (fun x _ => Finsupp.neg_apply d x)

theorem acc_sub (d1 d2 : T →₀ A) (t : T) : acc (d1 - d2) t = acc d1 t - acc d2 t := by
  rw [sub_eq_add_neg, acc_add, acc_neg, sub_eq_add_neg]

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
    ones, nothing more.  This is TARGETS2.md's step 4 as originally written; the relaxed
    `pending_*` fields follow from exactness. -/
theorem Step.of_pending_eq {f : A → A} {upper : Finset T} {b : T →₀ A}
    {σ σ' : State T A} {e : T →₀ A} (hP : σ.pending.Finite)
    (emit_support : ↑e.support ⊆ activeSet σ b upper)
    (emit_correct : ∀ t ∈ activeSet σ b upper,
      acc (σ.output + e) t = f (acc (σ.stored + b) t))
    (pending_eq : σ'.pending = pendedSet σ b upper ∪ (σ.pending ∩ Beyond upper))
    (stored_agrees : ∀ t ∈ Beyond upper, acc σ'.stored t = acc (σ.stored + b) t)
    (output_agrees : ∀ t ∈ Beyond upper, acc σ'.output t = acc (σ.output + e) t) :
    Step f upper b σ e σ' where
  emit_support := emit_support
  emit_correct := emit_correct
  pending_super := by rw [pending_eq]
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

/-- Pending sets are finite. -/
theorem Run.pending_finite {f : A → A} {n : ℕ} (R : Run T A f n) :
    ∀ r ≤ n, ((R.state r).pending).Finite := by
  intro r hr
  cases r with
  | zero =>
    rw [R.state_zero]
    exact Set.finite_empty
  | succ r => exact (R.step r (by omega)).pending_finite

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
    obtain ⟨-, -, p, hpP, hps⟩ := hsupp s hds
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
