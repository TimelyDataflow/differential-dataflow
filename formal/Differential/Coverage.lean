import Mathlib

/-!
# Interesting-time determination in differential dataflow

Differential dataflow represents evolving data by updates at lattice-valued times.
The data at any point in time is the accumulation of updates at less-or-equal times.
The output of a function applied to data evolves with the input, but it may change at different times.
The output changes can occur only at times that are the lattice join of times of input updates.
For total orders, this means the output can only change at input times, but not so for general orders.

This file is the *general keystone*.  With `Represents SUPP UPDS FUNC` as a claim that `FUNC` can be
represented by updates `UPDS` over support `SUPP`, we prove:

    exists_diff_trace_comp
        (hI : Represents E dI I)
        (f  : A → A) (hf0 : f 0 = 0)
    :
      ∃ S d, (↑S ⊆ cl E) ∧ Represents S d (fun t => f (I t))

In words: for *any* function `f` of the accumulated input, the output `f ∘ I` is again a difference
trace, whose support lies in the join-closure of the input's edit set.  This is a statement about
differential computation as such - it assumes nothing about `f` beyond `f 0 = 0`, so LINEAR, JOIN,
and REDUCE are all instances (REDUCE at full strength).

The *per-round refinement* - how an incremental operator realizes this schedule (novel-involving
joins, the frontier band, pending obligations: `round_coverage` and friends) - now lives in
`RoundCoverage.lean`, and the executable run on top of it in `Model.lean`.  See `DESIGN.md` for the
full stack.

A prose summary of the development lives in `README.md`, alongside this file.
-/

open Finset
-- `≤` is not decidable over an arbitrary lattice, so `Cut = filter (· ≤ t)` needs classical logic.
open scoped Classical

namespace Coverage

variable {T : Type*} [SemilatticeSup T]
variable {A : Type*} [AddCommGroup A]

/-- The elements of `E` at or below `t` - the down-set trace. -/
noncomputable def Cut (E : Finset T) (t : T) : Finset T := E.filter (· ≤ t)

/-- `d` represents `g` as a difference trace supported on `S`:
    `g t = ∑_{s ∈ S, s ≤ t} d s`.  (Collections start empty, so there is no base term.) -/
def Represents (S : Finset T) (d : T → A) (g : T → A) : Prop :=
  ∀ t, g t = ∑ s ∈ Cut S t, d s

/-- `g` depends on `t` only through `Cut E t`. -/
def Simple (E : Finset T) (g : T → A) : Prop :=
  ∀ s t, Cut E s = Cut E t → g s = g t

/-- A difference trace is `Simple` over its support. -/
theorem Represents.simple {S : Finset T} {d : T → A} {g : T → A}
    (h : Represents S d g) : Simple S g := by
  intro s t hst
  rw [h s, h t, hst]

omit [AddCommGroup A] in
/-- Post-composition by an opaque `f` preserves simplicity. -/
theorem Simple.comp {E : Finset T} {g : T → A} (h : Simple E g) (f : A → A) :
    Simple E (fun t => f (g t)) :=
  fun s t hst => congrArg f (h s t hst)

/-- Subtraction preserves simplicity over a common edit set. -/
theorem Simple.sub {E : Finset T} {g1 g2 : T → A} (h1 : Simple E g1) (h2 : Simple E g2) :
    Simple E (fun t => g1 t - g2 t) := by
  intro s t hst
  change g1 s - g2 s = g1 t - g2 t
  rw [h1 s t hst, h2 s t hst]

/-- **Trace addition.**  The sum of two difference traces is a difference trace over the union of
    their supports.  Its update at `s` is the sum of the two updates (each `0` off its support), so
    the sum's support lies in the union of the two supports. -/
theorem Represents.add {S1 S2 : Finset T} {d1 d2 : T → A} {g1 g2 : T → A}
    (h1 : Represents S1 d1 g1) (h2 : Represents S2 d2 g2) :
    Represents (S1 ∪ S2)
      (fun s => (if s ∈ S1 then d1 s else 0) + (if s ∈ S2 then d2 s else 0))
      (fun t => g1 t + g2 t) := by
  classical
  intro t
  have e1 : ∑ s ∈ Cut (S1 ∪ S2) t, (if s ∈ S1 then d1 s else 0) = ∑ s ∈ Cut S1 t, d1 s := by
    rw [← Finset.sum_filter]
    congr 1
    ext s; simp only [Cut, Finset.mem_filter, Finset.mem_union]; tauto
  have e2 : ∑ s ∈ Cut (S1 ∪ S2) t, (if s ∈ S2 then d2 s else 0) = ∑ s ∈ Cut S2 t, d2 s := by
    rw [← Finset.sum_filter]
    congr 1
    ext s; simp only [Cut, Finset.mem_filter, Finset.mem_union]; tauto
  change g1 t + g2 t = ∑ s ∈ Cut (S1 ∪ S2) t,
    ((if s ∈ S1 then d1 s else 0) + (if s ∈ S2 then d2 s else 0))
  rw [Finset.sum_add_distrib, e1, e2, h1 t, h2 t]

omit [SemilatticeSup T] in
/-- The support of a sum-trace's update function lies in the union of the two supports. -/
theorem Represents.add_support {S1 S2 : Finset T} {d1 d2 : T → A} {x : T}
    (hx : (if x ∈ S1 then d1 x else 0) + (if x ∈ S2 then d2 x else 0) ≠ 0) :
    d1 x ≠ 0 ∨ d2 x ≠ 0 := by
  by_contra hcon
  simp only [not_or, ne_eq, not_not] at hcon
  rw [hcon.1, hcon.2] at hx
  simp at hx

/-- `Represents` transfers along a pointwise equality of the represented function. -/
theorem Represents.congr {S : Finset T} {d : T → A} {g g' : T → A}
    (h : Represents S d g) (hgg : ∀ t, g t = g' t) : Represents S d g' := by
  intro t; rw [← hgg t]; exact h t

/-- The join-closure of a finite edit set (all sups of nonempty subsets, plus the elements). -/
noncomputable def cl (E : Finset T) : Set T := supClosure (E : Set T)

theorem subset_cl (E : Finset T) : (E : Set T) ⊆ cl E := subset_supClosure

/-- Membership in the join-closure: `x ∈ cl E` iff `x` is the join of a nonempty finite subset of
    `E`.  This is definitional (`supClosure` is defined by exactly this predicate). -/
theorem mem_cl_iff {E : Finset T} (x : T) :
    x ∈ cl E ↔ ∃ (t : Finset T) (ht : t.Nonempty), ↑t ⊆ (↑E : Set T) ∧ t.sup' ht id = x :=
  Iff.rfl

omit [AddCommGroup A] in
/-- A `Simple` predicate transfers to any larger edit set. -/
theorem Simple.mono {E C : Finset T} {g : T → A} (h : Simple E g) (hEC : E ⊆ C) :
    Simple C g := by
  intro s t hst
  refine h s t ?_
  ext x
  simp only [Cut, mem_filter]
  constructor
  · rintro ⟨hxE, hxs⟩
    have : x ∈ Cut C s := by simp [Cut, mem_filter, hEC hxE, hxs]
    rw [hst] at this
    simp only [Cut, mem_filter] at this
    exact ⟨hxE, this.2⟩
  · rintro ⟨hxE, hxt⟩
    have : x ∈ Cut C t := by simp [Cut, mem_filter, hEC hxE, hxt]
    rw [← hst] at this
    simp only [Cut, mem_filter] at this
    exact ⟨hxE, this.2⟩

/-- The join-closure of a finite set, materialized as a `Finset`. -/
noncomputable def clFinset (E : Finset T) : Finset T :=
  (E.finite_toSet.supClosure).toFinset

theorem coe_clFinset (E : Finset T) : (↑(clFinset E) : Set T) = cl E := by
  simp [clFinset, cl, Set.Finite.coe_toFinset]

theorem subset_clFinset (E : Finset T) : E ⊆ clFinset E := by
  intro x hx
  have : x ∈ (↑(clFinset E) : Set T) := by rw [coe_clFinset]; exact subset_supClosure hx
  exact this

theorem supClosed_clFinset (E : Finset T) : SupClosed (↑(clFinset E) : Set T) := by
  rw [coe_clFinset]; exact supClosed_supClosure

/-- The greedy difference-trace: `dtrace C b g x = (g x - b) - ∑_{c ∈ C, c < x} dtrace ... c`
    for `x ∈ C`, and `0` otherwise.  Defined by well-founded recursion on `#{c ∈ C | c < x}`. -/
noncomputable def dtrace (C : Finset T) (b : A) (g : T → A) : T → A := fun x =>
  if x ∈ C then (g x - b) - ∑ c ∈ (C.filter (· < x)).attach, dtrace C b g c.1 else 0
termination_by x => (C.filter (· < x)).card
decreasing_by
  rename_i c
  have hc := c.2
  simp only [mem_filter] at hc
  apply Finset.card_lt_card
  refine Finset.ssubset_iff_of_subset (fun a ha => ?_) |>.2 ⟨c.1, ?_, ?_⟩
  · simp only [mem_filter] at ha ⊢
    exact ⟨ha.1, lt_trans ha.2 hc.2⟩
  · simp only [mem_filter]; exact ⟨hc.1, hc.2⟩
  · simp only [mem_filter, lt_self_iff_false, and_false, not_false_eq_true]

/-- **Lemma A.** At a point `x ∈ C`, summing the trace over the cut recovers `g x - b`. -/
theorem dtrace_cut_eq {C : Finset T} {b : A} {g : T → A} {x : T} (hx : x ∈ C) :
    ∑ c ∈ Cut C x, dtrace C b g c = g x - b := by
  have hsplit : Cut C x = insert x (C.filter (· < x)) := by
    ext y
    simp only [Cut, mem_filter, mem_insert]
    constructor
    · rintro ⟨hyC, hyx⟩
      rcases lt_or_eq_of_le hyx with h | h
      · exact Or.inr ⟨hyC, h⟩
      · exact Or.inl h
    · rintro (rfl | ⟨hyC, hyx⟩)
      · exact ⟨hx, le_refl _⟩
      · exact ⟨hyC, le_of_lt hyx⟩
  have hxnotin : x ∉ C.filter (· < x) := by simp [mem_filter]
  rw [hsplit, Finset.sum_insert hxnotin]
  have hunfold : dtrace C b g x = (g x - b) - ∑ c ∈ (C.filter (· < x)).attach, dtrace C b g c.1 := by
    rw [dtrace]; simp only [hx, if_true]
  rw [hunfold, Finset.sum_attach (C.filter (· < x)) (fun c => dtrace C b g c)]
  abel

/-- **Reconstruction.** If `g` vanishes on empty cuts, the greedy trace `dtrace C 0 g` represents
    `g` over a sup-closed `C`. -/
theorem dtrace_represents {C : Finset T} (hC : SupClosed (↑C : Set T))
    {g : T → A} (h : Simple C g)
    (hbase : ∀ t, Cut C t = ∅ → g t = 0) :
    Represents C (dtrace C 0 g) g := by
  intro t
  by_cases hne : (Cut C t).Nonempty
  · set m := (Cut C t).sup' hne id with hm_def
    have hm_mem : m ∈ C := by
      rw [hm_def]
      apply Finset.sup'_induction
      · intro a1 ha1 a2 ha2
        exact Finset.mem_coe.1 (hC (Finset.mem_coe.2 ha1) (Finset.mem_coe.2 ha2))
      · intro i hi
        exact Finset.mem_of_mem_filter i hi
    have hle_t : m ≤ t := by
      rw [hm_def]
      exact Finset.sup'_le _ _ (fun i hi => (Finset.mem_filter.1 hi).2)
    have hcut_eq : Cut C t = Cut C m := by
      ext y
      simp only [Cut, Finset.mem_filter]
      constructor
      · rintro ⟨hyC, hyt⟩
        refine ⟨hyC, ?_⟩
        have hy : y ∈ Cut C t := by simp only [Cut, Finset.mem_filter]; exact ⟨hyC, hyt⟩
        rw [hm_def]
        simpa using Finset.le_sup' (id : T → T) hy
      · rintro ⟨hyC, hym⟩
        exact ⟨hyC, le_trans hym hle_t⟩
    calc g t = g m := h t m hcut_eq
      _ = ∑ c ∈ Cut C m, dtrace C 0 g c := by rw [dtrace_cut_eq hm_mem]; abel
      _ = ∑ c ∈ Cut C t, dtrace C 0 g c := by rw [hcut_eq]
  · rw [Finset.not_nonempty_iff_eq_empty] at hne
    simp only [hne, Finset.sum_empty]
    exact hbase t hne

/-- **support-in-upset engine.** If `g` vanishes on a lower set `D`, the greedy trace (with base
    `0`) vanishes on `D` too.  Proved by strong induction on `#{c ∈ C | c < x}`: a point of `D`
    has all its predecessors in `D` (lower set), which the induction kills, leaving `g x = 0`. -/
theorem dtrace_zero_on_lowerSet {C : Finset T} {g : T → A}
    {D : Set T} (hD : IsLowerSet D) (hvanish : ∀ t ∈ D, g t = 0) :
    ∀ (n : Nat) (x : T), (C.filter (· < x)).card ≤ n → x ∈ D → dtrace C 0 g x = 0 := by
  intro n
  induction n with
  | zero =>
    intro x hcard hxD
    by_cases hxC : x ∈ C
    · rw [dtrace]
      simp only [hxC, if_true]
      have hemp : C.filter (· < x) = ∅ := Finset.card_eq_zero.1 (Nat.le_zero.1 hcard)
      rw [hemp, Finset.attach_empty, Finset.sum_empty, hvanish x hxD]
      abel
    · rw [dtrace]; simp [hxC]
  | succ n ih =>
    intro x hcard hxD
    by_cases hxC : x ∈ C
    · rw [dtrace]
      simp only [hxC, if_true]
      rw [Finset.sum_attach (C.filter (· < x)) (fun c => dtrace C 0 g c)]
      have hsum : ∑ c ∈ C.filter (· < x), dtrace C 0 g c = 0 := by
        apply Finset.sum_eq_zero
        intro c hc
        simp only [mem_filter] at hc
        have hcD : c ∈ D := hD (le_of_lt hc.2) hxD
        have hlt : (C.filter (· < c)).card < (C.filter (· < x)).card := by
          apply Finset.card_lt_card
          refine Finset.ssubset_iff_of_subset (fun a ha => ?_) |>.2 ⟨c, ?_, ?_⟩
          · simp only [mem_filter] at ha ⊢
            exact ⟨ha.1, lt_trans ha.2 hc.2⟩
          · simp only [mem_filter]; exact ⟨hc.1, hc.2⟩
          · simp only [mem_filter, lt_self_iff_false, and_false, not_false_eq_true]
        exact ih c (by omega) hcD
      rw [hsum, hvanish x hxD]
      abel
    · rw [dtrace]; simp [hxC]

/-- **Core construction.** Over a sup-closed finite set `C`, a `Simple C` function that vanishes on
    empty cuts is a difference trace supported on `C`. -/
theorem exists_diff_trace_supClosed {C : Finset T} (hC : SupClosed (↑C : Set T))
    {g : T → A} (h : Simple C g) (hbase : ∀ t, Cut C t = ∅ → g t = 0) :
    ∃ (d : T → A), (∀ x, d x ≠ 0 → x ∈ C) ∧ Represents C d g := by
  refine ⟨dtrace C 0 g, ?_, dtrace_represents hC h hbase⟩
  intro x hx
  by_contra hxC
  exact hx (by rw [dtrace]; simp [hxC])

/-- **Vanishing core.** Over a sup-closed `C`, if `g` vanishes on a lower set `D` that contains
    every empty-cut time, then `g` is a difference trace whose support avoids `D` entirely. -/
theorem exists_diff_trace_supClosed_vanish {C : Finset T} (hC : SupClosed (↑C : Set T))
    {g : T → A} (h : Simple C g)
    {D : Set T} (hD : IsLowerSet D)
    (hbaseD : ∀ t, Cut C t = ∅ → t ∈ D)
    (hvanish : ∀ t ∈ D, g t = 0) :
    ∃ (d : T → A), (∀ x, d x ≠ 0 → x ∈ C ∧ x ∉ D) ∧ Represents C d g := by
  have hbase0 : ∀ t, Cut C t = ∅ → g t = 0 := fun t ht => hvanish t (hbaseD t ht)
  refine ⟨dtrace C 0 g, ?_, dtrace_represents hC h hbase0⟩
  intro x hx
  refine ⟨?_, ?_⟩
  · by_contra hxC; exact hx (by rw [dtrace]; simp [hxC])
  · intro hxD
    exact hx (dtrace_zero_on_lowerSet (C := C) (g := g) hD hvanish _ x le_rfl hxD)

/-- **Keystone.** A `Simple E` function is itself a difference trace whose support lies in
    the join-closure `cl E` of the edit set. -/
theorem exists_diff_trace {E : Finset T} {g : T → A} (h : Simple E g)
    (hbase : ∀ t, Cut E t = ∅ → g t = 0) :
    ∃ (S : Finset T) (d : T → A), (↑S ⊆ cl E) ∧ Represents S d g := by
  have hbaseC : ∀ t, Cut (clFinset E) t = ∅ → g t = 0 := by
    intro t ht
    apply hbase t
    simp only [Cut, Finset.filter_eq_empty_iff] at ht ⊢
    intro c hc
    exact ht (subset_clFinset E hc)
  obtain ⟨d, _hsupp, hrep⟩ :=
    exists_diff_trace_supClosed (supClosed_clFinset E) (h.mono (subset_clFinset E)) hbaseC
  exact ⟨clFinset E, d, (coe_clFinset E).le, hrep⟩

/-- **exists_diff_trace_comp (coverage).** If the input `I` is a difference trace with edit set `E`, then for any
    function `f`, the composite `f ∘ I` is a difference trace whose support lies in the
    join-closure `cl E`.  Hence an operator can realize the output using only update times in
    `cl E` - the value-blind join-closure of the input's edit times. -/
theorem exists_diff_trace_comp {E : Finset T} {dI : T → A} {I : T → A}
    (hI : Represents E dI I) (f : A → A) (hf0 : f 0 = 0) :
    ∃ (S : Finset T) (d : T → A), (↑S ⊆ cl E) ∧ Represents S d (fun t => f (I t)) :=
  exists_diff_trace (hI.simple.comp f) (by
    intro t ht
    have hI0 : I t = 0 := by rw [hI t, ht, Finset.sum_empty]
    simp only [hI0, hf0])

end Coverage
