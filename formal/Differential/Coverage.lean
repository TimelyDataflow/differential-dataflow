import Mathlib

/-!
# Interesting-time determination in differential dataflow

Differential dataflow represents evolving data by updates at lattice-valued times.
The data at any point in time is the accumulation of updates at less-or-equal times.
The output of a function applied to data evolves with the input, but it may change at different times.
The output changes can occur only at times that are the lattice join of times of input updates.
For total orders, this means the output can only change at input times, but not so for general orders.

More formally, with `Represents SUPP UPDS FUNC` as a claim that `FUNC` can be represented by updates `UPDS` over support `SUPP`, we prove:

    round_coverage
        (h_prev : Represents s_prev u_prev prev)
        (h_diff : Represents s_diff u_diff diff)
        (f : A → B)
    :
      ∃ s_edit u_edit,
          (∀ x, u_edit x ≠ 0 → x ∈ cl (s_prev ∪ s_diff) ∧ ∃ nu ∈ s_diff, nu ≤ x)
        ∧ Represents s_edit u_edit (fun t => f (prev t + diff t) - f (prev t))

In words, for prev and diff functions, the output correction f(prev + diff) - f(prev) can be represented
as updates with support that lies in the join-closure of the input supports, intersected with the up-set
of the support for diff updates.

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

/-! ## Stage 2: the per-round refinement

An incremental operator never closes the *prior* (old input) times against themselves.  It seeds
on the *novel* (new-batch + deferred) times and closes them against `novel ∪ prior`.  So it
produces only the joins that involve at least one novel time, restricted to the round's interval.
We must show this still covers every output change of the round.

The round difference is `g = f(Inew) - f(Iold)`.  Because the batch adds only novel updates,
`g` is identically zero wherever *no novel time reaches* - a lower set.  The `support-in-upset`
engine then confines the trace of `g` to `Above(novel)`.  Intersected with the keystone's
`cl(prior ∪ novel)`, that is exactly `cl(prior ∪ novel) ∩ Above(novel)` - the novel-involving
joins.  The prior-only closure carries `dg = 0` and is provably skippable.
-/

/-- The complement of an upper set generated by `N` - "no novel time reaches `t`" - is a lower set. -/
theorem isLowerSet_not_above (N : Finset T) :
    IsLowerSet {t : T | ∀ nu ∈ N, ¬ nu ≤ t} := by
  intro a b hba ha nu hnu hnub
  exact ha nu hnu (le_trans hnub hba)

/-- **Keystone, vanishing form.** A `Simple E` function that vanishes on a lower set `D`
    (which contains every time no `E`-edit reaches) is a difference trace supported on
    `cl E`, with support disjoint from `D`. -/
theorem exists_diff_trace_vanish {E : Finset T} {g : T → A} (h : Simple E g)
    {D : Set T} (hD : IsLowerSet D)
    (hbaseE : ∀ t, (∀ c ∈ E, ¬ c ≤ t) → t ∈ D)
    (hvanish : ∀ t ∈ D, g t = 0) :
    ∃ (S : Finset T) (d : T → A),
      (↑S ⊆ cl E) ∧ (∀ x, d x ≠ 0 → x ∈ cl E ∧ x ∉ D) ∧ Represents S d g := by
  have hbaseD : ∀ t, Cut (clFinset E) t = ∅ → t ∈ D := by
    intro t ht
    apply hbaseE
    intro c hc hct
    have hmem : c ∈ Cut (clFinset E) t := by
      simp only [Cut, mem_filter]
      exact ⟨subset_clFinset E hc, hct⟩
    rw [ht] at hmem
    simp at hmem
  obtain ⟨d, hsupp, hrep⟩ :=
    exists_diff_trace_supClosed_vanish (supClosed_clFinset E)
      (h.mono (subset_clFinset E)) hD hbaseD hvanish
  refine ⟨clFinset E, d, (coe_clFinset E).le, ?_, hrep⟩
  intro x hx
  obtain ⟨hxC, hxD⟩ := hsupp x hx
  refine ⟨?_, hxD⟩
  have : x ∈ (↑(clFinset E) : Set T) := hxC
  rwa [coe_clFinset] at this

/-- **Stage 2 (refined coverage).**  Let `g` be a round's output difference `f(Inew) - f(Iold)`.
    Assume it is `Simple` over `prior ∪ novel` and vanishes wherever no novel time reaches
    (`Inew = Iold` there).  Then every output change is supported on
    `cl(prior ∪ novel) ∩ Above(novel)` - precisely the novel-involving joins produced by closing
    `novel` against `novel ∪ prior`.  Closing `prior` by itself is unnecessary. -/
theorem round_change_coverage {prior novel : Finset T} {g : T → A}
    (h : Simple (prior ∪ novel) g)
    (hvanish : ∀ t, (∀ nu ∈ novel, ¬ nu ≤ t) → g t = 0) :
    ∃ (S : Finset T) (d : T → A),
      (∀ x, d x ≠ 0 → x ∈ cl (prior ∪ novel) ∧ ∃ nu ∈ novel, nu ≤ x)
      ∧ Represents S d g := by
  classical
  have hD : IsLowerSet {t : T | ∀ nu ∈ novel, ¬ nu ≤ t} := isLowerSet_not_above novel
  have hbaseE : ∀ t, (∀ c ∈ prior ∪ novel, ¬ c ≤ t) → t ∈ {t : T | ∀ nu ∈ novel, ¬ nu ≤ t} := by
    intro t ht
    simp only [Set.mem_setOf_eq]
    intro nu hnu
    exact ht nu (Finset.mem_union_right prior hnu)
  obtain ⟨S, d, _hScl, hsupp, hrep⟩ :=
    exists_diff_trace_vanish h hD hbaseE (fun t ht => hvanish t ht)
  refine ⟨S, d, ?_, hrep⟩
  intro x hx
  obtain ⟨hxcl, hxD⟩ := hsupp x hx
  refine ⟨hxcl, ?_⟩
  by_contra hcon
  apply hxD
  simp only [Set.mem_setOf_eq]
  intro nu hnu hnux
  exact hcon ⟨nu, hnu, hnux⟩

/-- **Round coverage.**  A round starts from an old input `Iold` - a difference trace over
    `prior` - and adds a batch delta `batch`, a difference trace over `novel` (a pure
    increment).  The new input is `Inew = Iold + batch`.  For any function `f`, the round's output
    change `g = f∘Inew - f∘Iold` is a difference trace whose support lies in
    `cl(prior ∪ novel) ∩ Above(novel)`.

    This grounds `round_change_coverage`: both its abstract hypotheses are discharged from the trace
    structure.  `g` is `Simple` because `Iold` and `batch` are (post-composition and subtraction
    preserve it); `g` vanishes off `Above(novel)` because there `Cut novel t = ∅`, so `batch t = 0`
    and `Inew t = Iold t`. -/
theorem round_coverage {prior novel : Finset T}
    {dOld : T → A} {Iold : T → A} (hold : Represents prior dOld Iold)
    {dBatch : T → A} {batch : T → A} (hbatch : Represents novel dBatch batch)
    (f : A → A) :
    ∃ (S : Finset T) (d : T → A),
      (∀ x, d x ≠ 0 → x ∈ cl (prior ∪ novel) ∧ ∃ nu ∈ novel, nu ≤ x)
      ∧ Represents S d (fun t => f (Iold t + batch t) - f (Iold t)) := by
  have hSimpleOld : Simple (prior ∪ novel) Iold :=
    (hold.simple).mono Finset.subset_union_left
  have hSimpleBatch : Simple (prior ∪ novel) batch :=
    (hbatch.simple).mono Finset.subset_union_right
  -- `g` is `Simple`: cuts agreeing on `prior ∪ novel` fix both `Iold` and `batch`, hence `g`.
  have hSimpleG : Simple (prior ∪ novel) (fun t => f (Iold t + batch t) - f (Iold t)) := by
    intro s t hst
    change f (Iold s + batch s) - f (Iold s) = f (Iold t + batch t) - f (Iold t)
    rw [hSimpleOld s t hst, hSimpleBatch s t hst]
  -- `g` vanishes where no novel time reaches: `batch t = 0` there, so `Inew t = Iold t`.
  have hvanish : ∀ t, (∀ nu ∈ novel, ¬ nu ≤ t) →
      (fun t => f (Iold t + batch t) - f (Iold t)) t = 0 := by
    intro t htv
    have hcut : Cut novel t = ∅ := by
      simp only [Cut, Finset.filter_eq_empty_iff]
      intro nu hnu
      exact htv nu hnu
    have hbatch0 : batch t = 0 := by
      rw [hbatch t, hcut, Finset.sum_empty]
    simp only [hbatch0, add_zero, sub_self]
  exact round_change_coverage hSimpleG hvanish

/-- **Linchpin.**  `cl(prior ∪ novel) ∩ Above(novel)` is *exactly* the set of joins of a nonempty
    finite subset of `prior ∪ novel` that includes at least one novel time.  That is the set an
    implementation enumerates by seeding on `novel` and closing under join against `novel ∪ prior`
    (`active_times` in the reduce operator).  So the machine-checked coverage set
    `cl(prior ∪ novel) ∩ Above(novel)` and the implementation's computed set coincide. -/
theorem cl_inter_above_eq_novelJoins {prior novel : Finset T} (x : T) :
    (x ∈ cl (prior ∪ novel) ∧ ∃ nu ∈ novel, nu ≤ x) ↔
      ∃ (F : Finset T) (hne : F.Nonempty),
        (↑F ⊆ (↑(prior ∪ novel) : Set T)) ∧ (∃ nu ∈ F, nu ∈ novel) ∧ F.sup' hne id = x := by
  constructor
  · rintro ⟨hxcl, nu, hnu, hnux⟩
    rw [mem_cl_iff] at hxcl
    obtain ⟨t, ht, hts, hsup⟩ := hxcl
    refine ⟨insert nu t, ⟨nu, Finset.mem_insert_self nu t⟩, ?_,
      ⟨nu, Finset.mem_insert_self nu t, hnu⟩, ?_⟩
    · rw [Finset.coe_insert, Set.insert_subset_iff]
      exact ⟨Finset.mem_coe.2 (Finset.mem_union.2 (Or.inr hnu)), hts⟩
    · apply le_antisymm
      · apply Finset.sup'_le
        intro b hb
        rw [Finset.mem_insert] at hb
        rcases hb with rfl | hbt
        · exact hnux
        · rw [← hsup]; exact Finset.le_sup' id hbt
      · rw [← hsup]
        apply Finset.sup'_le
        intro b hb
        exact Finset.le_sup' id (Finset.mem_insert_of_mem hb)
  · rintro ⟨F, hne, hFsub, ⟨nu, hnuF, hnunovel⟩, hsup⟩
    refine ⟨?_, nu, hnunovel, ?_⟩
    · rw [← hsup]
      exact finsetSup'_mem_supClosure hne (fun i hi => hFsub (Finset.mem_coe.2 hi))
    · rw [← hsup]
      simpa using Finset.le_sup' (id : T → T) hnuF

/-- **Frontier.**  When every novel time is at or beyond the frontier `lower`, every point of the
    round's coverage set is `≥ lower`.  So the operator's corrections all fall in `Above(lower)` -
    it never needs to emit behind the frontier.  Split by `upper`, this is `[lower, upper)` (emit
    now) and `Above(upper)` (defer as pending); there is no `< lower` remainder. -/
theorem round_support_ge_lower {prior novel : Finset T} {lower x : T}
    (hlower : ∀ nu ∈ novel, lower ≤ nu)
    (hx : x ∈ cl (prior ∪ novel) ∧ ∃ nu ∈ novel, nu ≤ x) :
    lower ≤ x := by
  obtain ⟨_, nu, hnu, hnux⟩ := hx
  exact le_trans (hlower nu hnu) hnux

/-! ## Stage 3: the round invariant

The operator stores an output trace `O` and maintains the invariant that `O` equals `f∘I` on the
*finalized* region - the times not yet at or beyond the frontier `lower`.  A round advances the
frontier to `upper` by emitting a correction, and must re-establish the invariant for `upper`.

The point - and the reason a round is not vacuous - is that advancing correctness across the whole
band requires touching only the *coverage* times, not every time below `upper`.  We prove: there is
an emitted correction `P`, a difference trace whose updates all fall in `[lower, upper)` and in the
coverage set `cl(Ein ∪ Eout)`, such that `O + P` is correct on the finalized region below `upper`.
By induction over rounds, the invariant holds throughout; the output is always correct up to the
frontier. -/

/-- The finalized region below a single frontier element `u` is a lower set. -/
theorem isLowerSet_not_le (u : T) : IsLowerSet {t : T | ¬ u ≤ t} := by
  intro a b hba ha hub
  exact ha (le_trans hub hba)

/-- **Keystone, vanishing form (base-decoupled).**  Like `exists_diff_trace_vanish`, but the base
    condition (`g = 0` on empty cuts, forcing base `0`) is supplied over the edit set directly,
    rather than requiring the lower set `D` to contain the empty-cut times.  This lets us confine
    the trace to `Above(lower)` even when `lower` is not itself edit-reachable. -/
theorem exists_diff_trace_vanish' {E : Finset T} {g : T → A} (h : Simple E g)
    (hbaseE : ∀ t, (∀ c ∈ E, ¬ c ≤ t) → g t = 0)
    {D : Set T} (hD : IsLowerSet D) (hvanish : ∀ t ∈ D, g t = 0) :
    ∃ (S : Finset T) (d : T → A),
      (↑S ⊆ cl E) ∧ (∀ x, d x ≠ 0 → x ∈ cl E ∧ x ∉ D) ∧ Represents S d g := by
  have hbase0 : ∀ t, Cut (clFinset E) t = ∅ → g t = 0 := by
    intro t ht
    apply hbaseE
    intro c hc hct
    have hmem : c ∈ Cut (clFinset E) t := by
      simp only [Cut, mem_filter]
      exact ⟨subset_clFinset E hc, hct⟩
    rw [ht] at hmem
    simp at hmem
  refine ⟨clFinset E, dtrace (clFinset E) 0 g, (coe_clFinset E).le, ?_,
    dtrace_represents (supClosed_clFinset E) (h.mono (subset_clFinset E)) hbase0⟩
  intro x hx
  refine ⟨?_, ?_⟩
  · have hxC : x ∈ clFinset E := by
      by_contra hxC; exact hx (by rw [dtrace]; simp [hxC])
    have hxset : x ∈ (↑(clFinset E) : Set T) := hxC
    rwa [coe_clFinset] at hxset
  · intro hxD
    exact hx (dtrace_zero_on_lowerSet (C := clFinset E) (g := g) hD hvanish _ x le_rfl hxD)

/-- **Round invariant advance.**  Given an input trace `I`, an output trace `O` agreeing with `f∘I`
    on the finalized region below `lower` (the invariant), and the base agreement `f bI = bO`, there
    is an emitted correction `P` - itself a difference trace - whose updates all lie in the band
    `[lower, upper)` and in the coverage set `cl(Ein ∪ Eout)`, such that `O + P` agrees with `f∘I`
    on the finalized region below `upper`.  Touching only the coverage times in the band suffices to
    advance the frontier; by induction the output stays correct up to the frontier at every round. -/
theorem round_advances_invariant {Ein Eout : Finset T}
    {dI : T → A} {I : T → A} (hI : Represents Ein dI I)
    {dO : T → A} {O : T → A} (hO : Represents Eout dO O)
    (f : A → A) (hinit : f 0 = 0)
    {lower upper : T}
    (hINV : ∀ t, ¬ lower ≤ t → O t = f (I t)) :
    ∃ (P : T → A) (SP : Finset T) (dP : T → A),
      Represents SP dP P
      ∧ (∀ x, dP x ≠ 0 → x ∈ cl (Ein ∪ Eout) ∧ lower ≤ x ∧ ¬ upper ≤ x)
      ∧ (∀ t, ¬ upper ≤ t → O t + P t = f (I t)) := by
  classical
  set delta : T → A := fun t => f (I t) - O t with hdelta
  have hSI : Simple (Ein ∪ Eout) I := (hI.simple).mono Finset.subset_union_left
  have hSfI : Simple (Ein ∪ Eout) (fun t => f (I t)) := hSI.comp f
  have hSO : Simple (Ein ∪ Eout) O := (hO.simple).mono Finset.subset_union_right
  have hSdelta : Simple (Ein ∪ Eout) delta := hSfI.sub hSO
  -- delta = 0 at empty cuts: I = 0, O = 0, and f 0 = 0.
  have hbasedelta : ∀ t, (∀ c ∈ Ein ∪ Eout, ¬ c ≤ t) → delta t = 0 := by
    intro t ht
    have hIcut : Cut Ein t = ∅ := by
      simp only [Cut, Finset.filter_eq_empty_iff]
      intro c hc; exact ht c (Finset.mem_union_left Eout hc)
    have hOcut : Cut Eout t = ∅ := by
      simp only [Cut, Finset.filter_eq_empty_iff]
      intro c hc; exact ht c (Finset.mem_union_right Ein hc)
    have hIb : I t = 0 := by rw [hI t, hIcut, Finset.sum_empty]
    have hOb : O t = 0 := by rw [hO t, hOcut, Finset.sum_empty]
    simp only [hdelta, hIb, hOb, hinit, sub_self]
  -- delta = 0 on the finalized region below `lower`, from the invariant.
  have hvandelta : ∀ t ∈ {t : T | ¬ lower ≤ t}, delta t = 0 := by
    intro t ht
    simp only [Set.mem_setOf_eq] at ht
    simp only [hdelta, hINV t ht, sub_self]
  obtain ⟨Sdelta, ddelta, hScldelta, hsuppdelta, hrepdelta⟩ :=
    exists_diff_trace_vanish' hSdelta hbasedelta (isLowerSet_not_le lower) hvandelta
  -- Emit delta's trace restricted to the finalized region below `upper`.
  refine ⟨fun t => ∑ s ∈ Cut Sdelta t, (if upper ≤ s then (0 : A) else ddelta s),
    Sdelta, (fun s => if upper ≤ s then (0 : A) else ddelta s), ?_, ?_, ?_⟩
  · intro t; rfl
  · intro x hx
    have key : ¬ upper ≤ x ∧ ddelta x ≠ 0 := by
      by_cases hux : upper ≤ x
      · exact absurd (if_pos hux) hx
      · refine ⟨hux, ?_⟩
        intro h0
        exact hx (show (if upper ≤ x then (0 : A) else ddelta x) = 0 by rw [if_neg hux]; exact h0)
    obtain ⟨hxcl, hxD⟩ := hsuppdelta x key.2
    simp only [Set.mem_setOf_eq, not_not] at hxD
    exact ⟨hxcl, hxD, key.1⟩
  · intro t htu
    change O t + (∑ s ∈ Cut Sdelta t, (if upper ≤ s then (0 : A) else ddelta s)) = f (I t)
    have hPeq : ∑ s ∈ Cut Sdelta t, (if upper ≤ s then (0 : A) else ddelta s) = delta t := by
      rw [hrepdelta t]
      apply Finset.sum_congr rfl
      intro s hs
      simp only [Cut, Finset.mem_filter] at hs
      rw [if_neg (fun hcon => htu (le_trans hcon hs.2))]
    rw [hPeq]
    simp only [hdelta]
    abel

/-- **Novel joins ∪ pending cover the round - and pending is exactly the staleness of `O`.**

    The correction the operator must realize is `f∘Inew - O`.  We decompose it as
    `(f∘Inew - f∘Iold) + (f∘Iold - O)` and give an explicit difference trace for it (so the operator
    can emit exactly these updates) whose every support point is **either**

    * in `cl(prior ∪ novel) ∩ Above(novel)` - the novel-involving joins computed from the input, **or**
    * in `cl(prior ∪ Eout) ∩ Above(lower)` - the change-points of `f∘Iold - O`, i.e. where the stored
      output is *stale*, all lying at or beyond the frontier.

    The second set is exactly the pending obligations: the invariant forces the staleness to vanish
    below `lower`, so the deferred work is precisely the output's stale points above the frontier.
    This proves the novel joins plus pending are *sufficient* - the operator never needs the full
    `cl(Ein ∪ Eout)` recompute, only the input closure plus the carried staleness. -/
theorem round_needed_set_decomposition {prior novel Eout : Finset T}
    {dIold : T → A} {Iold : T → A} (hIold : Represents prior dIold Iold)
    {dBatch : T → A} {batch : T → A} (hbatch : Represents novel dBatch batch)
    {dO : T → A} {O : T → A} (hO : Represents Eout dO O)
    (f : A → A) (hinit : f 0 = 0)
    {lower : T}
    (hINV : ∀ t, ¬ lower ≤ t → O t = f (Iold t)) :
    ∃ (S : Finset T) (d : T → A),
      Represents S d (fun t => f (Iold t + batch t) - O t)
      ∧ (∀ x, d x ≠ 0 →
          (x ∈ cl (prior ∪ novel) ∧ ∃ nu ∈ novel, nu ≤ x)
          ∨ (x ∈ cl (prior ∪ Eout) ∧ lower ≤ x)) := by
  classical
  -- Part 1: the incremental change `f∘Inew - f∘Iold`, covered by the novel-involving joins.
  obtain ⟨S1, d1, hsupp1, hrep1⟩ := round_coverage hIold hbatch f
  -- Part 2: the staleness `f∘Iold - O`, confined above the frontier (the pending obligations).
  have hSstale : Simple (prior ∪ Eout) (fun t => f (Iold t) - O t) :=
    (((hIold.simple).mono Finset.subset_union_left).comp f).sub
      ((hO.simple).mono Finset.subset_union_right)
  have hbaseStale : ∀ t, (∀ c ∈ prior ∪ Eout, ¬ c ≤ t) → (fun t => f (Iold t) - O t) t = 0 := by
    intro t ht
    have hIcut : Cut prior t = ∅ := by
      simp only [Cut, Finset.filter_eq_empty_iff]
      intro c hc; exact ht c (Finset.mem_union_left Eout hc)
    have hOcut : Cut Eout t = ∅ := by
      simp only [Cut, Finset.filter_eq_empty_iff]
      intro c hc; exact ht c (Finset.mem_union_right prior hc)
    have hIb : Iold t = 0 := by rw [hIold t, hIcut, Finset.sum_empty]
    have hOb : O t = 0 := by rw [hO t, hOcut, Finset.sum_empty]
    simp only [hIb, hOb, hinit, sub_self]
  have hvanStale : ∀ t ∈ {t : T | ¬ lower ≤ t}, (fun t => f (Iold t) - O t) t = 0 := by
    intro t ht
    simp only [Set.mem_setOf_eq] at ht
    simp only [hINV t ht, sub_self]
  obtain ⟨S2, d2, _hScl2, hsupp2, hrep2⟩ :=
    exists_diff_trace_vanish' hSstale hbaseStale (isLowerSet_not_le lower) hvanStale
  -- Combine: `(f∘Inew - f∘Iold) + (f∘Iold - O) = f∘Inew - O`.
  have hrep := (hrep1.add hrep2).congr (g' := fun t => f (Iold t + batch t) - O t) (fun t => by abel)
  refine ⟨S1 ∪ S2,
    (fun s => (if s ∈ S1 then d1 s else 0) + (if s ∈ S2 then d2 s else 0)), hrep, ?_⟩
  · intro x hx
    rcases Represents.add_support hx with h1 | h2
    · exact Or.inl (hsupp1 x h1)
    · obtain ⟨hxcl, hxD⟩ := hsupp2 x h2
      simp only [Set.mem_setOf_eq, not_not] at hxD
      exact Or.inr ⟨hxcl, hxD⟩

/-- **Pending is load-bearing.**  Consider a *draining* round: no new input (`novel = ∅`), the
    frontier simply advances over previously-deferred work.  Then the novel-involving joins are
    empty - `Above(∅) = ∅`, so every membership condition `∃ nu ∈ ∅, nu ≤ x` is false.  Yet the
    correction `f∘Iold - O` (the staleness) can be nonzero, and it is covered *entirely* by the
    pending obligations (`cl(prior ∪ Eout) ∩ Above(lower)`).  So the input-driven joins alone
    cannot cover a round; the deferred set is necessary, not a mere optimization. -/
theorem draining_round_needs_pending {prior Eout : Finset T}
    {dIold : T → A} {Iold : T → A} (hIold : Represents prior dIold Iold)
    {dO : T → A} {O : T → A} (hO : Represents Eout dO O)
    (f : A → A) (hinit : f 0 = 0)
    {lower : T}
    (hINV : ∀ t, ¬ lower ≤ t → O t = f (Iold t)) :
    ∃ (S : Finset T) (d : T → A),
      Represents S d (fun t => f (Iold t) - O t)
      ∧ (∀ x, d x ≠ 0 → x ∈ cl (prior ∪ Eout) ∧ lower ≤ x) := by
  -- Instantiate the decomposition with an empty batch (`novel = ∅`).
  have hbatch0 : Represents (∅ : Finset T) (fun _ => 0) (fun _ => (0 : A)) := by
    intro t; simp [Cut]
  obtain ⟨S, d, hrep, hsupp⟩ :=
    round_needed_set_decomposition hIold hbatch0 hO f hinit hINV
  refine ⟨S, d, hrep.congr (fun t => by simp), ?_⟩
  intro x hx
  -- The novel-joins disjunct is vacuous: it demands `∃ nu ∈ ∅, nu ≤ x`.
  rcases hsupp x hx with ⟨_, nu, hnu, _⟩ | h2
  · simp at hnu
  · exact h2

end Coverage
