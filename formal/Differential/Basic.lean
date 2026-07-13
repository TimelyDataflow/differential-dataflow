import Mathlib

/-!
# Difference traces and accumulation — the shared core

The accumulation homomorphism common to every file in this directory.  An update set `d : T →₀ A`
(finitely many updates at lattice-valued times `T`, valued in an abelian group `A`) accumulates at a
query time `t` to the sum of updates at times `≤ t`.  `acc` is an `AddCommGroup` homomorphism from
update sets to functions `T → A`, over any join-semilattice `T`.

`Coverage.lean` (interesting times), `Model.lean` (the incremental reduce operator), and
`Compositional.lean` (compositional adequacy) all build on this vocabulary.  Keeping it here, rather
than inside any one of them, is what points the dependency arrow the right way.
-/

open Finset
open scoped Classical

namespace Trace

variable {T : Type*} [SemilatticeSup T]
variable {A : Type*} [AddCommGroup A]

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

/-- `acc` as a `Finsupp.sum`, for transport along support relocations. -/
theorem acc_eq_finsupp_sum (d : T →₀ A) (t : T) :
    acc d t = d.sum fun x a => if x ≤ t then a else 0 := by
  unfold acc Finsupp.sum
  rw [Finset.sum_filter]

/-- Any support relocation `g` that preserves comparison to `t` on the support preserves the
    accumulation at `t`.  `Finsupp.mapDomain g` is relocation *with consolidation* (colliding times
    merge, cancelled updates vanish). -/
theorem acc_mapDomain (g : T → T) (d : T →₀ A) (t : T)
    (hg : ∀ x ∈ d.support, (g x ≤ t ↔ x ≤ t)) :
    acc (Finsupp.mapDomain g d) t = acc d t := by
  rw [acc_eq_finsupp_sum, acc_eq_finsupp_sum,
      Finsupp.sum_mapDomain_index (by simp) (fun b m₁ m₂ => by split <;> simp)]
  exact Finsupp.sum_congr fun x hx => by simp only [hg x hx]

theorem acc_neg (d : T →₀ A) (t : T) : acc (-d) t = -acc d t := by
  unfold acc
  rw [Finsupp.support_neg, ← Finset.sum_neg_distrib]
  exact Finset.sum_congr rfl (fun x _ => Finsupp.neg_apply d x)

theorem acc_sub (d1 d2 : T →₀ A) (t : T) : acc (d1 - d2) t = acc d1 t - acc d2 t := by
  rw [sub_eq_add_neg, acc_add, acc_neg, sub_eq_add_neg]

end Trace
