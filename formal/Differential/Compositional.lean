import Differential.Basic
import Differential.Coverage
import Differential.Model

/-!
# Compositional adequacy of differential dataflow operators

This file is the *denotational-adequacy / compositionality* axis, distinct from the
*incremental-execution* axis of `Coverage.lean` and `Model.lean`.  Nothing here is about
interesting times, pending sets, frontiers, or compaction.  The single claim is:

> Each operator's implementation on finitely-supported update traces computes the same thing
> as the operator's mathematics applied pointwise to the *accumulated* data — and this composes,
> so any program built from the operators maintains the value it was meant to compute, as if
> re-evaluated moment-by-moment.

## The spine

`Trace.acc : (T →₀ A) → (T → A)` accumulates updates (`acc d t = ∑_{s ≤ t} d s`).  It is an
`AddCommGroup` homomorphism (`Trace.acc_add`, `acc_zero`, `acc_sub`) over ANY `[SemilatticeSup T]`
and `[AddCommGroup A]`.  We add its missing structural fact — INJECTIVITY on finitely-supported
traces (`acc_injective`) — which makes "the implementation trace" well defined.

An operator is a COMMUTING SQUARE.  `Adequate D impl` (below) says it commutes.  It is heterogeneous
in the TIME type, because some operators (`enter`, temporal filters) move mass from `T` to a
different time lattice `T'`:

    δ  ───impl──▶  impl δ           (T →₀ A)  →  (T' →₀ B)
    │                 │
   acc               acc
    ▼                 ▼
  acc δ ───D────▶  D (acc δ)        (T → A)   →  (T' → B)

The compositionality payoff is `Adequate.comp`: squares compose.

Fully proved (axiom-clean — only `propext`, `Classical.choice`, `Quot.sound`):
* the spine — `acc_eq_zero`, `acc_injective`, `Adequate`, `Adequate.comp`, `Adequate.id`;
* all four batch operator squares — `linImpl_adequate` (LINEAR/SUM), `timeImpl_adequate`
  (enter/temporal), `reduce_adequate` (REDUCE, via `Coverage.exists_diff_trace_comp`),
  `join_adequate` (JOIN convolution);
* the incremental layer — `Refines`, the chain rule `Refines.comp`, `Refines.of_additive`;
* the end-to-end capstone `Program.adequate` over `id / linear / reduce / par / join / seq` (JOIN is
  `Adequate.join` — a bilinear `par`, mono-typed `β : A →+ A →+ A`);
* ITERATE (delay-free, arbitrary seed `s`, round-wise body): the recursion `iterate_unroll`;
  `round_matches` (round-`n` state = `n`-th from-scratch iterate); `leave_stabilizes` /
  `leave_converged` (stabilization is free from finite support — no `FiniteRounds` hypothesis);
  `leave_loop` (trace fixpoint) and `leave_fixpoint` (accumulated fixpoint), with `leave_fixpoint_enter`
  (re-add / semi-naive) and `leave_fixpoint_impulse` (start-from-X / pure iteration) as the two regimes;
* ITERATE of a lifted `Program` body (Phase B): `denote_slice` (denotation is pointwise, commutes with
  round-slicing), `round_wise` (a lifted body's round-wise action IS its outer `Program.denote`),
  `iterate_program` (ties ITERATE into the calculus), the `par` pairing former (`Adequate.add`), and
  `acc_prod_fst` (mutual recursion needs no product lemma — it is `linImpl_adequate` for `fst`);
* ITERATE with DELAY (a `Causal` body, generalizing round-wise): a forward delay is a JOIN against a
  fixed collection, whose output time is a lattice join `≥` the input (`sup_le_iff`), so the body stays
  causal.  `acc_unique` (any two loop solutions accumulate identically — evaluation-order independence
  IS adequacy) and `leave_acc_unique` (`leave` is well defined).

Deferred: the `joinConst` operator that realizes `LINEAR(F)` as a JOIN against a fixed collection
(needs a bilinear product on the group — the heterogeneous `A ×+ B → C` of `join_adequate`, which
collides with the mono-typed `Program`), and the semantic question of when different arrival
schedules compute the same value (confluence — genuinely open, distinct from adequacy above).
-/

open Finset
open scoped Classical

namespace Compositional

open Trace (acc)

variable {T T' T'' : Type*} [SemilatticeSup T] [SemilatticeSup T'] [SemilatticeSup T'']
variable {A A' B C : Type*} [AddCommGroup A] [AddCommGroup A'] [AddCommGroup B] [AddCommGroup C]

/-! ## Spine: `acc` is injective on finitely-supported traces -/

/-- If a trace accumulates to `0` everywhere, it is `0`.  Proof: were the support nonempty, a
    MINIMAL support element `m` has `acc d m = d m` (nothing else in the support is `≤ m`), and
    `d m ≠ 0`, contradicting `acc d m = 0`.  This is Möbius inversion in its cheapest form; the
    full inverse is `Coverage.dtrace` / `Coverage.dtrace_represents`. -/
theorem acc_eq_zero {d : T →₀ A} (h : ∀ t, acc d t = 0) : d = 0 := by
  by_contra hne
  have hsupp : d.support.Nonempty := Finsupp.support_nonempty_iff.mpr hne
  obtain ⟨m, hm_mem, hm_min⟩ := d.support.exists_minimal hsupp
  have hfilter : d.support.filter (· ≤ m) = {m} := by
    apply Finset.eq_singleton_iff_unique_mem.mpr
    refine ⟨by simp only [Finset.mem_filter]; exact ⟨hm_mem, le_refl m⟩, ?_⟩
    intro x hx
    rw [Finset.mem_filter] at hx
    rcases eq_or_lt_of_le hx.2 with heq | hlt
    · exact heq
    · exact absurd (lt_of_le_of_lt (hm_min hx.1 hlt.le) hlt) (lt_irrefl m)
  have hval : acc d m = d m := by
    change (∑ x ∈ d.support.filter (· ≤ m), d x) = d m
    rw [hfilter, Finset.sum_singleton]
  rw [h m] at hval
  exact (Finsupp.mem_support_iff.mp hm_mem) hval.symm

/-- `acc` is injective: a trace is determined by its accumulation.  Hence THE implementing trace of
    an operator is unique, and the squares below are equations, not mere existence claims. -/
theorem acc_injective {d1 d2 : T →₀ A} (h : acc d1 = acc d2) : d1 = d2 := by
  have hz : d1 - d2 = 0 := by
    apply acc_eq_zero
    intro t
    rw [Trace.acc_sub, congrFun h t, sub_self]
  exact sub_eq_zero.mp hz

/-! ## The commuting-square predicate and its composition law -/

/-- The (time-heterogeneous) unary square commutes: implementing then accumulating equals
    accumulating then applying the denotation `D`. -/
def Adequate (D : (T → A) → (T' → B)) (impl : (T →₀ A) → (T' →₀ B)) : Prop :=
  ∀ δ, acc (impl δ) = D (acc δ)

/-- The binary square (for `JOIN`): two accumulation legs feed one denotation, same output time. -/
def Adequate₂ (D : (T → A) → (T → A') → (T → B))
    (impl : (T →₀ A) → (T →₀ A') → (T →₀ B)) : Prop :=
  ∀ δ δ', acc (impl δ δ') = D (acc δ) (acc δ')

/-- **Compositionality, the whole point.**  Squares compose.  Two rewrites — this is why a
    program's correctness reduces to per-operator correctness. -/
theorem Adequate.comp {D₁ : (T → A) → (T' → B)} {D₂ : (T' → B) → (T'' → C)}
    {i₁ : (T →₀ A) → (T' →₀ B)} {i₂ : (T' →₀ B) → (T'' →₀ C)}
    (h₁ : Adequate D₁ i₁) (h₂ : Adequate D₂ i₂) :
    Adequate (D₂ ∘ D₁) (i₂ ∘ i₁) := by
  intro δ
  show acc (i₂ (i₁ δ)) = D₂ (D₁ (acc δ))
  rw [h₂ (i₁ δ), h₁ δ]

/-- Identity is adequate (the unit of composition). -/
theorem Adequate.id : Adequate (T := T) (id : (T → A) → (T → A)) id := fun _ => rfl

/-- Adequacy is closed under pointwise ADDITION of operators — the `par`/pairing primitive, and the
    reason mutual recursion needs no product decomposition (`acc` is additive). -/
theorem Adequate.add {D₁ D₂ : (T → A) → (T → B)} {i₁ i₂ : (T →₀ A) → (T →₀ B)}
    (h₁ : Adequate D₁ i₁) (h₂ : Adequate D₂ i₂) :
    Adequate (fun g => D₁ g + D₂ g) (fun δ => i₁ δ + i₂ δ) := by
  intro δ
  funext t
  show acc (i₁ δ + i₂ δ) t = (D₁ (acc δ) + D₂ (acc δ)) t
  rw [Trace.acc_add, congrFun (h₁ δ) t, congrFun (h₂ δ) t]
  simp only [Pi.add_apply]

/-! ## Shape 1a: linear, pointwise in time (`map`, `filter`, `SUM`, `negate`, `concat`)

Data/difference coordinates hit by a group hom `φ : A →+ B`, time untouched.  Implementation is
`Finsupp.mapRange φ`; denotation is `φ` pointwise.  No reconstruction — `φ` commutes with the sum. -/

/-- Implementation: apply `φ` to every update's value. -/
noncomputable def linImpl (φ : A →+ B) (δ : T →₀ A) : T →₀ B :=
  Finsupp.mapRange φ (map_zero φ) δ

theorem linImpl_adequate (φ : A →+ B) :
    Adequate (fun (g : T → A) (t : T) => φ (g t)) (linImpl φ) := by
  intro δ
  funext t
  calc acc (linImpl φ δ) t
      = ∑ x ∈ δ.support.filter (· ≤ t), (linImpl φ δ) x :=
        Trace.acc_eq_sum_superset _ Finsupp.support_mapRange t
    _ = ∑ x ∈ δ.support.filter (· ≤ t), φ (δ x) := by
        refine Finset.sum_congr rfl fun x _ => ?_
        simp only [linImpl, Finsupp.mapRange_apply]
    _ = φ (∑ x ∈ δ.support.filter (· ≤ t), δ x) := (map_sum φ _ _).symm
    _ = φ (acc δ t) := rfl

/-! ## Shape 1b: linear, ACTING ON TIME (`enter`, `enter_at`, temporal filters, `delay`)

The case the naive story misses.  These move mass along the timestamp coordinate via a monotone
`h : T → T'`.  Implementation is `Finsupp.mapDomain h`; denotation PRE-COMPOSES with the residual /
Galois adjoint `h♯`:

    acc (mapDomain h δ) t' = ∑_{s : h s ≤ t'} δ s = ∑_{s : s ≤ h♯ t'} δ s = acc δ (h♯ t').

`Trace.acc_mapDomain` is the special case `h♯ = id`.  For `enter : t ↦ (t,0)` into `T × ℕ`,
`h♯ (t,n) = t`: entered data reads as the outer accumulation at EVERY round.  This is exactly what
lets `enter_at` inject fresh diffs at a chosen round and thereby spoil the "value at round n =
bodyⁿ ⊥" shortcut for `ITERATE`. -/

/-- Implementation: relocate each update's time by `h` (consolidating collisions). -/
noncomputable def timeImpl (h : T → T') (δ : T →₀ A) : T' →₀ A :=
  Finsupp.mapDomain h δ

/-- Given a Galois adjoint `hstar` for `h` (`h s ≤ t' ↔ s ≤ hstar t'`), the time action is adequate
    with denotation "read the input accumulation at `hstar t'`". -/
theorem timeImpl_adequate (h : T → T') (hstar : T' → T)
    (hadj : ∀ s t', h s ≤ t' ↔ s ≤ hstar t') :
    Adequate (A := A) (fun (g : T → A) (t' : T') => g (hstar t')) (timeImpl h) := by
  intro δ
  funext t'
  show acc (Finsupp.mapDomain h δ) t' = acc δ (hstar t')
  rw [Trace.acc_eq_finsupp_sum, Trace.acc_eq_finsupp_sum,
      Finsupp.sum_mapDomain_index (by simp) (fun b m₁ m₂ => by split <;> simp)]
  exact Finsupp.sum_congr fun x _ => by simp only [hadj x t']

/-! ## Shape 2: opaque pointwise (`REDUCE`)

`φ : A → B` arbitrary, per key, with `φ 0 = 0`.  Existence of an implementing trace is exactly
`Coverage.exists_diff_trace_comp`; with `acc_injective` it is unique.  The only place the reduce
machinery of `Coverage.lean` is reused. -/

/-- Bridge to `Coverage.lean`: a `Represents S d g` witness (a `(support, update-function)` pair)
    yields a `Finsupp` whose `acc` is `g`.  Clamp `d` to `S` and check the accumulation against the
    `Represents` sum. -/
theorem represents_acc {S : Finset T} {d : T → A} {g : T → A} (hrep : Coverage.Represents S d g) :
    ∃ e : T →₀ A, acc e = g ∧ (↑e.support : Set T) ⊆ (↑S : Set T) := by
  refine ⟨Finsupp.onFinset S (fun x => if x ∈ S then d x else 0)
      (fun a ha => by by_contra hns; apply ha; simp only [if_neg hns]), ?_, ?_⟩
  · funext t
    rw [Trace.acc_eq_sum_superset _ Finsupp.support_onFinset_subset t,
        show g t = ∑ s ∈ Coverage.Cut S t, d s from hrep t]
    refine Finset.sum_congr rfl fun x hx => ?_
    show (if x ∈ S then d x else 0) = d x
    rw [if_pos (Finset.mem_filter.mp hx).1]
  · exact Finset.coe_subset.mpr Finsupp.support_onFinset_subset

/-- For `φ 0 = 0`, `φ ∘ acc δ` is realized by SOME finitely-supported trace — via
    `Coverage.exists_diff_trace_comp` (support in the join-closure of `δ`'s edits) and `represents_acc`.
    (Endomaps `A → A`, matching `exists_diff_trace_comp`; the `A → B` case is the same over `B`.) -/
theorem exists_acc_eq_comp (φ : A → A) (hφ : φ 0 = 0) (δ : T →₀ A) :
    ∃ e : T →₀ A, acc e = (fun t => φ (acc δ t))
      ∧ (↑e.support : Set T) ⊆ Coverage.cl δ.support := by
  obtain ⟨S, d, hScl, hrep⟩ := Coverage.exists_diff_trace_comp (Model.acc_represents δ) φ hφ
  obtain ⟨e, he, hes⟩ := represents_acc hrep
  exact ⟨e, he, hes.trans hScl⟩

/-- Implementation of `REDUCE φ`: the (unique, by `acc_injective`) trace accumulating to `φ ∘ acc δ`. -/
noncomputable def reduceImpl (φ : A → A) (hφ : φ 0 = 0) (δ : T →₀ A) : T →₀ A :=
  (exists_acc_eq_comp φ hφ δ).choose

theorem reduce_adequate (φ : A → A) (hφ : φ 0 = 0) :
    Adequate (fun (g : T → A) (t : T) => φ (g t)) (reduceImpl φ hφ) :=
  fun δ => (exists_acc_eq_comp φ hφ δ).choose_spec.1

/-! ## #1/#2: spatial coverage carried through the square, and its composition (work axis)

`Adequate` says the square commutes; it says nothing about WHERE the output edits land.
`Coverage.exists_diff_trace_comp` proves they lie in the join-closure of the input's edits, and
`exists_acc_eq_comp` now *carries* that clause rather than discarding it.  `AdequateCov` bundles the
bound onto the square, and `AdequateCov.comp` shows it composes — closure idempotence (neighbourhoods
do not inflate under nesting, `supClosure_min`) plus `.trans`.  This is the statement that *bounded
work is compositional*.  Mono-typed in time (same `T` in and out): the regime where coverage applies;
the time-relocation operators (`timeImpl`) live on a different bound. -/

/-- Adequacy WITH the spatial-coverage bound: the square commutes AND the output edits lie in the
    join-closure of the input's edit set. -/
def AdequateCov (D : (T → A) → (T → A)) (impl : (T →₀ A) → (T →₀ A)) : Prop :=
  ∀ δ, acc (impl δ) = D (acc δ) ∧ (↑(impl δ).support : Set T) ⊆ Coverage.cl δ.support

/-- The commuting-square half of `AdequateCov` is exactly `Adequate`. -/
theorem AdequateCov.toAdequate {D : (T → A) → (T → A)} {impl : (T →₀ A) → (T →₀ A)}
    (h : AdequateCov D impl) : Adequate D impl := fun δ => (h δ).1

/-- **#2 — the spatial bound composes.**  `i₁`'s edits lie in `cl δ.support`; `i₂`'s in the closure
    of `i₁`'s edits; closure-of-closure is closure, so the composite's edits stay in `cl δ.support`. -/
theorem AdequateCov.comp {D₁ D₂ : (T → A) → (T → A)} {i₁ i₂ : (T →₀ A) → (T →₀ A)}
    (h₁ : AdequateCov D₁ i₁) (h₂ : AdequateCov D₂ i₂) :
    AdequateCov (D₂ ∘ D₁) (i₂ ∘ i₁) := by
  intro δ
  refine ⟨?_, ?_⟩
  · show acc (i₂ (i₁ δ)) = D₂ (D₁ (acc δ))
    rw [(h₂ (i₁ δ)).1, (h₁ δ).1]
  · refine (h₂ (i₁ δ)).2.trans ?_
    exact supClosure_min (h₁ δ).2 supClosed_supClosure

/-- Identity is `AdequateCov` (edits unchanged, trivially in the closure). -/
theorem AdequateCov.id : AdequateCov (T := T) (A := A) id id :=
  fun δ => ⟨rfl, Coverage.subset_cl δ.support⟩

/-- LINEAR tightens the bound: an additive map manufactures no new times, so the output edits sit in
    the INPUT edits, a fortiori in their closure. -/
theorem linImpl_adequateCov (φ : A →+ A) :
    AdequateCov (fun (g : T → A) (t : T) => φ (g t)) (linImpl φ) := by
  intro δ
  refine ⟨linImpl_adequate φ δ, ?_⟩
  have hsub : (linImpl φ δ).support ⊆ δ.support := Finsupp.support_mapRange
  exact (Finset.coe_subset.mpr hsub).trans (Coverage.subset_cl δ.support)

/-- REDUCE at full strength: the output edits lie in the join-closure of the input edits — the clause
    `exists_acc_eq_comp` now carries. -/
theorem reduce_support (φ : A → A) (hφ : φ 0 = 0) (δ : T →₀ A) :
    (↑(reduceImpl φ hφ δ).support : Set T) ⊆ Coverage.cl δ.support :=
  (exists_acc_eq_comp φ hφ δ).choose_spec.2

theorem reduce_adequateCov (φ : A → A) (hφ : φ 0 = 0) :
    AdequateCov (fun (g : T → A) (t : T) => φ (g t)) (reduceImpl φ hφ) :=
  fun δ => ⟨reduce_adequate φ hφ δ, reduce_support φ hφ δ⟩

/-- `AdequateCov` is closed under pointwise ADDITION (the `par`/pairing primitive): the sum's edits
    lie in the union of the two branches' edits, hence in the common closure. -/
theorem AdequateCov.add {D₁ D₂ : (T → A) → (T → A)} {i₁ i₂ : (T →₀ A) → (T →₀ A)}
    (h₁ : AdequateCov D₁ i₁) (h₂ : AdequateCov D₂ i₂) :
    AdequateCov (fun g => D₁ g + D₂ g) (fun δ => i₁ δ + i₂ δ) := by
  intro δ
  refine ⟨?_, ?_⟩
  · funext t
    show acc (i₁ δ + i₂ δ) t = (D₁ (acc δ) + D₂ (acc δ)) t
    rw [Trace.acc_add, congrFun (h₁ δ).1 t, congrFun (h₂ δ).1 t]
    simp only [Pi.add_apply]
  · refine (Finset.coe_subset.mpr Finsupp.support_add).trans ?_
    rw [Finset.coe_union]
    exact Set.union_subset (h₁ δ).2 (h₂ δ).2

/-! ## Shape 3: bilinear (`JOIN`)

`β : A →+ A' →+ B` bilinear.  Output update at the JOIN `a ⊔ b` of paired input times, valued
`β (δ a) (δ' b)`.  The square reduces to bilinearity plus one lattice fact:

    a ⊔ b ≤ t  ↔  a ≤ t ∧ b ≤ t                    (`sup_le_iff`)

so that `acc (joinImpl β δ δ') t = β (acc δ t) (acc δ' t)`.  `JOIN` is "bilinearity + `sup_le_iff`". -/

/-- `acc · t` bundled as a group hom, for pushing through `Finsupp.sum`. -/
noncomputable def accHom (t : T) : (T →₀ A) →+ A :=
  AddMonoidHom.mk' (fun d => acc d t) (fun d1 d2 => Trace.acc_add d1 d2 t)

@[simp] theorem accHom_apply (t : T) (d : T →₀ A) : accHom t d = acc d t := rfl

/-- Implementation: convolution over the join-semilattice. -/
noncomputable def joinImpl (β : A →+ A' →+ B) (δ : T →₀ A) (δ' : T →₀ A') : T →₀ B :=
  δ.sum fun a x => δ'.sum fun b y => Finsupp.single (a ⊔ b) (β x y)

theorem join_adequate (β : A →+ A' →+ B) :
    Adequate₂ (fun (g : T → A) (g' : T → A') (t : T) => β (g t) (g' t)) (joinImpl β) := by
  intro δ δ'
  funext t
  show acc (joinImpl β δ δ') t = β (acc δ t) (acc δ' t)
  -- Both sides reduce to the same double sum over the supports.
  have hL : acc (joinImpl β δ δ') t
      = ∑ a ∈ δ.support, ∑ b ∈ δ'.support,
          (if a ≤ t ∧ b ≤ t then β (δ a) (δ' b) else 0) := by
    show accHom t (joinImpl β δ δ') = _
    rw [joinImpl, Finsupp.sum, map_sum]
    refine Finset.sum_congr rfl fun a _ => ?_
    rw [Finsupp.sum, map_sum]
    refine Finset.sum_congr rfl fun b _ => ?_
    show acc (Finsupp.single (a ⊔ b) (β (δ a) (δ' b))) t = _
    simp only [Trace.acc_single, sup_le_iff]
  have hR : β (acc δ t) (acc δ' t)
      = ∑ a ∈ δ.support, ∑ b ∈ δ'.support,
          (if a ≤ t ∧ b ≤ t then β (δ a) (δ' b) else 0) := by
    have e1 : acc δ t = ∑ a ∈ δ.support, (if a ≤ t then δ a else 0) := by
      rw [Trace.acc_eq_finsupp_sum, Finsupp.sum]
    have e2 : acc δ' t = ∑ b ∈ δ'.support, (if b ≤ t then δ' b else 0) := by
      rw [Trace.acc_eq_finsupp_sum, Finsupp.sum]
    rw [e1, e2, map_sum β, AddMonoidHom.finsetSum_apply]
    refine Finset.sum_congr rfl fun a _ => ?_
    rw [map_sum (β _)]
    refine Finset.sum_congr rfl fun b _ => ?_
    by_cases ha : a ≤ t <;> by_cases hb : b ≤ t <;> simp [ha, hb]
  rw [hL, hR]

/-! ## Shape 4: fixpoint (`ITERATE`)

Nested scope adds an iteration coordinate: inner time `T × ℕ`.

* `enter`    : place at round 0            (`timeImpl (·, 0)`, adjoint `Prod.fst`)
* `feedback` : increment the round          (`timeImpl (t,n) ↦ (t,n+1)`)
* `leave`    : project onto `T`, consolidating over all rounds  (`timeImpl Prod.fst`)

Loop variable defined by the trace equation `x = enter input + feedback (body x)`, NOT by `bodyⁿ ⊥`:
`enter_at` (Shape 1b) may inject fresh diffs at rounds `> 0`, so input mass is spread over ℕ.
Convergence needs FINITE support along ℕ per outer time (`FiniteRounds`), dischargeable by
assumption, a finite iteration domain, or an explicit non-termination effect. -/

section Iterate
variable (body : ((T × ℕ) →₀ A) → ((T × ℕ) →₀ A))

/-- Enter the nested scope at round 0. -/
noncomputable def enter (δ : T →₀ A) : (T × ℕ) →₀ A :=
  Finsupp.mapDomain (fun t => (t, 0)) δ

/-- Advance every update by one round. -/
noncomputable def feedback (δ : (T × ℕ) →₀ A) : (T × ℕ) →₀ A :=
  Finsupp.mapDomain (fun p => (p.1, p.2 + 1)) δ

/-- Leave the scope: forget the round, consolidating across all iterations. -/
noncomputable def leave (δ : (T × ℕ) →₀ A) : T →₀ A :=
  Finsupp.mapDomain Prod.fst δ

/-- Entered data reads as the outer accumulation at EVERY round (adjoint of `enter` is `Prod.fst`). -/
theorem acc_enter (input : T →₀ A) (t : T) (n : ℕ) :
    acc (enter input) (t, n) = acc input t := by
  rw [enter, Trace.acc_eq_finsupp_sum, Trace.acc_eq_finsupp_sum,
      Finsupp.sum_mapDomain_index (by simp) (fun _ m₁ m₂ => by split <;> simp)]
  refine Finsupp.sum_congr fun s _ => ?_
  have h : ((s, 0) ≤ (t, n)) ↔ (s ≤ t) := by simp [Prod.mk_le_mk]
  simp only [h]

/-- `feedback` lands only at rounds `≥ 1`: at round 0 it contributes nothing. -/
theorem acc_feedback_zero (y : (T × ℕ) →₀ A) (t : T) :
    acc (feedback y) (t, 0) = 0 := by
  rw [feedback, Trace.acc_eq_finsupp_sum,
      Finsupp.sum_mapDomain_index (by simp) (fun _ m₁ m₂ => by split <;> simp), Finsupp.sum]
  apply Finset.sum_eq_zero
  intro p _
  rcases p with ⟨p1, p2⟩
  simp [Prod.mk_le_mk]

/-- `feedback` at round `n+1` reads `y` at round `n`: it is the round-shift. -/
theorem acc_feedback_succ (y : (T × ℕ) →₀ A) (t : T) (n : ℕ) :
    acc (feedback y) (t, n + 1) = acc y (t, n) := by
  rw [feedback, Trace.acc_eq_finsupp_sum, Trace.acc_eq_finsupp_sum,
      Finsupp.sum_mapDomain_index (by simp) (fun _ m₁ m₂ => by split <;> simp)]
  refine Finsupp.sum_congr fun p _ => ?_
  rcases p with ⟨p1, p2⟩
  simp only [Prod.mk_le_mk, Nat.succ_le_succ_iff]

/-! ### `leave` structural lemmas — `feedback` is invisible to `leave` -/

theorem leave_add (a b : (T × ℕ) →₀ A) : leave (a + b) = leave a + leave b := by
  simp only [leave, Finsupp.mapDomain_add]

theorem leave_sub (a b : (T × ℕ) →₀ A) : leave (a - b) = leave a - leave b := by
  have h : leave ((a - b) + b) = leave (a - b) + leave b := leave_add (a - b) b
  rw [show (a - b) + b = a from by abel] at h
  rw [h]; abel

/-- `leave` forgets the round; `feedback` only shifts it, so `leave` cannot see `feedback`. -/
theorem leave_feedback (y : (T × ℕ) →₀ A) : leave (feedback y) = leave y := by
  unfold leave feedback
  rw [← Finsupp.mapDomain_comp]
  rfl

/-- `leave` undoes `enter` (both project the round coordinate away). -/
theorem leave_enter (input : T →₀ A) : leave (enter input) = input := by
  unfold leave enter
  rw [← Finsupp.mapDomain_comp]
  exact Finsupp.mapDomain_id

/-! ### The generalized loop: arbitrary seed `s` -/

/-- **The semantic recursion (abstract seed).**  For any seed trace `s`, the accumulated loop
    variable is the seed's round-0 accumulation, and at each further round adds the seed's fresh
    round to the body applied to the previous round.  `feedback` vanishes at round 0 and round-shifts
    otherwise; nothing here is specific to `enter`.  (`rw` only the left `x`; the right `body x` must
    survive.) -/
theorem iterate_unroll (s x : (T × ℕ) →₀ A)
    (hfix : x = s + feedback (body x)) :
    (∀ t, acc x (t, 0) = acc s (t, 0)) ∧
    (∀ t n, acc x (t, n + 1) = acc s (t, n + 1) + acc (body x) (t, n)) := by
  refine ⟨fun t => ?_, fun t n => ?_⟩
  · conv_lhs => rw [hfix]
    rw [Trace.acc_add, acc_feedback_zero, add_zero]
  · conv_lhs => rw [hfix]
    rw [Trace.acc_add, acc_feedback_succ]

/-- The from-scratch iteration on COLLECTIONS: round 0 is the seed's round-0 accumulation; each
    further round adds the seed's round-`n+1` accumulation to the body's round-wise action `f`.
    Defined with NO reference to `x` — this is the denotation the implementation must match. -/
noncomputable def iter (f : (T → A) → (T → A)) (s : (T × ℕ) →₀ A) : ℕ → (T → A)
  | 0 => fun t => acc s (t, 0)
  | n + 1 => (fun t => acc s (t, n + 1)) + f (iter f s n)

/-- **A1 — matches from-scratch.**  The implementation's round-`n` state IS the `n`-th from-scratch
    iterate, by induction, given a round-wise body (`hbody`: the body's round-`n` output is `f` of its
    round-`n` input).  This is "matches moment-by-moment re-evaluation" *by construction*. -/
theorem round_matches (f : (T → A) → (T → A)) (s x : (T × ℕ) →₀ A)
    (hfix : x = s + feedback (body x))
    (hbody : ∀ y n, (fun t => acc (body y) (t, n)) = f (fun t => acc y (t, n))) :
    ∀ n, (fun t => acc x (t, n)) = iter f s n := by
  intro n
  induction n with
  | zero =>
    funext t
    simp only [iter]
    exact (iterate_unroll body s x hfix).1 t
  | succ n ih =>
    funext t
    have hu := (iterate_unroll body s x hfix).2 t n
    have h1 : (fun t => acc (body x) (t, n)) = f (iter f s n) := by
      rw [hbody x n, ih]
    simp only [iter, Pi.add_apply]
    rw [hu, congrFun h1 t]

/-- **A2 — stabilization is free.**  `x` is finitely supported, so only finitely many rounds carry
    mass; `acc (leave x)` (the sum over all rounds) equals `acc x (·, K)` for every `K` past the last
    populated round.  No `FiniteRounds` hypothesis needed — it is a fact about any `Finsupp`. -/
theorem leave_stabilizes (x : (T × ℕ) →₀ A) :
    ∃ N, ∀ K, N ≤ K → ∀ t, acc (leave x) t = acc x (t, K) := by
  obtain ⟨N, hN⟩ : ∃ N, ∀ p ∈ x.support, p.2 ≤ N :=
    ⟨(x.support.image Prod.snd).sup id, fun p hp =>
      Finset.le_sup (f := id) (Finset.mem_image_of_mem Prod.snd hp)⟩
  refine ⟨N, fun K hK t => ?_⟩
  rw [leave, Trace.acc_eq_finsupp_sum,
      Finsupp.sum_mapDomain_index (by simp) (fun _ m₁ m₂ => by split <;> simp),
      Trace.acc_eq_finsupp_sum]
  refine Finsupp.sum_congr fun p hp => ?_
  have hp2 : p.2 ≤ K := le_trans (hN p hp) hK
  have hcond : (p ≤ (t, K)) ↔ (p.1 ≤ t) := by
    rw [Prod.le_def]; exact ⟨And.left, fun h => ⟨h, hp2⟩⟩
  simp only [hcond]

/-- **A2 (packaged).**  `leave` reads the converged iterate: `acc (leave x) = iter f s N` for `N`
    past the last populated round. -/
theorem leave_converged (f : (T → A) → (T → A)) (s x : (T × ℕ) →₀ A)
    (hfix : x = s + feedback (body x))
    (hbody : ∀ y n, (fun t => acc (body y) (t, n)) = f (fun t => acc y (t, n))) :
    ∃ N, (fun t => acc (leave x) t) = iter f s N := by
  obtain ⟨N, hN⟩ := leave_stabilizes x
  refine ⟨N, ?_⟩
  rw [← round_matches body f s x hfix hbody N]
  funext t
  exact hN N le_rfl t

/-- **L0 — the loop fixpoint at the trace level (unconditional).**  `leave x = leave s + leave (body
    x)`: apply `leave` to the loop equation; `feedback` drops out. -/
theorem leave_loop (s x : (T × ℕ) →₀ A) (hfix : x = s + feedback (body x)) :
    leave x = leave s + leave (body x) := by
  conv_lhs => rw [hfix]
  rw [leave_add, leave_feedback]

/-- **A3 — the accumulated fixpoint.**  `acc (leave x) = acc (leave s) + f (acc (leave x))`: the
    maintained output solves the loop's fixpoint equation, seed term `acc (leave s)` (the total
    accumulated seed). -/
theorem leave_fixpoint (f : (T → A) → (T → A)) (s x : (T × ℕ) →₀ A)
    (hfix : x = s + feedback (body x))
    (hbody : ∀ y n, (fun t => acc (body y) (t, n)) = f (fun t => acc y (t, n))) :
    (fun t => acc (leave x) t) = (fun t => acc (leave s) t) + f (fun t => acc (leave x) t) := by
  have e2 : (fun t => acc (leave (body x)) t) = f (fun t => acc (leave x) t) := by
    obtain ⟨N₁, h₁⟩ := leave_stabilizes x
    obtain ⟨N₂, h₂⟩ := leave_stabilizes (body x)
    funext t
    have harg : (fun t => acc x (t, max N₁ N₂)) = (fun t => acc (leave x) t) := by
      funext t'; exact (h₁ (max N₁ N₂) (le_max_left N₁ N₂) t').symm
    rw [h₂ (max N₁ N₂) (le_max_right N₁ N₂) t, congrFun (hbody x (max N₁ N₂)) t, harg]
  funext t
  have hL0 : acc (leave x) t = acc (leave s) t + acc (leave (body x)) t := by
    rw [leave_loop body s x hfix, Trace.acc_add]
  rw [hL0, congrFun e2 t]
  simp only [Pi.add_apply]

/-- **Re-add regime** (`enter`): `acc (leave x) = acc input + f (acc (leave x))` — a fixpoint of
    `z ↦ input + f z`.  This is `f x = 2 + x/2` (input `= 2`), converging to `4`. -/
theorem leave_fixpoint_enter (f : (T → A) → (T → A)) (input : T →₀ A) (x : (T × ℕ) →₀ A)
    (hfix : x = enter input + feedback (body x))
    (hbody : ∀ y n, (fun t => acc (body y) (t, n)) = f (fun t => acc y (t, n))) :
    (fun t => acc (leave x) t) = (fun t => acc input t) + f (fun t => acc (leave x) t) := by
  have h := leave_fixpoint body f (enter input) x hfix hbody
  rwa [show (fun t => acc (leave (enter input)) t) = (fun t => acc input t) from by
        rw [leave_enter]] at h

/-- **Start-from regime** (impulse seed `enter input − feedback (enter input)`, one DELAY at the
    seed): `acc (leave x) = f (acc (leave x))` — a fixpoint of `f` itself, the limit of pure iteration
    from `input`.  This is `f x = x/2` started at `2`, converging to `0`. -/
theorem leave_fixpoint_impulse (f : (T → A) → (T → A)) (input : T →₀ A) (x : (T × ℕ) →₀ A)
    (hfix : x = (enter input - feedback (enter input)) + feedback (body x))
    (hbody : ∀ y n, (fun t => acc (body y) (t, n)) = f (fun t => acc y (t, n))) :
    (fun t => acc (leave x) t) = f (fun t => acc (leave x) t) := by
  have h := leave_fixpoint body f (enter input - feedback (enter input)) x hfix hbody
  have hs0 : (fun t => acc (leave (enter input - feedback (enter input))) t) = (0 : T → A) := by
    have hz : leave (enter input - feedback (enter input)) = 0 := by
      rw [leave_sub, leave_feedback, sub_self]
    rw [hz]; funext t; exact Trace.acc_zero t
  rw [hs0, zero_add] at h
  exact h

/-! ### DELAY: causal bodies and uniqueness of the accumulated fixpoint

A body may DELAY on the iteration coordinate — a round-SHIFT `(t, n) ↦ (t, n + k)` (DD's `delay`)
reads round `n − k`, so its round-`n` output depends on a STRICTLY earlier round.  That breaks
round-wiseness but not causality: shifts move only forward (`k ≥ 0`), so the output at round `n` still
depends only on rounds `≤ n`.  `Causal` states exactly this, and it strictly generalizes round-wise
(round-wise reads exactly `n`; a shift reads `n − k`; both are `≤ n`).

(A JOIN against a fixed collection is NOT a delay in this accumulated sense: `acc (y ⋈ F) (·, n) =
β (acc y (·, n)) (acc F (·, n))` by `sup_le_iff`, so it reads round `n` — round-wise.  The forward
movement of individual joined updates to `max` times is invisible to the accumulation.  So JOIN lives
in the round-wise Phase A/B; the causal layer here is for the shift.)

For a causal body the loop no longer has a closed-form per-round `f`, but the accumulated output is
still uniquely determined: `feedback` advances the round by exactly one, so `acc x (·, n+1)` is fixed
by `acc x (·, ≤ n)`, and strong induction pins every round.  Two solution traces of the loop
equation — e.g. the incremental implementation and a from-scratch re-evaluation — therefore
ACCUMULATE TO THE SAME THING (`acc_unique`), and `leave` reads that common value (`leave_acc_unique`).
Uniqueness IS the adequacy statement: whatever order you evaluate in, you get the loop's value. -/

/-- A body is causal in the iteration coordinate: its round-`n` output depends only on rounds `≤ n`
    of the (accumulated) input. -/
def Causal (body : ((T × ℕ) →₀ A) → ((T × ℕ) →₀ A)) : Prop :=
  ∀ y z n, (∀ m, m ≤ n → (fun t => acc y (t, m)) = (fun t => acc z (t, m)))
         → (fun t => acc (body y) (t, n)) = (fun t => acc (body z) (t, n))

/-- **DELAY adequacy.**  For a causal body, any two solutions of the loop equation accumulate
    identically at every round — the loop's value is independent of the evaluation strategy. -/
theorem acc_unique (s x x' : (T × ℕ) →₀ A)
    (hx : x = s + feedback (body x)) (hx' : x' = s + feedback (body x'))
    (hc : Causal body) :
    ∀ n, (fun t => acc x (t, n)) = (fun t => acc x' (t, n)) := by
  intro n
  induction n using Nat.strong_induction_on with
  | _ n ih =>
    rcases n with _ | k
    · funext t
      rw [(iterate_unroll body s x hx).1 t, (iterate_unroll body s x' hx').1 t]
    · funext t
      have hagree : ∀ m, m ≤ k → (fun t => acc x (t, m)) = (fun t => acc x' (t, m)) :=
        fun m hm => ih m (Nat.lt_succ_of_le hm)
      rw [(iterate_unroll body s x hx).2 t k, (iterate_unroll body s x' hx').2 t k,
          congrFun (hc x x' k hagree) t]

/-- `leave` of the loop is well defined: for a causal body it reads the same accumulated value from
    any solution trace. -/
theorem leave_acc_unique (s x x' : (T × ℕ) →₀ A)
    (hx : x = s + feedback (body x)) (hx' : x' = s + feedback (body x'))
    (hc : Causal body) :
    (fun t => acc (leave x) t) = (fun t => acc (leave x') t) := by
  obtain ⟨N, hN⟩ := leave_stabilizes x
  obtain ⟨N', hN'⟩ := leave_stabilizes x'
  funext t
  rw [hN (max N N') (le_max_left N N') t, hN' (max N N') (le_max_right N N') t,
      congrFun (acc_unique body s x x' hx hx' hc (max N N')) t]

end Iterate

/-! ## The incremental refinement: where the non-triviality lives

The batch square `Adequate` is only a SPECIFICATION.  On its own it reads as function composition,
and for the linear operators it is nearly trivial (`φ` commutes with the accumulation sum).  What
makes differential dataflow a real thing is that `impl` is not run monolithically: input arrives as
a stream of batches, and the operator emits, per batch, the CHANGE in output — a discrete
derivative — reusing bounded state.  This layer states that, and it is exactly where the content is.

Two facets, worth separating:

* CORRECTNESS — the per-batch increments telescope back to the batch answer.  For every operator
  except `ITERATE` this is a trivial telescoping sum (`Refines` below is satisfiable by definition).
  `ITERATE` is the sole operator whose telescoping runs over a recursion and needs convergence
  (`FiniteRounds`) — which is why it is "the exception".

* LOCALITY / STATE-BOUNDING — the increment is supported near the batch (join-closure of the batch
  times against stored times: interesting times) and computable from a compacted summary rather than
  full history.  THIS is the analytic content, and it is REDUCE-hard; `Model.lean`'s
  `stream_correct` is precisely this facet for `REDUCE`, done with the compaction adversary.  For
  `LINEAR`/`SUM`/`CONCAT` it is trivial because the increment is state-free (`Refines.of_additive`).

The moment-by-moment claim in full is `Adequate D impl ∧ Refines impl step`: the incremental
execution, accumulated, is the pointwise denotation.  Layer 1 (batch) buys MODULARITY for Layer 2:
because increments compose by a chain rule, global incremental correctness assembles from
per-operator pieces instead of being reproven monolithically. -/

/-- `step` is the exact per-batch increment of `impl`: from prior input `δ` (the state) and a new
    batch `b`, emit the output delta.  This is the CORRECTNESS facet only; it says nothing about
    `step` being local or state-bounded. -/
def Refines (impl : (T →₀ A) → (T' →₀ B))
    (step : (T →₀ A) → (T →₀ A) → (T' →₀ B)) : Prop :=
  ∀ δ b, step δ b = impl (δ + b) - impl δ

/-- **The chain rule — incremental compositionality.**  To run `G ∘ F` incrementally, feed `F`'s
    output DELTA into `G`'s step at `G`-state `implF δ`.  The intermediate wire carries difference
    traces: this is *why* difference traces are the composable interface between operators. -/
theorem Refines.comp {implF : (T →₀ A) → (T' →₀ B)}
    {stepF : (T →₀ A) → (T →₀ A) → (T' →₀ B)}
    {implG : (T' →₀ B) → (T'' →₀ C)}
    {stepG : (T' →₀ B) → (T' →₀ B) → (T'' →₀ C)}
    (hF : Refines implF stepF) (hG : Refines implG stepG) :
    Refines (implG ∘ implF) (fun δ b => stepG (implF δ) (stepF δ b)) := by
  intro δ b
  show stepG (implF δ) (stepF δ b) = implG (implF (δ + b)) - implG (implF δ)
  rw [hG (implF δ) (stepF δ b), hF δ b]
  congr 1
  congr 1
  abel

/-- Every ADDITIVE `impl` (LINEAR/SUM/CONCAT/negate/`JOIN`-in-one-argument) is refined by a
    STATE-FREE step — the increment is just `impl b`, independent of accumulated state.  This is the
    formal reason the linear operators are incrementally trivial, and the reason all the analytic
    weight falls on the nonlinear `REDUCE` and the `ITERATE` fixpoint. -/
theorem Refines.of_additive (impl : (T →₀ A) →+ (T' →₀ B)) :
    Refines (fun δ => impl δ) (fun _ b => impl b) := by
  intro δ b
  show impl b = impl (δ + b) - impl δ
  rw [map_add]
  abel

/-- Adequacy is closed under BILINEAR combination of two operators — the `join` primitive.  It is
    `Adequate.add` with the bilinear `β` in place of `+`, and follows from `join_adequate`.  Mono-typed
    (`β : A →+ A →+ A`), matching `Program`'s single-group simplification; heterogeneous
    key/value-changing joins are the deferred typed-`Program X Y` refactor. -/
theorem Adequate.join (β : A →+ A →+ A) {Dp Dq : (T → A) → (T → A)}
    {ip iq : (T →₀ A) → (T →₀ A)} (hp : Adequate Dp ip) (hq : Adequate Dq iq) :
    Adequate (fun g t => β (Dp g t) (Dq g t)) (fun δ => joinImpl β (ip δ) (iq δ)) := by
  intro δ
  funext t
  show acc (joinImpl β (ip δ) (iq δ)) t = β (Dp (acc δ) t) (Dq (acc δ) t)
  have hj : acc (joinImpl β (ip δ) (iq δ)) t = β (acc (ip δ) t) (acc (iq δ) t) :=
    congrFun (join_adequate β (ip δ) (iq δ)) t
  rw [hj, congrFun (hp δ) t, congrFun (hq δ) t]

/-! ## Capstone: a program is adequate because its pieces are

A deep-embedded program over these generators, interpreted two ways — `impl` on traces and `denote`
on accumulations — agrees by induction, each case citing the operator lemma, composition citing
`Adequate.comp`.  Shown monomorphically (single group `A`, time untouched) for legibility — in
particular `join`'s `β : A →+ A →+ A` is the self-join/product on one key space; the production
version is heterogeneously typed. -/

inductive Program (A : Type*) [AddCommGroup A] : Type _
  | id      : Program A
  | linear  : (A →+ A) → Program A
  | reduce  : (φ : A → A) → φ 0 = 0 → Program A
  | par     : Program A → Program A → Program A                   -- run both, ADD outputs
  | join    : (A →+ A →+ A) → Program A → Program A → Program A   -- run both, JOIN outputs (bilinear)
  | seq     : Program A → Program A → Program A

/-- Denotation: pointwise action on accumulated data. -/
noncomputable def Program.denote : Program A → (T → A) → (T → A)
  | .id, g => g
  | .linear φ, g => fun t => φ (g t)
  | .reduce φ _, g => fun t => φ (g t)
  | .par p q, g => Program.denote p g + Program.denote q g
  | .join β p q, g => fun t => β (Program.denote p g t) (Program.denote q g t)
  | .seq p q, g => Program.denote q (Program.denote p g)

/-- Implementation: action on update traces. -/
noncomputable def Program.impl : Program A → (T →₀ A) → (T →₀ A)
  | .id, δ => δ
  | .linear φ, δ => linImpl φ δ
  | .reduce φ hφ, δ => reduceImpl φ hφ δ
  | .par p q, δ => Program.impl p δ + Program.impl q δ
  | .join β p q, δ => joinImpl β (Program.impl p δ) (Program.impl q δ)
  | .seq p q, δ => Program.impl q (Program.impl p δ)

/-- **End-to-end adequacy.**  Every program's square commutes: running the implementation and
    accumulating equals accumulating and re-evaluating the program pointwise — moment by moment. -/
theorem Program.adequate (p : Program A) :
    Adequate (T := T) (Program.denote (T := T) p) (Program.impl (T := T) p) := by
  induction p with
  | id => exact Adequate.id
  | linear φ => exact linImpl_adequate φ
  | reduce φ hφ => exact reduce_adequate φ hφ
  | par p q ihp ihq => exact ihp.add ihq
  | join β p q ihp ihq => exact Adequate.join β ihp ihq
  | seq p q ihp ihq => exact ihp.comp ihq

/-! ### #1/#2 for whole programs: bounded work composes end-to-end

`Program.adequateCov` lifts the batch square to `AdequateCov` over `id / linear / reduce / par / join /
seq`, by the same induction as `Program.adequate` with the coverage-carrying lemmas.  JOIN needs its
own support tightening (`joinImpl_support_cl`): the convolution's edits are joins of a `δ`-edit with a
`δ'`-edit, so they land in the join-closure of the two edit sets. -/

/-- The JOIN tightening: the convolution's output edits lie in the join-closure of the two inputs'
    edit sets — each output time is `a ⊔ b` for edits `a`, `b` of the two inputs. -/
theorem joinImpl_support_cl (β : A →+ A' →+ B) (δ : T →₀ A) (δ' : T →₀ A') :
    (↑(joinImpl β δ δ').support : Set T) ⊆ Coverage.cl (δ.support ∪ δ'.support) := by
  have hsupp : (joinImpl β δ δ').support ⊆
      δ.support.biUnion (fun a => δ'.support.biUnion (fun b => {a ⊔ b})) := by
    refine Finsupp.support_sum.trans (Finset.biUnion_mono fun a _ => ?_)
    exact Finsupp.support_sum.trans (Finset.biUnion_mono fun b _ => Finsupp.support_single_subset)
  intro x hx
  rw [Finset.mem_coe] at hx
  have hxF := hsupp hx
  rw [Finset.mem_biUnion] at hxF
  obtain ⟨a, ha, hxF⟩ := hxF
  rw [Finset.mem_biUnion] at hxF
  obtain ⟨b, hb, hxF⟩ := hxF
  rw [Finset.mem_singleton] at hxF
  subst hxF
  have ha' : a ∈ Coverage.cl (δ.support ∪ δ'.support) :=
    subset_supClosure (Finset.mem_coe.mpr (Finset.mem_union_left _ ha))
  have hb' : b ∈ Coverage.cl (δ.support ∪ δ'.support) :=
    subset_supClosure (Finset.mem_coe.mpr (Finset.mem_union_right _ hb))
  exact supClosed_supClosure ha' hb'

/-- `AdequateCov` is closed under BILINEAR combination (the `join` primitive): the acc-equation from
    `join_adequate`, the bound from `joinImpl_support_cl` narrowed to `cl δ.support` (both branches'
    edits already sit there, and closure-of-closure is closure). -/
theorem AdequateCov.join (β : A →+ A →+ A) {Dp Dq : (T → A) → (T → A)}
    {ip iq : (T →₀ A) → (T →₀ A)} (hp : AdequateCov Dp ip) (hq : AdequateCov Dq iq) :
    AdequateCov (fun g t => β (Dp g t) (Dq g t)) (fun δ => joinImpl β (ip δ) (iq δ)) := by
  intro δ
  refine ⟨?_, ?_⟩
  · funext t
    show acc (joinImpl β (ip δ) (iq δ)) t = β (Dp (acc δ) t) (Dq (acc δ) t)
    have hj : acc (joinImpl β (ip δ) (iq δ)) t = β (acc (ip δ) t) (acc (iq δ) t) :=
      congrFun (join_adequate β (ip δ) (iq δ)) t
    rw [hj, congrFun (hp δ).1 t, congrFun (hq δ).1 t]
  · refine (joinImpl_support_cl β (ip δ) (iq δ)).trans ?_
    refine supClosure_min ?_ supClosed_supClosure
    rw [Finset.coe_union]
    exact Set.union_subset (hp δ).2 (hq δ).2

/-- **End-to-end bounded work.**  Every program is `AdequateCov`: it maintains the right answer AND
    its edits stay in the join-closure of the input's edits — by induction, each case citing the
    coverage-carrying operator lemma, composition citing `AdequateCov.comp`. -/
theorem Program.adequateCov (p : Program A) :
    AdequateCov (T := T) (Program.denote (T := T) p) (Program.impl (T := T) p) := by
  induction p with
  | id => exact AdequateCov.id
  | linear φ => exact linImpl_adequateCov φ
  | reduce φ hφ => exact reduce_adequateCov φ hφ
  | par p q ihp ihq => exact ihp.add ihq
  | join β p q ihp ihq => exact AdequateCov.join β ihp ihq
  | seq p q ihp ihq => exact ihp.comp ihq

/-! ## Phase B: lifting a `Program` body into the loop

The `ITERATE` theorems of the `Iterate` section take the round-wise action `f` and its property
`hbody` abstractly.  Here we discharge them for a body that is a `Program` lifted into the nested
scope — i.e. the SAME program interpreted at time `T × ℕ` (`Program` is parameterized only by the
group, so `Program.impl (T := T × ℕ) p` is the lift).  Its round-wise action turns out to be the
program's ordinary outer denotation `Program.denote (T := T) p`. -/

/-- `Program.denote` is pointwise in time, so it commutes with slicing off a round: evaluating the
    `T × ℕ` denotation at `(t, n)` equals the `T` denotation of the round-`n` slice at `t`. -/
theorem denote_slice (p : Program A) (n : ℕ) :
    ∀ (g : T × ℕ → A) (t : T),
      Program.denote (T := T × ℕ) p g (t, n)
        = Program.denote (T := T) p (fun t => g (t, n)) t := by
  induction p with
  | id => intro g t; rfl
  | linear φ => intro g t; rfl
  | reduce φ hφ => intro g t; rfl
  | par p q ihp ihq =>
    intro g t
    show Program.denote (T := T × ℕ) p g (t, n) + Program.denote (T := T × ℕ) q g (t, n)
       = (Program.denote (T := T) p (fun t => g (t, n))
            + Program.denote (T := T) q (fun t => g (t, n))) t
    rw [ihp g t, ihq g t]
    simp only [Pi.add_apply]
  | join β p q ihp ihq =>
    intro g t
    show β (Program.denote (T := T × ℕ) p g (t, n)) (Program.denote (T := T × ℕ) q g (t, n))
       = β (Program.denote (T := T) p (fun t => g (t, n)) t)
           (Program.denote (T := T) q (fun t => g (t, n)) t)
    rw [ihp g t, ihq g t]
  | seq p q ihp ihq =>
    intro g t
    show Program.denote (T := T × ℕ) q (Program.denote (T := T × ℕ) p g) (t, n)
       = Program.denote (T := T) q (Program.denote (T := T) p (fun t => g (t, n))) t
    rw [ihq (Program.denote (T := T × ℕ) p g) t]
    congr 1
    funext t'
    exact ihp g t'

/-- **The round-wise action of a lifted `Program` body is its outer denotation.**  Discharges the
    `hbody` hypothesis of the `Iterate` theorems with `f = Program.denote p`. -/
theorem round_wise (p : Program A) (y : (T × ℕ) →₀ A) (n : ℕ) :
    (fun t => acc (Program.impl (T := T × ℕ) p y) (t, n))
      = Program.denote (T := T) p (fun t => acc y (t, n)) := by
  funext t
  rw [congrFun (Program.adequate (T := T × ℕ) p y) (t, n), denote_slice p n (acc y) t]

/-- **ITERATE of a `Program` body.**  A loop whose body is a lifted program `p` converges, and its
    accumulated output is the from-scratch iteration of `p`'s denotation seeded by `s`.  This ties
    `ITERATE` to the rest of the calculus. -/
theorem iterate_program (p : Program A) (s x : (T × ℕ) →₀ A)
    (hfix : x = s + feedback (Program.impl (T := T × ℕ) p x)) :
    ∃ N, (fun t => acc (leave x) t) = iter (Program.denote (T := T) p) s N :=
  leave_converged (Program.impl (T := T × ℕ) p) (Program.denote (T := T) p) s x hfix
    (fun y n => round_wise p y n)

/-- **Mutual recursion needs no product decomposition.**  `acc` commuting with a product projection
    is just `linImpl_adequate` for the projection hom (`AddMonoidHom.fst`): projecting a
    product-valued trace then accumulating equals accumulating then projecting.  With `linear`
    (projections/injections/block homs on `A₁ × A₂`) and `par` (summing branch contributions), a
    mutual system `A = f(A,B), B = g(A,B)` is a single `Program (A₁ × A₂)`, and `iterate_program`
    applies verbatim — faithful because DD iterates synchronously (Jacobi). -/
theorem acc_prod_fst {A₁ A₂ : Type*} [AddCommGroup A₁] [AddCommGroup A₂]
    (d : T →₀ (A₁ × A₂)) (t : T) :
    acc (linImpl (AddMonoidHom.fst A₁ A₂) d) t = (acc d t).1 :=
  congrFun (linImpl_adequate (AddMonoidHom.fst A₁ A₂) d) t

/-! ## #1'/#2' (first cut): the state axis — compaction sufficiency

The work axis (`AdequateCov`) bounds WHERE edits land; this bounds HOW MUCH history the operator must
keep.  The "additional information" is the frontier `F` — a finite set generating the upper set
`Compaction.Beyond F` of still-live query times.  Compacting a trace advances every update time to its
frontier representative (`Compaction.advance`, the join-with-frontier-meet); the temporal keystone
`acc_compact` says this is INVISIBLE beyond the frontier — the accumulation at every live `t` is
unchanged.  Hence any operator whose denotation is pointwise in time (LINEAR, REDUCE, and their
composites) can be fed the COMPACTED input and produce the same output beyond the frontier: the
pre-frontier history need not be retained as state (`adequate_compact_sufficient`).

This is the *base case* of the state axis, and it composes for the pointwise class
(`adequate_compact_sufficient_comp`: pointwise is closed under `∘`).  Deferred (the genuinely harder
part, see `DESIGN.md`): the frontier-STRENGTH gradient — a normal operator tolerates any old frontier,
a DELTA JOIN only compaction up to just behind the current frontier — and the composition of *state
demand* across a pipeline (#2' proper), where a downstream delta join forces finer compaction upstream. -/

section StateAxis
variable {T : Type*} [Lattice T]
variable {A : Type*} [AddCommGroup A]

/-- Compact a trace to a frontier `F`: advance every update time to its frontier representative
    `Compaction.advance · F hF`, consolidating collisions (`Finsupp.mapDomain`). -/
noncomputable def compact (F : Finset T) (hF : F.Nonempty) (d : T →₀ A) : T →₀ A :=
  Finsupp.mapDomain (fun x => Compaction.advance x F hF) d

/-- **Temporal keystone — compaction is invisible beyond the frontier.**  At every live query time
    `t ∈ Beyond F`, the compacted trace accumulates exactly as the original.  An instance of
    `Trace.acc_mapDomain`: `advance` preserves comparison-to-`t` for every `t` beyond the frontier
    (`Compaction.advance_le_iff`). -/
theorem acc_compact (F : Finset T) (hF : F.Nonempty) (d : T →₀ A) {t : T}
    (ht : t ∈ Compaction.Beyond F) :
    acc (compact F hF d) t = acc d t := by
  unfold compact
  apply Trace.acc_mapDomain
  intro x _
  exact Compaction.advance_le_iff hF ht

/-- **Compaction sufficiency for a pointwise operator (state-axis base case).**  If `impl` is adequate
    with a pointwise-in-time denotation (output at `t` depends on the input accumulation only at `t` —
    LINEAR, REDUCE), then beyond the frontier, running it on the COMPACTED input gives the same output:
    the operator never needs pre-frontier history as state. -/
theorem adequate_compact_sufficient {φ : A → A} {impl : (T →₀ A) → (T →₀ A)}
    (had : Adequate (fun (g : T → A) (t : T) => φ (g t)) impl)
    (F : Finset T) (hF : F.Nonempty) (δ : T →₀ A) {t : T} (ht : t ∈ Compaction.Beyond F) :
    acc (impl (compact F hF δ)) t = acc (impl δ) t := by
  have h1 : acc (impl (compact F hF δ)) t = φ (acc (compact F hF δ) t) := congrFun (had _) t
  have h2 : acc (impl δ) t = φ (acc δ t) := congrFun (had _) t
  rw [h1, h2, acc_compact F hF δ ht]

/-- **#2' base case — the pointwise class is closed under composition,** so compaction sufficiency
    composes: a two-stage pipeline of pointwise operators tolerates input compaction beyond the
    frontier.  (`ψ ∘ φ` is again pointwise, so `adequate_compact_sufficient` applies to the composite.) -/
theorem adequate_compact_sufficient_comp {φ ψ : A → A} {i₁ i₂ : (T →₀ A) → (T →₀ A)}
    (h₁ : Adequate (fun (g : T → A) (t : T) => φ (g t)) i₁)
    (h₂ : Adequate (fun (g : T → A) (t : T) => ψ (g t)) i₂)
    (F : Finset T) (hF : F.Nonempty) (δ : T →₀ A) {t : T} (ht : t ∈ Compaction.Beyond F) :
    acc (i₂ (i₁ (compact F hF δ))) t = acc (i₂ (i₁ δ)) t := by
  have h1 : acc (i₂ (i₁ (compact F hF δ))) t = ψ (acc (i₁ (compact F hF δ)) t) := congrFun (h₂ _) t
  have h2 : acc (i₂ (i₁ δ)) t = ψ (acc (i₁ δ) t) := congrFun (h₂ _) t
  rw [h1, h2]
  congr 1
  exact adequate_compact_sufficient h₁ F hF δ ht

end StateAxis

end Compositional
