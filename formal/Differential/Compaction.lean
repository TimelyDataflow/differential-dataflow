import Mathlib

/-!
# Soundness and optimality of frontier compaction

Differential dataflow compacts times by `Lattice::advance_by`: a time `t` and a frontier `F` (a
nonempty finite set of times) produce the representative

  `advance t F = ⨅_{f ∈ F} (t ⊔ f)` - the meet over the frontier of the joins.

We prove the two properties that make this a correct compaction.

* `advance_le_iff` - **soundness (the meet lemma).**  Beyond the frontier, the representative
  compares to a time exactly as the original does: for `s` at or beyond `F`,
  `advance t F ≤ s ↔ t ≤ s`.  So compaction preserves every accumulation at times beyond the
  frontier.
* `advance_eq_iff` - **optimality (canonicity).**  Two times receive the same representative *iff*
  they compare identically to every time beyond the frontier.  So `advance` is a canonical form for
  the "indistinguishable beyond `F`" equivalence: it collapses exactly the times that must be
  collapsed, and no more.

"Beyond the frontier" is the upper set `Beyond F`, defined as Mathlib's upper closure
`upperClosure` of the frontier: `s ∈ Beyond F` exactly when some `f ∈ F` has `f ≤ s`.
The frontier's being an antichain plays no role; only that it is nonempty and finite.

## Scope

These are pointwise facts about one time against one frontier, and nothing more.
Nothing here concerns the consolidation that merges and cancels co-located updates, which
frontiers an operator may compact by and when, or the correctness of a computation that reads
compacted state across many rounds.  Those questions remain open; an account of them would
use this file's two lemmas only to establish that compaction preserves everything observable
at or beyond the frontier.
-/

namespace Compaction

open Finset

variable {T : Type*} [Lattice T] {F : Finset T}

/-- The upper set of times at or beyond the frontier `F` - the upper closure (`upperClosure`) of
    `F`.  Membership unfolds via `mem_Beyond`. -/
def Beyond (F : Finset T) : Set T := ↑(upperClosure (F : Set T))

@[simp] theorem mem_Beyond {F : Finset T} {s : T} : s ∈ Beyond F ↔ ∃ f ∈ F, f ≤ s := by
  simp [Beyond]

/-- `advance_by`: the frontier-relative representative `⨅_{f ∈ F} (t ⊔ f)`. -/
def advance (t : T) (F : Finset T) (hF : F.Nonempty) : T := F.inf' hF (fun f => t ⊔ f)

/-- The representative dominates the original: `t ≤ advance t F`. -/
theorem le_advance (hF : F.Nonempty) (t : T) : t ≤ advance t F hF := by
  change t ≤ F.inf' hF (fun f => t ⊔ f)
  apply Finset.le_inf'
  intro f _
  exact le_sup_left

/-- **Soundness**  The representative and `t` compare identically to `s` beyond the frontier. -/
theorem advance_le_iff (hF : F.Nonempty) {t s : T} (hs : s ∈ Beyond F) :
    advance t F hF ≤ s ↔ t ≤ s := by
  constructor
  · exact fun h => le_trans (le_advance hF t) h
  · intro h
    obtain ⟨f0, hf0F, hf0s⟩ := mem_Beyond.1 hs
    exact le_trans (Finset.inf'_le (fun f => t ⊔ f) hf0F) (sup_le h hf0s)

/-- One direction of canonicity, phrased as monotonicity in the comparison behaviour. -/
theorem advance_le_advance (hF : F.Nonempty) {t1 t2 : T}
    (h : ∀ s ∈ Beyond F, t2 ≤ s → t1 ≤ s) :
    advance t1 F hF ≤ advance t2 F hF := by
  change advance t1 F hF ≤ F.inf' hF (fun f => t2 ⊔ f)
  apply Finset.le_inf'
  intro f hf
  show advance t1 F hF ≤ t2 ⊔ f
  have hb : (t2 ⊔ f) ∈ Beyond F := mem_Beyond.2 ⟨f, hf, le_sup_right⟩
  rw [advance_le_iff hF hb]
  exact h (t2 ⊔ f) hb le_sup_left

/-- **Canonicity**  Two times get the same representative iff they compare identically
    to every time beyond the frontier - `advance` is a canonical form for that equivalence. -/
theorem advance_eq_iff (hF : F.Nonempty) {t1 t2 : T} :
    advance t1 F hF = advance t2 F hF ↔ ∀ s ∈ Beyond F, (t1 ≤ s ↔ t2 ≤ s) := by
  constructor
  · intro heq s hs
    rw [← advance_le_iff (t := t1) hF hs, ← advance_le_iff (t := t2) hF hs, heq]
  · intro h
    exact le_antisymm
      (advance_le_advance hF (fun s hs => (h s hs).2))
      (advance_le_advance hF (fun s hs => (h s hs).1))

end Compaction
