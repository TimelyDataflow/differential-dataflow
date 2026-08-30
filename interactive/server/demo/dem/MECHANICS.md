# Mechanics lab — evening notes

Running log for new interacting mechanics on the Engadine world (branch
`mechanics`). Each iteration: implement + ground-truth a mechanism,
exercise it with live agents in asymmetric roles, then judge the design
space: too constrained and obvious? too flat and ambiguous? did
non-trivial interaction emerge?

## Mechanism 1: roads, bridges, and the access constraint (works.ddp)

Acts (attributed, one per cell, audited):
- **road** (cost 10): usable only while DRY (water == terrain there).
- **bridge** (cost 50): usable regardless of water.

Two adjacent usable road cells are CONNECTED iff their terrain heights
differ by ≤ 12 (the grade limit) — steep ground must be terraformed into a
road-bed before a road can climb it. The NETWORK is reachability from the
depot over those edges, live: flooding a non-bridge road severs everything
beyond it; draining restores it. ACCESS = network dilated by 3 cells.

**The constraint that makes it load-bearing: every terraform act must land
inside ACCESS** (end-state audit in v1 — the final network must serve every
dig; a mid-game act far from any road that later gets a road to it is
tolerated, temporary-works style).

Asymmetry (role table in the briefing, audited from the attributed acts):
- ROADWRIGHT: may build roads/bridges only.
- TERRAFORMER: may terraform only.

Intended tensions:
- The terraformer can do nothing until the roadwright reaches the work
  zone; the roadwright cannot climb grades the terraformer hasn't cut.
  Neither role is sufficient; pooling is impossible by construction.
- Routing through the flooded zone costs 5x (bridges) — or wait for the
  terraformer to drain it, which needs access first: a chicken-and-egg the
  pair must schedule around.
- Water edits can wash out roads (a dam that floods your own supply road).

Verification: python mirrors of usable/edges/network/access asserted equal
to the server's views; role, budget, one-act-per-cell, and access audits
at judgment.

### Trial 1: FAILURE (village wet; roadwright 390/400 spent, terraformer 0/650)

What happened: the terraformer (opus) surveyed, proved a sharp theorem —
under the briefed "cells now at 1775 are locked" rule the basin is sealed
(the lake sits AT the crest), so drainage is impossible — and pivoted to a
legal exploit: RAISE the four village cells to 1776, making islands
("dry" = water == terrain is satisfied by entombing the village under 65 m
of fill). It posted a compact plan (one access anchor at (89,47), ~266
spend), offered its spare budget for road-bed cuts, then sat fully blocked
behind the access gate while the roadwright (sonnet) spent 47 minutes and
390 coins discovering the terrain: a 13 m underwater step at (83,44) broke
its bridge chain — 200 coins of disconnected bridges — because grade
applies to TERRAIN even under bridges. Coordination via the site office
was real but temporally mismatched; the fast partner starved behind the
slow one.

### Verdict: the right kind of tension, three specific flaws

NOT too flat — every constraint bit hard, and both agents produced
genuine engineering reasoning (the sealed-basin proof, the exact 16-coin
reconnection plan). NOT too constrained — two qualitatively different
strategies (drain vs raise) were live until the lock rule killed one. The
flaws are specific and fixable:

1. **Bridges should span grade.** Terrain-grade between two bridge cells
   is physically meaningless (the deck is level); it cost 200 coins and
   was rightly flagged as "felt broken". Fix: a graded edge is required
   only between two plain roads; edges touching a bridge connect freely.
2. **The dry-predicate permits entombment.** "water == terrain" is
   satisfiable by burying the village. The goal needs physics: village
   cells must be dry AND within a small tolerance of their original
   heights (protect, don't inter).
3. **Total dependency starves the fast partner.** The terraformer COULD
   have usefully cut road-bed at the network frontier the whole time (the
   access halo covers it, and it even offered) — the failure was pace and
   route churn, not the mechanism. Keep the asymmetry; sharpen the
   briefing (explicit locked-cell list by file, an early-handshake
   requirement, same deadline discipline for both) and re-trial before
   adding the next mechanism.

Also: the setup's Dijkstra feasibility (340) mispriced reality — it
ignores grade for road-road steps and pointed at the spillway work zone,
not the raise-at-village plan the rules actually allow. Feasibility
metrics must price the same game the agents play.
