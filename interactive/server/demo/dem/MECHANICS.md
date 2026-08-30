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

### Trial 1: (pending)

### Verdict: (pending)
