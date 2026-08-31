# Scaling the physics: profiling, attribution, prioritized iteration

Running notes. Each entry: what was measured, on what, against which ground
truth, and what it cost. Variants live beside the programs they vary; nothing
here replaces `water.ddp` or `pathways.ddp` silently.

Method: `bench_water.py` stages one program on one board, times the initial
fill and a dam edit, samples the server's peak RSS, and asserts the
equilibrium equals `run_dem.priority_flood` exactly in both phases. Iteration
activity comes from `| inspect(label)` on the loop variable and its proposals:
each record prints with its `(epoch, PointStamp)` time and diff, so record
volume, retraction share, round count, and per-round width are all countable.
Instrumented runs are ~8x slower and are used only for counts.

## Iteration 1 (2026-08-31): prioritized water

### The finding

Releasing the boundary seeds in increasing height order — and dropping the
global ceiling seed — makes the initial fill **5–7x faster and 2.3–4.1x
smaller**, with edits 3–11x faster, on every board, exact against the
priority flood.

| board | cells | fill: base → seed | dam: base → seed | peak RSS: base → seed |
|---|---:|---|---|---|
| engadin_128 | 16,384 | 2.34s → **0.46s** (5.1x) | 0.48s → **0.06s** | 322 → **141 MB** |
| engadin_wide | 49,152 | 12.39s → **2.50s** (5.0x) | 0.23s → **0.07s** | 1,224 → **486 MB** |
| engadin_256 | 65,536 | 27.89s → **3.77s** (7.4x) | 1.95s → **0.18s** | 2,994 → **723 MB** |

The gap widens with size: over 4x the cells the baseline costs 11.9x the time
and 9.3x the memory, the prioritized variant 8.2x and 5.1x.

### Why: attribution on engadin_128 (fill + dam, instrumented)

| variant | proposals | retractions | rounds | busiest round |
|---|---:|---:|---:|---:|
| `water.ddp` (baseline) | 370,220 | 176,918 (47.8%) | 210 | 16,384 |
| `water_pri.ddp` (per-step delay) | 474,650 | 229,106 (48.3%) | 2,163 | 37,250 |
| `water_seed.ddp` (seed order) | **43,966** | **13,791 (31.4%)** | 432 | **396** |

Nearly half the baseline's work is churn: every cell starts at the global
ceiling and ratchets down through wrong intermediate levels, and round 0
touches all 16,384 cells at once. That synchronized descent is what the peak
RSS is made of. Seed ordering replaces it with a narrow wavefront — 396 cells
in the busiest round, 8.4x fewer proposals, 12.8x fewer retractions.

### What did not work, and why it matters

`water_pri.ddp` delays each proposal by its *rise*, `enter_at(level - pass)`,
intending the invariant "a cell is derived at round (level − lowest terrain)",
i.e. priority flood with the queue expressed as time. It is exact but slower
than the baseline (fill 2.89s, dam 7.83s, 16x worse than baseline).

The reason is structural, and worth recording: `enter_at` is a **relative**
delay, so along a path the delays *add*, while this recursion's level is a
**max** along the path (a bottleneck/minimax problem, not an additive one).
After the first hop the intended invariant is gone, cells are reached by
cheaper-in-delay paths later, and pairs of adjacent cells oscillate: cell
(56,95) alone logged 937 updates, flipping between 1997 and 2083 on
consecutive rounds for hundreds of rounds. Only 1 cell in 16,384 was derived
at the round its level predicts.

Seeds are the exception that works, because a seed's delay is measured from
time 0 — nothing accumulates — so `enter_at(height − lowest)` really does
release the lowest drain first.

**Does the core need changing?** For this recursion, an *absolute* `enter_at`
(enter at time T, rather than delay by k) would express true priority-flood
order, which relative delay cannot. That is the one core change these
measurements argue for, and it is not urgent: seed ordering already captures
most of the win without touching the server. Additive recursions — the
pathways route scope, where accumulated delay *is* accumulated cost — should
be able to use the relative form as it stands. That is the next experiment.

### The rules

`water_seed.ddp` is not more complex than `water.ddp`: it deletes the ceiling
seed (`mx`, `cellsu`, `top` — 4 lines) and adds a 6-line seed release. Both
ablations are separable and both were measured: dropping the ceiling alone
(`water_nt.ddp`) is worth only 2.34s → 2.11s; essentially all of the win is
the ordering.

    python3 bench_water.py water_seed.ddp engadin_256.txt --dam-x 148

## Iteration 1b: does "high first" check out? (fmcsherry's hypothesis)

Proposal: delay each proposal by its depth below the maximum height, so high
readings — which influence lower ones — are resolved first.

Measured, engadin_128, both exact against the priority flood:

| seed order | fill | peak RSS | proposals | retractions | busiest round |
|---|---|---|---:|---:|---:|
| ascending (`water_seed.ddp`) | **0.46s** | **147 MB** | 41,038 | 30.0% | 396 |
| baseline ceiling descent (`water.ddp`) | 2.34s | 322 MB | 370,220 | 47.8% | 16,384 |
| descending seeds (`water_hi.ddp`) | 4.65s | 539 MB | 582,538 | 48.6% | 3,582 |
| descending seeds + per-proposal depth (`water_hi_prop.ddp`) | 5.22s | 582 MB | — | — | — |

It does not check out, and the measurement says why: **the baseline already is
the high-first schedule.** Settle iteration by final-level quintile, initial
fill:

| quintile | levels | baseline median settle | baseline updates/cell | ascending updates/cell |
|---|---|---:|---:|---:|
| Q5 (highest) | 2526–3171 | iter 18 | 12.1 | 3.1 |
| Q3 | 1866–2203 | iter 24 | 12.8 | 1.4 |
| Q1 (lowest) | 1694–1755 | iter 56 | **43.3** | **2.3** |

Descending from a ceiling settles the peaks almost immediately and leaves the
valley floor to thrash through 43 revisions per cell. Ordering the *other*
way takes that to 2.3.

The reason is where the information comes from. A cell's level is the smallest,
over escape routes to the boundary, of the highest terrain on the route, so
influence flows **inward from the drain**, not downward from the peaks, and
levels are non-decreasing along an escape route. High terrain does set a lake's
level — but only through the pass on its cheapest route, and which pass that is
is unknown until the low wavefront arrives. Releasing high seeds first injects
upper bounds that are all retracted later: 14x the proposals, 48.6% of them
churn.

There is also a structural obstacle. A proposal's value is `max(t, L_n) >= L_n`
— never lower than the record it came from. Under a descending clock
(time = peak − level) a proposal would have to arrive *before* its own source:
a negative delay, which `enter_at` cannot express. Ascending order is the only
direction relative delays can even represent here.
