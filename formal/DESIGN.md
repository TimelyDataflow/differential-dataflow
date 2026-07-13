# Design note: coverage as the general theorem

The organizing idea behind the `formal/` reorganization, and the work still open. As each
piece lands it is described in `README.md` (what the files *are*); this note keeps only the
thesis and the open list. When the open list empties, this file goes away.

## The thesis

`Coverage.lean`'s keystone, `exists_diff_trace_comp`, is quantified over an *arbitrary*
function of the accumulated input (the only hypothesis is `f 0 = 0`): the output is again a
difference trace, with edits confined to the join-closure of the input's edits. That is a
statement about differential computation as such. REDUCE is just the instance at full
strength; LINEAR and JOIN add structure that *tightens* the bound. So the general result is
proved once and each operator is a tightening — not three standalone proofs.

On top of the correctness square (`Adequate`) sit two independent resource bounds, each
proved generally and each shown to compose:

- **Work** (spatial) — output edits stay in the join-closure of the input's edits
  (`AdequateCov`), and this composes to whole programs (`Program.adequateCov`).
- **State** (temporal) — beyond a frontier, the *compacted* input suffices, so a pointwise
  operator keeps no pre-frontier history (`acc_compact`, `adequate_compact_sufficient`).

The two axes are orthogonal: a DELTA JOIN is spatially cheap (a monotone index probe) yet
temporally demanding — it may compact only up to just behind the current frontier.

## Open work

- **State axis, the hard part.** Only the base case is done. Open: the frontier-*strength*
  gradient (stateless / normal / delta-join tolerate progressively less compaction) and the
  composition of state *demand* across a pipeline, where a downstream delta join forces finer
  compaction upstream.
- **ITERATE and confluence.** ITERATE couples a time operator (`feedback`) with a data
  operator over `T × ℕ`; its correctness is a causality argument on the timestamp, not a
  coverage argument (`acc_unique`). Confluence — that different frontier schedules reach the
  same value — is trivial for every operator *except* ITERATE, where the answer is a fixpoint
  the schedule helps reach, so it belongs there. Genuinely open.
- **File factoring (optional).** The operator tightenings and the time/iterate material could
  move to their own modules; low value until there is new content to put in them.
  `acc_mapDomain` stays foundational in `Basic`.

## What landed

- Split `Coverage` into the general keystone vs. `RoundCoverage` (the reduce schedule).
- Work axis (#1/#2): `AdequateCov` carries the join-closure bound through the square and
  `Program.adequateCov` composes it end-to-end.
- State axis (#1'/#2') first cut: `compact`, `acc_compact`, `adequate_compact_sufficient`.

All axiom-clean; each is described in `README.md`.
