# DDIR bench spike, parked: state and open items (2026-09-06)

This branch (`bench-spike`) is the side spike that followed the server
migration (PR #858). It is parked here, as a draft PR, so that what was
learned and what is still open is public rather than in one person's notes.
The findings themselves are in [`2026-09-04-findings.md`](2026-09-04-findings.md)
(twenty sections); this file is the disposition of everything the spike left
behind. Commit hashes are as of the rebase onto master-next 9a02d5dd.

## Ready to land, as one small PR

Corgi-backend fixes, each tied to a profile finding, each measured, tests green:

| commit | what | finding |
|---|---|---|
| 485eeef9 | `CorgiChunk::advance` advances each distinct time once, not once per row | §6 |
| 020bd146 | join projection compiled once per operator, not once per output block | §3 |
| 34c37a87 | typer memoizes `shape_of_term` within one lowering (nested conditionals were 3^depth) | §7 |
| b19e1cfe | the reduce probes each chunk with its slice of the change set | §14 |
| 9e5e7012 | the merge's key-order check compares adjacent rows in one pass | §16 |
| 29bc8933 | `cc.ddp` and `triangles.ddp` as example programs, with tests | §8 |

Plus, on `survey-merge`: 3c4c7953, `ColTimes::push_range` copies time ranges
as lanes (`extend_from_self`), not row by row. Independent of the corgi pin;
it was the per-row cost that mattered in the merge (§18).

## Decision needed: which merge

- da105bfa (`bench-spike`) merges integer lanes with integer compares, row by
  row. A shape-specific fast path. Recommendation: do not merge it.
- a8313a2d (`survey-merge`) merges by corgi's `survey`: exclusive runs copied
  by range, row work only inside matched `(key, val)` groups. At parity with
  the fast path (§18; re-timed after the rebase in
  `2026-09-06-6d8ed333-medium-rebased.md`), no per-row compare. It needs
  corgi's `survey` to gallop lane by lane over product columns. WIP PR #22
  does that for products of `u64` leaves only, and is not being merged as a
  spot fix: the general form is the subject of the lane-wise line of work
  (`corgi/dev/comparisons.md` on branch `corgi-comparisons-survey`,
  `corgi-sort-bfs`). `survey-merge` currently pins corgi at the #22 head
  (6d8ed333) for validation; re-pin when the general form lands.

## Open, with a measured shape

- **Hierarchical `min`** in the DDIR optimizer (§15: more than two levels pay).
- **`leave_dynamic` over multi-element stamps**, then the open-loop
  `tick n sync=k` (§11). DD PR #855 has the stamps work and needs a rebase.
- **A `sum` reducer**, then TPC-H by transcription; LDBC assessment in §10.
- **Four-worker scaling** is weak: a quarter idle, a fifth coordinating (§4, §9).
- **Linear ops after a join are a separate operator.** The optimizer fuses
  Linear into Linear only; scc's `trim` chain renders as join, arrange, join,
  linear[filter, project] on both backends. Pushing a sole-consumer,
  time-preserving Linear into the join's output logic is an optimizer rule
  plus a `post` chain on `Node::Join`; a small constant, not the scc gap
  (not among the >3% entries of the scc profile). Turning the label equality
  into a join key is an e-graph rewrite (PR #856).
- **Compiled baselines** exist only for scc (PR #837, four rungs; corgi is
  ~2.7x the compiled plan). A `.ddp → .rs` emitter would produce the
  "compiled DDIR plan" rung for every program mechanically; proposed, not started.

## Tried and recorded as no

- Four inline `PointStamp` coordinates (§5).
- `find_ranges` galloping from the previous hit (§13).
- The example driver's initial-epoch numbers were under-counts; the live
  server's are the standing ones (§19).

## Harness

`bench/bench.py` drives the live server binary; reports are JSONL plus a
markdown summary named by date, revision and scale. The reports on this
branch are its record and need not merge; the harness could, without them.

## Elsewhere

- WIP corgi PRs #19 and #20 are not merging (too complex; optimizes a method
  the batched merge makes dead).
- Worktree `WIP-effect` holds an uncommitted differential test of #21's
  producers against master's fused loops; a review artifact for a merged PR.
