# Current evidence, not a permanent performance verdict

This is the **replaceable index** of qualifying measurements. Anyone may refresh
it using [REFRESH.md](REFRESH.md); the author of an engine change need not do so,
and identifying the cause of a changed result is not required. Historical runs
remain in dated records. [GAPS.md](GAPS.md) is an investigation backlog, not the
source of truth for current timings.

## Qualification checkpoint: 2026-09-10

Upstream checked: `229508dd6f7d046c5d1ac1b96da4be2f7b100124` on master-next.
**No performance result in this follow-on qualifies that engine revision.**
The [September 7 measurements](MEASUREMENTS.md) are historical, mostly from an
older harness. They are useful leads, not numbers to beat before an observation
can be corrected.

Since the benchmark foundation at `75fba2b9`, upstream merged cursor changes
(#866), Corgi/server batching work (#867), trace-column/scratch reuse (#868),
monotone identifier matching (#869), and bounded proxy-reduction sweeps (#871).
These touch measured execution paths. Do not carry old timing or profile shares
forward merely because the written query is unchanged. No speedup from these
commits is measured or attributed here.

| Recipe / evidence | Latest qualifying record | Status |
| --- | --- | --- |
| `tiny-catalogue-v1` | None under the refresh contract | Portable correctness/overhead gate; not a scale-performance result |
| `sf0003-panel-v1` | None under the refresh contract | Portable small generated-data baseline, awaiting measurement |
| Historical SF1 census and IC11 bank sweep | [September 7](MEASUREMENTS.md) | Historical; experimental drivers are not packaged |
| Historical BI1, IC6, IC13 and survey probes | [September 7](MEASUREMENTS.md) | Historical; reproduction prerequisites remain open |
| List-ordering review probe | [M5](MEASUREMENTS.md#m5-review-measurements) | Reported by review, not independently reproduced |

The BI06 maintenance/rewrite campaign is a separate follow-on; this index does
not quietly substitute those plans, bindings, or update schedules for the
natural catalogue. Neither portable recipe reproduces the historical SF1 runs.

## Initial leads worth picking up

These are priorities suggested by the historical evidence, **not remeasured
bottlenecks on the revision above**:

- **LDBC-001, unnecessary group materialization:** BI1's authored count
  alternative removed about half a second per affected update. Measure an
  automatic rewrite on the unchanged natural plan; its SF1 result is still
  missing. Do not assign the handwritten rewrite's gain to that engine rule.
- **LDBC-003, request retirement:** IC6 deferred substantial work to the next
  graph tick; BI9's apparently cheap maintenance hid a retirement timeout.
  Reproduce whole request lifetimes after the newly merged reduction changes.
- **LDBC-004, recursive bounds:** a one-edge IC13 probe became expensive merely
  by adding isolated people. A small scaling diagnostic could make that
  algorithmic limitation easy to investigate without a large dataset.
- **LDBC-008, portable scale runs:** land selective preparation, reproducible
  workload banks and whole-process memory protection before another broad SF1
  campaign. This enables investigating the other gaps on an M4 safely.

See [GAPS.md](GAPS.md) for the scoped tasks, contrary results, representation
and typing limitations, and independent-answer coverage still to improve.

## Updating this page

For each new row link the dated record and state recipe version, tested engine
revision, machine class, validation, and whether it supersedes a named record
or starts a new measurement series. Keep failed/capped attempts visible as
such, without assigning them successful cycle latencies.

Replace the current interpretation when qualifying evidence disagrees, whether
faster or slower. Use **not reproduced at revision X** when an old symptom goes
away without a known cause; reserve **fixed by X** for an attributed fix. A
single noisy outlier is not a replacement baseline. Disagreement between valid
repeats is **contested**, not permission to select the fastest run.

Freshness is relative to the explicitly tested revision, not the date of this
document. Engine/dependency changes require a new run before claiming current
performance; workload/timing-contract changes require a new series or a fresh
control. A docs-only change does not invalidate a binary's measurement. There
is no automatic benchmark service or CI timing threshold in this PR: the
revision labels make unmeasured state explicit, and the procedure makes this
index replaceable without the original investigator's involvement.
