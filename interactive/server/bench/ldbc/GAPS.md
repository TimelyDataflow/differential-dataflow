# Gaps exposed by the SNB workload

Working backlog, initially assessed at `75fba2b9` (2026-09-07). This is not a
list of proven causes for every slow query. The [measurement record](MEASUREMENTS.md)
separates observations, experimental controls, and incomplete runs. Start with
the [runnable suite](SNB.md) and [data recipes](DATA.md), not a session-local tree.

**Freshness:** these are dated assessments, not automatically current defects.
[CURRENT.md](CURRENT.md) records which engine revisions have qualifying evidence;
as of the 2026-09-10 rebase to `229508dd`, the historical performance findings
need remeasurement. [REFRESH.md](REFRESH.md) defines portable recipes and permits
any investigator to replace an assessment with newer evidence, independently
of the engine change's author. Source changes alone do not establish a fix.

Keep the natural query definitions as the baseline. A hand-authored alternative
can establish an attainable improvement, but is not evidence that an automatic
optimizer implements it. Keep independent changes separate until measured.
The concerns below use the language of LDBC's
[pinned choke points](https://github.com/ldbc/ldbc_snb_docs/blob/b2269610f433da72e7c97041f01680aae369a903/choke-points.tex),
not a claim of official choke-point coverage or benchmark conformance.

## Working on an entry

Stable IDs are never renumbered. Record the owner/issue/PR when work starts;
initially all open entries are unassigned. Use **observed**, **hypothesis**,
**prototyped**, **not reproduced**, **contested**, or **fixed**, with the
assessment revision/date and evidence link. For each result:

- Preserve the reproduction: query, dataset hash, binding bank, exact delta,
  mode, workers, build, machine, and resource envelope. Label synthetic probes.
- State the observation separately from the proposed explanation. Include
  setup, release, and later cleanup; report whole cycles as well as phases.
- Attach compact measurements and validation evidence. A timeout or memory
  stop is a censored observation, not a timing sample or a zero.
- Close with a fix revision/PR, matched before/after results, and the relevant
  semantic check or static guarantee. Keep the closed entry and contrary results.
  CI should pin semantics; do not turn these M4 timings into CI thresholds.
- An unexplained disappearance can retire a performance hypothesis as
  **not reproduced at X**; it need not wait for a bisect or the original author.
  Changed workloads start new measurement series. Repeated contradictory
  evidence takes precedence over the prose here; preserve unresolved conflicts.

The recipes below name existing query definitions and a bounded first task.
Except for the ordinary suite commands, they are investigation recipes, not
claims that the historical experimental drivers have landed. To inspect a plan:

```sh
python3 interactive/server/bench/ldbc/suite.py --queries bi1 --emit
```

## LDBC-001: Aggregate observations materialize whole groups

**Prototyped; engine-level performance result still open.** BI1, BI5, BI11,
BI12, BI13, BI18; grouping and aggregate execution.

The relational compiler often lowers aggregates to `collect`/`fold`. An
authoring-level native-count experiment removed roughly 0.5 seconds per BI1
change: about 100x for one affected group, but only about 2x for a million-row
group with remaining count/sum work ([M2](MEASUREMENTS.md#m2-bi1-count-lowering)).
The separate DDIR collect/length prototype passed small checks but had no SF1
timing result. Neither prototype is part of the merged benchmark.

Start at [Compiler](snb/rel.py) and DDIR's reduction/optimization code. Recognize
an observed length or sum without constructing a list; first use BI11/BI18's
pure count patterns as small cases. Respect signed multiplicities: a rewrite
must match `collect` semantics, not assume every input is positive. Validate
insert/retract/restore and empty groups. Close only after an automatic rule
improves unchanged query definitions/DDP; record affected-group sizes and memory too.

## LDBC-002: Selectivity, unused work, and projection placement

**Observed plan opportunities; performance attribution partly hypothetical.**
IC6, BI19, and wide joins; filter placement, top-k, and intermediate width.

IC6 expands friendship/message relations before applying its tag restriction.
Historical binding took seconds, with substantial Join/Arrange scheduled work
([M3](MEASUREMENTS.md#m3-lifetime-and-recursion-diagnostics)). This suggests
cardinality/materialization work, not a demonstrated scalar-kernel diagnosis.
BI19 computes an unbounded rank whose output is discarded in the next
projection. Both natural forms deliberately remain in [queries.py](snb/queries.py).

Start with independent rules: remove unused rank; push a filter across a join
when its field dependencies allow it; prune unused columns. A hand-written
tag-first IC6 variant is a useful control, not a replacement baseline. Check
ties, multiplicities, and ordering before attempting top-k pushdown. Close
individual rules with unchanged answers and emitted/physical plan evidence;
do not require the entire optimizer backlog to land together.

## LDBC-003: Request retirement and deferred trace work

**Observed; responsible allocations/scheduling remain open.** IC6, IC11, BI9,
BI20; prepared-request and standing-query lifetime transitions.

IC6's first graph tick after release took 1.1–1.4 seconds; later empty-request
ticks were around 1 ms. BI9 had roughly 33 ms maintenance but exceeded 90 seconds
retiring its standing binding. The fresh IC11 sweep also has nontrivial empty
checks, and its 48-ID Vec run crossed the chosen memory cap during release
([M1](MEASUREMENTS.md#m1-ic11-request-cycles), [M3](MEASUREMENTS.md#m3-lifetime-and-recursion-diagnostics)).

First reproduce bind/read/retract/tick/empty, then several graph ticks with no
requests. Profile those boundaries separately, including trace merges and
retained batches. Scheduled Join time can be trace maintenance without new join
input. A change that merely moves work to the next peek/tick is not a cycle
speedup. Close with both complete-cycle cost and bounded retirement memory.

## LDBC-004: Global recursive hop bounds charge irrelevant vertices

**Observed structural limitation.** IC13, IC14, BI10, BI15, BI19, BI20;
path execution and deletion-safe termination.

[Context.shortest](snb/queries.py) uses a hop-indexed `|V|-1` bound. In a
synthetic IC13 probe the only edge and request were `0 -> 1`; adding isolated
people changed neither answer nor useful component, but increased Corgi bind
from 2.11 ms at 32 people to 574 ms at 10,295. Both backends show growth at
the smaller sizes ([M3](MEASUREMENTS.md#m3-lifetime-and-recursion-diagnostics)).

First add that small one-edge scaling diagnostic using the ordinary emitted
IC13 plan. Explore component/request-local bounds or alternative maintained
path algorithms. BI10's unit-weight edges and requested maximum may permit a
tighter bound; that is not a valid rule for arbitrary weighted paths. Deletion
must still terminate on cycles; unbounded relaxation risks count-to-infinity.
Close with insertions/deletions, disconnected components, ties, and scale tests.

## LDBC-005: Observable list ordering has an expensive adapter

**Observed by review; not a replicated end-to-end measurement here.** Corgi
Min/Collect on lists, including strings; nested ordering and materialization.

[signed_order_view](../../../src/corgi/reduce.rs) ranks list elements and then
refines lexicographic ranks by position. The review's 100,000-row probe measured
2.3–8.1x the replaced length-first sort's time, largely building ranks. These
are different ordering contracts, not interchangeable implementations; see
[M5](MEASUREMENTS.md#m5-review-measurements) for provenance and all four points.

First reproduce rank construction separately from sorting, varying length,
alphabet, nesting, ties, and block labels. Test resolving only tied groups,
avoiding unused suffix work, and scratch reuse. Preserve equality classes as
well as lexicographic order. Do not change physical arrangement-key ordering
to fix an observable Min/Collect cost. A successful microprobe then needs a
list-heavy query measurement before claiming LDBC improvement.

## LDBC-006: Representation and sharing boundaries amplify work

**Observed representation; performance benefit of alternatives is a hypothesis.**
Wide/message queries, nested results, and concurrent panels.

Strings are UTF-8 lists with 64-bit leaves, not packed bytes/dictionaries.
Post/Comment is a kind-tagged tuple rather than an enum layout. Server imports
share raw row arrangements, crossing row/column conversions; derived query
relations are not shared between programs. These are visible baseline choices,
not measured reasons to redesign everything at once.

Separate candidate experiments: native arrangement sharing, narrower projections,
string representation, enum-shaped messages, and sharing common derived work.
Hold query/data semantics constant. Compare isolated and concurrent panels,
including load/settle memory and updates while request inputs are empty. Retain
ordinary row and column controls; do not report omitted input tables as an
engine-memory optimization.

## LDBC-007: Kernel wins need whole-query evidence

**Prototyped; negative IC11 result retained.** Corgi structural survey/merge.

A single-pair tuple-list comparison prototype reduced a long-equality probe
from 33.0 ms to 0.367 ms. A matched IC11 SF1 A/B/B/A comparison subsequently
gave mean complete cycles of 60.69 / 60.73 ms: no material improvement
([M4](MEASUREMENTS.md#m4-survey-prototype-negative-control)).

Do not present this as an outstanding 90x LDBC win. The next task is to measure
how often the specialized shape occurs and how much time it consumes, or find
a wide workload where it matters. BI1 was not measured with this prototype.
Keep it independent of aggregate/plan changes so attribution survives.

## LDBC-008: Portable scale runs and representative workload banks

**Observed harness/resource gap.** All generated-data workloads.

The merged suite loads the full common substrate and uses a full same-plan
Python reference even with `--queries`/`--isolated`. Historical narrow SF1 runs
used experimental selective loading, complete supplied bindings, and digest
mode with a separate oracle. Those options and the stronger Darwin footprint
guard are not merged. RSS alone missed compressed-memory exhaustion and an
overnight run restarted the host. A later five-query run crossed a conservative
2-GiB process-group cap during setup; this is not proof the machine cannot fit
it ([M6](MEASUREMENTS.md#m6-capacity-and-coverage)).

First make a bounded scale path portable: preserve all rows of selected
relations, distinguish full versus selective substrate, and make validation
status explicit. Monitor Python plus server, compression, and swap; never lift
a failed cap automatically. Extend banks and affected-result deltas beyond
smoke cases. IC11's bank alone changed cost about 3x without an engine change.
The [data recipe](DATA.md) supplies identities, not a claim that SF1 is safe.

## LDBC-009: Independent semantic coverage and one endpoint question

**Observed validation gap; not a finding of wrong answers.** All reads; BI13.

All 41 reads have positive witnesses and same-plan execution checks. Fourteen
have some independent hand-checked expectations; 27 do not. Expand meaningful
boundary cases, especially request dependence, ties, temporal boundaries, and
updates that change answers. Prefer a few discriminating checks or static
guarantees over tests of incidental plan shape.

BI13 includes the message interval's end date but uses a strict profile-creation
bound. Settle the ambiguous specification endpoint before changing behavior.
Do not reopen the resolved review challenges: BI19/BI20's globally cheapest
paths, BI13's inclusive calendar-month count, IC14 v2's shared BI19 weight
formula, and BI16's induced-subgraph degree match the pinned specification.

## LDBC-010: Shape contracts are fatal, and scalar typing is incomplete

**Observed correctness/robustness limitations; not timing claims.** Server and
scalar compiler, both backends where applicable.

Source ascriptions are execution-time assertions. A malformed row can be
acknowledged at feed and then panic a worker on tick, disconnecting other
clients. First design admission-time validation for a complete feed batch,
before sharding; rejected batches must not partially enter the dataflow.
Test another tenant remaining live. The benchmark's private servers and
shape-correct inputs are not a fix for shared-server admission.

Separately, an encoded float and a user's single-lane integer newtype are
structurally indistinguishable to float operations. Empty `list()` gets its
element shape from a declared constructor, but `append(list(), xs)` does not
infer it from `xs`. Treat nominal numeric types, expected-shape inference,
and the f64 versus specification Float32 API choice as separate bounded tasks.

## Closed foundations (retain this history)

- **LDBC-011 — fixed:** equal-key chunk merging could cross the timestamp
  horizon of a neighboring chunk. [#862](https://github.com/TimelyDataflow/differential-dataflow/pull/862),
  `d77d8a70`, adds the horizon constraint and small/scale contract regression.
  This was a correctness fix, not an attributed performance improvement.
- **LDBC-012 — fixed:** snapshot quoting disagreed between loaders, and old
  headline update/restore totals included repeated Python full-graph differences.
  [#864](https://github.com/TimelyDataflow/differential-dataflow/pull/864),
  `75fba2b9`, shares the reader, computes deltas once, and separates timing
  components in v2 reports. Old reports require fresh comparison baselines;
  do not count removed Python work as a DDIR speedup.
