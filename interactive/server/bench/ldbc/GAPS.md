# Gaps to investigate

Assessed against master-next `229508dd`. [CURRENT.md](CURRENT.md) contains the
current standard-runner measurements and their limits; [REFRESH.md](REFRESH.md)
defines how to replace them. No unpublished experiment or old timing is required
to pick up a task here.

Keep natural queries as the baseline. A handwritten alternative can demonstrate
an opportunity, but does not establish that the engine finds it automatically.
Use **observed**, **hypothesis**, **prototyped**, **not reproduced**, **contested**
or **fixed**, with a tested revision and reproducible evidence. Anyone may update
an assessment; no permission from its original author or engine author is needed.
Stable gap IDs are not renumbered. The entries below are source-level observations
and investigation proposals, not attribution of current elapsed time.

## LDBC-001: Aggregate observations materialize whole groups

**Observed lowering; optimization task.** [Compiler](snb/rel.py) lowers counts
to collect/length and sums to collect/fold. When only an aggregate is observed,
the collected list's contents and order may be unnecessary.

Recognize list-content independence, or additive/multiplicity-preserving forms,
in the engine's optimizer. Start with BI11/BI18's pure counts, then mixed
count/sum queries. Respect signed differences and empty-group semantics; a
positive-input count rewrite is not automatically valid for every DDIR program.
Keep the query definition unchanged and compare emitted/physical plans, answers,
whole cycles and memory. Do not confuse a faster scalar fold with eliminating
the relational collection that feeds it.

## LDBC-002: Filter placement and unused intermediate work

**Observed plan opportunities.** IC6 expands friendship/message relations before
its tag restriction. BI19 computes a rank whose result is discarded by the next
projection. Both natural forms remain in [queries.py](snb/queries.py).

Independent tasks: push filters through joins according to field dependencies;
remove unused ranking; prune unused product fields. Check ties, multiplicities,
empty results and ordering. Retain handwritten alternatives only as explicitly
named controls alongside the natural plan, not hidden benchmark substitutions.

## LDBC-003: Account for request retirement and trace maintenance

**Investigation, not a diagnosed current bottleneck.** Interactive execution
includes bind/read/retract/tick/empty. A tick after retirement may also perform
trace work. Standing-query teardown lies outside ordinary update medians.

Use the standard runner's complete cycles and separately reported retirement
before pursuing phase-specific wins. Then isolate a slow lifecycle and profile
it, including subsequent empty-request graph ticks where relevant. New join
input is not necessary for a Join operator to spend time maintaining traces.
A change that moves work into the next command is not a cycle speedup.

## LDBC-004: Global recursive hop bounds

**Observed structural limitation.** [Context.shortest](snb/queries.py) uses a
hop-indexed `|V|-1` bound. Irrelevant vertices can enlarge this bound even when
the requested connected component and answer are unchanged.

Add a reproducible small diagnostic with one relevant edge and increasing
isolated vertices, then investigate request/component-local bounds or a
different maintained path algorithm. Unit-weight bounded-distance queries may
permit tighter bounds than arbitrary weighted paths. Preserve deletion-safe
termination on cycles; unbounded relaxation can count to infinity. No performance
claim is attached until that diagnostic is landed and measured.

## LDBC-005: Observable list-ordering adapter

**Observed implementation; cost needs attribution.**
[signed_order_view](../../../src/corgi/reduce.rs) builds lexicographic ranks for
list-valued Min/Collect inputs, including strings. This is distinct from physical
arrangement-key ordering.

Measure rank construction separately from sorting, varying length, alphabet,
nesting, ties and block labels. Investigate resolving only tied groups, avoiding
unused suffix work and reusing scratch. Preserve equality classes as well as
observable ordering. A microbenchmark improvement still needs a query-level
measurement before being credited to LDBC.

## LDBC-006: Representation and sharing boundaries

**Observed representation; alternatives are hypotheses.** Strings are UTF-8
lists with 64-bit leaves. Post/Comment use a kind-tagged tuple. Shared server
imports cross row/column boundaries; derived work is not automatically shared
between independent programs.

Separate experiments: narrower projections, native arrangement sharing, string
representation, enum-shaped messages, sharing common derived work. Hold the
input rows, query and output contract constant. Compare isolated and concurrent
runs, including load/settle memory and empty-request updates. Selective loading
is a workload/harness choice, not an engine-memory optimization.

## LDBC-007: Attribute whole-query cost before choosing a kernel

**Investigation discipline.** Structural comparison, sorting, gathering,
allocation, scalar evaluation, trace maintenance and client decoding are
different possible costs. A synthetic kernel win alone does not locate a query
bottleneck.

Start from a reproducible slow standard-runner case. Profile worker activity
and client phases separately; do not add worker CPU/activity totals to elapsed
latency. Keep each engine change independent until its whole-cycle effect is
measured. Profiling infrastructure is a separate deliverable, not a dependency
on an unpublished driver.

## LDBC-008: Safe scale runs and representative banks

**Observed harness gap.** The suite loads the full common substrate and uses a
same-plan Python reference even for `--queries`/`--isolated`. The RSS monitor
covers only the server and does not account for compressed memory or the Python
process. The default parameter bank is a smoke bank, not an official workload
distribution.

The current repeatability check also found that `parameters.py` selects its
default company from an unordered set. Even unused company fields can change
the recorded bank between processes. The current recipe fixes
`PYTHONHASHSEED=0`; the harness should use stable selection and distinguish
effective query parameters from unused default fields.

First make selective preparation, complete reproducible banks and whole-process
resource protection portable. Retain every row of selected relations and label
full versus selective inputs. Grow scale only after checking host headroom;
do not lift a failed cap automatically. Extend both parameter diversity and
updates that actually change query results. Do not interpret unchanged answers
as evidence of cheap affected-result maintenance.

## LDBC-009: Independent answers and temporal boundaries

**Observed validation gap, not a finding of incorrect results.** All 41 queries
have positive witnesses and same-plan checks. Fourteen have some independent
hand-checked expectations; 27 do not. Prefer a few discriminating checks or
static guarantees over tests of incidental plan shape.

Expand request-dependence, ties, temporal boundaries and affected-result updates.
BI13 includes the message interval's end date while using a strict bound for
profile creation; settle that specification endpoint before changing behavior.
Specification corrections start a new measurement series when answers change.

## LDBC-010: Input admission and scalar typing

**Observed robustness/type limitations.** Source ascriptions are execution-time
assertions. A malformed feed can be acknowledged and then panic a worker on tick,
affecting other clients. Design complete-batch validation before admission and
sharding, without partially admitting a rejected batch.

An encoded float and a user's single-lane integer newtype are structurally
indistinguishable to float operations. Empty `list()` gets an element shape from
a declared constructor, but `append(list(), xs)` does not infer it from `xs`.
Treat admission, nominal numeric types, expected-shape inference and the
Float32/f64 API choice as separate bounded tasks. Performance results with
shape-correct benchmark data do not resolve these contracts.
