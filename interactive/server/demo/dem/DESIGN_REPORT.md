# Civil logistics design report

## Slope-aware DDIR trail comparison (2026-08-31)

The shelter trail exposed a mismatch between the routing vocabulary and its
objective. The `grade` coefficient prices absolute elevation change, not
normalized slope. On a nearly monotone 744 m climb, switchbacks retain almost
the same elevation term and add distance, so the original route climbed
directly and reached 792.5‰ under an 800‰ delivery-time limit.

Routing now has an optional per-request maximum grade. The cap is maintained
as DDIR input 8, joined to the request before edge expansion, and filters
illegal edges inside the recursive shortest-path fixed point. The Python
Dijkstra, semantic history replay, client rollback, and authoritative judge
carry the same cap independently. Uncapped historical commands replay without
schema changes. The live viewer receives `route_options` rows containing the
owner, route, cap, exact predecessor, and accumulated cost.

Three agents used the shared DDIR server—not offline substitute routes—to
survey farm 11 to shelter 31 with fixed coefficients `(1,12,1000,0,1)`:

| Cap | Cells | Distance | Variation | Maximum | High-runoff cells | Cost | Judgment |
|---:|---:|---:|---:|---:|---:|---:|---|
| 400‰ | 56 | 3,421 m | 746 m | 400.0‰ | 1 | 18,332 | economical, but mildly rasterized |
| 300‰ | 80 | 4,737 m | 786 m | 293.3‰ | 17 | 22,888 | legible switchback; strong constraint |
| 250‰ | 100 | 6,545 m | 822 m | 245.3‰ | 19 | 28,722 | artificial overshoot and sawtooth |

All three were dry, had zero independently audited cap violations, and agreed
cell-for-cell with the authoritative route judge. No player walked, delivered,
built, or changed terrain, so the comparison did not create path-reuse
feedback between treatments.

This is a productive interaction rather than a one-dimensional calibration.
The gentler routes found dry high-accumulation corridors because runoff weight
was deliberately held at zero. A 300‰ cap therefore trades steepness for both
distance and drainage exposure. The next controlled sweep should hold the cap
at 300‰ and add a light runoff cost, while 400‰ is the better current candidate
for ordinary loaded travel. Hard caps also reveal 53 m grid aliasing; a soft
nonlinear steepness cost, turn cost, or finer/subcell trail graph may produce
more natural contour paths.

Operationally, this was also the first playtest in which the role agents used
DDIR as a parameterized planning service and left simultaneous, inspectable
proposals in the world. They still interpreted geometry privately, but the
quantitative plans, cap enforcement, and audit are shared and replayable.
With all three recursive alternatives live, one settled `ps` sample reported
5,213,936 KiB RSS (about 5.0 GiB), close to the earlier one-world baseline;
this is encouraging but is not a controlled peak-memory benchmark.

## Persistent pathways V4/V5: wide world, observatory, and alpine trail (2026-08-30)

The persistent world now uses `engadin_wide.txt`, a 256×192 zoom-11 crop at
the original approximately 53 m cell scale. It extends rather than refines the
terrain: every old cell is identical at `wide(x,y)=old(x+72,y+16)`. The
completed V3 world was first judged `SUCCESS`, then frozen into a genesis
snapshot containing 161 infrastructure cells, 1,211 traversal cells, 15
deliveries, and zero terrain edits. Parent events, relations, programs, and the
new DEM are hashed. Historical routes were not rerun against the larger graph.

V4 made the Piz Nair ridge observatory at `(55,110)`, elevation 2,861 m, a
required project. The selected quarry-to-observatory survey used coefficients
`(1,12,1000,1,1)` and produced a 164-cell, 10,179 m route with 1,346 m of total
elevation variation. It reused 111 established road/bridge cells before a
53-cell mountain spur. Two courier scouts established that spur; route cost
fell from 33,650 to 30,577 without changing the geometry.

The road role then built exactly 53 engineered cells. The calibrated fill-only
profile used 32 height-cell units across nine cells, with a 7 m maximum, and
needed no new bridge because all three high-runoff crossings were inherited
bridges on the valley trunk. Construction left 12/65 aggregate and 28/60 fill
rock. Agent 3 delivered the 20-unit foundation from the online quarry over the
fully connected route, after which the observatory became operational. No
protected site flooded. The chronological judge agreed on routes,
infrastructure, deliveries, traversals, paths, build revisions, terrain,
water, accumulation, connectivity, roles, materials, genesis hash, and DEM
hash: `VERDICT: SUCCESS`.

This was productive but exposed the cost of the global recursive route: the
53 one-cell revisions took 645.1 s, or 12.41 s per cell. The cell-by-cell event
boundary remains valuable for audit and hydraulic feedback, but a UI batch is
not enough; DDIR route maintenance itself needs to become cheaper.

V5 then added a deliberately different mountain endpoint: the Muottas ridge
shelter at `(213,75)`. A road is not prohibited merely by narrative. Its
counterfactual profile is naturally absurd—44 roads, one bridge, 4,074 fill
units, and a 252-unit maximum lift—while a foot route from the valley farm is
46 cells / 2,627 m, dry, and has a 792.5-permille maximum grade. The rules
therefore give foot travel an 800-permille limit, forbid road freight and
construction to the trail-only endpoint, and require exactly two five-unit
courier loads. Those real cargo trips established 45 new path cells, consumed
the last 10 units of the 200-unit lowland supply, and supplied the shelter
10/10. The route was retired and V5 also judged `SUCCESS` with the observatory
still operational.

The contrast is the most useful design conclusion of the turn:

- Piz Nair justifies a shared bulk-access project and three role handoffs.
- Muottas justifies a steep, cheap, low-capacity trail precisely because a
  road is physically and materially inappropriate.
- The same terrain and route objective can therefore support transport modes
  without arbitrary unlock trees. Demand size, slope tolerance, and material
  feasibility choose the mode.

The first exact routing optimization was then tested by recovery over the same
62 semantic commands. Inside `pathways::routes`, the N-row recursive frontier
now joins its singleton request before the eight-edge fan-out. This removes the
widest 8N intermediate arrangement without changing inputs, outputs, costs, or
tie-breaking. The recovered world reproduced both route costs and all 53 bed
heights/hydraulic deltas exactly, and the final judge remained `SUCCESS`.
Construction replay improved to 536.6 s / 10.32 s per cell, a 16.8% latency
reduction. A fresh optimized server settled around 4.9 GB RSS; one live
post-trail route accepted in 13.5 s and did not push the sampled RSS above that
level. RSS samples are allocator/high-water observations, not a controlled
peak benchmark.

The next exact scaling candidate is incumbent-bound A* pruning. A valid route
witness supplies an upper cost `U`; octile distance plus unavoidable endpoint
height difference is an admissible `h`. Offline reconstruction retained only
17,171 of 49,152 cells (34.9%) for the observatory request under `g+h<=U`.
The hard part is lifecycle correctness: road/path reuse can lower the witness,
while terrain and hydrology edits can raise it, so request and bound must be
refreshed atomically on every relevant revision and independently replayed.
`enter_at(g+h)` is also worth an A/B scheduling experiment, but its current
backend buckets priorities by powers of two and it does not reduce final state.
An explicit survey rectangle is useful later for discovered terrain, but it is
a new route-domain mechanic, not an exact optimization, and must be recorded
as such.

Good next geography is Pontresina as a road hub followed by a southeast
Bernina/next-valley contract. Weather, funiculars, and genuine fog should wait.
Honest observatory revelation requires filtered server/viewer relations,
monotone shared `known_cells`, routing restricted to knowledge available at
that historical revision, and replay auditing; the present global WebSocket,
DEM file, and shortest-path oracle make a visual fog mask cosmetic.

## Persistent pathways V3: quarry and watershed extension (2026-08-30)

The newest iteration keeps one 128×128 Engadine world and extends the completed
town network rather than staging another board. It adds a quarry bench at
`(98,22)` above Samedan and a watershed worksite at `(36,106)` above St.
Moritz. These are topographically defensible locations selected from the DEM;
the model makes no claim about real geology or avalanche zoning.

The bootstrap is deliberately chained:

1. Agent 1 carries two light five-unit loads to activate the quarry. Those two
   real journeys establish its desire path.
2. Agent 2 spends the valley's 17 aggregate units to extend the existing road
   network uphill. The calibrated contour route needs 15 cells, leaving only
   two cells of revision slack.
3. The quarry becomes a material source only when both activated and connected.
   It then unlocks 24 aggregate and 50 rock units once, never by acting as a
   reachability seed.
4. Agent 1 scouts the watershed alignment twice. Agent 2 builds its ordinary
   cells; agent 3 supplies a bridge if that crossing treatment was selected.
5. Agent 3 can deliver 30 units of bulk rock only from the online quarry over a
   fully connected route.

New surface roads use a 400-permille coarse-raster grade limit. A monotone
fixed-point computes the least fill-only profile over the whole remaining
alignment, including diagonal lengths and nearby connected boundaries. Each
accepted build commits one cell, consumes aggregate and rock, and edits the
same terrain input that drives priority-flood water and drainage accumulation.
The chronological replay recomputes hydrology and live routes after every such
revision.

At the watershed branch the two calibrated treatments are intentionally
unequal:

| Treatment | New cells after quarry access | Rock fill | Hydraulic effect |
|---|---:|---:|---|
| bridge-sensitive route | 7 roads + 1 bridge | 0 | channel terrain preserved |
| embankment at the same runoff crossing | 8 roads | 39 | six accumulation cells reroute; crossing accumulation 555 → 1 |

The embankment raises the runoff cell by 20 coarse elevation units and raises
two approach cells by another 19 units to retain grade. This is deliberately a
legible raster mechanism, not a literal road-design prescription. It creates
the requested choice between scarce structure capacity and disruptive,
material-hungry fill. A `preview` is read-only, and a route cannot switch from
one crossing treatment to the other after construction begins.

An exact offline chronological replay of the inherited 24 accepted V2 commands
plus the new bridge treatment reached the quarry and worksite without flooding
a protected valley site. Ten focused rules tests cover legacy compatibility,
graded connectivity, bridge exemption, least fill, infeasible fixed
boundaries, bridge/embankment alternatives, scouting, role enforcement, and
deterministic replay.

Three role-separated agents then completed the live upgraded world. The
courier selected a 52-cell contour route, activated the quarry with two porter
loads, and later scouted the worksite path. The road engineer built 15 quarry
cells, then advanced four watershed cells until the role gate rejected a
bridge at `(39,109)`. The structures agent built exactly that bridge; the road
engineer added the final three cells; the structures agent delivered 30/30
bulk rock. The final chronological judge agreed on all routes, journeys,
infrastructure, terrain, water, accumulation, connectivity, roles, and
materials: `VERDICT: SUCCESS`.

The route choice was meaningful but the safe treatment was somewhat dominant.
Runoff weight 1 produced a 7,106 m route with a 25 m maximum step and a bridge;
runoff weight 8 was only 88 m shorter, cost substantially more, and introduced
a 42 m step. At the chosen crossing, preview showed zero fill for the bridge
versus 39 rock units for an embankment that would reroute six accumulation
cells. With bridge kits still available, all agents preferred the bridge. A
future storm, bridge scarcity, or a downstream benefit from diversion would
be needed to make the embankment competitive rather than merely dangerous.

Two usability findings were clearer than any rules failure. First, after a
whole-route preview, issuing 15 identical one-cell quarry builds was tedious;
the client now provides `build-until`, which stops at the next role/choice gate
while retaining atomic historical revisions. Second, status initially combined
lowland cargo and quarry rock into
`220/200 source capacity`; the display now separates town/light cargo,
worksite rock, road aggregate, and road-fill rock. The inherited design-space
judgment below predates this V3 extension.

## Outcome

The logistics scenario adds three non-transferable roles to the equilibrium
water world:

- a road engineer with 16 surface-road cells;
- a structures engineer with one water-passing bridge deck;
- an earthworks engineer with 950 meter-cells of cut/fill.

Construction is historically constrained by the live transport network.
Surface roads can be built only on dry terrain, flooded roads immediately stop
providing access, bridge decks remain passable while water flows underneath,
and earthwork must be beside depot-connected infrastructure. Success requires
both a dry, unchanged village and a depot-to-town road/bridge route of at most
20 hops.

Two fresh, role-separated agent teams played without source, tests, prior runs,
or a known solution. The first run at 800 earth units failed cleanly and exposed
an unrecoverable planning trap. The second run, after increasing only the earth
grant to 950, recovered from the same class of hydraulic mistake and passed the
authoritative judge. The resulting design space is **productive**, not flat or
immediately obvious, although its hydraulic information needs improvement.

## What interacts

The mechanics create several real dependencies rather than three independent
budgets:

1. The earthworker cannot reach the outlet without a connected road alignment.
2. A road placed on an intended channel cell makes that terrain uneditable.
3. A bridge can preserve access while the earth beneath its deck is lowered.
4. Spending the bridge on construction access means an ordinary road must later
   close the town-service gap.
5. The lake shoreline recedes after each cut, so the next worksite may be dry
   but inaccessible until the road engineer advances.
6. Road cells used for access compete directly with the exact-20-hop service
   route; in the successful run the road budget ended at exactly 16/16.

This produced asymmetric agency. The structures engineer could not donate its
bridge unit to roads or earthwork; instead, it changed which cells those roles
could legally use. The road engineer's decisive contribution was not merely
supplying cells, but finding a one-cell saving in the historical access plan.

## Trial 1: diagnostic failure at 800 earth

Run artifacts are retained locally under
[`runs/trial-v1-01`](runs/trial-v1-01/), including the full
[`site_office.md`](runs/trial-v1-01/site_office.md) negotiation and
[`events.jsonl`](runs/trial-v1-01/events.jsonl) action record.

The team compared north and south outlets, two service/access bridge uses, and
multiple parallel road alignments. It changed the bridge from the service gap
at `(95,35)` to an excavation deck at `(95,28)`, expecting to replace the
service bridge with a dry road after drainage. The agreed first cut used 638
earth units. It was hydraulically plausible against the initial 1775 lake
outline but ended at `(94,30)`, which ceased to be connected to the residual
basin as the water surface fell.

After equilibrium exposed `(94,31)=1758` as the next dry saddle, the road and
earth roles staged one correction for another 51 units. The pool fell again to
1749, but the protected village remained wet. A terrain-only lower bound showed
that merely reaching the nearest editable crossing at y=35 required at least
160 additional earth units; only 111 remained. No illegal or rejected mutation
was attempted.

Final authoritative result:

```text
village dry: all false
village unchanged: all true
town service path: none
spend: road 9/16, bridge 1/1, earth 689/800
terrain/water/infrastructure/connectivity/spend replay: all true
DDIR violation views: empty
historical violations: none
VERDICT: FAILURE
```

Judgment: early collaboration was productive, but the 800-unit balance made a
reasonable incomplete survey terminally overconstrained. The failure was
diagnostically useful: the scarce bridge mattered, the roles corrected one
another, and the world distinguished a legal plan from a successful one.

## Iteration

Only the earth grant changed, from 800 to 950. Roads remained at 16 and the
bridge at one. The known direct design costs 773 earth, leaving 177 (18.6%) for
recovery; the first trial's terrain path after its mistake needed 236 more
units, putting a comparable recovery near 925. This made bad-but-plausible
sequencing recoverable without making earthwork irrelevant.

The judge was also tightened to compare DDIR's `balance` view with independent
grant-minus-spend arithmetic. Client-side rejected actions now produce one
event rather than duplicate records.

## Trial 2: recovery-balanced success

Run artifacts are retained locally under
[`runs/trial-v2-01`](runs/trial-v2-01/), with the full
[`site_office.md`](runs/trial-v2-01/site_office.md) and
[`events.jsonl`](runs/trial-v2-01/events.jsonl).

This fresh team initially proposed paving `(96,28)` and other high north-saddle
cells. Road and structures independently caught that surface roads would lock
the intended channel before any construction occurred. The team moved to a
parallel six-road spur and placed the bridge at `(95,29)` so the deck would
remain connected after its terrain was cut.

Their staged 540-earth outlet was legal and carefully equilibrated, but it
still mistook an initially wet inlet for a complete final connection. Revision
5 exposed `(95,31)=1752` as the next control, with 410 earth and ten roads
remaining. The recovery analysis found an eleven-cell terminal channel costing
233 earth. A naive cut-then-road ladder required eleven roads and would fail by
one.

The structures agent found the saving: omit road `(95,34)`. Earthwork at
`(95,34)` was reachable from road `(95,33)`, while `(95,35)` was independently
reachable from the pre-existing connected road `(96,35)`. After both cells were
cut in one atomic revision, road `(95,35)` simultaneously resumed construction
access and completed the exact-20-hop service route.

Condensed accepted-action transcript:

| Revisions | Role | Result |
|---|---|---|
| 1 | road | six-cell parallel access spur |
| 2 | structures | bridge `(95,29)` |
| 3--5 | earth | staged downstream leg, north saddle, and inlet; 540 total |
| 6--12 | road/earth | alternating roads `(95,30..33)` and cuts `(95,31..33)` |
| 13 | earth | atomic skip cut `(95,34)` and `(95,35)` |
| 14 | road | dual-purpose service/access road `(95,35)` |
| 15--24 | road/earth | alternating cuts `(95,36..40)` and finalized roads |
| 25 | earth | terminal cut `(95,41) 1710 -> 1708` |

No rejected client action or historical violation occurred. The office did
contain a crossed asynchronous plan update (an obsolete eight-road plan was
acknowledged after a newer six-road plan); explicit supersession notices kept
it from reaching the world.

Final authoritative result:

```text
village dry and unchanged: all true
town service path: 20 hops (limit 20)
spend: road 16/16, bridge 1/1, earth 773/950
passable infrastructure: 52/53
terrain/water/infrastructure/connectivity/spend/balance replay: all true
DDIR violation views: empty
historical violations: none
VERDICT: SUCCESS
```

All three role players independently classified the run as **productive**.

## Design-space judgment

The scenario is not too flat:

- resources changed decisions, ending with zero road and bridge slack;
- bridge location altered both the earth sequence and final road obligation;
- a single road cell distinguished success from failure;
- role decisions caused several explicit revisions to collaborators' plans.

It is not simply forced or obvious:

- the two blind teams selected different bridge cells;
- both compared credible service-bridge and construction-deck uses;
- the successful team corrected a paving conflict, a stale-plan race, an
  incomplete outlet, and a one-road deficit;
- a separate verified solution uses the bridge at `(95,35)` and a different
  access/channel alignment.

The weak point is legibility. An initially flooded cell looks hydraulically
connected even when it becomes the next barrier during drawdown. Radius-limited
maps make the full basin-to-receiver path laborious to audit. That difficulty is
plausible civil-engineering work, but too much of it currently comes from
transcribing raster heights rather than comparing designs.

## Recommended improvements

### 1. Add surveying without giving away the design

Add a no-spend, non-authoritative planning command that accepts a hypothetical
terrain batch and reports projected village levels, the controlling spill
elevation, resource cost, and cells that would lose construction access. Limit
it by survey tokens or expose it only to the earthworks role if unrestricted
preview makes the puzzle too obvious. A polyline profile is a simpler first
step.

### 2. Version shared plans

Use plan IDs and `supersedes: PLAN-ID` in the site office. The action client is
serialized and revision-safe, but prose plans can cross. A lightweight
proposal/acknowledgment ledger would preserve asynchronous collaboration while
preventing an agent from authorizing stale coordinates.

### 3. Make bridge geometry more physical

The current one-cell bridge is a useful raster abstraction but acts as a
persistent excavation deck. A richer structure should have two dry abutments,
an axis/span, a deck elevation, and possibly a maximum span. This would make the
structures role reason about alignment rather than a magic passable cell.

### 4. Introduce towns, works yards, and material delivery

The next strong generalization is a small logistics economy:

- a works yard supplies road aggregate;
- a quarry supplies rockfill after a road reaches it;
- a town supplies labor or machinery only while connected;
- bridges consume structural units, roads consume aggregate, and earthworks
  consume equipment-hours delivered over the live network.

Resources should remain non-fungible and capability-gated. Connecting a quarry
would enlarge the feasible dam/causeway family but cost road cells and travel
distance, producing the desired tension between a beautiful network and the
civil work it is meant to support.

### 5. Defer capacity-limited culverts and tunnels until flow has time

In the current static equilibrium, any terrain opening has unlimited effective
discharge: a one-cell pinhole eventually drains the same basin as a wide
channel. A meaningful culvert needs storm volume, discharge capacity, or a
finite time horizon. Until then, the present bridge overlay honestly models
"road above, water below"; calling a narrow opening a capacity-limited culvert
would promise dynamics the physics does not contain.

### 6. Broaden objectives after the survey/UI iteration

Once plans are easier to evaluate, add multiple towns, weighted service demand,
road grade/maintenance cost, and a second hazard or storm. That creates route
redundancy and robustness choices. Adding these before better surveying would
compound opacity rather than deepen strategy.

## Implementation and audit boundary

`logistics.ddp` incrementally maintains infrastructure, dry roads, bridge
passability, depot reachability, construction frontier, spend, balances, and
violation views. The Python rules engine independently replays every revision
because historical access cannot be reconstructed from final reachability
alone. Every accepted client action atomically stages all terrain and ledger
feeds at one server epoch, then ticks once. The final judge separately checks
priority-flood water, replayed terrain and infrastructure, connectivity,
resources, locks, and the hard goals.

Atomic staging is not durable transaction storage: a process crash between
staging and the event-log append remains a limitation. The append-only action
trace in DDIR is world authority while the server is alive.
