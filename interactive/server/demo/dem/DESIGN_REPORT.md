# Civil logistics design report

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
