# Persistent pathways protocol

This experiment asks whether cargo travel, topographic route choice, desire
paths, road investment, and coarse hydraulic engineering produce a useful
shared-world loop. Version 3 retains the completed valley network and adds a
mountain quarry plus a watershed worksite. Version 4 freezes that completed
world into a wider board and makes a Piz Nair ridge observatory the next
required project.

V4 uses the 256×192 `engadin_wide.txt` terrain at the same approximately 53 m
orthogonal and 75 m diagonal cell scale. It extends the V3 crop rather than
increasing resolution. All inherited cells translate by `(+72,+16)`; roads,
frozen journeys, deliveries, and any terrain works are imported as a hashed
genesis snapshot. The 256×256 `engadin_256.txt` board is a different,
higher-resolution experiment and is not the persistent game.

DDIR maintains equilibrium water, drainage area, weighted route surveys,
frozen journeys, public roads and bridges, source reachability, and delivered
demand.

## Route cost

Each survey declares five non-negative integer coefficients:

```text
distance, grade, water, runoff, reuse
```

DDIR runs an eight-neighbour min-cost fixed point. Entering a cell costs:

```text
distance × configured step length
+ grade  × |height change|
+ water  × water depth
+ runoff × min(drainage area, 100)
+ reuse  × step length × surface factor
```

The configured step lengths are 53/75 m on the 128 and wide boards and 26/37 m
on the optional fine 256 board. Surface factor is 2 on raw ground, 1 on an
established path, and 0 on a road or bridge. With distance 1, reuse values 0,
1, and 3 make raw movement cost respectively 1×, 3×, and 7× distance while
retaining 1× distance on infrastructure. Existing paths can therefore justify
a detour without making reuse mandatory.

Distance must be positive, so predecessor recursion terminates. The value
`(total cost, predecessor x, predecessor y)` gives deterministic lexicographic
tie-breaking. The judge recomputes every live request with an independent
Python Dijkstra.

`accum` is drainage area: the number of upstream cells whose modeled runoff
passes through a cell. It is proportional to discharge only under uniform
rainfall; it is not velocity or finite-time flow. It is both a visible stream
hint and a conservative crossing signal.

## Travel, paths, and construction

A completed cargo journey is frozen cell-by-cell. Later path formation or
construction may reroute future surveys but never rewrites historical travel.

Two journeys establish a desire path. Ordinary town traffic still requires
real cargo. In V3 the mountain courier may make at most two `scout` journeys
per live route, representing a cheap footpath to a new industrial endpoint.
Before a town has a complete public road, a porter can carry at most 5 units
per journey and at most 10 units total to that town. This permits exactly the
two real journeys needed to establish a path but cannot satisfy the town.
Once every cell of the selected route is in the source-connected public
network, bulk road freight can carry the remaining demand.

Construction may pave only established path cells and must advance from the
live source/yard-connected network. A dry, low-drainage cell becomes a surface
road. A wet cell or a cell with drainage area at least 256 on the default
board requires a bridge; the area-scaled threshold is 1024 on the 256 board.
Bridges remain passable while water continues beneath them.

Legacy V2 roads remain transport overlays. New V3 engineered roads obey a
400-permille coarse-grid grade envelope. The client computes the least
fill-only profile for the entire unbuilt alignment, commits one frontier cell,
charges one aggregate per road cell and one rock per elevation-unit-cell of
fill, and feeds the raised bed into the authoritative terrain relation. Water
and drainage then re-equilibrate.

Every route commits to one crossing treatment. A `bridge` alignment preserves
wet/high-runoff cells as bridge overlays. An `embankment` alignment raises such
cells by at least 20 coarse elevation units, consumes rock, and can reroute or
impound drainage. The large number is a deliberately visible raster
abstraction, not a claim about a literal 20-metre road deck. `preview` reports
both profiles before the first build. Once construction begins, the alignment
cannot switch treatment opportunistically.

The V3 bootstrap is intentionally non-fungible. Two five-unit courier loads
activate quarry 20 and establish its approach path. Seventeen valley aggregate
units can reach—but cannot bypass—the quarry. Only after that activated quarry
is connected to a lowland source does its one-time stock of 24 aggregate and
50 rock become available. Worksite 21 accepts 30 bulk-rock units only from the
online quarry over a wholly connected route.

## V4 wide-world observatory

V4 preserves that finished valley and adds site 30, the Piz Nair ridge
observatory at `(55,110)`, elevation 2861 m. The observatory does not seed
connectivity. Agent 1 must establish its approach with two scout journeys,
agent 2 owns every new engineered surface-road cell, and agent 3 owns required
bridges and the final 20-unit rock-foundation haul from the online quarry.
It is operational only when its pad is both fully supplied and connected to
the public network. The judge makes this a required success condition.

The initial material release is deliberately tighter than the geographic
search space: the online quarry provides 65 new aggregate and 60 road-fill
rock units. The calibrated contour approach reuses 111 inherited cells and
needs 53 new road cells, 32 fill units, and no new bridge. A poor alignment can
therefore consume the road or fill margin. Only one route survey may remain
live on the wide board because recursive routes, rather than hydrology, are the
dominant memory cost; alternatives must be compared serially and retired.

The full terrain remains visible in V4. An observatory later revealing shared
terrain is a good thematic payoff, but honest fog requires filtered server and
viewer relations plus route solving restricted to already-known cells. A
visual mask over today's global WebSocket and route oracle would only pretend
that information was hidden.

## V5 trail-only ridge shelter

Once the observatory is operational, V5 adds site 31 at `(213,75)`. It consumes
the final ten units of lowland supply as exactly two five-unit courier loads.
Each journey must start at an ordinary source, remain dry, and keep every edge
within the 800-permille foot-grade limit. Road freight is rejected, and a route
whose target is the trail outpost may not be paved or engineered. The two cargo
journeys themselves establish the desire path; free scouting is unnecessary.

This modal distinction comes from the terrain. The accepted 2,627 m foot route
has a 792.5-permille maximum grade. An engineered counterfactual needs 4,074
fill units and a 252-unit single-cell lift, far outside the remaining stock.
The trail rule recognizes a route humans could walk but should not road-build.

## Survey lifecycle and commands

At most four route surveys may remain live in V2/V3, and one in V4. Live surveys are the expensive
recursive state; frozen journeys and public infrastructure persist after a
survey is retired. Only the survey owner may retire it, including after it has
been used for deliveries. The chronological event replay preserves the route
that each historical journey actually used.

All mutations are serialized by the run lock and closed by one server tick.

```text
python3 interactive/server/demo/dem/pathways_client.py \
  --run-dir RUN --agent N status

# ids plus distance, grade, water, runoff, and reuse coefficients
... --agent N survey ROUTE FROM_SITE TO_SITE DIST GRADE WATER RUNOFF REUSE
... --agent N route ROUTE
... --agent N overlap ROUTE_A ROUTE_B
... --agent N retire ROUTE

# real porter trips form the path
... --agent N deliver TOWN 5 ROUTE
... --agent N deliver TOWN 5 ROUTE

# upgrade a connected prefix, then send the remaining demand as freight
... --agent N pave ROUTE CELL_COUNT
... --agent N deliver TOWN UNITS ROUTE

# V3 hill logistics
... --agent 1 deliver 20 5 QUARRY_ROUTE
... --agent 1 deliver 20 5 QUARRY_ROUTE
... --agent 1 scout WORKSITE_ROUTE
... --agent 1 scout WORKSITE_ROUTE
... --agent N preview ROUTE
... --agent 2 build ROUTE bridge
... --agent 3 build ROUTE bridge       # when the next cell is a bridge
... --agent 2 build ROUTE embankment   # alternative all-road alignment
... --agent 2 build-until ROUTE bridge # repeat atomic cells until a role gate
... --agent 3 deliver 21 30 WORKSITE_ROUTE

# V4 observatory continuation (quarry 20 -> observatory 30)
... --agent N survey ROUTE 20 30 DIST GRADE WATER RUNOFF REUSE
... --agent 1 scout ROUTE
... --agent 1 scout ROUTE
... --agent 2 build-until ROUTE bridge
... --agent 3 build ROUTE bridge       # only if construction reaches a crossing
... --agent 3 deliver 30 20 ROUTE

# V5 trail shelter (source 11 -> shelter 31); cargo forms the trail
... --agent 1 survey ROUTE 11 31 1 12 1000 0 1
... --agent 1 deliver 31 5 ROUTE
... --agent 1 deliver 31 5 ROUTE
... --agent 1 retire ROUTE

# Optional slope-aware survey; maximum edge grade is in permille
... --agent N survey ROUTE FROM TO DIST GRADE WATER RUNOFF REUSE MAX_GRADE
```

V3 makes the handoffs explicit. Agent 1 alone activates the quarry and scouts
footpaths. Agent 2 alone builds engineered surface roads. Agent 3 alone builds
bridges and performs the final bulk-rock haul. Any agent may survey, and all
paths and infrastructure are public. The bridge alignment therefore requires
all three roles; the embankment alternative removes the bridge handoff but
spends substantially more quarry rock and disturbs flow.

A positive optional `MAX_GRADE` is stored separately from the five cost
coefficients. DDIR joins it to the request and excludes any edge for which
`1000 * abs(height change) > MAX_GRADE * step length`. Omitting it preserves
the historical unlimited route search. `route_grade_caps` publishes explicit
inputs, `route_limits` publishes effective limits, and `route_options`
publishes live alternatives with owner, route, cap, predecessor, and cost.

## Authority, persistence, and limitations

DDIR is authoritative for current derived state. The client validates each
sequential construction or delivery action. The judge independently replays
accepted semantic events in order, recomputes the route at each historical
delivery and paving revision, and compares reconstructed routes,
infrastructure, traversals, path counts, delivery modes, connectivity, grants,
and bridge classifications with DDIR's final relations.

On the wide board the recursive frontier joins its route request before
fanning out over eight neighbour edges. This exact join reassociation removes
an 8N intermediate arrangement; recovery over the same 62 commands reproduced
all outputs and improved the 53-build sequence from 645.1 to 536.6 seconds.
The later grade-cap extension retains that join order, applies legality inside
the recursive relaxation, and is checked against the same independent
Dijkstra during both survey acceptance and judgment.

Event appends are flushed and synced, but the server mutation and filesystem
append are not one atomic transaction. A crash in that narrow interval can
still lose the final acknowledged action. For a stopped or crashed world,
`pathways_resume.py` stages one replacement from the saved briefing and
replays accepted commands; program hashes prevent recovery with changed DDIR
semantics for newly created runs.

This version intentionally omits path decay, road maintenance, production
rates, excavation/cut spoil, finite freight capacity, and time-dependent
culvert discharge. Fill is conserved against finite rock stock, but its coarse
height-cell unit is not a physical volume. Porter and road-freight limits are
capacity classes, not a traffic simulation.
