# Persistent pathways protocol

This experiment asks whether cargo travel, topographic route choice, desire
paths, road investment, and coarse hydraulic engineering produce a useful
shared-world loop. Version 3 retains the completed valley network and adds a
mountain quarry plus a watershed worksite.

The interactive default is the 128×128 `engadin_128.txt` terrain. Its cells are
approximately 53 m orthogonally and 75 m diagonally. Four settlements demand
cargo; a hillside quarry and valley farm supply it. The 256×256 board remains
available for higher-resolution experiments, but it is substantially more
expensive and is not the default game configuration.

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

The configured step lengths are 53/75 m on the default 128 board and 26/37 m
on the optional 256 board. Surface factor is 2 on raw ground, 1 on an
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

## Survey lifecycle and commands

At most four route surveys may remain live. Live surveys are the expensive
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
```

V3 makes the handoffs explicit. Agent 1 alone activates the quarry and scouts
footpaths. Agent 2 alone builds engineered surface roads. Agent 3 alone builds
bridges and performs the final bulk-rock haul. Any agent may survey, and all
paths and infrastructure are public. The bridge alignment therefore requires
all three roles; the embankment alternative removes the bridge handoff but
spends substantially more quarry rock and disturbs flow.

## Authority, persistence, and limitations

DDIR is authoritative for current derived state. The client validates each
sequential construction or delivery action. The judge independently replays
accepted semantic events in order, recomputes the route at each historical
delivery and paving revision, and compares reconstructed routes,
infrastructure, traversals, path counts, delivery modes, connectivity, grants,
and bridge classifications with DDIR's final relations.

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
