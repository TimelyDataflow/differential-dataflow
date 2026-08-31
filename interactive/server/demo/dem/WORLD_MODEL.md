# Civil world model and evolution contract

Status: draft inventory, 2026-08-31. This records the current persistent V5
world and proposes vocabulary for changing it without confusing a model law,
a game rule, a planning choice, and a visual effect. It is deliberately not a
promise that today's grid, tuple layouts, or mechanics will never change.

The implementation remains the authority for the current run. This document
is the human-readable map of that implementation and the compatibility target
for future work. When they disagree, the discrepancy is a bug in the document
until a versioned change explicitly adopts new semantics.

## What “contract” means here

A contract states the meaning, units, update behavior, and authority of an
interface for one declared version. It is not a claim that the interface is a
law of nature, or that it can never evolve.

Five categories keep unlike things from silently hardening into one ruleset:

| Category | Meaning | Current example |
|---|---|---|
| Observation/state | A measured, imported, or historically committed fact in this world | DEM elevation; a completed journey; a built bridge |
| Model mechanism | A deterministic approximation used to derive consequences | priority-flood water; lexicographic flow; path formation after two journeys |
| Institutional rule | A constraint imposed by the scenario or its referee | only agent 3 builds bridges; porter capacity is five units |
| Planning policy | A replaceable choice made while searching or operating | route coefficients; a 300‰ survey cap; bridge versus embankment |
| Experiment/presentation | A treatment under evaluation or a way of observing it | the three grade trials; viewer colors; a future LOD pyramid |

“Physics” in this repository means an exact fixed point of a stated model, not
a claim of physical fidelity. For example, the water relation is exactly the
priority flood specified below, while that priority flood omits rainfall rate,
infiltration, velocity, and elapsed time.

Compatibility has three useful strengths:

- **Exact compatibility:** the same inputs and event history produce the same
  rows. Engine optimizations and recovery should normally meet this bar.
- **Semantic compatibility:** concepts and physical units remain meaningful,
  but representation changes. A finer grid can preserve “elevation in metres”
  without preserving cell keys or the exact filled surface.
- **Declared evolution:** a mechanism or policy changes intentionally. Its
  version, migration boundary, and changed expectations are recorded.

Changing resolution is therefore not inherently a contract violation. It is a
representation migration and often a model change: narrow gorges appear,
one-cell structures change width, and boundary drainage can change. Programs
that import raw cell keys are grid-coupled and must be re-evaluated. Programs
that depend only on physical distance, grade, or area may be semantically
portable, but still require conformance tests on the new board.

## Current persistent world

The current V5 run uses `engadin_wide.txt`, a 256×192 crop of the Upper
Engadine. Its cells are approximately 53 m orthogonally and 75 m diagonally.
Integer terrain heights represent metres. `(0,0)` is the northwest corner;
`x` increases east and `y` increases south.

The board is a bounded model domain. Its edge is an open drain, so enlarging or
moving the board can change equilibrium water even when overlapping terrain
heights are identical. The real-data provenance and known geographic
artifacts are recorded in [`GEOGRAPHY.md`](GEOGRAPHY.md).

Four programs are installed in the live persistent server:

| Program | Inputs/imports | Role |
|---|---|---|
| `water` | mutable terrain input | publishes terrain and equilibrium water |
| `flow` | imports `water` | publishes downstream directions and drainage area |
| `pathways` | imports `terrain`, `water`, and `accum`; has game-event inputs | maintains routes, travel history, infrastructure, connectivity, and materials |
| `meta` | board metadata input | publishes scale, dimensions, and georeference tags |

`snow.ddp` and `ledger.ddp` are real demonstrations in this directory but are
not installed in the persistent V5 world. In particular, visible snow is not
currently coupled to elevation, temperature, meltwater, traffic, or
avalanches. The viewer's ability to display a `pile` trace does not make snow a
world mechanic.

## Dependency and action boundaries

The maintained dependencies are:

```text
terrain ──→ equilibrium water ──→ flow direction ──→ drainage accumulation
   │              │                                      │
   └──────────────┴──────────────────────────────────────┤
                                                         ↓
sites + route request + travel history + infrastructure ─→ route proposal
                           │                    │
                           ↓                    ↓
                    established paths     passable network
                                                │
deliveries ─→ quarry activation ────────────────┤
                                                ↓
                                        material availability
```

Several apparent feedback loops cross an explicit action boundary rather than
co-iterating instantaneously:

- A route does not create traffic. An accepted journey freezes that route's
  cells into traversal history.
- Traversal history can establish a path; the path changes the cost of future
  routes but never rewrites an old journey.
- A route does not build itself. An accepted construction revision adds
  infrastructure and may raise terrain.
- Raised terrain changes water and drainage; those changes can alter road
  passability and future routes after the same server tick settles.
- A delivery can activate a quarry; material is released only after the quarry
  is also connected to the public network.

This distinction matters for composition. A feedback that represents elapsed
time should cross an epoch or action boundary. A feedback intended to describe
one simultaneous equilibrium belongs in one coupled fixed point.

## Spatial and physical mechanisms

### Terrain

`terrain ((x,y); height_m)` is functional: each present cell has one integer
elevation. The initial values come from a committed DEM. Accepted engineered
fill can replace a value by atomically retracting the old height and inserting
the new one.

The persistent pathways game currently performs fill-only road profiling. It
does not conserve excavated soil, model borrow pits, represent road width, or
derive erosion. Earlier civil-work clients permit direct cuts and fills, but
that is a different scenario interface.

### Equilibrium water

`water ((x,y); level_m)` is the greatest fixed point

```text
w(c) = max(terrain(c), min(w(n) for four-neighbours n))
```

with domain-boundary cells pinned to their terrain height. This is equivalent
to a boundary-draining priority flood. Consequences include:

- `water >= terrain` at every cell;
- wet depth is `water - terrain`;
- depressions fill to their lowest modeled escape;
- equilibrium is instantaneous and has unlimited discharge capacity;
- there is no rainfall input, storage through time, infiltration, evaporation,
  velocity, pressure, or finite-capacity culvert.

A narrow opening has infinite long-run drainage capacity in this model. Adding
storm duration or culvert capacity is not a parameter tweak; it introduces a
new time-dependent mechanism.

### Flow and drainage accumulation

`flow ((x,y); (next_x,next_y))` chooses at most one four-neighbour whose
`(water level,x,y)` tuple is strictly smaller, choosing the minimum such tuple.
The strict total order makes the graph acyclic and makes flat lakes drain
deterministically by coordinate. That coordinate preference is a tie-break,
not physical in-lake routing.

`accum ((x,y); upstream_cells)` injects one unit at every cell and advances it
down `flow`. It is inclusive drainage area measured in cells. Under uniform
unit runoff it is proportional to discharge; otherwise it is neither water
volume, velocity, nor a finite-time hydrograph.

The route objective caps its runoff feature at 100 cells. The coarse pathways
board requires a bridge at accumulation 256 or above. The optional 26 m board
uses 1024 so the threshold represents approximately the same contributing
area. Any proposal that treats raw accumulation counts as resolution-neutral
is invalid outside its declared board.

### Snow

The optional `snow.ddp` model is an abelian four-neighbour sandpile. A cell
with four grains topples one grain to each neighbour; grains leaving the board
disappear. It exactly models that cellular automaton and nothing yet connects
its grains to metres of snow, altitude, temperature, rain, water, roads, or
terrain erosion. Snowfall and snowmelt therefore remain candidate mechanics,
not part of the persistent-world contract.

## Movement and construction mechanisms

### Route proposals

Routes use eight-neighbour movement. Each request supplies non-negative
integer weights for distance, absolute elevation change, water depth, drainage
accumulation, and surface reuse. Entering a destination cell costs:

```text
distance_weight × step_length_m
+ grade_weight × abs(height_change_m)
+ water_weight × water_depth_m
+ runoff_weight × min(upstream_cells, 100)
+ reuse_weight × step_length_m × surface_factor
```

Surface factor is 2 on raw ground, 1 on an established path, and 0 on a dry
road or bridge. Distance must have positive weight. `(cost, predecessor_x,
predecessor_y)` gives deterministic tie-breaking.

Despite its historical name, `grade_weight` prices absolute elevation change,
not normalized slope. The optional edge cap below is the current slope-aware
mechanism.

An optional maximum edge grade is expressed in permille. An edge is legal iff

```text
1000 × abs(height_change_m) <= max_grade_permille × step_length_m
```

The five cost weights and grade cap are planning policy supplied by a request;
they are not environmental facts. DDIR maintains the least-cost fixed point
and exact predecessor chain for every live request.

### Travel and path formation

An accepted journey records every traversed cell under an immutable trip id.
`path_use` is the number of frozen journeys through a cell. At two journeys,
the cell becomes an established path and receives the lower route surface
factor. There is currently no decay, seasonal damage, capacity, congestion,
or distinction among traffic weights.

The threshold of two is a game mechanism. The history-free implication
“frequent use can make future travel easier” is a candidate durable concept;
the exact threshold and cell representation are experimental calibration.

### Roads, bridges, and connectivity

Infrastructure kinds are legacy surface road, bridge, and engineered surface
road. A surface road is passable only while its terrain cell is dry. A bridge
is always passable and leaves terrain and drainage unchanged. Bridges do not
currently have span, abutment, deck-height, load, or capacity geometry.

The public network is the least reachability closure from ordinary sources and
works yards over passable eight-neighbour infrastructure. An activated quarry
does not seed connectivity. Engineered-road edges obey the configured 400‰
grade envelope; bridges exempt their incident edges, and inherited legacy
roads retain their historical ungraded connectivity.

New construction advances from the connected frontier over an established
path. Its fill-only profile is the least profile satisfying the grade envelope
against fixed connected boundaries. A road consumes one aggregate unit per
cell and one rock unit per positive metre-cell of fill. These denominations
are intentionally coarse and not resolution-neutral.

A wet cell or a cell above the drainage threshold requires a bridge under the
ordinary treatment. An embankment treatment instead adds at least 20 integer
height units at such crossings, then adds any approach fill required by grade.
The resulting terrain change genuinely recomputes water and accumulation, but
the 20-unit rule is a visible raster experiment rather than a literal design
standard.

## Sites, resources, and institutional rules

Site kinds currently mean:

| Kind | Meaning | Amount field |
|---:|---|---|
| 0 | town | cargo demand |
| 1 | ordinary source | cargo supply |
| 2 | works yard | scenario-defined capacity |
| 3 | quarry | light cargo required for activation |
| 4 | watershed worksite | bulk-rock demand |
| 5 | observatory | foundation-rock demand |
| 6 | trail-only shelter | light-supply demand |

The overloaded amount field is current representation, not a desirable
general schema. A future contract should distinguish inventories, demands,
activation requirements, and construction capabilities explicitly.

The current role split, grants, porter quotas, trail-only restriction, site
locations, demands, 400‰ road grade, 800‰ shelter travel grade, drainage
thresholds, and material stocks are institutional or experimental rules. They
are not consequences of the DEM. Some encode plausible engineering concerns;
their exact numbers remain scenario calibration.

## Time, authority, and enforcement

The server uses discrete epochs. A client atomically stages the rows of one
accepted semantic action, then `tick` advances inputs and runs every installed
dataflow to quiescence. There is no physical duration associated with a tick.

Authority is distributed deliberately:

| Concern | Current authority |
|---|---|
| Live base and derived relations | installed DDIR programs |
| Accepted command history | append-only `events.jsonl` plus migrated genesis snapshot |
| Scenario parameters and program identity | `briefing.json`, grid hash, and program hashes |
| Command legality, roles, quotas, and construction sequencing | Python client/coordinator at acceptance |
| Historical and final audit | independent Python replay and exact DDIR comparison |
| Presentation | viewer only; never authoritative |

DDIR derives consequences but does not currently prevent every invalid direct
feed. Participants act through the client, and the judge independently replays
the semantic event log. Calling DDIR “the source of truth” therefore means it
is authoritative for maintained world state under accepted inputs—not that the
server is presently a hardened, permissioned transaction referee.

Live route requests recompute after relevant changes. Historical traversals
and deliveries do not. V4 migrated older completed state as a hashed genesis
snapshot rather than replaying old choices on a larger graph.

## Current public relation inventory

DDIR notation below is `relation ((key); (value))`. Unit-valued relations omit
their value. This is an inventory of current exports, not yet a promise that
every tuple is a permanent public API.

### Board and hydrology

| Relation | Shape | Meaning |
|---|---|---|
| `meta` | `((tag); value)` | cell size, height unit, dimensions, northwest coordinate |
| `terrain` | `((x,y); height_m)` | current elevation |
| `water` | `((x,y); level_m)` | equilibrium filled-surface level |
| `flow` | `((x,y); (next_x,next_y))` | deterministic downstream neighbour |
| `accum` | `((x,y); upstream_cells)` | inclusive drainage area |

### Sites, travel, and routes

| Relation | Shape | Meaning |
|---|---|---|
| `sites` | `((x,y); (site_id,kind))` | spatial site identity |
| `towns`, `supply`, `yards` | `((x,y); (site_id,amount))` | role-specific site projections |
| `demand` | `((x,y); (0,amount))` | town-demand visualization projection |
| `route_requests` | `((route_id); request)` | live endpoints, owner, and five weights |
| `route_grade_caps` | `((route_id); cap_permille)` | explicitly supplied caps |
| `route_limits` | `((route_id); cap_permille)` | effective cap, including the unlimited default |
| `route_cost` | `((route_id); total_cost)` | target cost for a resolved route |
| `route_steps` | `((route_id,x,y); (cost,pred_x,pred_y))` | chosen predecessor chain |
| `route_path` | `((x,y); route_id)` | spatial projection of live paths |
| `route_options` | `((x,y); (agent,route,cap,pred_x,pred_y,cost))` | inspectable capped alternatives |
| `traversals` | `((trip_id,x,y); (agent,route_id))` | immutable journey geometry |
| `path_use` | `((x,y); count)` | historical journey count |
| `established_paths` | `((x,y))` | cells with at least two journeys |
| `surface_factor` | `((x,y); factor)` | minimum current raw/path/road route factor |

### Infrastructure, delivery, and materials

| Relation | Shape | Meaning |
|---|---|---|
| `infrastructure` | `((x,y); (kind,owner))` | inherited and newly built roads/bridges |
| `passable` | `((x,y))` | currently usable infrastructure |
| `connected` | `((x,y))` | source/yard-connected network closure |
| `deliveries` | `((delivery_id); (agent,target,units,route,mode))` | immutable cargo event |
| `delivered` | `((target_id); units)` | aggregate delivered quantity |
| `served_towns`, `fulfilled_towns` | site projections | network reach and demand completion |
| `build_actions` | `((revision,item); action)` | attributed construction history |
| `engineered_fill` | `((x,y); revision details)` | positive-fill provenance |
| `aggregate_spend`, `fill_spend` | `((0); units)` | current engineered-road material use |
| `activated_quarries`, `online_quarries` | quarry projections | activation and activation-plus-connectivity |
| `material_balance` | `((material_kind); units)` | aggregate and rock remaining |

The program source is the complete tuple-level definition. A future schema
catalog should make these layouts, units, cardinality, and stability available
through server introspection rather than relying on this table and comments.

## Program artifacts and composition

An installed DDIR dataflow and its source text are complementary artifacts:

- Source records meaning, can be reviewed or modified, and can be reinstalled
  after a restart.
- An installed dataflow continuously maintains current results and lets later
  programs import its exported traces without a client-side data dump.
- A durable intermediate data product should include both: versioned source
  plus a manifest and hash, and namespaced live exports while it is active.

Composition depends on the dependency shape:

- For an acyclic dependency such as `water → flow → route risk`, installed
  views are the natural composition. Each consumer imports its producer's live
  relation, and the server shares the maintained trace.
- Independent programs can import the same facts and publish competing or
  complementary products without combining source.
- A time-delayed feedback should use an explicit epoch boundary. The server
  has a one-tick export-to-input binding primitive for this state-machine
  interpretation.
- A true same-epoch mutual equilibrium must co-iterate in one coupled fixed
  point. Today that generally requires a compound DDIR program and source-level
  integration; separately installed programs cannot form an instantaneous
  cyclic import graph. A reusable module or named-rule composition mechanism
  would make this better than copy/paste.

Before coupling two simulations, their temporal claim must be explicit. In the
current world, traffic consults water continuously, while traffic affects
water only through an accepted construction action. That is an acyclic live
dependency plus an action-mediated feedback, not a simultaneous
traffic–hydrology equilibrium.

The current run recovery installs only the four core programs and replays
recognized semantic events. Arbitrary agent-installed programs are neither
archived nor recovered. Export names also share one global registry. A future
“recoverable, namespaced proposal” means recording a program's source/hash,
imports, exports, parameters, and installation order under collision-free
names, then reinstalling it after the core world. It does not mean that the
proposal is automatically allowed to mutate the world.

## Candidate contract for valuable intermediate products

An agent-produced relation becomes reusable when another agent can understand
its scope without reading the author's session transcript. A candidate product
manifest should state:

- source hash and program version;
- imported relations and their required contract versions;
- exported relation schemas, units, and cardinality;
- whether output is a fact, estimate, candidate, constraint, or score;
- parameter domain and scenario assumptions;
- update semantics: instantaneous fixed point, one-tick state, or immutable
  historical result;
- witnesses or violations that make results inspectable;
- conformance examples and known failure cases;
- expected resource envelope.

Use by another installed program measures the value of the maintained product.
Reuse or modification of its source measures the value of the logic. Both are
meaningful and should be recorded separately.

## Evolution rules for concurrent work

The following boundaries are intended to let specialists proceed without
surprising one another:

1. **Mechanics publish versioned meanings.** A new rainfall model may replace
   or supplement `accum`, but must not silently reinterpret upstream-cell count
   as discharge.
2. **Performance preserves declared semantics.** Scheduling, arrangement, and
   storage changes replay the same scenario and compare exact rows unless a
   weaker compatibility level was declared in advance.
3. **Usability consumes real workloads.** Authoring improvements are evaluated
   on programs that import the world and publish reusable products, not only on
   syntax examples.
4. **Visualization observes public relations.** LOD, windowed subscriptions,
   and rendering may evolve without becoming hidden game authority. If
   visibility becomes a rule, the server must publish an authoritative
   visibility relation.
5. **Scenario calibration is data.** Grants, thresholds, demands, sites, and
   evaluation ensembles are versioned separately from the general mechanisms
   they exercise.
6. **Raw spatial programs declare resolution assumptions.** Cell counts,
   one-cell widths, and literal coordinates are not portable merely because a
   relation name is unchanged.

A patch-level engine change should leave current rows exact. An additive
export or optional parameter can be a minor contract change. A new grid,
changed unit, altered fixed point, or changed enforcement rule creates a
declared scenario or contract version with migration and conformance evidence.

## Important omissions and open model choices

The current persistent world does not yet model:

- rainfall amount, spatial variation, duration, or storms;
- snow accumulation tied to altitude, temperature, melt, or rain transition;
- finite-time water transport, channel capacity, culvert capacity, or floods
  caused by discharge exceeding capacity;
- erosion, sediment, soil, excavation sources, or cut/fill conservation;
- road width, pavement structure, wear, maintenance, congestion, or vehicle
  classes;
- bridge span, abutments, deck grade, load, damage, or construction time;
- recurring production and consumption at towns;
- uncertainty ensembles or withheld evaluation scenarios;
- authoritative fog of war or spatial access control;
- a durable lifecycle and resource accounting for agent-authored programs.

These are not all priorities. The inventory exists so a new mechanism is
introduced knowingly and classified as an observation, model law,
institutional rule, policy, or experiment before it becomes a dependency for
others.

## Conformance obligations

The current implementation already checks several exact claims against
independent Python implementations:

- water equals a boundary-draining priority flood;
- flow accumulation equals topological accumulation under the lexicographic
  downstream rule;
- every live DDIR route agrees with heap Dijkstra in cost and geometry;
- chronological replay agrees on terrain, water, accumulation, paths,
  infrastructure, delivery, connectivity, roles, and materials.

Future contract tests should add schema/unit validation, program-import
examples, resolution-aware fixtures, and lifecycle recovery for installed data
products. A performance result is useful only after the relevant semantic
checks pass; a usability result is useful only after an unfamiliar author can
produce and explain a relation that another program can consume.
