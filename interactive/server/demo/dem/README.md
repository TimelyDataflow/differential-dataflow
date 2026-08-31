# The Engadine: equilibrium physics on a real DEM, live on the DDIR server

A shared world over real Swiss terrain (the Upper Engadine around St.
Moritz; ~53 m cells from the AWS Open Data terrain tiles, window committed
as `engadin_128.txt`, with a wider persistent-game continuation in
`engadin_wide.txt`, both regenerable by `fetch_dem.py`). Three physics
programs maintain fixed points over it, edits re-derive only their
consequences, and a civil-works protocol lets participants — human or
agent — change the world together.

[`WORLD_MODEL.md`](WORLD_MODEL.md) inventories the currently installed V5
world, relation schemas, authority boundaries, and known omissions. It also
defines an evolvable meaning of “contract”: mechanisms, institutional rules,
planning policies, experiments, and presentation can change independently
when their compatibility boundary is explicit.

`GEOGRAPHY.md` maps board coordinates to the real valley (verified
landmarks, georeferenced corners, orientation). A second board,
`engadin_256.txt` (~26 m cells, same reach with Celerina and Pontresina in
frame; `fetch_dem.py engadin_256`), exists for looking closer — the game
and its calibrations stay on `engadin_128.txt`.
The V4 pathways world instead uses `engadin_wide.txt`: a 256×192 crop at the
same coarse scale, preserving every old cell under a `(+72,+16)` transform.

Every driver in this directory cross-checks the server against an
independent Python implementation and asserts exact equality; "it looks
right" is never the standard.

## The physics (`.ddp` programs)

- **`water.ddp`** — equilibrium water: `w = max(t, min over neighbours of
  w)`, the greatest fixed point, iterated down from above (priority-flood
  depression filling). Boundary cells drain. Edits price at their basin:
  dam a gorge and ~1,300 of 16,384 cells re-derive in ~0.5 s.
- **`flow.ddp`** — flow routing over the filled surface (argmin neighbour
  under the strict total order `(surface, x, y)`, hence acyclic) and
  drainage accumulation as a *multiplicity* (one unit of mass per cell,
  advanced one hop per round; `A(c)` is the consolidated diff). The Inn
  shows up as the high-accumulation spine.
- **`snow.ddp`** — snowfall as an abelian sandpile: grains and the
  toppling odometer are multiplicities; each round fires every unstable
  cell once (parallel chip-firing — a legal schedule, so the abelian
  theorem gives THE stable pile). One added grain's avalanche is the
  measured delta.
- **`ledger.ddp`** — the civil-works ledger: attributed terraform acts,
  with per-agent expenditure (`spend` = Σ|Δh|) maintained by the world
  itself. The auditor is a dataflow.

## Quick start

    cargo build -p ddir-server --release
    python3 interactive/server/demo/dem/run_dem.py       # water: dam + notch, timed
    python3 interactive/server/demo/dem/run_physics.py   # + rivers and sandpile

## The civil-works game (for agents!)

`PROTOCOL.md` is the contract: terraform acts are dual-written (world +
ledger), budgets and locked cells bound the game, ticks are open, and only
the world is judged. To stage a scenario:

    python3 interactive/server/demo/dem/civil.py setup   # dam the gorge, flood
                                                         # the village, write
                                                         # briefing.json
    # ... participants play (see below) ...
    python3 interactive/server/demo/dem/civil.py judge   # dry? on budget?
                                                         # audit clean?

Participants act through `cw_client.py` (one process per act, safe to run
concurrently):

    cw_client.py --port 7997 --agent N sync          # once, first
    cw_client.py --port 7997 --agent N around X Y R  # local map, [d] = water depth
    cw_client.py --port 7997 --agent N water         # lake summary
    cw_client.py --port 7997 --agent N edit X Y H    # terraform (costs |H - old|)
    cw_client.py --port 7997 --agent N tick
    cw_client.py --port 7997 --agent N spend         # the auditor's view

### Briefing an agent

Give each agent: its id and budget, the village cells and lake level from
`briefing.json`, the locked-cell rule, the client commands above (with
"keep `around` radius ≤ 5, never dump raw peeks"), a shared site-office
text file for coordination, and the goal ("every village cell shows no
[depth] marker after a tick"). In the first live trial, two agents split a
spillway design one of them priced against a ring levee (696 vs 917
m-cells), corrected each other's mistakes through the site office, and
dried the village at 580 and 575 of their 600 budgets — with the line
"partial cuts do nothing, so we must BOTH finish" appearing in their own
planning notes.

## Civil logistics: asymmetric roads, bridges, and earthworks

`LOGISTICS_PROTOCOL.md` adds a second scenario without changing the baseline
game. A road engineer has scarce track material, a structures engineer owns a
water-passing bridge deck, and an earthworks engineer alone may terraform.
Earthworks require live depot-connected access; flooded surface roads retract
from that access graph; and success requires both a dry, unchanged village and
a short passable service road to town.

[`DESIGN_REPORT.md`](DESIGN_REPORT.md) records two blind three-agent trials: an
audit-clean 800-earth diagnostic failure, the 950-earth balance iteration, and
an authoritative success at road 16/16, bridge 1/1, and earth 773/950. It also
evaluates the design space and lays out the next town/quarry/material-delivery
experiment.

```text
cargo build -p ddir-server --release
python3 interactive/server/demo/dem/logistics_game.py setup \
    --run-dir interactive/server/demo/dem/runs/trial-01 --port 8011

# Give agents the generated briefing, LOGISTICS_PROTOCOL.md, their id, and
# the run's site_office.md. Each acts through logistics_client.py.

python3 interactive/server/demo/dem/logistics_game.py judge \
    --run-dir interactive/server/demo/dem/runs/trial-01
```

The logistics client uses fresh snapshots and atomic server-side feed batches;
one accepted command is one equilibrated revision. The judge independently
replays every revision, priority-floods every terrain state, and cross-checks
DDIR's road reachability, resources, and action audit. Pure rules and the known
feasibility proof run with:

```text
python3 -m unittest \
  interactive.server.demo.dem.test_logistics_rules \
  interactive.server.demo.dem.test_logistics_scenario -v
```

## Persistent pathways: towns, desire paths, and roads

`pathways.ddp` generalizes the one-off civil puzzle into an evolving transport
world. Agents submit weighted topographic surveys, DDIR finds their routes,
completed journeys leave frozen path use, and established paths can be upgraded
into a public road/bridge network that carries cargo from sources to towns.
Terrain, equilibrium water, and `accum` drainage remain the shared physics and
visualization substrate.

[`PATHWAYS_PROTOCOL.md`](PATHWAYS_PROTOCOL.md) defines the route objective and
construction rules. [`PATHWAYS_REPORT.md`](PATHWAYS_REPORT.md) records the V1
and V2 playtests, controlled reuse comparison, completed four-town world,
memory incident, and proposed hydraulic-road/mountain-path iterations.

V4 keeps one interactive world at a time on the 256×192 coarse board. It
freezes the completed V3 network into a hashed genesis snapshot and adds a
required Piz Nair ridge observatory. V5 follows it with a trail-only Muottas
ridge shelter whose two light-supply journeys establish the path. Route fixed
points remain substantially heavier than hydrology, so the wide world permits
one live survey. The current live-style migration command is:

```text
python3 interactive/server/demo/dem/pathways_game.py setup \
  --run-dir interactive/server/demo/dem/runs/pathways-observatory-01 \
  --port 8051 --ws-host HOST --grid engadin_wide.txt --version 4 \
  --migrate-from interactive/server/demo/dem/runs/pathways-hills-01

# after the V4 observatory succeeds and its route is retired
python3 interactive/server/demo/dem/pathways_game.py extend-trail \
  --run-dir interactive/server/demo/dem/runs/pathways-observatory-01
```

For a fresh V2 world on the smaller board:

```text
python3 interactive/server/demo/dem/pathways_game.py setup \
  --run-dir interactive/server/demo/dem/runs/pathways-01 --port 8081

python3 interactive/server/demo/dem/pathways_client.py \
  --run-dir interactive/server/demo/dem/runs/pathways-01 --agent 1 status

python3 interactive/server/demo/dem/pathways_game.py judge \
  --run-dir interactive/server/demo/dem/runs/pathways-01
```

DDIR's route fixed point is checked against `pathways_rules`' heap Dijkstra —
cost *and* geometry, since a grid carries many equal-cost paths and the reuse
mechanism is a claim about which cells a route takes. The judge compares both
for every live route; `check_routes.py` pins the same property on a small
synthetic board (water, drainage, an established path, a road, reuse weights
0/1/3) in a few seconds:

```text
python3 interactive/server/demo/dem/check_routes.py
```

A stopped or crashed V2 world can be rebuilt from its saved briefing and
accepted semantic event log. Recovery stages one replacement, verifies saved
program hashes when present, and publishes the destination only after every
command replays successfully:

```text
python3 interactive/server/demo/dem/pathways_resume.py \
  --from-run interactive/server/demo/dem/runs/pathways-01 \
  --run-dir interactive/server/demo/dem/runs/pathways-01-recovered \
  --port 8091 --ws-host 127.0.0.1
```

The viewer discovers `sites`, `route_path`, `route_options`, `path_use`,
established paths, and the canonical `infrastructure` trace alongside terrain,
water, drainage, and snow. Live `route_options` use exact DDIR predecessors to
draw several maximum-grade proposals as nested owner-coloured lines. The judge
also replays historical route caps, routes, porter, paving, and freight state
rather than trusting final connectivity. Pure route/network/replay rules run
with:

```text
python3 -m unittest interactive.server.demo.dem.test_pathways_rules -v
```
