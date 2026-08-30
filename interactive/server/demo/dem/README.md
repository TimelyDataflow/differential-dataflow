# The Engadine: equilibrium physics on a real DEM, live on the DDIR server

A shared world over real Swiss terrain (the Upper Engadine around St.
Moritz; ~53 m cells from the AWS Open Data terrain tiles, window committed
as `engadin_128.txt`, regenerable by `fetch_dem.py`). Three physics
programs maintain fixed points over it, edits re-derive only their
consequences, and a civil-works protocol lets participants — human or
agent — change the world together.

`GEOGRAPHY.md` maps board coordinates to the real valley (verified
landmarks, georeferenced corners, orientation). A second board,
`engadin_256.txt` (~26 m cells, same reach with Celerina and Pontresina in
frame; `fetch_dem.py engadin_256`), exists for looking closer — the game
and its calibrations stay on `engadin_128.txt`.

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
