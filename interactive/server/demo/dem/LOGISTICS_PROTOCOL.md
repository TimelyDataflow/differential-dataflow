# Civil logistics protocol, v2

This scenario extends the equilibrium-water game with a live transport
network, scarce role-specific resources, and historical construction access.
It is a cooperative prototype: clients serialize through a run lock and the
server stages every multi-feed action atomically, while the independent judge
replays every revision and rejects protocol violations.

## Roles and non-transferable resources

Each participant has one capability. Resources cannot be pooled or exchanged.

- **Agent 1 — road engineer:** build at most 16 surface-road cells.
- **Agent 2 — structures engineer:** build one bridge cell.
- **Agent 3 — earthworks engineer:** move at most 950 meter-cells of earth.

The asymmetry is intentional. A dry town with no service road fails, a road
network with no outlet fails, and an ordinary road cannot substitute for a
bridge over live water.

## Infrastructure and access

Surface roads and bridges are overlays; neither silently changes terrain.

- A surface road may be built only on a dry cell and must extend the currently
  depot-connected network. If it later floods, it immediately drops out of the
  passable/access graph.
- A bridge also extends the connected network, but it remains passable while
  wet. Because its deck is not inserted into `terrain`, equilibrium water
  continues underneath it.
- Terraforming is legal only next to the currently connected network. Terrain
  directly beneath a connected bridge may be cut, which models excavation
  under a bridge deck. Surface-road terrain may not be edited.
- The nature-built dam and the village footprint are locked against new
  construction.

The incremental `logistics.ddp` dataflow maintains infrastructure,
passability, depot reachability, construction access, spend, balances, and
violation views. Historical legality cannot be inferred from current access,
so the Python judge independently replays revisions in order and recomputes
priority-flood water after each earthwork batch.

## Revisions and atomic staging

One successful participant command is one revision. A revision contains one
agent, one action kind, and one or more cells. Before accepting it, the client:

1. locks the run and reads fresh server state;
2. replays the complete history and the proposed revision;
3. checks role, access, locks, stale heights, duplicates, and resource grant;
4. sends all terrain and audit feeds in one server `batch`;
5. ticks once to publish the revision and re-equilibrate the world.

The server batch is atomic staging at one open epoch: it validates every feed
before applying any and no other session can interleave a tick. It is not a
durable disk transaction. `events.jsonl` records accepted and rejected client
attempts for playtest analysis; DDIR's `actions` trace remains world authority.

## Commands

Every command names its run directory and agent:

```text
python3 logistics_client.py --run-dir RUN --agent N status
python3 logistics_client.py --run-dir RUN --agent N around X Y R
python3 logistics_client.py --run-dir RUN --agent N roads
python3 logistics_client.py --run-dir RUN --agent 1 road X Y [X Y ...]
python3 logistics_client.py --run-dir RUN --agent 2 bridge X Y [X Y ...]
python3 logistics_client.py --run-dir RUN --agent 3 terraform X Y H [X Y H ...]
```

`around` is live and cache-free; radius is capped at 6. `C` marks connected
surface road, `B` bridge, `r` flooded/unconnected road, and `+` a cell adjacent
to the connected network. Every mutating command ticks automatically.

Participants coordinate through the run's append-only `site_office.md`.
Plans should record surveyed alternatives, resource arithmetic, requested
handoffs, acknowledgments, committed revisions, and corrections.

## Judgment

Hard success requires all of the following:

- every village cell is dry and its terrain is unchanged;
- the town has a passable depot route of at most 20 road/bridge hops;
- role, access, lock, revision, cost, and resource replay is clean;
- actual terrain equals replayed terrain;
- actual water equals an independent Python priority flood;
- DDIR infrastructure, connectivity, and spend equal the independent replay;
- the locked nature-built dam is unchanged.

The report also shows resource slack and the passable/total infrastructure
ratio. A useful trial should expose cross-role dependencies and plan changes,
not merely three independent scripts.

## Interpreting playtests

- **Too constrained/obvious:** agents immediately converge on one forced route,
  communication is only completion notices, and every role has an obvious
  script with effectively zero choice.
- **Too flat/ambiguous:** many arbitrary variants tie, scarce resources remain
  irrelevant, and one role's decisions do not change another's plan.
- **Productive:** agents compare crossings/access routes, negotiate at least two
  prerequisites, correct an access or hydraulic mistake, and finish with a
  comprehensible scarce-resource margin.

Version 2 retains the same mechanics as the initial 800-unit trial but gives
earthworks a 950-unit recovery budget. The first blind team spent 689 units on
a plausible outlet before discovering a chain of receding-shore saddles; its
cheapest remaining terrain path cost another 236 units before construction
access. The new grant keeps that mistake barely recoverable while a verified
direct design costs 773 units.

The game intentionally treats a bridge as a raster overlay, not a
finite-capacity hydraulic structure. Culverts and general tunnels require a
later mechanism: equilibrium depression filling has no discharge or duration,
so an unconstrained pinhole would otherwise have infinite effective capacity.
