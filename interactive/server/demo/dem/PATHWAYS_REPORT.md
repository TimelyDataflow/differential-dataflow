# Persistent pathways design report

## Outcome

The pathways experiment turns the Engadine equilibrium terrain into a small
transport world: cargo journeys establish desire paths, paths can become roads
or bridges, and later route surveys value that public infrastructure. The
first complete V2 run fulfilled all four towns and passed independent route,
network, resource, construction-history, and delivery-history checks.

The central mechanism result is narrow but encouraging: **surface reuse is not
flat**. With the same origin, destination, and physical coefficients, reuse
weights 0, 1, and 3 produced materially different geometry. Reuse 1 created a
credible trunk and branch in this comparison; reuse 0 ignored an adjacent
road, while reuse 3 accepted a visible dogleg to remain on it.

This was a bounded, coordinator-directed, role-attributed playtest, not a blind
discovery trial. It demonstrates a productive feedback mechanism, not yet that
the whole game is non-obvious, well-calibrated, or strongly asymmetric.

## Experiment ladder

| Run | Board | Purpose | Result |
|---|---|---|---|
| `pathways-dev-01` | 256 | implementation smoke test | partial |
| `pathways-trial-01` | 256 | V1 three-player world | all four towns fulfilled |
| `pathways-feedback-01` | 256 | V2 raw → path → road feedback | mechanism verified; run interrupted by host reboot |
| `pathways-safe-01` | 128 | lower-memory V2 completion | all four towns fulfilled; authoritative success |

The V1 trial established that DDIR routing, drainage-sensitive bridge
classification, non-transferable construction grants, and public handoffs can
work together. Agents built 321 roads and four bridges and delivered 200 units
to Samedan, Celerina, St. Moritz, and Pontresina. A useful handoff occurred
when one agent's farm-to-Celerina road became another's continuation to
Pontresina.

V1 also exposed two design defects. Agents could create a path with a free
`walk ROUTE 2`, so traffic did not represent useful activity, and live routes
did not value paths or roads. The result was largely independent spokes that
happened to become public after construction.

V2 made three changes:

1. Every delivery freezes the route it actually traversed; there is no free
   walk action.
2. Two porter deliveries establish a path but cannot fulfill a town. Bulk
   freight requires a fully connected road/bridge route.
3. Raw ground, established paths, and infrastructure contribute surface
   factors 2, 1, and 0 to later route costs.

On the 256 feedback world, one fixed alignment changed cost 5,381 → 3,590 →
1,799 as it moved from raw ground to path to road. The later 128 run tested
whether that numerical feedback changed route *geometry* usefully.

## Controlled reuse comparison

Route 920 was first established from the valley farm to St. Moritz. It used 56
cells over 3,773 m, with 235 m total elevation variation, a 20 m maximum step,
and two bridge-threshold crossings. Two 5-unit porter deliveries formed its
path; Agent 1 then built 53 roads and two bridges and delivered the remaining
50 units as freight.

Three new surveys then connected the same farm to nearby Champfer. All used
distance/grade/water/runoff coefficients `(1,4,1000,8)` and differed only in
reuse weight:

| Route | Reuse | Cells | Distance | Elevation variation | Route-920 overlap |
|---|---:|---:|---:|---:|---:|
| 930 | 0 | 67 | 4,334 m | 222 m | 6 scattered cells |
| 931 | 1 | 57 | 3,980 m | 244 m | 46-cell continuous trunk |
| 932 | 3 | 63 | 4,166 m | 274 m | 55 of route 920's 56 cells |

Route 930 ran beside the road for 17 additional cells without entering it.
That is a useful control but visually odd for an infrastructure-aware
traveler. Route 931 reused 46 road cells, then took an 11-cell branch to
Champfer. Route 932 gained nine more trunk cells than 931 but paid six cells,
186 m of travel, and 30 m of elevation variation for the privilege; it nearly
visited St. Moritz before turning toward Champfer.

The players selected 931, retired both comparisons, made two porter trips,
and built only its missing 10 roads and one bridge. This supports the local
claim that reuse 1 was the plausible value for this origin/destination pair.
Because each reuse weight defines a different objective, their total route
costs are not directly comparable; geometry, overlap, grade, and construction
are the relevant evidence. Nothing here establishes reuse 1 as a global
calibration.

## Completing the one world

The northern half produced a second shared network:

| Route | Extension | Continuous public reuse | New construction | Delivery |
|---|---|---:|---:|---:|
| 920 | farm → St. Moritz | initial trunk | 53 roads, 2 bridges | 60 |
| 931 | farm → Champfer | 46 cells of 920 | 10 roads, 1 bridge | 40 |
| 933 | quarry → Samedan | 15 cells of southern network | 43 roads, 1 bridge | 40 |
| 934 | quarry → Bever | 57 cells of 933 | 28 roads | 40 |

Route 933 remained on the same 60-cell alignment as it changed cost 11,571 →
8,887 → 6,203 from raw branch to path to road. Route 934 then reused 57 cells
of 933 continuously—95% of the earlier route—before branching toward Bever.
It added 28 dry roads and reused the existing bridge.

The final authoritative result was:

```text
Samedan 40/40; Bever 40/40; St. Moritz 60/60; Champfer 40/40
Agent 1: 53 roads, 2 bridges
Agent 2: 43 roads, 1 bridge
Agent 3: 38 roads, 1 bridge
134 roads, 4 bridges, 180 cargo delivered
every town: 10 by porter, remainder by road freight
route/network/grant/path/bridge/delivery/history checks: all true
VERDICT: SUCCESS
```

The strengthened judge replays accepted events chronologically. At each
delivery or paving revision it independently recomputes the then-live route,
path use, connectivity, freight mode, bridge requirement, and grant spend,
then compares the reconstructed relations with DDIR's final state. This closes
the earlier final-state-only gap around frozen journeys and construction
order.

## Design-space judgment

Evidence that the mechanism is productive:

- existing infrastructure materially changed later route geometry;
- reuse 0, 1, and 3 were visibly and operationally different;
- the selected alignments accumulated two useful public trunks rather than
  four independent spokes;
- public construction created sequential dependence: later agents built only
  branches and reused bridges owned by earlier agents;
- real cargo journeys, rather than free path-painting, unlocked construction.

Important qualifications:

- the coordinator bootstrapped route 920 and scouts evaluated later routes;
  this was not a blind test of whether agents discover the loop;
- every final route used `(1,4,1000,8,1)`, so the run did not validate the
  three suggested survey profiles;
- all construction grants retained slack, so scarcity did not force a
  negotiation or handoff;
- agents had separate attribution but the same verbs. Their roles were weaker
  than the road/bridge/earth asymmetry in `DESIGN_REPORT.md`;
- supply is pooled, freight is instantaneous, and roads have no capacity or
  maintenance burden;
- two porter trips are better grounded than free walks but remain a coarse,
  procedural threshold.

The supported conclusion is therefore **productive feedback in a bounded
run**. Blind reruns, new layouts, coefficient sweeps, and genuinely distinct
capabilities are needed before making a broader claim about the design space.

## Operational incident and scale boundary

During the 256-board feedback work, macOS rebooted after a userspace-watchdog
panic reported missed WindowServer check-ins under extreme compressor and swap
pressure. Separate memory-pressure reports named `ddir_server` as the largest
process, while three full-grid DDIR worlds were resident and sampled at roughly
17–21 GiB apiece. This strongly implicates aggregate DDIR memory pressure in
starving WindowServer, but it does not isolate a particular server, route, or
dataflow operator as the cause.

The replacement changed three things together: it used the 128 board, kept
exactly one world, and capped live surveys at four. The completed world was
sampled near 5.35 GiB RSS. That is a lower-memory bounded configuration, not a
proof of memory safety or attribution to any single mitigation.

Unused and completed surveys can now be retired without deleting frozen
journeys or infrastructure. New runs also record DDIR program hashes, and the
recovery tool reconstructs sites, grants, geometry, limits, and rules from the
saved briefing before replaying accepted commands into one replacement world.

## Next mechanism: hydraulic roads

The next civil interaction should make the road type affect water rather than
merely respond to it:

- **Embankment road:** cheap bulk fill establishes a road-bed elevation and
  participates in the hydraulic surface. Crossing a drainage line without an
  opening can impound water upstream and threaten a town.
- **Bridge:** the transport deck crosses the cell or edge while the original
  terrain remains in the hydraulic surface. Water passes underneath.
- **Culvert:** a raised road retains a hydraulic outlet at a declared invert.
  In the equilibrium model, outlet size cannot limit eventual discharge. A
  drainage-area rating with washout/failure is an honest explicit engineering
  rule; physical capacity needs storm volume and time.
- **Cut/fill conservation:** road raising should consume material excavated at
  cuts or supplied by a quarry. Spoil placement is then a consequential action
  that can obstruct drainage rather than free disposal.

This creates the desired tension directly: the cheapest straight road may be
a dam, while a bridge, culvert, detour, or balanced earthwork costs scarce
resources but respects the watershed.

## Why go into the mountains?

The current sites are mostly on the valley floor, as real settlements tend to
be, so efficient logistics correctly stays low. Mountain paths need plausible
highland demand or through movement:

- huts and alpine pasture need small recurring food/fuel deliveries;
- avalanche-control, watershed, forestry, communications, and hydropower
  works need workers and material above the towns they protect or serve;
- quarries and reservoirs create highland supply as well as demand;
- an edge pass can represent commerce with the next valley before the full DEM
  is enlarged;
- a larger coarse board can eventually include two valleys and make a pass a
  genuine competing corridor.

Transport modes should react differently to slope. Foot travel can tolerate a
steep direct line but carry little; a pack trail tolerates less; a road carries
bulk freight but needs a low maximum slope and construction. The current
linear grade term minimizes total elevation variation and does not strongly
distinguish a brutal step from a switchback. A hard slope limit or nonlinear
per-edge slope cost is the smallest change likely to produce recognizable
mountain switchbacks.

A focused next playtest would add one hut, one avalanche or watershed worksite,
foot/road slope limits, and recurring cargo. The porter discovers and
establishes a trail; a road engineer reuses its gentler portions and builds
switchbacks; a civil specialist depends on that access to construct the work.
Tunnels, ropeways, seasons, decay, and multiple commodities can wait until that
loop proves interesting.

## Prioritized fixed-point experiments before a larger map

The recursive programs expose several `enter_at` candidates:

| Scope | Dependency | Priority hypothesis |
|---|---|---|
| `water::w` | cyclic depression-fill labels | compare high-first and low-boundary-first elevation schedules; both remain hypotheses until measured |
| `flow::m` | acyclic downhill accumulation | inject high/upstream mass earlier so downstream corrections consolidate |
| `pathways::routes` | positive weighted shortest paths | enter proposals at accumulated route cost, approximating prioritized Dijkstra |
| `snow::s` | cyclic abelian chip-firing | no terrain-informed priority follows from the current rule |
| predecessor/network scopes | acyclic chain or unweighted reachability | lower priority unless a useful rank is added |

The route scope is the most promising scaling target; the water scope is the
closest match to the observed altitude-scheduling idea. Experiments should be
variants, not silent replacements, and must compare exact outputs, cold load,
localized edit latency, iteration activity, and peak/RSS memory. The current
world remains live for visualization, so no second benchmark server was
started alongside it.
