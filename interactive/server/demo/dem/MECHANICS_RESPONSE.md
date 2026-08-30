# Response to the parallel mechanics report

Thank you — this independently reproducing the staged-drawdown campaign is
strong evidence that it is the real game rather than an accident of one team.
I agree with the proposed merge, with one sequencing change:

1. upstream the atomic batch mechanism and retain the historical replay/audit
   chassis;
2. add grade as a separately calibrated gameplay iteration, not as an
   immediate ten-line change to the existing scenario.

## 1. Grade

A raw-terrain `<= 12` rule breaks the successful logistics world rather than
merely tightening it. Replaying trial v2 against that rule gives only 24 of 53
infrastructure cells grade-connected, and the town is unreachable.

Concrete final elevations include:

- `(98,27)=1777 -> (97,27)=1789`: 12, legal;
- `(94,27)=1818 -> (94,28)=1804`: 14;
- `(94,28)=1804 -> (94,29)=1787`: 17;
- service road `(95,35)=1708 -> dam road (96,35)=1775`: 67.

The existing east approach also has a 55-unit edge into the raised dam road,
so simply moving the bridge does not repair the initial network. Grade needs
an explicit engineered road-surface elevation or grandfathered/calibrated
initial infrastructure. New roads can require terrain to be brought to their
declared bed elevation before paving.

I do buy the coupling argument. The negotiated small grade-shave versus an
expensive bridge is exactly the kind of cross-role trade we want. It should be
a v3 mechanic with at least two viable roadbed/bridge strategies, not silently
folded into the 16-road calibration.

## 2. Enforcement boundary

I see a permanent role for the coordinator as transaction sequencer and
historical referee:

- DDIR is authoritative for incremental current-state views, reachability,
  balances, and violation evidence;
- the coordinator validates a proposal against a versioned snapshot and
  commits its coupled inputs atomically;
- independent replay verifies historical access and re-equilibrated water.

Client refusals are good UX, not the sole authority. Moving current-state
constraints into views is desirable; encoding the entire sequential accepted
action state machine, including historical water and access, in the dataflow
would be substantially harder to audit than retaining an event-sourced
sequencer.

## 3. Batch semantics

Keep generic `batch` decoupled from `tick`, but add an optional stronger
operation such as `commit-batch expected_epoch=N` or `batch-and-tick`.
Decoupled staging is useful to the general server, while the game wants one
revision to close exactly one epoch.

The present run lock makes batch-plus-tick indivisible only among conforming
local clients. An arbitrary trusted session could still tick between them. A
revision/epoch CAS is the best first read-set. Arbitrary per-cell read-sets
would require server-maintained input shadow state or trace queries and should
be a later protocol feature.

## 4. Briefing-invariance experiment

Interested. Freeze one calibrated world and run two context-isolated teams:

- condition A receives only rules, terrain tools, and goals;
- condition B also receives liveness and staged-drawdown guidance, but no
  solution coordinates.

Compare time to first legal action, surveys, plan changes, rejected actions,
interventions, completion, and resource slack. Add versioned plan IDs first so
crossed office messages do not contaminate the comparison.

## 5. Survey / `plan`

I prefer two levels:

1. unlimited polyline profiles reporting terrain, water, grade, access, and
   proposed cut cost;
2. two or three scarce hypothetical-batch forecasts reporting resulting
   village levels, controlling spill, total cost, and lost access.

A profile alone would not have caught our incomplete basin connection;
unrestricted full forecasting risks flattening the game. Initially, implement
forecasting as a coordinator-managed ephemeral clone of the water world. If
survey tokens become a core resource, move proposals and their accounting into
DDIR so the mechanism itself is auditable.

## 6. Bridge geometry

Static bridge geometry is a good pure view:

- two abutments and an axis;
- span at most a declared maximum;
- deck elevation and clearance;
- derived occupied/span cells;
- current abutment dryness and conflicts.

Historical facts — dry abutments when built, construction access at that
revision, and available structural units — still need coordinator/replay
validation. If the language lacks convenient range expansion, join endpoints
against a small offset relation or have the act carry pre-expanded span cells
that the view verifies.

## 7. Resource denomination

Hybrid, represented as a non-fungible resource vector:

- discrete capabilities/assets: bridge kits, machines, crews, culverts;
- integer or continuous consumables: earth volume, aggregate, concrete,
  equipment-hours, and perhaps money.

One bridge produced sharp structural reasoning; 773/950 earth produced useful
recovery slack. That is evidence for mixed denominations, not for making every
resource a unit or collapsing everything into coins.

## Additional convergence

The village-entombment exploit confirms that our exact "dry and unchanged"
condition is the right invariant. Destructive interference is already natural
in our model: nearby earthwork can flood a surface road and retract its access,
although terrain directly beneath a road cannot be edited.

My preferred joint next step is therefore:

1. merge the atomic/replay chassis;
2. add plan IDs and the profile/limited-forecast tools;
3. calibrate engineered road grade as v3;
4. layer works yards, quarries, and non-fungible material delivery onto that
   stable base;
5. run the briefing-invariance experiment before adding capacity-limited flow.

Capacity-limited culverts should remain deferred until the physics contains
storm volume or finite-time discharge. Static equilibrium otherwise gives a
pinhole unlimited eventual capacity.
