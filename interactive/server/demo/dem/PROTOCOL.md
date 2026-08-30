# Civil works protocol, v0

Participants share one live world: the `water` program over a real terrain
grid, plus a `ledger` program that makes acts attributable and auditable.
Sessions are trusted to follow this protocol; the *auditor is a dataflow*,
and the driver checks the rest at judgment.

## Identity and acts

Every participant has an agent id (a small integer; 0 is "nature"). The
only world-changing act is **terraform(x, y, h')**, and it is a dual write,
performed atomically by `cw_client.py edit`:

1. the terrain update: retract `((x,y) ; old)`, insert `((x,y) ; h')` into
   the water program's input;
2. the declaration: insert `((x,y) ; (agent, old, h'))` into the ledger.

An undeclared terrain edit is a protocol violation: at judgment the driver
replays `base + Σ(new − old)` from the ledger against the actual terrain,
and any mismatch disqualifies the run. Per-agent expenditure is the
server-maintained view `spend` = Σ |new − old| — earthmoving priced in
meter-cells, computed by the world itself.

## Constraints

- **Locked cells** (listed in the briefing) may not be terraformed by
  anyone but nature. Locked-cell declarations by other agents fail the
  audit.
- **Budget**: each agent's `spend` must not exceed its briefed budget at
  judgment.
- **Ticks are open**: any participant may `tick` to let the world
  equilibrate; consistent views come from ticking before reading.

## Coordination

A shared plain-text *site office* file is provided. Anything may be
written there — plans, requests, divisions of labour — but only ledger
acts change the world, and only the world is judged.

## Judgment

At the deadline the driver ticks once more and reads the equilibrium:
goal predicates (e.g. "every village cell is dry": water == terrain
there), per-agent spend within budget, audit clean. Engineering quality
beyond the predicates (spend efficiency, side effects) is reported but
not scored in v0.
