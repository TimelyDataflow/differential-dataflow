# Handover: state of the civil world (for whoever takes the next shift)

Everything current is on branch `civil`. What it contains, in reading
order: `README.md` (front door), `GEOGRAPHY.md` (verified landmarks and
what resolution changes), `PROTOCOL.md` + `LOGISTICS_PROTOCOL.md` (the two
scenarios), `MECHANICS.md` / `DESIGN_REPORT.md` / `MECHANICS_RESPONSE.md`
(trial history and the agreed direction), `viz/NOTES.md` (viewer usage,
world-side questions now answered in place).

## Live worlds (mini-01, tailnet 100.65.36.17)

| ports (TCP/WS) | board | programs |
|---|---|---|
| 7996 / 7997 | engadin_128, dam staged at 1775 | water, flow, ledger, works, meta |
| 7994 / 7995 | engadin_256, natural equilibrium | water, meta |

Both are driver-owned time (`DDIR_TICK_MS=0`): whoever wants to see an
edit land must `tick`. Known issue for tail clients: a tail opened after
the last tick can stall mid-replay (diagnosis and candidate fix in
`interactive/server/NOTES.md`); a tick or retry unsticks it. The 7996
world predates the `depots` export — restage to pick it up.

## Recently landed

- `engadin_256.txt` (~26 m, Celerina and Pontresina in frame) via
  `fetch_dem.py` presets; the game stays calibrated on `engadin_128.txt`.
- `meta.ddp` — worlds now self-describe cell size, height unit, extent,
  and NW corner; feed it from your staging driver.
- `depots` exported from `works.ddp` and `logistics.ddp`, so clients can
  recompute hypothetical access (viz question 3).
- `viz/viewer.html` — 2D/3D live viewer with a what-if flood brush; its
  Forecast button is the agreed reference implementation to cross-check a
  coordinator-run forecast against, exactly.

## The agreed queue (from MECHANICS_RESPONSE.md, in order)

1. Upstream the atomic `batch` command as its own PR (general protocol
   feature, load-bearing in trial 2).
2. Plan IDs in the site office + survey tools: unlimited polyline
   profiles, two or three scarce hypothetical-batch forecasts.
3. Grade as engineered road-bed elevation, calibrated as a v3 scenario
   (raw-terrain ≤12 breaks the 16-road calibration — measured, 24/53).
4. Works yards, quarries, non-fungible material delivery.
5. The briefing-invariance experiment (two context-isolated teams, one
   taught staged drawdown, one not) — add plan IDs first.

A cheap standalone trial any shift can run: the staged-drawdown scenario
(road budget ~500, teach nothing) to see whether fresh agents discover
campaigning; both prior teams did, independently.

Deferred by agreement: capacity-limited culverts/tunnels until the
physics has time (equilibrium gives any pinhole unlimited discharge).
Candidate mechanic noted while answering viz question 2: an in-world
ledger chain-violation view (`act.old != height at accept`).
