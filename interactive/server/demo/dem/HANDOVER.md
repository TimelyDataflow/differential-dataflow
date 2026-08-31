# Handover: state of the civil world (for whoever takes the next shift)

Everything current is on branch `civil`. What it contains, in reading
order: `README.md` (front door), `GEOGRAPHY.md` (verified landmarks and
what resolution changes), `PROTOCOL.md` + `LOGISTICS_PROTOCOL.md` (the two
scenarios), `MECHANICS.md` / `DESIGN_REPORT.md` / `MECHANICS_RESPONSE.md`
(trial history and the agreed direction), `viz/NOTES.md` (viewer usage,
world-side questions now answered in place).

## Live worlds (mini-01, tailnet 100.65.36.17)

Both restaged 2026-08-30 evening on the binary that carries the tail fix,
both publishing `meta`:

| ports (TCP/WS) | board | programs | staged by |
|---|---|---|---|
| 7996 / 7997 | engadin_128, dam at 1775 | water, ledger, works, meta | `civil_roads.py setup --port 7996 --host 0.0.0.0` then `stage_board.py --board engadin_128 --port 7996 --meta-only` |
| 7994 / 7995 | engadin_256, natural equilibrium | water, meta | `stage_board.py --board engadin_256 --port 7994 --host 0.0.0.0` |

Both are driver-owned time (`DDIR_TICK_MS=0`): whoever wants to see an
edit land must `tick`. The 7996 world now exports `depots` (4 cells, the
depot block at (76,50)), so hypothetical access is computable client-side.

`stage_board.py` derives every `meta` value from the `fetch_dem.py`
preset, so a re-windowed board cannot drift from what it advertises; it
also verifies the staged equilibrium against an independent priority
flood before leaving the server up.

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
- The tail stall is fixed: a tail's `ok` is now its initial-snapshot
  boundary (server steps the new import through the closed epoch before
  acknowledging), pinned by a regression check in `demo/two_sessions.py`.
  Measured on the 256 world: `tail terrain` delivers all 65,536 rows
  before `ok` in 0.09 s, and a concurrent session's command is not
  delayed — the synchronous catch-up costs no observable fairness at this
  scale. The viewer's reconnect/watchdog are now only for old or remote
  servers.

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
