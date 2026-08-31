# Handover: state of the civil world (for whoever takes the next shift)

Everything current is on branch `civil`. What it contains, in reading
order: `README.md` (front door), `GEOGRAPHY.md` (verified landmarks and
what resolution changes), `PROTOCOL.md` + `LOGISTICS_PROTOCOL.md` (the two
scenarios), `MECHANICS.md` / `DESIGN_REPORT.md` / `MECHANICS_RESPONSE.md`
(trial history and the agreed direction), `viz/NOTES.md` (viewer usage,
world-side questions now answered in place).

The persistent transport generalization is documented separately in
`PATHWAYS_PROTOCOL.md` and `PATHWAYS_REPORT.md`. Read the report for the V1/V2
trial ladder, completed four-town result, qualifications, memory incident, and
proposed hydraulic-road/mountain-path iteration.

## Live pathways world (mini-00)

One lower-memory 128×128 pathways world is live at TCP `8051`, viewer
`ws://100.123.78.70:8052`. It exports terrain, equilibrium water, accumulation,
sites, paths, public roads/bridges, deliveries, and board `meta`. All four towns
are fulfilled; the chronological replay judge returns `SUCCESS`. Keep this as
the sole pathways world while it is being viewed—the prior host reboot occurred
under extreme memory pressure with three 256-board DDIR worlds resident.

## Live worlds (mini-01, tailnet 100.65.36.17)

Both restaged 2026-08-30 evening on the binary that carries the tail fix,
both publishing `meta`:

| ports (TCP/WS) | board | state | staged by |
|---|---|---|---|
| 7996 / 7997 | engadin_128, dam at 1775 | **up** (water, ledger, works, meta) | `civil_roads.py setup --port 7996 --host 0.0.0.0` then `stage_board.py --board engadin_128 --port 7996 --meta-only` |
| 7994 / 7995 | engadin_256, natural equilibrium | **stopped** — restage on demand (~30 s) | `stage_board.py --board engadin_256 --port 7994 --host 0.0.0.0` |

Keep only what you are using: the 256 viewing world held **3.2 GB** RSS
against the 128 game world's 585 MB, and it was stopped to keep this
machine off swap. Water alone on 65,536 cells costs ~50 KB per cell, so a
zoom-13 board over the same window (~262k cells) would want well over
10 GB as the physics stands — memory, not solve time, is what bounds how
fine a board can go today.

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
