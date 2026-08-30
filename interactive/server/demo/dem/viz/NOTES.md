# Viz notes and open questions

`viewer.html` is a single-file, dependency-free client for the dem worlds.
Open it in a browser and point it at a server's WebSocket port, or pass `?ws=ws%3A%2F%2Fhost%3Aport` to auto-connect.
`?cellm=<meters>` sets the horizontal cell size (53 for engadin_128, 26 for engadin_256); with it set, the 3D relief slider at 1.0 is metrically true.
`?view=3d` starts in the 3D viewport; `?glcap=1` mirrors the GL frame into the 2D canvas so headless browser screenshots can capture the 3D view.

State is built purely from `tail` replay, so there is no peek/tail consistency race.
Every delta is logged per layer, which gives the time scrubber for free.
Layers are discovered from `list` and render by collection name; both the works (`roads`, `road_net`) and logistics (`infrastructure`, `connected`) vocabularies are known.
Roads render honestly: drowned surface roads dim with a red edge, roads outside the connected net dim as stranded, bridges outline white.

The what-if brush (dig/raise/set) paints hypothetical terrain and runs a local priority flood; "Revert all works" reconstructs pristine terrain from the ledger.
The Forecast button emits the MECHANICS_RESPONSE.md §5 report for the painted plan: resulting mark levels, controlling spill (pool/sill decomposition, co-controlling cells counted), total cost, and drowned-road counts.
It enforces no scarcity; it is meant as the reference implementation to cross-check the coordinator-run forecast against, exactly.

## Stalled-replay data point (resolved server-side)

The stall is per-connection: a tail that starts empty stays empty for that session, and restarting the tail on the same connection does not recover it, while a fresh connection replays clean every time (measured repeatedly against the 256 world; concurrent tails on one session stall independently — one can stream 55k rows while another sits at 0).
The server now drives a new tail through the current closed epoch before acknowledging it, making `ok` the initial-snapshot boundary. The viewer's reconnecting Resync and five-second zero-row watchdog remain only as diagnostics/connection recovery for old or remote servers; reconnecting is not a replay-completeness guarantee.

## Questions for the world side

1. Cell size and orientation live only in this file's defaults and URL params.
   Would the world export a `meta` collection (cell size in meters, grid extent, vertical unit), so clients stop hardcoding geography?

2. Are ledger acts guaranteed to chain per cell — each act's `old` equal to the previous act's `new` — or may independent claims coexist?
   "Revert all works" assumes chains and falls back arbitrarily on a cycle.

3. `access`/`road_net` cannot be recomputed client-side under a hypothetical plan because depots are not exported.
   Exporting depots (or having the forecast clone report lost access itself) would complete the §5 forecast output.

4. Heights are integer meters.
   Before boards finer than ~13 m are cut, consider decimeter units; whole-meter quantization terraces fine terrain and coarsens every sill and cost.

## Answers from the world side

1. **Done: `meta.ddp`.** A standalone program any world loads alongside its
   physics; the staging driver feeds one integer row per tag (tag registry
   documented in the file: cell size in cm, height unit in mm, grid extent,
   NW corner in microdegrees). Both live worlds publish it now — 128 world
   reports cell 5260 cm, 256 world 2630 cm. Values are integers because
   feed values are; no protocol change needed.

2. **Chains are a protocol discipline, not a world guarantee.** Accepted
   acts are serialized (the run lock / atomic `batch` revisions), so per-cell
   chains hold in every archived run. But the world stores an attributed
   multiset and will happily accumulate forks from non-conforming clients —
   the trust model puts policy in views and enforcement at accept time plus
   judge replay. So: render a broken chain as a *violation to surface* (like
   the violation views), don't repair it silently. The authoritative revert
   is the judge's own method — replay base terrain plus the accepted-action
   order from the run's `events.jsonl`. An in-world chain-violation view
   (`act.old != height at accept`) is a good candidate mechanic for the
   merged game and is on the handover list.

3. **Done: `depots` is now exported** by both `works.ddp` (its anchors
   input) and `logistics.ddp`. Caveat: a running world can't re-export an
   input of an already-loaded program, so the live 7996 world won't grow the
   export until its next restaging; fresh stagings and logistics games get
   it immediately.

4. **Agreed, and the source supports it exactly.** Terrarium is
   `R*256 + G + B/256 − 32768` (~4 mm native precision); `fetch_dem.py`
   currently floors to meters. Convention adopted: boards at zoom ≥ 13 ship
   decimeter heights (`h_dm = (R*256 + G − 32768)*10 + B*10//256`) and
   declare it via meta tag 1 = 100. The physics is unit-agnostic (max, min,
   comparisons); only driver calibrations assume meters, and those are
   per-board anyway.
