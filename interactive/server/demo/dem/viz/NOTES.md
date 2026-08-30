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
