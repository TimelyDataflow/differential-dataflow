# Diagnostics console

A React frontend for the diagnostics WebSocket server defined in this
crate. An alternative to the single-file `diagnostics/index.html`; both
consume the same wire format (`Frame` envelopes, one per closed
timestamp).

## Running

Start a diagnostics-instrumented program (e.g.
`cargo run --release --example scc-bench -p diagnostics -- -w4`),
then in this directory:

```
npm install
npm run dev
```

Open the URL Vite prints, click Connect (defaults to
`ws://localhost:51371`).

For a production build:

```
npm run build
```

The output lands in `dist/` and can be served by any static file
server.

## Layout

```
src/
  collections.ts   TanStack DB local-only collections (operators, channels, statCounters)
  ws.ts            WebSocket bridge — applies one Frame as one transaction across collections
  derive.ts        recursive aggregates (transitive messages, descendants, sumElapsed)
  graph/
    buildScopeGraph.ts  pure: derived state + scope id → renderable graph
    layout.ts           async: chain detection + ELK layered layout
  components/
    Overview.tsx   sortable root-dataflow table
    Detail.tsx     scope graph hosting + breadcrumb + filter affordances
    Graph.tsx      effect-driven layout, JSX SVG render
  App.tsx          top-level shell: connect form, tabs, filter, scope stack
```

## Watermark contract

Each WebSocket message is one `Frame` (`{ type, ts_us, updates }`),
emitted by the server only after the dataflow's frontier has advanced
past `ts_us`. The bridge applies each Frame as one TanStack DB
`createTransaction`, so every `useLiveQuery` observer sees a sequence
of consistent closed-timestamp views — never a half-applied frame.
