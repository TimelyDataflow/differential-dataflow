import { createTransaction } from '@tanstack/db'
import {
  channels,
  operators,
  statCounters,
  statKey,
  clearAll,
  type StatKind,
} from './collections'

// Wire-format updates as emitted by `src/server.rs::JsonUpdate`.
type WireOperator = {
  type: 'Operator'
  id: number
  name: string
  addr: number[]
  diff: number
}
type WireChannel = {
  type: 'Channel'
  id: number
  scope_addr: number[]
  source: [number, number]
  target: [number, number]
  diff: number
}
type WireStat = {
  type: 'Stat'
  kind: StatKind
  id: number
  diff: number
}
type WireUpdate = WireOperator | WireChannel | WireStat

// Stats that are owned by an operator id; cleaned up when the operator
// is deleted. (Messages are owned by a channel id; cleaned up there.)
const OPERATOR_STAT_KINDS: StatKind[] = [
  'Elapsed',
  'ArrangementBatches',
  'ArrangementRecords',
  'Sharing',
  'BatcherRecords',
  'BatcherSize',
  'BatcherCapacity',
  'BatcherAllocations',
]

function applyStat(kind: StatKind, id: number, diff: number) {
  const key = statKey(kind, id)
  const existing = statCounters.get(key)
  if (existing == null) {
    if (diff !== 0) {
      statCounters.insert({ key, kind, id, value: diff })
    }
  } else {
    statCounters.update(key, (draft) => {
      draft.value += diff
    })
  }
}

function applyOperator(u: WireOperator) {
  if (u.diff > 0) {
    operators.insert({ id: u.id, name: u.name, addr: u.addr })
  } else if (u.diff < 0) {
    if (operators.has(u.id)) operators.delete(u.id)
    for (const k of OPERATOR_STAT_KINDS) {
      const sk = statKey(k, u.id)
      if (statCounters.has(sk)) statCounters.delete(sk)
    }
  }
}

function applyChannel(u: WireChannel) {
  if (u.diff > 0) {
    channels.insert({
      id: u.id,
      scope_addr: u.scope_addr,
      source: u.source,
      target: u.target,
    })
  } else if (u.diff < 0) {
    if (channels.has(u.id)) channels.delete(u.id)
    const sk = statKey('Messages', u.id)
    if (statCounters.has(sk)) statCounters.delete(sk)
  }
}

// A Frame is one closed-timestamp commit from the server. The server flushes
// the bucket for `ts_us` only after the capture-stream frontier has advanced
// past that timestamp, so every Frame is a transactionally complete view at
// that logical time.
type WireFrame = {
  type: 'Frame'
  ts_us: number
  updates: WireUpdate[]
}

// Apply one Frame as a single TanStack DB transaction across all three
// collections. Two consequences fall out of this:
//   1. A `useLiveQuery` observer never sees a half-applied frame.
//   2. Multiple independent live queries are mutually consistent: they all
//      observe the same sequence of closed timestamps, so cross-collection
//      invariants (e.g. "an operator and its stat counters appear together")
//      hold at every observable point.
export function applyFrame(frame: WireFrame) {
  if (frame.updates.length === 0) return
  const tx = createTransaction({
    mutationFn: async ({ transaction }) => {
      operators.utils.acceptMutations(transaction)
      channels.utils.acceptMutations(transaction)
      statCounters.utils.acceptMutations(transaction)
    },
  })
  tx.mutate(() => {
    for (const u of frame.updates) {
      switch (u.type) {
        case 'Operator':
          applyOperator(u)
          break
        case 'Channel':
          applyChannel(u)
          break
        case 'Stat':
          applyStat(u.kind, u.id, u.diff)
          break
      }
    }
  })
  void tx.commit()
}

export type ConnState =
  | { kind: 'idle' }
  | { kind: 'connecting'; url: string }
  | { kind: 'open'; url: string; updates: number }
  | { kind: 'closed'; updates: number }
  | { kind: 'error'; message: string }

export type WsConn = {
  close: () => void
}

export function connect(
  url: string,
  onState: (s: ConnState) => void,
  onError: (msg: string) => void,
): WsConn {
  clearAll()
  let updates = 0
  onState({ kind: 'connecting', url })

  const ws = new WebSocket(url)

  ws.addEventListener('open', () => {
    onState({ kind: 'open', url, updates })
  })

  ws.addEventListener('message', (event) => {
    try {
      const data = JSON.parse(event.data as string)
      if (data && data.type === 'Frame') {
        const frame = data as WireFrame
        applyFrame(frame)
        updates += frame.updates.length
        onState({ kind: 'open', url, updates })
      }
    } catch {
      // ignore parse errors — tolerate malformed frames
    }
  })

  ws.addEventListener('close', () => {
    onState({ kind: 'closed', updates })
  })

  ws.addEventListener('error', () => {
    onError(`WebSocket error connecting to ${url}`)
    onState({ kind: 'error', message: `Connection error: ${url}` })
  })

  return { close: () => ws.close() }
}
