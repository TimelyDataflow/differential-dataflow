import { createCollection, localOnlyCollectionOptions } from '@tanstack/db'

// Wire-protocol mirror collections. Each row maps 1:1 to a logical entity
// derived from the diagnostics dataflow; the WebSocket bridge in `ws.ts`
// translates incoming JSON diffs into mutations on these.

export type Operator = {
  id: number
  name: string
  addr: number[]
}

export type Channel = {
  id: number
  scope_addr: number[]
  source: [number, number]
  target: [number, number]
}

export type StatKind =
  | 'Elapsed'
  | 'Messages'
  | 'ArrangementBatches'
  | 'ArrangementRecords'
  | 'Sharing'
  | 'BatcherRecords'
  | 'BatcherSize'
  | 'BatcherCapacity'
  | 'BatcherAllocations'

// Stats are accumulated counters keyed by (kind, id). The wire protocol
// sends signed diffs; we maintain a running total per key.
export type StatCounter = {
  key: string // `${kind}:${id}`
  kind: StatKind
  id: number
  value: number
}

export const operators = createCollection(
  localOnlyCollectionOptions<Operator, number>({
    id: 'operators',
    getKey: (op) => op.id,
  }),
)

export const channels = createCollection(
  localOnlyCollectionOptions<Channel, number>({
    id: 'channels',
    getKey: (ch) => ch.id,
  }),
)

export const statCounters = createCollection(
  localOnlyCollectionOptions<StatCounter, string>({
    id: 'statCounters',
    getKey: (s) => s.key,
  }),
)

export function statKey(kind: StatKind, id: number): string {
  return `${kind}:${id}`
}

export function clearAll() {
  const opIds = operators.toArray.map((o) => o.id)
  if (opIds.length) operators.delete(opIds)
  const chIds = channels.toArray.map((c) => c.id)
  if (chIds.length) channels.delete(chIds)
  const sKeys = statCounters.toArray.map((s) => s.key)
  if (sKeys.length) statCounters.delete(sKeys)
}
