import type { Channel, Operator, StatCounter } from './collections'

// Derived view of a single operator, combining the raw `Operator` row with
// accumulated stats and structural facts (is_scope, is_arrangement).
export type OperatorView = Operator & {
  is_scope: boolean
  is_arrangement: boolean
  elapsed_ns: number
  arr_batches: number
  arr_records: number
  arr_sharing: number
  output_records: number
}

export type ChannelView = Channel & {
  records_sent: number
}

export type Derived = {
  operators: Map<number, OperatorView>
  channels: ChannelView[]
  addrToId: Map<string, number>
  childrenOf: Map<string, number[]>
  rootIds: number[]
  // Sums over the operator and its descendants.
  transitiveMessages: Map<number, number>
  descendantCount: Map<number, number>
  sumElapsed: Map<number, number>
}

export function addrKey(addr: readonly number[]): string {
  return addr.join(',')
}

export function addrParent(addr: readonly number[]): number[] {
  return addr.slice(0, -1)
}

// Derive everything view-side from the three base collections in one pass.
// This replaces `rebuildState()` + `computeTransitiveMessages()` from
// `index.html`. Recursive aggregates can't be expressed as a single
// TanStack DB query, so we recompute them imperatively over the live
// query outputs and rely on React memoization for change detection.
export function derive(
  operatorRows: Operator[],
  channelRows: Channel[],
  statRows: StatCounter[],
): Derived {
  // Bucket stats by (kind, id). Lookup is by id for operator-owned kinds
  // and by id for Messages (which is keyed on channel id).
  const elapsed = new Map<number, number>()
  const arrBatches = new Map<number, number>()
  const arrRecords = new Map<number, number>()
  const arrSharing = new Map<number, number>()
  const messages = new Map<number, number>()
  for (const s of statRows) {
    switch (s.kind) {
      case 'Elapsed':
        elapsed.set(s.id, s.value)
        break
      case 'ArrangementBatches':
        arrBatches.set(s.id, s.value)
        break
      case 'ArrangementRecords':
        arrRecords.set(s.id, s.value)
        break
      case 'Sharing':
        arrSharing.set(s.id, s.value)
        break
      case 'Messages':
        messages.set(s.id, s.value)
        break
      default:
        break
    }
  }

  const addrToId = new Map<string, number>()
  for (const op of operatorRows) addrToId.set(addrKey(op.addr), op.id)

  // childrenOf is keyed by parent addrKey.
  const childrenOf = new Map<string, number[]>()
  for (const op of operatorRows) {
    if (op.addr.length > 1) {
      const pk = addrKey(addrParent(op.addr))
      const list = childrenOf.get(pk)
      if (list) list.push(op.id)
      else childrenOf.set(pk, [op.id])
    }
  }

  const parentAddrs = new Set(childrenOf.keys())

  // First pass: build OperatorView entries (output_records filled below).
  const ops = new Map<number, OperatorView>()
  for (const op of operatorRows) {
    const batches = arrBatches.get(op.id) ?? 0
    const records = arrRecords.get(op.id) ?? 0
    const sharing = arrSharing.get(op.id) ?? 0
    ops.set(op.id, {
      ...op,
      is_scope: parentAddrs.has(addrKey(op.addr)),
      is_arrangement: batches > 0 || records > 0 || sharing > 0,
      elapsed_ns: elapsed.get(op.id) ?? 0,
      arr_batches: batches,
      arr_records: records,
      arr_sharing: sharing,
      output_records: 0,
    })
  }

  // Channels with accumulated record counts.
  const chans: ChannelView[] = channelRows.map((ch) => ({
    ...ch,
    records_sent: messages.get(ch.id) ?? 0,
  }))

  // Per-operator output_records by walking channels (skip scope ingress, source[0] === 0).
  for (const ch of chans) {
    if (ch.source[0] === 0) continue
    const srcAddr = [...ch.scope_addr, ch.source[0]]
    const srcId = addrToId.get(addrKey(srcAddr))
    if (srcId != null) {
      const o = ops.get(srcId)
      if (o) o.output_records += ch.records_sent
    }
  }

  const rootIds: number[] = []
  for (const op of ops.values()) {
    if (op.addr.length === 1) rootIds.push(op.id)
  }

  // Channels grouped by their containing scope addr — used for transitive sums.
  const channelsByScope = new Map<string, number>()
  for (const ch of chans) {
    const k = addrKey(ch.scope_addr)
    channelsByScope.set(k, (channelsByScope.get(k) ?? 0) + ch.records_sent)
  }

  const transitiveMessages = new Map<number, number>()
  const descendantCount = new Map<number, number>()
  const sumElapsed = new Map<number, number>()

  function visit(id: number) {
    if (transitiveMessages.has(id)) return
    const op = ops.get(id)
    if (!op) {
      transitiveMessages.set(id, 0)
      descendantCount.set(id, 0)
      sumElapsed.set(id, 0)
      return
    }
    let msgs = channelsByScope.get(addrKey(op.addr)) ?? 0
    let count = 1
    let elapsedTotal = op.elapsed_ns
    const kids = childrenOf.get(addrKey(op.addr)) ?? []
    for (const kid of kids) {
      visit(kid)
      msgs += transitiveMessages.get(kid) ?? 0
      count += descendantCount.get(kid) ?? 0
      elapsedTotal += sumElapsed.get(kid) ?? 0
    }
    transitiveMessages.set(id, msgs)
    descendantCount.set(id, count)
    sumElapsed.set(id, elapsedTotal)
  }
  for (const id of ops.keys()) visit(id)

  return {
    operators: ops,
    channels: chans,
    addrToId,
    childrenOf,
    rootIds,
    transitiveMessages,
    descendantCount,
    sumElapsed,
  }
}

export function formatNs(ns: number): string {
  if (!ns || ns <= 0) return ''
  const us = ns / 1e3
  if (us < 1000) return `${us.toFixed(0)}us`
  const ms = ns / 1e6
  if (ms < 1000) return `${ms.toFixed(1)}ms`
  const s = ns / 1e9
  if (s < 60) return `${s.toFixed(2)}s`
  const m = Math.floor(s / 60)
  const rs = (s % 60).toFixed(1)
  return `${m}m${rs}s`
}
