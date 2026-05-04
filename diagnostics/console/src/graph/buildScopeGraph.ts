import type { Derived } from '../derive'
import { addrKey, formatNs } from '../derive'

// A node in the visual scope graph (operator, scope, port, or feedback split).
// Layout fills in (x, y) once ELK runs. Width/height are determined here.
export type GraphNode = {
  id: string
  opId?: number
  label: string
  subtitle: string | null
  isScope: boolean
  isPort: boolean
  portDir?: 'in' | 'out'
  isFeedbackSrc?: boolean
  isFeedbackSink?: boolean
  feedbackColor?: string
  fill: string
  textFill: string
  stroke: string
  w: number
  h: number
}

export type GraphEdge = {
  from: string
  to: string
  label: string
  sent: number
  fromPort: string
  toPort: string
  feedbackColor?: string
}

export type ScopeGraph = {
  nodes: GraphNode[]
  edges: GraphEdge[]
  nodeMap: Record<string, GraphNode>
}

// Layout dimension constants. Kept identical to the original index.html.
export const L = {
  nodeW: 220,
  nodeH: 36,
  portW: 80,
  portH: 26,
  rx: 5,
}

const FEEDBACK_PALETTE = [
  '#e06c75',
  '#61afef',
  '#e5c07b',
  '#c678dd',
  '#56b6c2',
  '#d19a66',
  '#98c379',
  '#be5046',
]

// Height of a filter-shrunk node: a thin band so ELK reclaims the
// vertical real estate while keeping the graph topology intact.
const SHRUNK_H = 12

// Pure function: derive a renderable graph for a given scope from the
// already-derived state. Replaces `buildScopeGraph` in index.html.
export function buildScopeGraph(
  derived: Derived,
  scopeId: number,
  hideInactive: boolean,
  filterActive: boolean,
  filteredIds: Set<number>,
  compact: boolean,
): ScopeGraph | null {
  const scope = derived.operators.get(scopeId)
  if (!scope) return null
  const scopeAddrK = addrKey(scope.addr)
  const kids = derived.childrenOf.get(scopeAddrK) ?? []
  if (kids.length === 0) return null

  const nodes: GraphNode[] = []
  const nodeMap: Record<string, GraphNode> = {}

  for (const kid of kids) {
    const op = derived.operators.get(kid)
    if (!op) continue

    let fill: string
    let textFill: string
    if (op.is_scope) {
      fill = '#12b886'
      textFill = '#fff'
    } else if (op.is_arrangement) {
      fill = '#4a3a6a'
      textFill = '#ddf'
    } else {
      fill = '#2a2a3a'
      textFill = '#ccc'
    }

    let label = op.name
    if (label.length > 28) label = label.slice(0, 25) + '…'

    const subParts: string[] = []
    if (op.is_arrangement) {
      subParts.push(op.arr_records.toLocaleString() + ' updates')
    }
    if (op.elapsed_ns > 0) subParts.push(formatNs(op.elapsed_ns))
    const subtitle = subParts.length > 0 ? subParts.join('  ·  ') : null

    const id = (op.is_scope ? 'scope_' : 'op_') + kid
    // Filter-shrink: non-scope nodes that don't match the active filter
    // collapse to a thin band, dropping subtitle so ELK lays them out as
    // a flat row. Scopes always keep full size — they're navigational.
    // Gated by `compact`: off by default so dimming preserves spatial
    // stability across filter changes; toggle on when actively searching.
    const shrunk =
      filterActive && compact && !op.is_scope && !filteredIds.has(kid)
    const effectiveSubtitle = shrunk ? null : subtitle
    const effectiveH = shrunk
      ? SHRUNK_H
      : effectiveSubtitle
      ? L.nodeH + 16
      : L.nodeH
    const node: GraphNode = {
      id,
      opId: kid,
      label,
      subtitle: effectiveSubtitle,
      isScope: op.is_scope,
      isPort: false,
      fill,
      textFill,
      stroke: op.is_scope
        ? '#fff3'
        : op.is_arrangement
        ? '#7950f2'
        : '#5558',
      w: L.nodeW,
      h: effectiveH,
    }
    nodes.push(node)
    nodeMap[id] = node
  }

  // Channels at this scope. Resolves source/target endpoints to either
  // an existing child node or a synthetic boundary port.
  const scopeChannels = derived.channels.filter(
    (ch) => addrKey(ch.scope_addr) === scopeAddrK,
  )
  const edges: GraphEdge[] = []

  for (const ch of scopeChannels) {
    let fromId: string
    let toId: string

    if (ch.source[0] === 0) {
      const pid = 'port_in_' + ch.source[1]
      if (!nodeMap[pid]) {
        const n: GraphNode = {
          id: pid,
          label: 'in ' + ch.source[1],
          subtitle: null,
          isScope: false,
          isPort: true,
          portDir: 'in',
          fill: '#4a4a6a',
          textFill: '#ccc',
          stroke: '#6668',
          w: L.portW,
          h: L.portH,
        }
        nodes.push(n)
        nodeMap[pid] = n
      }
      fromId = pid
    } else {
      const childAddr = [...scope.addr, ch.source[0]]
      const childOpId = derived.addrToId.get(addrKey(childAddr))
      if (childOpId == null) continue
      const childOp = derived.operators.get(childOpId)
      if (!childOp) continue
      fromId = (childOp.is_scope ? 'scope_' : 'op_') + childOpId
    }

    if (ch.target[0] === 0) {
      const pid = 'port_out_' + ch.target[1]
      if (!nodeMap[pid]) {
        const n: GraphNode = {
          id: pid,
          label: 'out ' + ch.target[1],
          subtitle: null,
          isScope: false,
          isPort: true,
          portDir: 'out',
          fill: '#4a4a6a',
          textFill: '#ccc',
          stroke: '#6668',
          w: L.portW,
          h: L.portH,
        }
        nodes.push(n)
        nodeMap[pid] = n
      }
      toId = pid
    } else {
      const childAddr = [...scope.addr, ch.target[0]]
      const childOpId = derived.addrToId.get(addrKey(childAddr))
      if (childOpId == null) continue
      const childOp = derived.operators.get(childOpId)
      if (!childOp) continue
      toId = (childOp.is_scope ? 'scope_' : 'op_') + childOpId
    }

    if (!nodeMap[fromId] || !nodeMap[toId]) continue

    let label = ''
    if (ch.records_sent > 0) {
      label = ch.records_sent.toLocaleString() + ' records'
    }

    edges.push({
      from: fromId,
      to: toId,
      label,
      sent: ch.records_sent,
      fromPort: String(ch.source[1]),
      toPort: String(ch.target[1]),
    })
  }

  // Split Feedback/Variable nodes into source/sink pair so layered layout
  // doesn't treat them as a single back-edge target.
  const feedbackIds = nodes
    .filter(
      (n) =>
        !n.isPort &&
        !n.isScope &&
        n.label &&
        (n.label.toLowerCase().includes('feedback') ||
          n.label.toLowerCase().includes('variable')),
    )
    .map((n) => n.id)

  for (let fi = 0; fi < feedbackIds.length; fi++) {
    const fbId = feedbackIds[fi]
    const orig = nodeMap[fbId]
    const srcId = fbId + '__src'
    const sinkId = fbId + '__sink'
    const fbColor = FEEDBACK_PALETTE[fi % FEEDBACK_PALETTE.length]
    const op = orig.opId != null ? derived.operators.get(orig.opId) : undefined
    const localIdx =
      op && op.addr.length > 0 ? op.addr[op.addr.length - 1] : '?'
    const tag = orig.label + ' [' + localIdx + ']'

    const srcNode: GraphNode = {
      ...orig,
      id: srcId,
      label: tag + ' ▼',
      isFeedbackSrc: true,
      fill: fbColor,
      textFill: '#000',
      stroke: fbColor,
      feedbackColor: fbColor,
    }
    const sinkNode: GraphNode = {
      ...orig,
      id: sinkId,
      label: tag + ' ▲',
      isFeedbackSink: true,
      fill: fbColor,
      textFill: '#000',
      stroke: fbColor,
      feedbackColor: fbColor,
    }
    nodes.push(srcNode, sinkNode)
    nodeMap[srcId] = srcNode
    nodeMap[sinkId] = sinkNode

    for (const e of edges) {
      if (e.from === fbId) e.from = srcId
      if (e.to === fbId) {
        e.to = sinkId
        e.feedbackColor = fbColor
      }
    }
    const idx = nodes.indexOf(orig)
    if (idx >= 0) nodes.splice(idx, 1)
    delete nodeMap[fbId]
  }

  if (hideInactive) {
    const activeEdges = edges.filter((e) => e.sent > 0)
    const connected = new Set<string>()
    activeEdges.forEach((e) => {
      connected.add(e.from)
      connected.add(e.to)
    })
    const activeNodes = nodes.filter(
      (n) =>
        connected.has(n.id) ||
        n.isScope ||
        (n.opId != null && (derived.transitiveMessages.get(n.opId) ?? 0) > 0),
    )
    const activeNodeMap: Record<string, GraphNode> = {}
    activeNodes.forEach((n) => {
      activeNodeMap[n.id] = n
    })
    const finalEdges = activeEdges.filter(
      (e) => activeNodeMap[e.from] && activeNodeMap[e.to],
    )
    return { nodes: activeNodes, edges: finalEdges, nodeMap: activeNodeMap }
  }

  return { nodes, edges, nodeMap }
}
