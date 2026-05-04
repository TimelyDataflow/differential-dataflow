import ELK from 'elkjs/lib/elk.bundled.js'
import type { GraphNode, ScopeGraph } from './buildScopeGraph'

const elk = new ELK()
const STACK_GAP = 1

export type Point = { x: number; y: number }

export type LaidNode = GraphNode & { x: number; y: number }

export type EdgeRoute = {
  points: Point[]
  isBack: boolean
  label: string
  sent: number
  feedbackColor?: string
}

export type Layout = {
  nodes: LaidNode[]
  edgeRoutes: EdgeRoute[]
  width: number
  height: number
}

// Detect 1-in-1-out chains so we can stack them vertically into a single
// ELK supernode — keeps the layered output compact instead of sprawling.
function detectChains(graph: ScopeGraph): {
  chains: string[][]
  nodeChain: Record<string, number>
} {
  const { nodes, edges, nodeMap } = graph
  const outCount: Record<string, number> = {}
  const inCount: Record<string, number> = {}
  const outTarget: Record<string, string | null> = {}
  nodes.forEach((n) => {
    outCount[n.id] = 0
    inCount[n.id] = 0
  })
  edges.forEach((e) => {
    outCount[e.from] = (outCount[e.from] ?? 0) + 1
    inCount[e.to] = (inCount[e.to] ?? 0) + 1
    outTarget[e.from] = outCount[e.from] === 1 ? e.to : null
  })

  const visited = new Set<string>()
  const chains: string[][] = []
  const nodeChain: Record<string, number> = {}

  for (const n of nodes) {
    if (visited.has(n.id)) continue
    if (n.isScope || n.isPort) {
      visited.add(n.id)
      continue
    }
    const chain = [n.id]
    visited.add(n.id)
    let cur = n.id
    while (true) {
      const next = outTarget[cur]
      if (!next || visited.has(next)) break
      const nn = nodeMap[next]
      if (!nn || nn.isScope || nn.isPort) break
      if (outCount[cur] !== 1 || inCount[next] !== 1) break
      chain.push(next)
      visited.add(next)
      cur = next
    }
    if (chain.length > 1) {
      const ci = chains.length
      chains.push(chain)
      chain.forEach((id) => {
        nodeChain[id] = ci
      })
    }
  }
  return { chains, nodeChain }
}

// Run ELK on a scope graph, returning laid-out nodes and edge polylines.
// Replaces `computeLayout` from index.html, minus inline scope expansion.
export async function computeLayout(graph: ScopeGraph): Promise<Layout> {
  const { nodes, edges, nodeMap } = graph
  const { chains, nodeChain } = detectChains(graph)

  const nodeOutPorts: Record<string, Set<string>> = {}
  const nodeInPorts: Record<string, Set<string>> = {}
  edges.forEach((e) => {
    if (!nodeOutPorts[e.from]) nodeOutPorts[e.from] = new Set()
    if (!nodeInPorts[e.to]) nodeInPorts[e.to] = new Set()
    nodeOutPorts[e.from].add(e.fromPort || '0')
    nodeInPorts[e.to].add(e.toPort || '0')
  })

  function makeElkPorts(nodeId: string) {
    const ports: any[] = []
    for (const p of [...(nodeInPorts[nodeId] ?? new Set())].sort()) {
      ports.push({
        id: nodeId + '_in_' + p,
        layoutOptions: { 'elk.port.side': 'NORTH', 'elk.port.index': p },
      })
    }
    for (const p of [...(nodeOutPorts[nodeId] ?? new Set())].sort()) {
      ports.push({
        id: nodeId + '_out_' + p,
        layoutOptions: { 'elk.port.side': 'SOUTH', 'elk.port.index': p },
      })
    }
    return ports
  }

  const inChain = new Set<string>()
  chains.forEach((ch) => ch.forEach((id) => inChain.add(id)))

  const elkNodes: any[] = []

  chains.forEach((chain, ci) => {
    const stackW = Math.max(...chain.map((id) => nodeMap[id].w))
    const stackH =
      chain.reduce((s, id) => s + nodeMap[id].h, 0) +
      (chain.length - 1) * STACK_GAP
    const chainId = '__chain_' + ci
    const firstId = chain[0]
    const lastId = chain[chain.length - 1]
    const ports: any[] = []
    for (const p of [...(nodeInPorts[firstId] ?? new Set())].sort()) {
      ports.push({
        id: chainId + '_in_' + p,
        layoutOptions: { 'elk.port.side': 'NORTH', 'elk.port.index': p },
      })
    }
    for (const p of [...(nodeOutPorts[lastId] ?? new Set())].sort()) {
      ports.push({
        id: chainId + '_out_' + p,
        layoutOptions: { 'elk.port.side': 'SOUTH', 'elk.port.index': p },
      })
    }
    elkNodes.push({
      id: chainId,
      width: stackW,
      height: stackH,
      ports,
      layoutOptions: {
        'elk.algorithm': 'fixed',
        'elk.padding': '[top=0,left=0,bottom=0,right=0]',
        'elk.portConstraints': 'FIXED_SIDE',
      },
      children: chain.map((id, k) => {
        const n = nodeMap[id]
        const prevH = chain
          .slice(0, k)
          .reduce((s, pid) => s + nodeMap[pid].h + STACK_GAP, 0)
        return {
          id,
          width: n.w,
          height: n.h,
          x: (stackW - n.w) / 2,
          y: prevH,
        }
      }),
    })
  })

  for (const n of nodes) {
    if (inChain.has(n.id)) continue
    const elkNode: any = {
      id: n.id,
      width: n.w,
      height: n.h,
      ports: makeElkPorts(n.id),
      layoutOptions: { 'elk.portConstraints': 'FIXED_SIDE' } as Record<
        string,
        string
      >,
    }
    if (n.isPort) {
      elkNode.layoutOptions[
        'org.eclipse.elk.layered.layering.layerConstraint'
      ] = n.portDir === 'in' ? 'FIRST' : 'LAST'
    }
    elkNodes.push(elkNode)
  }

  function elkNodeId(id: string): string {
    return nodeChain[id] !== undefined ? '__chain_' + nodeChain[id] : id
  }
  function elkSourcePort(e: { from: string; fromPort: string }) {
    return elkNodeId(e.from) + '_out_' + (e.fromPort || '0')
  }
  function elkTargetPort(e: { to: string; toPort: string }) {
    return elkNodeId(e.to) + '_in_' + (e.toPort || '0')
  }

  const elkEdges: any[] = []
  const elkEdgeOrigIdx: number[] = []
  edges.forEach((e, i) => {
    const fromElk = elkNodeId(e.from)
    const toElk = elkNodeId(e.to)
    if (fromElk === toElk) return
    elkEdges.push({
      id: 'e_' + i,
      sources: [elkSourcePort(e)],
      targets: [elkTargetPort(e)],
      layoutOptions: {},
    })
    elkEdgeOrigIdx.push(i)
  })

  const elkGraph = {
    id: 'root',
    layoutOptions: {
      'elk.algorithm': 'layered',
      'elk.direction': 'DOWN',
      'elk.layered.spacing.nodeNodeBetweenLayers': '40',
      'elk.spacing.nodeNode': '20',
      'elk.spacing.edgeNode': '15',
      'elk.spacing.edgeEdge': '10',
      'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
      'elk.layered.nodePlacement.strategy': 'BRANDES_KOEPF',
      'elk.padding': '[top=20,left=20,bottom=20,right=20]',
      'elk.layered.feedbackEdges': 'true',
    },
    children: elkNodes,
    edges: elkEdges,
  }

  const laid: any = await elk.layout(elkGraph as any)
  const elkResultMap: Record<string, any> = {}
  ;(laid.children ?? []).forEach((en: any) => {
    elkResultMap[en.id] = en
  })

  const laidNodes: LaidNode[] = nodes.map((n) => {
    if (inChain.has(n.id)) {
      const compound = elkResultMap['__chain_' + nodeChain[n.id]]
      if (compound && compound.children) {
        const child = compound.children.find((c: any) => c.id === n.id)
        if (child) {
          return { ...n, x: compound.x + child.x, y: compound.y + child.y }
        }
      }
    } else {
      const en = elkResultMap[n.id]
      if (en) return { ...n, x: en.x, y: en.y }
    }
    return { ...n, x: 0, y: 0 }
  })

  const edgeRoutes: EdgeRoute[] = []
  ;(laid.edges ?? []).forEach((ee: any, ei: number) => {
    const origIdx = elkEdgeOrigIdx[ei]
    const origEdge = origIdx !== undefined ? edges[origIdx] : null
    const route: EdgeRoute = {
      points: [],
      isBack: false,
      label: origEdge ? origEdge.label : '',
      sent: origEdge ? origEdge.sent : 0,
      feedbackColor: origEdge?.feedbackColor,
    }
    if (ee.sections) {
      for (const sec of ee.sections) {
        route.points.push(sec.startPoint)
        if (sec.bendPoints) route.points.push(...sec.bendPoints)
        route.points.push(sec.endPoint)
      }
    }
    if (route.points.length >= 2) {
      const first = route.points[0]
      const last = route.points[route.points.length - 1]
      if (last.y < first.y) route.isBack = true
    }
    edgeRoutes.push(route)
  })

  return {
    nodes: laidNodes,
    edgeRoutes,
    width: laid.width || 400,
    height: laid.height || 300,
  }
}
