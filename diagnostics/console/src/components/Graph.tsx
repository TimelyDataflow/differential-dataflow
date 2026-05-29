import { useEffect, useMemo, useRef, useState } from 'react'
import type { Derived } from '../derive'
import { buildScopeGraph, L } from '../graph/buildScopeGraph'
import type {
  EdgeRoute,
  LaidNode,
  Layout,
} from '../graph/layout'
import { computeLayout } from '../graph/layout'

// Render the laid-out scope graph as JSX SVG. Layout runs in an effect
// (async, ELK is non-trivial); the latest result wins if the inputs
// change while a previous run is still in flight.
export function Graph({
  derived,
  scopeId,
  hideInactive,
  onPushScope,
  filterActive,
  filteredIds,
  compact,
}: {
  derived: Derived
  scopeId: number
  hideInactive: boolean
  onPushScope: (opId: number) => void
  filterActive: boolean
  filteredIds: Set<number>
  compact: boolean
}) {
  const graph = useMemo(
    () => buildScopeGraph(derived, scopeId, hideInactive, filterActive, filteredIds, compact),
    [derived, scopeId, hideInactive, filterActive, filteredIds, compact],
  )
  const [layout, setLayout] = useState<Layout | null>(null)
  const [error, setError] = useState<string | null>(null)
  const seqRef = useRef(0)

  useEffect(() => {
    if (!graph) {
      setLayout(null)
      return
    }
    const seq = ++seqRef.current
    setError(null)
    computeLayout(graph)
      .then((l) => {
        if (seq === seqRef.current) setLayout(l)
      })
      .catch((err) => {
        if (seq === seqRef.current) {
          setError(String(err))
          setLayout(null)
        }
      })
  }, [graph])

  if (!graph) return <div className="no-data">No operators to display</div>
  if (error) return <div className="no-data">Layout error: {error}</div>
  if (!layout) return <div className="no-data">Laying out…</div>

  return (
    <SvgGraph
      layout={layout}
      onPushScope={onPushScope}
      filterActive={filterActive}
      filteredIds={filteredIds}
    />
  )
}

function SvgGraph({
  layout,
  onPushScope,
  filterActive,
  filteredIds,
}: {
  layout: Layout
  onPushScope: (opId: number) => void
  filterActive: boolean
  filteredIds: Set<number>
}) {
  const fbColors = useMemo(() => {
    const s = new Set<string>()
    for (const r of layout.edgeRoutes) {
      if (r.feedbackColor) s.add(r.feedbackColor)
    }
    return [...s]
  }, [layout])

  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={layout.width}
      height={layout.height}
      style={{ display: 'block', maxWidth: 'none' }}
    >
      <defs>
        <ArrowMarker id="arrow" color="#888" />
        <ArrowMarker id="arrow-back" color="#c44" />
        <ArrowMarker id="arrow-dim" color="#555" />
        {fbColors.map((c) => (
          <ArrowMarker
            key={c}
            id={'arrow-fb-' + c.replace('#', '')}
            color={c}
          />
        ))}
      </defs>
      <g>
        <g className="edges">
          {layout.edgeRoutes.map((r, i) => (
            <EdgePath key={i} route={r} />
          ))}
        </g>
        <g className="nodes">
          {layout.nodes.map((n) => {
            const dim =
              filterActive &&
              n.opId != null &&
              !filteredIds.has(n.opId)
            return (
              <NodeShape
                key={n.id}
                node={n}
                onPushScope={onPushScope}
                dim={dim}
              />
            )
          })}
        </g>
      </g>
    </svg>
  )
}

function ArrowMarker({ id, color }: { id: string; color: string }) {
  return (
    <marker
      id={id}
      viewBox="0 0 10 10"
      refX="10"
      refY="5"
      markerWidth="7"
      markerHeight="7"
      orient="auto-start-reverse"
    >
      <path d="M0,1 L10,5 L0,9z" fill={color} />
    </marker>
  )
}

function EdgePath({ route }: { route: EdgeRoute }) {
  if (route.points.length < 2) return null
  const pts = route.points
  let d = `M${pts[0].x},${pts[0].y}`
  if (pts.length === 2) {
    const cy = Math.abs(pts[1].y - pts[0].y) * 0.3
    d += ` C${pts[0].x},${pts[0].y + cy} ${pts[1].x},${pts[1].y - cy} ${pts[1].x},${pts[1].y}`
  } else {
    for (let k = 1; k < pts.length; k++) d += ` L${pts[k].x},${pts[k].y}`
  }
  const sent = route.sent || 0
  const fb = route.feedbackColor
  const color = fb ? fb : sent === 0 ? '#555' : '#888'
  const marker = fb
    ? `url(#arrow-fb-${fb.replace('#', '')})`
    : sent === 0
    ? 'url(#arrow-dim)'
    : 'url(#arrow)'
  return (
    <path
      d={d}
      fill="none"
      stroke={color}
      strokeWidth={fb ? 2 : 1.2}
      strokeDasharray={sent === 0 && !fb ? '4,3' : undefined}
      markerEnd={marker}
    >
      {route.label && <title>{route.label}</title>}
    </path>
  )
}

function NodeShape({
  node,
  onPushScope,
  dim,
}: {
  node: LaidNode
  onPushScope: (opId: number) => void
  dim: boolean
}) {
  const t = `translate(${node.x},${node.y})`
  // If the node is filter-shrunk (thin band, no label), the thinness already
  // carries the "deemphasized" signal; full opacity keeps it visible at a
  // glance. Otherwise dim drops it to ~20% as before.
  const shrunk = !node.isPort && !node.isScope && node.h < 20
  const opacity = dim ? (shrunk ? 1 : 0.2) : 1
  if (node.isScope) {
    return (
      <ScopeShape
        node={node}
        transform={t}
        onPushScope={onPushScope}
        opacity={opacity}
      />
    )
  }
  return <FlatShape node={node} transform={t} opacity={opacity} />
}

function ScopeShape({
  node,
  transform,
  onPushScope,
  opacity,
}: {
  node: LaidNode
  transform: string
  onPushScope: (opId: number) => void
  opacity: number
}) {
  const [hover, setHover] = useState(false)
  const w = node.w
  const h = node.h
  const peak = 8
  const points = `0,${peak} ${w / 2},0 ${w},${peak} ${w},${h} 0,${h}`
  const hasSubtitle = node.subtitle != null
  const nameY = hasSubtitle ? h / 2 - 4 : h / 2 + 2
  return (
    <g
      transform={transform}
      style={{ cursor: 'pointer' }}
      opacity={opacity}
      onClick={(e) => {
        e.stopPropagation()
        if (node.opId != null) onPushScope(node.opId)
      }}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
    >
      <polygon
        points={points}
        fill={node.fill}
        stroke={hover ? '#fff' : node.stroke}
        strokeWidth={hover ? 2.5 : 1.5}
      />
      <text
        x={w / 2}
        y={nameY}
        fill={node.textFill}
        fontSize={11}
        fontFamily="monospace"
        textAnchor="middle"
        dominantBaseline="middle"
        pointerEvents="none"
      >
        {node.label}
      </text>
      {hasSubtitle && (
        <text
          x={w / 2}
          y={nameY + 14}
          fill={node.textFill}
          opacity={0.7}
          fontSize={9}
          fontFamily="monospace"
          textAnchor="middle"
          dominantBaseline="middle"
          pointerEvents="none"
        >
          {node.subtitle}
        </text>
      )}
      <text
        x={w - 8}
        y={h / 2 + 2}
        fill={node.textFill}
        opacity={0.5}
        fontSize={10}
        fontFamily="monospace"
        textAnchor="middle"
        dominantBaseline="middle"
        pointerEvents="none"
      >
        ▶
      </text>
    </g>
  )
}

function FlatShape({
  node,
  transform,
  opacity,
}: {
  node: LaidNode
  transform: string
  opacity: number
}) {
  const hasSubtitle = node.subtitle != null && !node.isPort
  const nameY = hasSubtitle ? node.h / 2 - 6 : node.h / 2
  // Filter-shrunk nodes get only the rect — no label fits in a thin band.
  const shrunk = !node.isPort && !node.isScope && node.h < 20
  return (
    <g transform={transform} opacity={opacity}>
      <rect
        x={0}
        y={0}
        width={node.w}
        height={node.h}
        rx={node.isPort ? node.h / 2 : L.rx}
        ry={node.isPort ? node.h / 2 : L.rx}
        fill={node.fill}
        stroke={node.stroke}
        strokeWidth={1}
      />
      {!shrunk && (
        <text
          x={node.w / 2}
          y={nameY}
          fill={node.textFill}
          fontSize={node.isPort ? 9 : 11}
          fontFamily="monospace"
          textAnchor="middle"
          dominantBaseline="middle"
          pointerEvents="none"
        >
          {node.label}
        </text>
      )}
      {hasSubtitle && (
        <text
          x={node.w / 2}
          y={nameY + 14}
          fill={node.textFill}
          opacity={0.7}
          fontSize={9}
          fontFamily="monospace"
          textAnchor="middle"
          dominantBaseline="middle"
          pointerEvents="none"
        >
          {node.subtitle}
        </text>
      )}
    </g>
  )
}
