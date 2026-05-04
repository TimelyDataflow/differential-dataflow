import { useState } from 'react'
import type { Derived } from '../derive'
import { addrKey, formatNs } from '../derive'
import { Graph } from './Graph'

export function Detail({
  derived,
  dataflowId,
  scopeStack,
  setScopeStack,
  filterActive,
  filteredIds,
  compact,
}: {
  derived: Derived
  dataflowId: number | null
  scopeStack: number[][]
  setScopeStack: (s: number[][]) => void
  filterActive: boolean
  filteredIds: Set<number>
  compact: boolean
}) {
  const [hideInactive, setHideInactive] = useState(false)

  if (dataflowId == null || !derived.operators.has(dataflowId)) {
    return <div className="no-data">Select a dataflow from the Overview tab</div>
  }
  const root = derived.operators.get(dataflowId)!
  const currentAddr =
    scopeStack.length > 0 ? scopeStack[scopeStack.length - 1] : root.addr
  const totalOps = derived.descendantCount.get(dataflowId) ?? 0
  const totalMsgs = derived.transitiveMessages.get(dataflowId) ?? 0
  const totalElapsed = derived.sumElapsed.get(dataflowId) ?? 0

  // Map current addr back to an operator id (the scope we're rendering).
  const currentScopeId = derived.addrToId.get(addrKey(currentAddr)) ?? dataflowId

  function pushScope(opId: number) {
    const op = derived.operators.get(opId)
    if (!op) return
    setScopeStack([...scopeStack, op.addr])
  }

  return (
    <>
      <div className="detail-header">
        <h2>{root.name}</h2>
        <span className="detail-stats">
          {totalOps} operators, {totalMsgs} records, {formatNs(totalElapsed)}
        </span>
      </div>
      <div className="controls">
        <button onClick={() => setScopeStack([])}>&lt;&lt; Root</button>
        <button
          onClick={() => setScopeStack(scopeStack.slice(0, -1))}
          disabled={scopeStack.length === 0}
        >
          &lt; Back
        </button>
        <span className="dim">|</span>
        <label style={{ fontSize: '0.85em', color: '#888' }}>
          <input
            type="checkbox"
            checked={hideInactive}
            onChange={(e) => setHideInactive(e.target.checked)}
          />{' '}
          Hide inactive
        </label>
      </div>
      <div className="legend">
        <span className="legend-item">
          <span className="legend-swatch" style={{ background: '#12b886' }} />{' '}
          Scope
        </span>
        <span className="legend-item">
          <span className="legend-swatch" style={{ background: '#2a2a3a', borderColor: '#888' }} />{' '}
          Operator
        </span>
        <span className="legend-item">
          <span className="legend-swatch" style={{ background: '#4a3a6a' }} />{' '}
          Arrangement
        </span>
        <span className="legend-item">
          <span className="legend-swatch" style={{ background: '#bbbbff' }} />{' '}
          Boundary port
        </span>
      </div>
      <div className="breadcrumb">
        <span>Scope:</span>
        <span className="current">[{currentAddr.join(', ')}]</span>
      </div>
      <div id="graph-container">
        <Graph
          derived={derived}
          scopeId={currentScopeId}
          hideInactive={hideInactive}
          onPushScope={pushScope}
          filterActive={filterActive}
          filteredIds={filteredIds}
          compact={compact}
        />
      </div>
    </>
  )
}
