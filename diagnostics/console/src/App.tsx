import { useMemo, useRef, useState } from 'react'
import { useLiveQuery } from '@tanstack/react-db'
import { ilike } from '@tanstack/db'
import { channels, operators, statCounters } from './collections'
import { connect, type ConnState, type WsConn } from './ws'
import { derive } from './derive'
import { Overview } from './components/Overview'
import { Detail } from './components/Detail'

type Tab = 'overview' | 'detail'

export function App() {
  const [url, setUrl] = useState('ws://localhost:51371')
  const [conn, setConn] = useState<WsConn | null>(null)
  const [state, setState] = useState<ConnState>({ kind: 'idle' })
  const [errors, setErrors] = useState<string[]>([])
  const [tab, setTab] = useState<Tab>('overview')
  const [selectedDataflowId, setSelectedDataflowId] = useState<number | null>(null)
  const [scopeStack, setScopeStack] = useState<number[][]>([])
  const [filter, setFilter] = useState('')
  const [compact, setCompact] = useState(false)
  const connRef = useRef<WsConn | null>(null)

  const { data: opRows } = useLiveQuery((q) => q.from({ operators }))
  const { data: chRows } = useLiveQuery((q) => q.from({ channels }))
  const { data: statRows } = useLiveQuery((q) => q.from({ statCounters }))

  // Live-queried filtered subset. The `where` clause runs inside TanStack DB
  // as an incremental view: when the operators collection changes, only
  // delta rows are re-evaluated, and only matching/un-matching transitions
  // are emitted to subscribers. This is what we point at to demonstrate the
  // IVM ("zero") aspect of the data layer. We always run the same query
  // shape (ilike against a pattern); the empty-filter case uses `%` which
  // matches every string, so no conditional query swap is needed.
  const filterTrimmed = filter.trim()
  const filterActive = filterTrimmed !== ''
  const filterPattern = filterActive ? `%${filterTrimmed}%` : `%`
  const { data: filteredOps } = useLiveQuery(
    (q) =>
      q
        .from({ operators })
        .where(({ operators }) => ilike(operators.name, filterPattern)),
    [filterPattern],
  )
  const filteredIds = useMemo(
    () => new Set((filteredOps ?? []).map((o) => o.id)),
    [filteredOps],
  )

  const derived = useMemo(
    () => derive(opRows ?? [], chRows ?? [], statRows ?? []),
    [opRows, chRows, statRows],
  )

  function handleConnect() {
    if (conn) {
      conn.close()
      connRef.current = null
      setConn(null)
      return
    }
    const c = connect(
      url.trim(),
      setState,
      (msg) => setErrors((prev) => [...prev, msg]),
    )
    connRef.current = c
    setConn(c)
  }

  function selectDataflow(id: number) {
    setSelectedDataflowId(id)
    setScopeStack([])
    setTab('detail')
  }

  const statusText =
    state.kind === 'idle'
      ? 'Not connected'
      : state.kind === 'connecting'
      ? `Connecting to ${state.url}…`
      : state.kind === 'open'
      ? `Live (${state.updates} updates, ${derived.operators.size} operators)`
      : state.kind === 'closed'
      ? `Disconnected (${state.updates} updates received)`
      : `Error: ${state.message}`

  const statusClass =
    state.kind === 'open' ? 'loaded' : state.kind === 'error' ? 'error' : ''

  const showMain = state.kind === 'open' || state.kind === 'closed'

  return (
    <>
      <div id="header">
        <h1>Differential Dataflow Diagnostics</h1>
        <span id="status" className={statusClass}>
          {statusText}
        </span>
      </div>

      <div id="connect-section">
        <div className="conn-row">
          <input
            className="ws-url"
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder="ws://localhost:51371"
          />
          <button onClick={handleConnect}>
            {conn ? 'Disconnect' : 'Connect'}
          </button>
          {showMain && (
            <>
              <span className="dim" style={{ marginLeft: '1em' }}>|</span>
              <label style={{ fontSize: '0.85em', color: '#888' }}>
                Filter operators
              </label>
              <input
                className="filter-input"
                value={filter}
                onChange={(e) => setFilter(e.target.value)}
                placeholder="ilike substring (e.g. arrange)"
              />
              {filterActive && (
                <span className="dim" style={{ fontSize: '0.8em' }}>
                  {filteredIds.size} operator{filteredIds.size === 1 ? '' : 's'} match
                </span>
              )}
              <label
                style={{ fontSize: '0.85em', color: '#888', marginLeft: '0.5em' }}
                title="Shrink non-matching nodes to thin bands. Off by default — preserves layout stability across filter changes."
              >
                <input
                  type="checkbox"
                  checked={compact}
                  onChange={(e) => setCompact(e.target.checked)}
                  disabled={!filterActive}
                />{' '}
                Compact non-matches
              </label>
            </>
          )}
        </div>
      </div>

      {showMain && (
        <div id="main-content">
          <div className="tab-bar">
            <button
              className={tab === 'overview' ? 'active' : ''}
              onClick={() => setTab('overview')}
            >
              Overview
            </button>
            <button
              className={tab === 'detail' ? 'active' : ''}
              onClick={() => setTab('detail')}
            >
              Dataflow Detail
            </button>
          </div>

          <div className={`tab-pane panel ${tab === 'overview' ? 'active' : ''}`}>
            {tab === 'overview' && (
              <Overview
                derived={derived}
                filterActive={filterActive}
                filteredIds={filteredIds}
                onSelect={selectDataflow}
              />
            )}
          </div>

          <div className={`tab-pane panel ${tab === 'detail' ? 'active' : ''}`}>
            {tab === 'detail' && (
              <Detail
                derived={derived}
                dataflowId={selectedDataflowId}
                scopeStack={scopeStack}
                setScopeStack={setScopeStack}
                filterActive={filterActive}
                filteredIds={filteredIds}
                compact={compact}
              />
            )}
          </div>
        </div>
      )}

      {errors.length > 0 && (
        <div id="log">
          {errors.map((e, i) => (
            <div key={i}>{e}</div>
          ))}
        </div>
      )}
    </>
  )
}
