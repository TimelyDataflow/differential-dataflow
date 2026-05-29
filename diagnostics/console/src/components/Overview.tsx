import { useMemo, useState } from 'react'
import type { Derived } from '../derive'
import { formatNs } from '../derive'

type SortCol = 'id' | 'name' | 'operators' | 'messages' | 'elapsed_ns'
type Sort = { col: SortCol; asc: boolean }

const COLUMNS: { key: SortCol; label: string; num?: boolean }[] = [
  { key: 'id', label: 'Addr' },
  { key: 'name', label: 'Name' },
  { key: 'operators', label: 'Operators', num: true },
  { key: 'messages', label: 'Records', num: true },
  { key: 'elapsed_ns', label: 'Elapsed', num: true },
]

export function Overview({
  derived,
  filterActive,
  filteredIds,
  onSelect,
}: {
  derived: Derived
  filterActive: boolean
  filteredIds: Set<number>
  onSelect: (id: number) => void
}) {
  const [sort, setSort] = useState<Sort>({ col: 'elapsed_ns', asc: false })

  const rows = useMemo(() => {
    const ids = filterActive
      ? derived.rootIds.filter((id) => filteredIds.has(id))
      : derived.rootIds
    const out = ids.map((id) => {
      const op = derived.operators.get(id)!
      return {
        id,
        name: op.name,
        operators: derived.descendantCount.get(id) ?? 0,
        messages: derived.transitiveMessages.get(id) ?? 0,
        elapsed_ns: derived.sumElapsed.get(id) ?? 0,
      }
    })
    out.sort((a, b) => {
      const va = a[sort.col]
      const vb = b[sort.col]
      if (typeof va === 'string' && typeof vb === 'string') {
        return sort.asc ? va.localeCompare(vb) : vb.localeCompare(va)
      }
      const na = va as number
      const nb = vb as number
      return sort.asc ? na - nb : nb - na
    })
    return out
  }, [derived, sort, filterActive, filteredIds])

  function clickHeader(col: SortCol) {
    setSort((s) => (s.col === col ? { col, asc: !s.asc } : { col, asc: false }))
  }

  if (rows.length === 0) {
    return (
      <div className="no-data">
        {filterActive
          ? 'No root dataflows match filter (the filter matches operator names — most matches are typically nested operators, visible in the Detail tab as un-dimmed nodes)'
          : 'No dataflows found'}
      </div>
    )
  }

  return (
    <table>
      <thead>
        <tr>
          {COLUMNS.map((c) => (
            <th key={c.key} onClick={() => clickHeader(c.key)}>
              {c.label}
              {sort.col === c.key && (
                <span className="sort-arrow">{sort.asc ? '▲' : '▼'}</span>
              )}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <tr key={r.id} className="clickable" onClick={() => onSelect(r.id)}>
            <td className="num">{r.id}</td>
            <td>{r.name}</td>
            <td className="num">{r.operators}</td>
            <td className="num">{r.messages}</td>
            <td className="num">{formatNs(r.elapsed_ns)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
