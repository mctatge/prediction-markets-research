import { useEffect, useRef } from 'react'
import type { ScreenerRow } from '../lib/types'
import type { SortDir, SortKey } from '../lib/urlState'
import { marketKey } from '../lib/urlState'
import type { ColumnDef } from './columns'

export interface PulseInfo {
  /** rowKey -> set of changed row fields (from the previous fetch of the same query) */
  changed: Map<string, Set<string>>
  seq: number
}

interface Props {
  rows: ScreenerRow[]
  columns: ColumnDef[]
  nowSec: number
  sort: SortKey
  dir: SortDir
  selKey: string
  starred: readonly string[]
  loading: boolean
  pulse: PulseInfo
  onSort: (key: SortKey) => void
  onRowClick: (key: string) => void
  onToggleStar: (key: string) => void
}

export function ScreenerTable({
  rows, columns, nowSec, sort, dir, selKey, starred, loading, pulse,
  onSort, onRowClick, onToggleStar,
}: Props) {
  const starSet = new Set(starred)
  const bodyRef = useRef<HTMLTableSectionElement>(null)

  // keep the selected row visible when j/k moves it
  useEffect(() => {
    if (!selKey || !bodyRef.current) return
    const el = bodyRef.current.querySelector<HTMLElement>('tr[data-selected="true"]')
    el?.scrollIntoView({ block: 'nearest' })
  }, [selKey])

  return (
    <div className={`tablewrap${loading ? ' is-loading' : ''}`}>
      <table className="scr">
        <thead>
          <tr>
            <th className="c-star" aria-label="watchlist star">★</th>
            {columns.map((col) => {
              const sortable = col.sortKey !== undefined
              const active = sortable && col.sortKey === sort
              return (
                <th
                  key={col.id}
                  title={col.headerTitle}
                  aria-sort={active ? (dir === 'asc' ? 'ascending' : 'descending') : undefined}
                  tabIndex={sortable ? 0 : undefined}
                  className={[
                    col.align === 'left' ? 'l' : '',
                    sortable ? 'sortable' : '',
                    active ? 'sorted' : '',
                  ].filter(Boolean).join(' ')}
                  onClick={sortable ? () => onSort(col.sortKey!) : undefined}
                  onKeyDown={
                    sortable
                      ? (e) => {
                          if (e.key === 'Enter' || e.key === ' ') {
                            e.preventDefault()
                            onSort(col.sortKey!)
                          }
                        }
                      : undefined
                  }
                >
                  {col.label}
                  {active && <span className="sort-arrow">{dir === 'asc' ? '▲' : '▼'}</span>}
                </th>
              )
            })}
          </tr>
        </thead>
        <tbody ref={bodyRef}>
          {rows.map((row) => {
            const key = marketKey(row.venue, row.market_id)
            const isSel = key === selKey
            const changedFields = pulse.changed.get(key)
            return (
              <tr
                key={key}
                data-selected={isSel || undefined}
                className={isSel ? 'selected' : undefined}
                onClick={() => onRowClick(key)}
              >
                <td className="c-star">
                  <button
                    className={`star-btn${starSet.has(key) ? ' on' : ''}`}
                    title={starSet.has(key) ? 'remove from watchlist' : 'add to watchlist'}
                    onClick={(e) => {
                      e.stopPropagation()
                      onToggleStar(key)
                    }}
                  >
                    {starSet.has(key) ? '★' : '☆'}
                  </button>
                </td>
                {columns.map((col) => {
                  const hit =
                    changedFields !== undefined &&
                    col.watch.some((f) => changedFields.has(f))
                  return (
                    <td
                      key={hit ? `${col.id}#${pulse.seq}` : col.id}
                      className={[
                        col.align === 'left' ? 'l' : '',
                        hit ? 'pulse' : '',
                      ].filter(Boolean).join(' ') || undefined}
                    >
                      {col.render(row, { nowSec })}
                    </td>
                  )
                })}
              </tr>
            )
          })}
        </tbody>
      </table>
      {rows.length === 0 && !loading && (
        <div className="empty-state">no markets match this screen</div>
      )}
    </div>
  )
}
