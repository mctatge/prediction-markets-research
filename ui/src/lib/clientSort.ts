// Client-side sorting is used ONLY for watchlist scope, where rows are
// fetched individually (no server pagination to violate). Mirrors the
// contract's semantics: NULLs always sort last regardless of dir.

import type { ScreenerRow } from './types'
import type { SortDir, SortKey } from './urlState'

function numOrNull(s: string | null): number | null {
  if (s === null) return null
  const v = parseFloat(s)
  return Number.isFinite(v) ? v : null
}

function sortValue(row: ScreenerRow, key: SortKey): number | string | null {
  switch (key) {
    case 'vol24h':
      return numOrNull(row.volume_24h)
    case 'volume':
      return numOrNull(row.volume)
    case 'price':
      return numOrNull(row.mid) ?? numOrNull(row.last)
    case 'spread':
      return numOrNull(row.spread)
    case 'close_ts':
      return row.close_ts
    case 'open_interest':
      return numOrNull(row.open_interest)
    case 'liquidity':
      return numOrNull(row.liquidity)
    case 'updated':
      return row.prices_updated_ts
    case 'title':
      return row.title
    case 'edge': {
      // |edge| to mirror the server's biggest-disagreement-first semantics
      const e = numOrNull(row.edge)
      return e === null ? null : Math.abs(e)
    }
    case 'exec_edge': {
      const x = numOrNull(row.exec_edge)
      return x === null ? null : Math.abs(x)
    }
    case 'skilled_ofi': {
      const o = numOrNull(row.skilled_ofi)
      return o === null ? null : Math.abs(o)
    }
  }
}

export function sortRowsClient(rows: ScreenerRow[], key: SortKey, dir: SortDir): ScreenerRow[] {
  const mult = dir === 'asc' ? 1 : -1
  return rows.slice().sort((a, b) => {
    const va = sortValue(a, key)
    const vb = sortValue(b, key)
    if (va === null && vb === null) return 0
    if (va === null) return 1 // nulls last regardless of dir
    if (vb === null) return -1
    if (typeof va === 'string' || typeof vb === 'string') {
      return mult * String(va).localeCompare(String(vb))
    }
    return mult * (va - vb)
  })
}
