import { fmtCount } from '../lib/format'

interface Props {
  page: number // 0-based
  pageSize: number
  total: number | null
  shown: number
  watchlistScope: boolean
  onPage: (page: number) => void
}

export function Pagination({ page, pageSize, total, shown, watchlistScope, onPage }: Props) {
  if (watchlistScope) {
    return (
      <footer className="pager">
        <span>{shown} starred row{shown === 1 ? '' : 's'} · watchlist scope (no pagination)</span>
      </footer>
    )
  }
  const start = page * pageSize + 1
  const end = page * pageSize + shown
  const lastPage = total !== null ? Math.max(0, Math.ceil(total / pageSize) - 1) : null
  return (
    <footer className="pager">
      <span>
        {shown === 0 ? 'no rows' : <>rows {fmtCount(start)}–{fmtCount(end)}</>}
        {total !== null && <> of {fmtCount(total)}</>}
      </span>
      <span className="pager-btns">
        <button disabled={page === 0} onClick={() => onPage(0)} title="first page">«</button>
        <button disabled={page === 0} onClick={() => onPage(page - 1)}>‹ prev</button>
        <span className="pager-page">
          page {fmtCount(page + 1)}{lastPage !== null && <> / {fmtCount(lastPage + 1)}</>}
        </span>
        <button
          disabled={lastPage !== null && page >= lastPage}
          onClick={() => onPage(page + 1)}
        >
          next ›
        </button>
        <button
          disabled={lastPage === null || page >= lastPage}
          onClick={() => lastPage !== null && onPage(lastPage)}
          title="last page"
        >
          »
        </button>
      </span>
    </footer>
  )
}
