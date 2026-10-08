import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'
import { fetchMeta, fetchScreener, fetchWatchlistRows, PAGE_SIZE } from './lib/api'
import type { MetaResponse, ScreenerRow } from './lib/types'
import type { SortKey } from './lib/urlState'
import { marketKey } from './lib/urlState'
import { useNowSec, useSearchParamsState } from './lib/useSearchParamsState'
import { getWatchlist, WATCHLIST_CAP } from './lib/watchlist'
import { sortRowsClient } from './lib/clientSort'
import { VIEW_COLUMNS, VIEW_LABELS } from './components/columns'
import type { PulseInfo } from './components/ScreenerTable'
import { ScreenerTable } from './components/ScreenerTable'
import { FilterRail } from './components/FilterRail'
import { StatusBar } from './components/StatusBar'
import { Pagination } from './components/Pagination'
import { DetailDrawer } from './components/DetailDrawer'
import { DecisionsPanel } from './components/DecisionsPanel'
import { ResearchPanel } from './components/ResearchPanel'

const VIEWS = ['skills', 'flow', 'decisions', 'prices', 'liquidity', 'edge'] as const

interface TableData {
  rows: ScreenerRow[]
  total: number | null
}

export default function App() {
  const [state, update] = useSearchParamsState()
  const nowSec = useNowSec()
  const watchlist = getWatchlist()
  const starred = useSyncExternalStore(watchlist.subscribe, watchlist.getSnapshot)

  const [meta, setMeta] = useState<MetaResponse | null>(null)
  const [metaError, setMetaError] = useState(false)
  const [autoRefresh, setAutoRefresh] = useState(false)
  const [refreshSeq, setRefreshSeq] = useState(0)
  const [data, setData] = useState<TableData>({ rows: [], total: null })
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [pulse, setPulse] = useState<PulseInfo>({ changed: new Map(), seq: 0 })
  const [railOpen, setRailOpen] = useState(true)
  const searchRef = useRef<HTMLInputElement>(null)
  const prevFetchRef = useRef<{ fingerprint: string; byKey: Map<string, ScreenerRow> } | null>(null)

  // ---- meta: fetch now + every 60s ----
  useEffect(() => {
    let alive = true
    const load = () =>
      fetchMeta()
        .then((m) => {
          if (!alive) return
          setMeta(m)
          setMetaError(false)
        })
        .catch(() => alive && setMetaError(true))
    load()
    const id = window.setInterval(load, 60_000)
    return () => {
      alive = false
      window.clearInterval(id)
    }
  }, [])

  // ---- table auto-refresh (off by default) ----
  useEffect(() => {
    if (!autoRefresh) return
    const id = window.setInterval(() => setRefreshSeq((s) => s + 1), 60_000)
    return () => window.clearInterval(id)
  }, [autoRefresh])

  // ---- query fingerprint: inputs that change the displayed rows ----
  const wlKeys = state.watchlist ? starred.slice(0, WATCHLIST_CAP) : null
  const fingerprint = useMemo(
    () =>
      JSON.stringify([
        state.venue, state.category, state.q, state.status,
        state.price_min, state.price_max, state.vol24h_min, state.liquidity_min,
        state.spread_max, state.close_within_h, state.exclude_churn,
        state.sort, state.dir, state.page,
        wlKeys,
      ]),
    [state, wlKeys],
  )

  // ---- data fetch ----
  useEffect(() => {
    const ctl = new AbortController()
    setLoading(true)
    setError(null)

    const run = async (): Promise<TableData> => {
      if (wlKeys !== null) {
        const rows = await fetchWatchlistRows(wlKeys, ctl.signal)
        return { rows: sortRowsClient(rows, state.sort, state.dir), total: rows.length }
      }
      const res = await fetchScreener(state, ctl.signal)
      return { rows: res.rows, total: res.total }
    }

    run()
      .then((d) => {
        if (ctl.signal.aborted) return
        // flash-on-change: diff against the previous fetch of the SAME query
        const prev = prevFetchRef.current
        const changed = new Map<string, Set<string>>()
        if (prev && prev.fingerprint === fingerprint) {
          for (const row of d.rows) {
            const key = marketKey(row.venue, row.market_id)
            const old = prev.byKey.get(key)
            if (!old) continue
            const fields = new Set<string>()
            for (const f of Object.keys(row) as (keyof ScreenerRow)[]) {
              if (row[f] !== old[f]) fields.add(f)
            }
            if (fields.size > 0) changed.set(key, fields)
          }
        }
        prevFetchRef.current = {
          fingerprint,
          byKey: new Map(d.rows.map((r) => [marketKey(r.venue, r.market_id), r])),
        }
        setPulse((p) => ({ changed, seq: p.seq + 1 }))
        setData(d)
        setLoading(false)
        // out-of-range page (e.g. hand-edited URL): clamp to the last real page
        if (
          wlKeys === null &&
          d.rows.length === 0 &&
          d.total !== null &&
          state.page > 0 &&
          state.page * PAGE_SIZE >= d.total
        ) {
          update({ page: Math.max(0, Math.ceil(d.total / PAGE_SIZE) - 1) })
        }
      })
      .catch((e: unknown) => {
        if (ctl.signal.aborted) return
        setError(e instanceof Error ? e.message : 'request failed')
        setLoading(false)
      })
    return () => ctl.abort()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fingerprint, refreshSeq])

  // ---- header sort click: toggle dir on same column, else desc ----
  const onSort = useCallback(
    (key: SortKey) => {
      if (state.sort === key) {
        update({ dir: state.dir === 'desc' ? 'asc' : 'desc' })
      } else {
        update({ sort: key, dir: key === 'title' || key === 'close_ts' ? 'asc' : 'desc' })
      }
    },
    [state.sort, state.dir, update],
  )

  const onToggleStar = useCallback(
    (key: string) => {
      if (watchlist.toggle(key) === 'full') {
        window.alert(`watchlist is capped at ${WATCHLIST_CAP} markets`)
      }
    },
    [watchlist],
  )

  // ---- keyboard: j/k row selection, / focus search, Esc close drawer ----
  const rowsRef = useRef(data.rows)
  rowsRef.current = data.rows
  const selRef = useRef(state.sel)
  selRef.current = state.sel
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      const target = e.target as HTMLElement | null
      const inField =
        target instanceof HTMLInputElement ||
        target instanceof HTMLTextAreaElement ||
        target instanceof HTMLSelectElement
      if (e.key === 'Escape') {
        if (inField) {
          target.blur()
        } else if (selRef.current) {
          update({ sel: '' })
        }
        return
      }
      if (inField) return
      if (e.key === '/') {
        e.preventDefault()
        searchRef.current?.focus()
        searchRef.current?.select()
        return
      }
      if (e.key === 'j' || e.key === 'k') {
        const rows = rowsRef.current
        if (rows.length === 0) return
        const keys = rows.map((r) => marketKey(r.venue, r.market_id))
        const cur = keys.indexOf(selRef.current)
        let next: number
        if (cur === -1) {
          next = e.key === 'j' ? 0 : keys.length - 1
        } else {
          next = Math.min(keys.length - 1, Math.max(0, cur + (e.key === 'j' ? 1 : -1)))
        }
        update({ sel: keys[next]! })
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [update])

  const columns = VIEW_COLUMNS[state.view]

  return (
    <div className="app">
      <StatusBar
        meta={meta}
        metaError={metaError}
        nowSec={nowSec}
        autoRefresh={autoRefresh}
        onToggleAutoRefresh={() => setAutoRefresh((v) => !v)}
      />
      {meta?.data_mode === 'synthetic' && (
        <div className="demo-banner" role="status">
          <strong>Synthetic demo</strong>
          <span>Markets, prices, wallet flow and paper results are generated examples.</span>
        </div>
      )}
      <div className="main">
        {railOpen && (
          <FilterRail
            state={state}
            meta={meta}
            watchCount={starred.length}
            searchRef={searchRef}
            update={update}
          />
        )}
        <div className="content">
          <div className="view-tabs">
            <button
              className="rail-toggle"
              onClick={() => setRailOpen((v) => !v)}
              title={railOpen ? 'hide filters' : 'show filters'}
            >
              {railOpen ? '⟨' : '⟩ filters'}
            </button>
            {VIEWS.map((v) => (
              <button
                key={v}
                className={`tab${state.view === v ? ' on' : ''}`}
                onClick={() => update({ view: v })}
              >
                {VIEW_LABELS[v]}
              </button>
            ))}
            {state.view === 'edge' && (
              <span
                className="tab-note"
                title="Experimental estimates do not establish a profitable following strategy."
              >
                Experimental model estimates · profitability unverified
              </span>
            )}
            {loading && <span className="loading-dot">fetching…</span>}
            <span className="tab-spacer" />
            <span className="kbd-hint">j/k rows · / search · esc close</span>
          </div>
          {error && (
            <div className="error-bar">
              {error}
              <button onClick={() => setRefreshSeq((s) => s + 1)}>retry</button>
            </div>
          )}
          {state.view === 'skills' ? (
            <ResearchPanel />
          ) : state.view === 'decisions' ? (
            <DecisionsPanel nowSec={nowSec} />
          ) : (
            <>
              <ScreenerTable
                rows={data.rows}
                columns={columns}
                nowSec={nowSec}
                sort={state.sort}
                dir={state.dir}
                selKey={state.sel}
                starred={starred}
                loading={loading}
                pulse={pulse}
                onSort={onSort}
                onRowClick={(key) => update({ sel: key })}
                onToggleStar={onToggleStar}
              />
              <Pagination
                page={state.page}
                pageSize={PAGE_SIZE}
                total={data.total}
                shown={data.rows.length}
                watchlistScope={state.watchlist}
                onPage={(page) => update({ page })}
              />
            </>
          )}
        </div>
        {state.sel && (
          <DetailDrawer
            synthetic={meta?.data_mode === 'synthetic'}
            key={state.sel}
            selKey={state.sel}
            nowSec={nowSec}
            starred={starred}
            onToggleStar={onToggleStar}
            onSelect={(key) => update({ sel: key })}
            onClose={() => update({ sel: '' })}
          />
        )}
      </div>
    </div>
  )
}
