// Docked right-side detail pane. Opens on row
// click; the /history chart loads async — the endpoint can take seconds, so
// the pane renders immediately with a shimmer where the chart will be.

import { useEffect, useState } from 'react'
import { fetchHistory, fetchMarket, ApiError } from '../lib/api'
import type { HistoryPoint, MarketResponse, ScreenerRow } from '../lib/types'
import { marketKey, splitMarketKey } from '../lib/urlState'
import {
  decimalSign,
  fmtAgo,
  fmtCloseIn,
  fmtPrice,
  fmtQty,
  fmtUtcDateTime,
} from '../lib/format'
import { PriceChart } from './PriceChart'
import { VenueLogo, venueName } from './VenueLogo'

interface Props {
  synthetic?: boolean
  selKey: string
  nowSec: number
  starred: readonly string[]
  onToggleStar: (key: string) => void
  onSelect: (key: string) => void
  onClose: () => void
}

const HOURS_CHOICES = [24, 72, 168] as const

function kv(label: string, value: string | null, title?: string | null) {
  return (
    <div className="kv" key={label}>
      <span>{label}</span>
      <b className={value === null ? 'null-val' : undefined} title={title ?? undefined}>
        {value ?? '—'}
      </b>
    </div>
  )
}

/** Stat tile for the headline quote and estimate values. */
function stat(label: string, raw: string | null, signed = false) {
  const { text, full } = fmtPrice(raw)
  let cls: string | undefined
  let shown = text
  if (full === null) {
    cls = 'null-val'
  } else if (signed) {
    const sign = decimalSign(raw)
    cls = sign > 0 ? 'up' : sign < 0 ? 'down' : undefined
    if (sign > 0) shown = `+${text}`
  }
  return (
    <div className="stat">
      <label>{label}</label>
      <b className={cls} title={full ?? undefined}>{shown}</b>
    </div>
  )
}

export function DetailDrawer({ synthetic = false, selKey, nowSec, starred, onToggleStar, onSelect, onClose }: Props) {
  const parsed = splitMarketKey(selKey)
  const [market, setMarket] = useState<MarketResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [hours, setHours] = useState<number>(24)
  const [history, setHistory] = useState<HistoryPoint[] | null>(null)
  const [historyError, setHistoryError] = useState(false)

  const venue = parsed?.venue ?? ''
  const marketId = parsed?.marketId ?? ''

  useEffect(() => {
    if (!venue || !marketId) return
    const ctl = new AbortController()
    setMarket(null)
    setError(null)
    fetchMarket(venue, marketId, ctl.signal)
      .then(setMarket)
      .catch((e: unknown) => {
        if (ctl.signal.aborted) return
        setError(e instanceof ApiError && e.status === 404 ? 'market not found' : 'failed to load market')
      })
    return () => ctl.abort()
  }, [venue, marketId])

  useEffect(() => {
    if (!venue || !marketId) return
    const ctl = new AbortController()
    setHistory(null) // shimmer while (re)loading
    setHistoryError(false)
    fetchHistory(venue, marketId, hours, ctl.signal)
      .then((h) => setHistory(h.points))
      .catch(() => {
        if (!ctl.signal.aborted) setHistoryError(true)
      })
    return () => ctl.abort()
  }, [venue, marketId, hours])

  if (!parsed) return null
  const row = market?.row
  const isStarred = starred.includes(selKey)

  return (
    <aside className="detail-pane" role="complementary" aria-label="market detail">
      <div className="dp-head">
        {row && (
          <span className={`dp-venue ${row.venue === 'kalshi' ? 'vK' : 'vP'}`}>
            <VenueLogo venue={row.venue} size={14} />
            {venueName(row.venue)}
          </span>
        )}
        <span className="dp-actions">
          <button
            className={`star-btn${isStarred ? ' on' : ''}`}
            onClick={() => onToggleStar(selKey)}
            title={isStarred ? 'remove from watchlist' : 'add to watchlist'}
          >
            {isStarred ? '★' : '☆'}
          </button>
          {row && !synthetic && (
            <a className="venue-link" href={row.url} target="_blank" rel="noopener noreferrer" title={row.url}>
              open on {row.venue} ↗
            </a>
          )}
          <button className="close-btn" onClick={onClose} title="close (Esc)">✕</button>
        </span>
      </div>

      {error && <div className="drawer-error">{error}</div>}
      {!row && !error && <div className="drawer-loading shimmer" style={{ height: 60 }} />}

      {row && (
        <>
          <h2 className="dp-title">{row.title}</h2>
          <div className="dp-sub" title={marketId}>
            <span className="mono">{marketId}</span>
            {' · '}
            {row.category ?? '—'} · {row.status}
            {row.recurrence ? ` · recurs ${row.recurrence}` : ''}
          </div>

          <div className="dp-stats">
            {stat('Bid', row.yes_bid)}
            {stat('Ask', row.yes_ask)}
            {stat('Spread', row.spread)}
            {stat('Model Δ', row.edge, true)}
          </div>

          <div className="drawer-section">
            <div className="drawer-sec-head">
              <span>price history</span>
              <span className="hours-btns">
                {HOURS_CHOICES.map((h) => (
                  <button
                    key={h}
                    className={h === hours ? 'on' : undefined}
                    onClick={() => setHours(h)}
                  >
                    {h === 24 ? '24h' : `${h / 24}d`}
                  </button>
                ))}
              </span>
            </div>
            {history === null && !historyError && <div className="shimmer chart-shimmer" />}
            {historyError && <div className="chart-empty">history unavailable</div>}
            {history !== null && <PriceChart points={history} />}
          </div>

          <div className="drawer-section">
            <div className="drawer-sec-head"><span>market</span></div>
            <div className="drawer-grid">
              {kv('mid', fmtPrice(row.mid).text, row.mid)}
              {kv('last', fmtPrice(row.last).text, row.last)}
              {kv('estimate p̂', fmtPrice(row.model_p).text, row.model_p)}
              {kv('model Δ', fmtPrice(row.edge).text, row.edge)}
              {kv('cost-adjusted Δ', fmtPrice(row.exec_edge).text, row.exec_edge)}
              {kv('skilled OFI', fmtPrice(row.skilled_ofi).text, row.skilled_ofi)}
            </div>
          </div>

          <div className="drawer-section">
            <div className="drawer-sec-head"><span>activity</span></div>
            <div className="drawer-grid">
              {kv('24h volume', fmtQty(row.volume_24h).text, row.volume_24h)}
              {kv('volume', fmtQty(row.volume).text, row.volume)}
              {kv('open interest', fmtQty(row.open_interest).text, row.open_interest)}
              {kv('liquidity', fmtQty(row.liquidity).text, row.liquidity)}
              {kv(
                'closes',
                row.close_ts === null
                  ? null
                  : `${fmtUtcDateTime(row.close_ts)} (${fmtCloseIn(row.close_ts, nowSec).text})`,
              )}
              {kv(
                'prices updated',
                row.prices_updated_ts === null ? null : `${fmtAgo(row.prices_updated_ts, nowSec).text} ago`,
              )}
            </div>
          </div>

          <div className="drawer-section drawer-ids">
            <div className="drawer-sec-head"><span>ids</span></div>
            {kv('market', row.market_id)}
            {kv('event', row.event_id)}
            {kv('series', row.series_id)}
            {kv('slug', row.slug)}
          </div>

          {market && market.siblings.length > 1 && (
            <div className="drawer-section">
              <div className="drawer-sec-head">
                <span>event siblings ({market.siblings.length})</span>
              </div>
              <table className="sib-table">
                <thead>
                  <tr><th className="l">market</th><th>mid</th><th>24h vol</th></tr>
                </thead>
                <tbody>
                  {market.siblings.map((s: ScreenerRow) => {
                    const key = marketKey(s.venue, s.market_id)
                    const cur = key === selKey
                    return (
                      <tr
                        key={key}
                        className={cur ? 'selected' : undefined}
                        onClick={() => !cur && onSelect(key)}
                      >
                        <td className="l">
                          <div className="mkt-title" title={s.title}>{cur ? '● ' : ''}{s.title}</div>
                          <div className="mkt-id">{s.market_id}</div>
                        </td>
                        <td title={s.mid ?? undefined}>{fmtPrice(s.mid).text}</td>
                        <td title={s.volume_24h ?? undefined}>{fmtQty(s.volume_24h).text}</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </aside>
  )
}
