import type { MetaResponse } from '../lib/types'
import { fmtAgo, fmtCount } from '../lib/format'
import { VenueLogo } from './VenueLogo'

interface Props {
  meta: MetaResponse | null
  metaError: boolean
  nowSec: number
  autoRefresh: boolean
  onToggleAutoRefresh: () => void
}

/** amber >10min, red >30min (status-bar freshness thresholds) */
function freshClass(ageSec: number): string {
  if (ageSec > 1800) return 'fr-stale'
  if (ageSec > 600) return 'fr-warn'
  return 'fr-ok'
}

export function StatusBar({ meta, metaError, nowSec, autoRefresh, onToggleAutoRefresh }: Props) {
  const prices = meta?.freshness.filter((f) => f.endpoint === 'prices') ?? []
  const counts = meta?.counts

  return (
    <header className="statusbar">
      <span className="app-name">
        <span className="app-mark" aria-hidden="true">◫</span>
        Prediction Markets <span className="sub">Research</span>
      </span>
      {counts && (
        <span className="sb-counts" title="open markets in the local dataset">
          open {fmtCount(counts['open_total'])}
          {' · '}
          <VenueLogo venue="kalshi" size={14} /> {fmtCount(counts['open_kalshi'])}
          {' · '}
          <VenueLogo venue="polymarket" size={14} /> {fmtCount(counts['open_polymarket'])}
        </span>
      )}
      <span className="sb-fresh">
        {prices.map((f) => {
          const a = fmtAgo(f.watermark_ts, nowSec)
          return (
            <span key={`${f.venue}:${f.endpoint}`} className={`fr ${freshClass(a.ageSec)}`}>
              {f.venue} {f.endpoint} {a.text === 'now' ? 'now' : `${a.text} ago`}
            </span>
          )
        })}
        {metaError && <span className="fr fr-stale">meta unreachable</span>}
      </span>
      <span className="sb-right">
        <button
          className={`ar-toggle${autoRefresh ? ' on' : ''}`}
          onClick={onToggleAutoRefresh}
          title="refetch the current page every 60s"
        >
          {autoRefresh ? '● auto-refresh 60s' : '○ auto-refresh off'}
        </button>
      </span>
    </header>
  )
}
