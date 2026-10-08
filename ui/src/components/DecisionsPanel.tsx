// Paper decision ledger. Self-contained — fetches
// /api/decisions on mount + every 60s; renders a summary strip and the
// recent ledger rows. Read-only like everything else in this UI.

import { useEffect, useState } from 'react'
import { fetchDecisions } from '../lib/api'
import type { DecisionRow, DecisionsResponse } from '../lib/types'
import { VenueLogo } from './VenueLogo'

// Pull cross-venue matched legs (same event_key) adjacent so a PM decision and
// its paired Kalshi decision render one above the other — the dual-venue
// quote comparison view. Unmatched rows keep descending timestamp order.
function groupByEvent(rows: DecisionRow[]): DecisionRow[] {
  const out: DecisionRow[] = []
  const done = new Set<string>()
  for (const r of rows) {
    if (r.event_key) {
      if (done.has(r.event_key)) continue
      done.add(r.event_key)
      for (const x of rows) if (x.event_key === r.event_key) out.push(x)
    } else {
      out.push(r)
    }
  }
  return out
}

function fmtAge(nowSec: number, ts: number): string {
  const s = Math.max(0, nowSec - ts)
  if (s < 90) return `${s}s`
  if (s < 5400) return `${Math.round(s / 60)}m`
  return `${Math.round(s / 3600)}h`
}

function GateDots({ gates }: { gates: Record<string, boolean> }) {
  const entries = Object.entries(gates)
  const failed = entries.filter(([, ok]) => !ok).map(([k]) => k)
  return (
    <span
      className="gate-dots"
      title={failed.length ? `failed: ${failed.join(', ')}` : 'all gates passed'}
    >
      {entries.map(([k, ok]) => (
        <span key={k} className={ok ? 'gate-dot ok' : 'gate-dot fail'} />
      ))}
    </span>
  )
}

function RetCell({ row }: { row: DecisionRow }) {
  if (!row.graded) return <td className="num mut">—</td>
  const v = parseFloat(row.graded.ret_per_dollar)
  return (
    <td className={`num ${v >= 0 ? 'up' : 'down'}`}>
      {(v * 100).toFixed(1)}%
    </td>
  )
}

export function DecisionsPanel({ nowSec }: { nowSec: number }) {
  const [data, setData] = useState<DecisionsResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let alive = true
    const ctrl = new AbortController()
    const load = () =>
      fetchDecisions(ctrl.signal)
        .then((d) => {
          if (!alive) return
          setData(d)
          setError(null)
        })
        .catch((e) => alive && setError(String(e?.message ?? e)))
    load()
    const id = window.setInterval(load, 60_000)
    return () => {
      alive = false
      ctrl.abort()
      window.clearInterval(id)
    }
  }, [])

  if (error) return <div className="error-bar">{error}</div>
  if (!data) return <div className="empty-state">loading decisions…</div>

  const s = data.summary
  const cum = s.equity.length ? s.equity[s.equity.length - 1].cum_ret : 0

  return (
    <div className="tablewrap">
      <div className="research-note">
        Paper records only. Returns use hypothetical entries; no orders are placed.
        Repeated decisions on the same event are not independent observations.
      </div>
      <div className="decisions-summary">
        <span><b>{s.decisions.toLocaleString()}</b> recorded</span>
        <span><b>{s.passed_gates.toLocaleString()}</b> passed gates</span>
        <span><b>{s.graded.toLocaleString()}</b> graded</span>
        <span>
          paper win rate <b>{s.win_rate != null ? `${(s.win_rate * 100).toFixed(1)}%` : '—'}</b>
        </span>
        <span>
          mean paper return <b className={s.mean_ret_per_dollar != null && s.mean_ret_per_dollar >= 0 ? 'up' : 'down'}>
            {s.mean_ret_per_dollar != null ? `${(s.mean_ret_per_dollar * 100).toFixed(1)}%` : '—'}
          </b>
        </span>
        <span title="Sum of per-decision return ratios; not a compounded portfolio return.">
          summed paper returns <b className={cum >= 0 ? 'up' : 'down'}>{(cum * 100).toFixed(1)}%</b>
        </span>
        <span className="mut">
          last pass {s.last_pass_ts ? `${fmtAge(nowSec, s.last_pass_ts)} ago` : 'never'}
          {' · record only'}
        </span>
      </div>
      {data.rows.length === 0 ? (
        <div className="empty-state">
          No paper decisions recorded. Load a research dataset or run the offline demo.
        </div>
      ) : (
        <table className="scr">
          <thead>
            <tr>
              <th>when</th>
              <th>venue</th>
              <th className="c-title">market</th>
              <th>paper side</th>
              <th className="num" title="Experimental expected value under the configured model and fee assumptions.">Est. EV</th>
              <th className="num">bid</th>
              <th className="num">ask</th>
              <th className="num">OFI</th>
              <th className="num">SK #</th>
              <th>gates</th>
              <th className="num">paper ret/$</th>
            </tr>
          </thead>
          <tbody>
            {groupByEvent(data.rows).map((r) => (
              <tr
                key={r.decision_id}
                className={[r.passed_gates ? '' : 'mut', r.event_key ? 'xv' : '']
                  .filter(Boolean)
                  .join(' ') || undefined}
              >
                <td className="num">{fmtAge(nowSec, r.ts)}</td>
                <td>
                  <span className="venue-cell">
                    <VenueLogo venue={r.venue} size={15} />
                    {r.event_key ? (
                      <span
                        className="xv-chip"
                        title={
                          r.signal_venue_id && r.signal_venue_id !== r.venue
                            ? `cross-venue match — signal from ${r.signal_venue_id}`
                            : 'cross-venue matched event'
                        }
                      >
                        ⇄
                      </span>
                    ) : null}
                  </span>
                </td>
                <td className="c-title" title={r.market_id}>
                  {r.title ?? r.market_id.slice(0, 18)}
                </td>
                <td>
                  <span className={`side-chip ${r.action === 'PASS' ? 'pass' : 'buy'}`}>
                    {r.action === 'PASS' ? `PASS (${r.side.replace('BUY_', '')})` : r.action}
                  </span>
                </td>
                <td className={`num ${r.ev && parseFloat(r.ev) >= 0 ? 'up' : 'down'}`}>
                  {r.ev ?? '—'}
                </td>
                <td className="num">{r.bid ?? '—'}</td>
                <td className="num">{r.ask ?? '—'}</td>
                <td className="num">{r.skilled_ofi ?? '—'}</td>
                <td className="num">{r.n_skilled_trades ?? '—'}</td>
                <td><GateDots gates={r.gates} /></td>
                <RetCell row={r} />
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  )
}
