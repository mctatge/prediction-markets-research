import { useEffect, useState } from 'react'
import { ApiError, fetchResearch } from '../lib/api'
import { fmtCount, fmtPrice, fmtQty, fmtUtcDateTime } from '../lib/format'
import type { ResearchResponse } from '../lib/types'

export function ResearchPanel() {
  const [data, setData] = useState<ResearchResponse | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    const ctrl = new AbortController()
    fetchResearch(ctrl.signal)
      .then((report) => { if (!ctrl.signal.aborted) setData(report) })
      .catch((e: unknown) => {
        if (ctrl.signal.aborted) return
        setError(e instanceof ApiError && e.status === 404
          ? 'No saved research report is available for this dataset.'
          : 'Unable to load the saved research report.')
      })
    return () => ctrl.abort()
  }, [])

  if (error) return <div className="error-bar">{error}</div>
  if (!data) return <div className="empty-state">Loading wallet evidence…</div>

  return (
    <div className="tablewrap">
      <div className="research-note">
        <strong>Historical skill → selected-trader flow → paper following.</strong>{' '}
        Wallet selection uses past trades. Persistent skill and profitable following
        are separate questions; these scores do not establish either result.
      </div>
      {data.data_mode === 'synthetic' && (
        <div className="research-note">
          Synthetic wallet histories and outcomes illustrate the method, not measured trader performance.
        </div>
      )}
      <div className="decisions-summary">
        <span><b>{fmtCount(data.wallets.length)}</b> wallets scored</span>
        <span><b>{fmtCount(data.selected_wallets.length)}</b> selected</span>
        <span><b>{fmtCount(data.flows.length)}</b> flow markets</span>
        <span><b>{fmtCount(data.decisions.length)}</b> paper decisions</span>
        <span><b>{fmtCount(data.grading?.graded)}</b> graded</span>
        {data.skill_cutoff_ts != null && (
          <span>history cutoff <b>{fmtUtcDateTime(data.skill_cutoff_ts)}</b></span>
        )}
      </div>
      {data.selection_rule && (
        <div className="research-note">Selection rule: {data.selection_rule}</div>
      )}
      {data.wallets.length === 0 ? (
        <div className="empty-state">
          No saved wallet scores. Open the offline demo to inspect a complete synthetic example.
        </div>
      ) : (
        <table className="scr">
          <caption className="sr-only">Wallet evidence from trades before the research cutoff</caption>
          <thead>
            <tr>
              <th>wallet</th>
              <th className="num">trades</th>
              <th className="num">markets</th>
              <th className="num">volume</th>
              <th className="num" title="Observed PnL in the scoring history, before following costs.">history PnL</th>
              <th className="num" title="Historical randomization score. A high score does not guarantee future skill.">skill z-score</th>
              <th>selected</th>
            </tr>
          </thead>
          <tbody>
            {data.wallets.map((wallet) => (
              <tr key={wallet.wallet}>
                <td className="mono" title={wallet.wallet}>{wallet.wallet}</td>
                <td className="num">{fmtCount(wallet.n_trades)}</td>
                <td className="num">{fmtCount(wallet.n_markets)}</td>
                <td className="num" title={wallet.total_volume}>{fmtQty(wallet.total_volume).text}</td>
                <td className="num" title={wallet.actual_pnl}>{fmtPrice(wallet.actual_pnl).text}</td>
                <td className="num" title={wallet.z_score}>{fmtPrice(wallet.z_score).text}</td>
                <td><span className={`side-chip ${wallet.selected ? 'buy' : 'pass'}`}>{wallet.selected ? 'yes' : 'no'}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {data.limitations && data.limitations.length > 0 && (
        <div className="research-note">
          <strong>Evidence limits</strong>
          <ul>{data.limitations.map((limit) => <li key={limit}>{limit}</li>)}</ul>
        </div>
      )}
    </div>
  )
}
