// Hand-rolled SVG line chart for the detail drawer. Floats are used for
// GEOMETRY only — every printed label comes from the original decimal
// strings with trailing zeros trimmed.

import type { HistoryPoint } from '../lib/types'
import { fmtUtcHM, trimDecimalStr } from '../lib/format'

const W = 420
const H = 150
const PAD = { l: 44, r: 10, t: 12, b: 18 }

export function PriceChart({ points }: { points: HistoryPoint[] }) {
  if (points.length < 2) {
    return <div className="chart-empty">no price history in this window</div>
  }

  const vals = points.map((pt) => parseFloat(pt.p))
  const ts = points.map((pt) => pt.t)
  let minI = 0
  let maxI = 0
  vals.forEach((v, i) => {
    if (v < vals[minI]!) minI = i
    if (v > vals[maxI]!) maxI = i
  })
  const lo = vals[minI]!
  const hi = vals[maxI]!
  const span = hi - lo || 0.01
  const yLo = lo - span * 0.12
  const yHi = hi + span * 0.12
  const t0 = ts[0]!
  const t1 = ts[ts.length - 1]!
  const tSpan = t1 - t0 || 1

  const x = (t: number) => PAD.l + ((t - t0) / tSpan) * (W - PAD.l - PAD.r)
  const y = (v: number) => PAD.t + (1 - (v - yLo) / (yHi - yLo)) * (H - PAD.t - PAD.b)

  const line = points.map((pt, i) => `${x(pt.t).toFixed(1)},${y(vals[i]!).toFixed(1)}`).join(' ')
  const area = `${x(t0).toFixed(1)},${H - PAD.b} ${line} ${x(t1).toFixed(1)},${H - PAD.b}`
  const lastV = vals[vals.length - 1]!
  const gridYs = [0.25, 0.5, 0.75].map((f) => PAD.t + f * (H - PAD.t - PAD.b))

  return (
    <svg
      className="price-chart"
      viewBox={`0 0 ${W} ${H}`}
      role="img"
      aria-label={`price history, ${points.length} points, low ${points[minI]!.p} high ${points[maxI]!.p}`}
    >
      {gridYs.map((gy) => (
        <line key={gy} className="chart-grid" x1={PAD.l} y1={gy} x2={W - PAD.r} y2={gy} />
      ))}
      <polygon className="chart-area" points={area} />
      <polyline className="chart-line" points={line} fill="none" />
      <circle className="chart-dot" cx={x(t1)} cy={y(lastV)} r={2.6} />

      {/* min/max labels use the verbatim strings, trimmed */}
      <text className="chart-label hi" x={PAD.l - 5} y={y(hi) + 3} textAnchor="end">
        {trimDecimalStr(points[maxI]!.p)}
      </text>
      <text className="chart-label lo" x={PAD.l - 5} y={y(lo) + 3} textAnchor="end">
        {trimDecimalStr(points[minI]!.p)}
      </text>
      <text className="chart-label last" x={x(t1) - 5} y={y(lastV) - 6} textAnchor="end">
        {trimDecimalStr(points[points.length - 1]!.p)}
      </text>
      <text className="chart-label" x={PAD.l} y={H - 5}>
        {fmtUtcHM(t0)}
      </text>
      <text className="chart-label" x={W - PAD.r} y={H - 5} textAnchor="end">
        {fmtUtcHM(t1)}
      </text>
    </svg>
  )
}
