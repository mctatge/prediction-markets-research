// Pure display formatters. Prices are decimal strings and are
// rendered from the string — parseFloat is allowed only for color/comparison
// logic, never to produce the displayed digits.

/**
 * Round a decimal string half-up to `maxDp` decimals, then trim trailing
 * zeros down to `minDp` decimals. Pure string arithmetic — no float ever.
 * "0.4200" -> "0.42", "0.4267" -> "0.427", "0.9999" -> "1.00".
 */
export function trimDecimalStr(s: string, maxDp = 3, minDp = 2): string {
  const neg = s.startsWith('-')
  const body = neg ? s.slice(1) : s
  if (!/^\d+(\.\d+)?$/.test(body)) return s // not a plain decimal — show verbatim
  const dot = body.indexOf('.')
  let int = dot === -1 ? body : body.slice(0, dot)
  let frac = dot === -1 ? '' : body.slice(dot + 1)

  if (frac.length > maxDp) {
    const roundUp = frac.charCodeAt(maxDp) >= 53 // '5'
    frac = frac.slice(0, maxDp)
    if (roundUp) {
      // increment int+frac as one digit string, with carry
      const digits = (int + frac).split('')
      let i = digits.length - 1
      while (i >= 0) {
        if (digits[i] === '9') {
          digits[i] = '0'
          i--
        } else {
          digits[i] = String.fromCharCode(digits[i]!.charCodeAt(0) + 1)
          break
        }
      }
      if (i < 0) digits.unshift('1')
      const joined = digits.join('')
      int = joined.slice(0, joined.length - maxDp) || '0'
      frac = joined.slice(joined.length - maxDp)
    }
  }
  while (frac.length > minDp && frac.endsWith('0')) frac = frac.slice(0, -1)
  while (frac.length < minDp) frac += '0'
  int = int.replace(/^0+(?=\d)/, '')
  const out = frac.length ? `${int}.${frac}` : int
  return neg ? `-${out}` : out
}

export interface PriceDisplay {
  text: string
  /** full verbatim value for the tooltip; null when there is none */
  full: string | null
}

/** Price cell: trimmed for display, full string preserved for the tooltip. */
export function fmtPrice(s: string | null): PriceDisplay {
  if (s === null || s === undefined || s === '') return { text: '—', full: null }
  return { text: trimDecimalStr(s), full: s }
}

/** Sign of a decimal string ("-0.03" -> -1, "0.00" -> 0). For color only. */
export function decimalSign(s: string | null): -1 | 0 | 1 {
  if (s === null) return 0
  const v = parseFloat(s)
  if (!Number.isFinite(v) || v === 0) return 0
  return v > 0 ? 1 : -1
}

/**
 * Quantity cell (volume / OI / liquidity — venue-native units, NOT prices):
 * humanized via float math, full string kept for the tooltip.
 */
export function fmtQty(s: string | null): PriceDisplay {
  if (s === null || s === undefined || s === '') return { text: '—', full: null }
  const v = parseFloat(s)
  if (!Number.isFinite(v)) return { text: s, full: s }
  const abs = Math.abs(v)
  let text: string
  if (abs >= 1e9) text = `${(v / 1e9).toFixed(1)}B`
  else if (abs >= 1e6) text = `${(v / 1e6).toFixed(1)}M`
  else if (abs >= 1e3) text = `${(v / 1e3).toFixed(1)}k`
  else text = String(Math.round(v))
  return { text, full: s }
}

export interface CloseInDisplay {
  text: string
  /** true when closing within 6h (render red) */
  urgent: boolean
  /** true when close_ts is in the past */
  past: boolean
}

/** Humanized time-to-close: "3h", "2d", "41m", "—" when unknown. */
export function fmtCloseIn(closeTs: number | null, nowSec: number): CloseInDisplay {
  if (closeTs === null) return { text: '—', urgent: false, past: false }
  const diff = closeTs - nowSec
  if (diff <= 0) return { text: 'ended', urgent: false, past: true }
  let text: string
  if (diff >= 48 * 3600) text = `${Math.floor(diff / 86400)}d`
  else if (diff >= 3600) text = `${Math.floor(diff / 3600)}h`
  else if (diff >= 60) text = `${Math.floor(diff / 60)}m`
  else text = '<1m'
  return { text, urgent: diff < 6 * 3600, past: false }
}

export interface AgoDisplay {
  text: string
  /** age in seconds; Infinity when unknown */
  ageSec: number
}

/** Relative "updated" time: "2m", "1h", "3d", "—". Caller picks the staleness class. */
export function fmtAgo(ts: number | null, nowSec: number): AgoDisplay {
  if (ts === null) return { text: '—', ageSec: Infinity }
  const diff = Math.max(0, nowSec - ts)
  let text: string
  if (diff < 60) text = 'now'
  else if (diff < 3600) text = `${Math.floor(diff / 60)}m`
  else if (diff < 86400) text = `${Math.floor(diff / 3600)}h`
  else text = `${Math.floor(diff / 86400)}d`
  return { text, ageSec: diff }
}

/** UTC HH:MM label for chart axes / status bar. */
export function fmtUtcHM(ts: number): string {
  const d = new Date(ts * 1000)
  const hh = String(d.getUTCHours()).padStart(2, '0')
  const mm = String(d.getUTCMinutes()).padStart(2, '0')
  return `${hh}:${mm}Z`
}

/** UTC date+time for the drawer ("Jul 29 18:00Z"). */
export function fmtUtcDateTime(ts: number): string {
  const d = new Date(ts * 1000)
  const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
  return `${months[d.getUTCMonth()]} ${d.getUTCDate()} ${fmtUtcHM(ts)}`
}

/** Thousands separators for integer counts (status bar). */
export function fmtCount(n: number | undefined | null): string {
  if (n === undefined || n === null) return '—'
  return n.toLocaleString('en-US')
}
