import { useEffect, useRef, useState } from 'react'
import type { RefObject } from 'react'
import type { MetaResponse } from '../lib/types'
import type { ScreenState } from '../lib/urlState'
import { DEFAULT_STATE } from '../lib/urlState'
import { fmtCount } from '../lib/format'

// ---- canned scans: filter+sort combos in one click ----
interface Scan {
  label: string
  patch: Partial<ScreenState>
}
const SCANS: Scan[] = [
  { label: 'High volume', patch: { sort: 'vol24h', dir: 'desc' } },
  { label: 'Closing soon', patch: { close_within_h: '24', sort: 'close_ts', dir: 'asc' } },
  { label: 'Wide spread', patch: { sort: 'spread', dir: 'desc' } },
  { label: 'New/Fresh', patch: { sort: 'updated', dir: 'desc' } },
  { label: 'Longshots', patch: { price_min: '', price_max: '0.10', sort: 'vol24h', dir: 'desc' } },
]

function scanActive(state: ScreenState, scan: Scan): boolean {
  return Object.entries(scan.patch).every(
    ([k, v]) => state[k as keyof ScreenState] === v,
  )
}

// ---- debounced text input (300ms) ----
interface DInputProps {
  value: string
  onCommit: (v: string) => void
  placeholder?: string
  inputRef?: RefObject<HTMLInputElement>
  className?: string
  ariaLabel?: string
}
function DebouncedInput({ value, onCommit, placeholder, inputRef, className, ariaLabel }: DInputProps) {
  const [val, setVal] = useState(value)
  const timer = useRef<number | undefined>(undefined)
  // external change (URL restore, reset, scan) wins
  useEffect(() => setVal(value), [value])
  useEffect(() => () => window.clearTimeout(timer.current), [])
  return (
    <input
      ref={inputRef}
      className={className}
      type="text"
      value={val}
      placeholder={placeholder}
      aria-label={ariaLabel}
      spellCheck={false}
      onChange={(e) => {
        const v = e.target.value
        setVal(v)
        window.clearTimeout(timer.current)
        timer.current = window.setTimeout(() => onCommit(v), 300)
      }}
      onKeyDown={(e) => {
        if (e.key === 'Enter') {
          window.clearTimeout(timer.current)
          onCommit(val)
        }
        if (e.key === 'Escape') (e.target as HTMLInputElement).blur()
      }}
      onBlur={() => {
        window.clearTimeout(timer.current)
        if (val !== value) onCommit(val)
      }}
    />
  )
}

interface Props {
  state: ScreenState
  meta: MetaResponse | null
  watchCount: number
  searchRef: RefObject<HTMLInputElement>
  update: (patch: Partial<ScreenState>) => void
}

export function FilterRail({ state, meta, watchCount, searchRef, update }: Props) {
  // categories: per-venue counts when a venue is selected, else merged sums
  const catMap = new Map<string, number>()
  if (meta) {
    for (const c of meta.categories) {
      if (state.venue && c.venue !== state.venue) continue
      catMap.set(c.category, (catMap.get(c.category) ?? 0) + c.n)
    }
  }
  const cats = [...catMap.entries()].sort((a, b) => b[1] - a[1])
  if (state.category && !catMap.has(state.category)) {
    cats.unshift([state.category, 0])
  }

  const num = (key: keyof ScreenState, label: string, placeholder: string) => (
    <label className="f-num">
      <span>{label}</span>
      <DebouncedInput
        ariaLabel={label}
        value={state[key] as string}
        placeholder={placeholder}
        onCommit={(v) => update({ [key]: v.trim() })}
      />
    </label>
  )

  return (
    <aside className="rail">
      <div className="rail-section">
        <button
          className={`wl-toggle${state.watchlist ? ' on' : ''}`}
          onClick={() => update({ watchlist: !state.watchlist })}
          title="scope the table to starred markets"
        >
          ★ Watchlist <span className="wl-count">{watchCount}</span>
        </button>
        {state.watchlist && (
          <div className="rail-note">
            scoped to starred rows — filters &amp; pagination off, sort is local
          </div>
        )}
      </div>

      <div className="rail-section">
        <div className="rail-head">scans</div>
        <div className="scan-list">
          {SCANS.map((s) => (
            <button
              key={s.label}
              className={`scan-btn${scanActive(state, s) ? ' on' : ''}`}
              onClick={() => update(s.patch)}
            >
              {s.label}
            </button>
          ))}
        </div>
      </div>

      <div className="rail-section">
        <div className="rail-head">search</div>
        <DebouncedInput
          className="q-input"
          ariaLabel="search markets"
          inputRef={searchRef}
          value={state.q}
          placeholder="/ title, id, slug…"
          onCommit={(v) => update({ q: v.trim() })}
        />
      </div>

      <div className="rail-section">
        <div className="rail-head">venue</div>
        <div className="seg">
          {([['', 'All'], ['kalshi', 'Kalshi'], ['polymarket', 'Polym.']] as const).map(([v, label]) => (
            <button
              key={v || 'all'}
              className={state.venue === v ? 'on' : undefined}
              onClick={() => update({ venue: v })}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      <div className="rail-section">
        <div className="rail-head">category</div>
        <select
          className="f-select"
          aria-label="category"
          value={state.category}
          onChange={(e) => update({ category: e.target.value })}
        >
          <option value="">all categories</option>
          {cats.map(([name, n]) => (
            <option key={name} value={name}>
              {name}{n > 0 ? ` (${fmtCount(n)})` : ''}
            </option>
          ))}
        </select>
      </div>

      <div className="rail-section">
        <div className="rail-head">status</div>
        <select
          className="f-select"
          aria-label="status"
          value={state.status}
          onChange={(e) => update({ status: e.target.value as ScreenState['status'] })}
        >
          <option value="open">open</option>
          <option value="closed">closed</option>
          <option value="settled">settled</option>
          <option value="all">all</option>
        </select>
      </div>

      <div className="rail-section">
        <div className="rail-head">numeric filters</div>
        <div className="f-num-pair">
          {num('price_min', 'price ≥', '0.00')}
          {num('price_max', 'price ≤', '1.00')}
        </div>
        {num('vol24h_min', 'min 24h vol', '0')}
        {num('liquidity_min', 'min liquidity', '0')}
        {num('spread_max', 'max spread', '0.05')}
        {num('close_within_h', 'closes within (h)', '24')}
        <label className="f-check">
          <input
            type="checkbox"
            checked={state.exclude_churn}
            onChange={(e) => update({ exclude_churn: e.target.checked })}
          />
          <span>exclude churn (5m/15m/hourly)</span>
        </label>
      </div>

      <div className="rail-section">
        <button
          className="reset-btn"
          onClick={() =>
            update({
              ...DEFAULT_STATE,
              // keep UI-only state: view tab, drawer, watchlist scope
              view: state.view,
              sel: state.sel,
              watchlist: state.watchlist,
            })
          }
        >
          reset filters
        </button>
      </div>
    </aside>
  )
}
