// Each view selects columns over the same market rows.
// Unavailable model estimates render as an em dash.

import type { ReactNode } from 'react'
import type { ScreenerRow } from '../lib/types'
import type { SortKey, ViewTab } from '../lib/urlState'
import {
  decimalSign,
  fmtAgo,
  fmtCloseIn,
  fmtPrice,
  fmtQty,
} from '../lib/format'
import { categoryGlyph } from '../lib/categoryGlyph'
import { VenueLogo } from './VenueLogo'

export interface ColumnCtx {
  nowSec: number
}

export interface ColumnDef {
  id: string
  label: string
  headerTitle?: string
  sortKey?: SortKey
  align: 'left' | 'right'
  /** row fields whose change should pulse this cell on auto-refresh */
  watch: (keyof ScreenerRow)[]
  render: (row: ScreenerRow, ctx: ColumnCtx) => ReactNode
}

function priceCell(v: string | null): ReactNode {
  const { text, full } = fmtPrice(v)
  return (
    <span className={full === null ? 'null-val' : undefined} title={full ?? undefined}>
      {text}
    </span>
  )
}

function signedPriceCell(v: string | null): ReactNode {
  const { text, full } = fmtPrice(v)
  if (full === null) return <span className="null-val">—</span>
  const sign = decimalSign(v)
  const cls = sign > 0 ? 'up' : sign < 0 ? 'down' : undefined
  return (
    <span className={cls} title={full}>
      {sign > 0 ? '+' : ''}
      {text}
    </span>
  )
}

function qtyCell(v: string | null): ReactNode {
  const { text, full } = fmtQty(v)
  return (
    <span className={full === null ? 'null-val' : undefined} title={full ?? undefined}>
      {text}
    </span>
  )
}

const venue: ColumnDef = {
  id: 'venue',
  label: 'Ven',
  align: 'left',
  watch: [],
  render: (r) => <VenueLogo venue={r.venue} size={16} />,
}

const title: ColumnDef = {
  id: 'title',
  label: 'Market',
  sortKey: 'title',
  align: 'left',
  watch: [],
  render: (r) => (
    <div className="cell-title">
      <span className="cat-glyph" aria-hidden="true">{categoryGlyph(r.category)}</span>
      <div className="cell-title-text">
        <div className="mkt-title" title={r.title}>
          {r.title}
        </div>
        <div className="mkt-id" title={r.market_id}>
          {r.market_id}
        </div>
      </div>
    </div>
  ),
}

const category: ColumnDef = {
  id: 'category',
  label: 'Cat',
  align: 'left',
  watch: [],
  render: (r) => <span className="cat">{r.category ?? '—'}</span>,
}

const yesBid: ColumnDef = {
  id: 'yes_bid', label: 'Bid', align: 'right', watch: ['yes_bid'],
  render: (r) => priceCell(r.yes_bid),
}
const yesAsk: ColumnDef = {
  id: 'yes_ask', label: 'Ask', align: 'right', watch: ['yes_ask'],
  render: (r) => priceCell(r.yes_ask),
}
const mid: ColumnDef = {
  id: 'mid', label: 'Mid', sortKey: 'price', align: 'right', watch: ['mid'],
  render: (r) => priceCell(r.mid),
}
const last: ColumnDef = {
  id: 'last', label: 'Last', align: 'right', watch: ['last'],
  render: (r) => priceCell(r.last),
}
const spread: ColumnDef = {
  id: 'spread', label: 'Sprd', sortKey: 'spread', align: 'right', watch: ['spread'],
  render: (r) => priceCell(r.spread),
}
const modelP: ColumnDef = {
  id: 'model_p', label: 'Est. p̂',
  headerTitle: 'Experimental probability estimate; not a validated forecast. Blank when unavailable.',
  align: 'right', watch: ['model_p'],
  render: (r) => priceCell(r.model_p),
}
const edge: ColumnDef = {
  id: 'edge', label: 'Model Δ', sortKey: 'edge',
  headerTitle: 'Estimate minus market baseline; absolute difference sort. Excludes spreads and fees; not realized profit.',
  align: 'right', watch: ['edge'],
  render: (r) => signedPriceCell(r.edge),
}
const execEdge: ColumnDef = {
  id: 'exec_edge', label: 'Cost adj.', sortKey: 'exec_edge',
  headerTitle: 'Experimental signed estimate after quote crossing and modeled fees. Does not establish achievable returns or fills.',
  align: 'right', watch: ['exec_edge'],
  render: (r) => signedPriceCell(r.exec_edge),
}
const vol24h: ColumnDef = {
  id: 'vol24h', label: '24h Vol', sortKey: 'vol24h', align: 'right', watch: ['volume_24h'],
  render: (r) => qtyCell(r.volume_24h),
}
const volume: ColumnDef = {
  id: 'volume', label: 'Vol', sortKey: 'volume', align: 'right', watch: ['volume'],
  render: (r) => qtyCell(r.volume),
}
const openInterest: ColumnDef = {
  id: 'open_interest', label: 'OI', sortKey: 'open_interest', align: 'right', watch: ['open_interest'],
  render: (r) => qtyCell(r.open_interest),
}
const liquidity: ColumnDef = {
  id: 'liquidity', label: 'Liq', sortKey: 'liquidity', align: 'right', watch: ['liquidity'],
  render: (r) => qtyCell(r.liquidity),
}
const closes: ColumnDef = {
  id: 'closes', label: 'Closes', sortKey: 'close_ts', align: 'right', watch: ['close_ts'],
  render: (r, ctx) => {
    const c = fmtCloseIn(r.close_ts, ctx.nowSec)
    const cls = c.urgent ? 'urgent' : c.past || r.close_ts === null ? 'null-val' : undefined
    return <span className={cls}>{c.text}</span>
  },
}
const updated: ColumnDef = {
  id: 'updated', label: 'Upd', sortKey: 'updated', align: 'right', watch: [],
  render: (r, ctx) => {
    const a = fmtAgo(r.prices_updated_ts, ctx.nowSec)
    const cls =
      r.prices_updated_ts === null ? 'null-val' : a.ageSec > 1800 ? 'stale-warn' : undefined
    return <span className={cls}>{a.text}</span>
  },
}
const skilledOfi: ColumnDef = {
  id: 'skilled_ofi', label: 'Sk OFI', sortKey: 'skilled_ofi',
  headerTitle: 'Order-flow imbalance from wallets selected by historical skill scores: +1 = net YES buying. Selection does not prove future skill.',
  align: 'right', watch: ['skilled_ofi'],
  render: (r) => signedPriceCell(r.skilled_ofi),
}
const nSkilled: ColumnDef = {
  id: 'n_skilled', label: 'Sk #',
  headerTitle: 'Number of trades from selected wallets in the 24-hour window',
  align: 'right', watch: ['n_skilled_trades'],
  render: (r) => {
    const n = r.n_skilled_trades
    if (n === null || n === undefined) return <span className="null-val">—</span>
    return <span>{n}</span>
  },
}

export const VIEW_COLUMNS: Record<ViewTab, ColumnDef[]> = {
  skills: [],
  prices: [
    venue, title, category, yesBid, yesAsk, mid, spread, modelP, edge,
    vol24h, openInterest, liquidity, closes, updated,
  ],
  liquidity: [
    venue, title, category, vol24h, volume, openInterest, liquidity,
    spread, yesBid, yesAsk, closes, updated,
  ],
  edge: [
    venue, title, category, modelP, edge, execEdge, skilledOfi, nSkilled, mid,
    yesBid, yesAsk, spread, last, vol24h, closes, updated,
  ],
  flow: [
    venue, title, category, skilledOfi, nSkilled, mid, spread,
    vol24h, liquidity, closes, updated,
  ],
  // The Decisions view renders its own panel (DecisionsPanel), not the
  // screener table — no market-row columns.
  decisions: [],
}

export const VIEW_LABELS: Record<ViewTab, string> = {
  skills: 'Wallet skill',
  prices: 'Prices',
  liquidity: 'Liquidity',
  edge: 'Model estimates',
  flow: 'Skilled flow',
  decisions: 'Paper decisions',
}
