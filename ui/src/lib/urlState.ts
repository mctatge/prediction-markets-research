// Screen state — filters, sort, page,
// view tab, watchlist scope, drawer selection — serializes to query params.
// Param names deliberately mirror the API contract where one exists.

export type SortKey =
  | 'vol24h'
  | 'volume'
  | 'price'
  | 'spread'
  | 'close_ts'
  | 'open_interest'
  | 'liquidity'
  | 'updated'
  | 'title'
  | 'edge'
  | 'exec_edge'
  | 'skilled_ofi'

export type SortDir = 'asc' | 'desc'
export type StatusFilter = 'open' | 'closed' | 'settled' | 'all'
export type ViewTab = 'skills' | 'prices' | 'liquidity' | 'edge' | 'flow' | 'decisions'
export type VenueFilter = '' | 'kalshi' | 'polymarket'

export interface ScreenState {
  venue: VenueFilter
  category: string
  q: string
  status: StatusFilter
  price_min: string
  price_max: string
  vol24h_min: string
  liquidity_min: string
  spread_max: string
  close_within_h: string
  exclude_churn: boolean
  sort: SortKey
  dir: SortDir
  page: number // 0-based
  view: ViewTab
  watchlist: boolean
  sel: string // "venue:market_id" or ''
}

export const DEFAULT_STATE: ScreenState = {
  venue: '',
  category: '',
  q: '',
  status: 'open',
  price_min: '',
  price_max: '',
  vol24h_min: '',
  liquidity_min: '',
  spread_max: '',
  close_within_h: '',
  exclude_churn: true,
  sort: 'vol24h',
  dir: 'desc',
  page: 0,
  view: 'skills',
  watchlist: false,
  sel: '',
}

const SORT_KEYS: readonly SortKey[] = [
  'vol24h', 'volume', 'price', 'spread', 'close_ts',
  'open_interest', 'liquidity', 'updated', 'title', 'edge', 'exec_edge', 'skilled_ofi',
]
const STATUSES: readonly StatusFilter[] = ['open', 'closed', 'settled', 'all']
const VIEWS: readonly ViewTab[] = ['skills', 'prices', 'liquidity', 'edge', 'flow', 'decisions']

/** Keys whose change invalidates the current page (page resets to 0). */
export const PAGE_RESET_KEYS: readonly (keyof ScreenState)[] = [
  'venue', 'category', 'q', 'status', 'price_min', 'price_max',
  'vol24h_min', 'liquidity_min', 'spread_max', 'close_within_h',
  'exclude_churn', 'sort', 'dir', 'watchlist',
]

/** Serialize to a query string, omitting defaults. '' when fully default. */
export function serializeState(s: ScreenState): string {
  const p = new URLSearchParams()
  if (s.venue) p.set('venue', s.venue)
  if (s.category) p.set('category', s.category)
  if (s.q) p.set('q', s.q)
  if (s.status !== DEFAULT_STATE.status) p.set('status', s.status)
  if (s.price_min) p.set('price_min', s.price_min)
  if (s.price_max) p.set('price_max', s.price_max)
  if (s.vol24h_min) p.set('vol24h_min', s.vol24h_min)
  if (s.liquidity_min) p.set('liquidity_min', s.liquidity_min)
  if (s.spread_max) p.set('spread_max', s.spread_max)
  if (s.close_within_h) p.set('close_within_h', s.close_within_h)
  if (!s.exclude_churn) p.set('exclude_churn', 'false')
  if (s.sort !== DEFAULT_STATE.sort) p.set('sort', s.sort)
  if (s.dir !== DEFAULT_STATE.dir) p.set('dir', s.dir)
  if (s.page > 0) p.set('page', String(s.page))
  if (s.view !== DEFAULT_STATE.view) p.set('view', s.view)
  if (s.watchlist) p.set('wl', '1')
  if (s.sel) p.set('sel', s.sel)
  return p.toString()
}

/** Parse a query string (with or without leading '?'). Tolerant of garbage. */
export function parseState(query: string): ScreenState {
  const p = new URLSearchParams(query.startsWith('?') ? query.slice(1) : query)
  const s: ScreenState = { ...DEFAULT_STATE }

  const venue = p.get('venue')
  if (venue === 'kalshi' || venue === 'polymarket') s.venue = venue
  s.category = p.get('category') ?? ''
  s.q = p.get('q') ?? ''
  const status = p.get('status') as StatusFilter | null
  if (status && STATUSES.includes(status)) s.status = status
  s.price_min = cleanNum(p.get('price_min'))
  s.price_max = cleanNum(p.get('price_max'))
  s.vol24h_min = cleanNum(p.get('vol24h_min'))
  s.liquidity_min = cleanNum(p.get('liquidity_min'))
  s.spread_max = cleanNum(p.get('spread_max'))
  s.close_within_h = cleanNum(p.get('close_within_h'))
  if (p.get('exclude_churn') === 'false') s.exclude_churn = false
  const sort = p.get('sort') as SortKey | null
  if (sort && SORT_KEYS.includes(sort)) s.sort = sort
  const dir = p.get('dir')
  if (dir === 'asc' || dir === 'desc') s.dir = dir
  const page = parseInt(p.get('page') ?? '', 10)
  if (Number.isFinite(page) && page > 0) s.page = page
  const view = p.get('view') as ViewTab | null
  if (view && VIEWS.includes(view)) s.view = view
  if (p.get('wl') === '1') s.watchlist = true
  s.sel = p.get('sel') ?? ''
  return s
}

function cleanNum(v: string | null): string {
  if (v === null) return ''
  return /^-?\d*\.?\d+$/.test(v) ? v : ''
}

/** Apply a patch; page auto-resets when any filter/sort key changes. */
export function applyPatch(prev: ScreenState, patch: Partial<ScreenState>): ScreenState {
  const next = { ...prev, ...patch }
  if (!('page' in patch)) {
    const touched = PAGE_RESET_KEYS.some(
      (k) => k in patch && patch[k] !== prev[k],
    )
    if (touched) next.page = 0
  }
  return next
}

/** Watchlist / selection key: "venue:market_id". */
export function marketKey(venue: string, marketId: string): string {
  return `${venue}:${marketId}`
}

/** Split a "venue:market_id" key at the FIRST colon (ids may contain colons). */
export function splitMarketKey(key: string): { venue: string; marketId: string } | null {
  const i = key.indexOf(':')
  if (i <= 0 || i === key.length - 1) return null
  return { venue: key.slice(0, i), marketId: key.slice(i + 1) }
}
