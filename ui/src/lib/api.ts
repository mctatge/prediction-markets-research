// Thin fetch layer over the documented research API.
// All requests are relative (/api/...) — vite dev proxies to 127.0.0.1:8100,
// prod is same-origin under FastAPI.

import type {
  DecisionsResponse,
  HistoryResponse,
  MarketResponse,
  MetaResponse,
  ResearchResponse,
  ScreenerResponse,
  ScreenerRow,
} from './types'
import type { ScreenState } from './urlState'
import { splitMarketKey } from './urlState'

export const PAGE_SIZE = 100

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

async function getJson<T>(url: string, signal?: AbortSignal): Promise<T> {
  const res = await fetch(url, { signal })
  if (!res.ok) throw new ApiError(res.status, `${res.status} ${res.statusText} — ${url}`)
  return (await res.json()) as T
}

/** Map screen state to contract query params (names are contract-exact). */
export function buildScreenerQuery(s: ScreenState, limit: number, offset: number): string {
  const p = new URLSearchParams()
  if (s.venue) p.set('venue', s.venue)
  if (s.category) p.set('category', s.category)
  if (s.q) p.set('q', s.q)
  if (s.status !== 'open') p.set('status', s.status) // "open" is the server default
  if (s.price_min) p.set('price_min', s.price_min)
  if (s.price_max) p.set('price_max', s.price_max)
  if (s.vol24h_min) p.set('vol24h_min', s.vol24h_min)
  if (s.liquidity_min) p.set('liquidity_min', s.liquidity_min)
  if (s.spread_max) p.set('spread_max', s.spread_max)
  if (s.close_within_h) p.set('close_within_h', s.close_within_h)
  if (!s.exclude_churn) p.set('exclude_churn', 'false') // true is the server default
  p.set('sort', s.sort)
  p.set('dir', s.dir)
  p.set('limit', String(limit))
  if (offset > 0) p.set('offset', String(offset))
  return p.toString()
}

export function fetchScreener(
  s: ScreenState,
  signal?: AbortSignal,
): Promise<ScreenerResponse> {
  const qs = buildScreenerQuery(s, PAGE_SIZE, s.page * PAGE_SIZE)
  return getJson<ScreenerResponse>(`/api/screener?${qs}`, signal)
}

export function fetchMarket(
  venue: string,
  marketId: string,
  signal?: AbortSignal,
): Promise<MarketResponse> {
  return getJson<MarketResponse>(
    `/api/market/${encodeURIComponent(venue)}/${encodeURIComponent(marketId)}`,
    signal,
  )
}

export function fetchHistory(
  venue: string,
  marketId: string,
  hours: number,
  signal?: AbortSignal,
): Promise<HistoryResponse> {
  return getJson<HistoryResponse>(
    `/api/market/${encodeURIComponent(venue)}/${encodeURIComponent(marketId)}/history?hours=${hours}`,
    signal,
  )
}

export function fetchMeta(signal?: AbortSignal): Promise<MetaResponse> {
  return getJson<MetaResponse>('/api/meta', signal)
}

/**
 * Watchlist scope: server pagination can't scope to stars, so each starred
 * market is fetched individually (bounded concurrency, cap enforced by the
 * caller). Stars that 404 (delisted) are silently skipped.
 */
export async function fetchWatchlistRows(
  keys: readonly string[],
  signal?: AbortSignal,
): Promise<ScreenerRow[]> {
  const CONCURRENCY = 8
  const out: (ScreenerRow | null)[] = new Array(keys.length).fill(null)
  let next = 0
  async function worker() {
    while (next < keys.length) {
      const i = next++
      const parsed = splitMarketKey(keys[i]!)
      if (!parsed) continue
      try {
        const res = await fetchMarket(parsed.venue, parsed.marketId, signal)
        out[i] = res.row
      } catch (e) {
        if (signal?.aborted) throw e
        if (e instanceof ApiError && e.status === 404) continue // delisted star
        throw e
      }
    }
  }
  await Promise.all(
    Array.from({ length: Math.min(CONCURRENCY, keys.length) }, () => worker()),
  )
  return out.filter((r): r is ScreenerRow => r !== null)
}

/** Paper decision ledger. */
export async function fetchDecisions(signal?: AbortSignal): Promise<DecisionsResponse> {
  return getJson<DecisionsResponse>('/api/decisions?limit=200', signal)
}

/** Saved wallet-skill research report; loading it never starts collection. */
export function fetchResearch(signal?: AbortSignal): Promise<ResearchResponse> {
  return getJson<ResearchResponse>('/api/research', signal)
}
