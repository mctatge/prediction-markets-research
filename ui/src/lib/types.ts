// Response shapes are documented in src/pmkt/ui/API_CONTRACT.md.
// All prices/probabilities are decimal STRINGS, never floats.
// Timestamps are unix seconds UTC. null = unknown / not-applicable.

export type Venue = 'kalshi' | 'polymarket'

export interface ScreenerRow {
  venue: Venue
  market_id: string
  event_id: string | null
  series_id: string | null
  title: string
  category: string | null
  status: string
  yes_bid: string | null
  yes_ask: string | null
  last: string | null
  mid: string | null
  spread: string | null
  volume: string | null
  volume_24h: string | null
  open_interest: string | null
  liquidity: string | null
  close_ts: number | null
  prices_updated_ts: number | null
  recurrence: string | null
  slug: string | null
  model_p: string | null // experimental estimate; null when unavailable
  edge: string | null // model estimate minus market baseline
  exec_edge: string | null // estimate after quote crossing and modeled fees
  skilled_ofi: string | null // signed OFI from skilled wallets, PM-only
  n_skilled_trades: number | null
  url: string
}

export interface ScreenerResponse {
  total: number
  generated_at: number
  rows: ScreenerRow[]
}

export interface MarketResponse {
  row: ScreenerRow
  siblings: ScreenerRow[]
}

export interface HistoryPoint {
  t: number
  p: string
}

export interface HistoryResponse {
  points: HistoryPoint[]
}

export interface MetaCategory {
  venue: Venue
  category: string
  n: number
}

export interface MetaFreshness {
  venue: Venue
  endpoint: string
  watermark_ts: number | null
}

export interface MetaResponse {
  data_mode?: 'synthetic' | 'local'
  venues: Venue[]
  categories: MetaCategory[]
  counts: Record<string, number>
  freshness: MetaFreshness[]
}

// ---- /api/decisions (paper decision ledger) ----

export interface DecisionGrade {
  won: boolean
  ret_per_dollar: string
  ev_gap: string | null
  entry_prob: string
}

export interface DecisionRow {
  decision_id: number
  ts: number
  venue: Venue
  market_id: string
  title: string | null
  category: string | null
  event_key: string | null
  signal_venue_id: string | null
  signal_market_id: string | null
  action: string
  side: string
  ev: string | null
  model_p: string | null
  bid: string | null
  ask: string | null
  spread: string | null
  skilled_ofi: string | null
  n_skilled_trades: number | null
  ofi_age_s: number | null
  gates: Record<string, boolean>
  passed_gates: boolean
  theta: string
  graded: DecisionGrade | null
}

export interface DecisionsSummary {
  decisions: number
  passed_gates: number
  graded: number
  last_pass_ts: number | null
  win_rate: number | null
  mean_ret_per_dollar: number | null
  equity: { ts: number; cum_ret: number }[]
}

export interface DecisionsResponse {
  generated_at: number
  summary: DecisionsSummary
  rows: DecisionRow[]
}

export interface ResearchWallet {
  wallet: string
  n_trades: number
  n_markets: number
  total_volume: string
  actual_pnl: string
  sigma: string
  z_score: string
  scorer_version: number
  selected: boolean
}

export interface ResearchResponse {
  data_mode: 'synthetic' | 'local'
  as_of_ts?: number
  skill_cutoff_ts?: number
  selection_rule?: string
  wallets: ResearchWallet[]
  selected_wallets: string[]
  flows: { venue_market_id: string }[]
  decisions: { venue_market_id: string }[]
  grading: { graded?: number } | null
  limitations?: string[]
}
