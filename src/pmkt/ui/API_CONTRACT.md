# Screener UI — API contract v1 (FROZEN 2026-07-12)

Both sides build against THIS file. Backend = FastAPI in `src/pmkt/ui/`;
frontend = React/Vite in `ui/`. Any change needs both sides updated together.

Conventions: all prices/probabilities are **decimal STRINGS** ("0.4200"),
never JSON floats (data invariants — the frontend renders them verbatim and may
parse for comparison only). Timestamps are unix seconds UTC (integers).
`null` = unknown/not-applicable.

**model_p / edge (v1.1, 2026-07-12; OFI-based edge 2026-07-14):** computed
LIVE from the CURRENT price at request time. Two model sources:
- **Kalshi:** FLB calibration curve via `pmkt_model_p` UDF (currently shadow
  → null; will activate when a non-shadow retrain passes the validation gate).
- **Polymarket:** OFI-adjusted probability = `base + α(category) × skilled_ofi`,
  clamped to [0,1]. Alpha is per-category via the `pmkt_ofi_alpha` UDF — see
  `src/pmkt/ui/ofi_live.py` (corrected map 2026-07-16; the original flat α=0.05
  and its "−4.2% Brier" basis were downgraded 2026-07-22 — see
  `docs/findings.md`). Populated only for markets with ≥3 skilled
  trades in the 24h window; `null` otherwise.
`base` = mid when the book is real (spread ≤ 0.20) else last; `edge` =
model_p − base, signed 4-dp string, RAW — fees/spread not deducted.
`exec_edge` = fee-adjusted executable edge: `model_p − ask − taker_fee`
(buy YES) or `model_p − bid + taker_fee` (buy NO), signed 4-dp string.
Positive = modeled YES margin after estimated costs; negative = modeled NO margin
(or costs exceeded the raw difference). These are experimental estimates, not
certified executable returns. NULL when model_p or the needed quote
side is missing. Taker fees: Kalshi 0.07 × P(1−P); Polymarket rate × P(1−P)
where rate depends on category (0.04–0.07).
`sort=edge` orders by **|edge|** (biggest disagreement first);
`sort=exec_edge` orders by **|exec_edge|** (biggest surviving edge first).
Forecast-less rows always last.

## GET /api/screener
Query params (all optional):
- `venue`: "kalshi" | "polymarket"
- `category`: exact match (from /api/meta)
- `q`: case-insensitive substring over title + market_id + event id + slug
- `status`: default "open"; also "closed" | "settled" | "all"
- `price_min`, `price_max`: floats 0-1 applied to mid (fallback last)
- `vol24h_min`, `liquidity_min`: floats, venue-native units
- `spread_max`: float 0-1 (yes_ask - yes_bid); rows with unknown spread pass
- `close_within_h`: float — close_ts within N hours from now (excludes null)
- `exclude_churn`: default true — drops recurrence in ('5m','15m','hourly')
- `sort`: "vol24h" (default) | "volume" | "price" | "spread" | "close_ts" |
          "open_interest" | "liquidity" | "updated" | "title" | "edge" (|edge| ordering) |
          "exec_edge" (|exec_edge| ordering)
- `dir`: "desc" (default) | "asc". NULLs always sort last regardless of dir.
- `limit`: int ≤ 200 (default 100); `offset`: int ≥ 0
Response 200:
```json
{ "total": 12345, "generated_at": 1783830000, "rows": [ScreenerRow, ...] }
```
ScreenerRow:
```json
{
  "venue": "kalshi", "market_id": "KXHIGHNY-26JUL12-B55",
  "event_id": "KXHIGHNY-26JUL12", "series_id": "KXHIGHNY",
  "title": "...", "category": "Climate", "status": "open",
  "yes_bid": "0.4200", "yes_ask": "0.4400", "last": "0.4300",
  "mid": "0.4300", "spread": "0.0200",
  "volume": "1523.00", "volume_24h": "410.00",
  "open_interest": "988.00", "liquidity": "12345.6789",
  "close_ts": 1783900000, "prices_updated_ts": 1783829000,
  "recurrence": null, "slug": null,
  "model_p": null, "edge": null, "exec_edge": null,
  "url": "https://kalshi.com/markets/KXHIGHNY"
}
```
`url`: kalshi → `https://kalshi.com/markets/{series_id}`;
polymarket → `https://polymarket.com/market/{slug}` (slug null → event slug
fallback → `https://polymarket.com`).

## GET /api/market/{venue}/{market_id}
Response 200: `{ "row": ScreenerRow, "siblings": [ScreenerRow, ...] }`
(siblings = same venue_event_id, screener-shaped, sorted by mid desc,
including the row itself). 404 if unknown.

## GET /api/market/{venue}/{market_id}/history?hours=24
Price history for the detail chart, from the raw lake via DuckDB (kalshi:
candle yes-bid/ask/trade closes; polymarket: prices-history minute points).
Response 200: `{ "points": [{"t": 1783820000, "p": "0.4300"}, ...] }`
sorted by t asc, ≤ 2000 points (server thins evenly if more). Empty list if
none. `hours` ≤ 168, default 24. This endpoint may be slow (seconds) — the
frontend must render the drawer immediately and fill the chart async.

## GET /api/meta

Additional fields: `data_mode` is `"synthetic"` when a valid saved demo report
identifies the workspace, otherwise `"local"`. `setup_required` is true when
there is no local hot store; then categories, counts and freshness are empty,
and `setup_hint` explains how to create an offline demo. This endpoint never
initializes a database or starts a collector.
```json
{
  "venues": ["kalshi", "polymarket"],
  "categories": [{"venue": "kalshi", "category": "Politics", "n": 123}, ...],
  "counts": {"open_total": 113000, "open_kalshi": 65000, "open_polymarket": 48000},
  "freshness": [{"venue": "kalshi", "endpoint": "prices", "watermark_ts": 1783829000}, ...]
}
```
(categories only where status open, sorted by n desc, top 40.)

## GET /api/research

Read-only saved research stages. This endpoint never calls a venue, fits a
model or writes data. With no saved demo report it returns:

```json
{"data_mode":"local","wallets":[],"selected_wallets":[],"flows":[],"decisions":[],"grading":null,"limitations":[]}
```

Synthetic demo responses have `schema_version: 1`, `data_mode: "synthetic"`,
`paper_only: true`, `as_of_ts`, `skill_cutoff_ts`, `selection_rule`,
`network_requests: 0`, `live_orders: 0`, `limitations: string[]`, and:

- `wallets`: `{wallet, n_trades, n_markets, total_volume, actual_pnl, sigma,
  z_score, scorer_version, selected}`. Counts/version are integers; `selected`
  is boolean; volume, PnL, sigma and z score are decimal strings.
- `selected_wallets`: wallet identifier strings. All training fills,
  resolutions and observed labels strictly precede the selection cutoff.
- `flows`: `{venue_market_id, skilled_ofi, unskilled_ofi, anti_ofi,
  z_weighted_ofi, skilled_volume, total_volume, n_skilled_trades, n_total_trades}`.
  Counts are integers; measurements are decimal strings or null. Flow contains
  only fills observed after selection and no later than the decision.
- `decisions`: frozen paper rows with `venue_market_id`, `action`, `side`,
  `bid_prob`, `ask_prob`, `fee_prob`, `ev`, `quote_ts`, `observed_ts`,
  `leader_price`, `gates`, `quote_source: "synthetic"`, and `paper_only: true`.
  Prices/estimated margin are decimal strings or null. Timestamps are UTC unix
  seconds; gates are booleans. Follower entries use later bid/ask quotes,
  separate from the leader's historical fill. A BUY action is a paper record.
- `grading`: outcome-grader batch summary, including `graded`,
  `mean_ret_per_dollar`, `win_rate`, and nullable diagnostics. Summary ratios
  are floats for display. `grades` contains frozen per-decision decimal-string
  entry/return values; outcomes are introduced after decision records.
- `evidence`: counts of source and eligible history/flow rows, plus synthetic
  `resolution_available_ts`.

When no hot store exists, `/api/screener` and `/api/decisions` return their
normal empty shapes with HTTP 200; `/api/meta` provides setup guidance.

## GET /api/decisions  (v1.4, 2026-07-20)
Shadow decision ledger (Decisions tab). Params: `limit` (default 200, max
500), `graded_only` (default false). Response:
```json
{
  "generated_at": 1784230000,
  "summary": {
    "decisions": 123, "passed_gates": 100, "graded": 40,
    "last_pass_ts": 1784229000, "win_rate": 0.55,
    "mean_ret_per_dollar": 0.012,
    "equity": [{"ts": 1784200000, "cum_ret": 0.31}, ...]
  },
  "rows": [{
    "decision_id": 1, "ts": 1784229000,
    "venue": "kalshi", "market_id": "KX…",
    "title": "…", "category": "Sports",
    "event_key": "KXMLBGAME-26JUL20DETPHI",
    "signal_venue_id": "polymarket", "signal_market_id": "0x…",
    "action": "PASS", "side": "BUY_YES",
    "ev": "-0.0180", "model_p": "0.5660",
    "bid": "0.5000", "ask": "0.5200", "spread": "0.0200",
    "skilled_ofi": "0.8000", "n_skilled_trades": 5, "ofi_age_s": 60,
    "gates": {"spread_ok": true, ...}, "passed_gates": true,
    "theta": "inf",
    "graded": {"won": true, "ret_per_dollar": "0.8721",
                "ev_gap": "0.4340", "entry_prob": "0.5200"} | null
  }]
}
```
Rows newest-first. `venue` is the execution venue of that decision.
`event_key` (nullable) ties a PM decision and its paired Kalshi decision for
the same matched event — both legs carry it (the UI groups them). For a Kalshi
row the signal is PM-sourced: `signal_venue_id="polymarket"` +
`signal_market_id` = the PM conditionId whose skilled OFI drove the call
(PM rows carry their own id). `side` is the indicated side even for PASS rows
(any hypothetical θ can be replayed). Numbers are 4-dp decimal strings
(data invariants); summary aggregates are floats (display only).

## Serving
- Backend: `uv run pmkt ui` → uvicorn on 127.0.0.1:8100, hot store opened
  READ-ONLY (sqlite URI mode=ro) — the UI must never be a second writer.
  CORS: allow http://localhost:5173 (vite dev).
- Prod: FastAPI serves `ui/dist/` at `/` when it exists (SPA fallback to
  index.html for non-/api paths).
- Frontend dev: vite on 5173, proxy `/api` → 8100.
