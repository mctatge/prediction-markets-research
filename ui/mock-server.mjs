#!/usr/bin/env node
// Synthetic development backend for the research UI.
// Zero dependencies. Serves local endpoints on 127.0.0.1:8100 with a
// deterministic ~16k-row synthetic universe, including the awkward cases:
//   - null-price rows (yes_bid/ask/mid/spread null)
//   - a polymarket row with null slug (url falls back to https://polymarket.com)
//   - churn rows (recurrence 5m/15m/hourly) excluded by default, 'daily' NOT excluded
//   - >15k total for pagination
//   - /history responds after a 1.5s delay (the real endpoint can be slow)
// Run: node mock-server.mjs   (or: npm run mock)

import http from 'node:http'

const PORT = Number(process.env.MOCK_PORT || 8100)
const NOW = () => Math.floor(Date.now() / 1000)

// ---------- deterministic PRNG ----------
function mulberry32(a) {
  return function () {
    a |= 0; a = (a + 0x6D2B79F5) | 0
    let t = Math.imul(a ^ (a >>> 15), 1 | a)
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
const rnd = mulberry32(20260712)
const pick = (arr) => arr[Math.floor(rnd() * arr.length)]
const rint = (a, b) => a + Math.floor(rnd() * (b - a + 1))

// ---------- universe generation ----------
const KALSHI_CATS = ['Politics', 'Economics', 'Climate', 'Companies', 'Financials', 'Science', 'Sports', 'Culture', 'Crypto', 'Health']
const POLY_CATS = ['Politics', 'Sports', 'Crypto', 'Pop Culture', 'Business', 'Science', 'Tech', 'Elections']
const SUBJECTS = ['Fed decision', 'NYC high temp', 'BTC price', 'ETH price', 'CPI YoY', 'GDP growth', 'Senate control', 'House control', 'Oscars winner', 'World Cup', 'Hurricane count', 'OpenAI release', 'Apple event', 'Jobless claims', 'Oil price', 'S&P close', 'Rain in Chicago', 'Election turnout', 'Album release', 'Rocket launch', 'Trade deal', 'Rate cut', 'Heat wave', 'Snowfall NYC', 'Tesla deliveries']
const QUALIFIERS = ['above', 'below', 'at least', 'exceeds', 'hits', 'wins', 'announced by', 'before', 'by end of']
const MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
const SERIES_WORDS = ['HIGHNY', 'FED', 'CPI', 'BTCD', 'ETHW', 'SENATE', 'HOUSE', 'OSCAR', 'WCUP', 'HURR', 'GDPQ', 'OILW', 'SNP', 'RAINCHI', 'TURNOUT', 'LAUNCH', 'HEAT', 'SNOWNYC', 'TSLA', 'CLAIMS']

const mils = (m) => (m / 1000).toFixed(4) // integer thousandths -> "0.4200"
const qty = (v, dp = 2) => v.toFixed(dp)

function makeTitle() {
  const n = [55, 90, 130000, 2.9, 3.5, 12, 100, 250][rint(0, 7)]
  return `${pick(SUBJECTS)} ${pick(QUALIFIERS)} ${n}${rnd() < 0.4 ? '%' : ''} ${rnd() < 0.5 ? 'in 2026' : `on ${pick(MONTHS)} ${rint(1, 28)}`}`
}
const slugify = (s) => s.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')

function priceFields(status) {
  // ~3% unknown-price rows (contract: null = unknown)
  if (rnd() < 0.03) {
    const last = rnd() < 0.5 ? mils(rint(10, 990)) : null
    return { yes_bid: null, yes_ask: null, mid: null, spread: null, last }
  }
  if (status === 'settled') {
    const settle = rnd() < 0.5 ? 0 : 1000
    return { yes_bid: null, yes_ask: null, mid: null, spread: null, last: mils(settle) }
  }
  const midM = rint(15, 985)
  const half = pick([5, 5, 10, 10, 15, 20, 30, 50]) // spread/2 in mils
  const bid = Math.max(1, midM - half)
  const ask = Math.min(999, midM + half)
  return {
    yes_bid: mils(bid),
    yes_ask: mils(ask),
    mid: mils(Math.round((bid + ask) / 2)),
    spread: mils(ask - bid),
    last: rnd() < 0.9 ? mils(midM + rint(-8, 8)) : null,
  }
}

function commonFields(status, now) {
  const vol = Math.pow(rnd(), 3) * 500000
  const vol24 = vol * rnd() * 0.35
  let close_ts
  if (status === 'open') {
    close_ts = rnd() < 0.05 ? null : now + Math.floor(Math.pow(rnd(), 1.6) * 180 * 86400) + rint(300, 7200)
  } else {
    close_ts = now - rint(3600, 90 * 86400)
  }
  return {
    status,
    volume: qty(vol),
    volume_24h: status === 'open' ? qty(vol24) : qty(0),
    open_interest: qty(Math.pow(rnd(), 2) * 800000),
    liquidity: qty(Math.pow(rnd(), 2) * 120000, 4),
    close_ts,
    prices_updated_ts: now - (rnd() < 0.88 ? rint(5, 600) : rint(1800, 14000)),
    model_p: null,
    edge: null,
    exec_edge: null,
    skilled_ofi: null,
    n_skilled_trades: null,
  }
}

function genRows() {
  const now = NOW()
  const rows = []

  // ---- Kalshi: ~10500 rows across series/events ----
  let k = 0
  for (let s = 0; s < 700 && k < 10500; s++) {
    const word = `${SERIES_WORDS[s % SERIES_WORDS.length]}${s >= SERIES_WORDS.length ? Math.floor(s / SERIES_WORDS.length) : ''}`
    const series = `KX${word}`
    const category = pick(KALSHI_CATS)
    const nEvents = rint(2, 6)
    for (let e = 0; e < nEvents && k < 10500; e++) {
      const event = `${series}-26${pick(MONTHS)}${String(rint(1, 28)).padStart(2, '0')}${e ? `V${e}` : ''}`
      const nMkts = rint(1, 7)
      const baseTitle = makeTitle()
      for (let m = 0; m < nMkts && k < 10500; m++) {
        const status = rnd() < 0.84 ? 'open' : rnd() < 0.6 ? 'closed' : 'settled'
        const strike = rint(30, 95)
        rows.push({
          venue: 'kalshi',
          market_id: `${event}-${pick(['T', 'B'])}${strike}${m ? `.${m}` : ''}`,
          event_id: event,
          series_id: series,
          title: nMkts > 1 ? `${baseTitle} — ≥${strike}` : baseTitle,
          category,
          ...priceFields(status),
          ...commonFields(status, now),
          recurrence: rnd() < 0.04 ? 'daily' : null, // 'daily' is NOT churn-excluded
          slug: null,
          url: `https://kalshi.com/markets/${series}`,
        })
        k++
      }
    }
  }

  // ---- Kalshi churn: 900 recurring micro-markets (excluded by default) ----
  for (let i = 0; i < 900; i++) {
    const rec = pick(['5m', '15m', 'hourly'])
    const series = `KX${pick(['BTCUD', 'ETHUD', 'SOLUD'])}${rec.toUpperCase()}`
    const event = `${series}-26JUL12H${String(rint(0, 23)).padStart(2, '0')}M${i}`
    const status = rnd() < 0.9 ? 'open' : 'closed'
    rows.push({
      venue: 'kalshi',
      market_id: `${event}-U`,
      event_id: event,
      series_id: series,
      title: `${series.includes('BTC') ? 'BTC' : series.includes('ETH') ? 'ETH' : 'SOL'} up — ${rec} window #${i}`,
      category: 'Crypto',
      ...priceFields(status),
      ...commonFields(status, now),
      close_ts: status === 'open' ? now + rint(60, 3600) : now - rint(60, 86400),
      recurrence: rec,
      slug: null,
      url: `https://kalshi.com/markets/${series}`,
    })
  }

  // ---- Polymarket: ~10000 rows ----
  for (let i = 0; i < 10000; i++) {
    const eventNum = 40000 + Math.floor(i / 3)
    const status = rnd() < 0.82 ? 'open' : rnd() < 0.7 ? 'closed' : 'settled'
    const title = makeTitle()
    const slug = rnd() < 0.05 ? null : `${slugify(title)}-${500000 + i}` // ~5% null slug
    rows.push({
      venue: 'polymarket',
      market_id: String(500000 + i),
      event_id: `evt-${eventNum}`,
      series_id: null,
      title,
      category: pick(POLY_CATS),
      ...priceFields(status),
      ...commonFields(status, now),
      recurrence: rnd() < 0.02 ? pick(['5m', '15m', 'hourly']) : null,
      slug,
      url: slug ? `https://polymarket.com/market/${slug}` : 'https://polymarket.com',
    })
  }

  return rows
}

const ROWS = genRows()
const BY_KEY = new Map(ROWS.map((r) => [`${r.venue}:${r.market_id}`, r]))
const openCount = (venue) => ROWS.filter((r) => r.status === 'open' && (!venue || r.venue === venue)).length
console.log(`[mock] universe: ${ROWS.length} rows (${openCount('kalshi')} open kalshi, ${openCount('polymarket')} open polymarket)`)

// ---------- filtering / sorting per contract ----------
const num = (s) => {
  if (s === null || s === undefined) return null
  const v = parseFloat(s)
  return Number.isFinite(v) ? v : null
}
const priceOf = (r) => num(r.mid) ?? num(r.last)

const SORT_FNS = {
  vol24h: (r) => num(r.volume_24h),
  volume: (r) => num(r.volume),
  price: priceOf,
  spread: (r) => num(r.spread),
  close_ts: (r) => r.close_ts,
  open_interest: (r) => num(r.open_interest),
  liquidity: (r) => num(r.liquidity),
  updated: (r) => r.prices_updated_ts,
  title: (r) => r.title,
}

function screener(params) {
  const now = NOW()
  const status = params.get('status') || 'open'
  const venue = params.get('venue')
  const category = params.get('category')
  const q = (params.get('q') || '').toLowerCase()
  const priceMin = num(params.get('price_min'))
  const priceMax = num(params.get('price_max'))
  const vol24hMin = num(params.get('vol24h_min'))
  const liqMin = num(params.get('liquidity_min'))
  const spreadMax = num(params.get('spread_max'))
  const closeWithinH = num(params.get('close_within_h'))
  const churnParam = (params.get('exclude_churn') || '').toLowerCase()
  const excludeChurn = !(churnParam === 'false' || churnParam === '0')
  const CHURN = new Set(['5m', '15m', 'hourly'])

  let rows = ROWS.filter((r) => {
    if (status !== 'all' && r.status !== status) return false
    if (venue && r.venue !== venue) return false
    if (category && r.category !== category) return false
    if (excludeChurn && r.recurrence !== null && CHURN.has(r.recurrence)) return false
    if (q) {
      const hay = `${r.title}\n${r.market_id}\n${r.event_id ?? ''}\n${r.slug ?? ''}`.toLowerCase()
      if (!hay.includes(q)) return false
    }
    if (priceMin !== null || priceMax !== null) {
      const p = priceOf(r)
      if (p === null) return false
      if (priceMin !== null && p < priceMin) return false
      if (priceMax !== null && p > priceMax) return false
    }
    if (vol24hMin !== null && (num(r.volume_24h) ?? -1) < vol24hMin) return false
    if (liqMin !== null && (num(r.liquidity) ?? -1) < liqMin) return false
    if (spreadMax !== null) {
      const sp = num(r.spread)
      if (sp !== null && sp > spreadMax) return false // unknown spread passes
    }
    if (closeWithinH !== null) {
      if (r.close_ts === null) return false
      const dt = r.close_ts - now
      if (dt < 0 || dt > closeWithinH * 3600) return false
    }
    return true
  })

  const sortKey = SORT_FNS[params.get('sort')] ? params.get('sort') : 'vol24h'
  const dir = params.get('dir') === 'asc' ? 1 : -1
  const fn = SORT_FNS[sortKey]
  rows = rows.slice().sort((a, b) => {
    const va = fn(a)
    const vb = fn(b)
    if (va === null && vb === null) return 0
    if (va === null) return 1 // nulls last regardless of dir
    if (vb === null) return -1
    if (typeof va === 'string') return dir * va.localeCompare(vb)
    return dir * (va - vb)
  })

  const limit = Math.min(200, Math.max(1, parseInt(params.get('limit') || '100', 10) || 100))
  const offset = Math.max(0, parseInt(params.get('offset') || '0', 10) || 0)
  return {
    total: rows.length,
    generated_at: now,
    rows: rows.slice(offset, offset + limit),
  }
}

// Live tick: each /api/screener response nudges ~15% of the returned open
// rows so auto-refresh has something to flash (Math.random on purpose —
// the tick is the one intentionally non-deterministic part).
function tickRows(pageRows) {
  const now = NOW()
  for (const r of pageRows) {
    if (r.status !== 'open' || r.mid === null) continue
    if (Math.random() > 0.15) continue
    const half = Math.max(5, Math.round((parseFloat(r.spread) * 1000) / 2))
    let midM = Math.round(parseFloat(r.mid) * 1000)
    midM = Math.min(985, Math.max(15, midM + Math.floor(Math.random() * 7) - 3))
    const bid = Math.max(1, midM - half)
    const ask = Math.min(999, midM + half)
    r.yes_bid = mils(bid)
    r.yes_ask = mils(ask)
    r.mid = mils(Math.round((bid + ask) / 2))
    r.spread = mils(ask - bid)
    if (r.last !== null) r.last = mils(midM)
    r.prices_updated_ts = now
  }
}

function meta() {
  const now = NOW()
  const catCounts = new Map() // "venue|cat" -> n (open only)
  for (const r of ROWS) {
    if (r.status !== 'open' || !r.category) continue
    const key = `${r.venue}|${r.category}`
    catCounts.set(key, (catCounts.get(key) ?? 0) + 1)
  }
  const categories = [...catCounts.entries()]
    .map(([key, n]) => {
      const [venue, category] = key.split('|')
      return { venue, category, n }
    })
    .sort((a, b) => b.n - a.n)
    .slice(0, 40)
  return {
    data_mode: 'synthetic',
    venues: ['kalshi', 'polymarket'],
    categories,
    counts: {
      open_total: openCount(),
      open_kalshi: openCount('kalshi'),
      open_polymarket: openCount('polymarket'),
    },
    freshness: [
      { venue: 'kalshi', endpoint: 'prices', watermark_ts: now - 140 },
      { venue: 'kalshi', endpoint: 'metadata', watermark_ts: now - 420 },
      { venue: 'polymarket', endpoint: 'prices', watermark_ts: now - 760 }, // >10m -> amber in the UI
      { venue: 'polymarket', endpoint: 'metadata', watermark_ts: now - 300 },
    ],
  }
}

function history(row, hours) {
  // deterministic-ish random walk anchored at the row's mid (or 0.5)
  const now = NOW()
  const seed = [...`${row.venue}:${row.market_id}`].reduce((a, c) => (a * 31 + c.charCodeAt(0)) | 0, 7)
  const hrnd = mulberry32(seed)
  const nPts = Math.min(2000, Math.max(24, Math.floor(hours * 12))) // ~5min cadence
  const stepSec = Math.floor((hours * 3600) / nPts)
  let p = num(row.mid) ?? num(row.last) ?? 0.5
  const pts = []
  for (let i = nPts - 1; i >= 0; i--) {
    pts.push({ t: now - i * stepSec, p: p.toFixed(4) })
    p = Math.min(0.99, Math.max(0.01, p + (hrnd() - 0.5) * 0.012))
  }
  return { points: pts }
}

// ---------- http ----------
function send(res, code, body) {
  const json = JSON.stringify(body)
  res.writeHead(code, {
    'content-type': 'application/json',
    'access-control-allow-origin': '*',
  })
  res.end(json)
}

const MARKET_RE = /^\/api\/market\/([^/]+)\/([^/]+?)(\/history)?$/

// Synthetic paper ledger — includes a cross-venue matched pair (a PM
// decision + its paired Kalshi decision sharing event_key) so the Decisions
// tab's venue column + ⇄ grouping can be verified.
function decisions() {
  const now = Math.floor(Date.now() / 1000)
  const EVT = 'DEMO-SPORTS-EVENT'
  const CID = 'synthetic-market-yes'
  const gates = { category_promoted: true, ofi_fresh: true, signal_support: true,
    quote_present: true, spread_ok: true, price_band: true, time_to_close: true }
  const rows = [
    { decision_id: 5, ts: now - 200, venue: 'polymarket',
      market_id: CID, title: 'Synthetic example: North vs South — North',
      category: 'Sports', event_key: EVT, signal_venue_id: 'polymarket',
      signal_market_id: CID, action: 'PASS', side: 'BUY_YES',
      ev: '0.0210', model_p: '0.5810', bid: '0.5000', ask: '0.5200',
      spread: '0.0200', skilled_ofi: '0.8000', n_skilled_trades: 6,
      ofi_age_s: 60, gates, passed_gates: true, theta: 'inf', graded: null },
    { decision_id: 6, ts: now - 200, venue: 'kalshi',
      market_id: `${EVT}-NORTH`, title: 'Synthetic example: Will North win?',
      category: 'Sports', event_key: EVT, signal_venue_id: 'polymarket',
      signal_market_id: CID, action: 'PASS', side: 'BUY_YES',
      ev: '0.0336', model_p: '0.5610', bid: '0.5000', ask: '0.5100',
      spread: '0.0100', skilled_ofi: '0.8000', n_skilled_trades: 6,
      ofi_age_s: 60, gates, passed_gates: true, theta: 'inf', graded: null },
    { decision_id: 4, ts: now - 900, venue: 'polymarket',
      market_id: 'synthetic-crypto-market', title: 'Synthetic example: price above threshold',
      category: 'Crypto', event_key: null, signal_venue_id: 'polymarket',
      signal_market_id: 'synthetic-crypto-market', action: 'PASS', side: 'BUY_NO',
      ev: '-0.0180', model_p: '0.4400', bid: '0.4500', ask: '0.4700',
      spread: '0.0200', skilled_ofi: '-0.5000', n_skilled_trades: 4,
      ofi_age_s: 120, gates, passed_gates: true, theta: 'inf',
      graded: { won: true, ret_per_dollar: '0.8721', ev_gap: '0.4340',
        entry_prob: '0.5300' } },
  ]
  return {
    generated_at: now,
    summary: { decisions: 128, passed_gates: 96, graded: 41,
      last_pass_ts: now - 200, win_rate: 0.556, mean_ret_per_dollar: 0.012,
      equity: [{ ts: now - 3600, cum_ret: 0.18 }, { ts: now - 900, cum_ret: 0.31 }] },
    rows,
  }
}

function research() {
  const now = NOW()
  const wallets = [
    { wallet: 'synthetic-wallet-selected', n_trades: 12, n_markets: 12,
      total_volume: '120.0000', actual_pnl: '48.0000', sigma: '13.8564',
      z_score: '3.4641', scorer_version: 1, selected: true },
    { wallet: 'synthetic-wallet-control', n_trades: 12, n_markets: 12,
      total_volume: '120.0000', actual_pnl: '0.0000', sigma: '17.3205',
      z_score: '0.0000', scorer_version: 1, selected: false },
  ]
  return {
    schema_version: 1, data_mode: 'synthetic', as_of_ts: now,
    skill_cutoff_ts: now - 86400, quote_source: 'synthetic',
    selection_rule: 'Historical z-score > 1.96, with at least 10 trades',
    network_requests: 0, live_orders: 0, wallets,
    selected_wallets: wallets.filter((wallet) => wallet.selected).map((wallet) => wallet.wallet),
    flows: [{ venue_market_id: 'synthetic-market-yes' }],
    decisions: [{ venue_market_id: 'synthetic-market-yes' }],
    grading: { graded: 1 },
    limitations: [
      'All observations are synthetic examples, not measured performance.',
      'A historical skill score does not establish persistence out of sample.',
      'Paper entries do not establish available fills or profitable following.',
    ],
  }
}

http
  .createServer((req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1')
    const path = url.pathname

    if (path === '/api/screener') {
      setTimeout(() => {
        const out = screener(url.searchParams)
        tickRows(out.rows)
        send(res, 200, out)
      }, 60 + Math.random() * 90)
      return
    }
    if (path === '/api/meta') {
      send(res, 200, meta())
      return
    }
    if (path === '/api/decisions') {
      send(res, 200, decisions())
      return
    }
    if (path === '/api/research') {
      send(res, 200, research())
      return
    }
    const m = path.match(MARKET_RE)
    if (m) {
      const venue = decodeURIComponent(m[1])
      const marketId = decodeURIComponent(m[2])
      const row = BY_KEY.get(`${venue}:${marketId}`)
      if (!row) {
        send(res, 404, { detail: 'market not found' })
        return
      }
      if (m[3]) {
        const hours = Math.min(168, Math.max(0.1, num(url.searchParams.get('hours')) ?? 24))
        setTimeout(() => send(res, 200, history(row, hours)), 1500) // deliberately slow
        return
      }
      const siblings = ROWS.filter((r) => r.venue === venue && r.event_id !== null && r.event_id === row.event_id)
        .slice()
        .sort((a, b) => (num(b.mid) ?? -1) - (num(a.mid) ?? -1))
      send(res, 200, { row, siblings })
      return
    }
    send(res, 404, { detail: 'unknown endpoint' })
  })
  .listen(PORT, '127.0.0.1', () => {
    console.log(`[mock] listening on http://127.0.0.1:${PORT}`)
  })
