import { describe, expect, it } from 'vitest'
import {
  applyPatch,
  DEFAULT_STATE,
  marketKey,
  parseState,
  serializeState,
  splitMarketKey,
} from './urlState'
import type { ScreenState } from './urlState'

describe('serializeState', () => {
  it('fully-default state serializes to empty string', () => {
    expect(serializeState(DEFAULT_STATE)).toBe('')
  })
  it('omits defaults, includes overrides', () => {
    const qs = serializeState({ ...DEFAULT_STATE, venue: 'kalshi', page: 3 })
    expect(qs).toContain('venue=kalshi')
    expect(qs).toContain('page=3')
    expect(qs).not.toContain('status=')
    expect(qs).not.toContain('sort=')
  })
  it('exclude_churn=false is explicit (default is true)', () => {
    expect(serializeState({ ...DEFAULT_STATE, exclude_churn: false })).toBe('exclude_churn=false')
  })
})

describe('roundtrip', () => {
  it('parse(serialize(s)) === s for a fully-loaded state', () => {
    const s: ScreenState = {
      venue: 'polymarket',
      category: 'Pop Culture',
      q: 'fed decision',
      status: 'all',
      price_min: '0.05',
      price_max: '0.95',
      vol24h_min: '1000',
      liquidity_min: '500',
      spread_max: '0.02',
      close_within_h: '24',
      exclude_churn: false,
      sort: 'close_ts',
      dir: 'asc',
      page: 7,
      view: 'edge',
      watchlist: true,
      sel: 'kalshi:KXHIGHNY-26JUL12-B55',
    }
    expect(parseState(serializeState(s))).toEqual(s)
  })
  it('roundtrips values needing URL encoding', () => {
    const s: ScreenState = { ...DEFAULT_STATE, q: 'a&b=c 100%', category: 'Pop Culture' }
    expect(parseState(serializeState(s))).toEqual(s)
  })
  it('accepts a leading question mark', () => {
    expect(parseState('?venue=kalshi').venue).toBe('kalshi')
  })
})

describe('parseState hardening', () => {
  it('garbage falls back to defaults', () => {
    const s = parseState('venue=nyse&status=bogus&sort=hax&dir=up&page=-2&view=3d&price_min=abc')
    expect(s).toEqual(DEFAULT_STATE)
  })
  it('non-numeric filter values are dropped', () => {
    expect(parseState('spread_max=wide').spread_max).toBe('')
    expect(parseState('spread_max=0.02').spread_max).toBe('0.02')
  })
})

describe('applyPatch page-reset', () => {
  it('filter changes reset the page', () => {
    const s = { ...DEFAULT_STATE, page: 5 }
    expect(applyPatch(s, { q: 'btc' }).page).toBe(0)
    expect(applyPatch(s, { sort: 'spread' }).page).toBe(0)
    expect(applyPatch(s, { venue: 'kalshi' }).page).toBe(0)
  })
  it('non-filter changes keep the page', () => {
    const s = { ...DEFAULT_STATE, page: 5 }
    expect(applyPatch(s, { sel: 'kalshi:X' }).page).toBe(5)
    expect(applyPatch(s, { view: 'edge' }).page).toBe(5)
  })
  it('explicit page in the patch wins', () => {
    const s = { ...DEFAULT_STATE, page: 5 }
    expect(applyPatch(s, { page: 2 }).page).toBe(2)
  })
  it('no-op filter patch does not reset the page', () => {
    const s = { ...DEFAULT_STATE, page: 5, q: 'btc' }
    expect(applyPatch(s, { q: 'btc' }).page).toBe(5)
  })
})

describe('market keys', () => {
  it('roundtrips ids containing colons', () => {
    const key = marketKey('kalshi', 'KX:WEIRD:ID')
    expect(splitMarketKey(key)).toEqual({ venue: 'kalshi', marketId: 'KX:WEIRD:ID' })
  })
  it('rejects malformed keys', () => {
    expect(splitMarketKey('no-colon')).toBeNull()
    expect(splitMarketKey(':leading')).toBeNull()
    expect(splitMarketKey('trailing:')).toBeNull()
  })
})
