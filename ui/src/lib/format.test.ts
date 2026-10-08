import { describe, expect, it } from 'vitest'
import {
  decimalSign,
  fmtAgo,
  fmtCloseIn,
  fmtPrice,
  fmtQty,
  trimDecimalStr,
} from './format'

describe('trimDecimalStr (pure string — no float in the display path)', () => {
  it('trims trailing zeros down to 2 decimals', () => {
    expect(trimDecimalStr('0.4200')).toBe('0.42')
    expect(trimDecimalStr('0.4300')).toBe('0.43')
    expect(trimDecimalStr('0.0200')).toBe('0.02')
    expect(trimDecimalStr('1.0000')).toBe('1.00')
  })
  it('keeps a meaningful 3rd decimal (subpenny ticks)', () => {
    expect(trimDecimalStr('0.4250')).toBe('0.425')
    expect(trimDecimalStr('0.0010')).toBe('0.001')
    expect(trimDecimalStr('0.999')).toBe('0.999')
  })
  it('rounds half-up at the 4th decimal, with carry', () => {
    expect(trimDecimalStr('0.4267')).toBe('0.427')
    expect(trimDecimalStr('0.4264')).toBe('0.426')
    expect(trimDecimalStr('0.9999')).toBe('1.00')
    expect(trimDecimalStr('0.0005')).toBe('0.001')
    expect(trimDecimalStr('0.99951')).toBe('1.00')
  })
  it('handles negatives (edge column later)', () => {
    expect(trimDecimalStr('-0.0300')).toBe('-0.03')
    expect(trimDecimalStr('-0.0305')).toBe('-0.031')
  })
  it('pads short values to 2 decimals', () => {
    expect(trimDecimalStr('0.5')).toBe('0.50')
    expect(trimDecimalStr('1')).toBe('1.00')
  })
  it('passes non-decimal garbage through verbatim', () => {
    expect(trimDecimalStr('n/a')).toBe('n/a')
  })
})

describe('fmtPrice', () => {
  it('em-dash for null, keeping full=null', () => {
    expect(fmtPrice(null)).toEqual({ text: '—', full: null })
  })
  it('keeps the verbatim string for the tooltip', () => {
    expect(fmtPrice('0.4200')).toEqual({ text: '0.42', full: '0.4200' })
  })
})

describe('decimalSign (color/comparison only)', () => {
  it('signs', () => {
    expect(decimalSign('0.0100')).toBe(1)
    expect(decimalSign('-0.0100')).toBe(-1)
    expect(decimalSign('0.0000')).toBe(0)
    expect(decimalSign(null)).toBe(0)
  })
})

describe('fmtQty', () => {
  it('humanizes venue-native quantities', () => {
    expect(fmtQty('410.00').text).toBe('410')
    expect(fmtQty('1523.00').text).toBe('1.5k')
    expect(fmtQty('12345.6789').text).toBe('12.3k')
    expect(fmtQty('2100000.00').text).toBe('2.1M')
    expect(fmtQty(null).text).toBe('—')
  })
  it('keeps the full string for the tooltip', () => {
    expect(fmtQty('12345.6789').full).toBe('12345.6789')
  })
})

describe('fmtCloseIn', () => {
  const now = 1_783_830_000
  it('humanizes: minutes / hours / days', () => {
    expect(fmtCloseIn(now + 41 * 60, now).text).toBe('41m')
    expect(fmtCloseIn(now + 3 * 3600, now).text).toBe('3h')
    expect(fmtCloseIn(now + 36 * 3600, now).text).toBe('36h')
    expect(fmtCloseIn(now + 2 * 86400, now).text).toBe('2d')
    expect(fmtCloseIn(now + 30, now).text).toBe('<1m')
  })
  it('urgent under 6h only', () => {
    expect(fmtCloseIn(now + 3 * 3600, now).urgent).toBe(true)
    expect(fmtCloseIn(now + 7 * 3600, now).urgent).toBe(false)
  })
  it('null and past', () => {
    expect(fmtCloseIn(null, now)).toEqual({ text: '—', urgent: false, past: false })
    expect(fmtCloseIn(now - 10, now).past).toBe(true)
  })
})

describe('fmtAgo', () => {
  const now = 1_783_830_000
  it('relative text + age', () => {
    expect(fmtAgo(now - 30, now).text).toBe('now')
    expect(fmtAgo(now - 120, now).text).toBe('2m')
    expect(fmtAgo(now - 2 * 3600, now).text).toBe('2h')
    expect(fmtAgo(now - 3 * 86400, now).text).toBe('3d')
    expect(fmtAgo(null, now)).toEqual({ text: '—', ageSec: Infinity })
  })
  it('exposes age so callers can style >30min staleness', () => {
    expect(fmtAgo(now - 1900, now).ageSec).toBe(1900)
  })
})
