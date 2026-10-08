import { describe, expect, it, vi } from 'vitest'
import { createWatchlist, WATCHLIST_CAP } from './watchlist'
import type { StorageLike } from './watchlist'

function memStorage(initial?: string): StorageLike & { data: Map<string, string> } {
  const data = new Map<string, string>()
  if (initial !== undefined) data.set('pmkt.watchlist.v1', initial)
  return {
    data,
    getItem: (k) => data.get(k) ?? null,
    setItem: (k, v) => void data.set(k, v),
  }
}

describe('watchlist store', () => {
  it('toggles add/remove and persists', () => {
    const storage = memStorage()
    const wl = createWatchlist(storage)
    expect(wl.toggle('kalshi:A')).toBe('added')
    expect(wl.toggle('polymarket:123')).toBe('added')
    expect(wl.has('kalshi:A')).toBe(true)
    expect(wl.count()).toBe(2)
    expect(wl.toggle('kalshi:A')).toBe('removed')
    expect(wl.has('kalshi:A')).toBe(false)
    expect(JSON.parse(storage.data.get('pmkt.watchlist.v1')!)).toEqual(['polymarket:123'])
  })

  it('loads persisted state', () => {
    const wl = createWatchlist(memStorage(JSON.stringify(['kalshi:X', 'polymarket:9'])))
    expect(wl.count()).toBe(2)
    expect(wl.has('polymarket:9')).toBe(true)
  })

  it('tolerates corrupt storage', () => {
    expect(createWatchlist(memStorage('{not json')).count()).toBe(0)
    expect(createWatchlist(memStorage('"a string"')).count()).toBe(0)
    expect(createWatchlist(memStorage(JSON.stringify([1, 'ok', null]))).count()).toBe(1)
  })

  it('enforces the 200-star cap', () => {
    const wl = createWatchlist(memStorage())
    for (let i = 0; i < WATCHLIST_CAP; i++) {
      expect(wl.toggle(`kalshi:M${i}`)).toBe('added')
    }
    expect(wl.toggle('kalshi:ONE-TOO-MANY')).toBe('full')
    expect(wl.count()).toBe(WATCHLIST_CAP)
    // removal still works at the cap
    expect(wl.toggle('kalshi:M0')).toBe('removed')
    expect(wl.toggle('kalshi:ONE-TOO-MANY')).toBe('added')
  })

  it('notifies subscribers; snapshot reference changes only on mutation', () => {
    const wl = createWatchlist(memStorage())
    const spy = vi.fn()
    const unsub = wl.subscribe(spy)
    const snap0 = wl.getSnapshot()
    expect(wl.getSnapshot()).toBe(snap0) // stable between mutations
    wl.toggle('kalshi:A')
    expect(spy).toHaveBeenCalledTimes(1)
    const snap1 = wl.getSnapshot()
    expect(snap1).not.toBe(snap0)
    expect(snap1).toEqual(['kalshi:A'])
    expect(wl.getSnapshot()).toBe(snap1)
    unsub()
    wl.toggle('kalshi:B')
    expect(spy).toHaveBeenCalledTimes(1)
  })

  it('reload() re-reads backing storage (cross-tab)', () => {
    const storage = memStorage()
    const wl = createWatchlist(storage)
    storage.data.set('pmkt.watchlist.v1', JSON.stringify(['kalshi:FROM-OTHER-TAB']))
    expect(wl.has('kalshi:FROM-OTHER-TAB')).toBe(false)
    wl.reload()
    expect(wl.has('kalshi:FROM-OTHER-TAB')).toBe(true)
  })
})
