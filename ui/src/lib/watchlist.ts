// Watchlist store: starred "venue:market_id" keys persisted to localStorage.
// A scope, not a page — the table can be scoped to these keys.
// Factory takes any Storage-like so tests run without a DOM.

export const WATCHLIST_CAP = 200
const STORAGE_KEY = 'pmkt.watchlist.v1'

export type StorageLike = Pick<Storage, 'getItem' | 'setItem'>
export type ToggleResult = 'added' | 'removed' | 'full'

export interface WatchlistStore {
  /** stable snapshot — reference changes only on mutation (useSyncExternalStore-safe) */
  getSnapshot(): readonly string[]
  has(key: string): boolean
  count(): number
  /** add/remove; 'full' when the cap would be exceeded */
  toggle(key: string): ToggleResult
  subscribe(fn: () => void): () => void
  /** re-read backing storage (for cross-tab 'storage' events) */
  reload(): void
}

export function createWatchlist(storage: StorageLike): WatchlistStore {
  let keys: string[] = read()
  let snapshot: readonly string[] = keys.slice()
  const listeners = new Set<() => void>()

  function read(): string[] {
    try {
      const raw = storage.getItem(STORAGE_KEY)
      if (!raw) return []
      const arr = JSON.parse(raw)
      if (!Array.isArray(arr)) return []
      return arr.filter((k): k is string => typeof k === 'string').slice(0, WATCHLIST_CAP)
    } catch {
      return []
    }
  }

  function persist() {
    try {
      storage.setItem(STORAGE_KEY, JSON.stringify(keys))
    } catch {
      // storage full / unavailable — keep in-memory state
    }
    snapshot = keys.slice()
    listeners.forEach((fn) => fn())
  }

  return {
    getSnapshot: () => snapshot,
    has: (key) => keys.includes(key),
    count: () => keys.length,
    toggle(key) {
      const i = keys.indexOf(key)
      if (i >= 0) {
        keys = keys.slice(0, i).concat(keys.slice(i + 1))
        persist()
        return 'removed'
      }
      if (keys.length >= WATCHLIST_CAP) return 'full'
      keys = keys.concat(key)
      persist()
      return 'added'
    },
    subscribe(fn) {
      listeners.add(fn)
      return () => listeners.delete(fn)
    },
    reload() {
      keys = read()
      snapshot = keys.slice()
      listeners.forEach((fn) => fn())
    },
  }
}

// Browser singleton (lazy so importing this module never touches window in tests).
let instance: WatchlistStore | null = null
export function getWatchlist(): WatchlistStore {
  if (!instance) {
    instance = createWatchlist(globalThis.localStorage)
    if (typeof window !== 'undefined') {
      window.addEventListener('storage', (e) => {
        if (e.key === STORAGE_KEY) instance!.reload()
      })
    }
  }
  return instance
}
