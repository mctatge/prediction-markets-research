// Tiny hand-rolled URL <-> state hook. No router: single page, so
// history.replaceState and popstate synchronize each state change.
// reflected in the query string; loading a URL restores the exact screen.

import { useCallback, useEffect, useState } from 'react'
import type { ScreenState } from './urlState'
import { applyPatch, parseState, serializeState } from './urlState'

export function useSearchParamsState(): [ScreenState, (patch: Partial<ScreenState>) => void] {
  const [state, setState] = useState<ScreenState>(() => parseState(window.location.search))

  // back/forward restores state from the URL
  useEffect(() => {
    const onPop = () => setState(parseState(window.location.search))
    window.addEventListener('popstate', onPop)
    return () => window.removeEventListener('popstate', onPop)
  }, [])

  // state -> URL (side effect kept out of the setState updater; StrictMode-safe)
  useEffect(() => {
    const qs = serializeState(state)
    const target = qs ? `?${qs}` : window.location.pathname
    const current = window.location.search
    if ((qs ? `?${qs}` : '') !== current) {
      window.history.replaceState(null, '', target)
    }
  }, [state])

  const update = useCallback((patch: Partial<ScreenState>) => {
    setState((prev) => applyPatch(prev, patch))
  }, [])

  return [state, update]
}

/** Re-render tick for relative timestamps ("2m ago"). */
export function useNowSec(intervalMs = 30_000): number {
  const [now, setNow] = useState(() => Math.floor(Date.now() / 1000))
  useEffect(() => {
    const id = window.setInterval(() => setNow(Math.floor(Date.now() / 1000)), intervalMs)
    return () => window.clearInterval(id)
  }, [intervalMs])
  return now
}
