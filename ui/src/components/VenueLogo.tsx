// Text identifiers avoid bundling third-party venue artwork.

const NAMES: Record<string, string> = {
  kalshi: 'Kalshi',
  polymarket: 'Polymarket',
}

export function venueName(venue: string): string {
  return NAMES[venue] ?? venue
}

export function VenueLogo({ venue, size = 16 }: { venue: string; size?: number }) {
  const name = venueName(venue)
  return (
    <span
      className={`vchip vlogo ${venue === 'kalshi' ? 'vK' : 'vP'}`}
      role="img"
      aria-label={name}
      title={name}
      style={{ width: size + 4, minHeight: size }}
    >
      {name.charAt(0).toUpperCase()}
    </span>
  )
}
