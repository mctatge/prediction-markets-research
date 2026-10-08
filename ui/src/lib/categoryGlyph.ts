// Category → symbol glyph for market-row chips. Exact names first (the
// category vocabulary is venue-supplied and open-ended), keyword fallback
// second, 📊 as the default. Pure display sugar — never used for logic.

const EXACT: Record<string, string> = {
  Sports: '🏆',
  Soccer: '⚽',
  '2026 FIFA World Cup': '⚽',
  Tennis: '🎾',
  MLB: '⚾',
  NBA: '🏀',
  NFL: '🏈',
  'Formula 1': '🏎️',
  Esports: '🎮',
  EWC: '🎮',
  Politics: '🗳️',
  Elections: '🗳️',
  Midterms: '🗳️',
  'primary elections': '🗳️',
  Trump: '🗳️',
  Economics: '🏦',
  'Economic Policy': '🏦',
  Finance: '📈',
  Financials: '📈',
  Commodities: '🛢️',
  Crypto: '🪙',
  Bitcoin: '🪙',
  Ethereum: '🪙',
  XRP: '🪙',
  FDV: '🪙',
  'Up or Down': '🪙',
  Weather: '🌤️',
  'Climate and Weather': '🌤️',
  Entertainment: '🎬',
  Culture: '🎬',
  TV: '📺',
  Mentions: '💬',
  AI: '🤖',
  Tech: '💻',
  'Science and Technology': '🔬',
  Companies: '🏢',
  'Middle East': '🌍',
  Iran: '🌍',
  UK: '🌍',
  Brazil: '🌍',
  Ethiopia: '🌍',
  'Military Strikes': '🌍',
}

const KEYWORDS: ReadonlyArray<readonly [RegExp, string]> = [
  [/elect|politic|senate|congress|president|governor/i, '🗳️'],
  [/crypto|bitcoin|ethereum|solana|token|coin|xrp/i, '🪙'],
  [/soccer|fifa|premier league|la liga|bundesliga/i, '⚽'],
  [/tennis/i, '🎾'],
  [/basket|nba/i, '🏀'],
  [/baseball|mlb/i, '⚾'],
  [/nfl|football/i, '🏈'],
  [/sport|league|cup|championship|open|tour|racing|olympi/i, '🏆'],
  [/weather|climate|temperature|hurricane/i, '🌤️'],
  [/econ|inflation|fed|rates|gdp|jobs|cpi/i, '🏦'],
  [/finan|stock|s&p|nasdaq|earnings|ipo/i, '📈'],
  [/movie|film|tv|music|award|celebrit|entertain/i, '🎬'],
  [/tech|ai|software|space/i, '💻'],
  [/war|geopolit|world|nato|strike|military|nuclear/i, '🌍'],
  [/health|pandemic|virus|disease/i, '🩺'],
]

export function categoryGlyph(category: string | null): string {
  if (!category) return '📊'
  const exact = EXACT[category]
  if (exact) return exact
  for (const [re, glyph] of KEYWORDS) {
    if (re.test(category)) return glyph
  }
  return '📊'
}
