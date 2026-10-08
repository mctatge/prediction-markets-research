# Data requirements and provenance

The repository distributes source code and generated synthetic examples. It does not bundle historical venue captures, downloaded research corpora, fitted wallet tables, models or historical paper ledgers. Install the offline example first; empirical backtests need separate inputs.

## Offline example

`pmkt demo` generates synthetic records and derived research artifacts in its output directory. Titles, identifiers, prices, outcomes, wallet activity and paper results belong to the example. They must not be used as market observations or evidence that a strategy works. No venue credentials or network requests are needed to generate the example.

## Historical corpus

Several probes expect [TimeSeventeen Polymarket-v1](https://huggingface.co/datasets/TimeSeventeen/Polymarket-v1), described by [Qin and Yang](https://arxiv.org/abs/2606.04217). The relevant `daily_aligned` layer contains trade-level market/token identifiers, maker and taker addresses, direction, price, amount, event timing and outcome labels. It covers the historical v1 exchange; it does not supply a contemporaneous CLOB order book or a continuing current-market feed.

The dataset card declares CC BY 4.0. Its attribution and modification requirements remain separate from this repository's software license, and that declaration does not guarantee every other permission for every use. Obtain data from its publisher under applicable terms rather than treating the software release as a redistribution license.

Some scripts expect the corpus under `data/external/polymarket-v1/daily_aligned/`; others accept additional locations or depend on curated projections. Read each script and the [backtest index](../backtests/README.md) before running. A matching path alone does not reproduce a historical dataset version. Record file hashes, partition coverage, schema, exclusions, score cutoff and fee assumptions for every empirical run.

## Venue captures and projections

The acquisition layer records parsed responses before building projections. Its envelope contains a schema version, venue, endpoint, query parameters, acquisition timestamp, HTTP status, elapsed time and payload. It is not a packet archive: JSON has been parsed, and some error text is truncated.

| Surface | Typical purpose |
|---|---|
| `data/raw/` | Compressed acquisition records |
| `data/db/hot.sqlite` | Operational store and local explorer reads |
| `data/curated/` | Rebuildable Parquet projections for research |
| `data/models/` | Fitted experimental artifacts |

Normalize timestamps to UTC and probabilities to the 0–1 scale. Preserve native market, condition, token and event identifiers as separate fields. Venue-native identifiers are not interchangeable across APIs. Kalshi prices/counts use fixed-point strings; Gamma list fields can arrive as JSON-encoded strings. Keep unknown values distinct from zero.

Acquisition time and trade time answer different questions. A delayed response can contain old trades. Resolutions and revised metadata can become available after the trade itself. Record both observation and event timing when constructing an ex-ante analysis.

## Access and operational scope

Review the [Kalshi API documentation](https://docs.kalshi.com/welcome), its linked Developer Agreement, and [Polymarket documentation](https://docs.polymarket.com/) and [terms](https://polymarket.com/tos) before live use. Public endpoints do not by themselves grant data redistribution rights. Account access, institutional data use, artwork and public display can have separate conditions. This repository makes no general clearance claim.

Gamma, global CLOB and global Data API integrations concern international Polymarket. They are not interchangeable with Polymarket US. API schemas, pagination, retention and fees change; a historical backtest does not validate current compatibility.

The local UI binds to loopback and has no multi-user authentication. Keep credentials in local environment configuration, outside version control. Continuous acquisition and analytical spill need explicit capacity planning. Stop collection before deleting its files; preserve original captures unless a separate data policy explicitly authorizes their removal. Never attach a second writer to the operational SQLite database.
