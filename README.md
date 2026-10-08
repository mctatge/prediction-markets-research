# Prediction Markets Research

A local research workspace for testing **whether trading skill can be identified from past activity before a new decision and whether following it remains profitable after realistic costs**.

Release candidate 0.1.0rc1. The offline example is the starting path; live adapters and historical strategies are experimental.

The project combines wallet-skill scores, aggregate order-flow signals, historical probes, a record-only decision ledger, and a market explorer for Kalshi and Polymarket data. The research follows aggregate activity from selected wallets. Paper decisions record hypothetical outcomes without submitting exchange orders.

**Historical results do not establish a profitable strategy.** Correcting future-informed flow and stale price baselines substantially reduced an early apparent forecasting advantage. Trader profitability, follower profitability, probability accuracy, and executable returns are separate questions. See [findings](docs/findings.md) and [methodology](docs/methodology.md).

The [primary research bibliography](docs/methodology.md#related-primary-research) links the papers behind the skill, flow and execution hypotheses and records the scope of each review.

## Run the offline example

Requirements: Python 3.12+, [uv](https://docs.astral.sh/uv/), and Node.js 22.12+ with npm. The checkout defaults to Python 3.14 through `.python-version`. The source checkout includes the frontend; the Python wheel alone does not bundle it. The runtime targets macOS and Linux. Native Windows is not supported.

From the repository root:

```sh
uv sync --locked
npm --prefix ui ci
npm --prefix ui run build
uv run pmkt demo --output .demo
uv run pmkt --data-dir .demo ui
```

Open [http://127.0.0.1:8100](http://127.0.0.1:8100). The example uses **synthetic data** and makes no venue HTTP requests. Dependency installation needs internet access; generating and browsing the installed example does not. No API key, wallet connection, or exchange account is needed for the example.

The generated `.demo/demo-report.json` explains the example and its outputs. Synthetic scores and paper returns illustrate the workflow; they are not empirical market findings. Use a new output directory for another example run: existing output is never overwritten. Omit `--output` to create a persistent temporary workspace and print its path. Supply `--as-of` with a Unix timestamp for a fixed example clock.

The example scores past activity using outcomes known before selection, aggregates later observed flow, prices hypothetical entries from a subsequent bid/ask quote, and grades them after resolution. It includes a future outcome excluded from selection, both YES and NO flow, a rejected stale quote, and a paper win and loss. Its fixed synthetic assumptions are documented in the report.

## What is included

| Component | Purpose |
|---|---|
| `src/pmkt/lake.py` | Build curated trades, resolutions and wallet-skill projections |
| `src/pmkt/model/` | Aggregate order flow, forecast experiments, fees and paper decisions |
| `src/pmkt/kalshi/`, `src/pmkt/polymarket/` | Venue adapters and acquisition code |
| `src/pmkt/ui/`, `ui/` | Read-only local API and market explorer |
| `backtests/` | Historical probes with individual input and interpretation limits |
| `tests/` | Synthetic and mocked checks |

Prices use Decimal values internally and decimal strings in the [UI API](src/pmkt/ui/API_CONTRACT.md). Raw acquisition records retain endpoint, parameters, acquisition time, status and parsed payload. Quote freshness and missing data remain material to any analysis.

## Use real data deliberately

Historical datasets, fitted models and historical paper ledgers are not bundled. The offline example does not reproduce the recorded research numbers. [Data requirements](docs/data.md) describe inputs, identifiers and provenance needed for a new run; the [backtest index](backtests/README.md) explains each probe.

Live adapters are experimental. Check each venue's current terms, API documentation and applicable access conditions before using them. The international Polymarket Gamma/CLOB/Data APIs are distinct from Polymarket US. The repository provides no public live-data service or API compatibility guarantee.

Collection and historical processing can consume substantial storage, memory and API capacity. Review command help and resource settings before enabling network access. A continuous collector is an advanced workload; starting the local explorer does not start one.

Venue commands require the global `--allow-network` option before the command. Continuous collection and WebSocket capture additionally require `--continuous`. These options enable experimental workloads; they do not impose a small watchlist or a total storage quota.

## Develop

```sh
uv run pytest
npm --prefix ui test
npm --prefix ui run build
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution scope and research standards. The original source is licensed under [MIT](LICENSE). Upstream market data, research corpora and third-party dependencies retain their own terms.
