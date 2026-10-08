# Historical research probes

These scripts investigate wallet skill, aggregate order flow, execution assumptions and cross-venue transfer. Historical datasets and saved projections are not bundled. The offline synthetic example does not reproduce the historical results.

Read [methodology](../docs/methodology.md), [findings](../docs/findings.md) and [data requirements](../docs/data.md) before running an empirical probe. Scripts are research artifacts with different sample, cohort and weighting conventions; they are not a uniform certified backtest framework.

| Script | Question | Interpretation limit |
|---|---|---|
| `skill_flow_backtest.py` | Do historically selected wallets differ, and does their aggregate flow correlate with outcomes? | Training fills are filtered by trade time without a resolution cutoff. Whole-lifetime flow and a near-settlement baseline invalidate its original forecasting headline. |
| `ofi_decision_time_probe.py` | How does the flow signal change when prices and flow are evaluated at a decision time? | Uses trade-derived midpoint proxies without an observed book. Verify resolution availability for training labels before calling a result ex-ante. |
| `ofi_deepening_backtest.py` | How does the flow relationship vary by category? | The retained implementation still uses event-axis `D`, whole-outcome flow and a final-price baseline. Its training filter omits resolution availability. It does not implement all previously described corrections. |
| `ofi_price_aware_probe.py` | Does weighting flow by price improve the relationship? | Uses prebuilt wallet labels without an as-of cutoff, full-outcome flow and final-trade residuals. Weighting variants remain exploratory. |
| `threshold_sweep.py` | Do flow cutoffs survive modeled costs? | Omits a training-resolution cutoff. Stale prints and assumed fills can overstate returns; Kalshi fees on Polymarket prices are transfer scenarios. Large scans require separate resource limits. |
| `ofi_latency_probe.py` | How quickly do trade prices respond after selected flow? | Uses a prebuilt cohort without proving label availability. Measures historical trade-price response, not follower fills or subsecond latency. |
| `ofi_resolution_probe.py` | What are directional payoff and return estimates when holding to resolution? | Uses a stricter pre-cutoff resolved cohort, but last-trade entries are not executable asks. Middle-price findings are exploratory. |
| `transfer_probe.py` | Does selected flow transfer across matched venue contracts? | Needs venue captures, curated projections and a separately reviewed contract mapping. Candle-close quotes, simulated fills and a small sports sample limit execution claims. |

## Run with identified inputs

Most corpus probes expect Parquet files under `data/external/polymarket-v1/daily_aligned/`. Other probes need curated trade, resolution, wallet and price projections. Inspect each script's input declarations and options first; running a script with missing data will not regenerate the corpus.

Install optional numerical dependencies for probes that import NumPy:

```sh
uv sync --locked --extra research
```

When the required inputs are present, scripts run from the repository root:

```sh
uv run python backtests/ofi_decision_time_probe.py
```

Record the exact command, code revision, file hashes, coverage, schema, resource limits and output. Historical inputs were not preserved as a complete release manifest, so matching previously recorded numbers is not guaranteed. Verify temporal labels and execution assumptions independently rather than treating a matching headline as validation.
