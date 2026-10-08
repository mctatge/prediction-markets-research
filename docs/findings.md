# Historical research findings

These are recorded exploratory findings from July 2026, with corrections applied to their interpretation. Historical inputs and complete run manifests are not bundled. The offline synthetic example illustrates the software and does not reproduce these numbers. A new empirical claim requires a new identified dataset and run.

## Forecasting gains shrank after correcting the baseline

An early skilled-flow analysis reported a 4.2% relative Brier improvement and a correlation near 0.045 with outcome-minus-price. It aggregated flow over the whole market lifetime and compared outcomes with a near-settlement last trade. That construction includes information unavailable at an earlier decision and does not establish a usable forecast.

The July 22 decision-time probe kept the historical wallet cohort recipe, ended flow at the decision time, and compared with a midpoint estimated from trades. Reported correlations remained modest, roughly 0.028–0.039 across variants, while the relative Brier improvement fell to about **0.18%**. The corpus had no observed order book: the midpoint was a trade-derived proxy, not an executable quote. Training-label availability also needs separate verification because this probe filters training trade times without requiring resolution before the cutoff.

The result weakens the earlier forecasting claim. It does not convert Brier improvement into profit, establish current performance, or prove that every possible skill signal is absent. The retained category-deepening script still uses event-axis `D` rather than consistently aligned token direction, so its output must not be treated as implementing all later described corrections. See [the decision-time probe](../backtests/ofi_decision_time_probe.py), [the backtest index](../backtests/README.md) and [methodology](methodology.md).

## Hold-to-resolution results did not establish follower profits

A separate July 19 probe selected a stricter historical cohort of 221 wallets using trades placed and resolved before 2025. In its March–June 2025 standard-binary sample, it recorded 17,706 flow events and an average directional payoff difference of about **1.89 cents per contract before fees**. Its aggregate return per dollar staked was approximately **−3%**; a favorable middle-price subgroup was selected after inspecting results.

Entry used the last observed trade rather than an available ask. The modeled fee sensitivities omitted observed book execution, and many events shared underlying outcomes. The cents-per-contract and return-per-dollar summaries use different denominators. These findings are compatible with a directional signal that a follower cannot profitably execute. The favorable subgroup remains exploratory. See [the resolution probe](../backtests/ofi_resolution_probe.py).

## Latency was not the only cost

The short-horizon latency study recorded a directional price response of approximately **0.43 cents by two hours**, with around 74% observed by fifteen minutes. Those estimates used trade prices in a historical four-month sample and a prebuilt wallet table whose selection-time label availability was not established. They do not measure subsecond opportunities or current venue latency.

A slower component remained, but a delayed price response alone does not establish positive net returns. Entry spread, fees, depth and whether the observed print was attainable still matter. The separate hold-to-resolution study had different cash flows and required only a modeled entry fee. See [the latency probe](../backtests/ofi_latency_probe.py).

## Calibration and cross venue transfer remain limited

An earlier Kalshi recalibration experiment looked stronger against last trades than against contemporaneous book midpoints. On the recorded short, liquid and sports-heavy sample, recalibration did not improve the midpoint baseline. That supports using the right baseline; it does not establish universal market efficiency.

The sports transfer probe found highly contemporaneous prices across Kalshi and Polymarket at minute resolution. Its follower-return experiment contained only 52 events and wide confidence intervals, so it could neither certify nor rule out profitability. Similar market titles and correlated prices also do not guarantee equivalent settlement conditions. See [the transfer probe](../backtests/transfer_probe.py).

## Current interpretation

The repository supports further research into historical skill, aggregate flow and paper decisions. It does not establish that selected traders can be followed profitably after realistic costs. Historical maker returns are a separate liquidity-provision question; a new order's queue position and adverse selection remain essential. No successful live-trading result is claimed.
