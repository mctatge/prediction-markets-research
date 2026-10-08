# Research methodology

The main question is whether historical trading activity identifies skill before a new decision and whether a follower can act on that information profitably. The implemented signals aggregate wallet activity; they do not reproduce each trader's orders, positions or complete portfolio.

## Separate the questions

| Question | Evidence required |
|---|---|
| Did a trader earn money? | Observed cash flows, positions, settlement, fees and capital exposure |
| Was performance distinguishable from luck? | A stated null model, dependence assumptions and selection correction |
| Could skill be identified beforehand? | Labels trained only from information available before each decision |
| Did selected flow improve a forecast? | A decision-time probability baseline and out-of-sample scoring |
| Could a follower earn money? | Available quotes and depth, delay, sizing, fees, fills and terminal cash flows |

A positive answer to an earlier question does not establish the later ones. A profitable trader can receive prices or fills unavailable to a follower.

## Wallet scores are research statistics

The wallet projection uses signed settlement PnL before fees. For trade $i$, with direction $d_i\in\{-1,+1\}$, dollar notional $u_i$, token price $p_i$, and terminal payoff $y_i\in\{0,1\}$, its convention is

$$
x_i=d_i\frac{u_i}{p_i}(y_i-p_i),
\qquad
z=\frac{\sum_i x_i}{\sqrt{\sum_i x_i^2}}.
$$

The denominator is the standard deviation under independent random sign flips of the observed trade contributions. It is an analytical approximation to that particular randomization model, not a complete portfolio return model or a universally calibrated significance test. Repeated trades on the same event, correlated positions, market selection and testing many wallets challenge its interpretation. The shared projection uses a $z>1.96$ threshold and a minimum trade count; individual probes use different cohorts and weighting conventions.

The v1 corpus supplies both sides of fills. Its projection unfolds maker and taker contributions. Later public trade captures supply taker activity, so coverage and execution role change over time. A wallet address is an account identifier; it is not a verified person or a complete inventory of that person's activity.

## Build every signal at its decision time

For a decision at $t$, training trades and their resolution labels must both be available before the training cutoff. A timestamp before the cutoff does not make a later settlement outcome available then. Rebuilding a full-history wallet table and joining it to older decisions creates label leakage. Keep a versioned score snapshot for each eligible training period.

The flow window must end at $t$. Align buys and sells to the correct outcome token before aggregation. The live scorer weights signed activity by shares; historical probes also use dollar-notional weighting. Record which estimator a run uses. Whole-market-lifetime flow and event-axis signs are not interchangeable with a token's trailing flow.

Use a fresh observed bid and ask for execution assumptions. A last trade can be stale or reflect one side of the spread. A trade-derived midpoint proxy is useful for sensitivity analysis but does not prove that a quote or fill was available. Record quote age, missing sides, depth, delayed observation and capture gaps. Match markets across venues by contract rules and resolution conditions, not title similarity alone.

## Score forecasts and monetary returns separately

Probability accuracy uses Brier loss,

$$
\operatorname{Brier}=\frac{1}{N}\sum_{j=1}^{N}(\hat p_j-y_j)^2.
$$

Lower Brier loss is not a monetary return and cannot be compared directly with fees expressed in cents. Report model and market loss on the same decisions, then evaluate a separately defined trading rule.

For a hypothetical hold-to-resolution purchase of $q$ YES contracts at ask $a$, net cash PnL is

$$
\Pi_{\mathrm{YES}}=q(y-a)-F_{\mathrm{entry}}-C_{\mathrm{slippage}}.
$$

For NO contracts with entry cost $1-b$, where $b$ is the YES bid, replace the payoff with $1-y$. Settlement has no modeled exit trade; a round trip needs separate exit fees and prices. Declare the fee schedule by venue, date and market, including rounding and exceptions. The checked-in fee module contains research assumptions rather than a promise that a future venue schedule matches them.

Maker evaluation requires a fill rule, queue assumptions and adverse-selection analysis. Returns on fills that occurred do not establish returns obtainable by a new resting order. Cluster uncertainty by co-resolving event, report exploratory subgroup selection, and use placebos and held-out periods before interpreting a positive estimate.

## Related primary research

These are the primary research sources recorded in the project bibliography. Links lead to publisher, author or repository copies; paper PDFs are not bundled. Review scope is stated below because access to an abstract or author summary does not establish a full-text replication.

| Source | Role in the research | Review scope |
|---|---|---|
| Bürgi, Deng and Whelan (2026), [Makers and Takers: The Economics of the Kalshi Prediction Market](https://www.karlwhelan.com/Papers/Kalshi.pdf) | Historical pricing and maker/taker economics; the sample ends in April 2025 | Full text |
| Gómez-Cram, Guo, Jensen and Kung (2026), [Prediction Market Accuracy: Crowd Wisdom or Informed Minority?](https://ssrn.com/abstract=6617059) | Distinguishing trading skill from luck and studying information incorporation | Abstract and summaries; full text unavailable during the recorded review |
| Reichenbach and Walther (2025), [Exploring Decentralized Prediction Markets: Accuracy, Skill, and Bias on Polymarket](https://ssrn.com/abstract=5910522) | Calibration, bias and persistent trader performance | Abstract and summaries; full text unavailable during the recorded review |
| Mitts and Ofir (2026), [From Iran to Taylor Swift: Informed Trading in Prediction Markets](https://ssrn.com/abstract=6426778) | Informed-flow screening and adverse-selection questions | [Author-written summary](https://corpgov.law.harvard.edu/2026/03/25/from-iran-to-taylor-swift-informed-trading-in-prediction-markets/); full paper unavailable during the recorded review |
| Qin and Yang (2026), [Polymarket-v1 Database](https://arxiv.org/abs/2606.04217) | Historical exchange fills and aligned research data | Full-text dataset documentation and a historical data sample; no order-book reconstruction is implied |

The project is not a certified replication of these papers. Paper findings motivate hypotheses and evaluation requirements; they are not project results or evidence that a follower can obtain the same returns.
