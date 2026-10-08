# Contributing

Contributions should make the research easier to inspect, reproduce and challenge. Useful work includes synthetic regression cases, adapter schema fixes, clearer data provenance, decision-time evaluation, portable resource controls and local explorer improvements.

Start a discussion or issue before a broad feature or methodology change. State the problem, expected behavior and evidence. A small reproducible synthetic case is preferable to uploading real wallet activity or a large dataset.

## Development checks

From the repository root:

```sh
uv sync --locked
uv run pytest
npm --prefix ui ci
npm --prefix ui test
npm --prefix ui run build
```

Keep tests offline by mocking HTTP and using temporary stores. Live checks must be explicit, rate-budgeted and separate from the default suite. Do not include credentials, private keys, downloaded datasets, personal paths or generated output in a contribution.

## Research changes

State the estimand: trader performance, skill selection, forecast accuracy or hypothetical follower returns. Specify the training cutoff, outcome-availability cutoff, flow window, price baseline, fees, execution assumptions, sample exclusions and dependence treatment. Separate exploratory choices from held-out validation. Preserve negative results and correct an earlier interpretation when stronger evidence changes it.

If a result needs unavailable inputs, describe the required schema and acquisition conditions. Do not claim exact reproduction without matching input versions and a recorded run. A profitable historical trader or a lower Brier score does not establish executable follower profit.

## API and source changes

Update the [API contract](src/pmkt/ui/API_CONTRACT.md) with any response or column change. Preserve Decimal price handling, UTC timestamps, acquisition records and the single-writer SQLite boundary. Keep network access explicit and the default example synthetic. Contributions must not add automatic live order submission.

Include required attribution for copied code or assets and identify their licenses. Third-party market data retains upstream terms. Report a suspected secret exposure without posting the secret in an issue or pull request.
