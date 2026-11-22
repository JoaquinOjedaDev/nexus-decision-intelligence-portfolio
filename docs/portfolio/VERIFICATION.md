# Local portfolio verification

This record describes the synthetic local portfolio path tested on 2026-11-10. Results for older private development commits and the original distributed-service gates are not transferred as current evidence.

| Check | Observed result |
| --- | --- |
| Clean-checkout portfolio acceptance | 22/22 workflow checks passed |
| dbt build and tests | 189 PASS, 0 WARN, 0 ERROR |
| Constrained plan | OPTIMAL and independently feasible; four recommendations, $2,974 within a synthetic $3,600 budget |
| Synthetic source validation | 167 records; 163 accepted and 4 quarantined with explicit reasons |
| Fresh demo startup | `prepare` then `serve` using the same new state directory succeeded; the control tower returned HTTP 200 on loopback |
| Focused document and AI tests | 3 passed; installed-model holdout skipped by default because it requires an explicit local opt-in |
| Locked dependency audit | 180 packages, no known vulnerabilities or adverse statuses on 2026-11-21 |

The default assistant cited source evidence and refused an action during acceptance. The test also checked exact approval binding, a single local mock effect after a retry, durable outcome and incident state after restart, and evidence hashes. These are local controls over simulated data and effects.

## Reproduce

From a fresh checkout with Python 3.12 or 3.13 and uv:

```sh
uv sync --locked --group portfolio --group dev
uv run --group portfolio nexus portfolio verify
```

For the interactive path, use the separate `prepare` and `serve` commands in the [demo guide](DEMO.md). On a managed network, `uv --system-certs` uses the operating system trust store while keeping TLS verification enabled.

Check this repository's Actions run for the **exact public commit** after publication. This local preview has no hosted CI result of its own. The default CI quality jobs and the optional full validation tier are described in [CI tiers](../runbooks/ci-tiers.md).

## Limits

The data, suppliers, policies, outcomes, costs, and orders are synthetic or fictional. No production deployment, external ERP, customer savings, full-scale load, multi-hour soak, or original G2-G7 acceptance is claimed. The installed-model holdout is a separate opt-in diagnostic and is not part of the default portfolio acceptance.
