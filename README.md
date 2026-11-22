# NEXUS Decision Intelligence

NEXUS is a local supply chain application for replenishment planning. It forecasts demand and recommends order quantities within budget, supplier, and storage constraints.

## Analysis guide

Pick one SKU and location, then follow its recommendation through four checks.

1. **Trace the inputs.** Follow the synthetic records through validation, quarantine, and point-in-time dbt tables in DuckDB. Check what data was available at the forecast cutoff. [Data lifecycle](docs/architecture/data-lifecycle.md)
2. **Read the forecast.** Inspect the 14-day demand range, then review the separate model comparison that kept the seasonal baseline. [Case study](docs/portfolio/CASE_STUDY.md)
3. **Assess the plan.** Compare costs and service levels under normal demand, demand surge, and supplier delay. Check pack sizes, capacity, and budget against the independent feasibility result. [Planning formulation](docs/decisioning/r4a-mathematical-formulation.md)
4. **Verify execution controls.** Check that an independent approver authorizes the exact recommendation and that a retry returns the same mock order reference. Trace the simulated outcome in the audit trail. [Connected runtime](docs/portfolio/CONNECTED_RUNTIME.md)

## Run locally

Requires Python 3.12 or 3.13 and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```sh
uv sync --locked --group portfolio --group dev
uv run --group portfolio nexus portfolio prepare-runtime
uv run --group portfolio nexus portfolio serve-runtime
```

Open [localhost:8766](http://127.0.0.1:8766). Run `uv run --group portfolio nexus portfolio verify-runtime` to check the local workflow.

[Security](SECURITY.md) · [MIT License](LICENSE)
