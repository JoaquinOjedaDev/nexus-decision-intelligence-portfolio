# NEXUS Decision Intelligence

NEXUS is a local supply chain application for replenishment planning. It forecasts demand and recommends order quantities within budget, supplier, and storage constraints.

- Validates synthetic inputs and builds point-in-time tables with dbt and DuckDB.
- Generates 14-day demand forecasts and compares plans under normal demand, demand surge, and supplier delay.
- Requires independent human approval and records mock orders, retries, and simulated outcomes in an audit trail.

## Run locally

Requires Python 3.12 or 3.13 and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```sh
uv sync --locked --group portfolio --group dev
uv run --group portfolio nexus portfolio prepare-runtime
uv run --group portfolio nexus portfolio serve-runtime
```

Open [localhost:8766](http://127.0.0.1:8766). Run `uv run --group portfolio nexus portfolio verify-runtime` to check the local workflow.

[Architecture](docs/architecture/data-lifecycle.md) · [Case study](docs/portfolio/CASE_STUDY.md) · [Security](SECURITY.md) · [MIT License](LICENSE)
