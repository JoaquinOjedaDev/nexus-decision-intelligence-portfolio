# NEXUS Decision Intelligence

NEXUS is a supply chain application for replenishment planning. It forecasts demand and recommends order quantities within budget, supplier, and storage constraints.

**[Open NEXUS in your browser](https://joaquinojedadev.github.io/nexus-decision-intelligence-portfolio/)** to explore a recorded synthetic plan, compare scenarios, and inspect the approval trail.

## Analysis guide

Pick one SKU and location, then follow its recommendation through four checks.

1. **Trace the inputs.** Follow the synthetic records through validation, quarantine, and point-in-time dbt tables in DuckDB. Check what data was available at the forecast cutoff. [Data lifecycle](docs/architecture/data-lifecycle.md)
2. **Read the forecast.** Inspect the 14-day demand range, then review the separate model comparison that kept the seasonal baseline. [Case study](docs/portfolio/CASE_STUDY.md)
3. **Assess the plan.** Compare costs and service levels under normal demand, demand surge, and supplier delay. Check pack sizes, capacity, and budget against the independent feasibility result. [Planning formulation](docs/decisioning/r4a-mathematical-formulation.md)
4. **Verify execution controls.** Check that an independent approver authorizes the exact recommendation and that a retry returns the same mock order reference. Trace the simulated outcome in the audit trail.

[Reproduce the workflow](docs/portfolio/CONNECTED_RUNTIME.md) · [Security](SECURITY.md) · [MIT License](LICENSE)
