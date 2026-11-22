# NEXUS: from forecasts to accountable decisions

NEXUS asks a business question: which replenishment orders should an operator approve when demand is uncertain, budget is limited, and supplier timing changes? The product connects data engineering, forecasting, optimization, approval policy, and outcome evidence instead of treating a prediction endpoint as the final deliverable.

## Data engineering

Synthetic CSV and JSONL inputs model batch records and inventory events. Pydantic contracts reject invalid inputs, quarantine preserves rejected evidence, and dbt builds point-in-time Gold tables in DuckDB. Point-in-time joins matter because information learned after a forecast cutoff must not leak into training or evaluation. Event-stream and durable-orchestration experiments live in separate, gated service lanes.

## Data science and machine learning

The repository evaluates probabilistic forecasts using rolling temporal windows and mature labels. Seasonal controls compete with LightGBM challengers. The frozen R3B result is CHALLENGER_LOST: the seasonal baseline remains champion. Keeping the simpler model when the challenger fails is a scientific result, not a project failure. Forecast uncertainty feeds planning; it is not replaced by an unjustified point estimate.

The interactive demo verifies pinned champion artifacts before computing planning recommendations. Its independent fresh R2 validation run demonstrates ingestion and dbt quality; it is not falsely presented as the training source of the frozen R3B champion. Lineage distinguishes those two evidence streams.

## Decision science

OR-Tools CP-SAT computes replenishment quantities under budget, supplier, pack-size, and inventory constraints. An independent feasibility check validates the result. Normal demand, demand surge, and supplier delay scenarios expose trade-offs. A heuristic is the comparison policy; synthetic scenario costs and fill rates are computed outcomes, not promised savings. Unsupported or infeasible results must fail closed.

## ML engineering and product engineering

FastAPI serves a lightweight control tower. SQLite persists recommendations, approval events, an execution outbox, unique mock ERP effects, and outcomes. The API enforces role permissions and binds approvals to a recommendation fingerprint. Repeated execution requests must return the existing effect rather than perform another purchase. This is bounded local idempotency, not a distributed exactly-once claim.

## AI engineering boundary

The default evidence assistant retrieves excerpts from project documents with source citations. An optional installed local model selects one read-only tool for the current recommendation and can cite a versioned, fictional supplier or approval document. The server supplies all quantities and document claims from verified evidence, checks the principal and current recommendation again, and can veto unsupported requests. Neither mode can approve or execute. This is a bounded local AI evaluation over synthetic evidence, not a production RAG or autonomous purchasing claim.

## Engineering judgment

A single-host portfolio tier reduces setup friction and makes the core workflow inspectable. The existing broker, Postgres, Dagster, and scientific gate code remains available for deeper engineering discussion. Production architecture would externalize identity, durable queues, monitoring, and state; the portable demo intentionally does not pretend those deployments have occurred.

## Honest resume wording

Built a reproducible supply-chain decision-intelligence prototype integrating point-in-time dbt data products, probabilistic forecasting, constrained optimization, and a human-approved execution workflow.

Implemented durable audit and idempotent mock-execution flows with server-side authorization, approval fingerprint binding, and restart tests; delivered a responsive control tower and evidence-backed technical case study.

Evaluated learned challengers against seasonal controls and retained the baseline when the challenger failed the promotion criteria. Do not add revenue, production traffic, latency, scale, or savings claims unless independently measured.

Built an opt-in local-model assistant that selects governed decision evidence, cites fictional supplier and policy documents, and refuses purchase actions while leaving human approval separate.
