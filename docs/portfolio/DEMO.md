# Five-minute portfolio demonstration

## Setup

Prefer watching first? The [recorded browser walkthrough](assets/portfolio-walkthrough.webm) shows the approval-to-outcome loop and system evidence. See [verification results](VERIFICATION.md) for current local acceptance and its limits.

Quick visual references: [mobile overview](assets/overview-mobile.png), [decision detail](assets/decision-desktop.png), [evidence](assets/evidence-desktop.png), and [incident state](assets/incident-desktop.png).

Use Python 3.12 or 3.13 and uv. From a complete source checkout, run `uv sync --locked --group portfolio --group dev`, then `uv run --group portfolio nexus portfolio prepare --state-dir artifacts/demo-v0.1`, then `uv run --group portfolio nexus portfolio serve --state-dir artifacts/demo-v0.1`. Open the loopback URL printed after evidence validation. No Docker, API key, or paid service is required. The first preparation runs data validation and optimization; subsequent starts reuse durable state. Use `uv run` rather than invoking the virtual-environment executable directly: it places dbt on the subprocess PATH. If this state becomes stale after an update, follow the printed commands using a new state directory and preserve the old one.

## 1. Explain the question

"We need to choose replenishment orders under uncertainty. A forecast alone cannot tell the business which constrained action to take, who authorized it, or whether it helped." Show the six lifecycle stages at the top of the control tower and the explicit synthetic-data badge.

## 2. Inspect evidence

Open a recommendation. Explain the frozen seasonal champion, the rejected learned challenger, the planning constraints, and the independent feasibility check. Compare normal demand, demand surge, and supplier delay. Show actual computed scenario outcomes; avoid claiming guaranteed savings.

## 3. Govern the action

Use the planner view to inspect a computed recommendation already awaiting review, then switch to the approver. Approve the exact fingerprint and request mock execution. Refresh or retry: the existing execution result must be reused. Switch to auditor and inspect the ordered audit trail. Explain that persona switching is for teaching, not production login.

## 4. Inspect the current feedback boundary

Inspect the persisted outcome and monitoring view. Trigger the labeled synthetic freshness incident as planner. Confirm firing status and a durable local notification, resolve it, then refresh to confirm both transitions remain. This is a real local alert lifecycle over a simulated breach, not automatic source/drift monitoring or external paging. It does not yet trigger retraining or replanning.

## 5. Show the engineering boundaries

Ask the default evidence assistant about forecasts, approval policy, or synthetic data.
Follow its project-document citations. If the installed local model is enabled, open a
current recommendation and ask why its quantity was proposed, then ask for the
fictional supplier invoice due date. Inspect the bound data and document citations.
Ask it to execute an order and show refusal. Finish with the release scope: this
is a tested local portfolio demonstration. The connected runtime separately links
validated sources to fresh forecasts, measured monitoring and durable replanning.
The frozen demonstration keeps its own scientific provenance. A mature-label
candidate is rejected by its coverage guards; the baseline remains in service.
Production identity, full-scale reliability, deployment and prospective model
switching remain open. Real customer data and real ERP connections are not used.

## Restart and reset

Stop the server with Ctrl+C and run the same serve command again; SQLite state survives. To demonstrate a fresh session, choose a new `--state-dir` for prepare and serve. Do not delete evidence or reuse a populated database with a different seed. The CLI verification command uses an isolated temporary directory and leaves the interactive state unchanged.
