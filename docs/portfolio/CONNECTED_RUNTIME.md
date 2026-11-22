# Connected local runtime

This optional mode links one validated synthetic source package to a fresh Gold snapshot, seasonal forecast, constrained plan, approval workflow, and durable simulated feedback. It uses a separate state directory from the frozen portfolio demo.

From the repository root:

```sh
uv sync --locked --group portfolio --group dev
uv run --group portfolio nexus portfolio prepare-runtime
uv run --group portfolio nexus portfolio serve-runtime
```

Open `http://127.0.0.1:8766/`. The default state is `artifacts/portfolio-runtime/`. Use a new state directory for changed scientific code or sources; startup verifies retained artifacts and refuses stale state rather than replacing decisions.

| Stage | Current local behavior |
| --- | --- |
| Source and Gold | Checksum-bound CSV/JSONL, contracts, quarantine, dbt, and point-in-time tables. |
| Forecast and plan | A new 14-day seasonal distribution and optimizer plan use the same source keys, cutoff, inventory, and lineage. |
| Approval and action | A material source change invalidates unexecuted advice. A new fingerprint requires a new independent approval before one idempotent mock effect. |
| Feedback | Simulated outcomes and measured local source freshness feed monitoring and a persistent replan queue. |

The frozen demo's R3B champion artifact is a **separate scientific fixture**. Connected mode does not claim that a fresh Gold snapshot trained that frozen model. The [learning experiment](LOCAL_LEARNING.md) trains a separate candidate on mature synthetic labels; its coverage guards reject promotion. Source changes do not automatically retrain or switch a model.

The repository contains opt-in PostgreSQL, broker, and orchestration paths for engineering exploration. This local mode does not prove the original distributed-service load, recovery, soak, deployment, or production identity gates. See the [release scope](RELEASE.md) and [security boundaries](SECURITY.md).
