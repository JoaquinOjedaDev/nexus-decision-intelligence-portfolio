# CI validation tiers

NEXUS keeps every verification gate while reducing accidental GitHub Actions usage. The default
workflow is intentionally lean; expensive cross-platform and live-service evidence is an explicit
milestone operation. Workflow design reduces hosted-runner minutes, but billing protection must be
configured separately in GitHub.

## Lean validation

The quality jobs initialize an isolated PostgreSQL 16 test cluster on Ubuntu
24.04. The native tests contribute to the unchanged 85% coverage requirement.
An `always()` step stops only that task-owned cluster; connection secrets stay
in ignored runner-local files. See [the verification record](../portfolio/VERIFICATION.md)
for dated results. For current hosted status, open the Actions tab of the
repository containing this checkout and select the exact commit's run.

Run CI for the exact PR head and check both quality jobs before merging. A local
pass or a successful run for an earlier commit does not establish a current CI
pass. The zero-paid-service policy requires confirmed no-overage protection
before starting hosted jobs. Do not bypass a required branch check.

Pull requests, pushes to `main`, and manually dispatched `lean` runs execute only the `quality`
matrix on Python 3.12 and 3.13:

- package build;
- Ruff lint and format checks;
- mypy;
- portable unit, property, contract and isolated native portfolio tests with coverage.

The portable selector excludes live-service R2D, complete R2D acceptance, R3B, and R4A tests. It
also skips the long executable release gates. This is the required feedback loop during ordinary
development.

Both matrix jobs use GitHub-hosted runners. To prevent paid overage, configure an Actions budget that permits no paid overage
and enable **Stop usage when budget limit is reached**. GitHub documents the behavior in
[Setting up budgets to control spending on metered products](https://docs.github.com/en/billing/how-tos/set-up-budgets).

## Full validation

Use a full run only at a milestone or release checkpoint and only when the available Actions quota
is acceptable:

1. Open the repository's **Actions** page.
2. Select the **ci** workflow and choose **Run workflow**.
3. Set `validation_tier` to `full`.
4. Run it against the exact branch or commit being promoted.

The full tier adds Windows smoke, portable cross-platform receipt joins, Redpanda/PostgreSQL/Dagster
service gates, learned-model and optimization gates, and the executable R0-R3 checks. Selecting
`lean` never implies that a skipped full gate passed.

## Local development

Portable development checks can run without GitHub-hosted compute:

```powershell
uv sync --locked --all-groups
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run python -m pytest -m "not r2d_acceptance and not r2d_integration and not r2d_service and not r3b and not r4a"
```

Run focused service or scientific suites locally when changing those components. Their gate pages
record the exact commands, environment, receipts, and nonclaims. Historical evidence remains bound
to its original commit; changing governed code requires a new milestone receipt before making a
current-identity PASS claim.

For the combined quality selection, provision a dedicated test cluster with
`scripts/native_postgres_test_cluster.py`, following its guarded init/start/stop
commands, and set `NEXUS_PORTFOLIO_PG_TEST_CONFIG` to its generated private
configuration before pytest. Never reuse an application database. Native schema
writes require authorization; without that configuration native tests skip and
the current portable-only coverage result is below the threshold. The workflow
contains the complete Linux initialization and cleanup commands.

Ordinary development can be verified locally without GitHub-hosted compute. The lean hosted workflow
still consumes included private-repository minutes; it becomes zero-overage only with the hard
budget described above. If the allowance is exhausted, continue locally and wait for it to reset
before requesting a new hosted attestation.
