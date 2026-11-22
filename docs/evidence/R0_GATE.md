# R0 foundational contract

R0 is the repository's early contract and architecture milestone. The executable R0 verifier checks required files, schema examples, local storage and dbt behavior. Its checks are narrower than the current portfolio workflow and the original full production gates.

From a locked local environment, run `uv run nexus r0 verify` for a fresh result. Docker-backed Compose proof is optional and requires a separate local environment. For the current single-host demonstration, run `uv run --group portfolio nexus portfolio verify` and read the [portfolio verification record](../portfolio/VERIFICATION.md).

The project uses synthetic fixtures, a versioned decision grain, explicit authorization roles, immutable recommendation identities, and a mock downstream effect. The [architecture lifecycle](../architecture/data-lifecycle.md) and [case study](../portfolio/CASE_STUDY.md) explain how those foundations connect to the current application. Historical R0 results in the private development archive do not attest this new source snapshot automatically.
