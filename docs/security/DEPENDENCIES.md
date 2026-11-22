# Dependency and local tracking boundaries

The repository uses a locked uv dependency graph. On 2026-11-21, `uv audit --locked --system-certs` found **no known vulnerabilities or adverse project statuses in 180 audited packages**. OpenTelemetry API 1.45.2 resolves the finding from the initial scan. [OpenTelemetry advisory](https://github.com/open-telemetry/opentelemetry-python/security/advisories/GHSA-mf5v-w3w3-f6cg)

```sh
uv audit --locked --system-certs
```

The optional MLflow path uses an exact, generated local SQLite tracking URI through the Python SDK. It does not start an MLflow HTTP Gateway, and its wrapper rejects remote or query-bearing tracking URIs. The default portfolio demo does not require MLflow, a hosted model, or provider credentials.

The lock audit covers known package advisories. It is separate from code review, secret scanning, threat modeling, and the original production service gates. See the [portfolio security boundaries](../portfolio/SECURITY.md) and root [security policy](../../SECURITY.md).
