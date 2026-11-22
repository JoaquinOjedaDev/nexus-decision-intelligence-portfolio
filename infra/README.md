# Local infrastructure profiles

The [flagship image candidate](flagship/README.md) now has a locked service group,
offline hash-verified Linux build and non-root read-only source-backed smoke.
It does not yet replace the operational skeleton below with a fully verified stack.

R0 deliberately keeps data preparation native to the locked Python environment. The selected laptop storage path is Parquet + DuckDB/dbt-duckdb; no abandoned object-store dependency is required.

Validate without starting services:

```powershell
docker compose -f infra/compose.yaml config --quiet
```

Start the operational skeleton:

```powershell
docker compose -f infra/compose.yaml --profile ops up -d --wait
docker compose -f infra/compose.yaml --profile ops ps
docker stats --no-stream
```

Start observability only when needed:

```powershell
docker compose -f infra/compose.yaml --profile ops --profile observability up -d --wait
```

Stop services without deleting recoverable volumes:

```powershell
docker compose -f infra/compose.yaml --profile ops --profile observability down
```

Image tags are pinned. Release images will be promoted by digest after the R0 pull/scan check. Redpanda is limited to one core and 768 MB application memory; PostgreSQL is limited to 512 MB. Prometheus retains six hours locally.
