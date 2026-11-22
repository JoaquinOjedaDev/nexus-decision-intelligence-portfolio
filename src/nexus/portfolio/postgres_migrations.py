"""Read the same canonical SQL in an editable checkout or an installed wheel.

Build configuration includes the infra directory as package resources. There is
no separately maintained SQL copy and no database dependency at import time.
Both migrations and runtime ledger verification must call this loader.
"""

from __future__ import annotations

from importlib.resources import files
from pathlib import Path

MIGRATION_FILES = {
    2: "002_portfolio_ops.sql",
    3: "003_portfolio_workflow.sql",
    4: "004_portfolio_runtime.sql",
    5: "005_portfolio_transport.sql",
    6: "006_portfolio_transport_fence.sql",
    7: "007_portfolio_publication.sql",
    8: "008_portfolio_source_watch.sql",
    9: "009_portfolio_recovery.sql",
    10: "010_portfolio_recovery_publication.sql",
    11: "011_portfolio_recovery_resolution.sql",
    12: "012_portfolio_capture_health.sql",
}
MAX_MIGRATION_BYTES = 256 * 1024


def read_migration_sql(version: int) -> bytes:
    """Return uncached UTF-8 SQL bytes; never search the cwd or an ambient path.

    An installed package with a missing resource fails closed. The source fallback
    is available only when this exact module lives in the expected src/ checkout.
    Reading without caching makes changes visible to the checksum ledger verifier.
    """
    if type(version) is not int or version not in MIGRATION_FILES:
        raise ValueError("Unsupported portfolio migration version")
    name = MIGRATION_FILES[version]
    resources = files("nexus.portfolio").joinpath("sql_migrations")
    if resources.is_dir():
        raw = resources.joinpath(name).read_bytes()
    else:
        module = Path(__file__).resolve()
        root = module.parents[3]
        if (
            module != root / "src/nexus/portfolio/postgres_migrations.py"
            or not (root / "pyproject.toml").is_file()
        ):
            raise FileNotFoundError("Installed portfolio SQL resources are missing")
        raw = (root / "infra/postgres/migrations" / name).read_bytes()
    if not 0 < len(raw) <= MAX_MIGRATION_BYTES or b"\x00" in raw:
        raise ValueError("Portfolio migration bytes violate the resource contract")
    raw.decode("utf-8")
    return raw
