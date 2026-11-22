"""Manage ONLY a new task-owned native PostgreSQL acceptance cluster.

No existing Windows service or installation data directory is touched.
Credentials stay under ignored artifacts with owner-only access.
This is a local functional test runtime, not the pinned Compose/soak gate.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import secrets
import socket
import subprocess
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

PROJECT = Path(__file__).resolve().parents[1]
ADMIN = "nexus_native_admin"
MIGRATOR = "nexus_ops_migrator"
APP = "nexus_ops_app"
DATABASE = "nexus_portfolio_test_foundation"
CONFIG = "connection-secrets.json"
FLAGS = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0


def safe_root(path: Path) -> Path:
    raw = path.absolute()
    if any(part.is_symlink() or part.is_junction() for part in (raw, *raw.parents)):
        raise ValueError("cluster path must not traverse a link")
    root = raw.resolve()
    if root == PROJECT / "artifacts" or not root.is_relative_to(PROJECT / "artifacts"):
        raise ValueError("cluster must be a separate project artifacts directory")
    return root


def run_binary(arguments: list[str], *, log: Path | None = None) -> str:
    # A Windows child server can inherit capture pipes after pg_ctl exits.
    # Real log handles avoid waiting indefinitely for that descendant's EOF.
    if log is not None:
        with log.open("a", encoding="utf-8") as handle:
            result = subprocess.run(
                arguments,
                stdout=handle,
                stderr=subprocess.STDOUT,
                timeout=45,
                creationflags=FLAGS,
                check=False,
            )
        if result.returncode != 0:
            raise RuntimeError(f"{Path(arguments[0]).name} failed; inspect the private cluster log")
        return ""
    result = subprocess.run(
        arguments, capture_output=True, text=True, timeout=45, creationflags=FLAGS, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(f"{Path(arguments[0]).name} failed; inspect the private cluster log")
    return result.stdout.strip()


def protect(root: Path) -> None:
    if os.name == "nt":
        output = run_binary(["whoami.exe", "/user", "/fo", "csv", "/nh"])
        sid = next(csv.reader(io.StringIO(output)))[1]
        if not sid.startswith("S-1-"):
            raise ValueError("cannot identify the current Windows user")
        run_binary(
            [
                "icacls.exe",
                str(root),
                "/inheritance:r",
                "/grant:r",
                f"*{sid}:(OI)(CI)F",
                "*S-1-5-18:(OI)(CI)F",
            ]
        )
    else:
        root.chmod(0o700)


def read_config(root: Path) -> dict[str, Any]:
    root = safe_root(root)
    value: dict[str, Any] = json.loads((root / CONFIG).read_text(encoding="utf-8"))
    if (
        value["contract_version"] != "nexus.native-postgres-test.v1"
        or Path(value["cluster_root"]).resolve() != root
        or Path(value["data_directory"]).resolve() != root / "data"
        or value["expected_database"] != DATABASE
        or not 49152 <= value["expected_port"] <= 65535
    ):
        raise ValueError("cluster identity or path mismatch")
    for field, name in (
        ("admin_dsn", ADMIN),
        ("migration_dsn", MIGRATOR),
        ("application_dsn", APP),
    ):
        parsed = conninfo_to_dict(value[field])
        if (
            set(parsed) != {"host", "port", "dbname", "user", "password", "connect_timeout"}
            or parsed["host"] != "127.0.0.1"
            or parsed["port"] != str(value["expected_port"])
            or parsed["dbname"] != DATABASE
            or parsed["user"] != name
            or parsed["connect_timeout"] != "5"
            or len(parsed["password"]) != 64
            or any(char not in "0123456789abcdef" for char in parsed["password"])
        ):
            raise ValueError("cluster DSN is not the exact private loopback identity")
    if set(value["binary_hashes"]) != {"postgres", "initdb", "pg_ctl"}:
        raise ValueError("cluster binary inventory mismatch")
    for name, expected in value["binary_hashes"].items():
        binary = Path(value["bin_dir"]) / (name + ".exe" if os.name == "nt" else name)
        if (
            any(part.is_symlink() or part.is_junction() for part in (binary, *binary.parents))
            or hashlib.sha256(binary.read_bytes()).hexdigest() != expected
        ):
            raise ValueError("cluster executable bytes/path changed")
    return value


def inspect_server(config: dict[str, Any]) -> dict[str, Any]:
    with psycopg.connect(config["admin_dsn"], connect_timeout=5) as db:
        row = db.execute(
            "SELECT current_database(),current_user,inet_server_port(),"
            "current_setting('data_directory'),current_setting('server_version'),"
            "current_setting('listen_addresses'),current_setting('fsync'),"
            "current_setting('synchronous_commit'),current_setting('data_checksums'),"
            "(SELECT system_identifier::text FROM pg_control_system())"
        ).fetchone()
        assert row is not None
        marker = db.execute("SELECT cluster_id FROM public.nexus_acceptance_identity").fetchall()
    if (
        row[0] != DATABASE
        or row[1] != ADMIN
        or row[2] != config["expected_port"]
        or Path(row[3]).resolve() != Path(config["data_directory"]).resolve()
        or row[5:9] != ("127.0.0.1", "on", "on", "on")
        or marker != [(config["cluster_id"],)]
    ):
        raise ValueError("server is not the exact isolated acceptance cluster")
    verify_retained_identity(config, row[9], create_identity=False)
    return {
        "status": "RUNNING",
        "database": row[0],
        "port": row[2],
        "server_version": row[4],
        "listen_addresses": row[5],
        "fsync": row[6],
        "synchronous_commit": row[7],
        "data_checksums": row[8],
        "cluster_id": config["cluster_id"],
        "system_identifier": row[9],
        "scope": "native PostgreSQL functional tests, not Compose or a soak",
    }


def start(root: Path) -> dict[str, Any]:
    value = read_config(root)
    port = value["expected_port"]
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))
    run_binary(
        [
            str(Path(value["bin_dir"]) / ("pg_ctl.exe" if os.name == "nt" else "pg_ctl")),
            "-D",
            value["data_directory"],
            "-l",
            str(root / "postgres.log"),
            "-w",
            "-t",
            "30",
            "-o",
            (
                f"-h 127.0.0.1 -p {port} -c shared_buffers=64MB -c work_mem=2MB "
                "-c max_connections=20 -c max_worker_processes=2 -c max_parallel_workers=2 "
                "-c autovacuum_max_workers=1 -c fsync=on -c synchronous_commit=on "
                # Ubuntu packages default to a system-owned Unix-socket directory.
                # This isolated SCRAM-authenticated cluster uses loopback TCP only.
                "-c full_page_writes=on -c unix_socket_directories="
            ),
            "start",
        ],
        log=root / "controller.log",
    )
    return value


def initialize(root: Path, bin_dir: Path, port: int) -> dict[str, Any]:
    root = safe_root(root)
    if root.exists():
        raise ValueError("initialization requires a new directory; preserve prior evidence")
    if not 49152 <= port <= 65535:
        raise ValueError("use an explicit high loopback-only test port")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))
    bin_dir = bin_dir.resolve()

    def executable(name: str) -> Path:
        return bin_dir / (name + ".exe" if os.name == "nt" else name)

    version = run_binary([str(executable("postgres")), "--version"])
    binary_hashes = {
        name: hashlib.sha256(executable(name).read_bytes()).hexdigest()
        for name in ("postgres", "initdb", "pg_ctl")
    }
    root.mkdir(parents=True, exist_ok=False)
    protect(root)
    passwords = {name: secrets.token_hex(32) for name in (ADMIN, MIGRATOR, APP)}
    common = {"host": "127.0.0.1", "port": port, "dbname": DATABASE, "connect_timeout": 5}
    value = {
        "contract_version": "nexus.native-postgres-test.v1",
        "cluster_id": secrets.token_hex(24),
        "cluster_root": str(root),
        "data_directory": str(root / "data"),
        "bin_dir": str(bin_dir),
        "binary_hashes": binary_hashes,
        "installed_version": version,
        "expected_database": DATABASE,
        "expected_port": port,
        "expected_admin_user": ADMIN,
        "expected_migration_user": MIGRATOR,
        "expected_application_user": APP,
        "admin_dsn": make_conninfo(**common, user=ADMIN, password=passwords[ADMIN]),
        "migration_dsn": make_conninfo(**common, user=MIGRATOR, password=passwords[MIGRATOR]),
        "application_dsn": make_conninfo(**common, user=APP, password=passwords[APP]),
    }
    with (root / CONFIG).open("x", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2)
    password_file = root / "bootstrap-password.txt"
    with password_file.open("x", encoding="utf-8") as handle:
        handle.write(passwords[ADMIN] + "\n")
    run_binary(
        [
            str(executable("initdb")),
            "-D",
            str(root / "data"),
            "-U",
            ADMIN,
            "--pwfile",
            str(password_file),
            "--auth=scram-sha-256",
            "--encoding=UTF8",
            "--locale=C",
            "--data-checksums",
            "--no-instructions",
        ],
        log=root / "initdb.log",
    )
    start(root)
    return bootstrap(root)


def bootstrap_dsn(value: dict[str, Any]) -> str:
    return make_conninfo(**{**conninfo_to_dict(value["admin_dsn"]), "dbname": "postgres"})


def verify_bootstrap(db: Any, value: dict[str, Any], *, create_identity: bool = True) -> None:
    row = db.execute(
        "SELECT current_database(),current_user,inet_server_port(),"
        "current_setting('data_directory'),current_setting('listen_addresses'),"
        "(SELECT system_identifier::text FROM pg_control_system())"
    ).fetchone()
    if (
        row is None
        or row[:3] != ("postgres", ADMIN, value["expected_port"])
        or Path(row[3]).resolve() != Path(value["data_directory"]).resolve()
        or row[4] != "127.0.0.1"
    ):
        raise ValueError("bootstrap server identity mismatch; no mutation or stop is authorized")
    verify_retained_identity(value, row[5], create_identity=create_identity)


def verify_retained_identity(
    value: dict[str, Any], system_identifier: str, *, create_identity: bool
) -> None:
    identity = {
        "cluster_id": value["cluster_id"],
        "system_identifier": system_identifier,
        "data_directory": value["data_directory"],
        "port": value["expected_port"],
    }
    path = Path(value["cluster_root"]) / "bootstrap-identity.json"
    if path.exists():
        if json.loads(path.read_text(encoding="utf-8")) != identity:
            raise ValueError("bootstrap system identity changed")
    elif create_identity:
        with path.open("x", encoding="utf-8") as handle:
            json.dump(identity, handle, indent=2)
    else:
        raise ValueError("retained bootstrap identity is missing")


def bootstrap(root: Path) -> dict[str, Any]:
    """Explicit recovery of a started but not yet provisioned private cluster."""
    value = read_config(root)
    passwords = {
        name: conninfo_to_dict(value[field])["password"]
        for name, field in ((MIGRATOR, "migration_dsn"), (APP, "application_dsn"))
    }
    # Only our new cluster's bootstrap database; no implicit default host or port.
    with psycopg.connect(bootstrap_dsn(value), autocommit=True) as db:
        verify_bootstrap(db, value)
        if (
            db.execute("SELECT 1 FROM pg_database WHERE datname=%s", (DATABASE,)).fetchone()
            or db.execute(
                "SELECT 1 FROM pg_roles WHERE rolname IN (%s,%s)", (MIGRATOR, APP)
            ).fetchone()
        ):
            raise ValueError(
                "provisioning already began; inspect retained state, do not overwrite it"
            )
        db.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DATABASE)))
        for name in (MIGRATOR, APP):
            db.execute(
                sql.SQL(
                    "CREATE ROLE {} LOGIN PASSWORD {} "
                    "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION"
                ).format(sql.Identifier(name), sql.Literal(passwords[name]))
            )
        db.execute(
            sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(sql.Identifier(DATABASE))
        )
        db.execute(
            sql.SQL("GRANT CONNECT,CREATE ON DATABASE {} TO {}").format(
                sql.Identifier(DATABASE), sql.Identifier(MIGRATOR)
            )
        )
        db.execute(
            sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                sql.Identifier(DATABASE), sql.Identifier(APP)
            )
        )
    with psycopg.connect(value["admin_dsn"]) as db:
        db.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        db.execute("CREATE TABLE public.nexus_acceptance_identity (cluster_id TEXT NOT NULL)")
        db.execute(
            "INSERT INTO public.nexus_acceptance_identity VALUES (%s)", (value["cluster_id"],)
        )
    return {**inspect_server(value), "config_path": str(root / CONFIG)}


def stop(root: Path, *, partial: bool = False) -> dict[str, Any]:
    value = read_config(root)
    if partial:
        with psycopg.connect(bootstrap_dsn(value)) as db:
            verify_bootstrap(db, value, create_identity=False)
    else:
        inspect_server(value)  # Never stop a server without matching the live identity.
    run_binary(
        [
            str(Path(value["bin_dir"]) / ("pg_ctl.exe" if os.name == "nt" else "pg_ctl")),
            "-D",
            value["data_directory"],
            "-w",
            "-t",
            "30",
            "-m",
            "fast",
            "stop",
        ],
        log=root / "controller.log",
    )
    return {"status": "STOPPED", "cluster_id": value["cluster_id"], "data_preserved": True}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=("init", "start", "bootstrap", "status", "stop", "stop-partial")
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--bin-dir", type=Path)
    parser.add_argument("--port", type=int, default=55438)
    args = parser.parse_args()
    selected = safe_root(args.root)
    if args.action == "init":
        if args.bin_dir is None:
            parser.error("--bin-dir is required for init")
        result = initialize(selected, args.bin_dir, args.port)
    elif args.action == "start":
        result = inspect_server(start(selected))
    elif args.action == "status":
        result = inspect_server(read_config(selected))
    elif args.action == "bootstrap":
        result = bootstrap(selected)
    else:
        result = stop(selected, partial=args.action == "stop-partial")
    print(json.dumps(result, indent=2))
