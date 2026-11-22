# Isolated service image foundation

This is an operational **image candidate**, not a completed Compose profile or
release. Broker/PostgreSQL provisioning, scoped identities, observability,
learning/AI services, load/recovery/soak and promotion gates remain open.

Earlier isolated development runs checked a fresh pinned broker/database and a
synthetic two-consumer business loop. Their dated operational receipts remain in
the private development archive; this reviewer copy does not present them as
current-commit release evidence. Full profile readiness remains open.
The source runtime stays under `/app/src` so the existing repository-root asset
contract resolves to `/app`. Host virtual environments, data, credentials and
runtime evidence are excluded. This is not an installed-wheel proof.

## Frozen dependency preparation

`uv sync --frozen --no-dev --group service` includes portfolio/dbt/optimization,
PostgreSQL, Kafka and the supervisor's explicit `psutil` dependency. It does not
silently install training/tracking/orchestration groups or use paid providers.
Use a separate `UV_PROJECT_ENVIRONMENT` for clean-install checks; never replace
the environment of an already running service.

Prepare a fresh Linux-amd64 wheel bundle from the committed lock (Python 3.12,
Debian Bookworm base). The host downloader needs a normally trusted TLS connection;
do not use `--trusted-host`, disable certificate validation or modify antivirus.
The verified host downloader was pip 26.2.1, bootstrapped from its PyPI wheel with
SHA-256 `71138adf1f4ca900cdb7d289c21b7494329f2332b6d85f0e1c42108c0384ed3e`.
The image's pip comes from its digest-pinned Python base.

```powershell
uv export --frozen --no-dev --group service --no-emit-project --format requirements-txt --output-file artifacts/service-wheels-new/requirements.txt
python -m pip download --require-hashes --only-binary=:all: --platform manylinux_2_28_x86_64 --platform manylinux_2_17_x86_64 --platform manylinux2014_x86_64 --platform manylinux_2_34_x86_64 --python-version 3.12 --implementation cp --abi cp312 --dest artifacts/service-wheels-new -r artifacts/service-wheels-new/requirements.txt
docker build --network=none --pull=false --platform linux/amd64 --build-context nexus_wheels=artifacts/service-wheels-new -f infra/flagship/Dockerfile -t nexus-flagship:service-candidate .
```

Use the exact export and its accompanying wheels as the named build context.
The build installs with `--no-index --require-hashes`, verifies package dependency
consistency and imports the actual service modules. No wheels, Git state or host
evidence are copied into the resulting runtime layers. The base image must already
be available locally for a fully disconnected build.

## Actual non-root, read-only smoke

Create a fresh evidence directory and resolve it to an absolute host path. Inspect
the built image ID and use that ID, not a mutable tag, for the following run:

```powershell
docker image inspect nexus-flagship:service-candidate --format '{{.Id}}'
docker run --rm --network none --read-only --cap-drop ALL --security-opt no-new-privileges --pids-limit 256 --memory 1536m --cpus 1 --tmpfs /tmp:rw,nosuid,nodev,size=256m --mount 'type=bind,source=ABSOLUTE_FRESH_EVIDENCE_DIRECTORY,target=/evidence' --entrypoint python IMAGE_ID /app/scripts/service_image_smoke.py
```

The script refuses root/host execution and existing output. It builds synthetic
source data, executes all 189 dbt results, forecasts/optimizes and independently
verifies evidence. It retains code/lock/harness identity, versions and timings in
`receipt.json`, plus the raw runtime artifacts. `/app` remains unwritable and the
launcher disables runtime networking. The container is ephemeral; its newly
generated evidence remains in the explicitly mounted directory.

This smoke is neither asynchronous service p95 nor a full-service resource test.
No host database, broker, demo, external ERP or model API is contacted. A complete
service release still needs the deployment manifest, explicit fresh bootstrap,
credential boundaries, real readiness and original reliability gates.
