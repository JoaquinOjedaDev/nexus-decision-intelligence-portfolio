# Immutable source runtime; no host environment, database, model or credentials.
FROM python@sha256:d50fb7611f86d04a3b0471b46d7557818d88983fc3136726336b2a4c657aa30b

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    HOME=/tmp \
    TZ=UTC

WORKDIR /app
COPY pyproject.toml uv.lock ./
# Explicit named context: only hash-verified Linux wheels and the frozen export.
# No package-index network or TLS exceptions during image construction.
RUN --mount=type=bind,from=nexus_wheels,source=.,target=/wheels,ro \
    python -m pip install --no-cache-dir --no-index --find-links=/wheels \
    --only-binary=:all: --require-hashes -r /wheels/requirements.txt \
    && python -m pip check

# Source location keeps PROJECT_ROOT=/app, with the exact frozen runtime assets.
COPY src ./src
COPY configs ./configs
COPY contracts ./contracts
COPY dbt ./dbt
COPY fixtures ./fixtures
COPY docs ./docs
COPY infra/postgres/migrations ./infra/postgres/migrations
COPY README.md LICENSE ./
COPY scripts/service_image_smoke.py ./scripts/service_image_smoke.py
COPY scripts/bootstrap_ops_service.py ./scripts/bootstrap_ops_service.py
COPY scripts/ops_profile_probe.py ./scripts/ops_profile_probe.py
COPY scripts/ops_business_acceptance.py ./scripts/ops_business_acceptance.py
RUN python -c "import dbt.cli.main, psycopg, confluent_kafka, psutil; import nexus.portfolio.service_worker, nexus.portfolio.service_supervisor"

# The deployment must mount owned writable state; source remains immutable.
USER 10001:10001
ENTRYPOINT ["python", "-m", "nexus.cli"]
CMD ["portfolio", "service", "--help"]
