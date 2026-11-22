"""Real loopback transport and cleanup tests; no database or scientific pipeline."""

from pathlib import Path

import httpx
import pytest
import test_portfolio_postgres_source_http_service as acceptance
from fastapi import FastAPI


@pytest.mark.parametrize("fail_inside", [False, True])
def test_http_harness_stops_its_server_on_success_and_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_inside: bool
) -> None:
    app = FastAPI()

    @app.get("/probe")
    def probe() -> dict[str, str]:
        return {"transport": "real-loopback"}

    monkeypatch.setattr(acceptance, "create_app", lambda *args, **kwargs: app)
    address = ""
    try:
        with acceptance.serving(tmp_path, None) as address:
            with httpx.Client(base_url=address, trust_env=False, timeout=5) as client:
                assert acceptance.response(client.get("/probe")) == {"transport": "real-loopback"}
            if fail_inside:
                raise ValueError("constructed acceptance failure")
    except ValueError as error:
        assert fail_inside and str(error) == "constructed acceptance failure"
    # Windows may time out rather than immediately refuse a closed loopback port.
    with (
        httpx.Client(base_url=address, trust_env=False, timeout=1) as client,
        pytest.raises((httpx.ConnectError, httpx.ConnectTimeout)),
    ):
        client.get("/probe")
