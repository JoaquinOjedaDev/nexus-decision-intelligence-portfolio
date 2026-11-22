"""Configured document evidence retains the HTTP session and CSRF boundary."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock

from fastapi.testclient import TestClient
from test_portfolio_local_copilot import request
from test_portfolio_local_copilot import seed as seed
from test_portfolio_local_copilot import store as store

from nexus.portfolio import insights
from nexus.portfolio.api import create_app
from nexus.portfolio.local_copilot_documents import load_corpus

ROOT = Path(__file__).resolve().parents[1]


def test_corpus_is_server_configured_and_inference_requires_session_and_csrf(store, monkeypatch):
    corpus = load_corpus(ROOT / "fixtures/local_copilot/g6-documents-v1/documents.json")
    app = create_app(store.path.parent, local_documents=corpus)
    app.state.portfolio_store = store
    app.state.local_model_provider = Mock()
    answer = Mock(return_value={"mode": "grounding-test"})
    monkeypatch.setattr(insights, "answer_local", answer)
    client = TestClient(app)
    body = request(store).model_dump()
    assert client.post("/api/copilot/local", json=body).status_code == 401
    token = client.post("/api/session", json={"persona": "planner"}).json()["csrf_token"]
    assert client.post("/api/copilot/local", json=body).status_code == 403
    answer.assert_not_called()
    headers = {"X-CSRF-Token": token}
    for field, value in (("as_of", "2027-03-15"), ("corpus", "/private/other.json")):
        assert (
            client.post(
                "/api/copilot/local", json={**body, field: value}, headers=headers
            ).status_code
            == 422
        )
    answer.assert_not_called()
    before = datetime.now(UTC).date()
    response = client.post("/api/copilot/local", json=body, headers=headers)
    after = datetime.now(UTC).date()
    assert response.status_code == 200
    assert answer.call_args.kwargs["corpus"] is corpus
    assert before <= answer.call_args.kwargs["as_of"] <= after
    assert answer.call_args.args[2]().role.value == "planner"


def test_default_application_has_no_document_corpus(store):
    app = create_app(store.path.parent)
    assert app.state.local_document_corpus is None
