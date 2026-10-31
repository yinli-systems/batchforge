"""Regression tests for the correctness review fixes that need no database."""
import itertools

import pytest
from fastapi.testclient import TestClient

from backend.app import create_app
from backend.config import Settings
from backend.store import terminal_status


def _client():
    # No lifespan: these requests are refused before any database access.
    return TestClient(create_app(Settings(database_url="postgresql://unused@127.0.0.1:1/unused")))


@pytest.mark.parametrize("path", ["/v1/files", "/v1/batches", "/v1/batches/batch_x/retry"])
def test_anonymous_mutation_is_refused_without_reading_its_body(path):
    oversized = b"x" * (2 * 1024 * 1024)
    response = _client().post(path, content=oversized, headers={"Content-Type": "application/json"})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "invalid_api_key"


def test_anonymous_worker_route_reports_worker_credential_error():
    response = _client().post("/internal/tasks/claim", content=b"{}", headers={"Content-Type": "application/json"})
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "invalid_worker_token"


def test_malformed_authorization_is_refused_before_body_is_buffered():
    response = _client().post("/v1/batches", content=b"x" * 300_000,
                              headers={"Authorization": "Basic abc", "Content-Type": "application/json"})
    assert response.status_code == 401


@pytest.mark.parametrize("completed,failed,cancelled,expired", list(itertools.product([0, 1], [0, 1], [0, 1], [False, True])))
def test_terminal_status_follows_the_sql_rule(completed, failed, cancelled, expired):
    # Same truth table as batchforge_finish_batch_v4 (migration 0004).
    sql_rule = "failed" if (cancelled > 0 and expired) or (completed == 0 and failed > 0) else "completed"
    assert terminal_status(completed, failed, cancelled, expired) == sql_rule


def test_expired_batch_with_cancellations_is_failed_even_with_completions():
    assert terminal_status(completed=3, failed=0, cancelled=1, expired=True) == "failed"
    assert terminal_status(completed=3, failed=0, cancelled=1, expired=False) == "completed"
