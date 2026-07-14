"""Tests for the application health endpoint."""

from http import HTTPStatus

from fastapi.testclient import TestClient

from streamlab.main import create_app


def test_health_returns_healthy_status(tmp_path) -> None:
    """The health endpoint reports that the application is healthy."""
    with TestClient(create_app(tmp_path / "health.duckdb")) as client:
        response = client.get("/health")

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {"status": "healthy"}
