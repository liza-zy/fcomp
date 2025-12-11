# tests/unit/test_health.py

from fastapi.testclient import TestClient
from apps.orchestrator_api.main import app

client = TestClient(app)


def test_health_ping():
    response = client.get("/health/ping")
    assert response.status_code == 200

    data = response.json()
    assert data == {"status": "ok"}