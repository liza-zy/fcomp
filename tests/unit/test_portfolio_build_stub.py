# tests/unit/test_portfolio_build_stub.py

from fastapi.testclient import TestClient
from apps.orchestrator_api.main import app

client = TestClient(app)


def test_portfolio_build_stub_returns_allocations_list():
    # Пока можно слать пустой json, так как это заглушка
    response = client.post("/portfolio/build", json={})
    assert response.status_code == 200

    data = response.json()
    assert "allocations" in data
    assert isinstance(data["allocations"], list)
    assert len(data["allocations"]) > 0

    first = data["allocations"][0]
    assert "ticker" in first
    assert "weight" in first
    assert isinstance(first["ticker"], str)
    assert isinstance(first["weight"], (int, float))