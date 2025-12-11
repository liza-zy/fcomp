# tests/e2e/test_api_flow.py

import pytest
import httpx
from httpx import ASGITransport

from apps.orchestrator_api.main import app


@pytest.mark.asyncio
async def test_quiz_and_portfolio_flow():
    transport = ASGITransport(app=app)

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:

        # 1) Квиз → получаем risk_class
        resp_quiz = await client.post("/quiz/score", json={"answers": {"risk_attitude": "Средний риск"}})
        assert resp_quiz.status_code == 200

        data_quiz = resp_quiz.json()
        assert "risk_class" in data_quiz
        risk_class = data_quiz["risk_class"]

        # 2) Собираем портфель
        payload = {
            "extra": {
                "risk_class": risk_class,
                "amount": 100000,
                "source": "test_e2e",
            }
        }

        resp_portfolio = await client.post("/portfolio/build", json=payload)
        assert resp_portfolio.status_code == 200

        data_portfolio = resp_portfolio.json()
        assert "allocations" in data_portfolio
        assert isinstance(data_portfolio["allocations"], list)
        assert len(data_portfolio["allocations"]) > 0