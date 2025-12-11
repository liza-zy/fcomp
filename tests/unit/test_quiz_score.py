from fastapi.testclient import TestClient

from apps.orchestrator_api.main import app

client = TestClient(app)


def test_quiz_score_returns_risk_class():
    """
    По умолчанию (пустой запрос) сервис должен вернуть валидный риск-профиль.
    Сейчас по логике скоринга: пустые ответы → минимальные баллы → Ultra-Conservative.
    """
    response = client.post("/quiz/score", json={})
    assert response.status_code == 200

    data = response.json()
    assert "risk_class" in data
    assert data["risk_class"] == "Ultra-Conservative"

    # Дополнительно проверим, что есть confidence и (опционально) соседний класс
    assert "confidence" in data
    assert 0.0 <= data["confidence"] <= 1.0
