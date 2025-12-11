# apps/orchestrator_api/routers/quiz.py

from typing import Any, Dict, Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from services.risk_quiz.runtime.service import score as risk_quiz_score

router = APIRouter()


class QuizScoreRequest(BaseModel):
    """
    answers — словарь вида:
    {
      "horizon": "horizon_2",
      "income_stability": "income_3",
      ...
    }
    Пустой словарь {} тоже допустим — тогда используем консервативные значения.
    """
    answers: Optional[Dict[str, Any]] = None
    

class QuizScoreResponse(BaseModel):
    risk_class: str = Field(..., description="Основной риск-профиль")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Уверенность в основном профиле")
    neighbor_class: Optional[str] = Field(
        None, description="Соседний профиль (более консервативный или агрессивный)"
    )
    neighbor_confidence: Optional[float] = Field(
        None, description="Уверенность в соседнем профиле"
    )
    score: float = Field(..., description="Суммарный числовой скор по ответам")
    profile_text: Optional[str] = Field(
        None, description="Человеко-понятное описание риск-профиля"
    )


@router.post("/score", response_model=QuizScoreResponse)
async def score_quiz(payload: QuizScoreRequest) -> QuizScoreResponse:
    answers = payload.answers or {}
    result = risk_quiz_score(answers)
    return QuizScoreResponse(**result)