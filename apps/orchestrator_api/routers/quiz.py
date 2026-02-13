# apps/orchestrator_api/routers/quiz.py

from typing import Any, Dict, Optional

from fastapi import BackgroundTasks
from fastapi import APIRouter
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import Any, Dict, Optional

from src.db import get_session
from src.db import SessionLocal
from src.models import User, QuizResult

from services.risk_quiz.runtime.service import score as risk_quiz_score

router = APIRouter()

def save_quiz_result_background(payload, result) -> None:
    db: Session = SessionLocal()
    try:
        user = db.query(User).filter(User.telegram_id == payload.telegram_id).one_or_none()
        if user:
            user.username = payload.username or user.username
            user.first_name = payload.first_name or user.first_name
            user.last_name = payload.last_name or user.last_name
        else:
            user = User(
                telegram_id=payload.telegram_id,
                username=payload.username,
                first_name=payload.first_name,
                last_name=payload.last_name,
            )
            db.add(user)
            db.flush()

        db.add(
            QuizResult(
                user_id=user.id,
                risk_class=result["risk_class"],
                confidence=float(result["confidence"]),
                neighbor_class=result.get("neighbor_class"),
                neighbor_confidence=(float(result["neighbor_confidence"]) if result.get("neighbor_confidence") is not None else None),
                # profile_text НЕ сохраняем
            )
        )
        db.commit()
    finally:
        db.close()

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
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    answers: Dict[str, Any]
    

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
async def score_quiz(payload: QuizScoreRequest,background_tasks: BackgroundTasks) -> QuizScoreResponse:
    answers = payload.answers or {}
    
    result = risk_quiz_score(answers)
    background_tasks.add_task(save_quiz_result_background, payload, result)
    return QuizScoreResponse(**result)