from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.db import get_session
from src.models import User, QuizResult  # проверь пути под твой проект

router = APIRouter(prefix="/profile", tags=["profile"])

DEFAULT_PROFILE = "Balanced"


class RiskProfileResponse(BaseModel):
    telegram_id: int
    risk_profile: str
    source: str  # "quiz" | "default"


@router.get("/risk", response_model=RiskProfileResponse)
def get_risk_profile(telegram_id: int, db: Session = Depends(get_session)):
    # ищем юзера
    user = db.query(User).filter(User.telegram_id == telegram_id).one_or_none()
    if not user:
        return RiskProfileResponse(telegram_id=telegram_id, risk_profile=DEFAULT_PROFILE, source="default")

    # ищем последний результат квиза
    q = (
        db.query(QuizResult)
        .filter(QuizResult.user_id == user.id)
        .order_by(QuizResult.created_at.desc())
        .limit(1)
        .one_or_none()
    )
    if not q or not q.risk_class:
        return RiskProfileResponse(telegram_id=telegram_id, risk_profile=DEFAULT_PROFILE, source="default")

    return RiskProfileResponse(telegram_id=telegram_id, risk_profile=q.risk_class, source="quiz")
