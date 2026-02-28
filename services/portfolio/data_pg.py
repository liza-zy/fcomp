from __future__ import annotations
from sqlalchemy.orm import Session
from sqlalchemy import select, desc
from apps.orchestrator_api.db import get_db  # или как у тебя называется
from apps.orchestrator_api.models import User, QuizResult  # адаптируй импорты
from sqlalchemy.orm import Session
from src.models import Portfolio, PortfolioWeight

def get_user_risk_class(db: Session, telegram_id: int) -> str | None:
    user = db.execute(select(User).where(User.telegram_id == telegram_id)).scalar_one_or_none()
    if not user:
        return None

    qr = db.execute(
        select(QuizResult)
        .where(QuizResult.user_id == user.id)
        .order_by(desc(QuizResult.created_at))
        .limit(1)
    ).scalar_one_or_none()

    return qr.risk_class if qr else None

def save_portfolio(
    db: Session,
    telegram_id: int,
    risk_profile: str | None,
    method: str,
    lookback: int,
    params: dict | None,
    weights: list[dict],  # [{instrument_uid, weight, secid, boardid, asset_class}, ...]
) -> int:
    p = Portfolio(
        telegram_id=telegram_id,
        risk_profile=risk_profile,
        method=method,
        lookback=lookback,
        params_json=params,
    )

    p.weights = [
        PortfolioWeight(
            instrument_uid=w["instrument_uid"],
            weight=w["weight"],
            secid=w.get("secid"),
            boardid=w.get("boardid"),
            asset_class=w.get("asset_class"),
        )
        for w in weights
    ]

    db.add(p)
    db.commit()
    db.refresh(p)
    return p.id