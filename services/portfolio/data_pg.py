# services/portfolio/data_pg.py
from __future__ import annotations

from typing import Optional, Any
from sqlalchemy.orm import Session

from src.models import User, QuizResult, Portfolio, PortfolioWeight


def get_user_risk_class(db: Session, telegram_id: int) -> Optional[str]:
    user = db.query(User).filter(User.telegram_id == telegram_id).one_or_none()
    if not user:
        return None

    qr = (
        db.query(QuizResult)
        .filter(QuizResult.user_id == user.id)
        .order_by(QuizResult.created_at.desc())
        .first()
    )
    return qr.risk_class if qr else None


def save_portfolio_run(
    db: Session,
    *,
    telegram_id: int,
    user_id: Optional[int],
    as_of,
    risk_profile: str,
    method: str,
    cov_method: str,
    lookback: int,
    constraints: dict,
    weights: dict[str, float],
    metrics: dict[str, Any],
) -> int:
    p = Portfolio(
        telegram_id=telegram_id,
        user_id=user_id,
        risk_profile=risk_profile,
        method=method,
        lookback=lookback,
        params_json={
            "as_of": str(as_of),
            "cov_method": cov_method,
            "constraints": constraints,
            "metrics": metrics,
        },
    )

    for instrument_uid, w in weights.items():
        p.weights.append(PortfolioWeight(instrument_uid=instrument_uid,secid=instrument_uid, boardid=None, weight=float(w)))

    db.add(p)
    db.commit()
    db.refresh(p)
    return p.id
