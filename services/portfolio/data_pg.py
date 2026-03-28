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
    weights: list[dict[str, Any]],
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

    for row in weights:
        p.weights.append(
            PortfolioWeight(
                instrument_uid=row["instrument_uid"],
                secid=row.get("secid"),
                boardid=row.get("boardid"),
                weight=float(row["weight"]),
            )
        )

    db.add(p)
    db.commit()
    db.refresh(p)
    return p.id
