from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pathlib import Path

from src.models import User

from services.portfolio.engine import PortfolioEngine
from services.portfolio.schemas import BuildPortfolioRequest, BuildPortfolioResponse
from services.portfolio.data_pg import get_user_risk_class, save_portfolio_run
from src.db import get_session

router = APIRouter(prefix="/portfolio", tags=["portfolio"])

APP_ROOT = Path(__file__).resolve().parents[3]  # .../app
engine = PortfolioEngine(
    duckdb_path=str(APP_ROOT / "data_lake" / "moex.duckdb"),
    risk_yaml_path=str(APP_ROOT / "services" / "risk_quiz" / "domain" / "questions.yaml"),
)

@router.post("/build", response_model=BuildPortfolioResponse)
def build_portfolio(req: BuildPortfolioRequest, db: Session = Depends(get_session)):
    risk_profile = req.risk_profile
    if not risk_profile:
        risk_profile = get_user_risk_class(db, req.telegram_id)
    if not risk_profile:
        raise HTTPException(status_code=400, detail="No risk profile: pass risk_profile or complete quiz")

    user = db.query(User).filter(User.telegram_id == req.telegram_id).one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

#    if user.portfolio_count >= user.portfolio_limit:
#        raise HTTPException(status_code=400, detail="Portfolio limit reached")

    user_id = user.id
    as_of = engine.market.get_as_of_common()

    resp = engine.build(req=req, risk_profile_key=risk_profile, as_of=as_of)
    if not resp.portfolios:
        raise HTTPException(status_code=400, detail="Portfolio build failed: not enough data after filtering")

    universe = engine.market.load_universe_for_risk_profile(as_of, engine.risk_profiles[risk_profile].index)
    secid_to_uid = dict(zip(universe["secid"], universe["instrument_uid"]))

    for p in resp.portfolios:
        weight_rows = []
        for secid, weight in p.weights.items():
            instrument_uid = secid_to_uid.get(secid)
            if not instrument_uid:
                continue
            weight_rows.append(
                {
                    "instrument_uid": instrument_uid,
                    "secid": secid,
                    "boardid": None,
                    "weight": weight,
                }
            )

#        save_portfolio_run(
#           db=db,
#            user_id=user_id,
#            telegram_id=req.telegram_id,
#            as_of=p.as_of,
#            risk_profile=p.risk_profile,
#            method=p.method,
#            cov_method=req.cov_method,
#            lookback=req.lookback,
#            constraints=req.constraints.model_dump(),
#            weights=weight_rows,
#            metrics=p.metrics,
#        )
#
#    user.portfolio_count += 1
    db.commit()

    return resp
