from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from services.portfolio.engine import PortfolioEngine
from services.portfolio.schemas import BuildPortfolioRequest, BuildPortfolioResponse
from services.portfolio.data_pg import get_user_risk_class, save_portfolio_run
from apps.orchestrator_api.db import get_db

router = APIRouter(prefix="/portfolio", tags=["portfolio"])

engine = PortfolioEngine(
    duckdb_path="data_lake/moex.duckdb",
    risk_yaml_path="services/risk_quiz/domain/questions.yaml",
)

@router.post("/build", response_model=BuildPortfolioResponse)
def build_portfolio(req: BuildPortfolioRequest, db: Session = Depends(get_db)):
    # 1) risk profile
    risk_profile = req.risk_profile
    if not risk_profile:
        risk_profile = get_user_risk_class(db, req.telegram_id)
    if not risk_profile:
        raise HTTPException(status_code=400, detail="No risk profile: pass risk_profile or complete quiz")

    # 2) as_of
    as_of = engine.market.get_as_of_common()

    # 3) build
    resp = engine.build(req=req, risk_profile_key=risk_profile, as_of=as_of)
    if not resp.portfolios:
        raise HTTPException(status_code=400, detail="Portfolio build failed: not enough data after filtering")

    # 4) save each portfolio
    for p in resp.portfolios:
        save_portfolio_run(
            db=db,
            telegram_id=req.telegram_id,
            as_of=p.as_of,
            risk_profile=p.risk_profile,
            method=p.method,
            cov_method=req.cov_method,
            lookback=req.lookback,
            constraints=req.constraints.model_dump(),
            weights=p.weights,
            metrics=p.metrics,
        )

    return resp