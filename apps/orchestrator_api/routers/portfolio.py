from __future__ import annotations

from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Optional

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from services.portfolio.data_pg import get_user_risk_class
from services.portfolio.engine import PortfolioEngine
from services.portfolio.schemas import (
    BuildPortfolioRequest,
    BuildPortfolioResponse,
    ExistingPortfolioOption,
    PortfolioMethod,
    PortfolioResult,
    PreviewChartPoint,
    PreviewHolding,
    PreviewPortfolioRequest,
    PreviewPortfolioResponse,
    PreviewStats,
    ReplacePortfolioRequest,
    ReplacePortfolioResponse,
    SavePreviewPortfolioRequest,
    SavePreviewPortfolioResponse,
)
from src.db import get_session
from src.models import Portfolio, PortfolioWeight, User

router = APIRouter(prefix="/portfolio", tags=["portfolio"])

APP_ROOT = Path(__file__).resolve().parents[3]
DUCKDB_PATH = str(APP_ROOT / "data_lake" / "moex.duckdb")

engine = PortfolioEngine(
    duckdb_path=DUCKDB_PATH,
    risk_yaml_path=str(APP_ROOT / "services" / "risk_quiz" / "domain" / "questions.yaml"),
)


class PortfolioListItemResponse(BaseModel):
    id: int
    name: Optional[str] = None
    risk_profile: Optional[str] = None
    method: str
    lookback: int
    position: Optional[int] = None
    status: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None


class PortfolioLimitsResponse(BaseModel):
    portfolio_limit: int
    portfolio_count: int


class PortfolioListResponse(BaseModel):
    portfolios: list[PortfolioListItemResponse]
    limits: PortfolioLimitsResponse


class PortfolioWeightResponse(BaseModel):
    portfolio_id: int
    instrument_uid: str
    secid: Optional[str] = None
    boardid: Optional[str] = None
    weight: float


class PortfolioDetailsResponse(BaseModel):
    portfolio: PortfolioListItemResponse
    weights: list[PortfolioWeightResponse]


class RenamePortfolioRequest(BaseModel):
    telegram_id: int = Field(..., description="Telegram user id")
    name: str = Field(..., min_length=1, max_length=100)


class DeletePortfolioResponse(BaseModel):
    ok: bool


def _get_user_by_telegram_id(db: Session, telegram_id: int) -> User:
    user = db.query(User).filter(User.telegram_id == telegram_id).one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


def _get_owned_portfolio(db: Session, portfolio_id: int, telegram_id: int) -> tuple[User, Portfolio]:
    user = _get_user_by_telegram_id(db, telegram_id)
    portfolio = (
        db.query(Portfolio)
        .filter(Portfolio.id == portfolio_id, Portfolio.user_id == user.id)
        .one_or_none()
    )
    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")
    return user, portfolio


def _serialize_portfolio(portfolio: Portfolio) -> PortfolioListItemResponse:
    return PortfolioListItemResponse(
        id=portfolio.id,
        name=portfolio.name,
        risk_profile=portfolio.risk_profile,
        method=portfolio.method,
        lookback=portfolio.lookback,
        position=portfolio.position,
        status=portfolio.status,
        created_at=portfolio.created_at,
        updated_at=portfolio.updated_at,
    )


def _serialize_weight(weight: PortfolioWeight) -> PortfolioWeightResponse:
    return PortfolioWeightResponse(
        portfolio_id=weight.portfolio_id,
        instrument_uid=weight.instrument_uid,
        secid=weight.secid,
        boardid=weight.boardid,
        weight=weight.weight,
    )


def _sync_portfolio_count(db: Session, user: User) -> None:
    actual_count = db.query(Portfolio).filter(Portfolio.user_id == user.id).count()
    user.portfolio_count = actual_count
    db.add(user)
    db.flush()


def _normalize_method(method: PortfolioMethod) -> str:
    if method == "equal_weight_top_assets":
        return "equal_weight_top_assets"
    return "max_sharpe"


def _get_next_position(db: Session, user_id: int) -> int:
    existing_max_position = (
        db.query(Portfolio.position)
        .filter(Portfolio.user_id == user_id)
        .order_by(Portfolio.position.desc().nulls_last())
        .first()
    )
    return (
        (existing_max_position[0] if existing_max_position and existing_max_position[0] is not None else 0)
        + 1
    )


def _get_existing_portfolio_options(db: Session, user_id: int) -> list[ExistingPortfolioOption]:
    portfolios = (
        db.query(Portfolio)
        .filter(Portfolio.user_id == user_id)
        .order_by(Portfolio.position.asc().nulls_last(), Portfolio.id.asc())
        .all()
    )
    return [
        ExistingPortfolioOption(
            id=p.id,
            name=p.name,
            position=p.position,
            status=p.status,
        )
        for p in portfolios
    ]


def _load_chart_and_stats_from_duckdb(secid_weights: dict[str, float]) -> tuple[list[PreviewChartPoint], PreviewStats]:
    if not secid_weights:
        return (
            [],
            PreviewStats(
                max_drawdown_6m=0.0,
                volatility_min_6m=0.0,
                volatility_max_6m=0.0,
                sharpe_6m=0.0,
            ),
        )

    con = duckdb.connect(DUCKDB_PATH)

    secids = list(secid_weights.keys())
    as_of_row = con.execute("SELECT MAX(dt) FROM bars_1d").fetchone()
    as_of = as_of_row[0]
    if as_of is None:
        return (
            [],
            PreviewStats(
                max_drawdown_6m=0.0,
                volatility_min_6m=0.0,
                volatility_max_6m=0.0,
                sharpe_6m=0.0,
            ),
        )

    from_dt = as_of - timedelta(days=183)

    placeholders = ",".join(["?"] * len(secids))
    query = f"""
        SELECT dt, secid, close
        FROM bars_1d
        WHERE dt >= ?
          AND secid IN ({placeholders})
          AND boardid IS NOT NULL
        ORDER BY dt, secid
    """
    rows = con.execute(query, [from_dt, *secids]).fetchall()

    if not rows:
        return (
            [],
            PreviewStats(
                max_drawdown_6m=0.0,
                volatility_min_6m=0.0,
                volatility_max_6m=0.0,
                sharpe_6m=0.0,
            ),
        )

    # dt -> {secid: close}
    by_dt: dict = {}
    first_close: dict[str, float] = {}

    for dt, secid, close in rows:
        if close is None:
            continue
        if secid not in first_close:
            first_close[secid] = float(close)
        by_dt.setdefault(dt, {})[secid] = float(close)

    chart_points: list[PreviewChartPoint] = []
    portfolio_values: list[float] = []

    for dt in sorted(by_dt.keys()):
        bucket = by_dt[dt]
        value = 0.0

        for secid, weight in secid_weights.items():
            base = first_close.get(secid)
            current = bucket.get(secid)
            if not base or current is None:
                continue
            rel = current / base
            value += weight * rel

        value = round(value * 100, 4)
        portfolio_values.append(value)
        chart_points.append(
            PreviewChartPoint(
                dt=str(dt),
                value=value,
            )
        )

    if not portfolio_values:
        return (
            [],
            PreviewStats(
                max_drawdown_6m=0.0,
                volatility_min_6m=0.0,
                volatility_max_6m=0.0,
                sharpe_6m=0.0,
            ),
        )

    running_max = portfolio_values[0]
    max_drawdown = 0.0

    for v in portfolio_values:
        running_max = max(running_max, v)
        dd = 0.0 if running_max == 0 else (running_max - v) / running_max
        max_drawdown = max(max_drawdown, dd)

    returns = []
    for i in range(1, len(portfolio_values)):
        prev = portfolio_values[i - 1]
        curr = portfolio_values[i]
        if prev != 0:
            returns.append((curr - prev) / prev)

    if returns:
        mean_ret = sum(returns) / len(returns)
        variance = sum((r - mean_ret) ** 2 for r in returns) / len(returns)
        daily_vol = variance ** 0.5
        annualized_vol = daily_vol * (252 ** 0.5)
        sharpe = 0.0 if daily_vol == 0 else (mean_ret / daily_vol) * (252 ** 0.5)
    else:
        annualized_vol = 0.0
        sharpe = 0.0

    stats = PreviewStats(
        max_drawdown_6m=round(max_drawdown * 100, 2),
        volatility_min_6m=round(max(0.0, annualized_vol * 100 * 0.85), 2),
        volatility_max_6m=round(annualized_vol * 100, 2),
        sharpe_6m=round(sharpe, 2),
    )

    return chart_points, stats


def _build_preview_holdings_from_result(result: PortfolioResult) -> list[PreviewHolding]:
    universe = engine.market.load_universe_for_risk_profile(
        result.as_of,
        engine.risk_profiles[result.risk_profile].index,
    )
    secid_to_uid = dict(zip(universe["secid"], universe["instrument_uid"]))

    holdings: list[PreviewHolding] = []
    for secid, weight in result.weights.items():
        holdings.append(
            PreviewHolding(
                secid=secid,
                instrument_uid=secid_to_uid.get(secid),
                boardid=None,
                weight=weight,
            )
        )

    holdings.sort(key=lambda x: x.weight, reverse=True)
    return holdings


def _materialize_holdings_to_positions(
    holdings: list[PreviewHolding],
    budget_rub: float,
    as_of_date,
) -> tuple[list[dict], float, float]:
    if not holdings or budget_rub <= 0:
        return [], 0.0, float(budget_rub)

    instrument_uids = [h.instrument_uid for h in holdings if h.instrument_uid]
    if not instrument_uids:
        return [], 0.0, float(budget_rub)

    prices_df = engine.market.load_latest_prices_and_lots(instrument_uids, as_of_date)
    if prices_df.empty:
        return [], 0.0, float(budget_rub)

    by_uid = {
        row["instrument_uid"]: row
        for _, row in prices_df.iterrows()
    }

    positions: list[dict] = []
    invested_rub = 0.0

    for h in holdings:
        instrument_uid = h.instrument_uid
        if not instrument_uid:
            continue

        row = by_uid.get(instrument_uid)
        if row is None:
            continue

        price = float(row["price"]) if row["price"] is not None else None
        lot = int(row["lot"]) if row["lot"] is not None else 1

        if price is None or price <= 0:
            continue
        if lot <= 0:
            lot = 1

        target_value_rub = float(h.weight) * float(budget_rub)
        lot_cost = price * lot

        quantity_lots = int(target_value_rub // lot_cost)
        quantity_units = quantity_lots * lot
        position_value_rub = float(quantity_units * price)

        actual_weight = 0.0 if budget_rub <= 0 else position_value_rub / float(budget_rub)

        positions.append(
            {
                "instrument_uid": instrument_uid,
                "secid": h.secid,
                "boardid": h.boardid,
                "target_weight": float(h.weight),
                "price": price,
                "lot": lot,
                "quantity_lots": quantity_lots,
                "quantity_units": quantity_units,
                "position_value_rub": position_value_rub,
                "actual_weight": actual_weight,
            }
        )

        invested_rub += position_value_rub

    cash_rub = float(budget_rub) - invested_rub
    if cash_rub < 0:
        cash_rub = 0.0

    return positions, invested_rub, cash_rub

def _find_unbuyable_secids(
    holdings: list[PreviewHolding],
    budget_rub: float,
    as_of_date,
) -> list[str]:
    positions, _, _ = _materialize_holdings_to_positions(
        holdings=holdings,
        budget_rub=budget_rub,
        as_of_date=as_of_date,
    )

    positive_secids = {
        p["secid"]
        for p in positions
        if p["quantity_lots"] > 0 and p["position_value_rub"] > 0
    }

    unbuyable = []
    for h in holdings:
        if h.secid and h.secid not in positive_secids:
            unbuyable.append(h.secid)

    return sorted(set(unbuyable))

def _build_feasible_preview(req: PreviewPortfolioRequest) -> PreviewPortfolioResponse:
    max_attempts = 5
    extra_excludes: list[str] = []

    for _ in range(max_attempts):
        constraints_data = req.constraints.model_dump()
        constraints_data["exclude_secids"] = list(
            set(constraints_data.get("exclude_secids", [])) | set(extra_excludes)
        )

        adjusted_req = PreviewPortfolioRequest(
            telegram_id=req.telegram_id,
            portfolio_name=req.portfolio_name,
            risk_profile=req.risk_profile,
            method=req.method,
            apply_ai=req.apply_ai,
            cov_method=req.cov_method,
            lookback=req.lookback,
            constraints=type(req.constraints)(**constraints_data),
        )

        as_of = engine.market.get_as_of_common()
        resp = engine.build(
            req=adjusted_req,
            risk_profile_key=adjusted_req.risk_profile,
            as_of=as_of,
        )

        if not resp.portfolios:
            raise HTTPException(status_code=400, detail="Portfolio preview build failed")

        selected = next((p for p in resp.portfolios if p.method == adjusted_req.method), None)
        if selected is None:
            selected = resp.portfolios[0]

        holdings = _build_preview_holdings_from_result(selected)
        unbuyable = _find_unbuyable_secids(
            holdings=holdings,
            budget_rub=adjusted_req.constraints.budget_rub,
            as_of_date=as_of,
        )

        if not unbuyable:
            chart_6m, stats = _load_chart_and_stats_from_duckdb(
                {h.secid: h.weight for h in holdings}
            )
            return PreviewPortfolioResponse(
                portfolio_name=adjusted_req.portfolio_name,
                risk_profile=selected.risk_profile,
                method=selected.method,
                apply_ai=adjusted_req.apply_ai,
                lookback=adjusted_req.lookback,
                holdings=holdings,
                chart_6m=chart_6m,
                stats=stats,
            )

        extra_excludes = list(set(extra_excludes) | set(unbuyable))

    raise HTTPException(
        status_code=400,
        detail="Could not build a feasible portfolio for the selected budget",
    )


def _save_portfolio_from_preview(
    db: Session,
    user: User,
    portfolio_name: str,
    risk_profile: str,
    method: str,
    lookback: int,
    apply_ai: bool,
    holdings: list[PreviewHolding],
    stats: PreviewStats,
    budget_rub: float,
    as_of_date,
    is_qualified_investor: bool,
) -> int:
    if isinstance(as_of_date, str):
        as_of_date = date.fromisoformat(as_of_date)

    next_position = _get_next_position(db, user.id)
    now = datetime.now(timezone.utc)

    positions, invested_rub, cash_rub = _materialize_holdings_to_positions(
        holdings=holdings,
        budget_rub=budget_rub,
        as_of_date=as_of_date,
    )

    positions = [
        p for p in positions
        if p["quantity_lots"] > 0 and p["position_value_rub"] > 0
    ]

    portfolio = Portfolio(
        user_id=user.id,
        telegram_id=user.telegram_id,
        risk_profile=risk_profile,
        method=method,
        lookback=lookback,
        budget_rub=budget_rub,
        as_of_date=datetime.combine(as_of_date, datetime.min.time(), tzinfo=timezone.utc),
        is_qualified_investor=is_qualified_investor,
        params_json={
            "apply_ai": apply_ai,
            "preview_stats": stats.model_dump(),
            "invested_rub": invested_rub,
            "cash_rub": cash_rub,
        },
        name=portfolio_name,
        position=next_position,
        status="active",
        created_at=now,
        updated_at=now,
    )
    db.add(portfolio)
    db.flush()

    for p in positions:
        db.add(
            PortfolioWeight(
                portfolio_id=portfolio.id,
                instrument_uid=p["instrument_uid"],
                secid=p["secid"],
                boardid=p["boardid"],
                weight=p["actual_weight"],
                price=p["price"],
                lot=p["lot"],
                quantity_lots=p["quantity_lots"],
                quantity_units=p["quantity_units"],
                position_value_rub=p["position_value_rub"],
            )
        )

    _sync_portfolio_count(db, user)
    db.commit()
    db.refresh(portfolio)
    return portfolio.id


@router.get("", response_model=PortfolioListResponse)
def list_portfolios(
    telegram_id: int = Query(...),
    db: Session = Depends(get_session),
) -> PortfolioListResponse:
    user = _get_user_by_telegram_id(db, telegram_id)
    _sync_portfolio_count(db, user)

    portfolios = (
        db.query(Portfolio)
        .filter(Portfolio.user_id == user.id)
        .order_by(Portfolio.position.asc().nulls_last(), Portfolio.id.asc())
        .all()
    )

    db.commit()
    db.refresh(user)

    return PortfolioListResponse(
        portfolios=[_serialize_portfolio(p) for p in portfolios],
        limits=PortfolioLimitsResponse(
            portfolio_limit=user.portfolio_limit,
            portfolio_count=user.portfolio_count,
        ),
    )


@router.get("/{portfolio_id}", response_model=PortfolioDetailsResponse)
def get_portfolio_details(
    portfolio_id: int,
    telegram_id: int = Query(...),
    db: Session = Depends(get_session),
) -> PortfolioDetailsResponse:
    _, portfolio = _get_owned_portfolio(db, portfolio_id, telegram_id)

    weights = (
        db.query(PortfolioWeight)
        .filter(PortfolioWeight.portfolio_id == portfolio.id)
        .order_by(PortfolioWeight.weight.desc(), PortfolioWeight.secid.asc().nulls_last())
        .all()
    )

    return PortfolioDetailsResponse(
        portfolio=_serialize_portfolio(portfolio),
        weights=[_serialize_weight(w) for w in weights],
    )


@router.patch("/{portfolio_id}", response_model=PortfolioListItemResponse)
def rename_portfolio(
    portfolio_id: int,
    payload: RenamePortfolioRequest,
    db: Session = Depends(get_session),
) -> PortfolioListItemResponse:
    _, portfolio = _get_owned_portfolio(db, portfolio_id, payload.telegram_id)

    next_name = payload.name.strip()
    if not next_name:
        raise HTTPException(status_code=400, detail="Portfolio name must not be empty")

    portfolio.name = next_name
    portfolio.updated_at = datetime.now(timezone.utc)

    db.add(portfolio)
    db.commit()
    db.refresh(portfolio)

    return _serialize_portfolio(portfolio)


@router.delete("/{portfolio_id}", response_model=DeletePortfolioResponse)
def delete_portfolio(
    portfolio_id: int,
    telegram_id: int = Query(...),
    db: Session = Depends(get_session),
) -> DeletePortfolioResponse:
    user, portfolio = _get_owned_portfolio(db, portfolio_id, telegram_id)

    db.delete(portfolio)
    db.flush()

    _sync_portfolio_count(db, user)
    db.commit()

    return DeletePortfolioResponse(ok=True)


@router.post("/preview", response_model=PreviewPortfolioResponse)
def preview_portfolio(
    req: PreviewPortfolioRequest,
    db: Session = Depends(get_session),
) -> PreviewPortfolioResponse:
    risk_profile = req.risk_profile or get_user_risk_class(db, req.telegram_id)
    if not risk_profile:
        raise HTTPException(
            status_code=400,
            detail="No risk profile: pass risk_profile or complete quiz",
        )

    adjusted_req = PreviewPortfolioRequest(
        telegram_id=req.telegram_id,
        portfolio_name=req.portfolio_name,
        risk_profile=risk_profile,
        method=req.method,
        apply_ai=req.apply_ai,
        cov_method=req.cov_method,
        lookback=req.lookback,
        constraints=req.constraints,
    )

    return _build_feasible_preview(adjusted_req)

@router.post("/save", response_model=SavePreviewPortfolioResponse)
def save_preview_portfolio(
    req: SavePreviewPortfolioRequest,
    db: Session = Depends(get_session),
) -> SavePreviewPortfolioResponse:
    user = _get_user_by_telegram_id(db, req.telegram_id)
    _sync_portfolio_count(db, user)

    if user.portfolio_count >= user.portfolio_limit:
        existing = _get_existing_portfolio_options(db, user.id)
        db.commit()
        return SavePreviewPortfolioResponse(
            ok=False,
            saved_portfolio_id=None,
            limit_exceeded=True,
            existing_portfolios=existing,
        )

    saved_id = _save_portfolio_from_preview(
        db=db,
        user=user,
        portfolio_name=req.portfolio_name,
        risk_profile=req.risk_profile,
        method=_normalize_method(req.method),
        lookback=req.lookback,
        apply_ai=req.apply_ai,
        holdings=req.holdings,
        stats=req.stats,
        budget_rub=req.budget_rub,
        as_of_date=req.as_of_date,
        is_qualified_investor=req.is_qualified_investor,
    )
    return SavePreviewPortfolioResponse(
        ok=True,
        saved_portfolio_id=saved_id,
        limit_exceeded=False,
        existing_portfolios=[],
    )


@router.post("/replace", response_model=ReplacePortfolioResponse)
def replace_portfolio(
    req: ReplacePortfolioRequest,
    db: Session = Depends(get_session),
) -> ReplacePortfolioResponse:
    user = _get_user_by_telegram_id(db, req.telegram_id)

    portfolio_to_delete = (
        db.query(Portfolio)
        .filter(Portfolio.id == req.delete_portfolio_id, Portfolio.user_id == user.id)
        .one_or_none()
    )
    if not portfolio_to_delete:
        raise HTTPException(status_code=404, detail="Portfolio to delete not found")

    deleted_id = portfolio_to_delete.id
    db.delete(portfolio_to_delete)
    db.flush()

    saved_id = _save_portfolio_from_preview(
        db=db,
        user=user,
        portfolio_name=req.portfolio_name,
        risk_profile=req.risk_profile,
        method=_normalize_method(req.method),
        lookback=req.lookback,
        apply_ai=req.apply_ai,
        holdings=req.holdings,
        stats=req.stats,
        budget_rub=req.budget_rub,
        as_of_date=req.as_of_date,
        is_qualified_investor=req.is_qualified_investor,
    )

    return ReplacePortfolioResponse(
        ok=True,
        saved_portfolio_id=saved_id,
        deleted_portfolio_id=deleted_id,
    )


@router.post("/build", response_model=BuildPortfolioResponse)
def build_portfolio(
    req: BuildPortfolioRequest,
    db: Session = Depends(get_session),
) -> BuildPortfolioResponse:
    risk_profile = req.risk_profile
    if not risk_profile:
        risk_profile = get_user_risk_class(db, req.telegram_id)
    if not risk_profile:
        raise HTTPException(
            status_code=400,
            detail="No risk profile: pass risk_profile or complete quiz",
        )

    user = db.query(User).filter(User.telegram_id == req.telegram_id).one_or_none()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    as_of = engine.market.get_as_of_common()

    resp = engine.build(req=req, risk_profile_key=risk_profile, as_of=as_of)
    if not resp.portfolios:
        raise HTTPException(
            status_code=400,
            detail="Portfolio build failed: not enough data after filtering",
        )

    universe = engine.market.load_universe_for_risk_profile(
        as_of,
        engine.risk_profiles[risk_profile].index,
    )
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

        _ = weight_rows
        _ = p
        _ = user

    db.commit()
    return resp
