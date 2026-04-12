from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field

PortfolioMethod = Literal["max_sharpe", "equal_weight_top_assets"]
CovMethod = Literal["ledoit", "ewma"]


class PortfolioConstraints(BaseModel):
    sectors_include: list[str] = Field(default_factory=list)
    currencies_include: list[str] = Field(default_factory=list)
    exclude_secids: list[str] = Field(default_factory=list)
    max_weight: float = 0.15
    min_weight: float = 0.01
    max_assets: int = 10
    budget_rub: float = 100_000
    is_qualified_investor: bool = False


class BuildPortfolioRequest(BaseModel):
    telegram_id: int
    risk_profile: str | None = None
    cov_method: CovMethod = "ledoit"
    lookback: int = 252
    constraints: PortfolioConstraints = PortfolioConstraints()


class PortfolioResult(BaseModel):
    method: Literal["max_sharpe", "max_return"]
    as_of: str
    risk_profile: str
    weights: dict[str, float]
    metrics: dict[str, float]
    chart_png_b64: str | None = None


class BuildPortfolioResponse(BaseModel):
    portfolios: list[PortfolioResult]


class PreviewHolding(BaseModel):
    secid: str
    instrument_uid: Optional[str] = None
    boardid: Optional[str] = None
    weight: float


class PreviewChartPoint(BaseModel):
    dt: str
    value: float


class PreviewStats(BaseModel):
    max_drawdown_6m: float
    volatility_min_6m: float
    volatility_max_6m: float
    sharpe_6m: float


class PreviewPortfolioRequest(BaseModel):
    telegram_id: int
    portfolio_name: str = Field(..., min_length=1, max_length=100)
    risk_profile: str
    method: PortfolioMethod
    apply_ai: bool = False
    cov_method: CovMethod = "ledoit"
    lookback: int = 252
    constraints: PortfolioConstraints = PortfolioConstraints()


class PreviewPortfolioResponse(BaseModel):
    portfolio_name: str
    risk_profile: str
    method: PortfolioMethod
    apply_ai: bool
    lookback: int
    holdings: list[PreviewHolding]
    chart_6m: list[PreviewChartPoint]
    stats: PreviewStats


class ExistingPortfolioOption(BaseModel):
    id: int
    name: Optional[str] = None
    position: Optional[int] = None
    status: Optional[str] = None


class SavePreviewPortfolioRequest(BaseModel):
    telegram_id: int
    portfolio_name: str
    risk_profile: str
    method: PortfolioMethod
    apply_ai: bool
    lookback: int
    holdings: list[PreviewHolding]
    chart_6m: list[PreviewChartPoint]
    stats: PreviewStats
    budget_rub: float
    as_of_date: str
    is_qualified_investor: bool = False

class SavePreviewPortfolioResponse(BaseModel):
    ok: bool
    saved_portfolio_id: Optional[int] = None
    limit_exceeded: bool = False
    existing_portfolios: list[ExistingPortfolioOption] = Field(default_factory=list)


class ReplacePortfolioRequest(BaseModel):
    telegram_id: int
    delete_portfolio_id: int
    portfolio_name: str = Field(..., min_length=1, max_length=100)
    risk_profile: str
    method: PortfolioMethod
    apply_ai: bool = False
    lookback: int = 252
    holdings: list[PreviewHolding]
    chart_6m: list[PreviewChartPoint]
    stats: PreviewStats
    budget_rub: float
    as_of_date: str
    is_qualified_investor: bool = False


class ReplacePortfolioResponse(BaseModel):
    ok: bool
    saved_portfolio_id: int
    deleted_portfolio_id: int
