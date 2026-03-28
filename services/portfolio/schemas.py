from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Literal

PortfolioMethod = Literal["max_sharpe", "max_return"]
CovMethod = Literal["ledoit", "ewma"]

class PortfolioConstraints(BaseModel):
    sectors_include: list[str] = Field(default_factory=list)   # например ["Energy", "IT"]
    currencies_include: list[str] = Field(default_factory=list) # например ["RUB", "CNY"]
    exclude_secids: list[str] = Field(default_factory=list)     # ["GAZP", "SBER"]
    max_weight: float = 0.15
    max_assets: int = 10
    budget_rub: float = 100_000

class BuildPortfolioRequest(BaseModel):
    telegram_id: int
    risk_profile: str | None = None         # если None -> берём из БД
    cov_method: CovMethod = "ledoit"
    lookback: int = 252
    constraints: PortfolioConstraints = PortfolioConstraints()

class PortfolioResult(BaseModel):
    method: PortfolioMethod
    as_of: str
    risk_profile: str
    weights: dict[str, float]               # secid -> weight
    metrics: dict[str, float]               # exp_return, vol, sharpe
    chart_png_b64: str | None = None

class BuildPortfolioResponse(BaseModel):
    portfolios: list[PortfolioResult]
