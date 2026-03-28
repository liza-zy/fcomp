from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol, Any


@dataclass(frozen=True)
class PortfolioBuildRequest:
    decision_date: date
    risk_profile_name: str
    method_name: str = "markowitz"


@dataclass(frozen=True)
class AssetRiskRequest:
    decision_date: date
    instrument_id: int
    secid: str


class PortfolioBuilderProtocol(Protocol):
    def build_portfolio(
        self,
        risk_profile_name: str,
        as_of_date: date,
        **kwargs: Any,
    ) -> dict: ...


class AssetRiskProfilerProtocol(Protocol):
    def classify_asset_risk(
        self,
        secid: str,
        as_of_date: date,
        **kwargs: Any,
    ) -> dict: ...


def build_portfolio_point_in_time(
    builder: PortfolioBuilderProtocol,
    req: PortfolioBuildRequest,
    **kwargs: Any,
) -> dict:
    return builder.build_portfolio(
        risk_profile_name=req.risk_profile_name,
        as_of_date=req.decision_date,
        **kwargs,
    )


def classify_asset_risk_point_in_time(
    profiler: AssetRiskProfilerProtocol,
    req: AssetRiskRequest,
    **kwargs: Any,
) -> dict:
    return profiler.classify_asset_risk(
        secid=req.secid,
        as_of_date=req.decision_date,
        **kwargs,
    )
