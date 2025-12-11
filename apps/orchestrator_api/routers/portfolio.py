# apps/orchestrator_api/routers/portfolio.py
from typing import List, Optional, Dict, Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

router = APIRouter()


class PortfolioBuildRequest(BaseModel):
    """
    Временный запрос. Можно будет расширить
    под реальный формат (список тикеров, капитал и т.п.).
    """
    tickers: Optional[List[str]] = None
    extra: Optional[Dict[str, Any]] = None


class Allocation(BaseModel):
    ticker: str = Field(..., description="Идентификатор актива")
    weight: float = Field(..., ge=0.0, le=1.0, description="Доля в портфеле")


class PortfolioBuildResponse(BaseModel):
    allocations: List[Allocation]


@router.post("/build", response_model=PortfolioBuildResponse)
async def build_portfolio_stub(payload: PortfolioBuildRequest) -> PortfolioBuildResponse:
    """
    Мок: возвращаем фиксированный список аллокаций.
    Потом здесь будет вызов реального ML/оптимизатора.
    """
    allocations = [
        Allocation(ticker="TST1", weight=0.5),
        Allocation(ticker="TST2", weight=0.3),
        Allocation(ticker="TST3", weight=0.2),
    ]
    return PortfolioBuildResponse(allocations=allocations)