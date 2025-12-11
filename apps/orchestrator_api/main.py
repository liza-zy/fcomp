# apps/orchestrator_api/main.py
from fastapi import FastAPI

from apps.orchestrator_api.routers.health import router as health_router
from apps.orchestrator_api.routers.quiz import router as quiz_router
from apps.orchestrator_api.routers.portfolio import router as portfolio_router

app = FastAPI(title="FinCompass Orchestrator API")

app.include_router(health_router, prefix="/health", tags=["health"])
app.include_router(quiz_router, prefix="/quiz", tags=["quiz"])
app.include_router(portfolio_router, prefix="/portfolio", tags=["portfolio"])