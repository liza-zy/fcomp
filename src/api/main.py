
from fastapi import FastAPI, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db import get_session

from apps.orchestrator_api.routers.market import router as market_router
from apps.orchestrator_api.routers.me import router as me_router
from apps.orchestrator_api.routers.quiz import router as quiz_router
from apps.orchestrator_api.routers.portfolio import router as portfolio_router
from src.db import create_tables
from apps.orchestrator_api.routers.profile import router as profile_router

app = FastAPI()

@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok"}

@app.get("/health/db")
def health_db(db: Session = Depends(get_session)):
    db.execute(text("SELECT 1"))
    return {"status": "ok"}

app.include_router(me_router, prefix="/me", tags=["me"])
app.include_router(quiz_router, prefix="/quiz", tags=["quiz"])
app.include_router(portfolio_router) 
app.include_router(profile_router, prefix="/api")
app.include_router(market_router)

@app.on_event("startup")
def _startup():
    create_tables()
