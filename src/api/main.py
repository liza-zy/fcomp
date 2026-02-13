from fastapi import FastAPI, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db import get_session

from apps.orchestrator_api.routers.quiz import router as quiz_router
from src.db import create_tables

app = FastAPI()

@app.get("/health/db")
def health_db(db: Session = Depends(get_session)):
    db.execute(text("SELECT 1"))
    return {"status": "ok"}

app.include_router(quiz_router, prefix="/quiz", tags=["quiz"])

@app.on_event("startup")
def _startup():
    create_tables()