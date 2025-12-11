# apps/orchestrator_api/routers/health.py
from fastapi import APIRouter

router = APIRouter()

@router.get("/ping")
async def ping():
    return {"status": "ok"}