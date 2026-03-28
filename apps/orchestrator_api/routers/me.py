from __future__ import annotations

import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import parse_qsl

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from src.db import get_session
from src.models import User

router = APIRouter()


class BootstrapRequest(BaseModel):
    init_data: str = Field(..., description="Telegram WebApp initData string")


class BootstrapUserResponse(BaseModel):
    id: int
    telegram_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    plan: str
    plan_expires_at: Optional[datetime] = None
    portfolio_limit: int
    portfolio_count: int

class BootstrapResponse(BaseModel):
    is_new_user: bool
    user: BootstrapUserResponse


def _validate_telegram_init_data(init_data: str) -> dict:
    print("BOOTSTRAP init_data raw:", repr(init_data))

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        raise HTTPException(status_code=500, detail="TELEGRAM_BOT_TOKEN is not set")

    pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    received_hash = pairs.pop("hash", None)

    if not received_hash:
        raise HTTPException(status_code=400, detail="Invalid init_data: missing hash")

    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))

    secret_key = hmac.new(
        b"WebAppData",
        bot_token.encode("utf-8"),
        hashlib.sha256,
    ).digest()

    calculated_hash = hmac.new(
        secret_key,
        data_check_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    if not hmac.compare_digest(calculated_hash, received_hash):
        raise HTTPException(status_code=401, detail="Invalid Telegram init_data hash")

    user_raw = pairs.get("user")
    if not user_raw:
        raise HTTPException(status_code=400, detail="Invalid init_data: missing user")

    try:
        user = json.loads(user_raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid init_data: bad user JSON") from exc

    return user


def _is_active_premium(user: User) -> bool:
    if user.plan != "premium":
        return False
    if user.plan_expires_at is None:
        return True
    return user.plan_expires_at > datetime.now(timezone.utc)


@router.post("/bootstrap", response_model=BootstrapResponse)
def bootstrap_me(
    payload: BootstrapRequest,
    db: Session = Depends(get_session),
) -> BootstrapResponse:
    tg_user = _validate_telegram_init_data(payload.init_data)

    telegram_id = tg_user["id"]

    user = db.query(User).filter(User.telegram_id == telegram_id).one_or_none()
    is_new_user = user is None

    if user is None:
        user = User(
            telegram_id=telegram_id,
            username=tg_user.get("username"),
            first_name=tg_user.get("first_name"),
            last_name=tg_user.get("last_name"),
            plan="free",
            plan_expires_at=None,
            portfolio_limit=1,
            portfolio_count=0,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    else:
        changed = False

        if tg_user.get("username") and user.username != tg_user.get("username"):
            user.username = tg_user.get("username")
            changed = True

        if tg_user.get("first_name") and user.first_name != tg_user.get("first_name"):
            user.first_name = tg_user.get("first_name")
            changed = True

        if tg_user.get("last_name") and user.last_name != tg_user.get("last_name"):
            user.last_name = tg_user.get("last_name")
            changed = True

        if user.plan == "premium" and user.plan_expires_at is not None:
            if user.plan_expires_at <= datetime.now(timezone.utc):
                user.plan = "free"
                user.plan_expires_at = None
                changed = True

        if changed:
            db.add(user)
            db.commit()
            db.refresh(user)

    return BootstrapResponse(
        is_new_user=is_new_user,
        user=BootstrapUserResponse(
            id=user.id,
            telegram_id=user.telegram_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            plan="premium" if _is_active_premium(user) else "free",
            plan_expires_at=user.plan_expires_at if _is_active_premium(user) else None,
            portfolio_limit=user.portfolio_limit,
            portfolio_count=user.portfolio_count,
        ),
    )
