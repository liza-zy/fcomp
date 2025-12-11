# apps/telegram_bot/handlers/portfolio.py

import os
from enum import Enum
from typing import Dict

import httpx
from aiogram import Router, F
from aiogram.types import Message
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from dotenv import load_dotenv

from apps.telegram_bot.handlers.start import build_main_menu_keyboard

load_dotenv()

ORCH_URL = os.getenv("ORCH_URL", "http://127.0.0.1:8000")

router = Router()


class PortfolioState(str, Enum):
    IDLE = "idle"
    WAITING_AMOUNT = "waiting_amount"


_user_states: Dict[int, PortfolioState] = {}


def _set_state(user_id: int, state: PortfolioState) -> None:
    _user_states[user_id] = state


def _get_state(user_id: int) -> PortfolioState:
    return _user_states.get(user_id, PortfolioState.IDLE)


def build_portfolio_keyboard():
    kb = ReplyKeyboardBuilder()
    kb.button(text="Назад в меню")
    return kb.as_markup(resize_keyboard=True)


@router.message(F.text == "Собрать портфель")
async def start_portfolio_flow(message: Message) -> None:
    """Запрос базового параметра — условной суммы инвестиций."""
    _set_state(message.from_user.id, PortfolioState.WAITING_AMOUNT)
    await message.answer(
        "Введите сумму, которую хотите инвестировать (в рублях):",
        reply_markup=build_portfolio_keyboard(),
    )


@router.message(F.text == "Назад в меню")
async def back_to_menu(message: Message) -> None:
    _set_state(message.from_user.id, PortfolioState.IDLE)
    await message.answer("Возвращаемся в главное меню.", reply_markup=build_main_menu_keyboard())


@router.message(F.text.regexp(r"^\d+(\.\d+)?$"))
async def handle_amount(message: Message) -> None:
    """Получаем сумму, отправляем запрос в orchestrator /portfolio/build."""
    if _get_state(message.from_user.id) != PortfolioState.WAITING_AMOUNT:
        return

    amount = float(message.text)

    payload = {
        "extra": {
            "amount": amount,
            "source": "telegram_bot",
        }
    }

    async with httpx.AsyncClient(base_url=ORCH_URL, timeout=5.0) as client:
        resp = await client.post("/portfolio/build", json=payload)
        resp.raise_for_status()
        data = resp.json()

    allocations = data.get("allocations", [])

    lines = [f"Собран базовый портфель на сумму {amount:,.0f} ₽:"]
    for alloc in allocations:
        ticker = alloc.get("ticker")
        weight = alloc.get("weight")
        lines.append(f"• {ticker}: {weight:.0%}")

    text = "\n".join(lines)

    _set_state(message.from_user.id, PortfolioState.IDLE)

    await message.answer(
        text,
        reply_markup=build_main_menu_keyboard(),
    )


@router.message()  # fallback для некорректного ввода
async def handle_other(message: Message) -> None:
    if _get_state(message.from_user.id) == PortfolioState.WAITING_AMOUNT:
        await message.answer("Пожалуйста, введите сумму цифрами, например: 100000")