# apps/telegram_bot/handlers/quiz.py

import os
from enum import Enum
from pathlib import Path
from typing import Dict, Any

import httpx
import yaml
from aiogram import Router, F
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from dotenv import load_dotenv

from apps.telegram_bot.handlers.start import build_main_menu_keyboard

load_dotenv()

ORCH_URL = os.getenv("ORCH_URL", "http://127.0.0.1:8000")

router = Router()

# Загружаем те же вопросы, что использует сервис risk_quiz
QUESTIONS_PATH = Path(__file__).resolve().parents[3] / "services" / "risk_quiz" / "domain" / "questions.yaml"
with QUESTIONS_PATH.open("r", encoding="utf-8") as f:
    _config = yaml.safe_load(f)

QUESTIONS = _config["questions"]  # список вопросов в нужном порядке


class QuizFSM(str, Enum):
    IDLE = "idle"
    IN_PROGRESS = "in_progress"


class QuizSession:
    def __init__(self) -> None:
        self.current_index: int = 0
        self.answers: Dict[str, str] = {}


# Простое хранилище в памяти: user_id -> QuizSession
_sessions: Dict[int, QuizSession] = {}
_states: Dict[int, QuizFSM] = {}


def _get_state(user_id: int) -> QuizFSM:
    return _states.get(user_id, QuizFSM.IDLE)


def _set_state(user_id: int, state: QuizFSM) -> None:
    _states[user_id] = state


def _get_session(user_id: int) -> QuizSession:
    if user_id not in _sessions:
        _sessions[user_id] = QuizSession()
    return _sessions[user_id]


def _reset_session(user_id: int) -> None:
    _sessions.pop(user_id, None)
    _states[user_id] = QuizFSM.IDLE


def _build_options_keyboard(question: Dict[str, Any]) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    for opt in question["options"]:
        kb.button(text=opt["text"])
    kb.button(text="Отмена")
    kb.adjust(1)
    return kb.as_markup(resize_keyboard=True)


async def _send_current_question(message: Message, session: QuizSession) -> None:
    idx = session.current_index
    if idx >= len(QUESTIONS):
        return

    q = QUESTIONS[idx]
    text = q["text"]
    kb = _build_options_keyboard(q)

    await message.answer(text, reply_markup=kb)


@router.message(F.text == "Опрос")
async def start_quiz(message: Message) -> None:
    """Старт опроса из главного меню."""
    user_id = message.from_user.id
    _reset_session(user_id)  # на всякий случай
    _set_state(user_id, QuizFSM.IN_PROGRESS)
    session = _get_session(user_id)

    await message.answer(
        "Пройдём короткий опрос, чтобы понять ваш риск-профиль.\n"
        "Отвечайте так, как вы чувствуете себя комфортно.",
    )
    await _send_current_question(message, session)


@router.message(F.text == "Отмена")
async def cancel_quiz(message: Message) -> None:
    user_id = message.from_user.id
    _reset_session(user_id)
    await message.answer(
        "Опрос отменён. Вернёмся в главное меню.",
        reply_markup=build_main_menu_keyboard(),
    )


@router.message()
async def handle_quiz_answer(message: Message) -> None:
    """Обработка ответов, пока опрос в процессе."""
    user_id = message.from_user.id
    state = _get_state(user_id)

    # Если мы не в состоянии опроса — игнорируем, пусть другие хэндлеры ловят
    if state != QuizFSM.IN_PROGRESS:
        return

    session = _get_session(user_id)
    idx = session.current_index

    # Перестраховка
    if idx >= len(QUESTIONS):
        await message.answer(
            "Кажется, опрос уже завершён. Начнём заново?",
            reply_markup=build_main_menu_keyboard(),
        )
        _reset_session(user_id)
        return

    current_question = QUESTIONS[idx]
    # Ищем опцию по тексту кнопки
    chosen_text = message.text.strip()
    matched_option = None
    for opt in current_question["options"]:
        if opt["text"] == chosen_text:
            matched_option = opt
            break

    if not matched_option:
        # Пользователь написал что-то своё, не нажал кнопку
        await message.answer(
            "Пожалуйста, выберите один из вариантов, используя кнопки под сообщением.",
            reply_markup=_build_options_keyboard(current_question),
        )
        return

    # Сохраняем ответ
    q_id = current_question["id"]
    session.answers[q_id] = matched_option["code"]
    session.current_index += 1

    # Если есть ещё вопросы — задаём следующий
    if session.current_index < len(QUESTIONS):
        await _send_current_question(message, session)
        return

    # Вопросы закончились — считаем риск-профиль через Orchestrator API
    _set_state(user_id, QuizFSM.IDLE)

    async with httpx.AsyncClient(base_url=ORCH_URL, timeout=10.0) as client:
        resp = await client.post("/quiz/score", json={"answers": session.answers})
        resp.raise_for_status()
        data = resp.json()

    risk_class = data.get("risk_class", "Unknown")
    confidence = data.get("confidence", 0.0)
    neighbor = data.get("neighbor_class")
    neighbor_conf = data.get("neighbor_confidence")
    profile_text = data.get("profile_text")

    # Собираем текст ответа
    lines = [
        "Спасибо! Вот ваш предварительный риск-профиль:",
        f"• Основной профиль: <b>{risk_class}</b> (уверенность {int(confidence * 100)}%)",
    ]
    if neighbor:
        lines.append(
            f"• Соседний профиль: <b>{neighbor}</b> (уверенность {int((neighbor_conf or 0) * 100)}%)"
        )

    if profile_text:
        lines.append("")
        lines.append(profile_text)

    lines.append("")
    lines.append("Этот результат не является индивидуальной инвестрекомендацией, "
                 "а помогает оценить ваше отношение к риску.")

    await message.answer(
        "\n".join(lines),
        reply_markup=build_main_menu_keyboard(),
    )

    # Очищаем сессию
    _reset_session(user_id)