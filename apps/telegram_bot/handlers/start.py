# apps/telegram_bot/handlers/start.py

from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder

router = Router()


def get_welcome_text() -> str:
    """Текст приветствия для /start — чистая функция для тестов."""
    return (
        "Привет! Я FinCompass бот.\n\n"
        "Помогу оценить твой риск-профиль и собрать базовый портфель."
    )


def build_main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню — чистая функция, тестируемая отдельно."""
    kb = ReplyKeyboardBuilder()
    kb.button(text="Опрос")
    kb.button(text="Собрать портфель")
    kb.adjust(2)
    return kb.as_markup(resize_keyboard=True, one_time_keyboard=False)


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    """Хэндлер /start."""
    await message.answer(
        text=get_welcome_text(),
        reply_markup=build_main_menu_keyboard(),
    )