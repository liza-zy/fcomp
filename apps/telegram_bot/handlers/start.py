# apps/telegram_bot/handlers/start.py

from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message, ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
from aiogram.utils.keyboard import ReplyKeyboardBuilder

router = Router()

WEBAPP_URL = "https://fcomp.duckdns.org/"  # именно https

def get_welcome_text() -> str:
    return (
        "Привет! Я FinCompass бот.\n\n"
        "Помогу оценить твой риск-профиль и собрать базовый портфель."
    )

#def build_main_menu_keyboard() -> ReplyKeyboardMarkup:
#    kb = ReplyKeyboardBuilder()
#    kb.button(text="Опрос")
#    kb.add(KeyboardButton(
#        text="Собрать портфель",
#        web_app=WebAppInfo(url=WEBAPP_URL),
#    ))
#    kb.adjust(2)
#    return kb.as_markup(resize_keyboard=True)

@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    await message.answer(
        text=get_welcome_text(),
        reply_markup=build_main_menu_keyboard(),
    )
