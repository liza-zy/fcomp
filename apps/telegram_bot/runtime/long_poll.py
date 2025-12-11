# apps/telegram_bot/runtime/long_poll.py

import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from dotenv import load_dotenv

from apps.telegram_bot.handlers.start import router as start_router
from apps.telegram_bot.handlers.quiz import router as quiz_router
from apps.telegram_bot.handlers.portfolio import router as portfolio_router
from aiogram.client.default import DefaultBotProperties

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")


async def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set in .env")

    bot = Bot(
    token=TELEGRAM_BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()

    # Регистрируем роутеры
    dp.include_router(start_router)
    dp.include_router(quiz_router)
    dp.include_router(portfolio_router)

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())