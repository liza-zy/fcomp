import asyncio, os
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message

import logging
logging.basicConfig(level=logging.INFO)
logging.info("BOT PROCESS STARTED")

from aiogram import Dispatcher

from apps.telegram_bot.handlers import router as handlers_router


BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "invalid")
dp = Dispatcher()
dp.include_router(handlers_router)

@dp.message(CommandStart())
async def start(message: Message):
    await message.answer("FinCompass bot is alive ✨")

async def main():
    if BOT_TOKEN == "invalid":
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in .env")
    bot = Bot(BOT_TOKEN)
    logging.info("Starting polling...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
