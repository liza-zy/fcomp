import asyncio, os
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "invalid")
dp = Dispatcher()

@dp.message(CommandStart())
async def start(message: Message):
    await message.answer("FinCompass bot is alive ✨")

async def main():
    if BOT_TOKEN == "invalid":
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN in .env")
    bot = Bot(BOT_TOKEN)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
