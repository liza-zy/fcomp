# apps/telegram_bot/handlers/__init__.py
from aiogram import Router

from .start import router as start_router
#from .quiz import router as quiz_router
#from .portfolio import router as portfolio_router

#router = Router()
router.include_router(start_router)
#router.include_router(quiz_router)
#router.include_router(portfolio_router)
