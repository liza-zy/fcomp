# tests/unit/test_bot_menu.py

from apps.telegram_bot.handlers.start import (
    get_welcome_text,
    build_main_menu_keyboard,
)


def test_welcome_text_not_empty():
    text = get_welcome_text()
    assert isinstance(text, str)
    assert "FinCompass" in text or "фин" in text.lower()


def test_main_menu_keyboard_contains_buttons():
    kb = build_main_menu_keyboard()

    # aiogram ReplyKeyboardMarkup хранит кнопки в kb.keyboard (список рядов)
    buttons = [btn for row in kb.keyboard for btn in row]
    texts = {btn.text for btn in buttons}

    assert "Опрос" in texts
    assert "Собрать портфель" in texts