import sys
import os
import asyncio
import time
from datetime import datetime
from statistics import mean
import random

# Добавляем корень проекта в PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# Импортируем модуль с опросом
from apps.telegram_bot.handlers import quiz as quiz_module


class DummyUser:
    def __init__(self, user_id: int, first_name: str):
        self.id = user_id
        self.is_bot = False
        self.first_name = first_name


class DummyChat:
    def __init__(self, chat_id: int):
        self.id = chat_id
        self.type = "private"


class DummyMessage:
    """
    Упрощённый объект Message: есть from_user, chat, text и метод answer().
    Для хэндлеров aiogram этого достаточно.
    """
    def __init__(self, user_id: int, text: str):
        self.message_id = 1
        self.date = datetime.now()
        self.from_user = DummyUser(user_id=user_id, first_name=f"User{user_id}")
        self.chat = DummyChat(chat_id=user_id)
        self.text = text

    async def answer(self, text: str, reply_markup=None, **kwargs):
        # Имитация отправки сообщения — ничего не делаем
        return None


# 👉 НАСТРОЙКИ ТЕСТА
NUM_USERS = 50        # сколько "виртуальных" пользователей одновременно проходят опрос
CONCURRENCY = 20      # сколько сценариев одновременно выполняется


async def run_quiz_for_user(user_id: int, results):
    """
    Полный сценарий: старт + ответы на все вопросы для одного пользователя.
    Ответы выбираются случайно из списка допустимых вариантов для каждого вопроса.
    """
    start_time = time.perf_counter()
    try:
        # Шаг 1: старт опроса (кнопка "Опрос")
        msg_start = DummyMessage(user_id=user_id, text="Опрос")
        await quiz_module.start_quiz(msg_start)

        # Дальше для каждого вопроса выбираем случайный вариант и отправляем как ответ
        for idx, question in enumerate(quiz_module.QUESTIONS):
            options = question.get("options", [])
            if not options:
                raise RuntimeError(f"У вопроса {question.get('id')} нет options")

            chosen_opt = random.choice(options)
            answer_text = chosen_opt["text"]

            msg_answer = DummyMessage(user_id=user_id, text=answer_text)
            await quiz_module.handle_quiz_answer(msg_answer)

        elapsed = (time.perf_counter() - start_time) * 1000  # ms
        results["latencies"].append(elapsed)
        results["success"] += 1

    except Exception as e:
        results["errors"] += 1
        print(f"[user {user_id}] Ошибка при прохождении анкеты: {e}")


async def main():
    results = {
        "success": 0,
        "errors": 0,
        "latencies": [],
    }

    sem = asyncio.Semaphore(CONCURRENCY)

    async def sem_wrapper(uid: int):
        async with sem:
            await run_quiz_for_user(uid, results)

    tasks = [
        asyncio.create_task(sem_wrapper(1000 + i))
        for i in range(NUM_USERS)
    ]
    await asyncio.gather(*tasks)

    if not results["latencies"]:
        print("Нет данных — тест не прошёл")
        return

    lat_sorted = sorted(results["latencies"])
    avg = mean(lat_sorted)
    p95 = lat_sorted[int(len(lat_sorted) * 0.95)]
    max_latency = max(lat_sorted)

    print(f"Всего пользователей: {NUM_USERS}")
    print(f"Успешно прошли опрос: {results['success']}")
    print(f"Ошибок: {results['errors']}")
    print(f"Среднее время полного прохождения опроса: {avg:.2f} ms")
    print(f"P95 времени прохождения: {p95:.2f} ms")
    print(f"Максимальное время прохождения: {max_latency:.2f} ms")


if __name__ == "__main__":
    asyncio.run(main())