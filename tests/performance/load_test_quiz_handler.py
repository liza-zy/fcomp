import sys
import os
import asyncio
import time
from datetime import datetime
from statistics import mean

# Добавляем корень проекта в PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# Импортируем хендлер
from apps.telegram_bot.handlers.quiz import start_quiz  


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
    Упрощённый объект Message, которого достаточно для вызова хендлера.
    У него есть from_user, chat, text и метод answer().
    """

    def __init__(self, user_id: int, text: str = "/start"):
        self.message_id = 1
        self.date = datetime.now()
        self.from_user = DummyUser(user_id=user_id, first_name=f"User{user_id}")
        self.chat = DummyChat(chat_id=user_id)
        self.text = text

    async def answer(self, text: str, reply_markup=None, **kwargs):
        # Имитируем отправку сообщения — ничего не делаем
        return None


TOTAL_CALLS = 200      # сколько раз вызываем хендлер
CONCURRENCY = 20       # сколько одновременно


async def worker(results):
    while True:
        if results["total"] >= TOTAL_CALLS:
            break

        i = results["total"]
        results["total"] += 1

        msg = DummyMessage(user_id=1000 + i, text="/start")

        start = time.perf_counter()
        try:
            # ❗ Здесь вызываем твой хендлер, который принимает Message
            await start_quiz(msg)
            elapsed = (time.perf_counter() - start) * 1000  # ms

            results["latencies"].append(elapsed)
            results["success"] += 1
        except Exception as e:
            results["errors"] += 1
            print(f"Ошибка при обработке сообщения {i}: {e}")


async def main():
    results = {
        "total": 0,
        "success": 0,
        "errors": 0,
        "latencies": [],
    }

    tasks = [
        asyncio.create_task(worker(results))
        for _ in range(CONCURRENCY)
    ]
    await asyncio.gather(*tasks)

    if not results["latencies"]:
        print("Нет данных — тест не прошёл")
        return

    lat_sorted = sorted(results["latencies"])
    avg = mean(lat_sorted)
    p95 = lat_sorted[int(len(lat_sorted) * 0.95)]
    max_latency = max(lat_sorted)

    print(f"Всего вызовов хендлера: {results['total']}")
    print(f"Успешных: {results['success']}")
    print(f"Ошибок: {results['errors']}")
    print(f"Среднее время обработки: {avg:.2f} ms")
    print(f"P95 времени обработки: {p95:.2f} ms")
    print(f"Максимальное время обработки: {max_latency:.2f} ms")


if __name__ == "__main__":
    asyncio.run(main())