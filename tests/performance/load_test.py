import asyncio
import time
from statistics import mean
import httpx

URL = "http://127.0.0.1:8000/health/ping"  # сюда можно поставить свой эндпоинт
TOTAL_REQUESTS = 300                   # всего запросов
CONCURRENCY = 20                       # сколько одновременно

async def worker(name, client, results):
    while True:
        try:
            start = time.perf_counter()
            response = await client.get(URL)
            elapsed = (time.perf_counter() - start) * 1000  # в миллисекундах

            results["latencies"].append(elapsed)
            results["total"] += 1

            if response.status_code == 200:
                results["success"] += 1
            else:
                results["errors"] += 1
        except Exception as e:
            results["errors"] += 1
        finally:
            # завершаем, когда достигли общего лимита
            if results["total"] >= TOTAL_REQUESTS:
                break

async def main():
    results = {
        "total": 0,
        "success": 0,
        "errors": 0,
        "latencies": [],
    }

    async with httpx.AsyncClient(timeout=5.0) as client:
        tasks = [
            asyncio.create_task(worker(f"worker-{i}", client, results))
            for i in range(CONCURRENCY)
        ]

        await asyncio.gather(*tasks)

    if results["latencies"]:
        latencies_sorted = sorted(results["latencies"])
        avg = mean(latencies_sorted)
        max_latency = max(latencies_sorted)
        p95_index = int(len(latencies_sorted) * 0.95) - 1
        p95 = latencies_sorted[p95_index]

        print(f"Всего запросов: {results['total']}")
        print(f"Успешных (200): {results['success']}")
        print(f"Ошибок: {results['errors']}")
        print(f"Среднее время ответа: {avg:.2f} ms")
        print(f"P95 времени ответа: {p95:.2f} ms")
        print(f"Максимальное время ответа: {max_latency:.2f} ms")
    else:
        print("Нет замеров — возможно, все запросы упали.")

if __name__ == "__main__":
    asyncio.run(main())