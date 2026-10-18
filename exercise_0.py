"""Задание 0. Последовательное ожидание и конкурентный запуск.

await в цикле не запускает все запросы одновременно: следующая
итерация начинается после завершения предыдущей. В исправлении
сначала создаются задачи для всех URL, затем ожидаются их результаты.

Фактические замеры после запуска:
Последовательно: 4,789 с.
Конкурентно: 1,552 с.
"""
import asyncio
from time import perf_counter_ns

import httpx

URLS = [f"https://picsum.photos/seed/{number}/800/600" for number in range(20)]


async def fetch(client: httpx.AsyncClient, url: str) -> int:
    response = await client.get(url)
    response.raise_for_status()
    return response.status_code


async def slow_main():
    codes = []
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        for address in URLS:
            codes.append(await fetch(client, address))
    return codes


async def fast_main():
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        pending = [asyncio.create_task(fetch(client, address)) for address in URLS]
        return await asyncio.gather(*pending)


async def compare():
    for title, operation in [("Последовательно", slow_main), ("Конкурентно", fast_main)]:
        begin = perf_counter_ns()
        codes = await operation()
        elapsed = (perf_counter_ns() - begin) / 1_000_000_000
        print(f"{title}: {elapsed:.3f} с; получено {len(codes)} ответов")


if __name__ == "__main__":
    asyncio.run(compare())
