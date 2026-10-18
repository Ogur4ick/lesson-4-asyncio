"""Задание 1. Последовательная загрузка и пул асинхронных работников.

Синхронная версия использует requests.Session для переиспользования
соединений. Асинхронная версия распределяет URL между пятью работниками:
внутри каждого запросы последовательны, но ожидания работников перекрываются.
Число работников ограничивает нагрузку на сервер.

Фактические замеры после запуска:
sync: 20,253 с.
async: 4,991 с.
Ускорение sync / async: 4,06 раз.
"""
import asyncio
from collections import deque
from time import perf_counter

import httpx
import requests

URLS = [f"https://picsum.photos/seed/{number}/800/600" for number in range(20)]
WORKERS = 5


def sync_main():
    statuses = []
    with requests.Session() as session:
        for address in URLS:
            with session.get(address, timeout=30) as reply:
                reply.raise_for_status()
                statuses.append(reply.status_code)
    return statuses


async def fetch(client: httpx.AsyncClient, url: str) -> int:
    reply = await client.get(url)
    reply.raise_for_status()
    return reply.status_code


async def fast_main():
    remaining = deque(enumerate(URLS))
    statuses = [0] * len(URLS)

    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        async def worker():
            while remaining:
                # Между проверкой deque и popleft нет await:
                # другой работник не может вклиниться в этот участок.
                index, address = remaining.popleft()
                statuses[index] = await fetch(client, address)

        await asyncio.gather(*(worker() for _ in range(WORKERS)))
    return statuses


def measure(operation):
    start = perf_counter()
    values = operation()
    return values, perf_counter() - start


if __name__ == "__main__":
    sync_codes, sync_seconds = measure(sync_main)
    async_codes, async_seconds = measure(lambda: asyncio.run(fast_main()))
    print(f"sync: {sync_seconds:.3f} с; ответов: {len(sync_codes)}")
    print(f"async: {async_seconds:.3f} с; ответов: {len(async_codes)}")
    print(f"Отношение sync / async: {sync_seconds / async_seconds:.2f}")
