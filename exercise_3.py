"""Задание 3*. Загрузка, хеширование и отзывчивость event loop.

Одинаковые тела ответов обрабатываются двумя способами: прямыми вызовами
heavy и отправкой heavy в ProcessPoolExecutor через run_in_executor.
При прямых вызовах нет передачи управления, поэтому точки замирают.
При ожидании результатов процессов ticker продолжает работать.
В обеих фазах измеряется не только время, но и максимальная пауза ticker.
Пул ограничен двумя процессами из-за памяти, нужной для body * 1000.

Фактические замеры после запуска:
Скачивание: 1,833 с.
Прямое вычисление: 0,741 с; пауза ticker: 0,835 с.
Процессы: 0,536 с; пауза ticker: 0,113 с.
"""
import asyncio
import hashlib
from concurrent.futures import ProcessPoolExecutor
from contextlib import suppress
from time import perf_counter

import httpx

URLS = [f"https://picsum.photos/seed/{number}/800/600" for number in range(20)]


def heavy(body: bytes) -> str:
    return hashlib.sha256(body * 1000).hexdigest()


async def ticker(gaps):
    previous = perf_counter()
    while True:
        print(".", end="", flush=True)
        await asyncio.sleep(0.1)
        current = perf_counter()
        gaps.append(current - previous)
        previous = current


async def download_all():
    async with httpx.AsyncClient(follow_redirects=True, timeout=30) as client:
        async def get_body(address):
            reply = await client.get(address)
            reply.raise_for_status()
            return reply.content
        return await asyncio.gather(*(get_body(address) for address in URLS))


async def hash_direct(bodies):
    return [heavy(body) for body in bodies]


async def hash_in_processes(bodies):
    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=2) as executor:
        jobs = [loop.run_in_executor(executor, heavy, body) for body in bodies]
        return await asyncio.gather(*jobs)


async def demonstrate(title, operation, bodies):
    gaps = []
    print(f"\n{title}", flush=True)
    pulse = asyncio.create_task(ticker(gaps))
    try:
        await asyncio.sleep(0.2)
        begin = perf_counter()
        hashes = await operation(bodies)
        duration = perf_counter() - begin
        # Даём ticker проснуться и записать паузу, включая блокировку.
        await asyncio.sleep(0.2)
    finally:
        pulse.cancel()
        with suppress(asyncio.CancelledError):
            await pulse
    print(f"\nВремя: {duration:.3f} с; максимальная пауза: {max(gaps, default=0):.3f} с")
    return hashes


async def cpu_main():
    started = perf_counter()
    bodies = await download_all()
    print(f"Скачивание: {perf_counter() - started:.3f} с")
    first = await demonstrate("Прямое вычисление", hash_direct, bodies)
    second = await demonstrate("Процессы", hash_in_processes, bodies)
    assert first == second
    print(f"Проверено совпадение {len(second)} хешей")


if __name__ == "__main__":
    asyncio.run(cpu_main())
