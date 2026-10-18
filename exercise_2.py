"""Задание 2. Повторные попытки и сбор независимых исходов.

Каждая попытка ограничена wait_for и сетевым таймаутом HTTPX.
Между тремя попытками две неблокирующие паузы: 1 и 2 секунды.
Временные HTTP-ошибки 408, 429 и 5xx повторяются; остальные статусы,
например 404, сразу попадают в список ошибок. Сетевые ошибки повторяются.
Вложенная collect перехватывает ожидаемые ошибки отдельно для каждого URL,
поэтому gather возвращает результаты остальных загрузок.

Фактические замеры после запуска:
Общее время, 22 URL: 18,09 с.
Результатов: 20. Ошибок: 2.
"""
import asyncio
from time import perf_counter

import httpx

URLS = [f"https://picsum.photos/seed/{number}/800/600" for number in range(20)]
TEST_URLS = URLS + ["https://no-such-site.invalid", "https://httpbin.org/delay/10"]
TIME_LIMIT = 5.0
MAX_ATTEMPTS = 3


def temporary_status(status: int) -> bool:
    return status in (408, 429) or 500 <= status < 600


async def robust_fetch(client: httpx.AsyncClient, url: str) -> int:
    pause = 1.0
    for number in range(1, MAX_ATTEMPTS + 1):
        try:
            reply = await asyncio.wait_for(
                client.get(url, timeout=TIME_LIMIT),
                timeout=TIME_LIMIT,
            )
            reply.raise_for_status()
            return reply.status_code
        except httpx.HTTPStatusError as error:
            if not temporary_status(error.response.status_code):
                raise
            if number == MAX_ATTEMPTS:
                raise
        except (httpx.RequestError, asyncio.TimeoutError):
            if number == MAX_ATTEMPTS:
                raise

        await asyncio.sleep(pause)
        pause *= 2
    raise AssertionError("MAX_ATTEMPTS должен быть больше нуля")


async def robust_main(urls=None):
    addresses = list(TEST_URLS if urls is None else urls)
    async with httpx.AsyncClient(follow_redirects=True) as client:
        async def collect(address):
            try:
                code = await robust_fetch(client, address)
                return True, {"url": address, "status": code}
            except (httpx.HTTPError, asyncio.TimeoutError) as error:
                return False, {
                    "url": address,
                    "type": type(error).__name__,
                    "message": str(error) or "Превышено время ожидания",
                }

        outcomes = await asyncio.gather(*(collect(address) for address in addresses))

    successes = [value for ok, value in outcomes if ok]
    failures = [value for ok, value in outcomes if not ok]
    return successes, failures


if __name__ == "__main__":
    started = perf_counter()
    successes, failures = asyncio.run(robust_main())
    print(f"Общее время: {perf_counter() - started:.3f} с")
    print(f"Результатов: {len(successes)}; ошибок: {len(failures)}")
    for item in successes:
        print(f"OK {item['status']} {item['url']}")
    for item in failures:
        print(f"ERROR {item['url']}: {item['type']}: {item['message']}")
