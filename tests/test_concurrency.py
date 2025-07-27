import asyncio
from typing import AsyncContextManager
from async_redis_rate_limiters.concurrency import DistributedSemaphoreManager


async def test_basic():
    shared = {"counter": 0}

    async def _worker(semaphore: AsyncContextManager, shared: dict):
        async with semaphore:
            shared["counter"] += 1
            if shared["counter"] > 2:
                raise Exception("Concurrent limit exceeded")
            await asyncio.sleep(0.001)
            shared["counter"] -= 1

    manager = DistributedSemaphoreManager(
        redis_url="redis://localhost:6379",
        redis_max_connections=10,
        redis_ttl=10,
    )
    semaphore = manager.get_semaphore("test", 2)
    tasks = [asyncio.create_task(_worker(semaphore, shared)) for _ in range(100)]
    await asyncio.gather(*tasks)
