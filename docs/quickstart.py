import asyncio
from typing import AsyncContextManager
from async_redis_rate_limiters import DistributedSemaphoreManager


async def worker(semaphore: AsyncContextManager):
    async with semaphore:
        # concurrency limit enforced here
        pass


async def main():
    manager = DistributedSemaphoreManager(
        redis_url="redis://localhost:6379",
        redis_max_connections=100,
        redis_ttl=3600,  # semaphore max duration (seconds)
    )
    # Limit the concurrency to 10 concurrent tasks for the key "test"
    semaphore = manager.get_semaphore("test", 10)
    tasks = [asyncio.create_task(worker(semaphore)) for _ in range(1000)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
