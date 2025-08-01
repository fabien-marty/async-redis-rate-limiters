import asyncio
import time
from async_redis_rate_limiters.concurrency import DistributedSemaphoreManager


concurrent = 0


async def _worker(manager: DistributedSemaphoreManager, key: str, max: int):
    global concurrent
    async with manager.get_semaphore(key, max):
        concurrent += 1
        if concurrent > max:
            print(concurrent)
            raise Exception("Concurrent limit exceeded")
        await asyncio.sleep(0.001)
        concurrent -= 1


async def main():
    max = 20
    manager = DistributedSemaphoreManager(
        redis_url="redis://localhost:6379",
    )
    before = time.perf_counter()
    tasks = [asyncio.create_task(_worker(manager, "test", max)) for _ in range(1_000)]
    await asyncio.gather(*tasks)
    after = time.perf_counter()
    print(f"Time taken: {after - before} seconds")


if __name__ == "__main__":
    asyncio.run(main())
