import asyncio
from async_redis_rate_limiters.concurrency import DistributedSemaphoreManager


async def test_basic():
    shared = {"counter": 0}

    async def _worker(
        manager: DistributedSemaphoreManager, key: str, value: int, shared: dict
    ):
        async with manager.get_semaphore(key, value):
            shared["counter"] += 1
            if shared["counter"] > value:
                raise Exception("Concurrent limit exceeded")
            await asyncio.sleep(0.001)
            shared["counter"] -= 1

    manager = DistributedSemaphoreManager(
        redis_url="redis://localhost:6379",
        redis_max_connections=10,
        redis_ttl=10,
    )
    tasks = [
        asyncio.create_task(_worker(manager, "test", 2, shared)) for _ in range(1000)
    ]
    await asyncio.gather(*tasks)
    assert manager._redis_memory_semaphore_helper is not None
    manager._redis_memory_semaphore_helper.clean()


async def test_memory_backend():
    shared = {"counter": 0}

    async def _worker(
        manager: DistributedSemaphoreManager, key: str, value: int, shared: dict
    ):
        async with manager.get_semaphore(key, value):
            shared["counter"] += 1
            if shared["counter"] > 2:
                raise Exception("Concurrent limit exceeded")
            await asyncio.sleep(0.001)
            shared["counter"] -= 1

    manager = DistributedSemaphoreManager(
        backend="memory",
    )
    tasks = [
        asyncio.create_task(_worker(manager, "test", 2, shared)) for _ in range(1000)
    ]
    await asyncio.gather(*tasks)
    assert manager._memory_memory_semaphore_helper is not None
    manager._memory_memory_semaphore_helper.clean()


async def test_clean():
    manager = DistributedSemaphoreManager(
        redis_url="redis://localhost:6379",
        redis_max_connections=10,
        redis_ttl=1,
    )
    async with manager.get_semaphore("test", 1):
        pass
    async with manager.get_semaphore("test", 1):
        pass
    assert manager._redis_memory_semaphore_helper is not None
    assert manager._redis_memory_semaphore_helper.clean() == 0
    await asyncio.sleep(1.2)
    assert manager._redis_memory_semaphore_helper.clean() == 1
