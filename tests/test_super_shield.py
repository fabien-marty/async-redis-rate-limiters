import asyncio
import time
from async_redis_rate_limiters.adapters.redis import _super_shield


async def _sleep(time: float, end_event: asyncio.Event):
    for _ in range(10):
        await asyncio.sleep(time / 10.0)
    end_event.set()


async def test_super_shield_normal():
    end_event = asyncio.Event()
    task = asyncio.create_task(_sleep(1.0, end_event))
    await _super_shield(task, timeout=2.0)
    assert end_event.is_set()


async def test_super_shield_timeout():
    end_event = asyncio.Event()
    task = asyncio.create_task(_sleep(2.0, end_event))
    try:
        await asyncio.wait_for(_super_shield(task, timeout=10.0), timeout=1.0)
    except asyncio.TimeoutError:
        pass
    assert end_event.is_set()


async def test_super_shield_multiple_timeout():
    end_event = asyncio.Event()
    task = asyncio.create_task(_sleep(5.0, end_event))
    try:
        async with asyncio.timeout(2.0):
            async with asyncio.timeout(1.0):
                await _super_shield(task, timeout=10.0)
    except asyncio.TimeoutError:
        pass
    assert end_event.is_set()


async def test_super_shield_real_timeout():
    end_event = asyncio.Event()
    task = asyncio.create_task(_sleep(2.0, end_event))
    before = time.perf_counter()
    await _super_shield(task, timeout=1.0)
    after = time.perf_counter()
    assert after - before > 1.0 and after - before < 1.1
    assert not end_event.is_set()
