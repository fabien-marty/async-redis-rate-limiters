import asyncio

import redis
from async_redis_rate_limiters.pool import RedisConnectionPool


async def test_basic_usage():
    pool = RedisConnectionPool(
        redis_url="redis://localhost:6379",
        max_connections=2,
        ttl=10,
    )
    async with await pool.context_manager() as redis_client:
        id1 = redis_client._redis_connection_pool_connection_id  # type: ignore
        assert await redis_client.ping() is True
    async with await pool.context_manager() as redis_client:
        id2 = redis_client._redis_connection_pool_connection_id  # type: ignore
        assert await redis_client.ping() is True
    assert id1 == id2  # connection reused
    async with await pool.context_manager() as redis_client1:
        async with await pool.context_manager() as redis_client2:
            assert await redis_client1.ping() is True
            assert await redis_client2.ping() is True
            assert (
                redis_client1._redis_connection_pool_connection_id  # type: ignore
                != redis_client2._redis_connection_pool_connection_id  # type: ignore
            )


async def test_expired_connection():
    pool = RedisConnectionPool(
        redis_url="redis://localhost:6379",
        max_connections=2,
        ttl=1,
    )
    async with await pool.context_manager() as redis_client:
        await redis_client.ping() is True
        id1 = redis_client._redis_connection_pool_connection_id  # type: ignore
    await asyncio.sleep(1.1)
    async with await pool.context_manager() as redis_client:
        await redis_client.ping() is True
        id2 = redis_client._redis_connection_pool_connection_id  # type: ignore
    assert id1 != id2  # connection reused


async def test_massive_concurrency():
    shared = {"counter": 0}

    async def _worker(pool: RedisConnectionPool, shared: dict):
        async with await pool.context_manager() as redis_client:
            shared["counter"] += 1
            if shared["counter"] > 1:
                raise Exception("Concurrent limit exceeded")
            await redis_client.ping() is True
            await asyncio.sleep(0.001)
            shared["counter"] -= 1

    pool = RedisConnectionPool(
        redis_url="redis://localhost:6379",
        max_connections=1,
        ttl=1,
    )
    tasks = [asyncio.create_task(_worker(pool, shared)) for _ in range(500)]
    await asyncio.gather(*tasks)


async def test_with_error():
    pool = RedisConnectionPool(
        redis_url="redis://foobar:6379",
        max_connections=1,
        ttl=1,
    )
    try:
        async with await pool.context_manager() as redis_client:
            await redis_client.ping()
    except redis.RedisError:
        pass
