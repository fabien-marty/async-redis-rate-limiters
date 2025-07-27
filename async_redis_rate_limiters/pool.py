import asyncio
import collections
from dataclasses import dataclass, field
import logging
import time
from types import TracebackType
from typing import AsyncContextManager
import uuid

import redis.asyncio as redis


@dataclass
class RedisConnectionPoolContextManager:
    """Async context manager for Redis connections from the pool.

    Provides automatic acquisition and release of Redis connections,
    ensuring proper resource cleanup when exiting the context.
    """

    _connection: redis.Redis
    _pool: "RedisConnectionPool"

    async def __aenter__(self) -> redis.Redis:
        return self._connection

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if isinstance(exc_value, redis.RedisError):
            # let's not reuse this connection
            self._connection._redis_connection_pool_error = True  # type: ignore
        await self._pool._release(self._connection)


@dataclass
class RedisConnectionPool:
    """Redis connection pool with automatic connection lifecycle management.

    This pool maintains a collection of Redis connections that can be reused
    across multiple operations. Connections are automatically created when needed
    and expired after a TTL to ensure freshness.

    Note: not thread safe.

    Example:
        ```python
        pool = RedisConnectionPool(
            redis_url="redis://localhost:6379",
            max_connections=50,
            ttl=60
        )

        # Use a connection from the pool
        async with await pool.get_connection() as redis_client:
            await redis_client.set("key", "value")
            value = await redis_client.get("key")
        ```

    """

    redis_url: str
    """Redis connection URL (e.g., "redis://localhost:6379")."""

    socket_connect_timeout: int = 10
    """Timeout for establishing socket connections (seconds)."""

    socket_timeout: int = 30
    """Timeout for socket operations (seconds)."""

    max_connections: int = 100
    """Maximum number of connections to track."""

    ttl: int = 30
    """Connection time to live (seconds)."""

    __logger: logging.Logger = logging.getLogger("async_redis_rate_limiters.pool")
    __pool: collections.deque[redis.Redis] = field(default_factory=collections.deque)
    __connection_counter: int = 0
    __not_full_event: asyncio.Event | None = None

    @property
    def _not_full_event(self) -> asyncio.Event:
        if self.__not_full_event is None:
            self.__not_full_event = asyncio.Event()
            self.__not_full_event.set()
        return self.__not_full_event

    def _make_redis_connection(self) -> redis.Redis:
        self.__logger.debug("Making a new connection")
        connection = redis.Redis.from_url(
            self.redis_url,
            socket_connect_timeout=self.socket_connect_timeout,
            socket_timeout=self.socket_timeout,
        )
        connection._redis_connection_pool_creation_time = time.perf_counter()  # type: ignore
        connection._redis_connection_pool_connection_id = str(uuid.uuid4())  # type: ignore
        connection._redis_connection_pool_error = False  # type: ignore
        return connection

    def _is_expired_connection(self, connection: redis.Redis) -> bool:
        return (
            time.perf_counter() - connection._redis_connection_pool_creation_time  # type: ignore
            > self.ttl
        )

    async def _get_connection(self) -> redis.Redis:
        while True:
            try:
                connection = self.__pool.popleft()
            except IndexError:
                if self.__connection_counter >= self.max_connections:
                    not_full_event = self._not_full_event
                    not_full_event.clear()
                    try:
                        await asyncio.wait_for(not_full_event.wait(), timeout=10)
                    except asyncio.TimeoutError:
                        # should never (or very rarely) happen
                        pass
                    continue
                self.__connection_counter += 1
                return self._make_redis_connection()
            break
        if self._is_expired_connection(connection):
            self.__logger.debug("Expired connection => let's make a new one")
            try:
                await asyncio.wait_for(
                    connection.aclose(), timeout=self.socket_connect_timeout
                )
            except Exception:
                pass
            return self._make_redis_connection()
        return connection

    async def _release(self, connection: redis.Redis):
        if connection._redis_connection_pool_error is True:  # type: ignore
            # let's not reuse this connection
            try:
                # try to close it
                await asyncio.wait_for(
                    connection.aclose(), timeout=self.socket_connect_timeout
                )
            except Exception:
                pass
            self.__connection_counter -= 1
        else:
            # let's reuse this connection
            self.__pool.append(connection)
        self._not_full_event.set()

    async def context_manager(self) -> AsyncContextManager[redis.Redis]:
        connection = await self._get_connection()
        return RedisConnectionPoolContextManager(_connection=connection, _pool=self)
