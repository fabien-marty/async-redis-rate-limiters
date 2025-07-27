import asyncio
from dataclasses import dataclass
import time
from typing import AsyncContextManager
import uuid

from async_redis_rate_limiters.lua import ACQUIRE_LUA_SCRIPT, RELEASE_LUA_SCRIPT
from async_redis_rate_limiters.pool import RedisConnectionPool
from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


@dataclass(kw_only=True)
class _RedisDistributedSemaphore:
    namespace: str
    redis_url: str
    key: str
    value: int
    ttl: int

    redis_number_of_attempts: int = 3
    redis_retry_min_delay: float = 1
    redis_retry_multiplier: float = 2
    redis_retry_max_delay: float = 60

    _pool_acquire: RedisConnectionPool
    _pool_release: RedisConnectionPool
    _pool_pubsub: RedisConnectionPool
    _blocking_wait_time: int = 10
    __client_id: str | None = None

    def _get_channel(self) -> str:
        return f"{self.namespace}:rate_limiter:channel:{self.key}"

    def _get_zset_key(self) -> str:
        return f"{self.namespace}:rate_limiter:zset:{self.key}"

    def _async_retrying(self) -> AsyncRetrying:
        return AsyncRetrying(
            stop=stop_after_attempt(self.redis_number_of_attempts),
            wait=wait_exponential(
                multiplier=self.redis_retry_multiplier,
                min=self.redis_retry_min_delay,
                max=self.redis_retry_max_delay,
            ),
            retry=retry_if_exception_type(),
            reraise=True,
        )

    async def __aenter__(self) -> None:
        if self.__client_id is not None:
            raise RuntimeError(
                "Semaphore already acquired (in the past) => don't reuse the same semaphore instance"
            )
        client_id = str(uuid.uuid4()).replace("-", "")
        async for attempt in self._async_retrying():
            with attempt:
                async with await self._pool_acquire.context_manager() as client:
                    acquire_script = client.register_script(ACQUIRE_LUA_SCRIPT)
                    async with (
                        await self._pool_pubsub.context_manager() as pubsub_client
                    ):
                        async with pubsub_client.pubsub() as pubsub:
                            # Subscribe to the channel
                            await pubsub.subscribe(self._get_channel())

                            # Try to get the lock
                            while True:
                                now = time.time()
                                acquired = await acquire_script(
                                    keys=[self._get_zset_key()],
                                    args=[
                                        self._get_channel(),
                                        client_id,
                                        self.value,
                                        self.ttl,
                                        now,
                                    ],
                                )
                                if acquired == 1:
                                    self.__client_id = client_id
                                    await pubsub.unsubscribe(self._get_channel())
                                    return None

                                # Wait for notification using pubsub
                                try:
                                    await asyncio.wait_for(
                                        pubsub.get_message(
                                            timeout=self._blocking_wait_time
                                        ),
                                        timeout=self._blocking_wait_time,
                                    )
                                except asyncio.TimeoutError:
                                    # Timeout reached, continue to retry acquisition
                                    pass

    async def __aexit__(self, exc_type, exc_value, traceback):
        assert self.__client_id is not None
        async for attempt in self._async_retrying():
            with attempt:
                async with await self._pool_release.context_manager() as client:
                    release_script = client.register_script(RELEASE_LUA_SCRIPT)
                    await release_script(
                        keys=[self._get_zset_key()],
                        args=[self._get_channel(), self.__client_id, self.ttl],
                    )
        return


@dataclass
class DistributedSemaphoreManager:
    namespace: str = "default"
    """Namespace for the semaphore."""

    redis_url: str = "redis://localhost:6379"
    """Redis connection URL (e.g., "redis://localhost:6379")."""

    redis_ttl: int = 310
    """Redis connection time to live (seconds)."""

    redis_max_connections: int = 300
    """Redis maximum number of connections."""

    redis_socket_timeout: int = 30
    """Redis timeout for socket operations (seconds)."""

    redis_socket_connect_timeout: int = 10
    """Redis timeout for establishing socket connections (seconds)."""

    redis_number_of_attempts: int = 3
    """Number of attempts to retry Redis operations."""

    redis_retry_multiplier: float = 2
    """Multiplier for the delay between Redis operations (in case of failures/retries)."""

    redis_retry_min_delay: float = 1
    """Minimum delay between Redis operations (seconds)."""

    redis_retry_max_delay: float = 60
    """Maximum delay between Redis operations (seconds)."""

    __blocking_wait_time: int = 10
    __pool_acquire: RedisConnectionPool | None = None
    __pool_pubsub: RedisConnectionPool | None = None
    __pool_release: RedisConnectionPool | None = None

    def __post_init__(self):
        if self.redis_max_connections < 3:
            raise ValueError("redis_max_connections must be at least 3")
        if self.redis_socket_timeout <= self.__blocking_wait_time:
            raise ValueError(
                "redis_socket_timeout must be greater than _blocking_wait_time"
            )

    def _make_redis_pool(self) -> RedisConnectionPool:
        return RedisConnectionPool(
            redis_url=self.redis_url,
            max_connections=self.redis_max_connections // 3,
            socket_connect_timeout=self.redis_socket_connect_timeout,
            socket_timeout=self.redis_socket_timeout,
        )

    @property
    def _pool_acquire(self) -> RedisConnectionPool:
        if self.__pool_acquire is None:
            self.__pool_acquire = self._make_redis_pool()
        return self.__pool_acquire

    @property
    def _pool_release(self) -> RedisConnectionPool:
        if self.__pool_release is None:
            self.__pool_release = self._make_redis_pool()
        return self.__pool_release

    @property
    def _pool_pubsub(self) -> RedisConnectionPool:
        if self.__pool_pubsub is None:
            self.__pool_pubsub = self._make_redis_pool()
        return self.__pool_pubsub

    def get_semaphore(self, key: str, value: int) -> AsyncContextManager[None]:
        """Get a distributed semaphore for the given key (with the given value)."""
        return _RedisDistributedSemaphore(
            namespace=self.namespace,
            redis_url=self.redis_url,
            key=key,
            value=value,
            ttl=self.redis_ttl,
            redis_number_of_attempts=self.redis_number_of_attempts,
            redis_retry_min_delay=self.redis_retry_min_delay,
            redis_retry_multiplier=self.redis_retry_multiplier,
            redis_retry_max_delay=self.redis_retry_max_delay,
            _pool_acquire=self._pool_acquire,
            _pool_release=self._pool_release,
            _pool_pubsub=self._pool_pubsub,
            _blocking_wait_time=self.__blocking_wait_time,
        )
