import asyncio
from dataclasses import dataclass, field
from typing import AsyncContextManager, Literal

from async_redis_rate_limiters.adapters.redis import _RedisDistributedSemaphore
from async_redis_rate_limiters.pool import RedisConnectionPool


@dataclass
class DistributedSemaphoreManager:
    namespace: str = "default"
    """Namespace for the semaphore."""

    backend: Literal["redis", "memory"] = "redis"
    """Backend to use 'redis' (default) or 'memory' (not distributed, only for testing)."""

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
    __redis_pool_acquire: RedisConnectionPool | None = None
    __redis_pool_pubsub: RedisConnectionPool | None = None
    __redis_pool_release: RedisConnectionPool | None = None

    # only for memory backend
    # (namespace, key) -> (semaphore, value)
    __memory_semaphores: dict[tuple[str, str], tuple[asyncio.Semaphore, int]] = field(
        default_factory=dict
    )

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
        if self.__redis_pool_acquire is None:
            self.__redis_pool_acquire = self._make_redis_pool()
        return self.__redis_pool_acquire

    @property
    def _pool_release(self) -> RedisConnectionPool:
        if self.__redis_pool_release is None:
            self.__redis_pool_release = self._make_redis_pool()
        return self.__redis_pool_release

    @property
    def _pool_pubsub(self) -> RedisConnectionPool:
        if self.__redis_pool_pubsub is None:
            self.__redis_pool_pubsub = self._make_redis_pool()
        return self.__redis_pool_pubsub

    def _get_redis_semaphore(self, key: str, value: int) -> AsyncContextManager[None]:
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
            _max_wait_time=self.__blocking_wait_time,
        )

    def _get_memory_semaphore(self, key: str, value: int) -> AsyncContextManager[None]:
        if (self.namespace, key) not in self.__memory_semaphores:
            self.__memory_semaphores[(self.namespace, key)] = (
                asyncio.Semaphore(value),
                value,
            )
        semaphore, stored_value = self.__memory_semaphores[(self.namespace, key)]
        if stored_value != value:
            raise Exception(
                "you can't change the value of a semaphore after it has been created (for the same key)"
            )
        return semaphore

    def get_semaphore(self, key: str, value: int) -> AsyncContextManager[None]:
        """Get a distributed semaphore for the given key (with the given value)."""
        if self.backend == "redis":
            return self._get_redis_semaphore(key, value)
        elif self.backend == "memory":
            return self._get_memory_semaphore(key, value)
        raise ValueError(f"Invalid backend: {self.backend}")
