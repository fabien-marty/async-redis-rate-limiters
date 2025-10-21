from dataclasses import dataclass
from typing import Any, AsyncContextManager, Literal

from redis.asyncio import BlockingConnectionPool

from async_redis_rate_limiters.adapters.redis import _RedisDistributedSemaphore

from redis.asyncio import Redis

from async_redis_rate_limiters.lua import ACQUIRE_LUA_SCRIPT, RELEASE_LUA_SCRIPT
from async_redis_rate_limiters.memory import (
    MemorySemaphoreHelper,
)


@dataclass
class DistributedSemaphoreManager:
    namespace: str = "default"
    """Namespace for the semaphore."""

    backend: Literal["redis", "memory"] = "redis"
    """Backend to use 'redis' (default) or 'memory' (not distributed, only for testing)."""

    redis_url: str = "redis://localhost:6379"
    """Redis connection URL (e.g., "redis://localhost:6379")."""

    redis_ttl: int = 310
    """Semaphore max duration (seconds), only for redis backend."""

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
    _redis_memory_semaphore_helper: MemorySemaphoreHelper | None = None
    _memory_memory_semaphore_helper: MemorySemaphoreHelper | None = None
    __acquire_pool: BlockingConnectionPool | None = None
    __release_pool: BlockingConnectionPool | None = None
    __acquire_client: Redis | None = None
    __release_client: Redis | None = None
    __acquire_script: Any = None
    __release_script: Any = None

    def __post_init__(self):
        if self.redis_max_connections < 2:
            raise ValueError("redis_max_connections must be at least 2")
        if self.redis_socket_timeout <= self.__blocking_wait_time:
            raise ValueError(
                "redis_socket_timeout must be greater than _blocking_wait_time"
            )
        if self.backend == "redis":
            self.__acquire_pool = BlockingConnectionPool.from_url(
                self.redis_url,
                max_connections=self.redis_max_connections // 2,
                timeout=None,
                retry_on_timeout=False,
                retry_on_error=False,
                health_check_interval=10,
                socket_connect_timeout=self.redis_socket_connect_timeout,
                socket_timeout=self.redis_socket_timeout,
            )
            self.__release_pool = BlockingConnectionPool.from_url(
                self.redis_url,
                max_connections=self.redis_max_connections // 2,
                timeout=None,
            )
            self.__acquire_client = Redis.from_pool(self.__acquire_pool)
            self.__release_client = Redis.from_pool(self.__release_pool)
            self.__acquire_script = self.__acquire_client.register_script(
                ACQUIRE_LUA_SCRIPT
            )
            self.__release_script = self.__release_client.register_script(
                RELEASE_LUA_SCRIPT
            )
        self._redis_memory_semaphore_helper = MemorySemaphoreHelper(
            namespace=self.namespace,
            ttl=self.redis_ttl,
        )
        self._memory_memory_semaphore_helper = MemorySemaphoreHelper(
            namespace=self.namespace,
            ttl=None,
        )

    def _get_redis_semaphore(self, key: str, value: int) -> AsyncContextManager[None]:
        assert self.__acquire_client is not None
        assert self.__release_client is not None
        assert self._redis_memory_semaphore_helper is not None
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
            _acquire_client=self.__acquire_client,
            _release_client=self.__release_client,
            _acquire_script=self.__acquire_script,
            _release_script=self.__release_script,
            _local_semaphore=self._redis_memory_semaphore_helper.get_semaphore(
                key=key,
                value=value,
            ),
        )

    def _get_memory_semaphore(self, key: str, value: int) -> AsyncContextManager[None]:
        assert self._memory_memory_semaphore_helper is not None
        return self._memory_memory_semaphore_helper.get_semaphore(key, value)

    def get_semaphore(self, key: str, value: int) -> AsyncContextManager[None]:
        """Get a distributed semaphore for the given key (with the given value)."""
        if self.backend == "redis":
            return self._get_redis_semaphore(key, value)
        elif self.backend == "memory":
            return self._get_memory_semaphore(key, value)
        raise ValueError(f"Invalid backend: {self.backend}")
