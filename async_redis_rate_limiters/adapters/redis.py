from dataclasses import dataclass
import time
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
    _max_wait_time: int
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
                                await pubsub.get_message(timeout=self._max_wait_time)

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
