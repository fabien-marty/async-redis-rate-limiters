import asyncio
from dataclasses import dataclass
import logging
import time
from typing import Any
import uuid

from redis.asyncio import Redis

from tenacity import (
    AsyncRetrying,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)


async def _super_shield(task: asyncio.Task, timeout: float = 10.0) -> None:
    """This function completly shields the given task against cancellations.

    It waits for the task to be completed up to the given timeout and completely
    ignore any cancellation error in this interval.

    """
    before = time.perf_counter()
    while True:
        try:
            async with asyncio.timeout(timeout):
                await asyncio.shield(task)
            break
        except (asyncio.TimeoutError, asyncio.CancelledError):
            if time.perf_counter() - before > timeout:
                logging.warning(
                    "super_shield: failed to end the shielded task within the timeout"
                )
                break


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

    _acquire_client: Redis
    _release_client: Redis
    _acquire_script: Any
    _release_script: Any

    _max_wait_time: int = 1
    __client_id: str | None = None
    __entered: bool = False

    def _get_list(self) -> str:
        return f"{self.namespace}:rate_limiter:list:{self.key}"

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
        if self.__entered:
            print("""BAD USAGE:
DON'T DO THIS:
                  
semaphore = manager.get_semaphore("test", 1)
                  
...
                  
with semaphore:
    # protected code
                  

BUT DO THIS INSTEAD:
                  
manager = DistributedSemaphoreManager(...) # the manager object should be shared
                  
...
                  
with manager.get_semaphore("test", 1):
    # protected code
""")
            raise RuntimeError(
                "Semaphore already acquired (in the past) => don't reuse the output of get_semaphore()"
            )
        self.__entered = True
        client_id = str(uuid.uuid4()).replace("-", "")
        try:
            async for attempt in self._async_retrying():
                with attempt:
                    # Try to get the lock
                    while True:
                        now = time.time()
                        acquired = await self._acquire_script(
                            keys=[self._get_zset_key()],
                            args=[
                                client_id,
                                self.value,
                                self.ttl,
                                now,
                            ],
                        )
                        if acquired == 1:
                            self.__client_id = client_id
                            return
                        # Wait for notification
                        while True:
                            res = await self._acquire_client.blpop(  # type: ignore
                                [self._get_list()], self._max_wait_time
                            )
                            if res is not None:
                                expire_at = float(res[1])
                                if expire_at < now:
                                    # this is an old notification => let's pop another one
                                    continue
                            # we timeouted or popped a recent notification => let's try again to get the lock
                            break
        except BaseException:
            # note: catch any exception here (including asyncio.CancelledError)
            # we have to clean all acquired resources here
            # before re-raising the exception
            # This is very important to avoid leaking semaphores!
            task = asyncio.create_task(self.__release(client_id))
            await _super_shield(task)
            raise

    async def __release(self, client_id: str) -> None:
        """Release logic, must be called with a shild to protected
        the code to be cancelled in the middle of the release."""
        assert self.__client_id is not None
        async for attempt in self._async_retrying():
            with attempt:
                now = time.time()
                await self._release_script(
                    keys=[self._get_zset_key(), self._get_list()],
                    args=[client_id, self.value, self.ttl, now],
                )

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.__client_id is not None:
            task = asyncio.create_task(self.__release(self.__client_id))
            await _super_shield(task)
