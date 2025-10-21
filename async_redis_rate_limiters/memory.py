import asyncio
from dataclasses import dataclass, field
import random
import time


@dataclass(frozen=True, kw_only=True)
class _KeyValue:
    key: str
    value: int


@dataclass(kw_only=True)
class _SemaphoreAndLastUsage:
    semaphore: asyncio.Semaphore
    _last_usage: float = field(default_factory=time.perf_counter)

    def is_expired(self, ttl: int | None) -> bool:
        if ttl is None:
            return False
        return time.perf_counter() - self._last_usage > ttl

    def update_last_usage(self) -> None:
        self._last_usage = time.perf_counter()


@dataclass(frozen=True)
class MemorySemaphoreHelper:
    namespace: str
    ttl: int | None
    __dict: dict[_KeyValue, _SemaphoreAndLastUsage] = field(default_factory=dict)

    def _reset(self) -> None:
        """Only for testing purposes."""
        self.__dict.clear()

    def clean(self) -> int:
        keys_to_remove = [
            key for key, value in self.__dict.items() if value.is_expired(self.ttl)
        ]
        for key in keys_to_remove:
            del self.__dict[key]
        return len(keys_to_remove)

    def may_clean(self) -> int:
        if random.randint(0, 999) == 500:
            return self.clean()
        return 0

    def get_semaphore(self, key: str, value: int) -> asyncio.Semaphore:
        self.may_clean()
        salu: _SemaphoreAndLastUsage
        kv = _KeyValue(key=key, value=value)
        if kv not in self.__dict:
            salu = _SemaphoreAndLastUsage(semaphore=asyncio.Semaphore(value))
            self.__dict[kv] = salu
        else:
            salu = self.__dict[kv]
            salu.update_last_usage()
        return salu.semaphore
