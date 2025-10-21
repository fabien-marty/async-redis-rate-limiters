# async-redis-rate-limiters

![Python Badge](https://raw.githubusercontent.com/fabien-marty/common/refs/heads/main/badges/python310plus.svg)
[![UV Badge](https://raw.githubusercontent.com/fabien-marty/common/refs/heads/main/badges/uv.svg)](https://docs.astral.sh/uv/)
[![Mergify Badge](https://raw.githubusercontent.com/fabien-marty/common/refs/heads/main/badges/mergify.svg)](https://mergify.com/)
[![Renovate Badge](https://raw.githubusercontent.com/fabien-marty/common/refs/heads/main/badges/renovate.svg)](https://docs.renovatebot.com/)
[![MIT Licensed](https://raw.githubusercontent.com/fabien-marty/common/refs/heads/main/badges/mit.svg)](https://en.wikipedia.org/wiki/MIT_License)

Rock solid async python generic distributed rate limiters (concurrency and time) backed by Redis.

> [!WARNING]  
> This is a very preliminary version of the library and only concurrency limiters are available for now.

## Features

- ✅ Support very high concurrency (>100K), keep a reasonable number of connections to Redis (default: 300)
- ✅ Rock solid with Redis/Network failures (multiple attempts, exponential backoff, etc.)
    - you can restart the Redis server during the execution without any exception or losing any semaphore! *(of course, if persistence is setup in the redis instance)*
- ✅ Very high performances with almost no polling at all
- ✅ Memory backend (for testing)

## Non-features

- ❌ No time based rate limiters (yet)
- ❌ No blocking support, only async Python

## Installation

```bash
pip install async-redis-rate-limiters
```

*(or same with your favorite package manager)*

## Usage

```python
import asyncio
from async_redis_rate_limiters import DistributedSemaphoreManager


async def worker(manager: DistributedSemaphoreManager):
    # Limit the concurrency to 10 concurrent tasks for the key "test"
    async with manager.get_semaphore("test", 10):
        # concurrency limit enforced here
        pass


async def main():
    manager = DistributedSemaphoreManager(
        redis_url="redis://localhost:6379",
        redis_max_connections=100,
        redis_ttl=3600,  # semaphore max duration (seconds)
    )
    tasks = [asyncio.create_task(worker(manager)) for _ in range(1000)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())

```

<details>

<summary>What about if you want to use the memory backend?</summary>

**WARNING: the memory backend is just a wrapper on a classic `asyncio.Semaphore`, it is not "distributed" at all!**

```python
manager = DistributedSemaphoreManager(
    backend = "memory"
)

# and use it classically
```

## Dev

- Lint the code:

`make lint`

- Run the tests:

`make test`

note: you need a redis instance listening to localhost:6379